// Copyright 2013 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package scrape

import (
	"encoding"
	"fmt"
	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"hash/fnv"
	"net"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
)

var targetMetadataCache = newMetadataMetricsCollector()

// MetadataMetricsCollector is a Custom Collector for the metadata cache metrics.
type MetadataMetricsCollector struct {
	CacheEntries *prometheus.Desc
	CacheBytes   *prometheus.Desc

	scrapeManager *Manager
}

func newMetadataMetricsCollector() *MetadataMetricsCollector {
	return &MetadataMetricsCollector{
		CacheEntries: prometheus.NewDesc(
			"prometheus_target_metadata_cache_entries",
			"Total number of metric metadata entries in the cache",
			[]string{"scrape_job"},
			nil,
		),
		CacheBytes: prometheus.NewDesc(
			"prometheus_target_metadata_cache_bytes",
			"The number of bytes that are currently used for storing metric metadata in the cache",
			[]string{"scrape_job"},
			nil,
		),
	}
}

func (mc *MetadataMetricsCollector) registerManager(m *Manager) {
	mc.scrapeManager = m
}

// Describe sends the metrics descriptions to the channel.
func (mc *MetadataMetricsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- mc.CacheEntries
	ch <- mc.CacheBytes
}

// Collect creates and sends the metrics for the metadata cache.
func (mc *MetadataMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	if mc.scrapeManager == nil {
		return
	}

	for tset, targets := range mc.scrapeManager.TargetsActive() {
		var size, length int
		for _, t := range targets {
			size += t.MetadataSize()
			length += t.MetadataLength()
		}

		ch <- prometheus.MustNewConstMetric(
			mc.CacheEntries,
			prometheus.GaugeValue,
			float64(length),
			tset,
		)

		ch <- prometheus.MustNewConstMetric(
			mc.CacheBytes,
			prometheus.GaugeValue,
			float64(size),
			tset,
		)
	}
}

// NewManager is the Manager constructor
/**
  NewManager方法实例化结构体Manager：
  结构体Manager维护map类型的scrapePools和targetSets，两者key都是job_name，但scrapePools的value对应结构体scrapepool，而targetSets的value对应的结构体是Group
 */
func NewManager(logger log.Logger, app storage.Appendable) *Manager {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	m := &Manager{
		append:        app,
		logger:        logger,
		scrapeConfigs: make(map[string]*config.ScrapeConfig),
		scrapePools:   make(map[string]*scrapePool),
		graceShut:     make(chan struct{}),
		triggerReload: make(chan struct{}, 1),
	}
	targetMetadataCache.registerManager(m)

	return m
}

// Manager maintains a set of scrape pools and manages start/stop cycles
// when receiving new target groups form the discovery manager.
/**
结构体Manager维护map类型的scrapePools和targetSets，两者key都是job_name，但scrapePools的value对应结构体scrapepool，而targetSets的value对应的结构体是Group
 */
type Manager struct {
	logger    log.Logger  //系统日志
	append    storage.Appendable  //存储监控指标
	graceShut chan struct{}  //退出

	jitterSeed    uint64     // Global jitterSeed seed is used to spread scrape workload across HA setup.
	mtxScrape     sync.Mutex // Guards the fields below.  读写锁
	scrapeConfigs map[string]*config.ScrapeConfig  //prometheus.yml的srape_config配置部分，key对应job_name，value对应job_name的配置参数
	scrapePools   map[string]*scrapePool           //key对应job_name，value对应结构体scrapePool，包含该job_name下所有的targets
	targetSets    map[string][]*targetgroup.Group  //key对应job_name，value对应结构体Group，包含job_name对应的Targets，Labels和Source

	triggerReload chan struct{}   //若有新的服务(targets)通过服务发现(serviceDisvoer)传过来，会向该管道传值，触发加载配置文件操作
}

// Run receives and saves target set updates and triggers the scraping loops reloading.
// Reloading happens in the background so that it doesn't block receiving targets updates.
/**
指标采集(scrapeManager)在main.go启动时，会起一个协程运行Run方法，从服务发现(serviceDiscover)实时获取被监控服务(targets)
 */
func (m *Manager) Run(tsets <-chan map[string][]*targetgroup.Group) error {
	//定时(5s)更新服务(targets)，结合triggerReload一起使用，即每5s判断一次triggerReload是否更新．
	/**
	若服务发现(serviceDiscovery)有服务(target)变动，Run方法就会向管道triggerReload注入值：m.triggerReload <- struct{}{}中，
	并起了一个协程，运行reloader方法．用于定时更新服务(targets)．启动这个协程应该是为了防止阻塞从服务发现(serviceDiscover)获取变动的服务(targets)

	reloader方法启动了一个定时器，在无限循环中每5s判断一下管道triggerReload，若有值，则执行reload方法．
	 */

	level.Info(m.logger).Log("--------->tsets:", tsets)
	go m.reloader()
	for {
		select {
		// 触发重新加载目标。添加新增
		//通过管道获取被监控的服务(targets)
		case ts := <-tsets:
			m.updateTsets(ts)
			level.Info(m.logger).Log("--------->ts:", tsets)

			select {
			// 关闭 Scrape Manager 处理信号
			//若从服务发现 (serviceDiscover)有服务(targets)变动，则给管道triggerReload传值，并触发reloader()方法更新服务
			case m.triggerReload <- struct{}{}:
			default:
			}

		case <-m.graceShut:
			return nil
		}
	}
}


func (m *Manager) reloader() {
	//定时器5s
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.graceShut:
			return
			// 若服务发现(serviceDiscovery)有服务(targets)变动，就会向管道triggerReload写入值，定时器每5s判断一次triggerReload管道是否有值，若有值，则触发reload方法
		case <-ticker.C:
			select {
			case <-m.triggerReload:
				m.reload()
			case <-m.graceShut:
				return
			}
		}
	}
}

/**
m.reloade的流程也很简单，setName指我们配置中的job，如果scrapePools不存在该job，则添加，添加前也是先校验该job的配置是否存在，不存在则报错，创建scrape pool。
总结看就是为每个job创建与之对应的scrape pool

reload方法会根据job_name比较targetSets，scrapePools和scrapeConfigs的一致性，并把每个job_name下的类型为[]*targetgroup.Group的groups通过协程传给sp.Sync方法，增加并发．

*/
func (m *Manager) reload() {
	//加锁
	m.mtxScrape.Lock()
	var wg sync.WaitGroup
	//setName对应job_name，
	//group的结构体包含job_name对应的Targets，Labels和source
	for setName, groups := range m.targetSets {
		//检查该scrape是否存在scrapePools，不存在则创建
		if _, ok := m.scrapePools[setName]; !ok {
			//读取该scrape的配置
			scrapeConfig, ok := m.scrapeConfigs[setName]
			//若该job_name不在scrapePools中，分为两种情况处理
			//(1)job_name不在scrapeConfigs中，则报错
			//(2)job_name在scrapeConfigs中，则需要把该job_name加到scrapePools中
			if !ok {
				// 未读取到该scrape的配置打印错误
				level.Error(m.logger).Log("msg", "error reloading target set", "err", "invalid config id:"+setName)
				// 跳出
				continue
			}
			// 创建该scrape的scrape pool
			sp, err := newScrapePool(scrapeConfig, m.append, m.jitterSeed, log.With(m.logger, "scrape_pool", setName))
			if err != nil {
				level.Error(m.logger).Log("msg", "error creating new scrape pool", "err", err, "scrape_pool", setName)
				continue
			}
			// 保存
			m.scrapePools[setName] = sp
		}

		wg.Add(1)
		// 并行运行，提升性能。
		// Run the sync in parallel as these take a while and at high load can't catch up.
		go func(sp *scrapePool, groups []*targetgroup.Group) {
			//把groups转换为targets类型
			sp.Sync(groups)
			wg.Done()
		}(m.scrapePools[setName], groups)

	}
	// 释放锁
	m.mtxScrape.Unlock()
	// 阻塞，等待所有pool运行完毕
	wg.Wait()
}

// setJitterSeed calculates a global jitterSeed per server relying on extra label set.
func (m *Manager) setJitterSeed(labels labels.Labels) error {
	h := fnv.New64a()
	hostname, err := getFqdn()
	if err != nil {
		return err
	}
	if _, err := fmt.Fprintf(h, "%s%s", hostname, labels.String()); err != nil {
		return err
	}
	m.jitterSeed = h.Sum64()
	return nil
}

// Stop cancels all running scrape pools and blocks until all have exited.
func (m *Manager) Stop() {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	for _, sp := range m.scrapePools {
		sp.stop()
	}
	close(m.graceShut)
}

func (m *Manager) updateTsets(tsets map[string][]*targetgroup.Group) {
	m.mtxScrape.Lock()
	m.targetSets = tsets
	m.mtxScrape.Unlock()
}

// ApplyConfig resets the manager's target providers and job configurations as defined by the new cfg.
func (m *Manager) ApplyConfig(cfg *config.Config) error {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()
	// 初始化map结构，用于保存配置
	c := make(map[string]*config.ScrapeConfig)
	for _, scfg := range cfg.ScrapeConfigs {
		// 配置读取维度
		c[scfg.JobName] = scfg
	}
	m.scrapeConfigs = c
	// 设置 所有时间序列和警告与外部通信时用的外部标签 external_labels
	if err := m.setJitterSeed(cfg.GlobalConfig.ExternalLabels); err != nil {
		return err
	}

	// Cleanup and reload pool if the configuration has changed.
	// 如果配置已经更改，清理历史配置，重新加载到池子中
	var failed bool
	for name, sp := range m.scrapePools {
		// 如果当前job不存在，则删除
		if cfg, ok := m.scrapeConfigs[name]; !ok {
			sp.stop()
			delete(m.scrapePools, name)
		} else if !reflect.DeepEqual(sp.config, cfg) {
			// 如果配置变更，重新启动reload，进行加载
			err := sp.reload(cfg)
			if err != nil {
				level.Error(m.logger).Log("msg", "error reloading scrape pool", "err", err, "scrape_pool", name)
				failed = true
			}
		}
	}
	// 失败 return
	if failed {
		return errors.New("failed to apply the new configuration")
	}
	return nil
}




// TargetsAll returns active and dropped targets grouped by job_name.
func (m *Manager) TargetsAll() map[string][]*Target {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	targets := make(map[string][]*Target, len(m.scrapePools))
	for tset, sp := range m.scrapePools {
		targets[tset] = append(sp.ActiveTargets(), sp.DroppedTargets()...)
	}
	return targets
}

// TargetsActive returns the active targets currently being scraped.
func (m *Manager) TargetsActive() map[string][]*Target {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	var (
		wg  sync.WaitGroup
		mtx sync.Mutex
	)

	targets := make(map[string][]*Target, len(m.scrapePools))
	wg.Add(len(m.scrapePools))
	for tset, sp := range m.scrapePools {
		// Running in parallel limits the blocking time of scrapePool to scrape
		// interval when there's an update from SD.
		go func(tset string, sp *scrapePool) {
			mtx.Lock()
			targets[tset] = sp.ActiveTargets()
			mtx.Unlock()
			wg.Done()
		}(tset, sp)
	}
	wg.Wait()
	return targets
}

// TargetsDropped returns the dropped targets during relabelling.
func (m *Manager) TargetsDropped() map[string][]*Target {
	m.mtxScrape.Lock()
	defer m.mtxScrape.Unlock()

	targets := make(map[string][]*Target, len(m.scrapePools))
	for tset, sp := range m.scrapePools {
		targets[tset] = sp.DroppedTargets()
	}
	return targets
}

// getFqdn returns a FQDN if it's possible, otherwise falls back to hostname.
func getFqdn() (string, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return "", err
	}

	ips, err := net.LookupIP(hostname)
	if err != nil {
		// Return the system hostname if we can't look up the IP address.
		return hostname, nil
	}

	lookup := func(ipStr encoding.TextMarshaler) (string, error) {
		ip, err := ipStr.MarshalText()
		if err != nil {
			return "", err
		}
		hosts, err := net.LookupAddr(string(ip))
		if err != nil || len(hosts) == 0 {
			return "", err
		}
		return hosts[0], nil
	}

	for _, addr := range ips {
		if ip := addr.To4(); ip != nil {
			if fqdn, err := lookup(ip); err == nil {
				return fqdn, nil
			}

		}

		if ip := addr.To16(); ip != nil {
			if fqdn, err := lookup(ip); err == nil {
				return fqdn, nil
			}

		}
	}
	return hostname, nil
}
