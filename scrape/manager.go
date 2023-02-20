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
	"hash/fnv"
	"net"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"

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
type Manager struct {
	logger    log.Logger
	append    storage.Appendable
	graceShut chan struct{}

	jitterSeed    uint64     // Global jitterSeed seed is used to spread scrape workload across HA setup.
	mtxScrape     sync.Mutex // Guards the fields below.
	scrapeConfigs map[string]*config.ScrapeConfig
	scrapePools   map[string]*scrapePool
	targetSets    map[string][]*targetgroup.Group

	triggerReload chan struct{}
}

// Run receives and saves target set updates and triggers the scraping loops reloading.
// Reloading happens in the background so that it doesn't block receiving targets updates.
func (m *Manager) Run(tsets <-chan map[string][]*targetgroup.Group) error {
	go m.reloader()
	for {
		select {
		case ts := <-tsets:
			m.updateTsets(ts)

			select {
			case m.triggerReload <- struct{}{}:
			default:
			}

		case <-m.graceShut:
			return nil
		}
	}
}

func (m *Manager) reloader() {
	//scrape模块加载采集点最小间隔5秒，避免采集点频繁变更导致scrape性能问题
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-m.graceShut:
			return
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

func (m *Manager) reload() {
	m.mtxScrape.Lock()
	var wg sync.WaitGroup
	for setName, groups := range m.targetSets {
		// targetSets是map[string][]*targetgroup.Group类型，key是job名称

		//创建scrapePool，一个job对应一个scrapePool结构体实例
		if _, ok := m.scrapePools[setName]; !ok {
			scrapeConfig, ok := m.scrapeConfigs[setName]
			if !ok {
				level.Error(m.logger).Log("msg", "error reloading target set", "err", "invalid config id:"+setName)
				continue
			}
			sp, err := newScrapePool(scrapeConfig, m.append, m.jitterSeed, log.With(m.logger, "scrape_pool", setName))
			if err != nil {
				level.Error(m.logger).Log("msg", "error creating new scrape pool", "err", err, "scrape_pool", setName)
				continue
			}
			m.scrapePools[setName] = sp
		}

		wg.Add(1)
		// Run the sync in parallel as these take a while and at high load can't catch up.
		go func(sp *scrapePool, groups []*targetgroup.Group) {
			sp.Sync(groups)
			wg.Done()
		}(m.scrapePools[setName], groups)

	}
	m.mtxScrape.Unlock()
	wg.Wait()
}

// setJitterSeed calculates a global jitterSeed per server relying on extra label set.
// 不同实例的hostname/externalLabels一般不同，故计算出来的jitterSeed值也不同。
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

	c := make(map[string]*config.ScrapeConfig)
	for _, scfg := range cfg.ScrapeConfigs {
		c[scfg.JobName] = scfg
	}
	m.scrapeConfigs = c

	/**
	prometheus拉取target的metrics时，都是错开时间分别拉取的，这样有以下好处：
		1、避免prometheus实例启动太多的goroutine同时拉取；
		2、多prometheus实例拉取同一个target时，避免同时拉取对target造成流量压力；
	 通过全局的scrape jitterSeed实现：
		1、不同实例：jitterSeed是跟特定prometheus实例相关的uint64哈希值，不同实例的jitterSeed不同；
		2、单实例不同target: 使用target哈希值与jitterSeed做异或操作，得到target拉取的启动时间，不同的target启动时间不同；
	*/
	// 使用全局配置来生成一个集群内不重复的seed
	if err := m.setJitterSeed(cfg.GlobalConfig.ExternalLabels); err != nil {
		return err
	}

	// Cleanup and reload pool if the configuration has changed.
	var failed bool
	// 根据解析出来的配置生成对应的ScrapePool， 如果有并且数据没有改变的话，那就不进行操作，否则
	/**
	下面会遍历所有的manager中存储的scrapePool，scrapePool和job是一对一关系：
		1、如果scrapePool对应的job在配置中不存在了，则scrapePool会被停止后删除掉；
		2、如果scrapePool对应的job配置和当前配置对比出现了变更，则将
	*/
	for name, sp := range m.scrapePools {
		if cfg, ok := m.scrapeConfigs[name]; !ok {
			//如果新配置没有该抓取job，则停止当前scrapePool
			sp.stop()
			delete(m.scrapePools, name)
		} else if !reflect.DeepEqual(sp.config, cfg) {
			//如果抓取job还在，但是和之前相比出现变更，则重新应用最新配置
			//reload重载配置：将最新job配置设置到scrapePool中，同时scrapePool下所有target会基于最新配置重新生成并重启，因为可能涉及到认证信息、抓取路径等信息变更
			err := sp.reload(cfg)
			if err != nil {
				level.Error(m.logger).Log("msg", "error reloading scrape pool", "err", err, "scrape_pool", name)
				failed = true
			}
		}
	}

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
