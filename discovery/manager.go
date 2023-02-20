// Copyright 2016 The Prometheus Authors
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

package discovery

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/prometheus/discovery/targetgroup"
)

var (
	failedConfigs = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "prometheus_sd_failed_configs",
			Help: "Current number of service discovery configurations that failed to load.",
		},
		[]string{"name"},
	)
	discoveredTargets = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "prometheus_sd_discovered_targets",
			Help: "Current number of discovered targets.",
		},
		[]string{"name", "config"},
	)
	receivedUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_received_updates_total",
			Help: "Total number of update events received from the SD providers.",
		},
		[]string{"name"},
	)
	delayedUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_updates_delayed_total",
			Help: "Total number of update events that couldn't be sent immediately.",
		},
		[]string{"name"},
	)
	sentUpdates = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_sd_updates_total",
			Help: "Total number of update events sent to the SD consumers.",
		},
		[]string{"name"},
	)
)

func init() {
	prometheus.MustRegister(failedConfigs, discoveredTargets, receivedUpdates, delayedUpdates, sentUpdates)
}

type poolKey struct {
	setName  string
	provider string
}

// provider holds a Discoverer instance, its configuration and its subscribers.
type provider struct {
	name   string
	d      Discoverer
	subs   []string
	config interface{}
}

// NewManager is the Discovery Manager constructor.
func NewManager(ctx context.Context, logger log.Logger, options ...func(*Manager)) *Manager {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	mgr := &Manager{
		logger:         logger,
		syncCh:         make(chan map[string][]*targetgroup.Group),
		targets:        make(map[poolKey]map[string]*targetgroup.Group),
		discoverCancel: []context.CancelFunc{},
		ctx:            ctx,
		updatert:       5 * time.Second,
		triggerSend:    make(chan struct{}, 1),
	}
	for _, option := range options {
		option(mgr)
	}
	return mgr
}

// Name sets the name of the manager.
func Name(n string) func(*Manager) {
	return func(m *Manager) {
		m.mtx.Lock()
		defer m.mtx.Unlock()
		m.name = n
	}
}

// Manager maintains a set of discovery providers and sends each update to a map channel.
// Targets are grouped by the target set name.
type Manager struct {
	logger         log.Logger
	name           string
	mtx            sync.RWMutex
	ctx            context.Context
	discoverCancel []context.CancelFunc

	// Some Discoverers(eg. k8s) send only the updates for a given target group
	// so we use map[tg.Source]*targetgroup.Group to know which group to update.
	targets map[poolKey]map[string]*targetgroup.Group
	// providers keeps track of SD providers.
	providers []*provider
	// The sync channel sends the updates as a map where the key is the job value from the scrape config.
	syncCh chan map[string][]*targetgroup.Group

	// How long to wait before sending updates to the channel. The variable
	// should only be modified in unit tests.
	updatert time.Duration

	// The triggerSend channel signals to the manager that new updates have been received from providers.
	triggerSend chan struct{}
}

// Run starts the background processing
func (m *Manager) Run() error {
	go m.sender()
	for range m.ctx.Done() {
		m.cancelDiscoverers()
		return m.ctx.Err()
	}
	return nil
}

// SyncCh returns a read only channel used by all the clients to receive target updates.
func (m *Manager) SyncCh() <-chan map[string][]*targetgroup.Group {
	return m.syncCh
}

// ApplyConfig removes all running discovery providers and starts new ones using the provided config.
//入参是map类型，key是job名称，value是Configs是Config类型切片，Config则对应的一种服务发现协议
func (m *Manager) ApplyConfig(cfg map[string]Configs) error {
	//加锁
	m.mtx.Lock()
	//函数执行完后解锁
	defer m.mtx.Unlock()
	//遍历已存在的target
	for pk := range m.targets {
		/**
		如果已存在的target在配置中已不存在对应的job配置，则移除该job对应的prometheus_sd_discovered_targets指标
		prometheus_sd_discovered_targets{config="prometheus", name="scrape"}:config标签指定job，name指定告警服务发现或抓取服务发现
		*/
		if _, ok := cfg[pk.setName]; !ok {
			// 删除带有指定标签和标签值的向量指标.如果删除了指标,返回 true
			discoveredTargets.DeleteLabelValues(m.name, pk.setName)
		}
	}

	/**
	取消所有Discoverer：
		当前运行的服务发现Discoverer是基于之前配置，现在需要重新加载新配置，所以取消所有现在运行的Discoverer
	*/
	m.cancelDiscoverers()
	//m.targets存储之前服务发现的所有采集点target，重新创建map则清空之前target
	m.targets = make(map[poolKey]map[string]*targetgroup.Group)
	//清空manager之前创建的provider，基于新配置重新生成
	m.providers = nil
	//清空manager的discoverCancel，因为上步骤已经停止所有discoverer，这个就没有用处了
	m.discoverCancel = nil

	failedCount := 0
	for name, scfg := range cfg {
		/**
		注册provider：
			基于Config创建Discoverer，然后封装到provider中，并将provider存放到manager的providers切片中
		*/
		failedCount += m.registerProviders(scfg, name)
		//prometheus_sd_discovered_targets值设置0：重置
		discoveredTargets.WithLabelValues(m.name, name).Set(0)
	}
	//provider注册失败数量
	failedConfigs.WithLabelValues(m.name).Set(float64(failedCount))

	//遍历provider列表，并启动provider
	for _, prov := range m.providers {
		m.startProvider(m.ctx, prov)
	}

	return nil
}

// StartCustomProvider is used for sdtool. Only use this if you know what you're doing.
func (m *Manager) StartCustomProvider(ctx context.Context, name string, worker Discoverer) {
	p := &provider{
		name: name,
		d:    worker,
		subs: []string{name},
	}
	m.providers = append(m.providers, p)
	m.startProvider(ctx, p)
}

func (m *Manager) startProvider(ctx context.Context, p *provider) {
	level.Debug(m.logger).Log("msg", "Starting provider", "provider", p.name, "subs", fmt.Sprintf("%v", p.subs))
	ctx, cancel := context.WithCancel(ctx)
	updates := make(chan []*targetgroup.Group)

	m.discoverCancel = append(m.discoverCancel, cancel)

	go p.d.Run(ctx, updates)
	go m.updater(ctx, p, updates)
}

func (m *Manager) updater(ctx context.Context, p *provider, updates chan []*targetgroup.Group) {
	for {
		select {
		case <-ctx.Done():
			return
		case tgs, ok := <-updates:
			receivedUpdates.WithLabelValues(m.name).Inc()
			if !ok {
				level.Debug(m.logger).Log("msg", "Discoverer channel closed", "provider", p.name)
				return
			}

			for _, s := range p.subs {
				m.updateGroup(poolKey{setName: s, provider: p.name}, tgs)
			}

			select {
			case m.triggerSend <- struct{}{}:
			default:
			}
		}
	}
}

/**
  select实现非阻塞读写
  select {
    case <- chan1:
    // 如果chan1成功读到数据，则进行该case处理语句
    case chan2 <- 1:
    // 如果成功向chan2写入数据，则进行该case处理语句
    default:
    // 如果上面都没有成功，则进入default处理流程

使用规则
1.如果没有default分支,select会阻塞在多个channel上，对多个channel的读/写事件进行监控。
2.如果有一个或多个IO操作可以完成，则Go运行时系统会随机的选择一个执行，否则的话，如果有default分支，则执行default分支语句，如果连default都没有，则select语句会一直阻塞，直到至少有一个IO操作可以进行。

注意：如果tsets是无缓冲channel，case ts := <-tsets:这里从channel取值要等case中逻辑处理完成，才能继续向channel写数据，
因为ts := <-tsets从channel取出数据，但是当前协程还在处理数据，没法继续接收数据，而channel又是无缓冲的，导致没法继续写入数据

for {
		select {
		case ts := <-tsets:
			xxx
		}
	}

*/
func (m *Manager) sender() {
	ticker := time.NewTicker(m.updatert)
	defer ticker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-ticker.C: // Some discoverers send updates too often so we throttle these with the ticker.
			select {
			case <-m.triggerSend:
				sentUpdates.WithLabelValues(m.name).Inc()
				select {
				case m.syncCh <- m.allGroups():
				default:
					delayedUpdates.WithLabelValues(m.name).Inc()
					level.Debug(m.logger).Log("msg", "Discovery receiver's channel was full so will retry the next cycle")
					select {
					case m.triggerSend <- struct{}{}:
					default:
					}
				}
			default:
			}
		}
	}
}

func (m *Manager) cancelDiscoverers() {
	for _, c := range m.discoverCancel {
		c()
	}
}

func (m *Manager) updateGroup(poolKey poolKey, tgs []*targetgroup.Group) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if _, ok := m.targets[poolKey]; !ok {
		m.targets[poolKey] = make(map[string]*targetgroup.Group)
	}
	for _, tg := range tgs {
		if tg != nil { // Some Discoverers send nil target group so need to check for it to avoid panics.
			m.targets[poolKey][tg.Source] = tg
		}
	}
}

func (m *Manager) allGroups() map[string][]*targetgroup.Group {
	m.mtx.RLock()
	defer m.mtx.RUnlock()

	tSets := map[string][]*targetgroup.Group{}
	for pkey, tsets := range m.targets {
		var n int
		for _, tg := range tsets {
			// Even if the target group 'tg' is empty we still need to send it to the 'Scrape manager'
			// to signal that it needs to stop all scrape loops for this target set.
			tSets[pkey.setName] = append(tSets[pkey.setName], tg)
			n += len(tg.Targets)
		}
		discoveredTargets.WithLabelValues(m.name, pkey.setName).Set(float64(n))
	}
	return tSets
}

// registerProviders returns a number of failed SD config.
/**
1、每个job下的每种服务发现协议会被注册一个provider
2、provider内部其实包装Discoverer
*/
func (m *Manager) registerProviders(cfgs Configs, setName string) int {
	var (
		failed int
		added  bool
	)
	add := func(cfg Config) {
		for _, p := range m.providers {
			if reflect.DeepEqual(cfg, p.config) {
				//如果不同job下Config存在相等清空，则不会重复创建provider，而是在原有provider下的subs中添加对应的job名称即可
				p.subs = append(p.subs, setName)
				added = true
				return
			}
		}
		//服务发现类型
		typ := cfg.Name()

		/**
		创建Discoverer：
		1、Config是一个接口类型，主要包含两个方法：
			Name() string
			NewDiscoverer(DiscovererOptions) (Discoverer, error)
		2、不同的服务发现协议实现的Config则NewDiscoverer()方法实现不一样
		3、NewDiscoverer()函数返回Discoverer，其也是一个接口类型：
			type Discoverer interface {
				Run(ctx context.Context, up chan<- []*targetgroup.Group)
			}
		4、Discoverer就是具体分服务发现实现逻辑，调用Run方法就是启动该服务发现，其有两个入参：
			ctx：用于控制停止该Discoverer
			up：channel类型，服务发现的target通过该通道传递给目标
		*/
		d, err := cfg.NewDiscoverer(DiscovererOptions{
			Logger: log.With(m.logger, "discovery", typ),
		})
		if err != nil {
			level.Error(m.logger).Log("msg", "Cannot create service discovery", "err", err, "type", typ)
			//Discoverer创建失败，则failed累加+1
			failed++
			return
		}
		//将Discoverer封装成provider，并存放到manager中providers切片中
		//provider将Discoverer，及其生成该provider的Config，以及该Config的job列表封装
		m.providers = append(m.providers, &provider{
			//provider名称，typ是服务发现类型，len(m.providers)是provider序号
			name:   fmt.Sprintf("%s/%d", typ, len(m.providers)),
			d:      d,                 //Discoverer
			config: cfg,               //生成该provider的Config
			subs:   []string{setName}, //job名称，注意这里是切片类型，如果不同job下Config存在相等清空，则不会重复创建provider，而是在原有provider下的subs中添加对应的job名称即可
		})
		added = true
	}
	for _, cfg := range cfgs {
		add(cfg)
	}
	if !added {
		//added=false，则表示没有一个provider注册成功
		// Add an empty target group to force the refresh of the corresponding
		// scrape pool and to notify the receiver that this target set has no
		// current targets.
		// It can happen because the combined set of SD configurations is empty
		// or because we fail to instantiate all the SD configurations.
		add(StaticConfig{{}})
	}
	return failed
}

// StaticProvider holds a list of target groups that never change.
type StaticProvider struct {
	TargetGroups []*targetgroup.Group
}

// Run implements the Worker interface.
func (sd *StaticProvider) Run(ctx context.Context, ch chan<- []*targetgroup.Group) {
	// We still have to consider that the consumer exits right away in which case
	// the context will be canceled.
	select {
	case ch <- sd.TargetGroups:
	case <-ctx.Done():
	}
	close(ch)
}
