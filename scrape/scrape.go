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

package scrape

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"reflect"
	"sync"
	"time"
	"unsafe"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	config_util "github.com/prometheus/common/config"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/version"

	"github.com/prometheus/prometheus/config"
	"github.com/prometheus/prometheus/discovery/targetgroup"
	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/pool"
	"github.com/prometheus/prometheus/pkg/relabel"
	"github.com/prometheus/prometheus/pkg/textparse"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/pkg/value"
	"github.com/prometheus/prometheus/storage"
)

// Temporary tolerance for scrape appends timestamps alignment, to enable better
// compression at the TSDB level.
// See https://github.com/prometheus/prometheus/issues/7846
const scrapeTimestampTolerance = 2 * time.Millisecond

// AlignScrapeTimestamps enables the tolerance for scrape appends timestamps described above.
var AlignScrapeTimestamps = true

var errNameLabelMandatory = fmt.Errorf("missing metric name (%s label)", labels.MetricName)

var (
	targetIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_interval_length_seconds",
			Help:       "Actual intervals between scrapes.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"interval"},
	)
	targetReloadIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_reload_length_seconds",
			Help:       "Actual interval to reload the scrape pool with a given configuration.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"interval"},
	)
	targetScrapePools = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pools_total",
			Help: "Total number of scrape pool creation attempts.",
		},
	)
	targetScrapePoolsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pools_failed_total",
			Help: "Total number of scrape pool creations that failed.",
		},
	)
	targetScrapePoolReloads = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_reloads_total",
			Help: "Total number of scrape pool reloads.",
		},
	)
	targetScrapePoolReloadsFailed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_reloads_failed_total",
			Help: "Total number of failed scrape pool reloads.",
		},
	)
	targetScrapePoolExceededTargetLimit = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_exceeded_target_limit_total",
			Help: "Total number of times scrape pools hit the target limit, during sync or config reload.",
		},
	)
	targetScrapePoolTargetLimit = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "prometheus_target_scrape_pool_target_limit",
			Help: "Maximum number of targets allowed in this scrape pool.",
		},
		[]string{"scrape_job"},
	)
	targetScrapePoolTargetsAdded = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "prometheus_target_scrape_pool_targets",
			Help: "Current number of targets in this scrape pool.",
		},
		[]string{"scrape_job"},
	)
	targetSyncIntervalLength = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "prometheus_target_sync_length_seconds",
			Help:       "Actual interval to sync the scrape pool.",
			Objectives: map[float64]float64{0.01: 0.001, 0.05: 0.005, 0.5: 0.05, 0.90: 0.01, 0.99: 0.001},
		},
		[]string{"scrape_job"},
	)
	targetScrapePoolSyncsCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_sync_total",
			Help: "Total number of syncs that were executed on a scrape pool.",
		},
		[]string{"scrape_job"},
	)
	targetScrapeSampleLimit = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_exceeded_sample_limit_total",
			Help: "Total number of scrapes that hit the sample limit and were rejected.",
		},
	)
	targetScrapeSampleDuplicate = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_duplicate_timestamp_total",
			Help: "Total number of samples rejected due to duplicate timestamps but different values.",
		},
	)
	targetScrapeSampleOutOfOrder = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_out_of_order_total",
			Help: "Total number of samples rejected due to not being out of the expected order.",
		},
	)
	targetScrapeSampleOutOfBounds = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_sample_out_of_bounds_total",
			Help: "Total number of samples rejected due to timestamp falling outside of the time bounds.",
		},
	)
	targetScrapeCacheFlushForced = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_cache_flush_forced_total",
			Help: "How many times a scrape cache was flushed due to getting big while scrapes are failing.",
		},
	)
	targetScrapeExemplarOutOfOrder = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrapes_exemplar_out_of_order_total",
			Help: "Total number of exemplar rejected due to not being out of the expected order.",
		},
	)
	targetScrapePoolExceededLabelLimits = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "prometheus_target_scrape_pool_exceeded_label_limits_total",
			Help: "Total number of times scrape pools hit the label limits, during sync or config reload.",
		},
	)
)

func init() {
	prometheus.MustRegister(
		targetIntervalLength,
		targetReloadIntervalLength,
		targetScrapePools,
		targetScrapePoolsFailed,
		targetScrapePoolReloads,
		targetScrapePoolReloadsFailed,
		targetSyncIntervalLength,
		targetScrapePoolSyncsCounter,
		targetScrapeSampleLimit,
		targetScrapeSampleDuplicate,
		targetScrapeSampleOutOfOrder,
		targetScrapeSampleOutOfBounds,
		targetScrapePoolExceededTargetLimit,
		targetScrapePoolTargetLimit,
		targetScrapePoolTargetsAdded,
		targetScrapeCacheFlushForced,
		targetMetadataCache,
		targetScrapeExemplarOutOfOrder,
		targetScrapePoolExceededLabelLimits,
	)
}

// scrapePool manages scrapes for sets of targets.
type scrapePool struct {
	appendable storage.Appendable
	logger     log.Logger
	// 取消
	cancel     context.CancelFunc

	// mtx must not be taken after targetMtx.
	mtx            sync.Mutex
	config         *config.ScrapeConfig
	client         *http.Client
	// 所有运行的loop
	loops          map[uint64]loop
	targetLimitHit bool // Internal state to speed up the target_limit checks.

	targetMtx sync.Mutex
	// activeTargets and loops must always be synchronized to have the same
	// set of hashes.
	// 正在运行的target
	activeTargets  map[uint64]*Target
	// 无效的target
	droppedTargets []*Target

	// 创建loop
	// Constructor for new scrape loops. This is settable for testing convenience.
	newLoop func(scrapeLoopOptions) loop
}

type labelLimits struct {
	labelLimit            int
	labelNameLengthLimit  int
	labelValueLengthLimit int
}

type scrapeLoopOptions struct {
	target          *Target
	scraper         scraper
	sampleLimit     int
	labelLimits     *labelLimits
	honorLabels     bool
	honorTimestamps bool
	mrc             []*relabel.Config
	cache           *scrapeCache
}

const maxAheadTime = 10 * time.Minute

type labelsMutator func(labels.Labels) labels.Labels

/**
scrape pool利用newLoop去为该job下的所有target生成对应的loop：
 */
func newScrapePool(cfg *config.ScrapeConfig, app storage.Appendable, jitterSeed uint64, logger log.Logger) (*scrapePool, error) {
	targetScrapePools.Inc()
	if logger == nil {
		logger = log.NewNopLogger()
	}
	// 创建http client，用于执行数据抓取
	client, err := config_util.NewClientFromConfig(cfg.HTTPClientConfig, cfg.JobName, config_util.WithHTTP2Disabled())
	if err != nil {
		targetScrapePoolsFailed.Inc()
		return nil, errors.Wrap(err, "error creating HTTP client")
	}
	// 设置buffers
	buffers := pool.New(1e3, 100e6, 3, func(sz int) interface{} { return make([]byte, 0, sz) })
	// 设置scrapePool的一些基础属性
	ctx, cancel := context.WithCancel(context.Background())
	sp := &scrapePool{
		cancel:        cancel,
		appendable:    app,
		config:        cfg,
		client:        client,
		activeTargets: map[uint64]*Target{},
		loops:         map[uint64]loop{},
		logger:        logger,
	}
	// newLoop用于生层loop，主要处理对应的target，可以理解为，每个target对应一个loop。
	sp.newLoop = func(opts scrapeLoopOptions) loop {
		// Update the targets retrieval function for metadata to a new scrape cache.
		cache := opts.cache
		if cache == nil {
			cache = newScrapeCache()
		}
		opts.target.SetMetadataStore(cache)

		return newScrapeLoop(
			ctx,
			opts.scraper,
			log.With(logger, "target", opts.target),
			buffers,
			func(l labels.Labels) labels.Labels {
				return mutateSampleLabels(l, opts.target, opts.honorLabels, opts.mrc)
			},
			func(l labels.Labels) labels.Labels { return mutateReportSampleLabels(l, opts.target) },
			func(ctx context.Context) storage.Appender { return appender(app.Appender(ctx), opts.sampleLimit) },
			cache,
			jitterSeed,
			opts.honorTimestamps,
			opts.labelLimits,
		)
	}

	return sp, nil
}

func (sp *scrapePool) ActiveTargets() []*Target {
	sp.targetMtx.Lock()
	defer sp.targetMtx.Unlock()

	var tActive []*Target
	for _, t := range sp.activeTargets {
		tActive = append(tActive, t)
	}
	return tActive
}

func (sp *scrapePool) DroppedTargets() []*Target {
	sp.targetMtx.Lock()
	defer sp.targetMtx.Unlock()
	return sp.droppedTargets
}

// stop terminates all scrape loops and returns after they all terminated.
func (sp *scrapePool) stop() {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()
	sp.cancel()
	var wg sync.WaitGroup

	sp.targetMtx.Lock()

	for fp, l := range sp.loops {
		wg.Add(1)

		go func(l loop) {
			l.stop()
			wg.Done()
		}(l)

		delete(sp.loops, fp)
		delete(sp.activeTargets, fp)
	}

	sp.targetMtx.Unlock()

	wg.Wait()
	sp.client.CloseIdleConnections()

	if sp.config != nil {
		targetScrapePoolSyncsCounter.DeleteLabelValues(sp.config.JobName)
		targetScrapePoolTargetLimit.DeleteLabelValues(sp.config.JobName)
		targetScrapePoolTargetsAdded.DeleteLabelValues(sp.config.JobName)
		targetSyncIntervalLength.DeleteLabelValues(sp.config.JobName)
	}
}

// reload the scrape pool with the given scrape configuration. The target state is preserved
// but all scrape loops are restarted with the new scrape configuration.
// This method returns after all scrape loops that were stopped have stopped scraping.
func (sp *scrapePool) reload(cfg *config.ScrapeConfig) error {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()
	targetScrapePoolReloads.Inc()
	start := time.Now()

	client, err := config_util.NewClientFromConfig(cfg.HTTPClientConfig, cfg.JobName, config_util.WithHTTP2Disabled())
	if err != nil {
		targetScrapePoolReloadsFailed.Inc()
		return errors.Wrap(err, "error creating HTTP client")
	}

	reuseCache := reusableCache(sp.config, cfg)
	sp.config = cfg
	oldClient := sp.client
	sp.client = client

	targetScrapePoolTargetLimit.WithLabelValues(sp.config.JobName).Set(float64(sp.config.TargetLimit))

	var (
		wg          sync.WaitGroup
		interval    = time.Duration(sp.config.ScrapeInterval)
		timeout     = time.Duration(sp.config.ScrapeTimeout)
		sampleLimit = int(sp.config.SampleLimit)
		labelLimits = &labelLimits{
			labelLimit:            int(sp.config.LabelLimit),
			labelNameLengthLimit:  int(sp.config.LabelNameLengthLimit),
			labelValueLengthLimit: int(sp.config.LabelValueLengthLimit),
		}
		honorLabels     = sp.config.HonorLabels
		honorTimestamps = sp.config.HonorTimestamps
		mrc             = sp.config.MetricRelabelConfigs
	)

	sp.targetMtx.Lock()

	forcedErr := sp.refreshTargetLimitErr()
	for fp, oldLoop := range sp.loops {
		var cache *scrapeCache
		if oc := oldLoop.getCache(); reuseCache && oc != nil {
			oldLoop.disableEndOfRunStalenessMarkers()
			cache = oc
		} else {
			cache = newScrapeCache()
		}
		var (
			t       = sp.activeTargets[fp]
			s       = &targetScraper{Target: t, client: sp.client, timeout: timeout}
			newLoop = sp.newLoop(scrapeLoopOptions{
				target:          t,
				scraper:         s,
				sampleLimit:     sampleLimit,
				labelLimits:     labelLimits,
				honorLabels:     honorLabels,
				honorTimestamps: honorTimestamps,
				mrc:             mrc,
				cache:           cache,
			})
		)
		wg.Add(1)

		go func(oldLoop, newLoop loop) {
			oldLoop.stop()
			wg.Done()

			newLoop.setForcedError(forcedErr)
			newLoop.run(interval, timeout, nil)
		}(oldLoop, newLoop)

		sp.loops[fp] = newLoop
	}

	sp.targetMtx.Unlock()

	wg.Wait()
	oldClient.CloseIdleConnections()
	targetReloadIntervalLength.WithLabelValues(interval.String()).Observe(
		time.Since(start).Seconds(),
	)
	return nil
}

// Sync converts target groups into actual scrape targets and synchronizes
// the currently running scraper with the resulting set and returns all scraped and dropped targets.
/**
sp.Sync方法引入了Target结构体，把[]*targetgroup.Group类型的groups转换为targets类型，其中每个groups对应一个job_name下多个targets．随后，调用sp.sync方法，同步scrape服务
 */
func (sp *scrapePool) Sync(tgs []*targetgroup.Group) {
	sp.mtx.Lock()
	defer sp.mtx.Unlock()
	start := time.Now()

	sp.targetMtx.Lock()
	var all []*Target
	sp.droppedTargets = []*Target{}
	// 遍历所有Group
	for _, tg := range tgs {
		// 转化对应 targets
		//转换targetgroup.Group类型为Target
		targets, err := targetsFromGroup(tg, sp.config)
		if err != nil {
			level.Error(sp.logger).Log("msg", "creating targets failed", "err", err)
			continue
		}
		// 将所有有效targets添加到all，等待处理
		// 这里有个疑问，tg对应一个target，为什么返回回来的targets不是对应一个target相关参数，需要用for循环？
		for _, t := range targets {
			// 检查该target的lable是否有效
			// 判断Target的有效label是否大于0
			if t.Labels().Len() > 0 {
				// 添加到all队列中
				all = append(all, t)
			} else if t.DiscoveredLabels().Len() > 0 {
				// 记录无效target
				// 若为无效Target，则加入scrapeLoop的droppedTargets中
				sp.droppedTargets = append(sp.droppedTargets, t)
			}
		}
	}
	sp.targetMtx.Unlock()

	/**
	处理all队列，执行scrape同步操作：在sync最后，调用了当前scrape pool的sync去处理all队列中的target，添加新的target，删除失效的target。

	sp.sync方法对比新的Target列表和原来的Target列表，若发现不在原来的Target列表中，则新建该targets的scrapeLoop，
	通过协程启动scrapeLoop的run方法，并发采集存储指标．然后判断原来的Target列表是否存在失效的Target，若存在，则移除
	 */
	sp.sync(all)

	targetSyncIntervalLength.WithLabelValues(sp.config.JobName).Observe(
		time.Since(start).Seconds(),
	)
	targetScrapePoolSyncsCounter.WithLabelValues(sp.config.JobName).Inc()
}

// sync takes a list of potentially duplicated targets, deduplicates them, starts
// scrape loops for new targets, and stops scrape loops for disappeared targets.
// It returns after all stopped scrape loops terminated.
func (sp *scrapePool) sync(targets []*Target) {
	var (
		// target 标记
		uniqueLoops = make(map[uint64]loop)
		// 采集周期
		interval    = time.Duration(sp.config.ScrapeInterval)
		// 指标采集超时时间
		timeout     = time.Duration(sp.config.ScrapeTimeout)
		// 指标采集的限额
		sampleLimit = int(sp.config.SampleLimit)
		labelLimits = &labelLimits{
			labelLimit:            int(sp.config.LabelLimit),
			labelNameLengthLimit:  int(sp.config.LabelNameLengthLimit),
			labelValueLengthLimit: int(sp.config.LabelValueLengthLimit),
		}
		// 重复lable是否覆盖
		honorLabels     = sp.config.HonorLabels
		honorTimestamps = sp.config.HonorTimestamps
		mrc             = sp.config.MetricRelabelConfigs
	)

	sp.targetMtx.Lock()
	// 遍历all队列中的所有target
	for _, t := range targets {
		// 生成对应的hash（对该hash算法感兴趣可以看下这里的源码）
		hash := t.hash()

		// 判断该target是否已经在运行了。如果没有则运行该target对应的loop，将该loop加入activeTargets中
		// 若发现不在原来的Target列表中，则新建该target的scrapeLoop
		if _, ok := sp.activeTargets[hash]; !ok {
			s := &targetScraper{Target: t, client: sp.client, timeout: timeout}
			l := sp.newLoop(scrapeLoopOptions{
				target:          t,
				scraper:         s,
				sampleLimit:     sampleLimit,
				labelLimits:     labelLimits,
				honorLabels:     honorLabels,
				honorTimestamps: honorTimestamps,
				mrc:             mrc,
			})

			sp.activeTargets[hash] = t
			sp.loops[hash] = l
			// 启动该loop
			uniqueLoops[hash] = l
		} else {
			// This might be a duplicated target.
			if _, ok := uniqueLoops[hash]; !ok {
				uniqueLoops[hash] = nil
			}
			// 该target对应的loop已经运行，设置最新的标签信息
			// Need to keep the most updated labels information
			// for displaying it in the Service Discovery web page.
			sp.activeTargets[hash].SetDiscoveredLabels(t.DiscoveredLabels())
		}
	}

	var wg sync.WaitGroup

	// 停止并且移除无效的targets与对应的loops
	// 遍历activeTargets正在执行的Target
	// Stop and remove old targets and scraper loops.
	// 判断原来的Target列表是否存在失效的Target，若存在则移除
	for hash := range sp.activeTargets {
		// 检查该hash对应的标记是否存在，放过不存在执行清除逻辑
		if _, ok := uniqueLoops[hash]; !ok {
			wg.Add(1)
			// 异步清除
			go func(l loop) {
				// 停止该loop
				l.stop()
				// 执行完成
				wg.Done()
			}(sp.loops[hash])
			// 从loops中删除该hash对应的loop
			delete(sp.loops, hash)
			// 从activeTargets中删除该hash对应的target
			delete(sp.activeTargets, hash)
		}
	}

	sp.targetMtx.Unlock()

	targetScrapePoolTargetsAdded.WithLabelValues(sp.config.JobName).Set(float64(len(uniqueLoops)))
	forcedErr := sp.refreshTargetLimitErr()
	for _, l := range sp.loops {
		l.setForcedError(forcedErr)
	}
	for _, l := range uniqueLoops {
		if l != nil {
			// 通过协程启动scrapeLoop的run方法，采集存储指标
			// sp.sync方法起了一个协程运行scrapeLoop的run方法去采集并存储监控指标(metrics)，run方法实现如下：
			go l.run(interval, timeout, nil)
		}
	}
	// Wait for all potentially stopped scrapers to terminate.
	// This covers the case of flapping targets. If the server is under high load, a new scraper
	// may be active and tries to insert. The old scraper that didn't terminate yet could still
	// be inserting a previous sample set.
	// 等待所有执行完成
	wg.Wait()
}

// refreshTargetLimitErr returns an error that can be passed to the scrape loops
// if the number of targets exceeds the configured limit.
func (sp *scrapePool) refreshTargetLimitErr() error {
	if sp.config == nil || sp.config.TargetLimit == 0 && !sp.targetLimitHit {
		return nil
	}
	l := len(sp.activeTargets)
	if l <= int(sp.config.TargetLimit) && !sp.targetLimitHit {
		return nil
	}
	var err error
	sp.targetLimitHit = l > int(sp.config.TargetLimit)
	if sp.targetLimitHit {
		targetScrapePoolExceededTargetLimit.Inc()
		err = fmt.Errorf("target_limit exceeded (number of targets: %d, limit: %d)", l, sp.config.TargetLimit)
	}
	return err
}

func verifyLabelLimits(lset labels.Labels, limits *labelLimits) error {
	if limits == nil {
		return nil
	}

	met := lset.Get(labels.MetricName)
	if limits.labelLimit > 0 {
		nbLabels := len(lset)
		if nbLabels > int(limits.labelLimit) {
			return fmt.Errorf("label_limit exceeded (metric: %.50s, number of label: %d, limit: %d)", met, nbLabels, limits.labelLimit)
		}
	}

	if limits.labelNameLengthLimit == 0 && limits.labelValueLengthLimit == 0 {
		return nil
	}

	for _, l := range lset {
		if limits.labelNameLengthLimit > 0 {
			nameLength := len(l.Name)
			if nameLength > int(limits.labelNameLengthLimit) {
				return fmt.Errorf("label_name_length_limit exceeded (metric: %.50s, label: %.50v, name length: %d, limit: %d)", met, l, nameLength, limits.labelNameLengthLimit)
			}
		}

		if limits.labelValueLengthLimit > 0 {
			valueLength := len(l.Value)
			if valueLength > int(limits.labelValueLengthLimit) {
				return fmt.Errorf("label_value_length_limit exceeded (metric: %.50s, label: %.50v, value length: %d, limit: %d)", met, l, valueLength, limits.labelValueLengthLimit)
			}
		}
	}
	return nil
}

func mutateSampleLabels(lset labels.Labels, target *Target, honor bool, rc []*relabel.Config) labels.Labels {
	lb := labels.NewBuilder(lset)

	if honor {
		for _, l := range target.Labels() {
			if !lset.Has(l.Name) {
				lb.Set(l.Name, l.Value)
			}
		}
	} else {
		for _, l := range target.Labels() {
			// existingValue will be empty if l.Name doesn't exist.
			existingValue := lset.Get(l.Name)
			if existingValue != "" {
				lb.Set(model.ExportedLabelPrefix+l.Name, existingValue)
			}
			// It is now safe to set the target label.
			lb.Set(l.Name, l.Value)
		}
	}

	res := lb.Labels()

	if len(rc) > 0 {
		res = relabel.Process(res, rc...)
	}

	return res
}

func mutateReportSampleLabels(lset labels.Labels, target *Target) labels.Labels {
	lb := labels.NewBuilder(lset)

	for _, l := range target.Labels() {
		lb.Set(model.ExportedLabelPrefix+l.Name, lset.Get(l.Name))
		lb.Set(l.Name, l.Value)
	}

	return lb.Labels()
}

// appender returns an appender for ingested samples from the target.
func appender(app storage.Appender, limit int) storage.Appender {
	app = &timeLimitAppender{
		Appender: app,
		maxTime:  timestamp.FromTime(time.Now().Add(maxAheadTime)),
	}

	// The limit is applied after metrics are potentially dropped via relabeling.
	if limit > 0 {
		app = &limitAppender{
			Appender: app,
			limit:    limit,
		}
	}
	return app
}

// A scraper retrieves samples and accepts a status report at the end.
type scraper interface {
	scrape(ctx context.Context, w io.Writer) (string, error)
	Report(start time.Time, dur time.Duration, err error)
	offset(interval time.Duration, jitterSeed uint64) time.Duration
}

// targetScraper implements the scraper interface for a target.
type targetScraper struct {
	*Target

	client  *http.Client
	req     *http.Request
	timeout time.Duration

	gzipr *gzip.Reader
	buf   *bufio.Reader
}

const acceptHeader = `application/openmetrics-text; version=0.0.1,text/plain;version=0.0.4;q=0.5,*/*;q=0.1`

var userAgentHeader = fmt.Sprintf("Prometheus/%s", version.Version)

/**
依赖scrape实现数据的抓取，使用GET方法。
 */
func (s *targetScraper) scrape(ctx context.Context, w io.Writer) (string, error) {
	if s.req == nil {
		// 新建Http Request
		req, err := http.NewRequest("GET", s.URL().String(), nil)
		if err != nil {
			return "", err
		}
		// 设置请求头
		req.Header.Add("Accept", acceptHeader)
		req.Header.Add("Accept-Encoding", "gzip")
		req.Header.Set("User-Agent", userAgentHeader)
		req.Header.Set("X-Prometheus-Scrape-Timeout-Seconds", fmt.Sprintf("%f", s.timeout.Seconds()))

		s.req = req
	}

	// 发起请求
	resp, err := s.client.Do(s.req.WithContext(ctx))
	if err != nil {
		return "", err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	// 错误处理
	if resp.StatusCode != http.StatusOK {
		return "", errors.Errorf("server returned HTTP status %s", resp.Status)
	}
	// 检查Content-Encoding
	if resp.Header.Get("Content-Encoding") != "gzip" {
		// copy buffer到w
		_, err = io.Copy(w, resp.Body)
		if err != nil {
			return "", err
		}
		return resp.Header.Get("Content-Type"), nil
	}

	if s.gzipr == nil {
		s.buf = bufio.NewReader(resp.Body)
		s.gzipr, err = gzip.NewReader(s.buf)
		if err != nil {
			return "", err
		}
	} else {
		s.buf.Reset(resp.Body)
		if err = s.gzipr.Reset(s.buf); err != nil {
			return "", err
		}
	}

	_, err = io.Copy(w, s.gzipr)
	s.gzipr.Close()
	if err != nil {
		return "", err
	}
	return resp.Header.Get("Content-Type"), nil
}

// A loop can run and be stopped again. It must not be reused after it was stopped.
type loop interface {
	run(interval, timeout time.Duration, errc chan<- error)
	setForcedError(err error)
	stop()
	getCache() *scrapeCache
	disableEndOfRunStalenessMarkers()
}
/**
	添加到本地数据库或者远程数据库的一个返回值:
 */
type cacheEntry struct {
	ref      uint64
	lastIter uint64		//上一个版本号
	hash     uint64		// hash值
	lset     labels.Labels	//包含的labels
}

type scrapeLoop struct {
	scraper         scraper
	l               log.Logger
	cache           *scrapeCache
	lastScrapeSize  int
	buffers         *pool.Pool
	jitterSeed      uint64
	honorTimestamps bool
	forcedErr       error
	forcedErrMtx    sync.Mutex
	labelLimits     *labelLimits

	appender            func(ctx context.Context) storage.Appender
	sampleMutator       labelsMutator
	reportSampleMutator labelsMutator

	parentCtx context.Context
	ctx       context.Context
	cancel    func()
	stopped   chan struct{}

	disabledEndOfRunStalenessMarkers bool
}

// scrapeCache tracks mappings of exposed metric strings to label sets and
// storage references. Additionally, it tracks staleness of series between
// scrapes.
type scrapeCache struct {
	iter uint64 // Current scrape iteration.  //被缓存的迭代次数

	// How many series and metadata entries there were at the last success.
	successfulCount int

	// Parsed string to an entry with information about the actual label set
	// and its storage reference.
	series map[string]*cacheEntry  //map类型,key是metric,value是cacheEntry结构体

	// Cache of dropped metric strings and their iteration. The iteration must
	// be a pointer so we can update it without setting a new entry with an unsafe
	// string in addDropped().
	droppedSeries map[string]*uint64  //缓存不合法指标(metrics)

	// seriesCur and seriesPrev store the labels of series that were seen
	// in the current and previous scrape.
	// We hold two maps and swap them out to save allocations.
	seriesCur  map[uint64]labels.Labels   //缓存本次scrape的指标(metrics)
	seriesPrev map[uint64]labels.Labels   //缓存上次scrape的指标(metrics)

	metaMtx  sync.Mutex  //同步锁
	metadata map[string]*metaEntry   //元数据
}

// metaEntry holds meta information about a metric.
type metaEntry struct {
	lastIter uint64 // Last scrape iteration the entry was observed at.
	typ      textparse.MetricType
	help     string
	unit     string
}

func (m *metaEntry) size() int {
	// The attribute lastIter although part of the struct it is not metadata.
	return len(m.help) + len(m.unit) + len(m.typ)
}

func newScrapeCache() *scrapeCache {
	return &scrapeCache{
		series:        map[string]*cacheEntry{},
		droppedSeries: map[string]*uint64{},
		seriesCur:     map[uint64]labels.Labels{},
		seriesPrev:    map[uint64]labels.Labels{},
		metadata:      map[string]*metaEntry{},
	}
}

func (c *scrapeCache) iterDone(flushCache bool) {
	c.metaMtx.Lock()
	count := len(c.series) + len(c.droppedSeries) + len(c.metadata)
	c.metaMtx.Unlock()

	if flushCache {
		c.successfulCount = count
	} else if count > c.successfulCount*2+1000 {
		// If a target had varying labels in scrapes that ultimately failed,
		// the caches would grow indefinitely. Force a flush when this happens.
		// We use the heuristic that this is a doubling of the cache size
		// since the last scrape, and allow an additional 1000 in case
		// initial scrapes all fail.
		flushCache = true
		targetScrapeCacheFlushForced.Inc()
	}

	if flushCache {
		// All caches may grow over time through series churn
		// or multiple string representations of the same metric. Clean up entries
		// that haven't appeared in the last scrape.
		for s, e := range c.series {
			if c.iter != e.lastIter {
				delete(c.series, s)
			}
		}
		for s, iter := range c.droppedSeries {
			if c.iter != *iter {
				delete(c.droppedSeries, s)
			}
		}
		c.metaMtx.Lock()
		for m, e := range c.metadata {
			// Keep metadata around for 10 scrapes after its metric disappeared.
			// 保留最近十个版本的metadata
			if c.iter-e.lastIter > 10 {
				delete(c.metadata, m)
			}
		}
		c.metaMtx.Unlock()

		//迭代版本号自增
		c.iter++
	}

	// Swap current and previous series.
	// 把上次采集的指标(metircs)集合和本次采集的指标集(metrics)互换
	c.seriesPrev, c.seriesCur = c.seriesCur, c.seriesPrev

	// We have to delete every single key in the map.
	for k := range c.seriesCur {
		// 删除本地获取的指标集(metrics)
		delete(c.seriesCur, k)
	}
}

//根据指标(metrics)信息获取结构体cacheEntry
func (c *scrapeCache) get(met string) (*cacheEntry, bool) {
	//series是map类型,key是metric,value是结构体cachaEntry
	e, ok := c.series[met]
	if !ok {
		return nil, false
	}
	e.lastIter = c.iter
	return e, true
}

//根据指标(metircs)信息添加该meitric的结构体cacheEntry
func (c *scrapeCache) addRef(met string, ref uint64, lset labels.Labels, hash uint64) {
	if ref == 0 {
		return
	}
	// series是map类型, key为metric, value是结构体cacheEntry
	c.series[met] = &cacheEntry{ref: ref, lastIter: c.iter, lset: lset, hash: hash}
}

//添加无效指标(metrics)到map类型droppedSeries
func (c *scrapeCache) addDropped(met string) {
	iter := c.iter
	// droppedSeries是map类型,以metric作为key, 版本作为value
	c.droppedSeries[met] = &iter
}

//判断指标(metrics)的合法性
func (c *scrapeCache) getDropped(met string) bool {
	// 判断metric是否在非法的dropperSeries的map类型里, key是metric, value是迭代版本号
	iterp, ok := c.droppedSeries[met]
	if ok {
		*iterp = c.iter
	}
	return ok
}

//添加不带时间戳的指标(metrics)到map类型seriesCur,以metric lset的hash值作为唯一标识
func (c *scrapeCache) trackStaleness(hash uint64, lset labels.Labels) {
	c.seriesCur[hash] = lset
}

// 比较两个map：seriesCur和seriesPrev，查找过期指标
func (c *scrapeCache) forEachStale(f func(labels.Labels) bool) {
	for h, lset := range c.seriesPrev {
		// 判断之前的metric是否在当前的seriesCur里.
		if _, ok := c.seriesCur[h]; !ok {
			if !f(lset) {
				break
			}
		}
	}
}

func (c *scrapeCache) setType(metric []byte, t textparse.MetricType) {
	c.metaMtx.Lock()

	e, ok := c.metadata[yoloString(metric)]
	if !ok {
		e = &metaEntry{typ: textparse.MetricTypeUnknown}
		c.metadata[string(metric)] = e
	}
	e.typ = t
	e.lastIter = c.iter

	c.metaMtx.Unlock()
}

func (c *scrapeCache) setHelp(metric, help []byte) {
	c.metaMtx.Lock()

	e, ok := c.metadata[yoloString(metric)]
	if !ok {
		e = &metaEntry{typ: textparse.MetricTypeUnknown}
		c.metadata[string(metric)] = e
	}
	if e.help != yoloString(help) {
		e.help = string(help)
	}
	e.lastIter = c.iter

	c.metaMtx.Unlock()
}

func (c *scrapeCache) setUnit(metric, unit []byte) {
	c.metaMtx.Lock()

	e, ok := c.metadata[yoloString(metric)]
	if !ok {
		e = &metaEntry{typ: textparse.MetricTypeUnknown}
		c.metadata[string(metric)] = e
	}
	if e.unit != yoloString(unit) {
		e.unit = string(unit)
	}
	e.lastIter = c.iter

	c.metaMtx.Unlock()
}

func (c *scrapeCache) GetMetadata(metric string) (MetricMetadata, bool) {
	c.metaMtx.Lock()
	defer c.metaMtx.Unlock()

	m, ok := c.metadata[metric]
	if !ok {
		return MetricMetadata{}, false
	}
	return MetricMetadata{
		Metric: metric,
		Type:   m.typ,
		Help:   m.help,
		Unit:   m.unit,
	}, true
}

func (c *scrapeCache) ListMetadata() []MetricMetadata {
	c.metaMtx.Lock()
	defer c.metaMtx.Unlock()

	res := make([]MetricMetadata, 0, len(c.metadata))

	for m, e := range c.metadata {
		res = append(res, MetricMetadata{
			Metric: m,
			Type:   e.typ,
			Help:   e.help,
			Unit:   e.unit,
		})
	}
	return res
}

// MetadataSize returns the size of the metadata cache.
func (c *scrapeCache) SizeMetadata() (s int) {
	c.metaMtx.Lock()
	defer c.metaMtx.Unlock()
	for _, e := range c.metadata {
		s += e.size()
	}

	return s
}

// MetadataLen returns the number of metadata entries in the cache.
func (c *scrapeCache) LengthMetadata() int {
	c.metaMtx.Lock()
	defer c.metaMtx.Unlock()

	return len(c.metadata)
}

func newScrapeLoop(ctx context.Context,
	sc scraper,
	l log.Logger,
	buffers *pool.Pool,
	sampleMutator labelsMutator,
	reportSampleMutator labelsMutator,
	appender func(ctx context.Context) storage.Appender,
	cache *scrapeCache,
	jitterSeed uint64,
	honorTimestamps bool,
	labelLimits *labelLimits,
) *scrapeLoop {
	if l == nil {
		l = log.NewNopLogger()
	}
	if buffers == nil {
		buffers = pool.New(1e3, 1e6, 3, func(sz int) interface{} { return make([]byte, 0, sz) })
	}
	if cache == nil {
		cache = newScrapeCache()
	}
	sl := &scrapeLoop{
		scraper:             sc,
		buffers:             buffers,
		cache:               cache,
		appender:            appender,
		sampleMutator:       sampleMutator,
		reportSampleMutator: reportSampleMutator,
		stopped:             make(chan struct{}),
		jitterSeed:          jitterSeed,
		l:                   l,
		parentCtx:           ctx,
		honorTimestamps:     honorTimestamps,
		labelLimits:         labelLimits,
	}
	sl.ctx, sl.cancel = context.WithCancel(ctx)

	return sl
}

/**
sp.sync方法起了一个协程运行scrapePool的run方法去采集并存储监控指标(metrics)，run方法实现如下：
 */
func (sl *scrapeLoop) run(interval, timeout time.Duration, errc chan<- error) {
	select {
	//检测超时
	case <-time.After(sl.scraper.offset(interval, sl.jitterSeed)):
		// Continue after a scraping offset.
		//停止，退出
	case <-sl.ctx.Done():
		close(sl.stopped)
		return
	}

	var last time.Time

	alignedScrapeTime := time.Now().Round(0)
	// 根据interval设置定时器
	//设置定时器
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

mainLoop:
	for {
		select {
		case <-sl.parentCtx.Done():
			close(sl.stopped)
			return
		case <-sl.ctx.Done():
			break mainLoop
		default:
		}

		// Temporary workaround for a jitter in go timers that causes disk space
		// increase in TSDB.
		// See https://github.com/prometheus/prometheus/issues/7846
		// Calling Round ensures the time used is the wall clock, as otherwise .Sub
		// and .Add on time.Time behave differently (see time package docs).
		scrapeTime := time.Now().Round(0)
		if AlignScrapeTimestamps && interval > 100*scrapeTimestampTolerance {
			// For some reason, a tick might have been skipped, in which case we
			// would call alignedScrapeTime.Add(interval) multiple times.
			for scrapeTime.Sub(alignedScrapeTime) >= interval {
				alignedScrapeTime = alignedScrapeTime.Add(interval)
			}
			// Align the scrape time if we are in the tolerance boundaries.
			if scrapeTime.Sub(alignedScrapeTime) <= scrapeTimestampTolerance {
				scrapeTime = alignedScrapeTime
			}
		}

		/**
		scrapeAndReport方法主要实现两个功能：指标采集(scrape)和指标存储．此外，为了实现对象的复用，在采集(scrape)过程中，使用了sync.Pool机制提高性能，
		即每次采集(scrape)完成后，都会申请和本次采集(scrape)指标存储空间一样的大小的bytes，加入到buffer中，以备下次指标采集(scrape)直接使用．
		 */
		last = sl.scrapeAndReport(interval, timeout, last, scrapeTime, errc)

		select {
		case <-sl.parentCtx.Done():
			close(sl.stopped)
			return
		case <-sl.ctx.Done():
			break mainLoop
		case <-ticker.C:
		}
	}

	close(sl.stopped)

	if !sl.disabledEndOfRunStalenessMarkers {
		sl.endOfRunStaleness(last, ticker, interval)
	}
}

// scrapeAndReport performs a scrape and then appends the result to the storage
// together with reporting metrics, by using as few appenders as possible.
// In the happy scenario, a single appender is used.
// This function uses sl.parentCtx instead of sl.ctx on purpose. A scrape should
// only be cancelled on shutdown, not on reloads.
func (sl *scrapeLoop) scrapeAndReport(interval, timeout time.Duration, last, appendTime time.Time, errc chan<- error) time.Time {
	start := time.Now()

	// 记录第一次
	// Only record after the first scrape.
	if !last.IsZero() {
		targetIntervalLength.WithLabelValues(interval.String()).Observe(
			time.Since(last).Seconds(),
		)
	}
	// 根据上次拉取数据的大小，设置buffer空间
	// 获取上次scrape(拉取)指标(metric)占用空间
	b := sl.buffers.Get(sl.lastScrapeSize).([]byte)
	// 对象复用
	defer sl.buffers.Put(b)
	//根据上次的占用的空间申请存储空间
	buf := bytes.NewBuffer(b)

	var total, added, seriesAdded int
	var err, appErr, scrapeErr error

	app := sl.appender(sl.parentCtx)
	defer func() {
		if err != nil {
			app.Rollback()
			return
		}
		err = app.Commit()
		if err != nil {
			level.Error(sl.l).Log("msg", "Scrape commit failed", "err", err)
		}
	}()

	defer func() {
		// 上报指标，进行统计
		if err = sl.report(app, appendTime, time.Since(start), total, added, seriesAdded, scrapeErr); err != nil {
			level.Warn(sl.l).Log("msg", "Appending scrape report failed", "err", err)
		}
	}()

	if forcedErr := sl.getForcedError(); forcedErr != nil {
		scrapeErr = forcedErr
		// Add stale markers.
		if _, _, _, err := sl.append(app, []byte{}, "", appendTime); err != nil {
			app.Rollback()
			app = sl.appender(sl.parentCtx)
			level.Warn(sl.l).Log("msg", "Append failed", "err", err)
		}
		if errc != nil {
			errc <- forcedErr
		}

		return start
	}

	var contentType string
	scrapeCtx, cancel := context.WithTimeout(sl.parentCtx, timeout)
	// 读取数据，设置到buffer中
	//开始scrape(拉取)指标
	contentType, scrapeErr = sl.scraper.scrape(scrapeCtx, buf)
	// 取消，结束scrape
	cancel()

	if scrapeErr == nil {
		b = buf.Bytes()
		// NOTE: There were issues with misbehaving clients in the past
		// that occasionally returned empty results. We don't want those
		// to falsely reset our buffer size.
		if len(b) > 0 {
			// 记录本次Scrape大小
			//存储本次scrape拉取磁盘占用的空间，留待下次scrape(拉取)使用
			sl.lastScrapeSize = len(b)
		}
	} else {
		// 错误处理
		level.Debug(sl.l).Log("msg", "Scrape failed", "err", scrapeErr)
		if errc != nil {
			errc <- scrapeErr
		}
	}

	// A failed scrape is the same as an empty scrape,
	// we still call sl.append to trigger stale markers.
	// 生成数据，存储指标
	//存储指标
	total, added, seriesAdded, appErr = sl.append(app, b, contentType, appendTime)
	if appErr != nil {
		app.Rollback()
		app = sl.appender(sl.parentCtx)
		level.Debug(sl.l).Log("msg", "Append failed", "err", appErr)
		// The append failed, probably due to a parse error or sample limit.
		// Call sl.append again with an empty scrape to trigger stale markers.
		if _, _, _, err := sl.append(app, []byte{}, "", appendTime); err != nil {
			app.Rollback()
			app = sl.appender(sl.parentCtx)
			level.Warn(sl.l).Log("msg", "Append failed", "err", err)
		}
	}

	if scrapeErr == nil {
		scrapeErr = appErr
	}

	return start
}

func (sl *scrapeLoop) setForcedError(err error) {
	sl.forcedErrMtx.Lock()
	defer sl.forcedErrMtx.Unlock()
	sl.forcedErr = err
}

func (sl *scrapeLoop) getForcedError() error {
	sl.forcedErrMtx.Lock()
	defer sl.forcedErrMtx.Unlock()
	return sl.forcedErr
}

func (sl *scrapeLoop) endOfRunStaleness(last time.Time, ticker *time.Ticker, interval time.Duration) {
	// Scraping has stopped. We want to write stale markers but
	// the target may be recreated, so we wait just over 2 scrape intervals
	// before creating them.
	// If the context is canceled, we presume the server is shutting down
	// and will restart where is was. We do not attempt to write stale markers
	// in this case.

	if last.IsZero() {
		// There never was a scrape, so there will be no stale markers.
		return
	}

	// Wait for when the next scrape would have been, record its timestamp.
	var staleTime time.Time
	select {
	case <-sl.parentCtx.Done():
		return
	case <-ticker.C:
		staleTime = time.Now()
	}

	// Wait for when the next scrape would have been, if the target was recreated
	// samples should have been ingested by now.
	select {
	case <-sl.parentCtx.Done():
		return
	case <-ticker.C:
	}

	// Wait for an extra 10% of the interval, just to be safe.
	select {
	case <-sl.parentCtx.Done():
		return
	case <-time.After(interval / 10):
	}

	// Call sl.append again with an empty scrape to trigger stale markers.
	// If the target has since been recreated and scraped, the
	// stale markers will be out of order and ignored.
	app := sl.appender(sl.ctx)
	var err error
	defer func() {
		if err != nil {
			app.Rollback()
			return
		}
		err = app.Commit()
		if err != nil {
			level.Warn(sl.l).Log("msg", "Stale commit failed", "err", err)
		}
	}()
	if _, _, _, err = sl.append(app, []byte{}, "", staleTime); err != nil {
		app.Rollback()
		app = sl.appender(sl.ctx)
		level.Warn(sl.l).Log("msg", "Stale append failed", "err", err)
	}
	if err = sl.reportStale(app, staleTime); err != nil {
		level.Warn(sl.l).Log("msg", "Stale report failed", "err", err)
	}
}

// Stop the scraping. May still write data and stale markers after it has
// returned. Cancel the context to stop all writes.
func (sl *scrapeLoop) stop() {
	sl.cancel()
	<-sl.stopped
}

func (sl *scrapeLoop) disableEndOfRunStalenessMarkers() {
	sl.disabledEndOfRunStalenessMarkers = true
}

func (sl *scrapeLoop) getCache() *scrapeCache {
	return sl.cache
}

type appendErrors struct {
	numOutOfOrder         int
	numDuplicates         int
	numOutOfBounds        int
	numExemplarOutOfOrder int
}

func (sl *scrapeLoop) append(app storage.Appender, b []byte, contentType string, ts time.Time) (total, added, seriesAdded int, err error) {
	var (
		p              = textparse.New(b, contentType)
		defTime        = timestamp.FromTime(ts)
		appErrs        = appendErrors{}
		sampleLimitErr error
	)

	defer func() {
		if err != nil {
			return
		}
		// Only perform cache cleaning if the scrape was not empty.
		// An empty scrape (usually) is used to indicate a failed scrape.
		sl.cache.iterDone(len(b) > 0)
	}()

loop:
	for {
		var (
			et          textparse.Entry
			sampleAdded bool
			e           exemplar.Exemplar
		)
		if et, err = p.Next(); err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
		switch et {
		case textparse.EntryType:
			sl.cache.setType(p.Type())
			continue
		case textparse.EntryHelp:
			sl.cache.setHelp(p.Help())
			continue
		case textparse.EntryUnit:
			sl.cache.setUnit(p.Unit())
			continue
		case textparse.EntryComment:
			continue
		default:
		}
		total++

		t := defTime
		met, tp, v := p.Series()
		if !sl.honorTimestamps {
			tp = nil
		}
		if tp != nil {
			t = *tp
		}

		if sl.cache.getDropped(yoloString(met)) {
			continue
		}
		ce, ok := sl.cache.get(yoloString(met))
		var (
			ref  uint64
			lset labels.Labels
			mets string
			hash uint64
		)

		if ok {
			ref = ce.ref
			lset = ce.lset
		} else {
			mets = p.Metric(&lset)
			hash = lset.Hash()

			// Hash label set as it is seen local to the target. Then add target labels
			// and relabeling and store the final label set.
			lset = sl.sampleMutator(lset)

			// The label set may be set to nil to indicate dropping.
			if lset == nil {
				sl.cache.addDropped(mets)
				continue
			}

			if !lset.Has(labels.MetricName) {
				err = errNameLabelMandatory
				break loop
			}

			// If any label limits is exceeded the scrape should fail.
			if err = verifyLabelLimits(lset, sl.labelLimits); err != nil {
				targetScrapePoolExceededLabelLimits.Inc()
				break loop
			}
		}

		ref, err = app.Append(ref, lset, t, v)
		sampleAdded, err = sl.checkAddError(ce, met, tp, err, &sampleLimitErr, &appErrs)
		if err != nil {
			if err != storage.ErrNotFound {
				level.Debug(sl.l).Log("msg", "Unexpected error", "series", string(met), "err", err)
			}
			break loop
		}

		if !ok {
			if tp == nil {
				// Bypass staleness logic if there is an explicit timestamp.
				sl.cache.trackStaleness(hash, lset)
			}
			sl.cache.addRef(mets, ref, lset, hash)
			if sampleAdded && sampleLimitErr == nil {
				seriesAdded++
			}
		}

		// Increment added even if there's an error so we correctly report the
		// number of samples remaining after relabeling.
		added++

		if hasExemplar := p.Exemplar(&e); hasExemplar {
			if !e.HasTs {
				e.Ts = t
			}
			_, exemplarErr := app.AppendExemplar(ref, lset, e)
			exemplarErr = sl.checkAddExemplarError(exemplarErr, e, &appErrs)
			if exemplarErr != nil {
				// Since exemplar storage is still experimental, we don't fail the scrape on ingestion errors.
				level.Debug(sl.l).Log("msg", "Error while adding exemplar in AddExemplar", "exemplar", fmt.Sprintf("%+v", e), "err", exemplarErr)
			}
		}

	}
	if sampleLimitErr != nil {
		if err == nil {
			err = sampleLimitErr
		}
		// We only want to increment this once per scrape, so this is Inc'd outside the loop.
		targetScrapeSampleLimit.Inc()
	}
	if appErrs.numOutOfOrder > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting out-of-order samples", "num_dropped", appErrs.numOutOfOrder)
	}
	if appErrs.numDuplicates > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting samples with different value but same timestamp", "num_dropped", appErrs.numDuplicates)
	}
	if appErrs.numOutOfBounds > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting samples that are too old or are too far into the future", "num_dropped", appErrs.numOutOfBounds)
	}
	if appErrs.numExemplarOutOfOrder > 0 {
		level.Warn(sl.l).Log("msg", "Error on ingesting out-of-order exemplars", "num_dropped", appErrs.numExemplarOutOfOrder)
	}
	if err == nil {
		sl.cache.forEachStale(func(lset labels.Labels) bool {
			// Series no longer exposed, mark it stale.
			_, err = app.Append(0, lset, defTime, math.Float64frombits(value.StaleNaN))
			switch errors.Cause(err) {
			case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
				// Do not count these in logging, as this is expected if a target
				// goes away and comes back again with a new scrape loop.
				err = nil
			}
			return err == nil
		})
	}
	return
}

func yoloString(b []byte) string {
	return *((*string)(unsafe.Pointer(&b)))
}

// Adds samples to the appender, checking the error, and then returns the # of samples added,
// whether the caller should continue to process more samples, and any sample limit errors.
func (sl *scrapeLoop) checkAddError(ce *cacheEntry, met []byte, tp *int64, err error, sampleLimitErr *error, appErrs *appendErrors) (bool, error) {
	switch errors.Cause(err) {
	case nil:
		if tp == nil && ce != nil {
			sl.cache.trackStaleness(ce.hash, ce.lset)
		}
		return true, nil
	case storage.ErrNotFound:
		return false, storage.ErrNotFound
	case storage.ErrOutOfOrderSample:
		appErrs.numOutOfOrder++
		level.Debug(sl.l).Log("msg", "Out of order sample", "series", string(met))
		targetScrapeSampleOutOfOrder.Inc()
		return false, nil
	case storage.ErrDuplicateSampleForTimestamp:
		appErrs.numDuplicates++
		level.Debug(sl.l).Log("msg", "Duplicate sample for timestamp", "series", string(met))
		targetScrapeSampleDuplicate.Inc()
		return false, nil
	case storage.ErrOutOfBounds:
		appErrs.numOutOfBounds++
		level.Debug(sl.l).Log("msg", "Out of bounds metric", "series", string(met))
		targetScrapeSampleOutOfBounds.Inc()
		return false, nil
	case errSampleLimit:
		// Keep on parsing output if we hit the limit, so we report the correct
		// total number of samples scraped.
		*sampleLimitErr = err
		return false, nil
	default:
		return false, err
	}
}

func (sl *scrapeLoop) checkAddExemplarError(err error, e exemplar.Exemplar, appErrs *appendErrors) error {
	switch errors.Cause(err) {
	case storage.ErrNotFound:
		return storage.ErrNotFound
	case storage.ErrOutOfOrderExemplar:
		appErrs.numExemplarOutOfOrder++
		level.Debug(sl.l).Log("msg", "Out of order exemplar", "exemplar", fmt.Sprintf("%+v", e))
		targetScrapeExemplarOutOfOrder.Inc()
		return nil
	default:
		return err
	}
}

// The constants are suffixed with the invalid \xff unicode rune to avoid collisions
// with scraped metrics in the cache.
const (
	scrapeHealthMetricName       = "up" + "\xff"
	scrapeDurationMetricName     = "scrape_duration_seconds" + "\xff"
	scrapeSamplesMetricName      = "scrape_samples_scraped" + "\xff"
	samplesPostRelabelMetricName = "scrape_samples_post_metric_relabeling" + "\xff"
	scrapeSeriesAddedMetricName  = "scrape_series_added" + "\xff"
)

func (sl *scrapeLoop) report(app storage.Appender, start time.Time, duration time.Duration, scraped, added, seriesAdded int, scrapeErr error) (err error) {
	sl.scraper.Report(start, duration, scrapeErr)

	ts := timestamp.FromTime(start)

	var health float64
	if scrapeErr == nil {
		health = 1
	}

	if err = sl.addReportSample(app, scrapeHealthMetricName, ts, health); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeDurationMetricName, ts, duration.Seconds()); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeSamplesMetricName, ts, float64(scraped)); err != nil {
		return
	}
	if err = sl.addReportSample(app, samplesPostRelabelMetricName, ts, float64(added)); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeSeriesAddedMetricName, ts, float64(seriesAdded)); err != nil {
		return
	}
	return
}

func (sl *scrapeLoop) reportStale(app storage.Appender, start time.Time) (err error) {
	ts := timestamp.FromTime(start)

	stale := math.Float64frombits(value.StaleNaN)

	if err = sl.addReportSample(app, scrapeHealthMetricName, ts, stale); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeDurationMetricName, ts, stale); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeSamplesMetricName, ts, stale); err != nil {
		return
	}
	if err = sl.addReportSample(app, samplesPostRelabelMetricName, ts, stale); err != nil {
		return
	}
	if err = sl.addReportSample(app, scrapeSeriesAddedMetricName, ts, stale); err != nil {
		return
	}
	return
}

func (sl *scrapeLoop) addReportSample(app storage.Appender, s string, t int64, v float64) error {
	ce, ok := sl.cache.get(s)
	var ref uint64
	var lset labels.Labels
	if ok {
		ref = ce.ref
		lset = ce.lset
	} else {
		lset = labels.Labels{
			// The constants are suffixed with the invalid \xff unicode rune to avoid collisions
			// with scraped metrics in the cache.
			// We have to drop it when building the actual metric.
			labels.Label{Name: labels.MetricName, Value: s[:len(s)-1]},
		}
		lset = sl.reportSampleMutator(lset)
	}

	ref, err := app.Append(ref, lset, t, v)
	switch errors.Cause(err) {
	case nil:
		if !ok {
			sl.cache.addRef(s, ref, lset, lset.Hash())
		}
		return nil
	case storage.ErrOutOfOrderSample, storage.ErrDuplicateSampleForTimestamp:
		// Do not log here, as this is expected if a target goes away and comes back
		// again with a new scrape loop.
		return nil
	default:
		return err
	}
}

// zeroConfig returns a new scrape config that only contains configuration items
// that alter metrics.
func zeroConfig(c *config.ScrapeConfig) *config.ScrapeConfig {
	z := *c
	// We zero out the fields that for sure don't affect scrape.
	z.ScrapeInterval = 0
	z.ScrapeTimeout = 0
	z.SampleLimit = 0
	z.LabelLimit = 0
	z.LabelNameLengthLimit = 0
	z.LabelValueLengthLimit = 0
	z.HTTPClientConfig = config_util.HTTPClientConfig{}
	return &z
}

// reusableCache compares two scrape config and tells whether the cache is still
// valid.
func reusableCache(r, l *config.ScrapeConfig) bool {
	if r == nil || l == nil {
		return false
	}
	return reflect.DeepEqual(zeroConfig(r), zeroConfig(l))
}
