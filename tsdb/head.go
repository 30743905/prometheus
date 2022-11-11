// Copyright 2017 The Prometheus Authors
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

package tsdb

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/oklog/ulid"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/atomic"

	"github.com/prometheus/prometheus/pkg/exemplar"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/prometheus/prometheus/tsdb/record"
	"github.com/prometheus/prometheus/tsdb/tombstones"
	"github.com/prometheus/prometheus/tsdb/tsdbutil"
	"github.com/prometheus/prometheus/tsdb/wal"
)

var (
	// ErrInvalidSample is returned if an appended sample is not valid and can't
	// be ingested.
	ErrInvalidSample = errors.New("invalid sample")
	// ErrInvalidExemplar is returned if an appended exemplar is not valid and can't
	// be ingested.
	ErrInvalidExemplar = errors.New("invalid exemplar")
	// ErrAppenderClosed is returned if an appender has already be successfully
	// rolled back or committed.
	ErrAppenderClosed = errors.New("appender closed")
)

type ExemplarStorage interface {
	storage.ExemplarQueryable
	AddExemplar(labels.Labels, exemplar.Exemplar) error
	ValidateExemplar(labels.Labels, exemplar.Exemplar) error
}

// Head handles reads and writes of time series data within a time window.
type Head struct {
	chunkRange atomic.Int64
	numSeries  atomic.Uint64
	// minTime: 已经写入的数据点的最小时间戳；
	// maxTime：已经写入的数据点的最大时间戳；
	minTime, maxTime atomic.Int64 // Current min and max of the samples included in the head.
	// minValidTime: 合法最小时间戳, 写入数据点的时间戳小于这个时间戳，Prometheus将直接丢弃，因为promtheus使用的拉模式采集的指标，时间戳是由exporter生成的，一般都在当前写入窗口期内
	minValidTime          atomic.Int64 // Mint allowed to be added to the head. It shouldn't be lower than the maxt of the last persisted block.
	lastWALTruncationTime atomic.Int64
	lastSeriesID          atomic.Uint64

	metrics       *headMetrics
	opts          *HeadOptions
	wal           *wal.WAL
	exemplars     ExemplarStorage
	logger        log.Logger
	appendPool    sync.Pool
	exemplarsPool sync.Pool
	seriesPool    sync.Pool
	bytesPool     sync.Pool
	memChunkPool  sync.Pool

	// All series addressable by their ID or hash.
	series *stripeSeries

	symMtx  sync.RWMutex
	symbols map[string]struct{}

	deletedMtx sync.Mutex
	deleted    map[uint64]int // Deleted series, and what WAL segment they must be kept until.

	postings *index.MemPostings // Postings lists for terms.

	tombstones *tombstones.MemTombstones

	iso *isolation

	cardinalityMutex      sync.Mutex
	cardinalityCache      *index.PostingsStats // Posting stats cache which will expire after 30sec.
	lastPostingsStatsCall time.Duration        // Last posting stats call (PostingsCardinalityStats()) time for caching.

	// chunkDiskMapper is used to write and read Head chunks to/from disk.
	chunkDiskMapper *chunks.ChunkDiskMapper

	closedMtx sync.Mutex
	closed    bool
}

// HeadOptions are parameters for the Head block.
type HeadOptions struct {
	ChunkRange int64
	// ChunkDirRoot is the parent directory of the chunks directory.
	ChunkDirRoot         string
	ChunkPool            chunkenc.Pool
	ChunkWriteBufferSize int
	// StripeSize sets the number of entries in the hash map, it must be a power of 2.
	// A larger StripeSize will allocate more memory up-front, but will increase performance when handling a large number of series.
	// A smaller StripeSize reduces the memory allocated, but can decrease performance with large number of series.
	StripeSize     int
	SeriesCallback SeriesLifecycleCallback
	NumExemplars   int
}

func DefaultHeadOptions() *HeadOptions {
	return &HeadOptions{
		ChunkRange:           DefaultBlockDuration,
		ChunkDirRoot:         "",
		ChunkPool:            chunkenc.NewPool(),
		ChunkWriteBufferSize: chunks.DefaultWriteBufferSize,
		StripeSize:           DefaultStripeSize,
		SeriesCallback:       &noopSeriesLifecycleCallback{},
	}
}

type headMetrics struct {
	activeAppenders          prometheus.Gauge
	series                   prometheus.GaugeFunc
	seriesCreated            prometheus.Counter
	seriesRemoved            prometheus.Counter
	seriesNotFound           prometheus.Counter
	chunks                   prometheus.Gauge
	chunksCreated            prometheus.Counter
	chunksRemoved            prometheus.Counter
	gcDuration               prometheus.Summary
	samplesAppended          prometheus.Counter
	outOfBoundSamples        prometheus.Counter
	outOfOrderSamples        prometheus.Counter
	walTruncateDuration      prometheus.Summary
	walCorruptionsTotal      prometheus.Counter
	walTotalReplayDuration   prometheus.Gauge
	headTruncateFail         prometheus.Counter
	headTruncateTotal        prometheus.Counter
	checkpointDeleteFail     prometheus.Counter
	checkpointDeleteTotal    prometheus.Counter
	checkpointCreationFail   prometheus.Counter
	checkpointCreationTotal  prometheus.Counter
	mmapChunkCorruptionTotal prometheus.Counter
}

func newHeadMetrics(h *Head, r prometheus.Registerer) *headMetrics {
	m := &headMetrics{
		activeAppenders: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_head_active_appenders",
			Help: "Number of currently active appender transactions",
		}),
		series: prometheus.NewGaugeFunc(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_head_series",
			Help: "Total number of series in the head block.",
		}, func() float64 {
			return float64(h.NumSeries())
		}),
		seriesCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_created_total",
			Help: "Total number of series created in the head",
		}),
		seriesRemoved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_removed_total",
			Help: "Total number of series removed in the head",
		}),
		seriesNotFound: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_series_not_found_total",
			Help: "Total number of requests for series that were not found.",
		}),
		chunks: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_head_chunks",
			Help: "Total number of chunks in the head block.",
		}),
		chunksCreated: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_chunks_created_total",
			Help: "Total number of chunks created in the head",
		}),
		chunksRemoved: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_chunks_removed_total",
			Help: "Total number of chunks removed in the head",
		}),
		gcDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Name: "prometheus_tsdb_head_gc_duration_seconds",
			Help: "Runtime of garbage collection in the head block.",
		}),
		walTruncateDuration: prometheus.NewSummary(prometheus.SummaryOpts{
			Name: "prometheus_tsdb_wal_truncate_duration_seconds",
			Help: "Duration of WAL truncation.",
		}),
		walCorruptionsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_wal_corruptions_total",
			Help: "Total number of WAL corruptions.",
		}),
		walTotalReplayDuration: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "prometheus_tsdb_data_replay_duration_seconds",
			Help: "Time taken to replay the data on disk.",
		}),
		samplesAppended: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_samples_appended_total",
			Help: "Total number of appended samples.",
		}),
		outOfBoundSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_out_of_bound_samples_total",
			Help: "Total number of out of bound samples ingestion failed attempts.",
		}),
		outOfOrderSamples: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_out_of_order_samples_total",
			Help: "Total number of out of order samples ingestion failed attempts.",
		}),
		headTruncateFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_truncations_failed_total",
			Help: "Total number of head truncations that failed.",
		}),
		headTruncateTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_head_truncations_total",
			Help: "Total number of head truncations attempted.",
		}),
		checkpointDeleteFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_deletions_failed_total",
			Help: "Total number of checkpoint deletions that failed.",
		}),
		checkpointDeleteTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_deletions_total",
			Help: "Total number of checkpoint deletions attempted.",
		}),
		checkpointCreationFail: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_creations_failed_total",
			Help: "Total number of checkpoint creations that failed.",
		}),
		checkpointCreationTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_checkpoint_creations_total",
			Help: "Total number of checkpoint creations attempted.",
		}),
		mmapChunkCorruptionTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "prometheus_tsdb_mmap_chunk_corruptions_total",
			Help: "Total number of memory-mapped chunk corruptions.",
		}),
	}

	if r != nil {
		r.MustRegister(
			m.activeAppenders,
			m.series,
			m.chunks,
			m.chunksCreated,
			m.chunksRemoved,
			m.seriesCreated,
			m.seriesRemoved,
			m.seriesNotFound,
			m.gcDuration,
			m.walTruncateDuration,
			m.walCorruptionsTotal,
			m.walTotalReplayDuration,
			m.samplesAppended,
			m.outOfBoundSamples,
			m.outOfOrderSamples,
			m.headTruncateFail,
			m.headTruncateTotal,
			m.checkpointDeleteFail,
			m.checkpointDeleteTotal,
			m.checkpointCreationFail,
			m.checkpointCreationTotal,
			m.mmapChunkCorruptionTotal,
			// Metrics bound to functions and not needed in tests
			// can be created and registered on the spot.
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_head_max_time",
				Help: "Maximum timestamp of the head block. The unit is decided by the library consumer.",
			}, func() float64 {
				return float64(h.MaxTime())
			}),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_head_min_time",
				Help: "Minimum time bound of the head block. The unit is decided by the library consumer.",
			}, func() float64 {
				return float64(h.MinTime())
			}),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_isolation_low_watermark",
				Help: "The lowest TSDB append ID that is still referenced.",
			}, func() float64 {
				return float64(h.iso.lowWatermark())
			}),
			prometheus.NewGaugeFunc(prometheus.GaugeOpts{
				Name: "prometheus_tsdb_isolation_high_watermark",
				Help: "The highest TSDB append ID that has been given out.",
			}, func() float64 {
				return float64(h.iso.lastAppendID())
			}),
		)
	}
	return m
}

const cardinalityCacheExpirationTime = time.Duration(30) * time.Second

// PostingsCardinalityStats returns top 10 highest cardinality stats By label and value names.
func (h *Head) PostingsCardinalityStats(statsByLabelName string) *index.PostingsStats {
	h.cardinalityMutex.Lock()
	defer h.cardinalityMutex.Unlock()
	currentTime := time.Duration(time.Now().Unix()) * time.Second
	seconds := currentTime - h.lastPostingsStatsCall
	if seconds > cardinalityCacheExpirationTime {
		h.cardinalityCache = nil
	}
	if h.cardinalityCache != nil {
		return h.cardinalityCache
	}
	h.cardinalityCache = h.postings.Stats(statsByLabelName)
	h.lastPostingsStatsCall = time.Duration(time.Now().Unix()) * time.Second

	return h.cardinalityCache
}

// NewHead opens the head block in dir.
func NewHead(r prometheus.Registerer, l log.Logger, wal *wal.WAL, opts *HeadOptions) (*Head, error) {
	if l == nil {
		l = log.NewNopLogger()
	}
	if opts.ChunkRange < 1 {
		return nil, errors.Errorf("invalid chunk range %d", opts.ChunkRange)
	}
	if opts.SeriesCallback == nil {
		opts.SeriesCallback = &noopSeriesLifecycleCallback{}
	}

	es, err := NewCircularExemplarStorage(opts.NumExemplars, r)
	if err != nil {
		return nil, err
	}

	h := &Head{
		wal:        wal,
		logger:     l,
		opts:       opts,
		exemplars:  es,
		series:     newStripeSeries(opts.StripeSize, opts.SeriesCallback),
		symbols:    map[string]struct{}{},
		postings:   index.NewUnorderedMemPostings(),
		tombstones: tombstones.NewMemTombstones(),
		iso:        newIsolation(),
		deleted:    map[uint64]int{},
		memChunkPool: sync.Pool{
			New: func() interface{} {
				return &memChunk{}
			},
		},
	}
	h.chunkRange.Store(opts.ChunkRange)
	h.minTime.Store(math.MaxInt64)
	h.maxTime.Store(math.MinInt64)
	h.lastWALTruncationTime.Store(math.MinInt64)
	h.metrics = newHeadMetrics(h, r)

	if opts.ChunkPool == nil {
		opts.ChunkPool = chunkenc.NewPool()
	}

	h.chunkDiskMapper, err = chunks.NewChunkDiskMapper(
		mmappedChunksDir(opts.ChunkDirRoot),
		opts.ChunkPool,
		opts.ChunkWriteBufferSize,
	)
	if err != nil {
		return nil, err
	}

	return h, nil
}

func mmappedChunksDir(dir string) string { return filepath.Join(dir, "chunks_head") }

func (h *Head) ExemplarQuerier(ctx context.Context) (storage.ExemplarQuerier, error) {
	return h.exemplars.ExemplarQuerier(ctx)
}

// processWALSamples adds a partition of samples it receives to the head and passes
// them on to other workers.
// Samples before the mint timestamp are discarded.
func (h *Head) processWALSamples(
	minValidTime int64,
	input <-chan []record.RefSample, output chan<- []record.RefSample,
) (unknownRefs uint64) {
	defer close(output)

	// Mitigate lock contention in getByID.
	refSeries := map[uint64]*memSeries{}

	mint, maxt := int64(math.MaxInt64), int64(math.MinInt64)

	for samples := range input {
		for _, s := range samples {
			if s.T < minValidTime {
				continue
			}
			ms := refSeries[s.Ref]
			if ms == nil {
				ms = h.series.getByID(s.Ref)
				if ms == nil {
					unknownRefs++
					continue
				}
				refSeries[s.Ref] = ms
			}
			if _, chunkCreated := ms.append(s.T, s.V, 0, h.chunkDiskMapper); chunkCreated {
				h.metrics.chunksCreated.Inc()
				h.metrics.chunks.Inc()
			}
			if s.T > maxt {
				maxt = s.T
			}
			if s.T < mint {
				mint = s.T
			}
		}
		output <- samples
	}
	h.updateMinMaxTime(mint, maxt)

	return unknownRefs
}

func (h *Head) updateMinMaxTime(mint, maxt int64) {
	for {
		lt := h.MinTime()
		if mint >= lt {
			break
		}
		if h.minTime.CAS(lt, mint) {
			break
		}
	}
	for {
		ht := h.MaxTime()
		if maxt <= ht {
			break
		}
		if h.maxTime.CAS(ht, maxt) {
			break
		}
	}
}

func (h *Head) loadWAL(r *wal.Reader, multiRef map[uint64]uint64, mmappedChunks map[uint64][]*mmappedChunk) (err error) {
	// Track number of samples that referenced a series we don't know about
	// for error reporting.
	var unknownRefs atomic.Uint64
	var unknownExemplarRefs atomic.Uint64

	// Start workers that each process samples for a partition of the series ID space.
	// They are connected through a ring of channels which ensures that all sample batches
	// read from the WAL are processed in order.
	var (
		wg             sync.WaitGroup
		n              = runtime.GOMAXPROCS(0)
		inputs         = make([]chan []record.RefSample, n)
		outputs        = make([]chan []record.RefSample, n)
		exemplarsInput chan record.RefExemplar

		dec    record.Decoder
		shards = make([][]record.RefSample, n)

		decoded                      = make(chan interface{}, 10)
		decodeErr, seriesCreationErr error
		seriesPool                   = sync.Pool{
			New: func() interface{} {
				return []record.RefSeries{}
			},
		}
		samplesPool = sync.Pool{
			New: func() interface{} {
				return []record.RefSample{}
			},
		}
		tstonesPool = sync.Pool{
			New: func() interface{} {
				return []tombstones.Stone{}
			},
		}
		exemplarsPool = sync.Pool{
			New: func() interface{} {
				return []record.RefExemplar{}
			},
		}
	)

	defer func() {
		// For CorruptionErr ensure to terminate all workers before exiting.
		_, ok := err.(*wal.CorruptionErr)
		if ok || seriesCreationErr != nil {
			for i := 0; i < n; i++ {
				close(inputs[i])
				for range outputs[i] {
				}
			}
			close(exemplarsInput)
			wg.Wait()
		}
	}()

	wg.Add(n)
	for i := 0; i < n; i++ {
		outputs[i] = make(chan []record.RefSample, 300)
		inputs[i] = make(chan []record.RefSample, 300)

		go func(input <-chan []record.RefSample, output chan<- []record.RefSample) {
			unknown := h.processWALSamples(h.minValidTime.Load(), input, output)
			unknownRefs.Add(unknown)
			wg.Done()
		}(inputs[i], outputs[i])
	}

	wg.Add(1)
	exemplarsInput = make(chan record.RefExemplar, 300)
	go func(input <-chan record.RefExemplar) {
		defer wg.Done()
		for e := range input {
			ms := h.series.getByID(e.Ref)
			if ms == nil {
				unknownExemplarRefs.Inc()
				continue
			}

			if e.T < h.minValidTime.Load() {
				continue
			}
			// At the moment the only possible error here is out of order exemplars, which we shouldn't see when
			// replaying the WAL, so lets just log the error if it's not that type.
			err = h.exemplars.AddExemplar(ms.lset, exemplar.Exemplar{Ts: e.T, Value: e.V, Labels: e.Labels})
			if err != nil && err == storage.ErrOutOfOrderExemplar {
				level.Warn(h.logger).Log("msg", "Unexpected error when replaying WAL on exemplar record", "err", err)
			}
		}
	}(exemplarsInput)

	go func() {
		defer close(decoded)
		for r.Next() {
			rec := r.Record()
			switch dec.Type(rec) {
			case record.Series:
				series := seriesPool.Get().([]record.RefSeries)[:0]
				series, err = dec.Series(rec, series)
				if err != nil {
					decodeErr = &wal.CorruptionErr{
						Err:     errors.Wrap(err, "decode series"),
						Segment: r.Segment(),
						Offset:  r.Offset(),
					}
					return
				}
				decoded <- series
			case record.Samples:
				samples := samplesPool.Get().([]record.RefSample)[:0]
				samples, err = dec.Samples(rec, samples)
				if err != nil {
					decodeErr = &wal.CorruptionErr{
						Err:     errors.Wrap(err, "decode samples"),
						Segment: r.Segment(),
						Offset:  r.Offset(),
					}
					return
				}
				decoded <- samples
			case record.Tombstones:
				tstones := tstonesPool.Get().([]tombstones.Stone)[:0]
				tstones, err = dec.Tombstones(rec, tstones)
				if err != nil {
					decodeErr = &wal.CorruptionErr{
						Err:     errors.Wrap(err, "decode tombstones"),
						Segment: r.Segment(),
						Offset:  r.Offset(),
					}
					return
				}
				decoded <- tstones
			case record.Exemplars:
				exemplars := exemplarsPool.Get().([]record.RefExemplar)[:0]
				exemplars, err = dec.Exemplars(rec, exemplars)
				if err != nil {
					decodeErr = &wal.CorruptionErr{
						Err:     errors.Wrap(err, "decode exemplars"),
						Segment: r.Segment(),
						Offset:  r.Offset(),
					}
					return
				}
				decoded <- exemplars
			default:
				// Noop.
			}
		}
	}()

Outer:
	for d := range decoded {
		switch v := d.(type) {
		case []record.RefSeries:
			for _, s := range v {
				series, created, err := h.getOrCreateWithID(s.Ref, s.Labels.Hash(), s.Labels)
				if err != nil {
					seriesCreationErr = err
					break Outer
				}

				if created {
					// If this series gets a duplicate record, we don't restore its mmapped chunks,
					// and instead restore everything from WAL records.
					series.mmappedChunks = mmappedChunks[series.ref]

					h.metrics.chunks.Add(float64(len(series.mmappedChunks)))
					h.metrics.chunksCreated.Add(float64(len(series.mmappedChunks)))

					if len(series.mmappedChunks) > 0 {
						h.updateMinMaxTime(series.minTime(), series.maxTime())
					}
				} else {
					// TODO(codesome) Discard old samples and mmapped chunks and use mmap chunks for the new series ID.

					// There's already a different ref for this series.
					multiRef[s.Ref] = series.ref
				}

				if h.lastSeriesID.Load() < s.Ref {
					h.lastSeriesID.Store(s.Ref)
				}
			}
			//nolint:staticcheck // Ignore SA6002 relax staticcheck verification.
			seriesPool.Put(v)
		case []record.RefSample:
			samples := v
			// We split up the samples into chunks of 5000 samples or less.
			// With O(300 * #cores) in-flight sample batches, large scrapes could otherwise
			// cause thousands of very large in flight buffers occupying large amounts
			// of unused memory.
			for len(samples) > 0 {
				m := 5000
				if len(samples) < m {
					m = len(samples)
				}
				for i := 0; i < n; i++ {
					var buf []record.RefSample
					select {
					case buf = <-outputs[i]:
					default:
					}
					shards[i] = buf[:0]
				}
				for _, sam := range samples[:m] {
					if r, ok := multiRef[sam.Ref]; ok {
						sam.Ref = r
					}
					mod := sam.Ref % uint64(n)
					shards[mod] = append(shards[mod], sam)
				}
				for i := 0; i < n; i++ {
					inputs[i] <- shards[i]
				}
				samples = samples[m:]
			}
			//nolint:staticcheck // Ignore SA6002 relax staticcheck verification.
			samplesPool.Put(v)
		case []tombstones.Stone:
			for _, s := range v {
				for _, itv := range s.Intervals {
					if itv.Maxt < h.minValidTime.Load() {
						continue
					}
					if m := h.series.getByID(s.Ref); m == nil {
						unknownRefs.Inc()
						continue
					}
					h.tombstones.AddInterval(s.Ref, itv)
				}
			}
			//nolint:staticcheck // Ignore SA6002 relax staticcheck verification.
			tstonesPool.Put(v)
		case []record.RefExemplar:
			for _, e := range v {
				exemplarsInput <- e
			}
			//nolint:staticcheck // Ignore SA6002 relax staticcheck verification.
			exemplarsPool.Put(v)
		default:
			panic(fmt.Errorf("unexpected decoded type: %T", d))
		}
	}

	if decodeErr != nil {
		return decodeErr
	}
	if seriesCreationErr != nil {
		// Drain the channel to unblock the goroutine.
		for range decoded {
		}
		return seriesCreationErr
	}

	// Signal termination to each worker and wait for it to close its output channel.
	for i := 0; i < n; i++ {
		close(inputs[i])
		for range outputs[i] {
		}
	}
	close(exemplarsInput)
	wg.Wait()

	if r.Err() != nil {
		return errors.Wrap(r.Err(), "read records")
	}

	if unknownRefs.Load() > 0 || unknownExemplarRefs.Load() > 0 {
		level.Warn(h.logger).Log("msg", "Unknown series references", "samples", unknownRefs.Load(), "exemplars", unknownExemplarRefs.Load())
	}
	return nil
}

// Init loads data from the write ahead log and prepares the head for writes.
// It should be called before using an appender so that it
// limits the ingested samples to the head min valid time.
func (h *Head) Init(minValidTime int64) error {
	h.minValidTime.Store(minValidTime)
	defer h.postings.EnsureOrder()
	defer h.gc() // After loading the wal remove the obsolete data from the head.

	level.Info(h.logger).Log("msg", "Replaying on-disk memory mappable chunks if any")
	start := time.Now()

	mmappedChunks, err := h.loadMmappedChunks()
	if err != nil {
		level.Error(h.logger).Log("msg", "Loading on-disk chunks failed", "err", err)
		if _, ok := errors.Cause(err).(*chunks.CorruptionErr); ok {
			h.metrics.mmapChunkCorruptionTotal.Inc()
		}
		// If this fails, data will be recovered from WAL.
		// Hence we wont lose any data (given WAL is not corrupt).
		mmappedChunks = h.removeCorruptedMmappedChunks(err)
	}

	level.Info(h.logger).Log("msg", "On-disk memory mappable chunks replay completed", "duration", time.Since(start).String())
	if h.wal == nil {
		level.Info(h.logger).Log("msg", "WAL not found")
		return nil
	}

	level.Info(h.logger).Log("msg", "Replaying WAL, this may take a while")

	checkpointReplayStart := time.Now()
	// Backfill the checkpoint first if it exists.
	dir, startFrom, err := wal.LastCheckpoint(h.wal.Dir())
	if err != nil && err != record.ErrNotFound {
		return errors.Wrap(err, "find last checkpoint")
	}
	multiRef := map[uint64]uint64{}
	if err == nil {
		sr, err := wal.NewSegmentsReader(dir)
		if err != nil {
			return errors.Wrap(err, "open checkpoint")
		}
		defer func() {
			if err := sr.Close(); err != nil {
				level.Warn(h.logger).Log("msg", "Error while closing the wal segments reader", "err", err)
			}
		}()

		// A corrupted checkpoint is a hard error for now and requires user
		// intervention. There's likely little data that can be recovered anyway.
		if err := h.loadWAL(wal.NewReader(sr), multiRef, mmappedChunks); err != nil {
			return errors.Wrap(err, "backfill checkpoint")
		}
		startFrom++
		level.Info(h.logger).Log("msg", "WAL checkpoint loaded")
	}
	checkpointReplayDuration := time.Since(checkpointReplayStart)

	walReplayStart := time.Now()
	// Find the last segment.
	_, last, err := wal.Segments(h.wal.Dir())
	if err != nil {
		return errors.Wrap(err, "finding WAL segments")
	}

	// Backfill segments from the most recent checkpoint onwards.
	for i := startFrom; i <= last; i++ {
		s, err := wal.OpenReadSegment(wal.SegmentName(h.wal.Dir(), i))
		if err != nil {
			return errors.Wrap(err, fmt.Sprintf("open WAL segment: %d", i))
		}

		sr := wal.NewSegmentBufReader(s)
		err = h.loadWAL(wal.NewReader(sr), multiRef, mmappedChunks)
		if err := sr.Close(); err != nil {
			level.Warn(h.logger).Log("msg", "Error while closing the wal segments reader", "err", err)
		}
		if err != nil {
			return err
		}
		level.Info(h.logger).Log("msg", "WAL segment loaded", "segment", i, "maxSegment", last)
	}

	walReplayDuration := time.Since(start)
	h.metrics.walTotalReplayDuration.Set(walReplayDuration.Seconds())
	level.Info(h.logger).Log(
		"msg", "WAL replay completed",
		"checkpoint_replay_duration", checkpointReplayDuration.String(),
		"wal_replay_duration", time.Since(walReplayStart).String(),
		"total_replay_duration", walReplayDuration.String(),
	)

	return nil
}

// SetMinValidTime sets the minimum timestamp the head can ingest.
func (h *Head) SetMinValidTime(minValidTime int64) {
	h.minValidTime.Store(minValidTime)
}

func (h *Head) loadMmappedChunks() (map[uint64][]*mmappedChunk, error) {
	mmappedChunks := map[uint64][]*mmappedChunk{}
	if err := h.chunkDiskMapper.IterateAllChunks(func(seriesRef, chunkRef uint64, mint, maxt int64, numSamples uint16) error {
		if maxt < h.minValidTime.Load() {
			return nil
		}

		slice := mmappedChunks[seriesRef]
		if len(slice) > 0 {
			if slice[len(slice)-1].maxTime >= mint {
				return &chunks.CorruptionErr{
					Err: errors.Errorf("out of sequence m-mapped chunk for series ref %d", seriesRef),
				}
			}
		}

		slice = append(slice, &mmappedChunk{
			ref:        chunkRef,
			minTime:    mint,
			maxTime:    maxt,
			numSamples: numSamples,
		})
		mmappedChunks[seriesRef] = slice
		return nil
	}); err != nil {
		return nil, errors.Wrap(err, "iterate on on-disk chunks")
	}
	return mmappedChunks, nil
}

// removeCorruptedMmappedChunks attempts to delete the corrupted mmapped chunks and if it fails, it clears all the previously
// loaded mmapped chunks.
func (h *Head) removeCorruptedMmappedChunks(err error) map[uint64][]*mmappedChunk {
	level.Info(h.logger).Log("msg", "Deleting mmapped chunk files")

	if err := h.chunkDiskMapper.DeleteCorrupted(err); err != nil {
		level.Info(h.logger).Log("msg", "Deletion of mmap chunk files failed, discarding chunk files completely", "err", err)
		return map[uint64][]*mmappedChunk{}
	}

	level.Info(h.logger).Log("msg", "Deletion of mmap chunk files successful, reattempting m-mapping the on-disk chunks")
	mmappedChunks, err := h.loadMmappedChunks()
	if err != nil {
		level.Error(h.logger).Log("msg", "Loading on-disk chunks failed, discarding chunk files completely", "err", err)
		mmappedChunks = map[uint64][]*mmappedChunk{}
	}

	return mmappedChunks
}

// Truncate removes old data before mint from the head and WAL.
func (h *Head) Truncate(mint int64) (err error) {
	initialize := h.MinTime() == math.MaxInt64
	if err := h.truncateMemory(mint); err != nil {
		return err
	}
	if initialize {
		return nil
	}
	return h.truncateWAL(mint)
}

// truncateMemory removes old data before mint from the head.
func (h *Head) truncateMemory(mint int64) (err error) {
	defer func() {
		if err != nil {
			h.metrics.headTruncateFail.Inc()
		}
	}()
	initialize := h.MinTime() == math.MaxInt64

	if h.MinTime() >= mint && !initialize {
		return nil
	}
	h.minTime.Store(mint)
	h.minValidTime.Store(mint)

	// Ensure that max time is at least as high as min time.
	for h.MaxTime() < mint {
		h.maxTime.CAS(h.MaxTime(), mint)
	}

	// This was an initial call to Truncate after loading blocks on startup.
	// We haven't read back the WAL yet, so do not attempt to truncate it.
	if initialize {
		return nil
	}

	h.metrics.headTruncateTotal.Inc()
	start := time.Now()

	actualMint := h.gc()
	level.Info(h.logger).Log("msg", "Head GC completed", "duration", time.Since(start))
	h.metrics.gcDuration.Observe(time.Since(start).Seconds())
	if actualMint > h.minTime.Load() {
		// The actual mint of the Head is higher than the one asked to truncate.
		appendableMinValidTime := h.appendableMinValidTime()
		if actualMint < appendableMinValidTime {
			h.minTime.Store(actualMint)
			h.minValidTime.Store(actualMint)
		} else {
			// The actual min time is in the appendable window.
			// So we set the mint to the appendableMinValidTime.
			h.minTime.Store(appendableMinValidTime)
			h.minValidTime.Store(appendableMinValidTime)
		}
	}

	// Truncate the chunk m-mapper.
	if err := h.chunkDiskMapper.Truncate(mint); err != nil {
		return errors.Wrap(err, "truncate chunks.HeadReadWriter")
	}
	return nil
}

// truncateWAL removes old data before mint from the WAL.
func (h *Head) truncateWAL(mint int64) error {
	if h.wal == nil || mint <= h.lastWALTruncationTime.Load() {
		return nil
	}
	start := time.Now()
	h.lastWALTruncationTime.Store(mint)

	first, last, err := wal.Segments(h.wal.Dir())
	if err != nil {
		return errors.Wrap(err, "get segment range")
	}
	// Start a new segment, so low ingestion volume TSDB don't have more WAL than
	// needed.
	if err := h.wal.NextSegment(); err != nil {
		return errors.Wrap(err, "next segment")
	}
	last-- // Never consider last segment for checkpoint.
	if last < 0 {
		return nil // no segments yet.
	}
	// The lower two thirds of segments should contain mostly obsolete samples.
	// If we have less than two segments, it's not worth checkpointing yet.
	// With the default 2h blocks, this will keeping up to around 3h worth
	// of WAL segments.
	last = first + (last-first)*2/3
	if last <= first {
		return nil
	}

	keep := func(id uint64) bool {
		if h.series.getByID(id) != nil {
			return true
		}
		h.deletedMtx.Lock()
		_, ok := h.deleted[id]
		h.deletedMtx.Unlock()
		return ok
	}
	h.metrics.checkpointCreationTotal.Inc()
	if _, err = wal.Checkpoint(h.logger, h.wal, first, last, keep, mint); err != nil {
		h.metrics.checkpointCreationFail.Inc()
		if _, ok := errors.Cause(err).(*wal.CorruptionErr); ok {
			h.metrics.walCorruptionsTotal.Inc()
		}
		return errors.Wrap(err, "create checkpoint")
	}
	if err := h.wal.Truncate(last + 1); err != nil {
		// If truncating fails, we'll just try again at the next checkpoint.
		// Leftover segments will just be ignored in the future if there's a checkpoint
		// that supersedes them.
		level.Error(h.logger).Log("msg", "truncating segments failed", "err", err)
	}

	// The checkpoint is written and segments before it is truncated, so we no
	// longer need to track deleted series that are before it.
	h.deletedMtx.Lock()
	for ref, segment := range h.deleted {
		if segment < first {
			delete(h.deleted, ref)
		}
	}
	h.deletedMtx.Unlock()

	h.metrics.checkpointDeleteTotal.Inc()
	if err := wal.DeleteCheckpoints(h.wal.Dir(), last); err != nil {
		// Leftover old checkpoints do not cause problems down the line beyond
		// occupying disk space.
		// They will just be ignored since a higher checkpoint exists.
		level.Error(h.logger).Log("msg", "delete old checkpoints", "err", err)
		h.metrics.checkpointDeleteFail.Inc()
	}
	h.metrics.walTruncateDuration.Observe(time.Since(start).Seconds())

	level.Info(h.logger).Log("msg", "WAL checkpoint complete",
		"first", first, "last", last, "duration", time.Since(start))

	return nil
}

// initTime initializes a head with the first timestamp. This only needs to be called
// for a completely fresh head with an empty WAL.
func (h *Head) initTime(t int64) {
	if !h.minTime.CAS(math.MaxInt64, t) {
		return
	}
	// Ensure that max time is initialized to at least the min time we just set.
	// Concurrent appenders may already have set it to a higher value.
	h.maxTime.CAS(math.MinInt64, t)
}

type Stats struct {
	NumSeries         uint64
	MinTime, MaxTime  int64
	IndexPostingStats *index.PostingsStats
}

// Stats returns important current HEAD statistics. Note that it is expensive to
// calculate these.
func (h *Head) Stats(statsByLabelName string) *Stats {
	return &Stats{
		NumSeries:         h.NumSeries(),
		MaxTime:           h.MaxTime(),
		MinTime:           h.MinTime(),
		IndexPostingStats: h.PostingsCardinalityStats(statsByLabelName),
	}
}

type RangeHead struct {
	head       *Head
	mint, maxt int64
}

// NewRangeHead returns a *RangeHead.
func NewRangeHead(head *Head, mint, maxt int64) *RangeHead {
	return &RangeHead{
		head: head,
		mint: mint,
		maxt: maxt,
	}
}

func (h *RangeHead) Index() (IndexReader, error) {
	return h.head.indexRange(h.mint, h.maxt), nil
}

func (h *RangeHead) Chunks() (ChunkReader, error) {
	return h.head.chunksRange(h.mint, h.maxt, h.head.iso.State())
}

func (h *RangeHead) Tombstones() (tombstones.Reader, error) {
	return h.head.tombstones, nil
}

func (h *RangeHead) MinTime() int64 {
	return h.mint
}

// MaxTime returns the max time of actual data fetch-able from the head.
// This controls the chunks time range which is closed [b.MinTime, b.MaxTime].
func (h *RangeHead) MaxTime() int64 {
	return h.maxt
}

// BlockMaxTime returns the max time of the potential block created from this head.
// It's different to MaxTime as we need to add +1 millisecond to block maxt because block
// intervals are half-open: [b.MinTime, b.MaxTime). Block intervals are always +1 than the total samples it includes.
func (h *RangeHead) BlockMaxTime() int64 {
	return h.MaxTime() + 1
}

func (h *RangeHead) NumSeries() uint64 {
	return h.head.NumSeries()
}

func (h *RangeHead) Meta() BlockMeta {
	return BlockMeta{
		MinTime: h.MinTime(),
		MaxTime: h.MaxTime(),
		ULID:    h.head.Meta().ULID,
		Stats: BlockStats{
			NumSeries: h.NumSeries(),
		},
	}
}

// String returns an human readable representation of the range head. It's important to
// keep this function in order to avoid the struct dump when the head is stringified in
// errors or logs.
func (h *RangeHead) String() string {
	return fmt.Sprintf("range head (mint: %d, maxt: %d)", h.MinTime(), h.MaxTime())
}

// initAppender is a helper to initialize the time bounds of the head
// upon the first sample it receives.
type initAppender struct {
	app  storage.Appender
	head *Head
}

func (a *initAppender) Append(ref uint64, lset labels.Labels, t int64, v float64) (uint64, error) {
	if a.app != nil {
		return a.app.Append(ref, lset, t, v)
	}

	a.head.initTime(t)
	a.app = a.head.appender()
	return a.app.Append(ref, lset, t, v)
}

func (a *initAppender) AppendExemplar(ref uint64, l labels.Labels, e exemplar.Exemplar) (uint64, error) {
	// Check if exemplar storage is enabled.
	if a.head.opts.NumExemplars <= 0 {
		return 0, nil
	}

	if a.app != nil {
		return a.app.AppendExemplar(ref, l, e)
	}
	// We should never reach here given we would call Append before AppendExemplar
	// and we probably want to always base head/WAL min time on sample times.
	a.head.initTime(e.Ts)
	a.app = a.head.appender()

	return a.app.AppendExemplar(ref, l, e)
}

var _ storage.GetRef = &initAppender{}

func (a *initAppender) GetRef(lset labels.Labels) (uint64, labels.Labels) {
	if g, ok := a.app.(storage.GetRef); ok {
		return g.GetRef(lset)
	}
	return 0, nil
}

func (a *initAppender) Commit() error {
	if a.app == nil {
		return nil
	}
	return a.app.Commit()
}

func (a *initAppender) Rollback() error {
	if a.app == nil {
		return nil
	}
	return a.app.Rollback()
}

// Appender returns a new Appender on the database.
// 会根据具体情形决定返回的 Appender 实例
// Appender 实例共两类
// initAppender 会在接收到第一个数据点时初始化 Head 的起始时间(minTime、maxTime)，参见：head.initTime(t)逻辑，然后才是交由h.appender()处理
// headAppender 逻辑相对简单
func (h *Head) Appender(_ context.Context) storage.Appender {
	/**
	指标：prometheus_tsdb_head_active_appenders
	描述：当前活跃的appender交易数
	*/
	h.metrics.activeAppenders.Inc()

	// The head cache might not have a starting point yet. The init appender
	// picks up the first appended timestamp as the base.
	if h.MinTime() == math.MaxInt64 {
		return &initAppender{
			head: h,
		}
	}
	return h.appender()
}

func (h *Head) appender() *headAppender {
	appendID := h.iso.newAppendID()
	cleanupAppendIDsBelow := h.iso.lowWatermark()

	// Allocate the exemplars buffer only if exemplars are enabled.
	var exemplarsBuf []exemplarWithSeriesRef
	if h.opts.NumExemplars > 0 {
		exemplarsBuf = h.getExemplarBuffer()
	}

	return &headAppender{
		head:                  h,
		minValidTime:          h.appendableMinValidTime(),
		mint:                  math.MaxInt64,
		maxt:                  math.MinInt64,
		samples:               h.getAppendBuffer(),
		sampleSeries:          h.getSeriesBuffer(),
		exemplars:             exemplarsBuf,
		appendID:              appendID,
		cleanupAppendIDsBelow: cleanupAppendIDsBelow,
		exemplarAppender:      h.exemplars,
	}
}

func (h *Head) appendableMinValidTime() int64 {
	// Setting the minimum valid time to whichever is greater, the head min valid time or the compaction window,
	// ensures that no samples will be added within the compaction window to avoid races.
	return max(h.minValidTime.Load(), h.MaxTime()-h.chunkRange.Load()/2)
}

func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func (h *Head) ExemplarAppender() storage.ExemplarAppender {
	h.metrics.activeAppenders.Inc()

	// The head cache might not have a starting point yet. The init appender
	// picks up the first appended timestamp as the base.
	if h.MinTime() == math.MaxInt64 {
		return &initAppender{
			head: h,
		}
	}
	return h.appender()
}

func (h *Head) getAppendBuffer() []record.RefSample {
	b := h.appendPool.Get()
	if b == nil {
		return make([]record.RefSample, 0, 512)
	}
	return b.([]record.RefSample)
}

func (h *Head) putAppendBuffer(b []record.RefSample) {
	//nolint:staticcheck // Ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.appendPool.Put(b[:0])
}

func (h *Head) getExemplarBuffer() []exemplarWithSeriesRef {
	b := h.exemplarsPool.Get()
	if b == nil {
		return make([]exemplarWithSeriesRef, 0, 512)
	}
	return b.([]exemplarWithSeriesRef)
}

func (h *Head) putExemplarBuffer(b []exemplarWithSeriesRef) {
	if b == nil {
		return
	}

	//nolint:staticcheck // Ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.exemplarsPool.Put(b[:0])
}

func (h *Head) getSeriesBuffer() []*memSeries {
	b := h.seriesPool.Get()
	if b == nil {
		return make([]*memSeries, 0, 512)
	}
	return b.([]*memSeries)
}

func (h *Head) putSeriesBuffer(b []*memSeries) {
	//nolint:staticcheck // Ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.seriesPool.Put(b[:0])
}

func (h *Head) getBytesBuffer() []byte {
	b := h.bytesPool.Get()
	if b == nil {
		return make([]byte, 0, 1024)
	}
	return b.([]byte)
}

func (h *Head) putBytesBuffer(b []byte) {
	//nolint:staticcheck // Ignore SA6002 safe to ignore and actually fixing it has some performance penalty.
	h.bytesPool.Put(b[:0])
}

type exemplarWithSeriesRef struct {
	ref      uint64
	exemplar exemplar.Exemplar
}

type headAppender struct {
	head             *Head
	minValidTime     int64 // No samples below this timestamp are allowed.
	mint, maxt       int64
	exemplarAppender ExemplarStorage

	series       []record.RefSeries
	samples      []record.RefSample
	exemplars    []exemplarWithSeriesRef
	sampleSeries []*memSeries

	appendID, cleanupAppendIDsBelow uint64
	closed                          bool
}

func (a *headAppender) Append(ref uint64, lset labels.Labels, t int64, v float64) (uint64, error) {
	if t < a.minValidTime {
		a.head.metrics.outOfBoundSamples.Inc()
		return 0, storage.ErrOutOfBounds
	}

	s := a.head.series.getByID(ref)
	if s == nil {
		// Ensure no empty labels have gotten through.
		lset = lset.WithoutEmpty()
		if len(lset) == 0 {
			return 0, errors.Wrap(ErrInvalidSample, "empty labelset")
		}

		if l, dup := lset.HasDuplicateLabelNames(); dup {
			return 0, errors.Wrap(ErrInvalidSample, fmt.Sprintf(`label name "%s" is not unique`, l))
		}

		var created bool
		var err error
		// 如果lset对应的series没有，则建一个。同时把新建的series放入倒排Posting映射里面
		s, created, err = a.head.getOrCreate(lset.Hash(), lset)
		if err != nil {
			return 0, err
		}
		if created {
			// 如果新创建了一个，则将新建的也放到a.series里面
			a.series = append(a.series, record.RefSeries{
				Ref:    s.ref,
				Labels: lset,
			})
		}
	}

	s.Lock()
	if err := s.appendable(t, v); err != nil {
		s.Unlock()
		if err == storage.ErrOutOfOrderSample {
			a.head.metrics.outOfOrderSamples.Inc()
		}
		return 0, err
	}
	// 设置为等待提交状态
	s.pendingCommit = true
	s.Unlock()

	if t < a.mint {
		a.mint = t
	}
	if t > a.maxt {
		a.maxt = t
	}
	// 为了事务概念，放入temp存储，等待真正commit时候再写入memSeries
	// Prometheus在add数据点的时候并没有直接add到memSeries(也就是query所用到的结构体里),而是加入到一个临时的samples切片里面。
	// 同时还将这个数据点对应的memSeries同步增加到另一个sampleSeries里面。
	a.samples = append(a.samples, record.RefSample{
		Ref: s.ref,
		T:   t,
		V:   v,
	})
	a.sampleSeries = append(a.sampleSeries, s)
	return s.ref, nil
}

// AppendExemplar for headAppender assumes the series ref already exists, and so it doesn't
// use getOrCreate or make any of the lset sanity checks that Append does.
func (a *headAppender) AppendExemplar(ref uint64, _ labels.Labels, e exemplar.Exemplar) (uint64, error) {
	// Check if exemplar storage is enabled.
	if a.head.opts.NumExemplars <= 0 {
		return 0, nil
	}

	s := a.head.series.getByID(ref)
	if s == nil {
		return 0, fmt.Errorf("unknown series ref. when trying to add exemplar: %d", ref)
	}

	// Ensure no empty labels have gotten through.
	e.Labels = e.Labels.WithoutEmpty()

	err := a.exemplarAppender.ValidateExemplar(s.lset, e)
	if err != nil {
		if err == storage.ErrDuplicateExemplar {
			// Duplicate, don't return an error but don't accept the exemplar.
			return 0, nil
		}
		return 0, err
	}

	a.exemplars = append(a.exemplars, exemplarWithSeriesRef{ref, e})

	return s.ref, nil
}

var _ storage.GetRef = &headAppender{}

func (a *headAppender) GetRef(lset labels.Labels) (uint64, labels.Labels) {
	s := a.head.series.getByHash(lset.Hash(), lset)
	if s == nil {
		return 0, nil
	}
	// returned labels must be suitable to pass to Append()
	return s.ref, s.lset
}

func (a *headAppender) log() error {
	if a.head.wal == nil {
		return nil
	}

	buf := a.head.getBytesBuffer()
	defer func() { a.head.putBytesBuffer(buf) }()

	var rec []byte
	var enc record.Encoder

	if len(a.series) > 0 {
		rec = enc.Series(a.series, buf)
		buf = rec[:0]

		if err := a.head.wal.Log(rec); err != nil { //tsdb/head.go:1418
			return errors.Wrap(err, "log series")
		}
	}
	if len(a.samples) > 0 {
		rec = enc.Samples(a.samples, buf)
		buf = rec[:0]

		if err := a.head.wal.Log(rec); err != nil {
			return errors.Wrap(err, "log samples")
		}
	}
	if len(a.exemplars) > 0 {
		rec = enc.Exemplars(exemplarsForEncoding(a.exemplars), buf)
		buf = rec[:0]

		if err := a.head.wal.Log(rec); err != nil {
			return errors.Wrap(err, "log exemplars")
		}
	}
	return nil
}

func exemplarsForEncoding(es []exemplarWithSeriesRef) []record.RefExemplar {
	ret := make([]record.RefExemplar, 0, len(es))
	for _, e := range es {
		ret = append(ret, record.RefExemplar{
			Ref:    e.ref,
			T:      e.exemplar.Ts,
			V:      e.exemplar.Value,
			Labels: e.exemplar.Labels,
		})
	}
	return ret
}

func (a *headAppender) Commit() (err error) {
	if a.closed {
		return ErrAppenderClosed
	}
	defer func() { a.closed = true }()

	if err := a.log(); err != nil {
		_ = a.Rollback() // Most likely the same error will happen again.
		return errors.Wrap(err, "write to WAL")
	}

	// No errors logging to WAL, so pass the exemplars along to the in memory storage.
	for _, e := range a.exemplars {
		s := a.head.series.getByID(e.ref)
		// We don't instrument exemplar appends here, all is instrumented by storage.
		if err := a.exemplarAppender.AddExemplar(s.lset, e.exemplar); err != nil {
			if err == storage.ErrOutOfOrderExemplar {
				continue
			}
			level.Debug(a.head.logger).Log("msg", "Unknown error while adding exemplar", "err", err)
		}
	}

	defer a.head.metrics.activeAppenders.Dec()
	defer a.head.putAppendBuffer(a.samples)
	defer a.head.putSeriesBuffer(a.sampleSeries)
	defer a.head.putExemplarBuffer(a.exemplars)
	defer a.head.iso.closeAppend(a.appendID)

	total := len(a.samples)
	var series *memSeries
	for i, s := range a.samples {
		series = a.sampleSeries[i]
		series.Lock()
		ok, chunkCreated := series.append(s.T, s.V, a.appendID, a.head.chunkDiskMapper)
		series.cleanupAppendIDsBelow(a.cleanupAppendIDsBelow)
		series.pendingCommit = false
		series.Unlock()

		if !ok {
			total--
			a.head.metrics.outOfOrderSamples.Inc()
		}
		if chunkCreated {
			a.head.metrics.chunks.Inc()
			a.head.metrics.chunksCreated.Inc()
		}
	}

	a.head.metrics.samplesAppended.Add(float64(total))
	a.head.updateMinMaxTime(a.mint, a.maxt)

	return nil
}

func (a *headAppender) Rollback() (err error) {
	if a.closed {
		return ErrAppenderClosed
	}
	defer func() { a.closed = true }()
	defer a.head.metrics.activeAppenders.Dec()
	defer a.head.iso.closeAppend(a.appendID)
	defer a.head.putSeriesBuffer(a.sampleSeries)

	var series *memSeries
	for i := range a.samples {
		series = a.sampleSeries[i]
		series.Lock()
		series.cleanupAppendIDsBelow(a.cleanupAppendIDsBelow)
		series.pendingCommit = false
		series.Unlock()
	}
	a.head.putAppendBuffer(a.samples)
	a.head.putExemplarBuffer(a.exemplars)
	a.samples = nil
	a.exemplars = nil

	// Series are created in the head memory regardless of rollback. Thus we have
	// to log them to the WAL in any case.
	return a.log()
}

// Delete all samples in the range of [mint, maxt] for series that satisfy the given
// label matchers.
func (h *Head) Delete(mint, maxt int64, ms ...*labels.Matcher) error {
	// Do not delete anything beyond the currently valid range.
	mint, maxt = clampInterval(mint, maxt, h.MinTime(), h.MaxTime())

	ir := h.indexRange(mint, maxt)

	p, err := PostingsForMatchers(ir, ms...)
	if err != nil {
		return errors.Wrap(err, "select series")
	}

	var stones []tombstones.Stone
	for p.Next() {
		series := h.series.getByID(p.At())

		series.RLock()
		t0, t1 := series.minTime(), series.maxTime()
		series.RUnlock()
		if t0 == math.MinInt64 || t1 == math.MinInt64 {
			continue
		}
		// Delete only until the current values and not beyond.
		t0, t1 = clampInterval(mint, maxt, t0, t1)
		stones = append(stones, tombstones.Stone{Ref: p.At(), Intervals: tombstones.Intervals{{Mint: t0, Maxt: t1}}})
	}
	if p.Err() != nil {
		return p.Err()
	}
	if h.wal != nil {
		var enc record.Encoder
		if err := h.wal.Log(enc.Tombstones(stones, nil)); err != nil {
			return err
		}
	}
	for _, s := range stones {
		h.tombstones.AddInterval(s.Ref, s.Intervals[0])
	}

	return nil
}

// gc removes data before the minimum timestamp from the head.
// It returns the actual min times of the chunks present in the Head.
func (h *Head) gc() int64 {
	// Only data strictly lower than this timestamp must be deleted.
	mint := h.MinTime()

	// Drop old chunks and remember series IDs and hashes if they can be
	// deleted entirely.
	deleted, chunksRemoved, actualMint := h.series.gc(mint)
	seriesRemoved := len(deleted)

	h.metrics.seriesRemoved.Add(float64(seriesRemoved))
	h.metrics.chunksRemoved.Add(float64(chunksRemoved))
	h.metrics.chunks.Sub(float64(chunksRemoved))
	h.numSeries.Sub(uint64(seriesRemoved))

	// Remove deleted series IDs from the postings lists.
	h.postings.Delete(deleted)

	if h.wal != nil {
		_, last, _ := wal.Segments(h.wal.Dir())
		h.deletedMtx.Lock()
		// Keep series records until we're past segment 'last'
		// because the WAL will still have samples records with
		// this ref ID. If we didn't keep these series records then
		// on start up when we replay the WAL, or any other code
		// that reads the WAL, wouldn't be able to use those
		// samples since we would have no labels for that ref ID.
		for ref := range deleted {
			h.deleted[ref] = last
		}
		h.deletedMtx.Unlock()
	}

	// Rebuild symbols and label value indices from what is left in the postings terms.
	// symMtx ensures that append of symbols and postings is disabled for rebuild time.
	h.symMtx.Lock()
	defer h.symMtx.Unlock()

	symbols := make(map[string]struct{}, len(h.symbols))
	if err := h.postings.Iter(func(l labels.Label, _ index.Postings) error {
		symbols[l.Name] = struct{}{}
		symbols[l.Value] = struct{}{}
		return nil
	}); err != nil {
		// This should never happen, as the iteration function only returns nil.
		panic(err)
	}
	h.symbols = symbols

	return actualMint
}

// Tombstones returns a new reader over the head's tombstones
func (h *Head) Tombstones() (tombstones.Reader, error) {
	return h.tombstones, nil
}

// Index returns an IndexReader against the block.
func (h *Head) Index() (IndexReader, error) {
	return h.indexRange(math.MinInt64, math.MaxInt64), nil
}

func (h *Head) indexRange(mint, maxt int64) *headIndexReader {
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	return &headIndexReader{head: h, mint: mint, maxt: maxt}
}

// Chunks returns a ChunkReader against the block.
func (h *Head) Chunks() (ChunkReader, error) {
	return h.chunksRange(math.MinInt64, math.MaxInt64, h.iso.State())
}

func (h *Head) chunksRange(mint, maxt int64, is *isolationState) (*headChunkReader, error) {
	h.closedMtx.Lock()
	defer h.closedMtx.Unlock()
	if h.closed {
		return nil, errors.New("can't read from a closed head")
	}
	if hmin := h.MinTime(); hmin > mint {
		mint = hmin
	}
	return &headChunkReader{
		head:     h,
		mint:     mint,
		maxt:     maxt,
		isoState: is,
	}, nil
}

// NumSeries returns the number of active series in the head.
func (h *Head) NumSeries() uint64 {
	return h.numSeries.Load()
}

// Meta returns meta information about the head.
// The head is dynamic so will return dynamic results.
func (h *Head) Meta() BlockMeta {
	var id [16]byte
	copy(id[:], "______head______")
	return BlockMeta{
		MinTime: h.MinTime(),
		MaxTime: h.MaxTime(),
		ULID:    ulid.ULID(id),
		Stats: BlockStats{
			NumSeries: h.NumSeries(),
		},
	}
}

// MinTime returns the lowest time bound on visible data in the head.
func (h *Head) MinTime() int64 {
	return h.minTime.Load()
}

// MaxTime returns the highest timestamp seen in data of the head.
func (h *Head) MaxTime() int64 {
	return h.maxTime.Load()
}

// compactable returns whether the head has a compactable range.
// The head has a compactable range when the head time range is 1.5 times the chunk range.
// The 0.5 acts as a buffer of the appendable window.
func (h *Head) compactable() bool {
	return h.MaxTime()-h.MinTime() > h.chunkRange.Load()/2*3
}

// Close flushes the WAL and closes the head.
func (h *Head) Close() error {
	h.closedMtx.Lock()
	defer h.closedMtx.Unlock()
	h.closed = true
	errs := tsdb_errors.NewMulti(h.chunkDiskMapper.Close())
	if h.wal != nil {
		errs.Add(h.wal.Close())
	}
	return errs.Err()
}

// String returns an human readable representation of the TSDB head. It's important to
// keep this function in order to avoid the struct dump when the head is stringified in
// errors or logs.
func (h *Head) String() string {
	return "head"
}

type headChunkReader struct {
	head       *Head
	mint, maxt int64
	isoState   *isolationState
}

func (h *headChunkReader) Close() error {
	h.isoState.Close()
	return nil
}

// packChunkID packs a seriesID and a chunkID within it into a global 8 byte ID.
// It panicks if the seriesID exceeds 5 bytes or the chunk ID 3 bytes.
func packChunkID(seriesID, chunkID uint64) uint64 {
	if seriesID > (1<<40)-1 {
		panic("series ID exceeds 5 bytes")
	}
	if chunkID > (1<<24)-1 {
		panic("chunk ID exceeds 3 bytes")
	}
	return (seriesID << 24) | chunkID
}

func unpackChunkID(id uint64) (seriesID, chunkID uint64) {
	return id >> 24, (id << 40) >> 40
}

// Chunk returns the chunk for the reference number.
func (h *headChunkReader) Chunk(ref uint64) (chunkenc.Chunk, error) {
	sid, cid := unpackChunkID(ref)

	s := h.head.series.getByID(sid)
	// This means that the series has been garbage collected.
	if s == nil {
		return nil, storage.ErrNotFound
	}

	s.Lock()
	c, garbageCollect, err := s.chunk(int(cid), h.head.chunkDiskMapper)
	if err != nil {
		s.Unlock()
		return nil, err
	}
	defer func() {
		if garbageCollect {
			// Set this to nil so that Go GC can collect it after it has been used.
			c.chunk = nil
			s.memChunkPool.Put(c)
		}
	}()

	// This means that the chunk is outside the specified range.
	if !c.OverlapsClosedInterval(h.mint, h.maxt) {
		s.Unlock()
		return nil, storage.ErrNotFound
	}
	s.Unlock()

	return &safeChunk{
		Chunk:           c.chunk,
		s:               s,
		cid:             int(cid),
		isoState:        h.isoState,
		chunkDiskMapper: h.head.chunkDiskMapper,
	}, nil
}

type safeChunk struct {
	chunkenc.Chunk
	s               *memSeries
	cid             int
	isoState        *isolationState
	chunkDiskMapper *chunks.ChunkDiskMapper
}

func (c *safeChunk) Iterator(reuseIter chunkenc.Iterator) chunkenc.Iterator {
	c.s.Lock()
	it := c.s.iterator(c.cid, c.isoState, c.chunkDiskMapper, reuseIter)
	c.s.Unlock()
	return it
}

type headIndexReader struct {
	head       *Head
	mint, maxt int64
}

func (h *headIndexReader) Close() error {
	return nil
}

func (h *headIndexReader) Symbols() index.StringIter {
	h.head.symMtx.RLock()
	res := make([]string, 0, len(h.head.symbols))

	for s := range h.head.symbols {
		res = append(res, s)
	}
	h.head.symMtx.RUnlock()

	sort.Strings(res)
	return index.NewStringListIter(res)
}

// SortedLabelValues returns label values present in the head for the
// specific label name that are within the time range mint to maxt.
// If matchers are specified the returned result set is reduced
// to label values of metrics matching the matchers.
func (h *headIndexReader) SortedLabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	values, err := h.LabelValues(name, matchers...)
	if err == nil {
		sort.Strings(values)
	}
	return values, err
}

// LabelValues returns label values present in the head for the
// specific label name that are within the time range mint to maxt.
// If matchers are specified the returned result set is reduced
// to label values of metrics matching the matchers.
func (h *headIndexReader) LabelValues(name string, matchers ...*labels.Matcher) ([]string, error) {
	if h.maxt < h.head.MinTime() || h.mint > h.head.MaxTime() {
		return []string{}, nil
	}

	if len(matchers) == 0 {
		h.head.symMtx.RLock()
		defer h.head.symMtx.RUnlock()
		return h.head.postings.LabelValues(name), nil
	}

	return labelValuesWithMatchers(h, name, matchers...)
}

// LabelNames returns all the unique label names present in the head
// that are within the time range mint to maxt.
func (h *headIndexReader) LabelNames() ([]string, error) {
	h.head.symMtx.RLock()
	if h.maxt < h.head.MinTime() || h.mint > h.head.MaxTime() {
		h.head.symMtx.RUnlock()
		return []string{}, nil
	}

	labelNames := h.head.postings.LabelNames()
	h.head.symMtx.RUnlock()

	sort.Strings(labelNames)
	return labelNames, nil
}

// Postings returns the postings list iterator for the label pairs.
func (h *headIndexReader) Postings(name string, values ...string) (index.Postings, error) {
	res := make([]index.Postings, 0, len(values))
	for _, value := range values {
		res = append(res, h.head.postings.Get(name, value))
	}
	return index.Merge(res...), nil
}

func (h *headIndexReader) SortedPostings(p index.Postings) index.Postings {
	series := make([]*memSeries, 0, 128)

	// Fetch all the series only once.
	for p.Next() {
		s := h.head.series.getByID(p.At())
		if s == nil {
			level.Debug(h.head.logger).Log("msg", "Looked up series not found")
		} else {
			series = append(series, s)
		}
	}
	if err := p.Err(); err != nil {
		return index.ErrPostings(errors.Wrap(err, "expand postings"))
	}

	sort.Slice(series, func(i, j int) bool {
		return labels.Compare(series[i].lset, series[j].lset) < 0
	})

	// Convert back to list.
	ep := make([]uint64, 0, len(series))
	for _, p := range series {
		ep = append(ep, p.ref)
	}
	return index.NewListPostings(ep)
}

// Series returns the series for the given reference.
func (h *headIndexReader) Series(ref uint64, lbls *labels.Labels, chks *[]chunks.Meta) error {
	s := h.head.series.getByID(ref)

	if s == nil {
		h.head.metrics.seriesNotFound.Inc()
		return storage.ErrNotFound
	}
	*lbls = append((*lbls)[:0], s.lset...)

	s.Lock()
	defer s.Unlock()

	*chks = (*chks)[:0]

	for i, c := range s.mmappedChunks {
		// Do not expose chunks that are outside of the specified range.
		if !c.OverlapsClosedInterval(h.mint, h.maxt) {
			continue
		}
		*chks = append(*chks, chunks.Meta{
			MinTime: c.minTime,
			MaxTime: c.maxTime,
			Ref:     packChunkID(s.ref, uint64(s.chunkID(i))),
		})
	}
	if s.headChunk != nil && s.headChunk.OverlapsClosedInterval(h.mint, h.maxt) {
		*chks = append(*chks, chunks.Meta{
			MinTime: s.headChunk.minTime,
			MaxTime: math.MaxInt64, // Set the head chunks as open (being appended to).
			Ref:     packChunkID(s.ref, uint64(s.chunkID(len(s.mmappedChunks)))),
		})
	}

	return nil
}

// LabelValueFor returns label value for the given label name in the series referred to by ID.
func (h *headIndexReader) LabelValueFor(id uint64, label string) (string, error) {
	memSeries := h.head.series.getByID(id)
	if memSeries == nil {
		return "", storage.ErrNotFound
	}

	value := memSeries.lset.Get(label)
	if value == "" {
		return "", storage.ErrNotFound
	}

	return value, nil
}

func (h *Head) getOrCreate(hash uint64, lset labels.Labels) (*memSeries, bool, error) {
	// Just using `getOrCreateWithID` below would be semantically sufficient, but we'd create
	// a new series on every sample inserted via Add(), which causes allocations
	// and makes our series IDs rather random and harder to compress in postings.
	s := h.series.getByHash(hash, lset)
	if s != nil {
		return s, false, nil
	}

	// Optimistically assume that we are the first one to create the series.
	id := h.lastSeriesID.Inc()

	return h.getOrCreateWithID(id, hash, lset)
}

func (h *Head) getOrCreateWithID(id, hash uint64, lset labels.Labels) (*memSeries, bool, error) {
	// 如果lset对应的series没有，则建一个。同时把新建的series放入倒排Posting映射里面
	s, created, err := h.series.getOrSet(hash, lset, func() *memSeries {
		return newMemSeries(lset, id, h.chunkRange.Load(), &h.memChunkPool)
	})
	if err != nil {
		return nil, false, err
	}
	if !created {
		return s, false, nil
	}

	h.metrics.seriesCreated.Inc()
	h.numSeries.Inc()

	h.symMtx.Lock()
	defer h.symMtx.Unlock()

	for _, l := range lset {
		h.symbols[l.Name] = struct{}{}
		h.symbols[l.Value] = struct{}{}
	}

	h.postings.Add(id, lset)
	return s, true, nil
}

// seriesHashmap is a simple hashmap for memSeries by their label set. It is built
// on top of a regular hashmap and holds a slice of series to resolve hash collisions.
// Its methods require the hash to be submitted with it to avoid re-computations throughout
// the code.
type seriesHashmap map[uint64][]*memSeries

func (m seriesHashmap) get(hash uint64, lset labels.Labels) *memSeries {
	for _, s := range m[hash] {
		if labels.Equal(s.lset, lset) {
			return s
		}
	}
	return nil
}

func (m seriesHashmap) set(hash uint64, s *memSeries) {
	l := m[hash]
	for i, prev := range l {
		if labels.Equal(prev.lset, s.lset) {
			l[i] = s
			return
		}
	}
	m[hash] = append(l, s)
}

func (m seriesHashmap) del(hash uint64, lset labels.Labels) {
	var rem []*memSeries
	for _, s := range m[hash] {
		if !labels.Equal(s.lset, lset) {
			rem = append(rem, s)
		}
	}
	if len(rem) == 0 {
		delete(m, hash)
	} else {
		m[hash] = rem
	}
}

const (
	// DefaultStripeSize is the default number of entries to allocate in the stripeSeries hash map.
	DefaultStripeSize = 1 << 14
)

// stripeSeries locks modulo ranges of IDs and hashes to reduce lock contention.
// The locks are padded to not be on the same cache line. Filling the padded space
// with the maps was profiled to be slower – likely due to the additional pointer
// dereferences.
// 为了让Prometheus在内存和磁盘中保存更大的数据量，势必需要进行压缩。而memChunk在内存中保存的正是采用XOR算法压缩过的数据

/**
但是ref仅仅是供Prometheus内部使用的，如果用户要查询某个具体的时间序列，通常会利用一堆的label用以唯一指定一个时间序列。那么如何通过一堆label最快地找到对应的series呢？
哈希表显然是最佳的方案。基于label计算一个哈希值，维护一张哈希值与memSeries的映射表，如果产生哈希碰撞的话，则直接用label进行匹配。
因此，Prometheus有必要在内存中维护如下所示的两张哈希表，从而无论利用ref还是label都能很快找到对应的memSeries：
	series map[uint64]*memSeries // ref到memSeries的映射
	hashes map[uint64][]*memSeries // labels的哈希值到memSeries的映射
*/
type stripeSeries struct {
	size   int
	series []map[uint64]*memSeries // 记录refId到memSeries的映射
	hashes []seriesHashmap         // 记录hash值到memSeries,hash冲突采用拉链法,而hash值是依据labelSets的值而算出来
	// 由于在Prometheus中会频繁的对map[hash/refId]memSeries进行操作，例如检查这个labelSet对应的memSeries是否存在，
	// 不存在则创建等。由于golang的map非线程安全，所以其采用了分段锁去拆分锁。
	locks                   []stripeLock // 分段锁
	seriesLifecycleCallback SeriesLifecycleCallback
}

type stripeLock struct {
	sync.RWMutex
	// Padding to avoid multiple locks being on the same cache line.
	_ [40]byte
}

func newStripeSeries(stripeSize int, seriesCallback SeriesLifecycleCallback) *stripeSeries {
	s := &stripeSeries{
		size:                    stripeSize,
		series:                  make([]map[uint64]*memSeries, stripeSize),
		hashes:                  make([]seriesHashmap, stripeSize),
		locks:                   make([]stripeLock, stripeSize),
		seriesLifecycleCallback: seriesCallback,
	}

	for i := range s.series {
		s.series[i] = map[uint64]*memSeries{}
	}
	for i := range s.hashes {
		s.hashes[i] = seriesHashmap{}
	}
	return s
}

// gc garbage collects old chunks that are strictly before mint and removes
// series entirely that have no chunks left.
func (s *stripeSeries) gc(mint int64) (map[uint64]struct{}, int, int64) {
	var (
		deleted                  = map[uint64]struct{}{}
		deletedForCallback       = []labels.Labels{}
		rmChunks                 = 0
		actualMint         int64 = math.MaxInt64
	)
	// Run through all series and truncate old chunks. Mark those with no
	// chunks left as deleted and store their ID.
	for i := 0; i < s.size; i++ {
		s.locks[i].Lock()

		for hash, all := range s.hashes[i] {
			for _, series := range all {
				series.Lock()
				rmChunks += series.truncateChunksBefore(mint)

				if len(series.mmappedChunks) > 0 || series.headChunk != nil || series.pendingCommit {
					seriesMint := series.minTime()
					if seriesMint < actualMint {
						actualMint = seriesMint
					}
					series.Unlock()
					continue
				}

				// The series is gone entirely. We need to keep the series lock
				// and make sure we have acquired the stripe locks for hash and ID of the
				// series alike.
				// If we don't hold them all, there's a very small chance that a series receives
				// samples again while we are half-way into deleting it.
				j := int(series.ref) & (s.size - 1)

				if i != j {
					s.locks[j].Lock()
				}

				deleted[series.ref] = struct{}{}
				s.hashes[i].del(hash, series.lset)
				delete(s.series[j], series.ref)
				deletedForCallback = append(deletedForCallback, series.lset)

				if i != j {
					s.locks[j].Unlock()
				}

				series.Unlock()
			}
		}

		s.locks[i].Unlock()

		s.seriesLifecycleCallback.PostDeletion(deletedForCallback...)
		deletedForCallback = deletedForCallback[:0]
	}

	if actualMint == math.MaxInt64 {
		actualMint = mint
	}

	return deleted, rmChunks, actualMint
}

func (s *stripeSeries) getByID(id uint64) *memSeries {
	i := id & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.series[i][id]
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getByHash(hash uint64, lset labels.Labels) *memSeries {
	/**
	位运算(&)效率要比取模运算(%)高很多，主要原因是位运算直接对内存数据进行操作，不需要转成十进制，因此处理速度非常快。
		a % b == a & (b - 1)
		前提：b 为 2^n
		s.size = 16384 = 2 ^ 14
	*/
	i := hash & uint64(s.size-1)

	s.locks[i].RLock()
	series := s.hashes[i].get(hash, lset)
	s.locks[i].RUnlock()

	return series
}

func (s *stripeSeries) getOrSet(hash uint64, lset labels.Labels, createSeries func() *memSeries) (*memSeries, bool, error) {
	// PreCreation is called here to avoid calling it inside the lock.
	// It is not necessary to call it just before creating a series,
	// rather it gives a 'hint' whether to create a series or not.
	preCreationErr := s.seriesLifecycleCallback.PreCreation(lset)

	// Create the series, unless the PreCreation() callback as failed.
	// If failed, we'll not allow to create a new series anyway.
	var series *memSeries
	if preCreationErr == nil {
		series = createSeries()
	}

	/**
	Prometheus将一整个大的哈希表进行了切片，切割成了16k个小的哈希表。如果想要利用ref找到对应的series，首先要将ref对16K取模，假设得到的值为x，找到对应的小哈希表series[x]。
	至于对小哈希表的操作，只需要锁住模对应的locks[x]，从而大大减小了读写memSeries时对锁的抢占造成的损耗，提高了并发性能。对于基于label哈希值的读写，操作类似。

	然而上述数据结构仅仅只能支持对于时间序列的精确查询，必须严格指定每一个label的值从而能够唯一地确定一条时间序列。但很多时候，模糊查询才是更为常用的。例如，我们想知道访问路径为/的各类HTTP请求的数目（请求的方法可以为GET，POST等等），此时提交给Prometheus的查询条件如下：

	s.size = 16384 = 2^14 = 16k

		当b为2^n时：a%b=a&(b-1)
	*/
	i := hash & uint64(s.size-1)
	s.locks[i].Lock()

	if prev := s.hashes[i].get(hash, lset); prev != nil {
		s.locks[i].Unlock()
		return prev, false, nil
	}
	if preCreationErr == nil {
		s.hashes[i].set(hash, series)
	}
	s.locks[i].Unlock()

	if preCreationErr != nil {
		// The callback prevented creation of series.
		return nil, false, preCreationErr
	}
	// Setting the series in the s.hashes marks the creation of series
	// as any further calls to this methods would return that series.
	s.seriesLifecycleCallback.PostCreation(series.lset)

	i = series.ref & uint64(s.size-1)

	s.locks[i].Lock()
	s.series[i][series.ref] = series
	s.locks[i].Unlock()

	return series, true, nil
}

type sample struct {
	t int64
	v float64
}

func newSample(t int64, v float64) tsdbutil.Sample { return sample{t, v} }
func (s sample) T() int64                          { return s.t }
func (s sample) V() float64                        { return s.v }

// memSeries is the in-memory representation of a series. None of its methods
// are goroutine safe and it is the caller's responsibility to lock it.
/**
	一个memSeries主要由三部分组成：
		lset：用以识别这个series的label集合
		ref：每接收到一个新的时间序列（即它的label集合与已有的时间序列都不同）Prometheus就会用一个唯一的整数标识它，如果有ref，我们就能轻易找到相应的series
		memChunks：每一个memChunk是一个时间段内该时间序列所有sample的集合。如果我们想要读取[tx, ty]（t1 < tx < t2, t2 < ty < t3 ）时间范围内该时间序列的数据，
			只需要对[t1, t3]范围内的两个memChunk的sample数据进行裁剪即可，从而提高了查询的效率。每当采集到新的sample，Prometheus就会用Gorilla中类似的算法将它压缩至最新的memChunk中

但是ref仅仅是供Prometheus内部使用的，如果用户要查询某个具体的时间序列，通常会利用一堆的label用以唯一指定一个时间序列。那么如何通过一堆label最快地找到对应的series呢？
哈希表显然是最佳的方案。基于label计算一个哈希值，维护一张哈希值与memSeries的映射表，如果产生哈希碰撞的话，则直接用label进行匹配。因此，Prometheus有必要在内存中维护如下所示的两张哈希表，
从而无论利用ref还是label都能很快找到对应的memSeries：
{
	series map[uint64]*memSeries // ref到memSeries的映射
	hashes map[uint64][]*memSeries // labels的哈希值到memSeries的映射
}
然而我们知道Golang中的map并不是并发安全的，而Prometheus中又有大量对于memSeries的增删操作，如果在读写上述结构时简单地用一把大锁锁住，显然无法满足性能要求。所以Prometheus用了如下数据结构将锁的控制精细化:
const stripSize = 1 << 14

// 为表达直观，已将Prometheus原生数据结构简化
*/
type memSeries struct {
	sync.RWMutex

	// ref 相当于这个series的uid :在getOrCreate函数中给出了明确的解释，使用递增的正整数作为ref，而没有使用hash因为hash random且不利于索引
	ref uint64
	// lset 这个series 的label map
	lset          labels.Labels
	mmappedChunks []*mmappedChunk
	// headChunk 即memChunk 在这个series在这段时间内的数据集合
	// headChunk 一般都是active chunk 一直有samples写入
	headChunk    *memChunk
	chunkRange   int64
	firstChunkID int

	// nextAt ：下一个headChunk的开始时间
	// 设置一个最低的限制，下一个chunk必须创建的时间，其实是受 const samplesPerChunk = 120(当sample 达到120个的时候回cutHeadChunk) 和 chunkRange(default：2h) 影响的。
	nextAt int64 // Timestamp at which to cut the next chunk.
	//  保存最新的4个sample
	sampleBuf [4]sample
	// memSeries是否处于等待提交状态
	pendingCommit bool // Whether there are samples waiting to be committed to this series.

	app chunkenc.Appender // Current appender for the chunk.

	/**
	memChunkPool: 由于golang内建的GC机制会影响应用的性能，为了减少GC，golang提供了对象重用的机制，也就是sync.Pool对象池。
	sync.Pool是可伸缩的，并发安全的。其大小仅受限于内存的大小，可以被看作是一个存放可重用对象的值的容器。
	设计的目的是存放已经分配的但是暂时不用的对象，在需要用到的时候直接从pool中取。
	*/
	memChunkPool *sync.Pool
	// txs: 事务ID记录的一个结构体. 这个又是另一个模块（isolation）的知识体系
	txs *txRing
}

func newMemSeries(lset labels.Labels, id uint64, chunkRange int64, memChunkPool *sync.Pool) *memSeries {
	s := &memSeries{
		lset:         lset,
		ref:          id,
		chunkRange:   chunkRange,
		nextAt:       math.MinInt64,
		txs:          newTxRing(4),
		memChunkPool: memChunkPool,
	}
	return s
}

func (s *memSeries) minTime() int64 {
	if len(s.mmappedChunks) > 0 {
		return s.mmappedChunks[0].minTime
	}
	if s.headChunk != nil {
		return s.headChunk.minTime
	}
	return math.MinInt64
}

func (s *memSeries) maxTime() int64 {
	c := s.head()
	if c == nil {
		return math.MinInt64
	}
	return c.maxTime
}

/**
  将目前的headchunk 写入磁盘同时在内存中建立映射保存元数据,memSeries的指针在指向新的headChunk
	当前的headChunk达到full chunk条件的时候，会使用该方法重新初始化一个新的headChunk，并将memSeries的headChunk指向该chunk。
*/
func (s *memSeries) cutNewHeadChunk(mint int64, chunkDiskMapper *chunks.ChunkDiskMapper) *memChunk {
	s.mmapCurrentHeadChunk(chunkDiskMapper)

	// 初始化chunk的时候 chunk 为 XORChunk，并使用XORChunk中的Appender()方法 返回一个xorAppender
	// 结构体，这个结构体具体实现了Append方法，并赋值给 s.app。
	s.headChunk = &memChunk{
		chunk:   chunkenc.NewXORChunk(),
		minTime: mint,
		maxTime: math.MinInt64,
	}

	// Set upper bound on when the next chunk must be started. An earlier timestamp
	// may be chosen dynamically at a later point.
	fmt.Println("--->chunkRange:", s.chunkRange)
	s.nextAt = rangeForTimestamp(mint, s.chunkRange)

	app, err := s.headChunk.chunk.Appender()
	if err != nil {
		panic(err)
	}
	s.app = app
	return s.headChunk
}

/**
  将headChunk 写入磁盘 建立M-map映射
  	将目前的headChunk 写入到磁盘中，同时会建立内存映射。内存映射主要是 mmappedChunks []*mmappedChunk 进行内存中存储。
*/
func (s *memSeries) mmapCurrentHeadChunk(chunkDiskMapper *chunks.ChunkDiskMapper) {
	if s.headChunk == nil {
		// There is no head chunk, so nothing to m-map here.
		return
	}

	/**
	当 active chunk 写满sample后，就会使用 chunks.ChunkDiskMapper -> chunkDiskMapper.WriteChunk() 写入到磁盘中，
	同时生成 chunkRef，这个值 represent ：该时间序列磁盘中的chunk 在内存中的映射。
	mmappedChunks []*mmappedChunk 维护着该时间序列的所有的chunk。
	*/
	// 将full chunk 写入到磁盘
	fmt.Println("--->writechunk:", s.headChunk.minTime, s.headChunk.maxTime, time.Now())
	chunkRef, err := chunkDiskMapper.WriteChunk(s.ref, s.headChunk.minTime, s.headChunk.maxTime, s.headChunk.chunk)
	if err != nil {
		if err != chunks.ErrChunkDiskMapperClosed {
			panic(err)
		}
	}
	// 建立内存映射
	s.mmappedChunks = append(s.mmappedChunks, &mmappedChunk{
		ref:        chunkRef,
		numSamples: uint16(s.headChunk.chunk.NumSamples()),
		minTime:    s.headChunk.minTime,
		maxTime:    s.headChunk.maxTime,
	})
}

// appendable checks whether the given sample is valid for appending to the series.
func (s *memSeries) appendable(t int64, v float64) error {
	c := s.head()
	if c == nil {
		return nil
	}

	if t > c.maxTime {
		return nil
	}
	if t < c.maxTime {
		// 乱序样本
		return storage.ErrOutOfOrderSample
	}
	// We are allowing exact duplicates as we can encounter them in valid cases
	// like federation and erroring out at that time would be extremely noisy.
	if math.Float64bits(s.sampleBuf[3].v) != math.Float64bits(v) {
		// 重复样本
		return storage.ErrDuplicateSampleForTimestamp
	}
	return nil
}

// chunk returns the chunk for the chunk id from memory or by m-mapping it from the disk.
// If garbageCollect is true, it means that the returned *memChunk
// (and not the chunkenc.Chunk inside it) can be garbage collected after it's usage.
/**
  根据id，找到想查找的chunk在内存映射中的索引，从而找到该chunk
*/
func (s *memSeries) chunk(id int, chunkDiskMapper *chunks.ChunkDiskMapper) (chunk *memChunk, garbageCollect bool, err error) {
	// ix represents the index of chunk in the s.mmappedChunks slice. The chunk id's are
	// incremented by 1 when new chunk is created, hence (id - firstChunkID) gives the slice index.
	// The max index for the s.mmappedChunks slice can be len(s.mmappedChunks)-1, hence if the ix
	// is len(s.mmappedChunks), it represents the next chunk, which is the head chunk.
	// ix是 该chunk在m-map中的索引. chunk的id 是从1开始逐渐增加的1,2,3，...
	ix := id - s.firstChunkID
	if ix < 0 || ix > len(s.mmappedChunks) {
		return nil, false, storage.ErrNotFound
	}
	if ix == len(s.mmappedChunks) { // 查找的是active chunk 正在执行写入的headChunk
		if s.headChunk == nil {
			return nil, false, errors.New("invalid head chunk")
		}
		return s.headChunk, false, nil // 由于是active chunk 所以不能被GC 回收。
	}
	// 查找的chunk已经落盘,根据m-map的ref 在磁盘中查找对应的chunk 这个落盘的chunk是没有mint，maxt
	chk, err := chunkDiskMapper.Chunk(s.mmappedChunks[ix].ref)
	if err != nil {
		if _, ok := err.(*chunks.CorruptionErr); ok {
			panic(err)
		}
		return nil, false, err
	}
	// 这里是一个trick，从池子里拿一个memChunk内存空间 然后进行初始化，这个是并发安全的。
	mc := s.memChunkPool.Get().(*memChunk)
	mc.chunk = chk
	mc.minTime = s.mmappedChunks[ix].minTime
	mc.maxTime = s.mmappedChunks[ix].maxTime
	return mc, true, nil // true: 表示该chunk使用后，可以被回收。
}

func (s *memSeries) chunkID(pos int) int {
	return pos + s.firstChunkID
}

// truncateChunksBefore removes all chunks from the series that
// have no timestamp at or after mint.
// Chunk IDs remain unchanged.
/**
这个函数的作用是给一个时间，把这个时间点之前的chunk都从series中剔除掉。
*/
func (s *memSeries) truncateChunksBefore(mint int64) (removed int) {
	// headchunk的最大时间小于给的这个时间段 说明 把这个series中所有的chunk都要清零。
	if s.headChunk != nil && s.headChunk.maxTime < mint {
		// If head chunk is truncated, we can truncate all mmapped chunks.
		removed = 1 + len(s.mmappedChunks)
		s.firstChunkID += removed
		s.headChunk = nil     // 清空
		s.mmappedChunks = nil // 清空
		return removed
	}
	// 判断每一个落盘的chunk中的时间 与 截断时间对比
	if len(s.mmappedChunks) > 0 {
		for i, c := range s.mmappedChunks { // 这个c其实不是chunk 只是磁盘某个chunk中在内存中的元数据
			if c.maxTime >= mint {
				break
			}
			removed = i + 1
		}
		s.mmappedChunks = append(s.mmappedChunks[:0], s.mmappedChunks[removed:]...)
		s.firstChunkID += removed
	}
	return removed
}

// append adds the sample (t, v) to the series. The caller also has to provide
// the appendID for isolation. (The appendID can be zero, which results in no
// isolation for this append.)
// It is unsafe to call this concurrently with s.iterator(...) without holding the series lock.
func (s *memSeries) append(t int64, v float64, appendID uint64, chunkDiskMapper *chunks.ChunkDiskMapper) (sampleInOrder, chunkCreated bool) {
	// Based on Gorilla white papers this offers near-optimal compression ratio
	// so anything bigger that this has diminishing returns and increases
	// the time range within which we have to decompress all samples.
	// 一个chunk最多120个sample
	// 这个就是定义了 一个full chunk的一个条件，当有120个samples 就证明full chunk，如果设置的采集时间是每15s采集一次，那么一个full chunk（每一个时间序列的）需要15s * 120 = 30min
	const samplesPerChunk = 120

	c := s.head()

	if c == nil {
		if len(s.mmappedChunks) > 0 && s.mmappedChunks[len(s.mmappedChunks)-1].maxTime >= t { // 该headChunk的最大时间都大于插入的时间t了，所以这个sample无法被append
			// Out of order sample. Sample timestamp is already in the mmaped chunks, so ignore it.
			return false, false
		}
		// There is no chunk in this series yet, create the first chunk for the sample.
		c = s.cutNewHeadChunk(t, chunkDiskMapper)
		chunkCreated = true
	}
	numSamples := c.chunk.NumSamples()

	// Out of order sample.
	if c.maxTime >= t {
		return false, chunkCreated
	}
	// If we reach 25% of a chunk's desired sample count, set a definitive time
	// at which to start the next chunk.
	// At latest it must happen at the timestamp set when the chunk was cut.
	// 到1/4时，重新计算nextAt(120点以后的时间)
	/**
	headChunk 满足什么条件才会触发memSeries指针指向新的headChunk，以及落盘和创建内存映射呢？
		1、headChunk达到120个samples
		2、headChunk时间跨度达到默认chunkRange设置的2小时
	*/

	fmt.Println("numSamples" + strconv.Itoa(numSamples))
	if numSamples == samplesPerChunk/4 {
		// 推断并设置下一个headChunk创建时间。这个函数有一个分母有一个+1操作，是为了防止分母为0。
		fmt.Println("s.nextAt:", s.nextAt, time.Now())
		s.nextAt = computeChunkEndTime(c.minTime, c.maxTime, s.nextAt)
		fmt.Println("=========", c.minTime, c.maxTime, s.nextAt, time.Now())
	}
	// 到达时间，创建新的headChunk
	if t >= s.nextAt {
		// 当达到nextAt后，写入老的headChunk数据，并新建headChunk：
		c = s.cutNewHeadChunk(t, chunkDiskMapper)
		chunkCreated = true
		fmt.Println("==============>创建新chunk:", time.Now())

	}
	// 向headChunk插入t/v数据
	s.app.Append(t, v)

	c.maxTime = t

	s.sampleBuf[0] = s.sampleBuf[1]
	s.sampleBuf[1] = s.sampleBuf[2]
	s.sampleBuf[2] = s.sampleBuf[3]
	s.sampleBuf[3] = sample{t: t, v: v}

	// 是否需要被隔离？ 如果是appendID == 0 则不需要。 主要是在查询和插入的时候。
	if appendID > 0 {
		s.txs.add(appendID)
	}

	return true, chunkCreated
}

// cleanupAppendIDsBelow cleans up older appendIDs. Has to be called after
// acquiring lock.
func (s *memSeries) cleanupAppendIDsBelow(bound uint64) {
	s.txs.cleanupAppendIDsBelow(bound)
}

// computeChunkEndTime estimates the end timestamp based the beginning of a
// chunk, its current timestamp and the upper bound up to which we insert data.
// It assumes that the time range is 1/4 full.
func computeChunkEndTime(start, cur, max int64) int64 {
	a := (max - start) / ((cur - start + 1) * 4)
	if a == 0 {
		return max
	}
	return start + (max-start)/a
}

// iterator returns a chunk iterator.
// It is unsafe to call this concurrently with s.append(...) without holding the series lock.
func (s *memSeries) iterator(id int, isoState *isolationState, chunkDiskMapper *chunks.ChunkDiskMapper, it chunkenc.Iterator) chunkenc.Iterator {
	// 根据id 从 memseries中找到对应的c -> memchunk
	c, garbageCollect, err := s.chunk(id, chunkDiskMapper)
	// TODO(fabxc): Work around! An error will be returns when a querier have retrieved a pointer to a
	// series's chunk, which got then garbage collected before it got
	// accessed.  We must ensure to not garbage collect as long as any
	// readers still hold a reference.
	if err != nil {
		return chunkenc.NewNopIterator()
	}
	defer func() {
		if garbageCollect {
			// Set this to nil so that Go GC can collect it after it has been used.
			// This should be done always at the end.
			c.chunk = nil
			s.memChunkPool.Put(c)
		}
	}()

	// ix： c 在 memseries中的索引值
	ix := id - s.firstChunkID
	// c 有多少个numSamples
	numSamples := c.chunk.NumSamples()
	stopAfter := numSamples

	if isoState != nil {
		totalSamples := 0    // Total samples in this series.
		previousSamples := 0 // Samples before this chunk.

		for j, d := range s.mmappedChunks {
			totalSamples += int(d.numSamples)
			if j < ix {
				previousSamples += int(d.numSamples)
			}
		}

		if s.headChunk != nil {
			totalSamples += s.headChunk.chunk.NumSamples()
		}

		// Removing the extra transactionIDs that are relevant for samples that
		// come after this chunk, from the total transactionIDs.
		appendIDsToConsider := s.txs.txIDCount - (totalSamples - (previousSamples + numSamples))

		// Iterate over the appendIDs, find the first one that the isolation state says not
		// to return.
		it := s.txs.iterator()
		for index := 0; index < appendIDsToConsider; index++ {
			// 没有初始化txs 所以就是postion=0  第一个位置的appendID
			appendID := it.At()
			if appendID <= isoState.maxAppendID { // Easy check first.
				if _, ok := isoState.incompleteAppends[appendID]; !ok { // 没有检测到这个某个sample的完成操作
					it.Next()
					continue
				}
			}
			// 还没到目前chunk的上一个chunk或者到了上一个chunk但是没遍历完呢。 大于0 说明 index目前curChunk上
			stopAfter = numSamples - (appendIDsToConsider - index)
			if stopAfter < 0 { // index还没遍历到 curChunk上
				stopAfter = 0 // Stopped in a previous chunk.
			}
			break
		}
	}

	if stopAfter == 0 {
		return chunkenc.NewNopIterator()
	}

	if id-s.firstChunkID < len(s.mmappedChunks) {
		if stopAfter == numSamples {
			return c.chunk.Iterator(it) // index 到了curChunk的末尾，其中it 传入的object可以将Iterator实例化，
		}
		if msIter, ok := it.(*stopIterator); ok {
			msIter.Iterator = c.chunk.Iterator(msIter.Iterator)
			msIter.i = -1
			msIter.stopAfter = stopAfter
			return msIter
		}
		return &stopIterator{
			Iterator:  c.chunk.Iterator(it),
			i:         -1,
			stopAfter: stopAfter,
		}
	}
	// Serve the last 4 samples for the last chunk from the sample buffer
	// as their compressed bytes may be mutated by added samples.
	if msIter, ok := it.(*memSafeIterator); ok {
		msIter.Iterator = c.chunk.Iterator(msIter.Iterator)
		msIter.i = -1
		msIter.total = numSamples
		msIter.stopAfter = stopAfter
		msIter.buf = s.sampleBuf
		return msIter
	}
	return &memSafeIterator{
		stopIterator: stopIterator{
			Iterator:  c.chunk.Iterator(it),
			i:         -1,
			stopAfter: stopAfter,
		},
		total: numSamples,
		buf:   s.sampleBuf,
	}
}

func (s *memSeries) head() *memChunk {
	return s.headChunk
}

type memChunk struct {
	chunk            chunkenc.Chunk
	minTime, maxTime int64
}

// OverlapsClosedInterval returns true if the chunk overlaps [mint, maxt].
func (mc *memChunk) OverlapsClosedInterval(mint, maxt int64) bool {
	return mc.minTime <= maxt && mint <= mc.maxTime
}

type stopIterator struct {
	chunkenc.Iterator

	i, stopAfter int
}

func (it *stopIterator) Next() bool {
	if it.i+1 >= it.stopAfter {
		return false
	}
	it.i++
	return it.Iterator.Next()
}

type memSafeIterator struct {
	stopIterator

	total int
	buf   [4]sample
}

func (it *memSafeIterator) Seek(t int64) bool {
	if it.Err() != nil {
		return false
	}

	ts, _ := it.At()

	for t > ts || it.i == -1 {
		if !it.Next() {
			return false
		}
		ts, _ = it.At()
	}

	return true
}

func (it *memSafeIterator) Next() bool {
	if it.i+1 >= it.stopAfter {
		return false
	}
	it.i++
	if it.total-it.i > 4 {
		return it.Iterator.Next()
	}
	return true
}

func (it *memSafeIterator) At() (int64, float64) {
	if it.total-it.i > 4 {
		return it.Iterator.At()
	}
	s := it.buf[4-(it.total-it.i)]
	return s.t, s.v
}

type mmappedChunk struct {
	ref              uint64
	numSamples       uint16
	minTime, maxTime int64
}

// Returns true if the chunk overlaps [mint, maxt].
func (mc *mmappedChunk) OverlapsClosedInterval(mint, maxt int64) bool {
	return mc.minTime <= maxt && mint <= mc.maxTime
}

// SeriesLifecycleCallback specifies a list of callbacks that will be called during a lifecycle of a series.
// It is always a no-op in Prometheus and mainly meant for external users who import TSDB.
// All the callbacks should be safe to be called concurrently.
// It is up to the user to implement soft or hard consistency by making the callbacks
// atomic or non-atomic. Atomic callbacks can cause degradation performance.
type SeriesLifecycleCallback interface {
	// PreCreation is called before creating a series to indicate if the series can be created.
	// A non nil error means the series should not be created.
	PreCreation(labels.Labels) error
	// PostCreation is called after creating a series to indicate a creation of series.
	PostCreation(labels.Labels)
	// PostDeletion is called after deletion of series.
	PostDeletion(...labels.Labels)
}

type noopSeriesLifecycleCallback struct{}

func (noopSeriesLifecycleCallback) PreCreation(labels.Labels) error { return nil }
func (noopSeriesLifecycleCallback) PostCreation(labels.Labels)      {}
func (noopSeriesLifecycleCallback) PostDeletion(...labels.Labels)   {}

func (h *Head) Size() int64 {
	var walSize int64
	if h.wal != nil {
		walSize, _ = h.wal.Size()
	}
	cdmSize, _ := h.chunkDiskMapper.Size()
	return walSize + cdmSize
}

func (h *RangeHead) Size() int64 {
	return h.head.Size()
}
