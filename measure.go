// Package measure provides a Datastore wrapper that records metrics
// using github.com/whyrusleeping/go-metrics.
package measure

import (
	"fmt"
	"io"
	"time"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/whyrusleeping/go-metrics"
)

const (
	defaultReservoirSize = 256
	defaultAlpha         = 0.015
)

// New wraps the datastore, providing metrics on the operations. The
// metrics are registered with names starting with prefix and a dot.
//
// If prefix is not unique, New will panic. Call Close to release the
// prefix.
func New(prefix string, ds datastore.Datastore) *measure {
	m := &measure{
		backend: ds,

		putCount:   registerCounter(prefix + ".put.count"),
		putErr:     registerCounter(prefix + ".put.err"),
		putLatency: registerHistogram(prefix + ".put.latency"),
		putSize:    registerHistogram(prefix + ".put.size"),

		getCount:   registerCounter(prefix + ".get.count"),
		getErr:     registerCounter(prefix + ".get.err"),
		getLatency: registerHistogram(prefix + ".get.latency"),
		getSize:    registerHistogram(prefix + ".get.size"),

		hasCount:   registerCounter(prefix + ".has.count"),
		hasErr:     registerCounter(prefix + ".has.err"),
		hasLatency: registerHistogram(prefix + ".has.latency"),

		deleteCount:   registerCounter(prefix + ".delete.count"),
		deleteErr:     registerCounter(prefix + ".delete.err"),
		deleteLatency: registerHistogram(prefix + ".delete.latency"),

		queryCount:   registerCounter(prefix + ".query.count"),
		queryErr:     registerCounter(prefix + ".query.err"),
		queryLatency: registerHistogram(prefix + ".query.latency"),
	}
	return m
}

type measure struct {
	backend datastore.Datastore

	putCount   namedCounter
	putErr     namedCounter
	putLatency namedHistogram
	putSize    namedHistogram

	getCount   namedCounter
	getErr     namedCounter
	getLatency namedHistogram
	getSize    namedHistogram

	hasCount   namedCounter
	hasErr     namedCounter
	hasLatency namedHistogram

	deleteCount   namedCounter
	deleteErr     namedCounter
	deleteLatency namedHistogram

	queryCount   namedCounter
	queryErr     namedCounter
	queryLatency namedHistogram
}

type namedCounter struct {
	name string
	metrics.Counter
}

type namedHistogram struct {
	name string
	metrics.Histogram
}

func registerCounter(name string) namedCounter {
	counter := metrics.NewCounter()
	if err := metrics.Register(name, counter); err != nil {
		panic(fmt.Sprintf("duplicate metric \"%s\"", name))
	}
	return namedCounter{name, counter}
}

func registerHistogram(name string) namedHistogram {
	s := metrics.NewExpDecaySample(defaultReservoirSize, defaultAlpha)
	hist := metrics.NewHistogram(s)
	if err := metrics.Register(name, hist); err != nil {
		panic(fmt.Sprintf("duplicate metric \"%s\"", name))
	}
	return namedHistogram{name, hist}
}

func recordLatency(h namedHistogram, start time.Time) {
	elapsed := time.Now().Sub(start) / time.Microsecond
	h.Update(int64(elapsed))
}

func (m *measure) Put(key datastore.Key, value interface{}) error {
	defer recordLatency(m.putLatency, time.Now())
	m.putCount.Inc(1)
	if b, ok := value.([]byte); ok {
		m.putSize.Update(int64(len(b)))
	}
	err := m.backend.Put(key, value)
	if err != nil {
		m.putErr.Inc(1)
	}
	return err
}

func (m *measure) Get(key datastore.Key) (value interface{}, err error) {
	defer recordLatency(m.getLatency, time.Now())
	m.getCount.Inc(1)
	value, err = m.backend.Get(key)
	if err != nil {
		m.getErr.Inc(1)
	} else {
		if b, ok := value.([]byte); ok {
			m.getSize.Update(int64(len(b)))
		}
	}
	return value, err
}

func (m *measure) Has(key datastore.Key) (exists bool, err error) {
	defer recordLatency(m.hasLatency, time.Now())
	m.hasCount.Inc(1)
	exists, err = m.backend.Has(key)
	if err != nil {
		m.hasErr.Inc(1)
	}
	return exists, err
}

func (m *measure) Delete(key datastore.Key) error {
	defer recordLatency(m.deleteLatency, time.Now())
	m.deleteCount.Inc(1)
	err := m.backend.Delete(key)
	if err != nil {
		m.deleteErr.Inc(1)
	}
	return err
}

func (m *measure) Query(q query.Query) (query.Results, error) {
	defer recordLatency(m.queryLatency, time.Now())
	m.queryCount.Inc(1)
	res, err := m.backend.Query(q)
	if err != nil {
		m.queryErr.Inc(1)
	}
	return res, err
}

type measuredBatch struct {
	puts    int
	deletes int

	putts datastore.Batch
	delts datastore.Batch

	m *measure
}

func (m *measure) Batch() (datastore.Batch, error) {
	bds, ok := m.backend.(datastore.Batching)
	if !ok {
		return nil, datastore.ErrBatchUnsupported
	}
	pb, err := bds.Batch()
	if err != nil {
		return nil, err
	}

	db, err := bds.Batch()
	if err != nil {
		return nil, err
	}

	return &measuredBatch{
		putts: pb,
		delts: db,

		m: m,
	}, nil
}

func (mt *measuredBatch) Put(key datastore.Key, val interface{}) error {
	mt.puts++
	valb, ok := val.([]byte)
	if ok {
		mt.m.putSize.Update(int64(len(valb)))
	}
	return mt.putts.Put(key, val)
}

func (mt *measuredBatch) Delete(key datastore.Key) error {
	mt.deletes++
	return mt.delts.Delete(key)
}

func (mt *measuredBatch) Commit() error {
	err := logBatchCommit(mt.delts, mt.deletes, mt.m.deleteCount, mt.m.deleteErr, mt.m.deleteLatency)
	if err != nil {
		return err
	}

	err = logBatchCommit(mt.putts, mt.puts, mt.m.putCount, mt.m.putErr, mt.m.putLatency)
	if err != nil {
		return err
	}

	return nil
}

func logBatchCommit(b datastore.Batch, n int, num, errs metrics.Counter, lat namedHistogram) error {
	if n > 0 {
		before := time.Now()
		err := b.Commit()
		took := int(time.Now().Sub(before)/time.Microsecond) / n
		num.Inc(int64(n))
		for i := 0; i < n; i++ {
			lat.Update(int64(took))
		}
		if err != nil {
			errs.Inc(1)
			return err
		}
	}
	return nil
}

func (m *measure) Close() error {
	metrics.Unregister(m.putCount.name)
	metrics.Unregister(m.putErr.name)
	metrics.Unregister(m.putLatency.name)
	metrics.Unregister(m.putSize.name)
	metrics.Unregister(m.getCount.name)
	metrics.Unregister(m.getErr.name)
	metrics.Unregister(m.getLatency.name)
	metrics.Unregister(m.getSize.name)
	metrics.Unregister(m.hasCount.name)
	metrics.Unregister(m.hasErr.name)
	metrics.Unregister(m.hasLatency.name)
	metrics.Unregister(m.deleteCount.name)
	metrics.Unregister(m.deleteErr.name)
	metrics.Unregister(m.deleteLatency.name)
	metrics.Unregister(m.queryCount.name)
	metrics.Unregister(m.queryErr.name)
	metrics.Unregister(m.queryLatency.name)

	if c, ok := m.backend.(io.Closer); ok {
		return c.Close()
	}
	return nil
}
