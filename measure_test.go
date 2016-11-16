package measure

import (
	"testing"

	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	"github.com/ipfs/go-metrics-interface"
)

var (
	repo map[string]float64 = make(map[string]float64)
)

type metricsMock string

func (m metricsMock) Counter() metrics.Counter { return m }
func (m metricsMock) Gauge() metrics.Gauge { return m }
func (m metricsMock) Histogram(_ []float64) metrics.Histogram { return m }
func (m metricsMock) Summary(_ metrics.SummaryOpts) metrics.Summary { return m }

func (m metricsMock) Set(x float64) { repo[string(m)] = x }
func (m metricsMock) Add(x float64) { repo[string(m)] += x }
func (m metricsMock) Sub(x float64) { repo[string(m)] -= x }
func (m metricsMock) Inc() { repo[string(m)] += 1 }
func (m metricsMock) Dec() { repo[string(m)] -= 1 }
func (m metricsMock) Observe(x float64) { repo[string(m)] = x }

func newMockCreator(name, _ string) metrics.Creator {
	return metricsMock(name)
}

func TestPutCallsGetMeasured(t *testing.T) {
	metrics.InjectImpl(newMockCreator)
	defer metrics.InjectImpl(nil)

	ds := New("ds", datastore.NewMapDatastore())

	// test Put

	ds.Put(datastore.NewKey("a"), struct{}{})
	ds.Put(datastore.NewKey("b"), struct{}{})
	ds.Put(datastore.NewKey("c"), struct{}{})
	ds.Put(datastore.NewKey("d"), []byte("hello"))

	if len(repo) != 3 {
		t.Fatalf("should report 3 metrics after only puts, has %d", len(repo))
	}
	if puts := int(repo["ds.put_total"]); puts != 4 {
		t.Fatalf("should report 4 puts, does %d", puts)
	}
	if putLat := repo["ds.put.latency_seconds"]; putLat > 1 {
		t.Fatalf("spurious put latency reported - %g", putLat)
	}
	if putSize := int(repo["ds.put.size_bytes"]); putSize != len("hello") {
		t.Fatal("should report put of size %d, does %d", len("hello"), putSize)
	}
}

func TestGetCallsGetMeasured(t *testing.T) {
	metrics.InjectImpl(newMockCreator)
	defer metrics.InjectImpl(nil)

	repo = make(map[string]float64)
	ds := New("ds", datastore.NewMapDatastore())

	ds.Get(datastore.NewKey("a"))
	ds.Get(datastore.NewKey("b"))
	ds.Get(datastore.NewKey("c"))
	ds.Put(datastore.NewKey("d"), []byte("hello"))
	ds.Get(datastore.NewKey("d"))

	if getMetrics := len(repo) - 3; getMetrics!= 4 {
		t.Fatalf("should report 4 extra metrics after gets, does %d", getMetrics)
	}
	if gets := int(repo["ds.get_total"]); gets != 4 {
		t.Fatalf("should report 4 gets, does %d", gets)
	}
	if getErrs := int(repo["ds.get.errors_total"]); getErrs != 3 {
		t.Fatalf("should report 3 errored gets, does %d", getErrs)
	}
	if getLat := repo["ds.get.latency_seconds"]; getLat > 1 {
		t.Fatalf("spurious get latency reported - %g", getLat)
	}
	if getSize := int(repo["ds.get.size_bytes"]); getSize != len("hello") {
		t.Fatalf("should report get of size %d, does %d", len("hello"), getSize)
	}
}

func TestHasCallsGetMeasured(t *testing.T) {
	metrics.InjectImpl(newMockCreator)
	defer metrics.InjectImpl(nil)

	repo = make(map[string]float64)
	ds := New("ds", datastore.NewMapDatastore())

	ds.Has(datastore.NewKey("a"))
	ds.Has(datastore.NewKey("b"))
	ds.Has(datastore.NewKey("b"))

	if len(repo) != 2 {
		t.Fatalf("should report 2 metrics after only Has calls, does %d", len(repo))
	}
	if has := int(repo["ds.has_total"]); has != 3 {
		t.Fatalf("should report 3 has calls, does %d", has)
	}
	if hasLat := repo["ds.has.latency_seconds"]; hasLat > 1 {
		t.Fatalf("spurious has latency reported - %g", hasLat)
	}
}

func TestDeleteCallsGetMeasured(t *testing.T) {
	metrics.InjectImpl(newMockCreator)
	defer metrics.InjectImpl(nil)

	repo = make(map[string]float64)
	ds := New("ds", datastore.NewMapDatastore())

	ds.Put(datastore.NewKey("a"), struct{}{})
	ds.Put(datastore.NewKey("b"), struct{}{})
	ds.Delete(datastore.NewKey("a"))
	ds.Delete(datastore.NewKey("b"))
	ds.Delete(datastore.NewKey("b"))
	ds.Delete(datastore.NewKey("c"))

	if delMetrics := len(repo) - 2; delMetrics != 3 {
		t.Fatalf("should report 3 extra metrics after deletes, does %d", delMetrics)
	}
	if del := int(repo["ds.delete_total"]); del != 4 {
		t.Fatalf("should report 4 deletes, does %d", del)
	}
	if delErrs := int(repo["ds.delete.errors_total"]); delErrs != 2 {
		t.Fatalf("should report 2 errored deletes, does %d", delErrs)
	}
	if delLat := repo["ds.delete.latency_seconds"]; delLat > 1 {
		t.Fatalf("spurious del latency reported - %g", delLat)
	}
}

func TestQueryCallsGetMeasured(t *testing.T) {
	metrics.InjectImpl(newMockCreator)
	defer metrics.InjectImpl(nil)

	repo = make(map[string]float64)
	ds := New("ds", datastore.NewMapDatastore())

	ds.Query(query.Query{Prefix: "a"})
	ds.Query(query.Query{Prefix: "b"})
	ds.Query(query.Query{Prefix: "c", KeysOnly: true})

	if len(repo) != 2 {
		t.Fatalf("should report 2 metrics after only queries, does %d", len(repo))
	}
	if queries := int(repo["ds.query_total"]); queries != 3 {
		t.Fatalf("should report 3 queries, does %d", queries)
	}
	if queryLat := repo["ds.query.latency_seconds"]; queryLat > 1 {
		t.Fatalf("spurious query latency reported - %g", queryLat)
	}
}
