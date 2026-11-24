package rhizodat

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

// A cloud-operations service: its inventory row in PostgreSQL, its place in
// the dependency graph in drevo. Every write is one meta-transaction over both.
type Service struct {
	Entity
	Name   string   `rhizodat:"name,store=inventory"`
	Region string   `rhizodat:"region,store=inventory"`
	Tier   int      `rhizodat:"tier,store=graph"`
	Owner  UUID     `rhizodat:"owner,store=graph"`
	Tags   []string `rhizodat:"tags,store=graph"`
	Title  string   `rhizodat:"title,store=graph"` // stored as dat_title, like Python's
}

func TestAnEntitySpansPostgresAndDrevo(t *testing.T) {
	d := newDB(t)
	graph, err := OpenDrevo(testDSN(t, "RHIZODAT_TEST_DREVO_BOLT"), "graph")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(graph.Close)
	c, err := NewCoordinator(d.journal())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	// A type of its own per run: the test graph is shared.
	typeName := "ops.Service" + strings.ReplaceAll(string(newUUID(t)), "-", "")[20:]
	services, err := NewRepository[Service](c, []PartStore{d.namedStore("inventory"), graph}, EntityOptions{Type: typeName})
	if err != nil {
		t.Fatal(err)
	}
	if err := services.Install(); err != nil {
		t.Fatal(err)
	}

	owner := newUUID(t)
	svc := &Service{Name: "billing", Region: "eu-1", Tier: 1, Owner: owner, Tags: []string{"pci"}, Title: "Billing"}
	added, err := services.Add(svc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { services.Delete(svc.DatID) })
	got, err := services.Get(svc.DatID, ReadOptions{})
	if err != nil || got == nil {
		t.Fatalf("get: %+v %v", got, err)
	}
	want := *svc
	if !reflect.DeepEqual(*got, want) {
		t.Fatalf("read back %+v\nwant %+v", *got, want)
	}

	time.Sleep(5 * time.Millisecond)
	got.Tier, got.Tags = 2, nil
	if _, err := services.Update(got); err != nil {
		t.Fatal(err)
	}
	then := hlcTime(added.DatTxTime)
	before, err := services.Get(svc.DatID, ReadOptions{AsOf: &then})
	if err != nil || before.Tier != 1 || !reflect.DeepEqual(before.Tags, []string{"pci"}) {
		t.Fatalf("as of the add: %+v %v", before, err)
	}
	history, err := services.History(svc.DatID)
	if err != nil || len(history) != 2 || history[1].Tier != 2 || history[1].Tags != nil {
		t.Fatalf("history %+v %v", history, err)
	}
	// The graph holds the version as the Python facade's nodes do.
	rows, err := graph.Execute("MATCH (v:"+partTable(typeName)+" {dat_id: $id}) RETURN v.dat_title AS title ORDER BY v.dat_tx_time",
		Params{"id": string(svc.DatID)})
	if err != nil || len(rows.Values) != 2 || rows.Values[0][0] != "Billing" {
		t.Fatalf("the graph's versions %+v %v", rows, err)
	}

	if _, err := services.Delete(svc.DatID); err != nil {
		t.Fatal(err)
	}
	if gone, _ := services.Get(svc.DatID, ReadOptions{IncludeDeleted: true}); gone != nil {
		t.Fatalf("erased in both stores, still read: %+v", gone)
	}
}

func TestGoTypesGetThePythonFacadesDrevoKinds(t *testing.T) {
	var (
		text   string
		count  int
		ratio  float64
		flag   bool
		id     UUID
		ptrID  *UUID
		at     time.Time
		labels []string
		attrs  map[string]any
		price  Decimal
	)
	cases := []struct {
		value any
		want  string
	}{
		{&text, "native"}, {&count, "native"}, {&ratio, "native"}, {&flag, "native"},
		{&id, "uuid"}, {&ptrID, "uuid"}, {&at, "json"}, {&labels, "json"}, {&attrs, "json"},
		{&price, "json"},
	}
	for _, c := range cases {
		typ := reflect.TypeOf(c.value).Elem()
		if got, ok := drevoFieldKind(typ); !ok || got != c.want {
			t.Errorf("%v: %q %v, want %q", typ, got, ok, c.want)
		}
	}
	if _, ok := drevoFieldKind(reflect.TypeOf(make(chan int))); ok {
		t.Error("a channel is not storable")
	}
}

// A probe: its definition in PostgreSQL, its last measurement in ClickHouse.
type Probe struct {
	Entity
	Target   string    `rhizodat:"target,store=inventory"`
	Interval int       `rhizodat:"interval,store=inventory"`
	Latency  float64   `rhizodat:"latency,store=telemetry"`
	At       time.Time `rhizodat:"at,store=telemetry"`
	Up       bool      `rhizodat:"up,store=telemetry"`
	Labels   []string  `rhizodat:"labels,store=telemetry"`
}

func TestAnEntitySpansPostgresAndClickHouse(t *testing.T) {
	d := newDB(t)
	suffix := strings.ReplaceAll(string(newUUID(t)), "-", "")[20:]
	telemetry, err := OpenClickHouse(testDSN(t, "RHIZODAT_TEST_CLICKHOUSE_DSN"), "telemetry")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(telemetry.Close)
	c, err := NewCoordinator(d.journal())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	typeName := "ops.Probe" + suffix // the test ClickHouse is shared
	t.Cleanup(func() { telemetry.Execute("DROP TABLE IF EXISTS "+partTable(typeName), nil) })
	probes, err := NewRepository[Probe](c, []PartStore{d.namedStore("inventory"), telemetry}, EntityOptions{Type: typeName})
	if err != nil {
		t.Fatal(err)
	}
	if err := probes.Install(); err != nil {
		t.Fatal(err)
	}

	at := time.Date(2026, 11, 24, 10, 0, 0, 123456000, time.UTC)
	probe := &Probe{Target: "https://billing", Interval: 30, Latency: 12.5, At: at, Up: true, Labels: []string{"eu"}}
	added, err := probes.Add(probe)
	if err != nil {
		t.Fatal(err)
	}
	got, err := probes.Get(probe.DatID, ReadOptions{})
	if err != nil || got == nil {
		t.Fatalf("get: %+v %v", got, err)
	}
	if got.Target != probe.Target || got.Interval != 30 || got.Latency != 12.5 || !got.At.Equal(at) || !got.Up ||
		!reflect.DeepEqual(got.Labels, []string{"eu"}) {
		t.Fatalf("read back %+v", got)
	}

	time.Sleep(5 * time.Millisecond)
	got.Latency, got.Up = 950, false
	if _, err := probes.Update(got); err != nil {
		t.Fatal(err)
	}
	then := hlcTime(added.DatTxTime)
	if before, err := probes.Get(probe.DatID, ReadOptions{AsOf: &then}); err != nil || before.Latency != 12.5 || !before.Up {
		t.Fatalf("as of the add: %+v %v", before, err)
	}
	if history, err := probes.History(probe.DatID); err != nil || len(history) != 2 || history[1].Up {
		t.Fatalf("history %+v %v", history, err)
	}
	if _, err := probes.Delete(probe.DatID); err != nil {
		t.Fatal(err)
	}
	if gone, _ := probes.Get(probe.DatID, ReadOptions{IncludeDeleted: true}); gone != nil {
		t.Fatalf("erased in both stores, still read: %+v", gone)
	}
}

func TestGoTypesGetThePythonFacadesClickHouseColumns(t *testing.T) {
	var (
		text   string
		count  int
		ratio  float32
		flag   bool
		id     UUID
		at     time.Time
		day    Date
		price  Decimal
		blob   []byte
		labels []string
	)
	cases := []struct {
		value any
		want  string
	}{
		{&text, "String"}, {&count, "Int64"}, {&ratio, "Float64"}, {&flag, "Bool"}, {&id, "UUID"},
		{&at, "DateTime64(6, 'UTC')"}, {&day, "Date32"}, {&price, "json"}, {&blob, "json"},
		{&labels, "json"},
	}
	for _, c := range cases {
		typ := reflect.TypeOf(c.value).Elem()
		if got, ok := clickhouseColumnType(typ); !ok || got != c.want {
			t.Errorf("%v: %q %v, want %q", typ, got, ok, c.want)
		}
	}
	if _, ok := clickhouseColumnType(reflect.TypeOf(make(chan int))); ok {
		t.Error("a channel is not storable")
	}
}
