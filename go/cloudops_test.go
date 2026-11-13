// Scenario: monitoring and managing cloud resources over all four stores, from Go.
//
// The Go counterpart of tests/scenarios/test_cloud_ops.py, on application
// tables (Go has no entity repositories yet). The control plane keeps:
//
//   - the inventory in PostgreSQL: one row per version of a resource (kind,
//     name, region, size); every row carries the dat_tx_time of the
//     meta-transaction that wrote it, so the configuration as of any moment
//     is the latest version at or before it;
//   - the dependencies in the drevo graph: a resource node per resource and
//     a DEPENDS_ON edge to what it needs;
//   - the telemetry in ClickHouse, aggregated there (latency, errors,
//     requests per minute);
//   - every raw API request the control plane made in an Iceberg lake, the
//     audit trail.
//
// It runs the way an operator's tooling would: create the schema, provision a
// stack together with its API requests in one meta-transaction (an
// interrupted provisioning leaves neither resources nor requests), ask the
// graph for the blast radius of a failing database, find the unhealthy
// service in ClickHouse, scale it with its API request in one transaction and
// read the configuration as of before the incident, and audit its requests in
// the lake.
package rhizodat_test

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/ice1x/rhizodat/go"
)

// dsn is the environment's var, else .env.test's when its server answers;
// otherwise the scenario is skipped.
func dsn(t *testing.T, name string) string {
	t.Helper()
	if v := os.Getenv(name); v != "" {
		return v
	}
	f, err := os.Open("../.env.test")
	if err != nil {
		t.Skipf("no %s and no .env.test", name)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if v, ok := strings.CutPrefix(scanner.Text(), name+"="); ok {
			if u, err := url.Parse(v); err == nil {
				if conn, err := net.DialTimeout("tcp", u.Host, 500*time.Millisecond); err == nil {
					conn.Close()
					return v
				}
			}
			t.Skipf("%s from .env.test is not running (docker compose up -d --wait)", name)
		}
	}
	t.Skipf("no %s in .env.test", name)
	return ""
}

// must is v, or a panic with err: the test fails with its stack (cleanups
// still run). Go cannot pass a call's two results after a t argument.
func must[T any](v T, err error) T {
	if err != nil {
		panic(err)
	}
	return v
}

// estate is the four stores of one control plane, named after a test run.
type estate struct {
	t           *testing.T
	tag         string
	pg          string
	admin       *rhizodat.PostgresStore
	inventory   *rhizodat.PostgresStore
	telemetry   *rhizodat.ClickHouseStore
	topology    *rhizodat.DrevoStore
	lake        *rhizodat.IcebergStore
	coordinator *rhizodat.Coordinator
}

func (e *estate) metricTable() string  { return "ops_metric_" + e.tag }
func (e *estate) requestTable() string { return "ops_" + e.tag + ".api_request" }
func (e *estate) label() string        { return "Resource" + e.tag }

func newEstate(t *testing.T) *estate {
	id := strings.ReplaceAll(must(rhizodat.NewID()), "-", "")[20:]
	e := &estate{t: t, tag: id, pg: dsn(t, "RHIZODAT_TEST_PG_DSN")}
	schema := "ops_" + id
	e.admin = must(rhizodat.OpenPostgres(e.pg, "admin", ""))
	t.Cleanup(func() {
		e.admin.Execute(fmt.Sprintf("DROP SCHEMA IF EXISTS %s, %s_journal CASCADE", schema, schema))
		e.admin.Close()
	})
	for _, sql := range []string{
		"CREATE SCHEMA " + schema,
		"CREATE SCHEMA " + schema + "_journal",
		"CREATE TABLE " + schema + ".resource (id uuid, kind text, name text, region text, size int, dat_txn uuid, dat_tx_time bigint)",
	} {
		must(e.admin.Execute(sql))
	}
	e.inventory = must(rhizodat.OpenPostgres(e.pg, "inventory_"+id, schema))
	t.Cleanup(e.inventory.Close)
	if err := e.inventory.Install(); err != nil {
		t.Fatal(err)
	}

	e.telemetry = must(rhizodat.OpenClickHouse(dsn(t, "RHIZODAT_TEST_CLICKHOUSE_DSN"), "telemetry_"+id))
	t.Cleanup(e.telemetry.Close)
	if err := e.telemetry.Install(); err != nil {
		t.Fatal(err)
	}
	must(e.telemetry.Execute(fmt.Sprintf(
		"CREATE TABLE %s (resource String, minute DateTime, latency_ms Float64, errors Int64, requests Int64, dat_txn UUID, dat_tx_time Int64) ENGINE = MergeTree ORDER BY (resource, minute)",
		e.metricTable()), nil))
	t.Cleanup(func() { e.telemetry.Execute("DROP TABLE IF EXISTS "+e.metricTable(), nil) })

	e.topology = must(rhizodat.OpenDrevo(dsn(t, "RHIZODAT_TEST_DREVO_BOLT"), "topology_"+id))
	t.Cleanup(e.topology.Close)
	t.Cleanup(func() { e.topology.Execute(fmt.Sprintf("MATCH (r:%s) DETACH DELETE r", e.label()), nil) })

	e.lake = must(rhizodat.OpenIceberg(dsn(t, "RHIZODAT_TEST_ICEBERG_REST_DSN"), "lake_"+id))
	t.Cleanup(e.lake.Close)
	if err := e.lake.CreateTable(e.requestTable(), []rhizodat.Column{
		{Name: "id", Type: "uuid", Required: true},
		{Name: "resource", Type: "string"},
		{Name: "method", Type: "string"},
		{Name: "path", Type: "string"},
		{Name: "status", Type: "int"},
		{Name: "at", Type: "timestamptz"},
		{Name: "dat_txn", Type: "uuid"},
		{Name: "dat_tx_time", Type: "long"},
	}); err != nil {
		t.Fatal(err)
	}

	journal := must(rhizodat.OpenPostgresJournal(e.pg, schema+"_journal"))
	defer journal.Close()
	e.coordinator = must(rhizodat.NewCoordinator(journal))
	t.Cleanup(e.coordinator.Close)
	return e
}

type resource struct {
	ID     rhizodat.UUID
	Kind   string
	Name   string
	Region string
	Size   int
	Needs  []string // names of the resources it depends on
}

// request records the API call that made a change, in the same unit of work.
func (e *estate) request(u *rhizodat.Unit, r resource, method string, status int, at time.Time) error {
	id := rhizodat.UUID(must(rhizodat.NewID()))
	return u.Append(e.lake, e.requestTable(), []string{"id", "resource", "method", "path", "status", "at"},
		[][]any{{id, string(r.ID), method, "/v1/" + r.Kind + "s/" + r.Name, status, at}})
}

// provision creates a stack: each resource's first version, its node and
// edges, and the API request that created it — or none of them.
func (e *estate) provision(stack []resource, extra func(*rhizodat.Unit) error) (rhizodat.Committed, error) {
	at := time.Now().UTC()
	return e.coordinator.Transactional(3, func(u *rhizodat.Unit) error {
		for _, r := range stack {
			if err := u.Insert(e.inventory, "resource", []string{"id", "kind", "name", "region", "size"},
				[][]any{{r.ID, r.Kind, r.Name, r.Region, r.Size}}); err != nil {
				return err
			}
			if err := u.Run(e.topology, fmt.Sprintf("CREATE (:%s {id: $id, name: $name, kind: $kind, txn: $dat_txn})", e.label()),
				rhizodat.Params{"id": r.ID, "name": r.Name, "kind": r.Kind}); err != nil {
				return err
			}
			if err := e.request(u, r, "POST", 201, at); err != nil {
				return err
			}
		}
		for _, r := range stack {
			for _, need := range r.Needs {
				if err := u.Run(e.topology, fmt.Sprintf(
					"MATCH (a:%[1]s {name: $from}), (b:%[1]s {name: $to}) CREATE (a)-[:DEPENDS_ON]->(b)", e.label()),
					rhizodat.Params{"from": r.Name, "to": need}); err != nil {
					return err
				}
			}
		}
		if extra != nil {
			return extra(u)
		}
		return nil
	})
}

func (e *estate) stack() []resource {
	id := func() rhizodat.UUID { return rhizodat.UUID(must(rhizodat.NewID())) }
	return []resource{
		{ID: id(), Kind: "network", Name: "vpc-main", Region: "eu-west-1", Size: 1},
		{ID: id(), Kind: "database", Name: "orders-db", Region: "eu-west-1", Size: 4, Needs: []string{"vpc-main"}},
		{ID: id(), Kind: "service", Name: "orders-api", Region: "eu-west-1", Size: 2, Needs: []string{"orders-db"}},
		{ID: id(), Kind: "service", Name: "billing", Region: "eu-west-1", Size: 1, Needs: []string{"orders-db"}},
		{ID: id(), Kind: "balancer", Name: "public-lb", Region: "eu-west-1", Size: 1, Needs: []string{"orders-api"}},
	}
}

func (e *estate) inventorySize() int {
	rows := must(e.admin.Execute(fmt.Sprintf("SELECT count(DISTINCT id) AS n FROM ops_%s.resource", e.tag)))
	return int(rows.Values[0][0].(int64))
}

func (e *estate) requestCount() int {
	return len(must(e.lake.Scan(e.requestTable(), rhizodat.ScanOptions{Columns: []string{"id"}})).Values)
}

func TestCloudOpsAStackIsProvisionedWithItsRequestsOrNotAtAll(t *testing.T) {
	e := newEstate(t)
	// The audit store is down: the provisioning aborts before anything is
	// written, retryably (Transactional re-ran it, then gave up).
	down := must(rhizodat.OpenPostgres("postgresql://postgres:x@127.0.0.1:1/none?connect_timeout=2", "audit", ""))
	defer down.Close()
	_, err := e.provision(e.stack(), func(u *rhizodat.Unit) error {
		return u.Insert(down, "audit", []string{"note"}, [][]any{{"provisioned"}})
	})
	var aborted *rhizodat.AbortedError
	if !errors.As(err, &aborted) || !aborted.Retryable {
		t.Fatalf("an interrupted provisioning: %v", err)
	}
	if e.inventorySize() != 0 || e.requestCount() != 0 {
		t.Fatalf("an aborted provisioning left %d resources and %d requests", e.inventorySize(), e.requestCount())
	}
	nodes := must(e.topology.Execute(fmt.Sprintf("MATCH (r:%s) RETURN count(r) AS n", e.label()), nil))
	if nodes.Values[0][0] != int64(0) {
		t.Fatalf("an aborted provisioning left %v nodes", nodes.Values)
	}

	done := must(e.provision(e.stack(), nil))
	if e.inventorySize() != 5 || e.requestCount() != 5 {
		t.Fatalf("%d resources, %d requests", e.inventorySize(), e.requestCount())
	}
	stamped := must(e.topology.Execute(fmt.Sprintf("MATCH (r:%s) RETURN DISTINCT r.txn AS txn", e.label()), nil))
	if len(stamped.Values) != 1 || stamped.Values[0][0] != string(done.DatTxn) {
		t.Fatalf("the graph carries %v, the commit %s", stamped.Values, done.DatTxn)
	}
}

func TestCloudOpsTheBlastRadiusOfAFailingDatabaseComesFromTheGraph(t *testing.T) {
	e := newEstate(t)
	must(e.provision(e.stack(), nil))
	rows := must(e.topology.Execute(fmt.Sprintf(
		"MATCH (r:%[1]s)-[:DEPENDS_ON*1..]->(db:%[1]s {name: $db}) RETURN DISTINCT r.name AS name", e.label()),
		rhizodat.Params{"db": "orders-db"}))
	var hit []string
	for _, r := range rows.Values {
		hit = append(hit, r[0].(string))
	}
	sort.Strings(hit)
	if strings.Join(hit, ",") != "billing,orders-api,public-lb" {
		t.Fatalf("blast radius %v", hit)
	}
}

func TestCloudOpsTheUnhealthyServiceIsFoundScaledAndAudited(t *testing.T) {
	e := newEstate(t)
	stack := e.stack()
	provisioned := must(e.provision(stack, nil))
	api, billing := stack[2], stack[3]

	// Telemetry arrives in bulk, in its own meta-transactions.
	start := time.Date(2026, 11, 14, 9, 0, 0, 0, time.UTC)
	var samples [][]any
	for m := 0; m < 10; m++ {
		minute := start.Add(time.Duration(m) * time.Minute).Format("2006-01-02 15:04:05")
		samples = append(samples,
			[]any{string(api.ID), minute, 450.0 + float64(m), 40, 100},
			[]any{string(billing.ID), minute, 20.0, 0, 50})
	}
	must(e.coordinator.Transactional(3, func(u *rhizodat.Unit) error {
		return u.InsertClickHouse(e.telemetry, e.metricTable(), []string{"resource", "minute", "latency_ms", "errors", "requests"}, samples)
	}))

	unhealthy := must(e.telemetry.Execute(fmt.Sprintf(
		"SELECT resource, sum(errors) / sum(requests) AS rate FROM %s GROUP BY resource HAVING rate > {limit:Float64} ORDER BY rate DESC",
		e.metricTable()), rhizodat.Params{"limit": 0.05}))
	if len(unhealthy.Values) != 1 || unhealthy.Values[0][0] != string(api.ID) {
		t.Fatalf("unhealthy %v", unhealthy.Values)
	}

	// Scale it: a new inventory version and the API request, together.
	scaledAt := time.Now().UTC()
	scaled := must(e.coordinator.Transactional(3, func(u *rhizodat.Unit) error {
		if err := u.Insert(e.inventory, "resource", []string{"id", "kind", "name", "region", "size"},
			[][]any{{api.ID, api.Kind, api.Name, api.Region, 6}}); err != nil {
			return err
		}
		return e.request(u, api, "PATCH", 200, scaledAt)
	}))

	sizeAsOf := func(at int64) int64 {
		rows := must(e.admin.Execute(fmt.Sprintf(
			"SELECT size FROM ops_%s.resource WHERE id = $1 AND dat_tx_time <= $2 ORDER BY dat_tx_time DESC LIMIT 1", e.tag),
			api.ID, at))
		return rows.Values[0][0].(int64)
	}
	if now, before := sizeAsOf(scaled.DatTxTime), sizeAsOf(scaled.DatTxTime-1); now != 6 || before != 2 {
		t.Fatalf("size now %d, before the incident %d", now, before)
	}
	if before := sizeAsOf(provisioned.DatTxTime); before != 2 {
		t.Fatalf("size as provisioned %d", before)
	}

	// The audit trail of the service, natively in the lake.
	audit := must(e.lake.Scan(e.requestTable(), rhizodat.ScanOptions{
		Columns: []string{"method", "status", "dat_txn"},
		Filter:  fmt.Sprintf("resource = '%s'", api.ID),
	}))
	var calls []string
	for _, r := range audit.Values {
		calls = append(calls, fmt.Sprintf("%s %d", r[0], r[1]))
	}
	sort.Strings(calls)
	if strings.Join(calls, ",") != "PATCH 200,POST 201" {
		t.Fatalf("audit %v", calls)
	}
	for _, r := range audit.Values {
		if r[0] == "PATCH" && r[2] != scaled.DatTxn {
			t.Fatalf("the PATCH carries %v, the scaling commit %s", r[2], scaled.DatTxn)
		}
	}
}
