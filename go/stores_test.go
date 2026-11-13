package rhizodat

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestClickHouseExecuteReturnsTypedRows(t *testing.T) {
	ch, err := OpenClickHouse(testDSN(t, "RHIZODAT_TEST_CLICKHOUSE_DSN"), "metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	rows, err := ch.Execute(
		"SELECT {host:String} AS host, toInt64({n:Int64}) AS n, toDate32('2026-11-14') AS d, toDecimal64('12.34', 2) AS price",
		Params{"host": "web1", "n": 42},
	)
	if err != nil {
		t.Fatal(err)
	}
	row := rows.Maps()[0]
	if row["host"] != "web1" || row["n"] != int64(42) || row["d"] != Date("2026-11-14") || row["price"] != Decimal("12.34") {
		t.Fatalf("row %#v", row)
	}
	if _, err := ch.Execute("SELECT {d:Date}", Params{"d": Date("2026-11-14")}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a date parameter: %v", err)
	}
}

func TestDrevoExecuteBindsParameters(t *testing.T) {
	graph, err := OpenDrevo(testDSN(t, "RHIZODAT_TEST_DREVO_BOLT"), "topology")
	if err != nil {
		t.Fatal(err)
	}
	defer graph.Close()
	rows, err := graph.Execute("RETURN $name AS name, $tags AS tags", Params{"name": "web1", "tags": []string{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(rows.Columns, ",") != "name,tags" || rows.Values[0][0] != "web1" {
		t.Fatalf("got %+v", rows)
	}
}

// A resource is provisioned in one meta-transaction: its row in the inventory
// (PostgreSQL), its first metric (ClickHouse) and its node in the topology
// (drevo); all three carry the same dat_txn.
func TestOneMetaTransactionOverThreeStores(t *testing.T) {
	d := newDB(t)
	inventory := d.store("inventory")
	ch, err := OpenClickHouse(testDSN(t, "RHIZODAT_TEST_CLICKHOUSE_DSN"), "metrics_"+d.base)
	if err != nil {
		t.Fatal(err)
	}
	defer ch.Close()
	if err := ch.Install(); err != nil {
		t.Fatal(err)
	}
	table := "metric_" + d.base
	if _, err := ch.Execute(fmt.Sprintf("CREATE TABLE %s (host String, cpu Float64, dat_txn UUID, dat_tx_time Int64) ENGINE = MergeTree ORDER BY host", table), nil); err != nil {
		t.Fatal(err)
	}
	defer ch.Execute("DROP TABLE "+table, nil)
	graph, err := OpenDrevo(testDSN(t, "RHIZODAT_TEST_DREVO_BOLT"), "topology_"+d.base)
	if err != nil {
		t.Fatal(err)
	}
	defer graph.Close()
	label := "Host" + d.base
	defer graph.Execute(fmt.Sprintf("MATCH (h:%s) DETACH DELETE h", label), nil)

	coordinator, err := NewCoordinator(d.journal())
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	done, err := coordinator.Transactional(5, func(u *Unit) error {
		if err := u.Insert(inventory, "resource", []string{"name", "region"}, [][]any{{"web1", "eu-west-1"}}); err != nil {
			return err
		}
		if err := u.InsertClickHouse(ch, table, []string{"host", "cpu"}, [][]any{{"web1", 0.25}, {"web1", 0.75}}); err != nil {
			return err
		}
		return u.Run(graph, fmt.Sprintf("CREATE (:%s {name: $name, txn: $dat_txn})", label), Params{"name": "web1"})
	})
	if err != nil {
		t.Fatal(err)
	}
	pg := d.exec("SELECT dat_txn FROM " + d.base + "_inventory.resource")
	metrics, err := ch.Execute(fmt.Sprintf("SELECT count() AS n, any(toString(dat_txn)) AS t FROM %s", table), nil)
	if err != nil {
		t.Fatal(err)
	}
	nodes, err := graph.Execute(fmt.Sprintf("MATCH (h:%s) RETURN h.txn AS txn", label), nil)
	if err != nil {
		t.Fatal(err)
	}
	if pg.Values[0][0] != done.DatTxn {
		t.Fatalf("inventory has %v, the commit %s", pg.Values, done.DatTxn)
	}
	if m := metrics.Maps()[0]; m["n"] != int64(2) || m["t"] != string(done.DatTxn) {
		t.Fatalf("metrics %v", m)
	}
	if len(nodes.Values) != 1 || nodes.Values[0][0] != string(done.DatTxn) {
		t.Fatalf("topology %v", nodes.Values)
	}
}

func TestADrevoJournalOpens(t *testing.T) {
	j, err := OpenDrevoJournal(testDSN(t, "RHIZODAT_TEST_DREVO_BOLT"), "")
	if err != nil {
		t.Fatal(err)
	}
	j.Close()
	if _, err := OpenDrevoJournal("drevo+bolt://127.0.0.1:1", "no spaces"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a bad label: %v", err)
	}
}
