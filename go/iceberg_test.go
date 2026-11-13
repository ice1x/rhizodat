package rhizodat

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestRawRequestsGoToIcebergWithTheInventory(t *testing.T) {
	d := newDB(t)
	inventory := d.store("inventory")
	lake, err := OpenIceberg(testDSN(t, "RHIZODAT_TEST_ICEBERG_REST_DSN"), "lake")
	if err != nil {
		t.Fatal(err)
	}
	defer lake.Close()
	table := d.base + ".api_request"
	columns := []Column{
		{Name: "id", Type: "uuid", Required: true},
		{Name: "path", Type: "string"},
		{Name: "status", Type: "int"},
		{Name: "at", Type: "timestamptz"},
		{Name: "dat_txn", Type: "uuid"},
		{Name: "dat_tx_time", Type: "long"},
	}
	if err := lake.CreateTable(table, columns); err != nil {
		t.Fatal(err)
	}
	if err := lake.CreateTable(table, columns); err != nil {
		t.Fatalf("CreateTable is not idempotent: %v", err)
	}
	coordinator, err := NewCoordinator(d.journal())
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	at := time.Date(2026, 11, 14, 9, 30, 0, 0, time.UTC)
	done, err := coordinator.Transactional(3, func(u *Unit) error {
		if err := u.Insert(inventory, "resource", []string{"name"}, [][]any{{"api-gw"}}); err != nil {
			return err
		}
		return u.Append(lake, table, []string{"id", "path", "status", "at"}, [][]any{
			{UUID("01920000-0000-7000-8000-000000000001"), "/v1/vms", 200, at},
			{UUID("01920000-0000-7000-8000-000000000002"), "/v1/vms", 503, at.Add(time.Minute)},
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	failed, err := lake.Scan(table, ScanOptions{Columns: []string{"id", "status", "at", "dat_txn"}, Filter: "status >= 500"})
	if err != nil {
		t.Fatal(err)
	}
	if len(failed.Values) != 1 {
		t.Fatalf("got %v", failed.Values)
	}
	row := failed.Maps()[0]
	if row["id"] != UUID("01920000-0000-7000-8000-000000000002") || row["status"] != int64(503) || row["dat_txn"] != done.DatTxn {
		t.Fatalf("row %#v", row)
	}
	if got, ok := row["at"].(time.Time); !ok || !got.Equal(at.Add(time.Minute)) {
		t.Fatalf("at came back as %#v", row["at"])
	}
	limited, err := lake.Scan(table, ScanOptions{Limit: 1})
	if err != nil || len(limited.Values) != 1 {
		t.Fatalf("limit: %v %v", limited, err)
	}
	if d.count("inventory") != 1 {
		t.Fatal("the inventory row is missing")
	}
}

func TestIcebergRefusals(t *testing.T) {
	lake, err := OpenIceberg(testDSN(t, "RHIZODAT_TEST_ICEBERG_REST_DSN"), "lake")
	if err != nil {
		t.Fatal(err)
	}
	defer lake.Close()
	err = lake.RunOperations([]map[string]any{{"op": "drop_column", "table": "a.b", "name": "c"}})
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "not yet in Rust") {
		t.Fatalf("drop_column: %v", err)
	}
	unit, _ := NewUnit()
	defer unit.Close()
	if err := unit.Append(lake, "ns.t", []string{"a"}, [][]any{{1}, {"x"}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("mixed types: %v", err)
	}
	if _, err := OpenIceberg("iceberg+sql://nope", "x"); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a SQL catalog: %v", err)
	}
}
