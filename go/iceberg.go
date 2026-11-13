package rhizodat

/*
#include <stdlib.h>
#include "rhizodat.h"
*/
import "C"

import (
	"encoding/json"
	"runtime"
	"sync"
)

// IcebergStore is one Iceberg catalog (REST or AWS Glue) as a rhizodat store.
// It connects on each call. It is safe for concurrent use. Close it when done.
type IcebergStore struct {
	ptr  *C.RhizodatIcebergStore
	once sync.Once
}

// OpenIceberg is a store named name over dsn ("iceberg+rest://..." or
// "iceberg+glue://<region>?warehouse=s3://...").
func OpenIceberg(dsn, name string) (*IcebergStore, error) {
	cDSN, cName := C.CString(dsn), C.CString(name)
	defer freeC(cDSN, cName)
	var ptr *C.RhizodatIcebergStore
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_iceberg_store_open(cDSN, cName, &ptr, e)
	}); err != nil {
		return nil, err
	}
	s := &IcebergStore{ptr: ptr}
	runtime.SetFinalizer(s, (*IcebergStore).Close)
	return s, nil
}

// Column is a column of CreateTable: its name, its Iceberg type (boolean,
// int, long, float, double, date, time, timestamp, timestamptz, string,
// uuid, binary, decimal(P, S)) and whether it is required.
type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Required bool   `json:"required,omitempty"`
}

// RunOperations runs the operations of an iceberg migration step, in order
// (each a map with "op" and its arguments, as in a migration file). The
// native core has create_namespace and create_table; each is a no-op when its
// result is there.
func (s *IcebergStore) RunOperations(ops []map[string]any) error {
	defer runtime.KeepAlive(s)
	text, err := json.Marshal(ops)
	if err != nil {
		return err
	}
	cOps := C.CString(string(text))
	defer freeC(cOps)
	return call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_iceberg_store_run_operations(s.ptr, cOps, e)
	})
}

// CreateTable creates the namespace of table ("namespace.table") and the
// table if missing. A table written in meta-transactions needs dat_txn
// (uuid or string) and dat_tx_time (long) columns.
func (s *IcebergStore) CreateTable(table string, columns []Column) error {
	namespace := table
	for i := len(table) - 1; i >= 0; i-- {
		if table[i] == '.' {
			namespace = table[:i]
			break
		}
	}
	return s.RunOperations([]map[string]any{
		{"op": "create_namespace", "namespace": namespace},
		{"op": "create_table", "table": table, "columns": columns},
	})
}

// ScanOptions narrow a scan; the zero value reads every column and row of the
// current snapshot.
type ScanOptions struct {
	Columns    []string `json:"columns,omitempty"`
	Limit      int      `json:"limit,omitempty"`
	SnapshotID int64    `json:"snapshot_id,omitempty"`
	// Filter is a row filter in pyiceberg's syntax ("status >= 500 and path = '/v1'").
	Filter string `json:"filter,omitempty"`
}

// Scan reads table ("namespace.table") outside any meta-transaction.
func (s *IcebergStore) Scan(table string, options ScanOptions) (*Rows, error) {
	defer runtime.KeepAlive(s)
	text, err := json.Marshal(options)
	if err != nil {
		return nil, err
	}
	cTable, cOptions := C.CString(table), C.CString(string(text))
	defer freeC(cTable, cOptions)
	var out *C.char
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_iceberg_store_scan(s.ptr, cTable, cOptions, &out, e)
	}); err != nil {
		return nil, err
	}
	return decodeRows(takeString(out))
}

// Close frees the store. Calling it again does nothing.
func (s *IcebergStore) Close() {
	s.once.Do(func() {
		C.rhizodat_iceberg_store_free(s.ptr)
		runtime.SetFinalizer(s, nil)
	})
}

// Append buffers rows for table ("namespace.table") of an Iceberg store, as
// Insert. Columns are matched by name and cast to the table's types at the
// commit (a time.Time, Date or Decimal becomes a timestamp, date or decimal
// column); table columns the rows lack are null. Iceberg is one-phase: data
// files are written at stage and appended after the decision.
func (u *Unit) Append(store *IcebergStore, table string, columns []string, rows [][]any) error {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(store)
	cTable, cCols, cRows, err := tableArgs(table, columns, rows)
	if err != nil {
		return err
	}
	defer freeC(cTable, cCols, cRows)
	return call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_unit_iceberg_append(u.ptr, store.ptr, cTable, cCols, cRows, e)
	})
}
