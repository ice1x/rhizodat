package rhizodat

/*
#include <stdlib.h>
#include "rhizodat.h"
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sort"
	"sync"
)

// Params are named query parameters: ClickHouse {name:Type} placeholders,
// Cypher $name parameters.
type Params map[string]any

// encodeParams is params as a JSON object of tagged values (nil for none).
func encodeParams(params Params) (*C.char, error) {
	if len(params) == 0 {
		return nil, nil
	}
	names := make([]string, 0, len(params))
	for name := range params {
		names = append(names, name)
	}
	sort.Strings(names)
	object := make(map[string]any, len(params))
	for _, name := range names {
		value, err := encodeValue(params[name])
		if err != nil {
			return nil, &Error{Status: StatusInvalidArgument, Message: fmt.Sprintf("%s: %v", name, err)}
		}
		object[name] = value
	}
	text, err := json.Marshal(object)
	if err != nil {
		return nil, err
	}
	return C.CString(string(text)), nil
}

// ClickHouseStore is one ClickHouse database as a rhizodat store. It is safe
// for concurrent use. Close it when done.
type ClickHouseStore struct {
	ptr  *C.RhizodatClickHouseStore
	once sync.Once
}

// OpenClickHouse is a store named name over dsn ("clickhouse://...").
func OpenClickHouse(dsn, name string) (*ClickHouseStore, error) {
	cDSN, cName := C.CString(dsn), C.CString(name)
	defer freeC(cDSN, cName)
	var ptr *C.RhizodatClickHouseStore
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_clickhouse_store_open(cDSN, cName, &ptr, e)
	}); err != nil {
		return nil, err
	}
	s := &ClickHouseStore{ptr: ptr}
	runtime.SetFinalizer(s, (*ClickHouseStore).Close)
	return s, nil
}

// Install creates the store's service table dat_applied if it does not exist.
func (s *ClickHouseStore) Install() error {
	defer runtime.KeepAlive(s)
	return call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_clickhouse_store_install(s.ptr, e)
	})
}

// Execute runs one statement outside any meta-transaction, with params bound
// server-side to its {name:Type} placeholders (strings, numbers, booleans,
// UUIDs, nil). A SELECT (WITH, SHOW, DESCRIBE, EXISTS, EXPLAIN) returns its
// rows; any other statement runs for its effect and returns no rows. The rows
// travel as Arrow: cast a UUID column (toString) and use Date32 for dates.
func (s *ClickHouseStore) Execute(sql string, params Params) (*Rows, error) {
	defer runtime.KeepAlive(s)
	cParams, err := encodeParams(params)
	if err != nil {
		return nil, err
	}
	cSQL := C.CString(sql)
	defer freeC(cSQL, cParams)
	var out *C.char
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_clickhouse_store_execute(s.ptr, cSQL, cParams, &out, e)
	}); err != nil {
		return nil, err
	}
	return decodeRows(takeString(out))
}

// Close frees the store. Calling it again does nothing.
func (s *ClickHouseStore) Close() {
	s.once.Do(func() {
		C.rhizodat_clickhouse_store_free(s.ptr)
		runtime.SetFinalizer(s, nil)
	})
}

// DrevoStore is one drevo graph as a rhizodat store. It is safe for
// concurrent use. Close it when done.
type DrevoStore struct {
	ptr  *C.RhizodatDrevoStore
	once sync.Once
}

// OpenDrevo is a graph store named name over dsn ("drevo+bolt://...").
func OpenDrevo(dsn, name string) (*DrevoStore, error) {
	cDSN, cName := C.CString(dsn), C.CString(name)
	defer freeC(cDSN, cName)
	var ptr *C.RhizodatDrevoStore
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_drevo_store_open(cDSN, cName, &ptr, e)
	}); err != nil {
		return nil, err
	}
	s := &DrevoStore{ptr: ptr}
	runtime.SetFinalizer(s, (*DrevoStore).Close)
	return s, nil
}

// Execute runs one Cypher statement outside any meta-transaction with params
// (strings, numbers, booleans, UUIDs, lists, nil). Columns come in name order.
// A write fails retryably while a branch of this graph is prepared.
func (s *DrevoStore) Execute(cypher string, params Params) (*Rows, error) {
	defer runtime.KeepAlive(s)
	cParams, err := encodeParams(params)
	if err != nil {
		return nil, err
	}
	cCypher := C.CString(cypher)
	defer freeC(cCypher, cParams)
	var out *C.char
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_drevo_store_execute(s.ptr, cCypher, cParams, &out, e)
	}); err != nil {
		return nil, err
	}
	return decodeRows(takeString(out))
}

// Close frees the store. Calling it again does nothing.
func (s *DrevoStore) Close() {
	s.once.Do(func() {
		C.rhizodat_drevo_store_free(s.ptr)
		runtime.SetFinalizer(s, nil)
	})
}

// OpenDrevoJournal is the journal in the graph of dsn, its records under
// label ("" for DatJournal). The graph must hold no store's data.
func OpenDrevoJournal(dsn, label string) (*Journal, error) {
	cDSN, cLabel := cString(dsn, false), cString(label, true)
	defer freeC(cDSN, cLabel)
	var ptr *C.RhizodatJournal
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_drevo_journal_open(cDSN, cLabel, &ptr, e)
	}); err != nil {
		return nil, err
	}
	j := &Journal{ptr: ptr}
	runtime.SetFinalizer(j, (*Journal).Close)
	return j, nil
}

// InsertClickHouse buffers rows for table in a ClickHouse store, as Insert.
// ClickHouse is one-phase: the rows are applied after the decision, from the
// journal.
func (u *Unit) InsertClickHouse(store *ClickHouseStore, table string, columns []string, rows [][]any) error {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(store)
	cTable, cCols, cRows, err := tableArgs(table, columns, rows)
	if err != nil {
		return err
	}
	defer freeC(cTable, cCols, cRows)
	return call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_unit_clickhouse_insert(u.ptr, store.ptr, cTable, cCols, cRows, e)
	})
}

// Run buffers a Cypher statement for a drevo store. It runs inside the
// store's branch at the commit, with $dat_txn and $dat_tx_time added to
// params, so it may stamp what it writes with $dat_txn.
func (u *Unit) Run(store *DrevoStore, cypher string, params Params) error {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(store)
	cParams, err := encodeParams(params)
	if err != nil {
		return err
	}
	cCypher := C.CString(cypher)
	defer freeC(cCypher, cParams)
	return call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_unit_drevo_run(u.ptr, store.ptr, cCypher, cParams, e)
	})
}

// tableArgs is a table's name, columns and rows as C strings to free.
func tableArgs(table string, columns []string, rows [][]any) (*C.char, *C.char, *C.char, error) {
	cols, err := json.Marshal(columns)
	if err != nil {
		return nil, nil, nil, err
	}
	list := make([]any, len(rows))
	for i, r := range rows {
		list[i] = r
	}
	values, err := encodeValues(list)
	if err != nil {
		return nil, nil, nil, &Error{Status: StatusInvalidArgument, Message: err.Error()}
	}
	return C.CString(table), C.CString(string(cols)), C.CString(values), nil
}
