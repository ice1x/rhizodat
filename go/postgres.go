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
	"sync"
	"unsafe"
)

// cString is s as a C string the caller frees with C.free; "" is NULL when
// orNull is set (an optional argument).
func cString(s string, orNull bool) *C.char {
	if orNull && s == "" {
		return nil
	}
	return C.CString(s)
}

func freeC(ptrs ...*C.char) {
	for _, p := range ptrs {
		if p != nil {
			C.free(unsafe.Pointer(p))
		}
	}
}

// PostgresStore is one PostgreSQL database as a rhizodat store. It is safe for
// concurrent use. Close it when done (a finalizer frees it otherwise).
type PostgresStore struct {
	ptr  *C.RhizodatPostgresStore
	once sync.Once
}

// OpenPostgres is a store named name over dsn ("postgresql://..."), confined
// to schema unless it is "" (no two stores may share one schema). It connects
// on first use.
func OpenPostgres(dsn, name, schema string) (*PostgresStore, error) {
	cDSN, cName, cSchema := cString(dsn, false), cString(name, false), cString(schema, true)
	defer freeC(cDSN, cName, cSchema)
	var ptr *C.RhizodatPostgresStore
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_postgres_store_open(cDSN, cName, cSchema, &ptr, e)
	}); err != nil {
		return nil, err
	}
	s := &PostgresStore{ptr: ptr}
	runtime.SetFinalizer(s, (*PostgresStore).Close)
	return s, nil
}

// Install creates the store's service table dat_applied if it does not exist.
func (s *PostgresStore) Install() error {
	defer runtime.KeepAlive(s)
	return call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_postgres_store_install(s.ptr, e)
	})
}

// Rows is a statement's result: its column names and its rows, values in
// column order (see the package's value types).
type Rows struct {
	Columns []string
	Values  [][]any
}

// Maps is each row as a map from column name to value.
func (r *Rows) Maps() []map[string]any {
	out := make([]map[string]any, len(r.Values))
	for i, row := range r.Values {
		m := make(map[string]any, len(r.Columns))
		for j, c := range r.Columns {
			m[c] = row[j]
		}
		out[i] = m
	}
	return out
}

// Execute runs one statement in autocommit mode, outside any
// meta-transaction, with params bound to $1..$n by the statement's parameter
// types.
func (s *PostgresStore) Execute(sql string, params ...any) (*Rows, error) {
	defer runtime.KeepAlive(s)
	var cParams *C.char
	if len(params) > 0 {
		text, err := encodeValues(params)
		if err != nil {
			return nil, &Error{Status: StatusInvalidArgument, Message: err.Error()}
		}
		cParams = C.CString(text)
	}
	cSQL := C.CString(sql)
	defer freeC(cSQL, cParams)
	var out *C.char
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_postgres_store_execute(s.ptr, cSQL, cParams, &out, e)
	}); err != nil {
		return nil, err
	}
	return decodeRows(takeString(out))
}

func decodeRows(text string) (*Rows, error) {
	var raw struct {
		Columns []string        `json:"columns"`
		Rows    json.RawMessage `json:"rows"`
	}
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		return nil, fmt.Errorf("rhizodat: a result that is not JSON: %w", err)
	}
	rows, err := decodeValues(raw.Rows)
	if err != nil {
		return nil, fmt.Errorf("rhizodat: a result value: %w", err)
	}
	values := make([][]any, len(rows))
	for i, row := range rows {
		values[i] = row.([]any)
	}
	return &Rows{Columns: raw.Columns, Values: values}, nil
}

// Close frees the store. Calling it again does nothing.
func (s *PostgresStore) Close() {
	s.once.Do(func() {
		C.rhizodat_postgres_store_free(s.ptr)
		runtime.SetFinalizer(s, nil)
	})
}

// Journal records meta-transactions over several stores. Close it when done;
// a coordinator made with it keeps its own reference.
type Journal struct {
	ptr  *C.RhizodatJournal
	once sync.Once
}

// OpenPostgresJournal is the durable journal in PostgreSQL: the table
// dat_journal in schema (or the DSN's schema= parameter), created if missing.
// The schema must exist: a missing one puts the table in public.
func OpenPostgresJournal(dsn, schema string) (*Journal, error) {
	cDSN, cSchema := cString(dsn, false), cString(schema, true)
	defer freeC(cDSN, cSchema)
	var ptr *C.RhizodatJournal
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_postgres_journal_open(cDSN, cSchema, &ptr, e)
	}); err != nil {
		return nil, err
	}
	j := &Journal{ptr: ptr}
	runtime.SetFinalizer(j, (*Journal).Close)
	return j, nil
}

// Close frees the journal. Calling it again does nothing.
func (j *Journal) Close() {
	j.once.Do(func() {
		C.rhizodat_journal_free(j.ptr)
		runtime.SetFinalizer(j, nil)
	})
}
