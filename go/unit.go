package rhizodat

/*
#include <stdlib.h>
#include "rhizodat.h"
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime"
	"sync"
)

// Unit is a unit of work: writes buffered per store, committed together by a
// Coordinator. It keeps its writes after a commit. Close it when done.
type Unit struct {
	ptr  *C.RhizodatUnit
	once sync.Once
}

// NewUnit is an empty unit of work.
func NewUnit() (*Unit, error) {
	var ptr *C.RhizodatUnit
	if err := call(func(e **C.char) C.enum_RhizodatStatus { return C.rhizodat_unit_new(&ptr, e) }); err != nil {
		return nil, err
	}
	u := &Unit{ptr: ptr}
	runtime.SetFinalizer(u, (*Unit).Close)
	return u, nil
}

// Insert buffers rows for table in a PostgreSQL store, one value per column.
// They are written at the commit with the meta-transaction's dat_txn and
// dat_tx_time (leave those columns out).
func (u *Unit) Insert(store *PostgresStore, table string, columns []string, rows [][]any) error {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(store)
	cols, err := json.Marshal(columns)
	if err != nil {
		return err
	}
	list := make([]any, len(rows))
	for i, r := range rows {
		list[i] = r
	}
	values, err := encodeValues(list)
	if err != nil {
		return &Error{Status: StatusInvalidArgument, Message: err.Error()}
	}
	cTable, cCols, cRows := C.CString(table), C.CString(string(cols)), C.CString(values)
	defer freeC(cTable, cCols, cRows)
	return call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_unit_postgres_insert(u.ptr, store.ptr, cTable, cCols, cRows, e)
	})
}

// Close frees the unit of work. Calling it again does nothing.
func (u *Unit) Close() {
	u.once.Do(func() {
		C.rhizodat_unit_free(u.ptr)
		runtime.SetFinalizer(u, nil)
	})
}

// Committed is a successful commit.
type Committed struct {
	// DatTxn is the meta-transaction's id.
	DatTxn UUID
	// DatTxTime is its transaction time (0 when there was nothing to commit).
	DatTxTime int64
	// Pending is set when the decision is durable but recovery still finishes
	// some stores. Do not re-run the unit of work.
	Pending bool
}

// Sentinels for errors.Is on commit errors.
var (
	ErrAborted = errors.New("rhizodat: meta-transaction aborted")
	ErrInDoubt = errors.New("rhizodat: meta-transaction in doubt")
)

// AbortedError is a commit that wrote nothing. Retryable says a re-run may
// succeed.
type AbortedError struct {
	DatTxn    UUID
	Retryable bool
	Message   string
}

func (e *AbortedError) Error() string {
	return fmt.Sprintf("rhizodat: meta-transaction %s aborted: %s", e.DatTxn, e.Message)
}

// Is matches ErrAborted.
func (e *AbortedError) Is(target error) bool { return target == ErrAborted }

// InDoubtError is a commit whose outcome is unknown: recovery resolves it
// from the journal. Do not re-run the unit of work, it may have committed.
type InDoubtError struct {
	DatTxn  UUID
	Message string
}

func (e *InDoubtError) Error() string {
	return fmt.Sprintf("rhizodat: meta-transaction %s in doubt: %s", e.DatTxn, e.Message)
}

// Is matches ErrInDoubt.
func (e *InDoubtError) Is(target error) bool { return target == ErrInDoubt }

// Coordinator commits units of work, through a journal when they write to
// several stores. It is safe for concurrent use. Close it when done.
type Coordinator struct {
	ptr  *C.RhizodatCoordinator
	once sync.Once
}

// NewCoordinator commits through journal; nil for none (then a unit of work
// may write to one store only).
func NewCoordinator(journal *Journal) (*Coordinator, error) {
	var cJournal *C.RhizodatJournal
	if journal != nil {
		cJournal = journal.ptr
		defer runtime.KeepAlive(journal)
	}
	var ptr *C.RhizodatCoordinator
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return C.rhizodat_coordinator_new(cJournal, &ptr, e)
	}); err != nil {
		return nil, err
	}
	c := &Coordinator{ptr: ptr}
	runtime.SetFinalizer(c, (*Coordinator).Close)
	return c, nil
}

type outcome struct {
	Status    string `json:"status"`
	DatTxn    string `json:"dat_txn"`
	DatTxTime *int64 `json:"dat_tx_time"`
	Retryable bool   `json:"retryable"`
	Error     string `json:"error"`
}

// Commit commits unit as one new meta-transaction: every store's writes, or
// none of them. A failure is an *AbortedError, an *InDoubtError, or an *Error
// for a unit of work the core refuses.
func (c *Coordinator) Commit(unit *Unit) (Committed, error) {
	defer runtime.KeepAlive(c)
	defer runtime.KeepAlive(unit)
	var out, message *C.char
	status := Status(C.rhizodat_coordinator_commit(c.ptr, unit.ptr, &out, &message))
	text := takeString(message)
	var o outcome
	if out != nil {
		if err := json.Unmarshal([]byte(takeString(out)), &o); err != nil {
			return Committed{}, fmt.Errorf("rhizodat: an outcome that is not JSON: %w", err)
		}
	}
	switch status {
	case StatusOK:
		done := Committed{DatTxn: UUID(o.DatTxn), Pending: o.Status == "pending"}
		if o.DatTxTime != nil {
			done.DatTxTime = *o.DatTxTime
		}
		return done, nil
	case StatusAborted:
		return Committed{}, &AbortedError{DatTxn: UUID(o.DatTxn), Retryable: o.Retryable, Message: text}
	case StatusInDoubt:
		return Committed{}, &InDoubtError{DatTxn: UUID(o.DatTxn), Message: text}
	}
	return Committed{}, statusError(status, text)
}

// Transactional builds a unit of work with build and commits it, re-running
// both up to attempts times in all while the commit aborts retryably. An error
// from build is returned as it is, with nothing written.
func (c *Coordinator) Transactional(attempts int, build func(*Unit) error) (Committed, error) {
	if attempts < 1 {
		return Committed{}, &Error{Status: StatusInvalidArgument, Message: "attempts must be at least 1"}
	}
	var last error
	for i := 0; i < attempts; i++ {
		unit, err := NewUnit()
		if err != nil {
			return Committed{}, err
		}
		if err := build(unit); err != nil {
			unit.Close()
			return Committed{}, err
		}
		done, err := c.Commit(unit)
		unit.Close()
		var aborted *AbortedError
		if err == nil || !errors.As(err, &aborted) || !aborted.Retryable {
			return done, err
		}
		last = err
	}
	return Committed{}, last
}

// Close frees the coordinator. Calling it again does nothing.
func (c *Coordinator) Close() {
	c.once.Do(func() {
		C.rhizodat_coordinator_free(c.ptr)
		runtime.SetFinalizer(c, nil)
	})
}
