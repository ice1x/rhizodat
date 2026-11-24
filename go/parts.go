package rhizodat

/*
#include <stdlib.h>
#include "rhizodat.h"
*/
import "C"

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"runtime"
	"sort"
)

// Entity parts: what one store keeps of one entity type. Every write adds a
// version, tagged with its meta-transaction (DatTxn) and its transaction time
// (DatTxTime, a hybrid logical clock); a soft delete is one more version marked
// Deleted, an erase removes every version. The layout is the Python facade's,
// so either reads the other's versions.

// PartColumn is one field of a part and its store type. In PostgreSQL: text,
// boolean, bigint, double precision, numeric, timestamptz, date, uuid, bytea,
// or jsonb (any value, lists and maps included). In drevo: native (strings,
// numbers, booleans), uuid, or json (tagged JSON text). Reference marks a field
// holding entity ids (drevo links its targets).
type PartColumn struct {
	Name      string
	Type      string
	Reference bool
}

// PartSpec is one store's share of one entity type: its type name, its table
// and its fields in order.
type PartSpec struct {
	Type    string
	Table   string
	Columns []PartColumn
}

func (s PartSpec) json() (string, error) {
	columns := make([][3]any, len(s.Columns))
	for i, c := range s.Columns {
		columns[i] = [3]any{c.Name, c.Type, c.Reference}
	}
	text, err := json.Marshal(map[string]any{"type": s.Type, "table": s.Table, "columns": columns})
	return string(text), err
}

// index is the position of field, or -1.
func (s PartSpec) index(field string) int {
	for i, c := range s.Columns {
		if c.Name == field {
			return i
		}
	}
	return -1
}

// PartVersion is one stored version of a part.
type PartVersion struct {
	// DatTxn is the meta-transaction that wrote it; "" for a unit of work's own
	// version, not committed yet.
	DatTxn UUID
	// DatTxTime is its transaction time; nil while not committed.
	DatTxTime *int64
	// Created is the time of the entity's first version; nil for a unit of
	// work's own first version.
	Created *int64
	// Deleted marks a soft delete.
	Deleted bool
	// Values are the fields' values, in the spec's order.
	Values []any
}

// Get is the value of field, nil when the spec has no such field.
func (v *PartVersion) Get(spec PartSpec, field string) any {
	if i := spec.index(field); i >= 0 && i < len(v.Values) {
		return v.Values[i]
	}
	return nil
}

// EntityVersion is a version with the entity it belongs to.
type EntityVersion struct {
	DatID UUID
	PartVersion
}

// OwnPart is what a unit of work holds of one part: Touched is false when it
// did not write it; Version is nil when it erased it last.
type OwnPart struct {
	Touched bool
	Version *PartVersion
}

type rawVersion struct {
	DatID     *string         `json:"dat_id"`
	DatTxn    *string         `json:"dat_txn"`
	DatTxTime *int64          `json:"dat_tx_time"`
	Created   *int64          `json:"created"`
	Deleted   bool            `json:"deleted"`
	Values    json.RawMessage `json:"values"`
}

func (r *rawVersion) version() (*PartVersion, error) {
	values, err := decodeValues(r.Values)
	if err != nil {
		return nil, fmt.Errorf("rhizodat: a part value: %w", err)
	}
	v := &PartVersion{DatTxTime: r.DatTxTime, Created: r.Created, Deleted: r.Deleted, Values: values}
	if r.DatTxn != nil {
		v.DatTxn = UUID(*r.DatTxn)
	}
	return v, nil
}

func decodeJSON(text string, into any) error {
	decoder := json.NewDecoder(bytes.NewReader([]byte(text)))
	if err := decoder.Decode(into); err != nil {
		return fmt.Errorf("rhizodat: an answer that is not JSON: %w", err)
	}
	return nil
}

func decodeVersion(text string) (*PartVersion, error) {
	var raw *rawVersion
	if err := decodeJSON(text, &raw); err != nil || raw == nil {
		return nil, err
	}
	return raw.version()
}

func decodeVersions(text string) ([]EntityVersion, error) {
	var raws []rawVersion
	if err := decodeJSON(text, &raws); err != nil {
		return nil, err
	}
	out := make([]EntityVersion, len(raws))
	for i := range raws {
		v, err := raws[i].version()
		if err != nil {
			return nil, err
		}
		out[i].PartVersion = *v
		if raws[i].DatID != nil {
			out[i].DatID = UUID(*raws[i].DatID)
		}
	}
	return out, nil
}

// partArgs are the C strings every part call takes; free them with freeC.
type partArgs struct{ spec, id *C.char }

func newPartArgs(spec PartSpec, id UUID) (partArgs, error) {
	text, err := spec.json()
	if err != nil {
		return partArgs{}, &Error{Status: StatusInvalidArgument, Message: err.Error()}
	}
	args := partArgs{spec: C.CString(text)}
	if id != "" {
		args.id = C.CString(string(id))
	}
	return args, nil
}

func (a partArgs) free() { freeC(a.spec, a.id) }

// optionalTime is t as a C pointer for the call's duration (nil for none).
func optionalTime(t *int64) *C.int64_t {
	if t == nil {
		return nil
	}
	v := C.int64_t(*t)
	return &v
}

// --- the C ABI's part functions, shared by every store ------------------------------

// partCalls are one store's part functions of the C ABI, bound to its handle.
type partCalls struct {
	install  func(spec *C.char, e **C.char) C.enum_RhizodatStatus
	read     func(spec, id *C.char, knownAt *C.int64_t, out, e **C.char) C.enum_RhizodatStatus
	history  func(spec, id *C.char, out, e **C.char) C.enum_RhizodatStatus
	versions func(spec *C.char, knownAt *C.int64_t, out, e **C.char) C.enum_RhizodatStatus
	expired  func(spec *C.char, before, limit C.int64_t, out, e **C.char) C.enum_RhizodatStatus
}

// unitPartCalls are one store's unit-of-work part functions, bound to the
// unit's and the store's handles.
type unitPartCalls struct {
	write func(spec, id, fields, values *C.char, created *C.int64_t, deleted C.bool, e **C.char) C.enum_RhizodatStatus
	erase func(spec, id *C.char, e **C.char) C.enum_RhizodatStatus
	own   func(spec, id *C.char, out, e **C.char) C.enum_RhizodatStatus
}

func (p partCalls) installPart(spec PartSpec) error {
	args, err := newPartArgs(spec, "")
	if err != nil {
		return err
	}
	defer args.free()
	return call(func(e **C.char) C.enum_RhizodatStatus { return p.install(args.spec, e) })
}

func (p partCalls) readPart(spec PartSpec, id UUID, knownAt *int64) (*PartVersion, error) {
	args, err := newPartArgs(spec, id)
	if err != nil {
		return nil, err
	}
	defer args.free()
	var out *C.char
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return p.read(args.spec, args.id, optionalTime(knownAt), &out, e)
	}); err != nil {
		return nil, err
	}
	return decodeVersion(takeString(out))
}

func (p partCalls) partHistory(spec PartSpec, id UUID) ([]PartVersion, error) {
	args, err := newPartArgs(spec, id)
	if err != nil {
		return nil, err
	}
	defer args.free()
	var out *C.char
	if err := call(func(e **C.char) C.enum_RhizodatStatus { return p.history(args.spec, args.id, &out, e) }); err != nil {
		return nil, err
	}
	versions, err := decodeVersions(takeString(out))
	if err != nil {
		return nil, err
	}
	history := make([]PartVersion, len(versions))
	for i := range versions {
		history[i] = versions[i].PartVersion
	}
	return history, nil
}

func (p partCalls) partVersions(spec PartSpec, knownAt *int64) ([]EntityVersion, error) {
	args, err := newPartArgs(spec, "")
	if err != nil {
		return nil, err
	}
	defer args.free()
	var out *C.char
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return p.versions(args.spec, optionalTime(knownAt), &out, e)
	}); err != nil {
		return nil, err
	}
	return decodeVersions(takeString(out))
}

func (p partCalls) expiredParts(spec PartSpec, deletedBefore, limit int64) ([]UUID, error) {
	args, err := newPartArgs(spec, "")
	if err != nil {
		return nil, err
	}
	defer args.free()
	var out *C.char
	if err := call(func(e **C.char) C.enum_RhizodatStatus {
		return p.expired(args.spec, C.int64_t(deletedBefore), C.int64_t(limit), &out, e)
	}); err != nil {
		return nil, err
	}
	var ids []UUID
	if err := decodeJSON(takeString(out), &ids); err != nil {
		return nil, err
	}
	return ids, nil
}

func (p unitPartCalls) writePart(spec PartSpec, id UUID, values map[string]any, created *int64, deleted bool) error {
	args, err := newPartArgs(spec, id)
	if err != nil {
		return err
	}
	defer args.free()
	fields := make([]string, 0, len(values))
	for f := range values {
		fields = append(fields, f)
	}
	sort.Strings(fields)
	row := make([]any, len(fields))
	for i, f := range fields {
		row[i] = values[f]
	}
	cValues, err := encodeValues(row)
	if err != nil {
		return &Error{Status: StatusInvalidArgument, Message: err.Error()}
	}
	cFields, err := json.Marshal(fields)
	if err != nil {
		return &Error{Status: StatusInvalidArgument, Message: err.Error()}
	}
	f, v := C.CString(string(cFields)), C.CString(cValues)
	defer freeC(f, v)
	return call(func(e **C.char) C.enum_RhizodatStatus {
		return p.write(args.spec, args.id, f, v, optionalTime(created), C.bool(deleted), e)
	})
}

func (p unitPartCalls) erasePart(spec PartSpec, id UUID) error {
	args, err := newPartArgs(spec, id)
	if err != nil {
		return err
	}
	defer args.free()
	return call(func(e **C.char) C.enum_RhizodatStatus { return p.erase(args.spec, args.id, e) })
}

func (p unitPartCalls) ownPart(spec PartSpec, id UUID) (OwnPart, error) {
	args, err := newPartArgs(spec, id)
	if err != nil {
		return OwnPart{}, err
	}
	defer args.free()
	var out *C.char
	if err := call(func(e **C.char) C.enum_RhizodatStatus { return p.own(args.spec, args.id, &out, e) }); err != nil {
		return OwnPart{}, err
	}
	var raw struct {
		Touched bool        `json:"touched"`
		Version *rawVersion `json:"version"`
	}
	if err := decodeJSON(takeString(out), &raw); err != nil {
		return OwnPart{}, err
	}
	own := OwnPart{Touched: raw.Touched}
	if raw.Version != nil {
		v, err := raw.Version.version()
		if err != nil {
			return OwnPart{}, err
		}
		own.Version = v
	}
	return own, nil
}

// WritePart buffers a version of id's part in store: values by field (fields
// left out are null), created the time of the entity's first version (nil
// when this is the first), deleted for a soft delete. It is written at the
// commit, stamped with the meta-transaction.
func (u *Unit) WritePart(store PartStore, spec PartSpec, id UUID, values map[string]any, created *int64, deleted bool) error {
	return store.writePart(u, spec, id, values, created, deleted)
}

// ErasePart buffers the removal of every version of id's part in store
// (committed ones too), at the commit.
func (u *Unit) ErasePart(store PartStore, spec PartSpec, id UUID) error {
	return store.erasePart(u, spec, id)
}

// OwnPart is what this unit of work holds of id's part in store.
func (u *Unit) OwnPart(store PartStore, spec PartSpec, id UUID) (OwnPart, error) {
	return store.ownPart(u, spec, id)
}

// --- PostgreSQL --------------------------------------------------------------------

func (s *PostgresStore) parts() partCalls {
	return partCalls{
		install: func(spec *C.char, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_postgres_store_install_part(s.ptr, spec, e)
		},
		read: func(spec, id *C.char, knownAt *C.int64_t, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_postgres_store_read_part(s.ptr, spec, id, knownAt, out, e)
		},
		history: func(spec, id *C.char, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_postgres_store_part_history(s.ptr, spec, id, out, e)
		},
		versions: func(spec *C.char, knownAt *C.int64_t, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_postgres_store_part_versions(s.ptr, spec, knownAt, out, e)
		},
		expired: func(spec *C.char, before, limit C.int64_t, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_postgres_store_expired_parts(s.ptr, spec, before, limit, out, e)
		},
	}
}

func (s *PostgresStore) unitParts(u *Unit) unitPartCalls {
	return unitPartCalls{
		write: func(spec, id, fields, values *C.char, created *C.int64_t, deleted C.bool, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_unit_postgres_write_part(u.ptr, s.ptr, spec, id, fields, values, created, deleted, e)
		},
		erase: func(spec, id *C.char, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_unit_postgres_erase_part(u.ptr, s.ptr, spec, id, e)
		},
		own: func(spec, id *C.char, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_unit_postgres_own_part(u.ptr, s.ptr, spec, id, out, e)
		},
	}
}

// InstallPart creates the part's table and index (and dat_applied) if missing.
func (s *PostgresStore) InstallPart(spec PartSpec) error {
	defer runtime.KeepAlive(s)
	return s.parts().installPart(spec)
}

// ReadPart is the latest committed version of id's part known at knownAt (any,
// when nil), or nil when there is none.
func (s *PostgresStore) ReadPart(spec PartSpec, id UUID, knownAt *int64) (*PartVersion, error) {
	defer runtime.KeepAlive(s)
	return s.parts().readPart(spec, id, knownAt)
}

// PartHistory is every committed version of id's part, oldest first.
func (s *PostgresStore) PartHistory(spec PartSpec, id UUID) ([]PartVersion, error) {
	defer runtime.KeepAlive(s)
	return s.parts().partHistory(spec, id)
}

// PartVersions is every committed version of every entity's part known at
// knownAt (all, when nil), oldest first.
func (s *PostgresStore) PartVersions(spec PartSpec, knownAt *int64) ([]EntityVersion, error) {
	defer runtime.KeepAlive(s)
	return s.parts().partVersions(spec, knownAt)
}

// ExpiredParts is the entities whose latest version is a soft delete older
// than deletedBefore (a transaction time), at most limit of them.
func (s *PostgresStore) ExpiredParts(spec PartSpec, deletedBefore int64, limit int64) ([]UUID, error) {
	defer runtime.KeepAlive(s)
	return s.parts().expiredParts(spec, deletedBefore, limit)
}

func (s *PostgresStore) partColumnType(t reflect.Type) (string, bool) { return postgresColumnType(t) }
func (s *PostgresStore) installPart(spec PartSpec) error              { return s.InstallPart(spec) }
func (s *PostgresStore) readPart(spec PartSpec, id UUID, knownAt *int64) (*PartVersion, error) {
	return s.ReadPart(spec, id, knownAt)
}
func (s *PostgresStore) partHistory(spec PartSpec, id UUID) ([]PartVersion, error) {
	return s.PartHistory(spec, id)
}
func (s *PostgresStore) expiredParts(spec PartSpec, deletedBefore, limit int64) ([]UUID, error) {
	return s.ExpiredParts(spec, deletedBefore, limit)
}
func (s *PostgresStore) writePart(u *Unit, spec PartSpec, id UUID, values map[string]any, created *int64, deleted bool) error {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(s)
	return s.unitParts(u).writePart(spec, id, values, created, deleted)
}
func (s *PostgresStore) erasePart(u *Unit, spec PartSpec, id UUID) error {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(s)
	return s.unitParts(u).erasePart(spec, id)
}
func (s *PostgresStore) ownPart(u *Unit, spec PartSpec, id UUID) (OwnPart, error) {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(s)
	return s.unitParts(u).ownPart(spec, id)
}

// --- drevo -------------------------------------------------------------------------

func (s *DrevoStore) parts() partCalls {
	return partCalls{
		install: func(spec *C.char, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_drevo_store_install_part(s.ptr, spec, e)
		},
		read: func(spec, id *C.char, knownAt *C.int64_t, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_drevo_store_read_part(s.ptr, spec, id, knownAt, out, e)
		},
		history: func(spec, id *C.char, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_drevo_store_part_history(s.ptr, spec, id, out, e)
		},
		versions: func(spec *C.char, knownAt *C.int64_t, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_drevo_store_part_versions(s.ptr, spec, knownAt, out, e)
		},
		expired: func(spec *C.char, before, limit C.int64_t, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_drevo_store_expired_parts(s.ptr, spec, before, limit, out, e)
		},
	}
}

func (s *DrevoStore) unitParts(u *Unit) unitPartCalls {
	return unitPartCalls{
		write: func(spec, id, fields, values *C.char, created *C.int64_t, deleted C.bool, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_unit_drevo_write_part(u.ptr, s.ptr, spec, id, fields, values, created, deleted, e)
		},
		erase: func(spec, id *C.char, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_unit_drevo_erase_part(u.ptr, s.ptr, spec, id, e)
		},
		own: func(spec, id *C.char, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_unit_drevo_own_part(u.ptr, s.ptr, spec, id, out, e)
		},
	}
}

// InstallPart creates the index the part's versions need if missing. The
// part's Table is the versions' label.
func (s *DrevoStore) InstallPart(spec PartSpec) error {
	defer runtime.KeepAlive(s)
	return s.parts().installPart(spec)
}

// ReadPart is the latest committed version of id's part known at knownAt (any,
// when nil), or nil when there is none.
func (s *DrevoStore) ReadPart(spec PartSpec, id UUID, knownAt *int64) (*PartVersion, error) {
	defer runtime.KeepAlive(s)
	return s.parts().readPart(spec, id, knownAt)
}

// PartHistory is every committed version of id's part, oldest first.
func (s *DrevoStore) PartHistory(spec PartSpec, id UUID) ([]PartVersion, error) {
	defer runtime.KeepAlive(s)
	return s.parts().partHistory(spec, id)
}

// PartVersions is every committed version of every entity's part known at
// knownAt (all, when nil), oldest first.
func (s *DrevoStore) PartVersions(spec PartSpec, knownAt *int64) ([]EntityVersion, error) {
	defer runtime.KeepAlive(s)
	return s.parts().partVersions(spec, knownAt)
}

// ExpiredParts is the entities whose latest version is a soft delete older
// than deletedBefore, at most limit of them.
func (s *DrevoStore) ExpiredParts(spec PartSpec, deletedBefore int64, limit int64) ([]UUID, error) {
	defer runtime.KeepAlive(s)
	return s.parts().expiredParts(spec, deletedBefore, limit)
}

func (s *DrevoStore) partColumnType(t reflect.Type) (string, bool) { return drevoFieldKind(t) }
func (s *DrevoStore) installPart(spec PartSpec) error              { return s.InstallPart(spec) }
func (s *DrevoStore) readPart(spec PartSpec, id UUID, knownAt *int64) (*PartVersion, error) {
	return s.ReadPart(spec, id, knownAt)
}
func (s *DrevoStore) partHistory(spec PartSpec, id UUID) ([]PartVersion, error) {
	return s.PartHistory(spec, id)
}
func (s *DrevoStore) expiredParts(spec PartSpec, deletedBefore, limit int64) ([]UUID, error) {
	return s.ExpiredParts(spec, deletedBefore, limit)
}
func (s *DrevoStore) writePart(u *Unit, spec PartSpec, id UUID, values map[string]any, created *int64, deleted bool) error {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(s)
	return s.unitParts(u).writePart(spec, id, values, created, deleted)
}
func (s *DrevoStore) erasePart(u *Unit, spec PartSpec, id UUID) error {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(s)
	return s.unitParts(u).erasePart(spec, id)
}
func (s *DrevoStore) ownPart(u *Unit, spec PartSpec, id UUID) (OwnPart, error) {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(s)
	return s.unitParts(u).ownPart(spec, id)
}

// drevoFieldKind is how drevo keeps a Go type, as the Python facade keeps its
// annotations: strings, numbers and booleans as themselves, ids as their text,
// anything else (times, decimals, lists, maps) as tagged JSON.
func drevoFieldKind(t reflect.Type) (string, bool) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if _, ok := postgresColumnType(t); !ok {
		return "", false
	}
	switch t {
	case reflect.TypeOf(UUID("")):
		return "uuid", true
	case reflect.TypeOf(Decimal("")), reflect.TypeOf(Date("")), reflect.TypeOf(NaiveDateTime("")):
		return "json", true
	}
	switch t.Kind() {
	case reflect.String, reflect.Bool, reflect.Float32, reflect.Float64,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "native", true
	}
	return "json", true
}

// --- ClickHouse --------------------------------------------------------------------

func (s *ClickHouseStore) parts() partCalls {
	return partCalls{
		install: func(spec *C.char, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_clickhouse_store_install_part(s.ptr, spec, e)
		},
		read: func(spec, id *C.char, knownAt *C.int64_t, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_clickhouse_store_read_part(s.ptr, spec, id, knownAt, out, e)
		},
		history: func(spec, id *C.char, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_clickhouse_store_part_history(s.ptr, spec, id, out, e)
		},
		versions: func(spec *C.char, knownAt *C.int64_t, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_clickhouse_store_part_versions(s.ptr, spec, knownAt, out, e)
		},
		expired: func(spec *C.char, before, limit C.int64_t, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_clickhouse_store_expired_parts(s.ptr, spec, before, limit, out, e)
		},
	}
}

func (s *ClickHouseStore) unitParts(u *Unit) unitPartCalls {
	return unitPartCalls{
		write: func(spec, id, fields, values *C.char, created *C.int64_t, deleted C.bool, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_unit_clickhouse_write_part(u.ptr, s.ptr, spec, id, fields, values, created, deleted, e)
		},
		erase: func(spec, id *C.char, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_unit_clickhouse_erase_part(u.ptr, s.ptr, spec, id, e)
		},
		own: func(spec, id *C.char, out, e **C.char) C.enum_RhizodatStatus {
			return C.rhizodat_unit_clickhouse_own_part(u.ptr, s.ptr, spec, id, out, e)
		},
	}
}

// InstallPart creates the part's table if missing.
func (s *ClickHouseStore) InstallPart(spec PartSpec) error {
	defer runtime.KeepAlive(s)
	return s.parts().installPart(spec)
}

// ReadPart is the latest committed version of id's part known at knownAt (any,
// when nil), or nil when there is none.
func (s *ClickHouseStore) ReadPart(spec PartSpec, id UUID, knownAt *int64) (*PartVersion, error) {
	defer runtime.KeepAlive(s)
	return s.parts().readPart(spec, id, knownAt)
}

// PartHistory is every committed version of id's part, oldest first.
func (s *ClickHouseStore) PartHistory(spec PartSpec, id UUID) ([]PartVersion, error) {
	defer runtime.KeepAlive(s)
	return s.parts().partHistory(spec, id)
}

// PartVersions is every committed version of every entity's part known at
// knownAt (all, when nil), oldest first.
func (s *ClickHouseStore) PartVersions(spec PartSpec, knownAt *int64) ([]EntityVersion, error) {
	defer runtime.KeepAlive(s)
	return s.parts().partVersions(spec, knownAt)
}

// ExpiredParts is the entities whose latest version is a soft delete older
// than deletedBefore, at most limit of them.
func (s *ClickHouseStore) ExpiredParts(spec PartSpec, deletedBefore int64, limit int64) ([]UUID, error) {
	defer runtime.KeepAlive(s)
	return s.parts().expiredParts(spec, deletedBefore, limit)
}

func (s *ClickHouseStore) partColumnType(t reflect.Type) (string, bool) {
	return clickhouseColumnType(t)
}
func (s *ClickHouseStore) installPart(spec PartSpec) error { return s.InstallPart(spec) }
func (s *ClickHouseStore) readPart(spec PartSpec, id UUID, knownAt *int64) (*PartVersion, error) {
	return s.ReadPart(spec, id, knownAt)
}
func (s *ClickHouseStore) partHistory(spec PartSpec, id UUID) ([]PartVersion, error) {
	return s.PartHistory(spec, id)
}
func (s *ClickHouseStore) expiredParts(spec PartSpec, deletedBefore, limit int64) ([]UUID, error) {
	return s.ExpiredParts(spec, deletedBefore, limit)
}
func (s *ClickHouseStore) writePart(u *Unit, spec PartSpec, id UUID, values map[string]any, created *int64, deleted bool) error {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(s)
	return s.unitParts(u).writePart(spec, id, values, created, deleted)
}
func (s *ClickHouseStore) erasePart(u *Unit, spec PartSpec, id UUID) error {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(s)
	return s.unitParts(u).erasePart(spec, id)
}
func (s *ClickHouseStore) ownPart(u *Unit, spec PartSpec, id UUID) (OwnPart, error) {
	defer runtime.KeepAlive(u)
	defer runtime.KeepAlive(s)
	return s.unitParts(u).ownPart(spec, id)
}

// clickhouseColumnType is the ClickHouse column of a Go type, as the Python
// facade types its annotations: strings, integers, floats, booleans, ids,
// times and dates natively, anything else as tagged JSON in a String.
func clickhouseColumnType(t reflect.Type) (string, bool) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if _, ok := postgresColumnType(t); !ok {
		return "", false
	}
	switch t {
	case timeType:
		return "DateTime64(6, 'UTC')", true
	case reflect.TypeOf(UUID("")):
		return "UUID", true
	case reflect.TypeOf(Date("")):
		return "Date32", true
	case reflect.TypeOf(Decimal("")), reflect.TypeOf(NaiveDateTime("")):
		return "json", true
	}
	switch t.Kind() {
	case reflect.String:
		return "String", true
	case reflect.Bool:
		return "Bool", true
	case reflect.Float32, reflect.Float64:
		return "Float64", true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "Int64", true
	}
	return "json", true
}
