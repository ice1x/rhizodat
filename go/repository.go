package rhizodat

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"
)

// Entity repositories: an entity type is a Go struct that embeds Entity and
// maps each field to a store with a tag,
//
//	type Issue struct {
//		rhizodat.Entity
//		Title  string   `rhizodat:"title,store=tracker"`
//		Labels []string `rhizodat:"labels,store=search"`
//	}
//
// Each store keeps its share of the entity (a part, see PartSpec) and every
// version of it; a write adds a version of every part in one meta-transaction,
// a read assembles the parts of one version. The tables are the Python
// facade's (same names, columns and encoding), so a type declared alike in
// both — same type name, field names and stores — is shared by them. Fields
// without a rhizodat tag are not stored.

// Entity is what every entity carries: its id and the times of its versions.
// Embed it in an entity type.
type Entity struct {
	// DatID is the entity's id; Add makes a new UUIDv7 when it is empty.
	DatID UUID
	// DatCreatedAt is when its first version was committed.
	DatCreatedAt time.Time
	// DatUpdatedAt is when this version was committed.
	DatUpdatedAt time.Time
	// DatDeletedAt is when it was soft-deleted (zero when it is live).
	DatDeletedAt time.Time
	// DatVersion is the transaction time of the version this was read (or
	// written) at; 0 for an entity built by hand. Update and Save of an entity
	// with a version write only if it is still the latest, else fail with
	// ErrVersionConflict; with 0 the last writer wins.
	DatVersion int64
}

// Ref is a reference to another entity of type T, by its id: stored as the
// id (a uuid column, the id's text in drevo and Iceberg), and in drevo each
// version links the entities it refers to. The zero Ref refers to nothing and
// is stored as null.
type Ref[T any] struct {
	DatID UUID
}

func (r Ref[T]) refID() UUID { return r.DatID }

// RefTo is a reference to target, an entity (a struct embedding Entity).
func RefTo[T any](target *T) Ref[T] {
	id := reflect.ValueOf(target).Elem().FieldByName("DatID")
	if !id.IsValid() || id.Type() != reflect.TypeOf(UUID("")) {
		return Ref[T]{}
	}
	return Ref[T]{DatID: UUID(id.String())}
}

type reference interface{ refID() UUID }

var referenceType = reflect.TypeOf((*reference)(nil)).Elem()

// isRef is whether t is a Ref.
func isRef(t reflect.Type) bool { return t.Kind() == reflect.Struct && t.Implements(referenceType) }

// holdsRefs is whether values of t hold references (a Ref, or a list or map of them).
func holdsRefs(t reflect.Type) bool {
	switch t.Kind() {
	case reflect.Pointer, reflect.Slice, reflect.Array:
		return holdsRefs(t.Elem())
	case reflect.Map:
		return holdsRefs(t.Elem())
	}
	return isRef(t)
}

// Validity is the period an entity's version holds for (valid time), from
// ValidFrom (inclusive) to ValidTo (exclusive); a zero end is open. Embed it in
// an entity type to make it bitemporal: every part stores the period
// (valid_from and valid_to, as the Python facade's valid_time types do), and a
// read as of a moment returns, among the versions known then, the latest one
// whose period holds at it.
type Validity struct {
	ValidFrom time.Time
	ValidTo   time.Time
}

var validityType = reflect.TypeOf(Validity{})

// Errors of entity writes, for errors.Is.
var (
	// ErrEntityExists is an Add of an entity that has versions (live or
	// soft-deleted), or a Save over a soft delete.
	ErrEntityExists = errors.New("rhizodat: entity exists")
	// ErrEntityNotFound is a write to an entity with no live version.
	ErrEntityNotFound = errors.New("rhizodat: entity not found")
	// ErrVersionConflict is a write based on a version of an entity that is
	// no longer the latest (a *VersionConflictError). Transactional does not
	// re-run it: read the entity again and redo the change.
	ErrVersionConflict = errors.New("rhizodat: version conflict")
	// ErrNotRestorable is a Restore of an entity that is not soft-deleted, or
	// left its window.
	ErrNotRestorable = errors.New("rhizodat: entity not restorable")
)

// PartStore is a store that holds entity parts. *PostgresStore is one.
type PartStore interface {
	// Name is the store's name, the one entity tags refer to.
	Name() string
	partColumnType(t reflect.Type) (string, bool)
	partTable(table string) string
	installPart(spec PartSpec) error
	readPart(spec PartSpec, id UUID, knownAt *int64) (*PartVersion, error)
	partHistory(spec PartSpec, id UUID) ([]PartVersion, error)
	expiredParts(spec PartSpec, deletedBefore, limit int64) ([]UUID, error)
	writePart(u *Unit, spec PartSpec, id UUID, values map[string]any, created *int64, deleted bool) error
	erasePart(u *Unit, spec PartSpec, id UUID) error
	ownPart(u *Unit, spec PartSpec, id UUID) (OwnPart, error)
	lockPart(u *Unit, spec PartSpec, id UUID, expected *int64) error
}

// VersionConflictError is a write based on version Expected of an entity
// whose latest live version is Actual (nil: it has none). It matches
// ErrVersionConflict.
type VersionConflictError struct {
	TypeName string
	DatID    UUID
	Expected int64
	Actual   *int64
}

func (e *VersionConflictError) Error() string {
	state := "it is deleted"
	if e.Actual != nil {
		state = fmt.Sprintf("the latest is %d", *e.Actual)
	}
	return fmt.Sprintf("rhizodat: %s %s: written from version %d, but %s", e.TypeName, e.DatID, e.Expected, state)
}

// Is matches ErrVersionConflict.
func (e *VersionConflictError) Is(target error) bool { return target == ErrVersionConflict }

// EntityOptions declares an entity type.
type EntityOptions struct {
	// Type is the type name (required), e.g. "tracker.Issue"; the parts'
	// tables are named after it (tracker_issue).
	Type string
	// SoftDelete, when positive, makes Delete hide the entity for this window
	// (Restore brings it back, PurgeExpired erases it after); zero erases it.
	SoftDelete time.Duration
	// Attempts is how many times a write is run while it aborts retryably
	// (default 3).
	Attempts int
}

// ReadOptions are a read's moments. Without valid time, AsOf and KnownAt name
// the same moment of knowledge (a transaction time): give one of them.
type ReadOptions struct {
	AsOf           *time.Time
	KnownAt        *time.Time
	IncludeDeleted bool
}

// --- time ------------------------------------------------------------------------

const (
	logicalBits      = 16
	consistentReads  = 8
	defaultAttempts  = 3
	hlcMillisecond   = int64(1) << logicalBits
	maxLogicalInAtMs = hlcMillisecond - 1
)

// hlcBound is the largest transaction time within at's millisecond: reads as
// of a moment include it.
func hlcBound(at time.Time) int64 { return at.UnixMilli()<<logicalBits | maxLogicalInAtMs }

// hlcTime is the wall-clock time of a transaction time, in UTC.
func hlcTime(ts int64) time.Time { return time.UnixMilli(ts >> logicalBits).UTC() }

func hlcNow() int64 { return time.Now().UnixMilli() << logicalBits }

// --- the type ----------------------------------------------------------------------

type mappedField struct {
	name  string
	store string
	index []int
}

type kind struct {
	typeName string
	fields   []mappedField
	stores   []string // in declaration order
	specs    map[string]PartSpec
	entity   []int // the index of the embedded Entity
	validity []int // the index of the embedded Validity; nil without valid time
	soft     time.Duration
}

var (
	entityType   = reflect.TypeOf(Entity{})
	notIdent     = regexp.MustCompile(`[^a-z0-9_]`)
	timeType     = reflect.TypeOf(time.Time{})
	bytesType    = reflect.TypeOf([]byte(nil))
	mapValueType = reflect.TypeOf(Map(nil))
)

// partTable is the table of a type name, as the Python facade derives it:
// "tracker.Issue" is tracker_issue.
func partTable(typeName string) string {
	name := notIdent.ReplaceAllString(strings.ToLower(typeName), "_")
	if name == "" || name[0] < 'a' || name[0] > 'z' {
		name = "e_" + name
	}
	if len(name) > 63 {
		name = name[:63]
	}
	return name
}

func newKind(t reflect.Type, stores map[string]PartStore, options EntityOptions) (*kind, error) {
	if options.Type == "" {
		return nil, fmt.Errorf("rhizodat: %v needs a type name (EntityOptions.Type)", t)
	}
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("rhizodat: %s: an entity type is a struct, not %v", options.Type, t)
	}
	k := &kind{typeName: options.Type, specs: map[string]PartSpec{}, soft: options.SoftDelete}
	seen := map[string]bool{}
	columns := map[string][]PartColumn{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Anonymous && f.Type == entityType {
			k.entity = f.Index
			continue
		}
		if f.Anonymous && f.Type == validityType {
			k.validity = f.Index
			continue
		}
		tag, ok := f.Tag.Lookup("rhizodat")
		if !ok {
			continue
		}
		name, store, err := parseTag(tag)
		if err != nil {
			return nil, fmt.Errorf("rhizodat: %s.%s: %w", options.Type, f.Name, err)
		}
		if !f.IsExported() {
			return nil, fmt.Errorf("rhizodat: %s.%s: a mapped field must be exported", options.Type, f.Name)
		}
		if name == "valid_from" || name == "valid_to" {
			return nil, fmt.Errorf("rhizodat: %s: %q is the validity's (embed rhizodat.Validity)", options.Type, name)
		}
		if seen[name] {
			return nil, fmt.Errorf("rhizodat: %s: field %q is mapped twice", options.Type, name)
		}
		seen[name] = true
		s, ok := stores[store]
		if !ok {
			return nil, fmt.Errorf("rhizodat: %s.%s: no store named %q among the repository's", options.Type, name, store)
		}
		column, ok := s.partColumnType(f.Type)
		if !ok {
			return nil, fmt.Errorf("rhizodat: %s.%s: a %v cannot be stored", options.Type, name, f.Type)
		}
		if _, known := columns[store]; !known {
			k.stores = append(k.stores, store)
		}
		columns[store] = append(columns[store], PartColumn{Name: name, Type: column, Reference: holdsRefs(f.Type)})
		k.fields = append(k.fields, mappedField{name: name, store: store, index: f.Index})
	}
	if k.entity == nil {
		return nil, fmt.Errorf("rhizodat: %s: %v must embed rhizodat.Entity", options.Type, t)
	}
	if len(k.fields) == 0 {
		return nil, fmt.Errorf("rhizodat: %s: no field is mapped to a store", options.Type)
	}
	for _, store := range k.stores {
		if k.validity != nil { // every part carries the period its version holds for
			column, _ := stores[store].partColumnType(reflect.TypeOf((*time.Time)(nil)))
			columns[store] = append(columns[store],
				PartColumn{Name: "valid_from", Type: column}, PartColumn{Name: "valid_to", Type: column})
		}
		table := stores[store].partTable(partTable(options.Type))
		k.specs[store] = PartSpec{Type: options.Type, Table: table, Columns: columns[store]}
	}
	return k, nil
}

func parseTag(tag string) (name, store string, err error) {
	parts := strings.Split(tag, ",")
	name = strings.TrimSpace(parts[0])
	for _, option := range parts[1:] {
		if s, ok := strings.CutPrefix(strings.TrimSpace(option), "store="); ok {
			store = s
		}
	}
	if name == "" || store == "" {
		return "", "", fmt.Errorf(`the tag %q needs a name and a store: rhizodat:"field,store=name"`, tag)
	}
	return name, store, nil
}

// split is obj's values by store, as stored, with its period in each part.
func (k *kind) split(obj reflect.Value) (map[string]map[string]any, error) {
	parts := make(map[string]map[string]any, len(k.stores))
	for _, store := range k.stores {
		parts[store] = map[string]any{}
	}
	for _, f := range k.fields {
		parts[f.store][f.name] = storable(obj.FieldByIndex(f.index))
	}
	if k.validity != nil {
		v := obj.FieldByIndex(k.validity).Interface().(Validity)
		if !v.ValidFrom.IsZero() && !v.ValidTo.IsZero() && !v.ValidFrom.Before(v.ValidTo) {
			return nil, fmt.Errorf("rhizodat: %s: ValidFrom must be before ValidTo (%s >= %s)",
				k.typeName, v.ValidFrom.Format(time.RFC3339), v.ValidTo.Format(time.RFC3339))
		}
		for _, values := range parts {
			values["valid_from"], values["valid_to"] = optionalTimeValue(v.ValidFrom), optionalTimeValue(v.ValidTo)
		}
	}
	return parts, nil
}

func optionalTimeValue(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}

// period is the validity of a snapshot's version.
func (k *kind) period(s snapshot) Validity {
	v, spec := k.first(s), k.specs[k.stores[0]]
	from, _ := v.Get(spec, "valid_from").(time.Time)
	to, _ := v.Get(spec, "valid_to").(time.Time)
	return Validity{ValidFrom: from, ValidTo: to}
}

// holds is whether the version's period contains at.
func (k *kind) holds(s snapshot, at time.Time) bool {
	p := k.period(s)
	return (p.ValidFrom.IsZero() || !at.Before(p.ValidFrom)) && (p.ValidTo.IsZero() || at.Before(p.ValidTo))
}

// snapshot is the parts of one version of one entity, by store.
type snapshot map[string]*PartVersion

func (k *kind) first(s snapshot) *PartVersion { return s[k.stores[0]] }

// values is a snapshot's values by store, to write them again.
func (k *kind) values(s snapshot) map[string]map[string]any {
	parts := make(map[string]map[string]any, len(s))
	for store, v := range s {
		spec := k.specs[store]
		values := make(map[string]any, len(spec.Columns))
		for i, c := range spec.Columns {
			values[c.Name] = v.Values[i]
		}
		parts[store] = values
	}
	return parts
}

func (k *kind) assemble(id UUID, s snapshot, into reflect.Value) error {
	v := k.first(s)
	e := Entity{DatID: id}
	if v.Created != nil {
		e.DatCreatedAt = hlcTime(*v.Created)
	}
	if v.DatTxTime != nil {
		e.DatUpdatedAt = hlcTime(*v.DatTxTime)
		e.DatVersion = *v.DatTxTime
		if v.Deleted {
			e.DatDeletedAt = e.DatUpdatedAt
		}
	}
	into.FieldByIndex(k.entity).Set(reflect.ValueOf(e))
	if k.validity != nil {
		into.FieldByIndex(k.validity).Set(reflect.ValueOf(k.period(s)))
	}
	for _, f := range k.fields {
		value := s[f.store].Get(k.specs[f.store], f.name)
		if err := assign(into.FieldByIndex(f.index), value); err != nil {
			return fmt.Errorf("rhizodat: %s.%s: %w", k.typeName, f.name, err)
		}
	}
	return nil
}

// --- values ------------------------------------------------------------------------

// storable is a field's value as the package's value types carry it. An empty
// UUID, Decimal or Date (the zero value) is null: it has no value to parse.
func storable(v reflect.Value) any {
	if isRef(v.Type()) {
		if id := v.Interface().(reference).refID(); id != "" {
			return id
		}
		return nil
	}
	switch v.Type() {
	case reflect.TypeOf(UUID("")), reflect.TypeOf(Decimal("")), reflect.TypeOf(Date("")):
		if v.String() == "" {
			return nil
		}
	}
	switch v.Kind() {
	case reflect.Pointer, reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return storable(v.Elem())
	case reflect.Map:
		if v.Type() == mapValueType || v.IsNil() {
			if v.IsNil() {
				return nil
			}
			return v.Interface()
		}
		m := make(map[string]any, v.Len())
		for it := v.MapRange(); it.Next(); {
			m[it.Key().String()] = storable(it.Value())
		}
		return m
	case reflect.Slice:
		if v.Type() == bytesType || v.Type() == mapValueType {
			return v.Interface()
		}
		if v.IsNil() {
			return nil
		}
		fallthrough
	case reflect.Array:
		list := make([]any, v.Len())
		for i := range list {
			list[i] = storable(v.Index(i))
		}
		return list
	}
	return v.Interface()
}

// assign sets dst to a stored value.
func assign(dst reflect.Value, src any) error {
	if src == nil {
		dst.Set(reflect.Zero(dst.Type()))
		return nil
	}
	t := dst.Type()
	if s := reflect.ValueOf(src); s.Type() == t {
		dst.Set(s)
		return nil
	}
	if isRef(t) {
		if s := reflect.ValueOf(src); s.Kind() == reflect.String {
			dst.Field(0).SetString(s.String())
			return nil
		}
	}
	switch t.Kind() {
	case reflect.Pointer:
		elem := reflect.New(t.Elem())
		if err := assign(elem.Elem(), src); err != nil {
			return err
		}
		dst.Set(elem)
		return nil
	case reflect.Interface:
		dst.Set(reflect.ValueOf(src))
		return nil
	case reflect.String:
		if s := reflect.ValueOf(src); s.Kind() == reflect.String {
			dst.SetString(s.String())
			return nil
		}
	case reflect.Bool:
		if b, ok := src.(bool); ok {
			dst.SetBool(b)
			return nil
		}
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		if n, ok := src.(int64); ok && !dst.OverflowInt(n) {
			dst.SetInt(n)
			return nil
		}
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		if n, ok := src.(int64); ok && n >= 0 && !dst.OverflowUint(uint64(n)) {
			dst.SetUint(uint64(n))
			return nil
		}
	case reflect.Float32, reflect.Float64:
		switch n := src.(type) {
		case float64:
			dst.SetFloat(n)
			return nil
		case int64:
			dst.SetFloat(float64(n))
			return nil
		}
	case reflect.Slice, reflect.Array:
		if list, ok := src.([]any); ok {
			out := dst
			if t.Kind() == reflect.Slice {
				out = reflect.MakeSlice(t, len(list), len(list))
			} else if len(list) != t.Len() {
				return fmt.Errorf("%d items for a %v", len(list), t)
			}
			for i, item := range list {
				if err := assign(out.Index(i), item); err != nil {
					return fmt.Errorf("item %d: %w", i, err)
				}
			}
			dst.Set(out)
			return nil
		}
	case reflect.Map:
		if m, ok := src.(Map); ok && t.Key().Kind() == reflect.String {
			out := reflect.MakeMapWithSize(t, len(m))
			for _, p := range m {
				value := reflect.New(t.Elem()).Elem()
				if err := assign(value, p.Value); err != nil {
					return fmt.Errorf("key %q: %w", p.Key, err)
				}
				out.SetMapIndex(reflect.ValueOf(p.Key).Convert(t.Key()), value)
			}
			dst.Set(out)
			return nil
		}
	}
	return fmt.Errorf("a stored %T does not fit a %v", src, t)
}

// postgresColumnType is the PostgreSQL column of a Go type, as the Python
// facade types its annotations (jsonb for lists and maps).
func postgresColumnType(t reflect.Type) (string, bool) {
	if t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if isRef(t) {
		return "uuid", true
	}
	switch t {
	case timeType:
		return "timestamptz", true
	case reflect.TypeOf(UUID("")):
		return "uuid", true
	case reflect.TypeOf(Decimal("")):
		return "numeric", true
	case reflect.TypeOf(Date("")):
		return "date", true
	case bytesType:
		return "bytea", true
	case mapValueType:
		return "jsonb", true
	}
	switch t.Kind() {
	case reflect.String:
		return "text", true
	case reflect.Bool:
		return "boolean", true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return "bigint", true
	case reflect.Float32, reflect.Float64:
		return "double precision", true
	case reflect.Slice, reflect.Array:
		_, ok := jsonColumn(t.Elem())
		return "jsonb", ok
	case reflect.Map:
		_, ok := jsonColumn(t.Elem())
		return "jsonb", ok && t.Key().Kind() == reflect.String
	case reflect.Interface:
		return "jsonb", t.NumMethod() == 0
	}
	return "", false
}

// jsonColumn is whether a list's items or a map's values can be stored.
func jsonColumn(t reflect.Type) (string, bool) {
	if t == timeType {
		return "jsonb", true
	}
	if t.Kind() == reflect.Struct && !isRef(t) {
		return "", false
	}
	return postgresColumnType(t)
}

// --- the repository ----------------------------------------------------------------

// Repository reads and writes one entity type: committed reads, and one
// meta-transaction per write (re-run while it aborts retryably). It is safe
// for concurrent use.
type Repository[T any] struct {
	coordinator *Coordinator
	stores      map[string]PartStore
	kind        *kind
	attempts    int
}

// NewRepository is the repository of T over stores, committing through
// coordinator. T must embed Entity and map its fields to the stores' names.
func NewRepository[T any](coordinator *Coordinator, stores []PartStore, options EntityOptions) (*Repository[T], error) {
	byName := make(map[string]PartStore, len(stores))
	for _, s := range stores {
		byName[s.Name()] = s
	}
	k, err := newKind(reflect.TypeOf((*T)(nil)).Elem(), byName, options)
	if err != nil {
		return nil, err
	}
	attempts := options.Attempts
	if attempts == 0 {
		attempts = defaultAttempts
	}
	return &Repository[T]{coordinator: coordinator, stores: byName, kind: k, attempts: attempts}, nil
}

// Install creates every part's table if missing. Idempotent.
func (r *Repository[T]) Install() error {
	for _, store := range r.kind.stores {
		if err := r.stores[store].installPart(r.kind.specs[store]); err != nil {
			return err
		}
	}
	return nil
}

func (r *Repository[T]) assemble(id UUID, s snapshot) (*T, error) {
	obj := new(T)
	if err := r.kind.assemble(id, s, reflect.ValueOf(obj).Elem()); err != nil {
		return nil, err
	}
	return obj, nil
}

// Get is the entity as known at the read's moment (now when none), nil when
// it has no live version then (or none at all, with IncludeDeleted).
func (r *Repository[T]) Get(id UUID, options ReadOptions) (*T, error) {
	if r.kind.validity != nil {
		return r.getValid(id, options)
	}
	if options.AsOf != nil && options.KnownAt != nil {
		return nil, fmt.Errorf("rhizodat: %s has no valid time: AsOf and KnownAt are the same moment, give one", r.kind.typeName)
	}
	var bound *int64
	if at := options.AsOf; at != nil || options.KnownAt != nil {
		if at == nil {
			at = options.KnownAt
		}
		b := hlcBound(*at)
		bound = &b
	}
	s, err := r.consistent(id, bound)
	if err != nil || s == nil || (r.kind.first(s).Deleted && !options.IncludeDeleted) {
		return nil, err
	}
	return r.assemble(id, s)
}

// getValid is Get for a type with valid time: among the versions known at
// KnownAt (now when nil), the latest one whose period holds at AsOf (now).
func (r *Repository[T]) getValid(id UUID, options ReadOptions) (*T, error) {
	var bound *int64
	if options.KnownAt != nil {
		b := hlcBound(*options.KnownAt)
		bound = &b
	}
	known, err := r.snapshots(id, bound)
	if err != nil || len(known) == 0 || (r.kind.first(known[len(known)-1]).Deleted && !options.IncludeDeleted) {
		return nil, err
	}
	at := time.Now()
	if options.AsOf != nil {
		at = *options.AsOf
	}
	for i := len(known) - 1; i >= 0; i-- {
		if r.kind.holds(known[i], at) {
			return r.assemble(id, known[i])
		}
	}
	return nil, nil
}

// consistent reads the parts of one version. Stores finish a meta-transaction
// one after another, so a part may already show a newer version than another:
// it reads again at the oldest of the latest times until every part agrees.
func (r *Repository[T]) consistent(id UUID, knownAt *int64) (snapshot, error) {
	for range consistentReads {
		s := snapshot{}
		for _, store := range r.kind.stores {
			v, err := r.stores[store].readPart(r.kind.specs[store], id, knownAt)
			if err != nil {
				return nil, err
			}
			if v != nil {
				s[store] = v
			}
		}
		if len(s) < len(r.kind.stores) {
			return nil, nil // none, a hard delete, or a first write still finishing
		}
		txn, oldest, agree := r.kind.first(s).DatTxn, int64(-1), true
		for _, v := range s {
			agree = agree && v.DatTxn == txn
			if t := timeOf(v); oldest < 0 || t < oldest {
				oldest = t
			}
		}
		if agree {
			return s, nil
		}
		knownAt = &oldest
	}
	return nil, nil
}

func timeOf(v *PartVersion) int64 {
	if v.DatTxTime == nil {
		return 0
	}
	return *v.DatTxTime
}

// History is every committed version, oldest first (soft deletes included).
func (r *Repository[T]) History(id UUID) ([]T, error) {
	complete, err := r.snapshots(id, nil)
	if err != nil {
		return nil, err
	}
	out := make([]T, len(complete))
	for i, s := range complete {
		obj, err := r.assemble(id, s)
		if err != nil {
			return nil, err
		}
		out[i] = *obj
	}
	return out, nil
}

// snapshots is every version every store has (and knew at knownAt, when it is
// not nil), oldest first.
func (r *Repository[T]) snapshots(id UUID, knownAt *int64) ([]snapshot, error) {
	byTxn := map[UUID]snapshot{}
	for _, store := range r.kind.stores {
		versions, err := r.stores[store].partHistory(r.kind.specs[store], id)
		if err != nil {
			return nil, err
		}
		for i := range versions {
			v := versions[i]
			if knownAt != nil && (v.DatTxTime == nil || *v.DatTxTime > *knownAt) {
				continue
			}
			if byTxn[v.DatTxn] == nil {
				byTxn[v.DatTxn] = snapshot{}
			}
			byTxn[v.DatTxn][store] = &v
		}
	}
	var complete []snapshot
	for _, s := range byTxn {
		if len(s) == len(r.kind.stores) {
			complete = append(complete, s)
		}
	}
	sort.Slice(complete, func(i, j int) bool {
		return timeOf(r.kind.first(complete[i])) < timeOf(r.kind.first(complete[j]))
	})
	return complete, nil
}

// run runs work as one meta-transaction and the creation times of what it wrote.
func (r *Repository[T]) run(work func(*UnitRepository[T]) error) (Committed, map[UUID]*int64, error) {
	var in *UnitRepository[T]
	done, err := r.coordinator.Transactional(r.attempts, func(u *Unit) error {
		in = r.In(u)
		return work(in)
	})
	if err != nil {
		return Committed{}, nil, err
	}
	return done, in.created, nil
}

// write runs work for obj and stamps obj with its new times.
func (r *Repository[T]) write(obj *T, work func(*UnitRepository[T]) error) (Committed, error) {
	e := reflect.ValueOf(obj).Elem().FieldByIndex(r.kind.entity).Addr().Interface().(*Entity)
	if e.DatID == "" {
		id, err := NewID()
		if err != nil {
			return Committed{}, err
		}
		e.DatID = UUID(id)
	}
	done, created, err := r.run(work)
	if err != nil {
		return Committed{}, err
	}
	at := done.DatTxTime
	first := at
	if c := created[e.DatID]; c != nil {
		first = *c
	}
	e.DatCreatedAt, e.DatUpdatedAt, e.DatDeletedAt = hlcTime(first), hlcTime(at), time.Time{}
	e.DatVersion = at
	return done, nil
}

// Add commits obj as a new entity (a new id when it has none) and stamps its
// id and times. It fails with ErrEntityExists when the id has versions.
func (r *Repository[T]) Add(obj *T) (Committed, error) {
	return r.write(obj, func(in *UnitRepository[T]) error { return in.Add(obj) })
}

// Update commits a new version of a live entity and stamps its times.
func (r *Repository[T]) Update(obj *T) (Committed, error) {
	return r.write(obj, func(in *UnitRepository[T]) error { return in.Update(obj) })
}

// Save is Add or Update, whichever applies.
func (r *Repository[T]) Save(obj *T) (Committed, error) {
	return r.write(obj, func(in *UnitRepository[T]) error { return in.Save(obj) })
}

// Delete erases every version of the entity, or adds a soft delete when the
// type has a SoftDelete window.
func (r *Repository[T]) Delete(id UUID) (Committed, error) {
	done, _, err := r.run(func(in *UnitRepository[T]) error { return in.Delete(id) })
	return done, err
}

// DeleteVersion is Delete if expected is still the latest version, else
// ErrVersionConflict.
func (r *Repository[T]) DeleteVersion(id UUID, expected int64) (Committed, error) {
	done, _, err := r.run(func(in *UnitRepository[T]) error { return in.DeleteVersion(id, expected) })
	return done, err
}

// Restore brings a soft-deleted entity back within its window.
func (r *Repository[T]) Restore(id UUID) (Committed, error) {
	done, _, err := r.run(func(in *UnitRepository[T]) error { return in.Restore(id) })
	return done, err
}

// PurgeExpired erases at most limit soft-deleted entities whose window is
// over, in one meta-transaction; it returns how many it erased.
func (r *Repository[T]) PurgeExpired(limit int) (int, error) {
	if r.kind.soft <= 0 {
		return 0, nil
	}
	before := hlcNow() - r.kind.soft.Milliseconds()*hlcMillisecond
	seen := map[UUID]bool{}
	for _, store := range r.kind.stores {
		ids, err := r.stores[store].expiredParts(r.kind.specs[store], before, int64(limit))
		if err != nil {
			return 0, err
		}
		for _, id := range ids {
			seen[id] = true
		}
	}
	if len(seen) == 0 {
		return 0, nil
	}
	ids := make([]UUID, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	if len(ids) > limit {
		ids = ids[:limit]
	}
	purged := 0
	_, _, err := r.run(func(in *UnitRepository[T]) error {
		purged = 0
		for _, id := range ids {
			erased, err := in.purge(id, before)
			if err != nil {
				return err
			}
			if erased {
				purged++
			}
		}
		return nil
	})
	return purged, err
}

// In is the repository inside a unit of work: its writes commit with the
// rest of the unit, its reads see them.
func (r *Repository[T]) In(u *Unit) *UnitRepository[T] {
	return &UnitRepository[T]{repo: r, unit: u, created: map[UUID]*int64{}, checked: map[UUID]bool{}, locked: map[UUID]bool{}}
}

// UnitRepository reads and writes one entity type inside one unit of work.
type UnitRepository[T any] struct {
	repo    *Repository[T]
	unit    *Unit
	created map[UUID]*int64
	checked map[UUID]bool
	locked  map[UUID]bool
}

// check is the version precondition: lock the entity where a store can (the
// core checks expected again under the lock at commit), then compare its
// latest version here. 0 is no precondition.
func (in *UnitRepository[T]) check(id UUID, expected int64) error {
	if expected == 0 || in.checked[id] {
		return nil
	}
	if err := in.lock(id, &expected); err != nil {
		return err
	}
	s, err := in.latest(id)
	if err != nil {
		return err
	}
	var actual *int64
	if s != nil && !in.repo.kind.first(s).Deleted {
		actual = in.repo.kind.first(s).DatTxTime
	}
	if actual == nil || *actual != expected {
		return &VersionConflictError{TypeName: in.repo.kind.typeName, DatID: id, Expected: expected, Actual: actual}
	}
	in.checked[id] = true
	return nil
}

// lock buffers the entity's lock in every store of its parts (PostgreSQL and
// drevo take it; the others only compare), once per unit of work.
func (in *UnitRepository[T]) lock(id UUID, expected *int64) error {
	if in.locked[id] {
		return nil
	}
	k := in.repo.kind
	for _, store := range k.stores {
		if err := in.repo.stores[store].lockPart(in.unit, k.specs[store], id, expected); err != nil {
			return err
		}
	}
	in.locked[id] = true
	return nil
}

// Get is the latest live version, this unit of work's writes included; nil
// when there is none.
func (in *UnitRepository[T]) Get(id UUID) (*T, error) {
	s, err := in.latest(id)
	if err != nil || s == nil || in.repo.kind.first(s).Deleted {
		return nil, err
	}
	return in.repo.assemble(id, s)
}

func (in *UnitRepository[T]) latest(id UUID) (snapshot, error) {
	k := in.repo.kind
	s := snapshot{}
	for _, store := range k.stores {
		own, err := in.repo.stores[store].ownPart(in.unit, k.specs[store], id)
		if err != nil {
			return nil, err
		}
		v := own.Version
		if !own.Touched {
			if v, err = in.repo.stores[store].readPart(k.specs[store], id, nil); err != nil {
				return nil, err
			}
		}
		if v != nil {
			s[store] = v
		}
	}
	if len(s) < len(k.stores) {
		return nil, nil
	}
	return s, nil
}

func (in *UnitRepository[T]) live(id UUID) (snapshot, error) {
	s, err := in.latest(id)
	if err != nil {
		return nil, err
	}
	if s == nil || in.repo.kind.first(s).Deleted {
		return nil, fmt.Errorf("%w: %s %s", ErrEntityNotFound, in.repo.kind.typeName, id)
	}
	return s, nil
}

func (in *UnitRepository[T]) entity(obj *T) *Entity {
	return reflect.ValueOf(obj).Elem().FieldByIndex(in.repo.kind.entity).Addr().Interface().(*Entity)
}

// Add buffers obj as a new entity (a new id when it has none).
func (in *UnitRepository[T]) Add(obj *T) error {
	e := in.entity(obj)
	if e.DatID == "" {
		id, err := NewID()
		if err != nil {
			return err
		}
		e.DatID = UUID(id)
	}
	s, err := in.latest(e.DatID)
	if err != nil {
		return err
	}
	if s != nil {
		return fmt.Errorf("%w: %s %s", ErrEntityExists, in.repo.kind.typeName, e.DatID)
	}
	parts, err := in.repo.kind.split(reflect.ValueOf(obj).Elem())
	if err != nil {
		return err
	}
	return in.write(e.DatID, parts, nil, false)
}

// Update buffers a new version of a live entity; ErrVersionConflict when obj
// has a DatVersion that is no longer the latest.
func (in *UnitRepository[T]) Update(obj *T) error {
	e := in.entity(obj)
	id := e.DatID
	if err := in.check(id, e.DatVersion); err != nil {
		return err
	}
	s, err := in.live(id)
	if err != nil {
		return err
	}
	parts, err := in.repo.kind.split(reflect.ValueOf(obj).Elem())
	if err != nil {
		return err
	}
	return in.write(id, parts, in.repo.kind.first(s).Created, false)
}

// Save is Add or Update, whichever applies; ErrEntityExists over a soft delete.
func (in *UnitRepository[T]) Save(obj *T) error {
	e := in.entity(obj)
	if e.DatID == "" {
		return in.Add(obj)
	}
	s, err := in.latest(e.DatID)
	if err != nil {
		return err
	}
	var created *int64
	if s != nil {
		if err := in.check(e.DatID, e.DatVersion); err != nil {
			return err
		}
		if in.repo.kind.first(s).Deleted {
			return fmt.Errorf("%w: %s %s is soft-deleted: restore it first", ErrEntityExists, in.repo.kind.typeName, e.DatID)
		}
		created = in.repo.kind.first(s).Created
	}
	parts, err := in.repo.kind.split(reflect.ValueOf(obj).Elem())
	if err != nil {
		return err
	}
	return in.write(e.DatID, parts, created, false)
}

// Delete buffers the erase of every version, or a soft delete.
func (in *UnitRepository[T]) Delete(id UUID) error {
	return in.DeleteVersion(id, 0)
}

// DeleteVersion is Delete if expected is still the latest version (0: no
// check), else ErrVersionConflict.
func (in *UnitRepository[T]) DeleteVersion(id UUID, expected int64) error {
	if err := in.check(id, expected); err != nil {
		return err
	}
	s, err := in.live(id)
	if err != nil {
		return err
	}
	if in.repo.kind.soft > 0 {
		return in.write(id, in.repo.kind.values(s), in.repo.kind.first(s).Created, true)
	}
	return in.erase(id)
}

// Restore buffers a soft-deleted entity's return, within its window.
func (in *UnitRepository[T]) Restore(id UUID) error {
	name := fmt.Sprintf("%s %s", in.repo.kind.typeName, id)
	if in.repo.kind.soft <= 0 {
		return fmt.Errorf("%w: %s: the type has no soft delete; a delete erases it", ErrNotRestorable, name)
	}
	s, err := in.latest(id)
	if err != nil {
		return err
	}
	if s == nil {
		return fmt.Errorf("%w: %s does not exist (or was purged)", ErrNotRestorable, name)
	}
	v := in.repo.kind.first(s)
	if !v.Deleted {
		return fmt.Errorf("%w: %s is not deleted", ErrNotRestorable, name)
	}
	if v.DatTxTime != nil && hlcNow()-*v.DatTxTime > in.repo.kind.soft.Milliseconds()*hlcMillisecond {
		return fmt.Errorf("%w: %s left its restore window of %v", ErrNotRestorable, name, in.repo.kind.soft)
	}
	return in.write(id, in.repo.kind.values(s), v.Created, false)
}

// purge erases a soft-deleted entity still deleted before the bound.
func (in *UnitRepository[T]) purge(id UUID, deletedBefore int64) (bool, error) {
	s, err := in.latest(id)
	if err != nil || s == nil {
		return false, err
	}
	v := in.repo.kind.first(s)
	if !v.Deleted || v.DatTxTime == nil || *v.DatTxTime >= deletedBefore {
		return false, nil
	}
	return true, in.erase(id)
}

func (in *UnitRepository[T]) write(id UUID, parts map[string]map[string]any, created *int64, deleted bool) error {
	k := in.repo.kind
	if created != nil { // a new version of an existing entity: no write slips past a check
		if err := in.lock(id, nil); err != nil {
			return err
		}
	}
	for _, store := range k.stores {
		if err := in.repo.stores[store].writePart(in.unit, k.specs[store], id, parts[store], created, deleted); err != nil {
			return err
		}
	}
	in.created[id] = created
	return nil
}

func (in *UnitRepository[T]) erase(id UUID) error {
	k := in.repo.kind
	if err := in.lock(id, nil); err != nil {
		return err
	}
	for _, store := range k.stores {
		if err := in.repo.stores[store].erasePart(in.unit, k.specs[store], id); err != nil {
			return err
		}
	}
	delete(in.created, id)
	return nil
}
