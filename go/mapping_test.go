package rhizodat

import (
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPartTablesAreNamedLikeThePythonFacades(t *testing.T) {
	// From rhizodat.core.parts.Part.table.
	cases := map[string]string{
		"tracker.Issue":         "tracker_issue",
		"Issue":                 "issue",
		"9lives":                "e_9lives",
		"a-b c.D":               "a_b_c_d",
		strings.Repeat("x", 80): strings.Repeat("x", 63),
	}
	for name, want := range cases {
		if got := partTable(name); got != want {
			t.Errorf("partTable(%q) = %q, want %q", name, got, want)
		}
	}
}

func TestGoTypesGetThePythonFacadesColumns(t *testing.T) {
	var (
		text     string
		count    int32
		big      uint64
		ratio    float32
		flag     bool
		at       time.Time
		id       UUID
		price    Decimal
		day      Date
		blob     []byte
		optional *int
		labels   []string
		nested   [][]int
		attrs    map[string]any
		ordered  Map
		anything any
		times    []time.Time
	)
	cases := []struct {
		value any
		want  string
	}{
		{&text, "text"}, {&count, "bigint"}, {&big, "bigint"}, {&ratio, "double precision"},
		{&flag, "boolean"}, {&at, "timestamptz"}, {&id, "uuid"}, {&price, "numeric"},
		{&day, "date"}, {&blob, "bytea"}, {&optional, "bigint"}, {&labels, "jsonb"},
		{&nested, "jsonb"}, {&attrs, "jsonb"}, {&ordered, "jsonb"}, {&anything, "jsonb"},
		{&times, "jsonb"},
	}
	for _, c := range cases {
		typ := reflect.TypeOf(c.value).Elem()
		if got, ok := postgresColumnType(typ); !ok || got != c.want {
			t.Errorf("%v: %q %v, want %q", typ, got, ok, c.want)
		}
	}
	for _, refused := range []any{
		new(chan int), new(struct{ A int }), new([]struct{ A int }), new(map[int]string),
		new(complex128), new(func()), new(interface{ M() }),
	} {
		typ := reflect.TypeOf(refused).Elem()
		if _, ok := postgresColumnType(typ); ok {
			t.Errorf("%v should not be storable", typ)
		}
	}
}

func TestValuesComeBackIntoTheirFields(t *testing.T) {
	type fields struct {
		Count    int16
		Big      uint32
		Ratio    float32
		Whole    float64
		Labels   []string
		Fixed    [2]int
		Attrs    map[string]int
		Optional *string
		Anything any
		Day      Date
	}
	stored := map[string]any{
		"Count": int64(7), "Big": int64(9), "Ratio": 0.5, "Whole": int64(3),
		"Labels": []any{"a", "b"}, "Fixed": []any{int64(1), int64(2)},
		"Attrs": Map{{"x", int64(1)}}, "Optional": "here", "Anything": []any{"z"},
		"Day": Date("2026-11-24"),
	}
	var got fields
	v := reflect.ValueOf(&got).Elem()
	for name, value := range stored {
		if err := assign(v.FieldByName(name), value); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	here := "here"
	want := fields{Count: 7, Big: 9, Ratio: 0.5, Whole: 3, Labels: []string{"a", "b"}, Fixed: [2]int{1, 2},
		Attrs: map[string]int{"x": 1}, Optional: &here, Anything: []any{"z"}, Day: "2026-11-24"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
	if err := assign(v.FieldByName("Optional"), nil); err != nil || got.Optional != nil {
		t.Fatalf("null clears: %v %v", got.Optional, err)
	}

	for field, value := range map[string]any{
		"Count": int64(1 << 20), "Big": int64(-1), "Labels": "not a list",
		"Fixed": []any{int64(1)}, "Attrs": "not a map", "Day": int64(3),
	} {
		if err := assign(v.FieldByName(field), value); err == nil {
			t.Errorf("%s accepted %#v", field, value)
		}
	}
}

func TestFieldsAreStoredAsPlainValues(t *testing.T) {
	n := 4
	type fields struct {
		Labels  []string
		Attrs   map[string]int
		Ptr     *int
		Nil     *int
		NilList []string
		NilMap  map[string]int
		Ordered Map
		ID      UUID
		Blob    []byte
	}
	in := fields{Labels: []string{"a"}, Attrs: map[string]int{"x": 1}, Ptr: &n, Ordered: Map{{"k", 1}}, Blob: []byte{1}}
	v := reflect.ValueOf(in)
	got := map[string]any{}
	for i := 0; i < v.NumField(); i++ {
		got[v.Type().Field(i).Name] = storable(v.Field(i))
	}
	want := map[string]any{
		"Labels": []any{"a"}, "Attrs": map[string]any{"x": 1}, "Ptr": 4, "Nil": nil,
		"NilList": nil, "NilMap": nil, "Ordered": Map{{"k", 1}}, "ID": nil, "Blob": []byte{1},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v\nwant %#v", got, want)
	}
}

func TestTransactionTimesAndWallClockTimes(t *testing.T) {
	at := time.Date(2026, 11, 24, 10, 0, 0, 0, time.UTC)
	bound := hlcBound(at)
	if hlcTime(bound) != at || bound&maxLogicalInAtMs != maxLogicalInAtMs {
		t.Fatalf("bound %d of %v reads back as %v", bound, at, hlcTime(bound))
	}
	if hlcBound(at.Add(time.Millisecond)) <= bound {
		t.Fatal("a later millisecond is a later bound")
	}
}
