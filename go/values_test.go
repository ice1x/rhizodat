package rhizodat

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"testing/quick"
	"time"
)

func roundTrip(t *testing.T, v any) any {
	t.Helper()
	text, err := encodeValues([]any{v})
	if err != nil {
		t.Fatalf("encode %#v: %v", v, err)
	}
	back, err := decodeValues([]byte(text))
	if err != nil {
		t.Fatalf("decode %s: %v", text, err)
	}
	return back[0]
}

func TestValuesKeepTheirTypeThroughTaggedJSON(t *testing.T) {
	at := time.Date(2026, 11, 14, 9, 30, 15, 123456000, time.FixedZone("", 2*3600))
	for _, tc := range []struct {
		in, want any
	}{
		{nil, nil},
		{true, true},
		{42, int64(42)},
		{int64(math.MaxInt64), int64(math.MaxInt64)},
		{uint32(7), int64(7)},
		{1.5, 1.5},
		{"web1", "web1"},
		{UUID("01920000-0000-7000-8000-000000000001"), UUID("01920000-0000-7000-8000-000000000001")},
		{Decimal("12.3400"), Decimal("12.3400")},
		{Date("2026-11-14"), Date("2026-11-14")},
		{[]byte{0, 1, 255}, []byte{0, 1, 255}},
		{at, at},
		{NaiveDateTime("2026-11-14T09:30:15"), NaiveDateTime("2026-11-14T09:30:15")},
		{[]any{1, "a", nil}, []any{int64(1), "a", nil}},
		{[]string{"a", "b"}, []any{"a", "b"}},
		{Map{{"z", 1}, {"a", "x"}}, Map{{"z", int64(1)}, {"a", "x"}}},
		{map[string]any{"b": 2, "a": 1}, Map{{"a", int64(1)}, {"b", int64(2)}}},
	} {
		got := roundTrip(t, tc.in)
		if tm, ok := tc.want.(time.Time); ok {
			if g, ok := got.(time.Time); !ok || !g.Equal(tm) {
				t.Errorf("%#v came back as %#v", tc.in, got)
			}
			continue
		}
		if !reflect.DeepEqual(got, tc.want) {
			t.Errorf("%#v came back as %#v, want %#v", tc.in, got, tc.want)
		}
	}
}

func TestNonFiniteFloatsAreTagged(t *testing.T) {
	text, err := encodeValues([]any{math.Inf(1), math.Inf(-1), math.NaN()})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(text, `{"$":"float","v":"inf"}`) {
		t.Fatalf("infinity is not tagged: %s", text)
	}
	back, err := decodeValues([]byte(text))
	if err != nil {
		t.Fatal(err)
	}
	if !math.IsInf(back[0].(float64), 1) || !math.IsInf(back[1].(float64), -1) || !math.IsNaN(back[2].(float64)) {
		t.Fatalf("got %v", back)
	}
}

func TestWhatHasNoTaggedFormIsRefused(t *testing.T) {
	for _, v := range []any{struct{}{}, uint64(math.MaxUint64), map[int]string{1: "a"}, make(chan int)} {
		if _, err := encodeValues([]any{v}); err == nil {
			t.Errorf("%T was encoded", v)
		}
	}
}

func TestAPlainObjectFromTheCoreIsRefused(t *testing.T) {
	if _, err := decodeValues([]byte(`[{"a": 1}]`)); err == nil {
		t.Fatal("a plain object decoded")
	}
	if _, err := decodeValues([]byte(`[{"$": "nope", "v": 1}]`)); err == nil {
		t.Fatal("an unknown tag decoded")
	}
}

func TestIntegersAndStringsSurviveAnyValue(t *testing.T) {
	property := func(n int64, s string, f float64) bool {
		back := []any{roundTrip(t, n), roundTrip(t, s), roundTrip(t, f)}
		return back[0] == n && back[1] == s && (back[2] == f || (math.IsNaN(f) && math.IsNaN(back[2].(float64))))
	}
	if err := quick.Check(property, nil); err != nil {
		t.Fatal(err)
	}
}

func TestEncodedValuesAreTheCoresJSON(t *testing.T) {
	text, err := encodeValues([]any{UUID("01920000-0000-7000-8000-000000000001"), []byte("hi")})
	if err != nil {
		t.Fatal(err)
	}
	var raw []map[string]string
	if err := json.Unmarshal([]byte(text), &raw); err != nil {
		t.Fatal(err)
	}
	if raw[0]["$"] != "uuid" || raw[1]["$"] != "bytes" || raw[1]["v"] != "aGk=" {
		t.Fatalf("got %s", text)
	}
}
