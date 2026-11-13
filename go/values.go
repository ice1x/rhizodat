package rhizodat

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Values cross the C ABI as the core's tagged JSON: JSON scalars and arrays,
// and {"$": tag, "v": value} for what JSON cannot carry exactly. In Go:
//
//	nil, bool, string            as themselves
//	any integer type             int64 (back: int64)
//	float32, float64             float64 (NaN and infinities tagged)
//	UUID, Decimal, Date          tagged text
//	time.Time                    datetime with its offset (back: time.Time)
//	NaiveDateTime                datetime without an offset
//	[]byte                       bytes (base64)
//	slices and arrays            lists (back: []any)
//	Map, map[string]any          maps (back: Map, in order; map keys sorted)

// UUID is a UUID as its text; it binds to a uuid column.
type UUID string

// Decimal is an exact decimal as its text ("12.3400"); it binds to numeric.
type Decimal string

// Date is a date, "YYYY-MM-DD".
type Date string

// NaiveDateTime is a date and time without an offset, ISO 8601.
type NaiveDateTime string

// Pair is one entry of a Map.
type Pair struct {
	Key   string
	Value any
}

// Map is a map with string keys that keeps its order.
type Map []Pair

func tagged(tag string, v any) map[string]any { return map[string]any{"$": tag, "v": v} }

func encodeValue(v any) (any, error) {
	switch x := v.(type) {
	case nil, bool, string:
		return x, nil
	case UUID:
		return tagged("uuid", string(x)), nil
	case Decimal:
		return tagged("decimal", string(x)), nil
	case Date:
		return tagged("date", string(x)), nil
	case NaiveDateTime:
		return tagged("datetime", string(x)), nil
	case time.Time:
		return tagged("datetime", x.Format("2006-01-02T15:04:05.999999-07:00")), nil
	case []byte:
		return tagged("bytes", base64.StdEncoding.EncodeToString(x)), nil
	case float32:
		return encodeFloat(float64(x)), nil
	case float64:
		return encodeFloat(x), nil
	case Map:
		pairs := make([]any, len(x))
		for i, p := range x {
			value, err := encodeValue(p.Value)
			if err != nil {
				return nil, fmt.Errorf("map key %q: %w", p.Key, err)
			}
			pairs[i] = []any{p.Key, value}
		}
		return tagged("map", pairs), nil
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		m := make(Map, len(keys))
		for i, k := range keys {
			m[i] = Pair{k, x[k]}
		}
		return encodeValue(m)
	}
	rv := reflect.ValueOf(v)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return rv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		u := rv.Uint()
		if u > math.MaxInt64 {
			return nil, fmt.Errorf("%d does not fit a 64-bit signed integer", u)
		}
		return int64(u), nil
	case reflect.Slice, reflect.Array:
		list := make([]any, rv.Len())
		for i := range list {
			item, err := encodeValue(rv.Index(i).Interface())
			if err != nil {
				return nil, fmt.Errorf("item %d: %w", i, err)
			}
			list[i] = item
		}
		return list, nil
	}
	return nil, fmt.Errorf("a %T has no rhizodat value (see the package's value types)", v)
}

func encodeFloat(f float64) any {
	switch {
	case math.IsNaN(f):
		return tagged("float", "nan")
	case math.IsInf(f, 1):
		return tagged("float", "inf")
	case math.IsInf(f, -1):
		return tagged("float", "-inf")
	}
	return f
}

// encodeValues is values as a JSON array of tagged values.
func encodeValues(values []any) (string, error) {
	encoded, err := encodeValue(values)
	if err != nil {
		return "", err
	}
	text, err := json.Marshal(encoded)
	return string(text), err
}

func decodeValue(j any) (any, error) {
	switch x := j.(type) {
	case nil, bool, string:
		return x, nil
	case json.Number:
		if !strings.ContainsAny(string(x), ".eE") {
			return strconv.ParseInt(string(x), 10, 64)
		}
		return strconv.ParseFloat(string(x), 64)
	case []any:
		list := make([]any, len(x))
		for i, item := range x {
			v, err := decodeValue(item)
			if err != nil {
				return nil, err
			}
			list[i] = v
		}
		return list, nil
	case map[string]any:
		return decodeTagged(x)
	}
	return nil, fmt.Errorf("unexpected JSON %T", j)
}

func decodeTagged(object map[string]any) (any, error) {
	tag, ok := object["$"].(string)
	value, has := object["v"]
	if len(object) != 2 || !ok || !has {
		return nil, fmt.Errorf("%v is not a tagged value", object)
	}
	text, _ := value.(string)
	switch tag {
	case "uuid":
		return UUID(text), nil
	case "decimal":
		return Decimal(text), nil
	case "date":
		return Date(text), nil
	case "datetime":
		for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.999999999-07:00"} {
			if t, err := time.Parse(layout, text); err == nil {
				return t, nil
			}
		}
		return NaiveDateTime(text), nil
	case "bytes":
		return base64.StdEncoding.DecodeString(text)
	case "float":
		switch text {
		case "nan":
			return math.NaN(), nil
		case "inf":
			return math.Inf(1), nil
		case "-inf":
			return math.Inf(-1), nil
		}
		return nil, fmt.Errorf("float %q", text)
	case "map":
		pairs, ok := value.([]any)
		if !ok {
			return nil, fmt.Errorf("a map is not a list of pairs")
		}
		m := make(Map, len(pairs))
		for i, raw := range pairs {
			pair, ok := raw.([]any)
			if !ok || len(pair) != 2 {
				return nil, fmt.Errorf("a map entry is not a [key, value] pair")
			}
			key, ok := pair[0].(string)
			if !ok {
				return nil, fmt.Errorf("a map key is not a string")
			}
			v, err := decodeValue(pair[1])
			if err != nil {
				return nil, err
			}
			m[i] = Pair{key, v}
		}
		return m, nil
	}
	return nil, fmt.Errorf("unknown tag %q", tag)
}

// decodeValues is a JSON array of tagged values.
func decodeValues(text []byte) ([]any, error) {
	decoder := json.NewDecoder(bytes.NewReader(text))
	decoder.UseNumber()
	var raw []any
	if err := decoder.Decode(&raw); err != nil {
		return nil, err
	}
	decoded, err := decodeValue(raw)
	if err != nil {
		return nil, err
	}
	return decoded.([]any), nil
}
