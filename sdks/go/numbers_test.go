package kernable

import (
	"encoding/json"
	"testing"
)

// Generic data keeps every number exact: an integer beyond 2^53 does not become the nearest
// float64, and a payload reads back to the same JSON.
func TestExactNumbers(t *testing.T) {
	payload := []byte(`{"big":9007199254740993,"min":-9223372036854775808,"max":18446744073709551615,"f":0.1,"neg":-0.0}`)
	var m map[string]any
	if err := decodeInto(payload, &m); err != nil {
		t.Fatal(err)
	}
	if n, ok := m["big"].(json.Number); !ok || n.String() != "9007199254740993" {
		t.Fatalf("big: %#v", m["big"])
	}
	out, err := encode(m)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != `{"big":9007199254740993,"f":0.1,"max":18446744073709551615,"min":-9223372036854775808,"neg":-0.0}` {
		t.Fatalf("round trip: %s", out)
	}
	generic, err := decodeAny(payload)
	if err != nil {
		t.Fatal(err)
	}
	back, err := into[map[string]any](generic)
	if err != nil || back["max"].(json.Number).String() != "18446744073709551615" {
		t.Fatalf("into: %#v %v", back["max"], err)
	}
	// Typed fields decode as before.
	var typed struct {
		Big int64   `json:"big"`
		F   float64 `json:"f"`
	}
	if err := decodeInto(payload, &typed); err != nil || typed.Big != 9007199254740993 || typed.F != 0.1 {
		t.Fatalf("typed: %+v %v", typed, err)
	}
	// Like json.Unmarshal, data after the value is an error.
	if err := decodeInto([]byte(`{} {}`), &m); err == nil {
		t.Fatal("trailing data accepted")
	}
}
