package rhizodat

import (
	"errors"
	"os"
	"regexp"
	"sort"
	"sync"
	"testing"
)

func TestVersionIsTheLibrarysCrateVersion(t *testing.T) {
	manifest, err := os.ReadFile("../crates/rhizodat-ffi/Cargo.toml")
	if err != nil {
		t.Fatal(err)
	}
	want := regexp.MustCompile(`(?m)^version = "([^"]+)"`).FindSubmatch(manifest)[1]
	if got := Version(); got != string(want) {
		t.Fatalf("Version() = %q, want %q", got, want)
	}
}

var uuid7 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)

func TestNewIDIsAUUID7(t *testing.T) {
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	if !uuid7.MatchString(id) {
		t.Fatalf("%q is not a UUIDv7", id)
	}
}

func TestIDsFromOneGoroutineAreOrdered(t *testing.T) {
	ids := make([]string, 2000)
	for i := range ids {
		id, err := NewID()
		if err != nil {
			t.Fatal(err)
		}
		ids[i] = id
	}
	if !sort.StringsAreSorted(ids) {
		t.Fatal("ids from one goroutine are not in order")
	}
}

func TestIDsFromManyGoroutinesAreUnique(t *testing.T) {
	var (
		mu   sync.Mutex
		seen = map[string]bool{}
		wg   sync.WaitGroup
	)
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				id, err := NewID()
				if err != nil {
					t.Error(err)
					return
				}
				mu.Lock()
				seen[id] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != 16*500 {
		t.Fatalf("%d unique ids, want %d", len(seen), 16*500)
	}
}

func TestAStatusBecomesATypedError(t *testing.T) {
	err := statusError(statusInvalidArgument, "out is null")
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("%v is not an *Error", err)
	}
	if e.Status != StatusInvalidArgument || e.Message != "out is null" {
		t.Fatalf("got %+v", e)
	}
	if !errors.Is(err, ErrInvalidArgument) {
		t.Fatal("an invalid-argument error is not ErrInvalidArgument")
	}
	if statusError(statusOK, "") != nil {
		t.Fatal("OK is not an error")
	}
}
