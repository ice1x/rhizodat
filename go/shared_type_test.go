package rhizodat

import (
	"encoding/json"
	"os/exec"
	"reflect"
	"strings"
	"testing"
	"time"
)

// facade runs go/testdata/facade.py, the Python facade's side: the same
// tracker.Issue over the same stores and journal. It skips without uv (CI's
// go job installs it on Linux, where the stores run).
func facade(t *testing.T, d *db, args ...string) string {
	t.Helper()
	uv, err := exec.LookPath("uv")
	if err != nil {
		t.Skip("no uv: the Python facade's side cannot run")
	}
	argv := append([]string{"run", "--no-sync", "python", "go/testdata/facade.py", args[0], d.dsn,
		d.base + "_tracker", d.base + "_labels", d.base + "_journal"}, args[1:]...)
	cmd := exec.Command(uv, argv...)
	cmd.Dir = ".."
	out, err := cmd.Output()
	if err != nil {
		var stderr string
		if exit, ok := err.(*exec.ExitError); ok {
			stderr = string(exit.Stderr)
		}
		t.Fatalf("the Python facade failed: %v\n%s", err, stderr)
	}
	return strings.TrimSpace(string(out))
}

// trackerOn is the tracker's stores and journal in d, as newTracker makes them.
func trackerOn(t *testing.T, d *db) *Repository[Issue] {
	t.Helper()
	c, err := NewCoordinator(d.journal())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	tr := tracker{coordinator: c, stores: []PartStore{d.namedStore("tracker"), d.namedStore("labels")}}
	return tr.repo(t, EntityOptions{})
}

func TestATypeIsSharedWithThePythonFacade(t *testing.T) {
	d := newDB(t)
	issues := trackerOn(t, d)

	due := time.Date(2026, 12, 1, 9, 0, 0, 0, time.UTC)
	fromGo := &Issue{Title: "From Go", Points: 3, Due: &due, Labels: []string{"go", "ffi"},
		Estimate: "2.50", Extra: map[string]string{"by": "go"}}
	if _, err := issues.Add(fromGo); err != nil {
		t.Fatal(err)
	}
	var read struct {
		Title    string            `json:"title"`
		Points   int               `json:"points"`
		Closed   bool              `json:"closed"`
		Due      *time.Time        `json:"due"`
		Labels   []string          `json:"labels"`
		Estimate *string           `json:"estimate"`
		Extra    map[string]string `json:"extra"`
		Created  time.Time         `json:"created"`
	}
	if err := json.Unmarshal([]byte(facade(t, d, "read", string(fromGo.DatID))), &read); err != nil {
		t.Fatal(err)
	}
	if read.Title != "From Go" || read.Points != 3 || read.Closed || read.Due == nil || !read.Due.Equal(due) ||
		!reflect.DeepEqual(read.Labels, []string{"go", "ffi"}) || read.Estimate == nil || *read.Estimate != "2.50" ||
		!reflect.DeepEqual(read.Extra, map[string]string{"by": "go"}) {
		t.Fatalf("Python read Go's issue as %+v", read)
	}
	if !read.Created.Equal(fromGo.DatCreatedAt) {
		t.Fatalf("created at %v in Python, %v in Go", read.Created, fromGo.DatCreatedAt)
	}

	id := UUID(facade(t, d, "add"))
	fromPython, err := issues.Get(id, ReadOptions{})
	if err != nil || fromPython == nil {
		t.Fatalf("Go reads Python's issue: %+v %v", fromPython, err)
	}
	if fromPython.Title != "From Python" || fromPython.Points != 8 || !fromPython.Closed || fromPython.Due != nil ||
		!reflect.DeepEqual(fromPython.Labels, []string{"py"}) || fromPython.Estimate != "1.25" ||
		!reflect.DeepEqual(fromPython.Extra, map[string]string{"by": "python"}) {
		t.Fatalf("Go read Python's issue as %+v", fromPython)
	}
	fromPython.Points = 13
	if _, err := issues.Update(fromPython); err != nil {
		t.Fatal(err)
	}
	if history, _ := issues.History(id); len(history) != 2 {
		t.Fatalf("one history across both facades: %+v", history)
	}
}
