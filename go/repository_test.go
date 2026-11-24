package rhizodat

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

// A bug tracker's issue: its workflow fields in one PostgreSQL store, its
// labels and notes in another, so every write is a meta-transaction over both.
type Issue struct {
	Entity
	Title    string            `rhizodat:"title,store=tracker"`
	Points   int               `rhizodat:"points,store=tracker"`
	Closed   bool              `rhizodat:"closed,store=tracker"`
	Due      *time.Time        `rhizodat:"due,store=tracker"`
	Labels   []string          `rhizodat:"labels,store=labels"`
	Estimate Decimal           `rhizodat:"estimate,store=labels"`
	Extra    map[string]string `rhizodat:"extra,store=labels"`
	internal string            // not mapped
}

type tracker struct {
	coordinator *Coordinator
	stores      []PartStore
}

func newTracker(t *testing.T) tracker {
	t.Helper()
	d := newDB(t)
	journal := d.journal()
	c, err := NewCoordinator(journal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return tracker{coordinator: c, stores: []PartStore{d.namedStore("tracker"), d.namedStore("labels")}}
}

// namedStore is a store called exactly name (entity tags refer to it), in its
// own schema.
func (d *db) namedStore(name string) *PostgresStore {
	d.t.Helper()
	schema := d.base + "_" + name
	d.exec("CREATE SCHEMA " + schema)
	s, err := OpenPostgres(d.dsn, name, schema)
	if err != nil {
		d.t.Fatal(err)
	}
	d.t.Cleanup(s.Close)
	return s
}

func (tr tracker) repo(t *testing.T, options EntityOptions) *Repository[Issue] {
	t.Helper()
	if options.Type == "" {
		options.Type = "tracker.Issue"
	}
	repo, err := NewRepository[Issue](tr.coordinator, tr.stores, options)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.Install(); err != nil {
		t.Fatal(err)
	}
	return repo
}

func TestAnEntityIsAddedReadUpdatedAndKeepsItsHistory(t *testing.T) {
	repo := newTracker(t).repo(t, EntityOptions{})
	due := time.Date(2026, 12, 1, 9, 0, 0, 0, time.UTC)
	issue := &Issue{Title: "Crash on save", Points: 3, Due: &due, Labels: []string{"ui", "p1"},
		Estimate: "2.50", Extra: map[string]string{"os": "macOS"}, internal: "x"}
	added, err := repo.Add(issue)
	if err != nil {
		t.Fatal(err)
	}
	if issue.DatID == "" || issue.DatCreatedAt.IsZero() || !issue.DatUpdatedAt.Equal(issue.DatCreatedAt) {
		t.Fatalf("Add stamps the id and times: %+v", issue.Entity)
	}

	got, err := repo.Get(issue.DatID, ReadOptions{})
	if err != nil || got == nil {
		t.Fatalf("get: %+v %v", got, err)
	}
	if got.Title != "Crash on save" || got.Points != 3 || got.Due == nil || !got.Due.Equal(due) ||
		!reflect.DeepEqual(got.Labels, []string{"ui", "p1"}) || got.Estimate != "2.50" ||
		!reflect.DeepEqual(got.Extra, map[string]string{"os": "macOS"}) || got.internal != "" {
		t.Fatalf("read back %+v", got)
	}
	if !got.DatCreatedAt.Equal(issue.DatCreatedAt) || !got.DatDeletedAt.IsZero() {
		t.Fatalf("times %+v vs %+v", got.Entity, issue.Entity)
	}

	time.Sleep(5 * time.Millisecond) // the update is known at a later millisecond
	got.Points, got.Closed, got.Due = 5, true, nil
	if _, err := repo.Update(got); err != nil {
		t.Fatal(err)
	}
	latest, _ := repo.Get(issue.DatID, ReadOptions{})
	if latest.Points != 5 || !latest.Closed || latest.Due != nil || !latest.DatCreatedAt.Equal(issue.DatCreatedAt) {
		t.Fatalf("latest %+v", latest)
	}
	if !latest.DatUpdatedAt.After(latest.DatCreatedAt) {
		t.Fatalf("updated %v, created %v", latest.DatUpdatedAt, latest.DatCreatedAt)
	}

	firstKnown := hlcTime(added.DatTxTime)
	before, err := repo.Get(issue.DatID, ReadOptions{AsOf: &firstKnown})
	if err != nil || before == nil || before.Points != 3 || before.Closed {
		t.Fatalf("as of the add: %+v %v", before, err)
	}
	earlier := firstKnown.Add(-time.Second)
	if none, err := repo.Get(issue.DatID, ReadOptions{KnownAt: &earlier}); none != nil || err != nil {
		t.Fatalf("before it existed: %+v %v", none, err)
	}
	if _, err := repo.Get(issue.DatID, ReadOptions{AsOf: &firstKnown, KnownAt: &firstKnown}); err == nil {
		t.Fatal("as_of and known_at are one moment without valid time")
	}

	history, err := repo.History(issue.DatID)
	if err != nil || len(history) != 2 || history[0].Points != 3 || history[1].Points != 5 {
		t.Fatalf("history %+v %v", history, err)
	}
}

func TestWritesAreRefusedForWhatIsNotThere(t *testing.T) {
	repo := newTracker(t).repo(t, EntityOptions{})
	issue := &Issue{Title: "t"}
	if _, err := repo.Add(issue); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Add(issue); !errors.Is(err, ErrEntityExists) {
		t.Fatalf("a second add: %v", err)
	}
	ghost := &Issue{Entity: Entity{DatID: newUUID(t)}, Title: "ghost"}
	if _, err := repo.Update(ghost); !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("an update of nothing: %v", err)
	}
	if _, err := repo.Delete(ghost.DatID); !errors.Is(err, ErrEntityNotFound) {
		t.Fatalf("a delete of nothing: %v", err)
	}
	if got, err := repo.Get(ghost.DatID, ReadOptions{}); got != nil || err != nil {
		t.Fatalf("a get of nothing: %+v %v", got, err)
	}
	if _, err := repo.Save(ghost); err != nil {
		t.Fatalf("save adds: %v", err)
	}
	ghost.Title = "seen"
	if _, err := repo.Save(ghost); err != nil {
		t.Fatalf("save updates: %v", err)
	}
	if got, _ := repo.Get(ghost.DatID, ReadOptions{}); got.Title != "seen" {
		t.Fatalf("saved %+v", got)
	}
}

func TestAHardDeleteErasesEveryVersion(t *testing.T) {
	repo := newTracker(t).repo(t, EntityOptions{})
	issue := &Issue{Title: "t"}
	repo.Add(issue)
	issue.Points = 2
	repo.Update(issue)
	if _, err := repo.Delete(issue.DatID); err != nil {
		t.Fatal(err)
	}
	if got, _ := repo.Get(issue.DatID, ReadOptions{IncludeDeleted: true}); got != nil {
		t.Fatalf("erased, still read: %+v", got)
	}
	if history, _ := repo.History(issue.DatID); len(history) != 0 {
		t.Fatalf("erased history %+v", history)
	}
}

func TestASoftDeleteHidesRestoresAndPurges(t *testing.T) {
	repo := newTracker(t).repo(t, EntityOptions{SoftDelete: time.Hour})
	issue := &Issue{Title: "t", Labels: []string{"a"}}
	repo.Add(issue)
	if _, err := repo.Restore(issue.DatID); !errors.Is(err, ErrNotRestorable) {
		t.Fatalf("a live entity: %v", err)
	}
	if _, err := repo.Delete(issue.DatID); err != nil {
		t.Fatal(err)
	}
	if got, _ := repo.Get(issue.DatID, ReadOptions{}); got != nil {
		t.Fatalf("deleted, still read: %+v", got)
	}
	deleted, err := repo.Get(issue.DatID, ReadOptions{IncludeDeleted: true})
	if err != nil || deleted == nil || deleted.DatDeletedAt.IsZero() || !reflect.DeepEqual(deleted.Labels, []string{"a"}) {
		t.Fatalf("with deleted: %+v %v", deleted, err)
	}
	if _, err := repo.Save(&Issue{Entity: Entity{DatID: issue.DatID}}); !errors.Is(err, ErrEntityExists) {
		t.Fatalf("a save over a soft delete: %v", err)
	}
	if _, err := repo.Restore(issue.DatID); err != nil {
		t.Fatal(err)
	}
	if got, _ := repo.Get(issue.DatID, ReadOptions{}); got == nil || got.Title != "t" {
		t.Fatalf("restored %+v", got)
	}
	if n, err := repo.PurgeExpired(100); n != 0 || err != nil {
		t.Fatalf("nothing expired: %d %v", n, err)
	}

	short := newTracker(t).repo(t, EntityOptions{SoftDelete: time.Millisecond})
	gone := &Issue{Title: "gone"}
	short.Add(gone)
	short.Delete(gone.DatID)
	time.Sleep(10 * time.Millisecond)
	if _, err := short.Restore(gone.DatID); !errors.Is(err, ErrNotRestorable) {
		t.Fatalf("past its window: %v", err)
	}
	if n, err := short.PurgeExpired(100); n != 1 || err != nil {
		t.Fatalf("purged %d %v", n, err)
	}
	if got, _ := short.Get(gone.DatID, ReadOptions{IncludeDeleted: true}); got != nil {
		t.Fatalf("purged, still read: %+v", got)
	}
}

func TestAUnitOfWorkSeesItsOwnWritesAndCommitsThemTogether(t *testing.T) {
	tr := newTracker(t)
	repo := tr.repo(t, EntityOptions{})
	a, b := &Issue{Title: "a"}, &Issue{Title: "b"}
	done, err := tr.coordinator.Transactional(3, func(u *Unit) error {
		in := repo.In(u)
		if err := in.Add(a); err != nil {
			return err
		}
		if err := in.Add(b); err != nil {
			return err
		}
		own, err := in.Get(a.DatID)
		if err != nil || own == nil || own.Title != "a" {
			t.Fatalf("the unit's own write: %+v %v", own, err)
		}
		b.Points = 8
		return in.Update(b)
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, issue := range []*Issue{a, b} {
		got, _ := repo.Get(issue.DatID, ReadOptions{})
		if got == nil || got.Title != issue.Title {
			t.Fatalf("committed %+v", got)
		}
	}
	if history, _ := repo.History(b.DatID); len(history) != 1 || history[0].Points != 8 {
		t.Fatalf("one version per meta-transaction: %+v", history)
	}
	if latest, _ := repo.Get(a.DatID, ReadOptions{}); hlcBound(latest.DatUpdatedAt) < done.DatTxTime {
		t.Fatalf("stamped with the commit's time: %v %d", latest.DatUpdatedAt, done.DatTxTime)
	}
}

func TestEntityTypesAreChecked(t *testing.T) {
	tr := newTracker(t)
	type noEntity struct {
		Title string `rhizodat:"title,store=tracker"`
	}
	type badStore struct {
		Entity
		Title string `rhizodat:"title,store=nowhere"`
	}
	type noStore struct {
		Entity
		Title string `rhizodat:"title"`
	}
	type badType struct {
		Entity
		Ch chan int `rhizodat:"ch,store=tracker"`
	}
	type twice struct {
		Entity
		A string `rhizodat:"a,store=tracker"`
		B string `rhizodat:"a,store=labels"`
	}
	checks := map[string]error{
		"Entity":    tryRepository[noEntity](tr),
		"nowhere":   tryRepository[badStore](tr),
		"store=":    tryRepository[noStore](tr),
		"chan":      tryRepository[badType](tr),
		"twice":     tryRepository[twice](tr),
		"type name": func() error { _, err := NewRepository[Issue](tr.coordinator, tr.stores, EntityOptions{}); return err }(),
	}
	for want, err := range checks {
		if err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("want an error with %q, got %v", want, err)
		}
	}
}

func tryRepository[T any](tr tracker) error {
	_, err := NewRepository[T](tr.coordinator, tr.stores, EntityOptions{Type: "t.T"})
	return err
}
