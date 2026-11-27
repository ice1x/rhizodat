package rhizodat

import (
	"errors"
	"strings"
	"testing"
	"time"
)

// Version preconditions: an entity read (or written) carries DatVersion, and
// a write based on a version that is no longer the latest fails with
// ErrVersionConflict instead of losing the newer write.

func TestARereadEntityCarriesTheVersionItWasReadAt(t *testing.T) {
	repo := newTracker(t).repo(t, EntityOptions{})
	issue := &Issue{Title: "Crash on save"}
	if issue.DatVersion != 0 {
		t.Fatalf("an entity built by hand has no version: %d", issue.DatVersion)
	}
	added, err := repo.Add(issue)
	if err != nil {
		t.Fatal(err)
	}
	if issue.DatVersion != added.DatTxTime {
		t.Fatalf("the write stamps its version: %d, committed at %d", issue.DatVersion, added.DatTxTime)
	}
	got, _ := repo.Get(issue.DatID, ReadOptions{})
	history, _ := repo.History(issue.DatID)
	if got.DatVersion != added.DatTxTime || len(history) != 1 || history[0].DatVersion != added.DatTxTime {
		t.Fatalf("read at %d, history %+v", got.DatVersion, history)
	}
}

func TestAStaleUpdateIsRefused(t *testing.T) {
	repo := newTracker(t).repo(t, EntityOptions{})
	issue := &Issue{Title: "t", Points: 1}
	repo.Add(issue)
	stale, _ := repo.Get(issue.DatID, ReadOptions{})

	issue.Points = 2
	moved, err := repo.Update(issue)
	if err != nil {
		t.Fatal(err)
	}
	stale.Title = "lost"
	_, err = repo.Update(stale)
	var conflict *VersionConflictError
	if !errors.Is(err, ErrVersionConflict) || !errors.As(err, &conflict) {
		t.Fatalf("a stale update: %v", err)
	}
	if conflict.DatID != issue.DatID || conflict.Expected != stale.DatVersion ||
		conflict.Actual == nil || *conflict.Actual != moved.DatTxTime || conflict.TypeName != "tracker.Issue" {
		t.Fatalf("conflict %+v", conflict)
	}
	if _, err := repo.Save(stale); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("a stale save: %v", err)
	}
	latest, _ := repo.Get(issue.DatID, ReadOptions{})
	if latest.Title != "t" || latest.Points != 2 {
		t.Fatalf("the newer write stays: %+v", latest)
	}

	stale.DatVersion = 0 // unchecked: last writer wins, as before preconditions
	if _, err := repo.Update(stale); err != nil {
		t.Fatal(err)
	}
	if history, _ := repo.History(issue.DatID); len(history) != 3 {
		t.Fatalf("history %+v", history)
	}
}

func TestADeleteOfAnExpectedVersionChecksIt(t *testing.T) {
	repo := newTracker(t).repo(t, EntityOptions{SoftDelete: time.Hour})
	issue := &Issue{Title: "t"}
	repo.Add(issue)
	first := issue.DatVersion
	issue.Points = 3
	repo.Update(issue)
	if _, err := repo.DeleteVersion(issue.DatID, first); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("a delete of an old version: %v", err)
	}
	if _, err := repo.DeleteVersion(issue.DatID, issue.DatVersion); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Update(issue); !errors.Is(err, ErrVersionConflict) {
		t.Fatalf("an update of a deleted entity's version: %v", err)
	}
}

// The core checks again at commit, under the entity's locks in PostgreSQL and
// drevo: a version that moved after the repository's check is refused there,
// and the conflict is not retried.
func TestAVersionThatMovesAfterTheCheckIsRefusedAtCommit(t *testing.T) {
	d := newDB(t)
	graph, err := OpenDrevo(testDSN(t, "RHIZODAT_TEST_DREVO_BOLT"), "graph")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(graph.Close)
	c, err := NewCoordinator(d.journal())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	typeName := "ops.VersionedService" + strings.ReplaceAll(string(newUUID(t)), "-", "")[20:]
	services, err := NewRepository[Service](c, []PartStore{d.namedStore("inventory"), graph}, EntityOptions{Type: typeName})
	if err != nil {
		t.Fatal(err)
	}
	if err := services.Install(); err != nil {
		t.Fatal(err)
	}
	svc := &Service{Name: "billing", Region: "eu-1", Tier: 1}
	if _, err := services.Add(svc); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { services.Delete(svc.DatID) })
	mine, _ := services.Get(svc.DatID, ReadOptions{})
	theirs, _ := services.Get(svc.DatID, ReadOptions{})

	runs := 0
	_, err = c.Transactional(3, func(u *Unit) error {
		runs++
		mine.Tier = 2
		if err := services.In(u).Update(mine); err != nil { // checked: still the latest
			return err
		}
		if runs == 1 {
			theirs.Region = "us-2"
			if _, err := services.Update(theirs); err != nil { // commits in between
				t.Fatal(err)
			}
		}
		return nil
	})
	var conflict *VersionConflictError
	if !errors.As(err, &conflict) || conflict.Expected != mine.DatVersion || runs != 1 {
		t.Fatalf("the commit after the move: %v (runs %d)", err, runs)
	}
	if conflict.Actual == nil || *conflict.Actual != theirs.DatVersion || conflict.TypeName != typeName {
		t.Fatalf("conflict %+v, theirs at %d", conflict, theirs.DatVersion)
	}
	latest, _ := services.Get(svc.DatID, ReadOptions{})
	if latest.Region != "us-2" || latest.Tier != 1 {
		t.Fatalf("their write stays, nothing of mine: %+v", latest)
	}
}
