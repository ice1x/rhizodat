package rhizodat

import (
	"errors"
	"reflect"
	"testing"
)

var issueSpec = PartSpec{
	Type:  "tracker.Issue",
	Table: "tracker_issue",
	Columns: []PartColumn{
		{"title", "text"},
		{"points", "bigint"},
		{"tags", "jsonb"},
	},
}

func newUUID(t *testing.T) UUID {
	t.Helper()
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	return UUID(id)
}

func commitUnit(t *testing.T, c *Coordinator, build func(*Unit) error) Committed {
	t.Helper()
	u, err := NewUnit()
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if err := build(u); err != nil {
		t.Fatal(err)
	}
	done, err := c.Commit(u)
	if err != nil {
		t.Fatal(err)
	}
	return done
}

func TestPartsAreWrittenInAUnitAndReadByKnownTime(t *testing.T) {
	d := newDB(t)
	pg := d.store("pg")
	if err := pg.InstallPart(issueSpec); err != nil {
		t.Fatal(err)
	}
	c, err := NewCoordinator(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	id := newUUID(t)

	first := commitUnit(t, c, func(u *Unit) error {
		if own, err := u.OwnPart(pg, issueSpec, id); err != nil || own.Touched {
			t.Fatalf("an untouched part: %+v %v", own, err)
		}
		err := u.WritePart(pg, issueSpec, id, map[string]any{"title": "Crash on save", "points": 3, "tags": []string{"ui"}}, nil, false)
		if err != nil {
			return err
		}
		own, err := u.OwnPart(pg, issueSpec, id)
		if err != nil || !own.Touched || own.Version == nil || own.Version.Created != nil {
			t.Fatalf("the unit's own version: %+v %v", own, err)
		}
		if got := own.Version.Values; !reflect.DeepEqual(got, []any{"Crash on save", int64(3), []any{"ui"}}) {
			t.Fatalf("own values %#v", got)
		}
		return nil
	})

	v1, err := pg.ReadPart(issueSpec, id, nil)
	if err != nil || v1 == nil {
		t.Fatalf("read: %+v %v", v1, err)
	}
	if v1.DatTxn != first.DatTxn || v1.DatTxTime == nil || *v1.DatTxTime != first.DatTxTime ||
		v1.Created == nil || *v1.Created != first.DatTxTime || v1.Deleted {
		t.Fatalf("the first version %+v (commit %+v)", v1, first)
	}
	if got := v1.Get(issueSpec, "points"); got != int64(3) {
		t.Fatalf("points %#v", got)
	}

	created := first.DatTxTime
	commitUnit(t, c, func(u *Unit) error {
		return u.WritePart(pg, issueSpec, id, map[string]any{"title": "Crash on save", "points": 5}, &created, false)
	})
	latest, err := pg.ReadPart(issueSpec, id, nil)
	if err != nil || latest.Get(issueSpec, "points") != int64(5) || latest.Get(issueSpec, "tags") != nil || *latest.Created != created {
		t.Fatalf("the latest version %+v %v", latest, err)
	}
	if old, _ := pg.ReadPart(issueSpec, id, &created); old.Get(issueSpec, "points") != int64(3) {
		t.Fatalf("as known at the first commit: %+v", old)
	}
	before := created - 1
	if none, err := pg.ReadPart(issueSpec, id, &before); none != nil || err != nil {
		t.Fatalf("before the first commit: %+v %v", none, err)
	}

	history, err := pg.PartHistory(issueSpec, id)
	if err != nil || len(history) != 2 || history[0].Get(issueSpec, "points") != int64(3) {
		t.Fatalf("history %+v %v", history, err)
	}
	all, err := pg.PartVersions(issueSpec, nil)
	if err != nil || len(all) != 2 || all[0].DatID != id {
		t.Fatalf("every version %+v %v", all, err)
	}

	commitUnit(t, c, func(u *Unit) error {
		if err := u.ErasePart(pg, issueSpec, id); err != nil {
			return err
		}
		if own, err := u.OwnPart(pg, issueSpec, id); err != nil || !own.Touched || own.Version != nil {
			t.Fatalf("an erased part: %+v %v", own, err)
		}
		return nil
	})
	if gone, err := pg.ReadPart(issueSpec, id, nil); gone != nil || err != nil {
		t.Fatalf("erased: %+v %v", gone, err)
	}
}

func TestASoftDeletedPartExpires(t *testing.T) {
	d := newDB(t)
	pg := d.store("pg")
	if err := pg.InstallPart(issueSpec); err != nil {
		t.Fatal(err)
	}
	c, err := NewCoordinator(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	live, gone := newUUID(t), newUUID(t)
	done := commitUnit(t, c, func(u *Unit) error {
		if err := u.WritePart(pg, issueSpec, live, map[string]any{"title": "a"}, nil, false); err != nil {
			return err
		}
		return u.WritePart(pg, issueSpec, gone, map[string]any{"title": "b"}, nil, true)
	})
	if ids, err := pg.ExpiredParts(issueSpec, done.DatTxTime, 10); err != nil || len(ids) != 0 {
		t.Fatalf("not older than the bound: %v %v", ids, err)
	}
	ids, err := pg.ExpiredParts(issueSpec, done.DatTxTime+1, 10)
	if err != nil || !reflect.DeepEqual(ids, []UUID{gone}) {
		t.Fatalf("expired %v %v", ids, err)
	}
}

func TestMalformedPartsAreRefused(t *testing.T) {
	d := newDB(t)
	pg := d.store("pg")
	bad := PartSpec{Type: "x", Table: "x", Columns: []PartColumn{{"a", "varchar"}}}
	if err := pg.InstallPart(bad); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("an unknown column type: %v", err)
	}
	u, err := NewUnit()
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if err := u.WritePart(pg, issueSpec, newUUID(t), map[string]any{"nope": 1}, nil, false); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("an unknown field: %v", err)
	}
	if err := u.WritePart(pg, issueSpec, "not-a-uuid", nil, nil, false); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a bad id: %v", err)
	}
	if err := u.WritePart(pg, issueSpec, newUUID(t), map[string]any{"title": struct{}{}}, nil, false); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a value without a rhizodat type: %v", err)
	}
}
