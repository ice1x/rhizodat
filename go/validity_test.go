package rhizodat

import (
	"strings"
	"testing"
	"time"
)

// A contract's price holds for a period (valid time), and the repository
// knows each price from when it was committed (transaction time).
type Contract struct {
	Entity
	Validity
	Customer string `rhizodat:"customer,store=billing"`
	Price    int    `rhizodat:"price,store=billing"`
}

func day(month time.Month, d int) time.Time { return time.Date(2026, month, d, 0, 0, 0, 0, time.UTC) }

func TestAValidTimeEntityIsReadAsItHeldAndAsItWasKnown(t *testing.T) {
	d := newDB(t)
	c, err := NewCoordinator(nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	contracts, err := NewRepository[Contract](c, []PartStore{d.namedStore("billing")}, EntityOptions{Type: "billing.Contract"})
	if err != nil {
		t.Fatal(err)
	}
	if err := contracts.Install(); err != nil {
		t.Fatal(err)
	}

	contract := &Contract{Validity: Validity{ValidFrom: day(1, 1)}, Customer: "acme", Price: 10}
	first, err := contracts.Add(contract)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	raised := *contract
	raised.ValidFrom, raised.Price = day(3, 1), 12
	if _, err := contracts.Update(&raised); err != nil {
		t.Fatal(err)
	}

	at := func(when time.Time, known *time.Time) *Contract {
		t.Helper()
		got, err := contracts.Get(contract.DatID, ReadOptions{AsOf: &when, KnownAt: known})
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := at(day(2, 1), nil); got == nil || got.Price != 10 || !got.ValidFrom.Equal(day(1, 1)) {
		t.Fatalf("in February the first price held: %+v", got)
	}
	if got := at(day(4, 1), nil); got == nil || got.Price != 12 {
		t.Fatalf("in April the raised price holds: %+v", got)
	}
	then := hlcTime(first.DatTxTime)
	if got := at(day(4, 1), &then); got == nil || got.Price != 10 {
		t.Fatalf("before the raise was known, April had the first price: %+v", got)
	}
	if got := at(day(1, 1).Add(-time.Hour), nil); got != nil {
		t.Fatalf("before any period: %+v", got)
	}
	if got, _ := contracts.Get(contract.DatID, ReadOptions{}); got == nil || got.Price != 12 {
		t.Fatalf("now: %+v", got)
	}
	if history, _ := contracts.History(contract.DatID); len(history) != 2 || !history[1].ValidFrom.Equal(day(3, 1)) {
		t.Fatalf("history %+v", history)
	}

	ended := raised
	ended.ValidTo = day(6, 1)
	if _, err := contracts.Update(&ended); err != nil {
		t.Fatal(err)
	}
	if got := at(day(5, 31), nil); got == nil || !got.ValidTo.Equal(day(6, 1)) {
		t.Fatalf("before the end: %+v", got)
	}
	// As in the Python reference, a read takes the latest version whose period
	// holds: past the narrowed period, the earlier open-ended one still does.
	if got := at(day(7, 1), nil); got == nil || !got.ValidTo.IsZero() || got.Price != 12 {
		t.Fatalf("after the end: %+v", got)
	}

	backwards := *contract
	backwards.ValidFrom, backwards.ValidTo = day(5, 1), day(4, 1)
	if _, err := contracts.Update(&backwards); err == nil || !strings.Contains(err.Error(), "ValidFrom must be before ValidTo") {
		t.Fatalf("a period that ends before it starts: %v", err)
	}
}

func TestValidityColumnsAreAddedToEveryPart(t *testing.T) {
	d := newDB(t)
	c, _ := NewCoordinator(nil)
	t.Cleanup(c.Close)
	repo, err := NewRepository[Contract](c, []PartStore{d.namedStore("billing")}, EntityOptions{Type: "billing.Contract"})
	if err != nil {
		t.Fatal(err)
	}
	spec := repo.kind.specs["billing"]
	var names []string
	for _, column := range spec.Columns {
		names = append(names, column.Name+":"+column.Type)
	}
	if got := strings.Join(names, ","); got != "customer:text,price:bigint,valid_from:timestamptz,valid_to:timestamptz" {
		t.Fatalf("columns %s", got)
	}
}
