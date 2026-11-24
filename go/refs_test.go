package rhizodat

import (
	"reflect"
	"strings"
	"testing"
)

type Person struct {
	Entity
	Name string `rhizodat:"name,store=people"`
}

// A ticket refers to people: its assignee from PostgreSQL and the graph, its
// watchers in the graph, where each version links the people it names.
type Ticket struct {
	Entity
	Title    string        `rhizodat:"title,store=people"`
	Assignee Ref[Person]   `rhizodat:"assignee,store=graph"`
	Reviewer *Ref[Person]  `rhizodat:"reviewer,store=people"`
	Watchers []Ref[Person] `rhizodat:"watchers,store=graph"`
}

func TestReferencesAreStoredAsIdsAndLinkedInTheGraph(t *testing.T) {
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
	people := d.namedStore("people")
	suffix := strings.ReplaceAll(string(newUUID(t)), "-", "")[20:]
	persons, err := NewRepository[Person](c, []PartStore{people}, EntityOptions{Type: "desk.Person"})
	if err != nil {
		t.Fatal(err)
	}
	tickets, err := NewRepository[Ticket](c, []PartStore{people, graph}, EntityOptions{Type: "desk.Ticket" + suffix})
	if err != nil {
		t.Fatal(err)
	}
	for _, install := range []func() error{persons.Install, tickets.Install} {
		if err := install(); err != nil {
			t.Fatal(err)
		}
	}

	ada, grace := &Person{Name: "Ada"}, &Person{Name: "Grace"}
	for _, p := range []*Person{ada, grace} {
		if _, err := persons.Add(p); err != nil {
			t.Fatal(err)
		}
	}
	reviewer := RefTo(grace)
	ticket := &Ticket{Title: "Crash", Assignee: RefTo(ada), Reviewer: &reviewer,
		Watchers: []Ref[Person]{RefTo(ada), RefTo(grace)}}
	if _, err := tickets.Add(ticket); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { tickets.Delete(ticket.DatID) })

	got, err := tickets.Get(ticket.DatID, ReadOptions{})
	if err != nil || got == nil {
		t.Fatalf("get: %+v %v", got, err)
	}
	if got.Assignee.DatID != ada.DatID || got.Reviewer == nil || got.Reviewer.DatID != grace.DatID ||
		!reflect.DeepEqual(got.Watchers, []Ref[Person]{{ada.DatID}, {grace.DatID}}) {
		t.Fatalf("references %+v", got)
	}
	whom, err := persons.Get(got.Assignee.DatID, ReadOptions{})
	if err != nil || whom == nil || whom.Name != "Ada" {
		t.Fatalf("the assignee %+v %v", whom, err)
	}

	label := partTable("desk.Ticket" + suffix)
	rows, err := graph.Execute("MATCH (v:"+label+" {dat_id: $id})-[:watchers]->(a:DatEntity) RETURN a.dat_id AS id ORDER BY id",
		Params{"id": string(ticket.DatID)})
	if err != nil || len(rows.Values) != 2 {
		t.Fatalf("the version links its watchers: %+v %v", rows, err)
	}
	rows, err = graph.Execute("MATCH (v:"+label+" {dat_id: $id})-[:assignee]->(a:DatEntity) RETURN a.dat_id AS id",
		Params{"id": string(ticket.DatID)})
	if err != nil || len(rows.Values) != 1 || rows.Values[0][0] != string(ada.DatID) {
		t.Fatalf("the version links its assignee: %+v %v", rows, err)
	}

	got.Reviewer, got.Assignee = nil, Ref[Person]{}
	if _, err := tickets.Update(got); err != nil {
		t.Fatal(err)
	}
	if latest, _ := tickets.Get(ticket.DatID, ReadOptions{}); latest.Reviewer != nil || latest.Assignee.DatID != "" {
		t.Fatalf("cleared references %+v", latest)
	}
}

func TestReferenceFieldsAreMarkedAndTyped(t *testing.T) {
	cases := []struct {
		value               any
		pg, drevo, ch, berg string
		reference           bool
	}{
		{new(Ref[Person]), "uuid", "uuid", "UUID", "id", true},
		{new(*Ref[Person]), "uuid", "uuid", "UUID", "id", true},
		{new([]Ref[Person]), "jsonb", "json", "json", "json", true},
		{new(map[string]Ref[Person]), "jsonb", "json", "json", "json", true},
		{new(UUID), "uuid", "uuid", "UUID", "id", false},
		{new([]string), "jsonb", "json", "json", "json", false},
	}
	for _, c := range cases {
		typ := reflect.TypeOf(c.value).Elem()
		pg, _ := postgresColumnType(typ)
		drevo, _ := drevoFieldKind(typ)
		ch, _ := clickhouseColumnType(typ)
		berg, _ := icebergFieldKind(typ)
		if pg != c.pg || drevo != c.drevo || ch != c.ch || berg != c.berg || holdsRefs(typ) != c.reference {
			t.Errorf("%v: %s %s %s %s %v", typ, pg, drevo, ch, berg, holdsRefs(typ))
		}
	}
	if ref := RefTo(&Person{Entity: Entity{DatID: "x"}}); ref.DatID != "x" {
		t.Fatalf("RefTo %+v", ref)
	}
}
