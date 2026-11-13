package rhizodat

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"
)

// testDSN is the environment's var, else the one in ../.env.test when its
// server answers; otherwise the test is skipped (a DSN from the environment
// must work).
func testDSN(t *testing.T, name string) string {
	t.Helper()
	if dsn := os.Getenv(name); dsn != "" {
		return dsn
	}
	f, err := os.Open("../.env.test")
	if err != nil {
		t.Skipf("no %s and no .env.test", name)
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		if dsn, ok := strings.CutPrefix(scanner.Text(), name+"="); ok {
			u, err := url.Parse(dsn)
			if err == nil {
				if conn, err := net.DialTimeout("tcp", u.Host, 500*time.Millisecond); err == nil {
					conn.Close()
					return dsn
				}
			}
			t.Skipf("%s from .env.test is not running (docker compose up -d --wait)", name)
		}
	}
	t.Skipf("no %s in .env.test", name)
	return ""
}

func pgDSN(t *testing.T) string { return testDSN(t, "RHIZODAT_TEST_PG_DSN") }

// db is a test's own schemas, dropped after it: base_<store> per store (with
// an events table) and base_journal.
type db struct {
	t     *testing.T
	dsn   string
	base  string
	admin *PostgresStore
}

func newDB(t *testing.T) *db {
	dsn := pgDSN(t)
	id, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	admin, err := OpenPostgres(dsn, "admin", "")
	if err != nil {
		t.Fatal(err)
	}
	d := &db{t: t, dsn: dsn, base: "go_" + strings.ReplaceAll(id, "-", "")[20:], admin: admin}
	t.Cleanup(func() {
		rows, err := admin.Execute("SELECT nspname FROM pg_namespace WHERE nspname LIKE $1", d.base+"%")
		if err == nil {
			for _, r := range rows.Values {
				admin.Execute(fmt.Sprintf("DROP SCHEMA %s CASCADE", r[0]))
			}
		}
		admin.Close()
	})
	return d
}

func (d *db) exec(sql string, params ...any) *Rows {
	d.t.Helper()
	rows, err := d.admin.Execute(sql, params...)
	if err != nil {
		d.t.Fatal(err)
	}
	return rows
}

func (d *db) store(name string) *PostgresStore {
	d.t.Helper()
	schema := d.base + "_" + name
	d.exec("CREATE SCHEMA " + schema)
	d.exec("CREATE TABLE " + schema + ".resource (name text, region text, size bigint, dat_txn uuid, dat_tx_time bigint)")
	s, err := OpenPostgres(d.dsn, name+"_"+d.base, schema)
	if err != nil {
		d.t.Fatal(err)
	}
	if err := s.Install(); err != nil {
		d.t.Fatal(err)
	}
	d.t.Cleanup(s.Close)
	return s
}

func (d *db) journal() *Journal {
	d.t.Helper()
	d.exec("CREATE SCHEMA " + d.base + "_journal")
	j, err := OpenPostgresJournal(d.dsn, d.base+"_journal")
	if err != nil {
		d.t.Fatal(err)
	}
	d.t.Cleanup(j.Close)
	return j
}

func (d *db) count(name string) int64 {
	return d.exec("SELECT count(*) AS n FROM " + d.base + "_" + name + ".resource").Values[0][0].(int64)
}

func TestExecuteBindsAndReturnsTypedValues(t *testing.T) {
	d := newDB(t)
	at := time.Date(2026, 11, 14, 9, 30, 0, 0, time.UTC)
	rows := d.exec(
		// A date or time binds through text in the native port: $n::text::timestamptz.
		"SELECT $1::uuid AS id, $2::numeric AS price, $3::text AS name, $4::bigint AS n, $5::text::timestamptz AS at, NULL::int AS gone",
		UUID("01920000-0000-7000-8000-000000000001"), Decimal("12.3400"), "web1", 42, at.Format(time.RFC3339),
	)
	if strings.Join(rows.Columns, ",") != "id,price,name,n,at,gone" {
		t.Fatalf("columns %v", rows.Columns)
	}
	row := rows.Maps()[0]
	if row["id"] != UUID("01920000-0000-7000-8000-000000000001") || row["price"] != Decimal("12.3400") ||
		row["name"] != "web1" || row["n"] != int64(42) || row["gone"] != nil {
		t.Fatalf("row %#v", row)
	}
	if got, ok := row["at"].(time.Time); !ok || !got.Equal(at) {
		t.Fatalf("at came back as %#v", row["at"])
	}
}

func TestAServerErrorCarriesTheServersMessage(t *testing.T) {
	d := newDB(t)
	_, err := d.admin.Execute("SELECT * FROM no_such_table")
	if !errors.Is(err, ErrFailed) || !strings.Contains(err.Error(), "42P01") {
		t.Fatalf("got %v", err)
	}
}

func TestOneStoreCommitsInOnePhase(t *testing.T) {
	d := newDB(t)
	inventory := d.store("inventory")
	coordinator, err := NewCoordinator(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	done, err := coordinator.Transactional(3, func(u *Unit) error {
		return u.Insert(inventory, "resource", []string{"name", "region", "size"}, [][]any{
			{"web1", "eu-west-1", 2}, {"db1", "eu-west-1", 8},
		})
	})
	if err != nil {
		t.Fatal(err)
	}
	if done.Pending || done.DatTxTime <= 0 || done.DatTxn == "" {
		t.Fatalf("got %+v", done)
	}
	if n := d.count("inventory"); n != 2 {
		t.Fatalf("%d rows", n)
	}
	txns := d.exec("SELECT DISTINCT dat_txn FROM " + d.base + "_inventory.resource")
	if len(txns.Values) != 1 || txns.Values[0][0] != done.DatTxn {
		t.Fatalf("rows carry %v, the commit says %s", txns.Values, done.DatTxn)
	}
}

func TestTwoStoresCommitTogetherThroughTheJournal(t *testing.T) {
	d := newDB(t)
	inventory, billing := d.store("inventory"), d.store("billing")
	coordinator, err := NewCoordinator(d.journal())
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	done, err := coordinator.Transactional(3, func(u *Unit) error {
		if err := u.Insert(inventory, "resource", []string{"name", "size"}, [][]any{{"web1", 2}}); err != nil {
			return err
		}
		return u.Insert(billing, "resource", []string{"name", "size"}, [][]any{{"web1", 200}, {"web1", 300}})
	})
	if err != nil {
		t.Fatal(err)
	}
	if a, b := d.count("inventory"), d.count("billing"); a != 1 || b != 2 {
		t.Fatalf("counts %d, %d", a, b)
	}
	state := d.exec("SELECT state FROM "+d.base+"_journal.dat_journal WHERE dat_txn = $1", done.DatTxn)
	if len(state.Values) != 1 {
		t.Fatalf("journal has %v", state.Values)
	}
}

func TestAStoreThatIsDownAbortsRetryablyAndWritesNothing(t *testing.T) {
	d := newDB(t)
	inventory := d.store("inventory")
	down, err := OpenPostgres("postgresql://postgres:x@127.0.0.1:1/none?connect_timeout=2", "down", "")
	if err != nil {
		t.Fatal(err)
	}
	defer down.Close()
	coordinator, err := NewCoordinator(d.journal())
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	attempts := 0
	_, err = coordinator.Transactional(2, func(u *Unit) error {
		attempts++
		if err := u.Insert(inventory, "resource", []string{"name"}, [][]any{{"web1"}}); err != nil {
			return err
		}
		return u.Insert(down, "resource", []string{"name"}, [][]any{{"web1"}})
	})
	var aborted *AbortedError
	if !errors.As(err, &aborted) || !aborted.Retryable || aborted.DatTxn == "" || !errors.Is(err, ErrAborted) {
		t.Fatalf("got %v", err)
	}
	if attempts != 2 {
		t.Fatalf("%d attempts, want 2 (a retryable abort is re-run)", attempts)
	}
	if n := d.count("inventory"); n != 0 {
		t.Fatalf("%d rows written", n)
	}
}

func TestABuildErrorWritesNothingAndIsNotRetried(t *testing.T) {
	d := newDB(t)
	inventory := d.store("inventory")
	coordinator, err := NewCoordinator(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	mine := errors.New("validation failed")
	attempts := 0
	_, err = coordinator.Transactional(3, func(u *Unit) error {
		attempts++
		u.Insert(inventory, "resource", []string{"name"}, [][]any{{"web1"}})
		return mine
	})
	if !errors.Is(err, mine) || attempts != 1 || d.count("inventory") != 0 {
		t.Fatalf("err %v after %d attempts, %d rows", err, attempts, d.count("inventory"))
	}
}

func TestRefusals(t *testing.T) {
	d := newDB(t)
	a, b := d.store("a"), d.store("b")
	coordinator, err := NewCoordinator(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer coordinator.Close()
	_, err = coordinator.Transactional(1, func(u *Unit) error {
		u.Insert(a, "resource", []string{"name"}, [][]any{{"x"}})
		return u.Insert(b, "resource", []string{"name"}, [][]any{{"x"}})
	})
	if !errors.Is(err, ErrInvalidArgument) || !strings.Contains(err.Error(), "journal") {
		t.Fatalf("two stores without a journal: %v", err)
	}
	unit, _ := NewUnit()
	defer unit.Close()
	if err := unit.Insert(a, "resource", []string{"name", "dat_txn"}, [][]any{{"x", nil}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a reserved column: %v", err)
	}
	if err := unit.Insert(a, "resource", []string{"name"}, [][]any{{struct{}{}}}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a value with no rhizodat type: %v", err)
	}
	if _, err := OpenPostgres("mysql://nope", "x", ""); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("a bad DSN: %v", err)
	}
}
