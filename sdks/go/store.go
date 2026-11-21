package kernable

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strconv"
	"strings"
	"sync"

	_ "modernc.org/sqlite"
)

// Store is a listener's store: JSON values under (namespace, key) in a SQLite file (or in
// memory), written in one transaction with the listener's position per batch of events. So after
// a crash every event has taken effect exactly once, and a restart continues after the stored
// position instead of rebuilding. Give a read model one with the Store option; its handlers get
// it as ctx.Store (also query handlers). The tables are the other SDKs': a store file is
// readable by each.
type Store struct {
	mu   sync.Mutex
	db   *sql.DB
	conn *sql.Conn
	tx   bool
	// The global position up to which the stored data is complete: the last committed batch.
	committed uint64
}

// openStore opens url (sqlite:<path>, a bare path, or memory:). If the stored data was built at
// another schema, it is cleared and the read model rebuilds from the first event.
func openStore(ctx context.Context, url, schema string) (*Store, error) {
	path := strings.TrimPrefix(url, "sqlite:")
	if url == "memory:" {
		path = ":memory:"
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	conn, err := db.Conn(ctx)
	if err != nil {
		db.Close()
		return nil, err
	}
	s := &Store{db: db, conn: conn}
	fail := func(err error) (*Store, error) { conn.Close(); db.Close(); return nil, err }
	for _, q := range []string{
		"PRAGMA journal_mode = WAL",
		"PRAGMA secure_delete = ON",
		"CREATE TABLE IF NOT EXISTS data (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (ns, key))",
		"CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL)",
	} {
		if _, err := conn.ExecContext(ctx, q); err != nil {
			return fail(err)
		}
	}
	stored, _, err := s.meta(ctx, "schema")
	if err != nil {
		return fail(err)
	}
	if stored != schema {
		for _, q := range []string{"BEGIN", "DELETE FROM data", "DELETE FROM meta WHERE key LIKE 'position:%'"} {
			if _, err := conn.ExecContext(ctx, q); err != nil {
				return fail(err)
			}
		}
		if _, err := conn.ExecContext(ctx, "INSERT OR REPLACE INTO meta (key, value) VALUES ('schema', ?), ('position', '0')", schema); err != nil {
			return fail(err)
		}
		if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
			return fail(err)
		}
	}
	position, _, err := s.meta(ctx, "position")
	if err != nil {
		return fail(err)
	}
	s.committed, _ = strconv.ParseUint(position, 10, 64)
	// A crash after a batch that erased a subject, before the log was emptied, left the removed
	// values' old pages in it: empty it before anything else.
	if err := s.checkpoint(); err != nil {
		return fail(err)
	}
	return s, nil
}

func (s *Store) meta(ctx context.Context, key string) (string, bool, error) {
	var v string
	err := s.conn.QueryRowContext(ctx, "SELECT value FROM meta WHERE key = ?", key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	return v, err == nil, err
}

// Get reads the value under key in namespace ns into v; false if there is none.
func (s *Store) Get(ns, key string, v any) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var raw string
	err := s.conn.QueryRowContext(context.Background(), "SELECT value FROM data WHERE ns = ? AND key = ?", ns, key).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, unmarshal([]byte(raw), v)
}

// Set stores value (as JSON) under key in namespace ns, replacing a value there.
func (s *Store) Set(ns, key string, value any) error {
	b, err := encode(value)
	if err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err = s.conn.ExecContext(context.Background(), "INSERT OR REPLACE INTO data (ns, key, value) VALUES (?, ?, ?)", ns, key, string(b))
	return err
}

// Remove removes the value under key in namespace ns, if there is one.
func (s *Store) Remove(ns, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.conn.ExecContext(context.Background(), "DELETE FROM data WHERE ns = ? AND key = ?", ns, key)
	return err
}

// StoreEntry is a key and its value, as Store.All lists them.
type StoreEntry struct {
	Key   string
	Value json.RawMessage
}

// All is every key and value of a namespace, ordered by key.
func (s *Store) All(ns string) ([]StoreEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	rows, err := s.conn.QueryContext(context.Background(), "SELECT key, value FROM data WHERE ns = ? ORDER BY key", ns)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StoreEntry
	for rows.Next() {
		var k, v string
		if err := rows.Scan(&k, &v); err != nil {
			return nil, err
		}
		out = append(out, StoreEntry{k, json.RawMessage(v)})
	}
	return out, rows.Err()
}

// Reset clears a namespace, or all data without one.
func (s *Store) Reset(ns ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(ns) == 0 {
		_, err := s.conn.ExecContext(context.Background(), "DELETE FROM data")
		return err
	}
	for _, n := range ns {
		if _, err := s.conn.ExecContext(context.Background(), "DELETE FROM data WHERE ns = ?", n); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) exec(q string, args ...any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, err := s.conn.ExecContext(context.Background(), q, args...)
	return err
}

// begin starts a batch.
func (s *Store) begin() error {
	if err := s.exec("BEGIN"); err != nil {
		return err
	}
	s.tx = true
	return nil
}

// committedFor is the position up to which the data is complete for the events of partition
// part: every partition has its own positions. "": a kernel without partitions.
func (s *Store) committedFor(part string) uint64 {
	if part == "" {
		return s.committed
	}
	v, _, _ := s.meta(context.Background(), "position:"+part)
	n, _ := strconv.ParseUint(v, 10, 64)
	return n
}

// commit ends a batch: its data and position (of partition part) take effect together. After a
// batch with an erasure, the log of old pages is emptied too: what the batch removed must not
// stay on disk.
func (s *Store) commit(position uint64, erased bool, part string) error {
	p := strconv.FormatUint(position, 10)
	var err error
	if part == "" {
		err = s.exec("UPDATE meta SET value = ? WHERE key = 'position'", p)
	} else {
		err = s.exec("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)", "position:"+part, p)
	}
	if err != nil {
		return err
	}
	if err := s.exec("COMMIT"); err != nil {
		return err
	}
	s.tx = false
	if part == "" {
		s.committed = position
	}
	if erased {
		return s.checkpoint()
	}
	return nil
}

// savepoint marks where an event's writes begin within a batch, so they can be dropped alone.
func (s *Store) savepoint() error { return s.exec("SAVEPOINT event") }

// release keeps the event's writes.
func (s *Store) release() error { return s.exec("RELEASE event") }

// rollbackEvent drops the event's writes, keeping the rest of the batch.
func (s *Store) rollbackEvent() error {
	if err := s.exec("ROLLBACK TO event"); err != nil {
		return err
	}
	return s.exec("RELEASE event")
}

// rollback drops a failed batch.
func (s *Store) rollback() {
	if s.tx {
		_ = s.exec("ROLLBACK")
		s.tx = false
	}
}

// close closes the database, leaving no old pages behind.
func (s *Store) close() {
	_ = s.checkpoint()
	s.conn.Close()
	s.db.Close()
}

// checkpoint writes the write-ahead log into the database file and truncates it: until then,
// the log keeps old page images, values deleted since included.
func (s *Store) checkpoint() error { return s.exec("PRAGMA wal_checkpoint(TRUNCATE)") }
