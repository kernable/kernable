"""A read model's store: a key-value store with namespaces in a SQLite file (or in memory). The listener writes its data and its position in one transaction
per batch, so after a crash every event has taken effect exactly once, and a restart continues
where it left off instead of replaying everything."""

import json
import sqlite3
import threading
from typing import Any


class Store:
    """A listener's store: JSON values under keys, grouped in namespaces (`ns`), in a SQLite file
    or in memory. Give a read model or reactor one with `store="sqlite:carts.db"`; its handlers
    then get it as `ctx.store` (`ListenerContext`, `QueryContext`), e.g.
    `ctx.store.set("carts", evt.aggregate_id, row)` and `ctx.store.get("carts", data["id"])`.
    What an event handler writes takes effect together with the listener's position, once per
    batch. Thread-safe."""

    def __init__(self, url: str, schema: str):
        """Opens `url` (`sqlite:<path>`, a bare path, or `memory:`). If the stored data was built at
        another `schema`, it is cleared and the read model rebuilds from the first event."""
        path = ":memory:" if url == "memory:" else url.removeprefix("sqlite:")
        self._db = sqlite3.connect(path, isolation_level=None, check_same_thread=False)
        self._lock = threading.RLock()
        self._closed = False
        self._db.executescript("""
            PRAGMA journal_mode = WAL;
            PRAGMA secure_delete = ON;
            CREATE TABLE IF NOT EXISTS data (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL,
                                             PRIMARY KEY (ns, key));
            CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        """)
        if self._meta("schema") != schema:
            with self._lock:
                self._db.execute("BEGIN")
                self._db.execute("DELETE FROM data")
                self._db.execute("INSERT OR REPLACE INTO meta (key, value) VALUES ('schema', ?), ('position', '0')",
                                 (schema,))
                self._db.execute("COMMIT")
        #: The position up to which the stored data is complete.
        self.committed = int(self._meta("position") or 0)

    def _meta(self, key: str) -> str | None:
        row = self._db.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
        return row[0] if row else None

    def get(self, ns: str, key: str, default: Any = None) -> Any:
        """The value under `key` in namespace `ns`, decoded from JSON, or `default` if there is
        none."""
        with self._lock:
            row = self._db.execute("SELECT value FROM data WHERE ns = ? AND key = ?", (ns, key)).fetchone()
        return json.loads(row[0]) if row else default

    def set(self, ns: str, key: str, value: Any) -> None:
        """Stores `value` (anything `json.dumps` takes) under `key` in namespace `ns`, replacing
        a value there."""
        with self._lock:
            self._db.execute("INSERT OR REPLACE INTO data (ns, key, value) VALUES (?, ?, ?)",
                             (ns, key, json.dumps(value)))

    def remove(self, ns: str, key: str) -> None:
        """Removes the value under `key` in namespace `ns`, if there is one."""
        with self._lock:
            self._db.execute("DELETE FROM data WHERE ns = ? AND key = ?", (ns, key))

    def all(self, ns: str) -> list[tuple[str, Any]]:
        """Every key and value of a namespace, ordered by key."""
        with self._lock:
            rows = self._db.execute("SELECT key, value FROM data WHERE ns = ? ORDER BY key", (ns,)).fetchall()
        return [(k, json.loads(v)) for k, v in rows]

    def reset(self, ns: str | None = None) -> None:
        """Clears a namespace, or all data."""
        with self._lock:
            if ns is None:
                self._db.execute("DELETE FROM data")
            else:
                self._db.execute("DELETE FROM data WHERE ns = ?", (ns,))

    def _begin(self) -> None:
        self._lock.acquire()
        try:
            self._db.execute("BEGIN")
        except BaseException:
            self._lock.release()
            raise

    def _commit(self, position: int) -> None:
        """Ends the batch; if this raises, the batch is still open and `_rollback` ends it."""
        self._db.execute("UPDATE meta SET value = ? WHERE key = 'position'", (str(position),))
        self._db.execute("COMMIT")
        self.committed = position
        self._lock.release()

    def _checkpoint(self) -> None:
        """Copies the WAL into the database and truncates it: old page images, e.g. of values a
        read model removed after an erasure, stay in the WAL until then (`secure_delete` clears
        only the database's freed pages)."""
        with self._lock:
            self._db.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    def _rollback(self) -> None:
        try:
            if self._db.in_transaction:
                self._db.execute("ROLLBACK")
        finally:
            self._lock.release()

    def close(self) -> None:
        """Closes the database, truncating its WAL first; the store cannot be used afterwards. A
        listener closes its store when its `run()` ends; closing it again does nothing."""
        with self._lock:
            if self._closed:
                return
            self._closed = True
            self._db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
            self._db.close()
