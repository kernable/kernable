// A read model's store: a key-value store with namespaces in a SQLite file (or in memory).
// The listener writes its data and its position in one
// transaction per batch, so after a crash every event has taken effect exactly once, and a
// restart continues where it left off instead of replaying everything.

import type { DatabaseSync } from "node:sqlite";

/**
 * A listener's store: JSON values under `(namespace, key)` in a SQLite file (or in memory),
 * written in one transaction with the listener's position per batch of events. So after a crash
 * every event has taken effect exactly once, and a restart continues after the stored position
 * instead of rebuilding. Give a read model one with `ListenerOptions.store`; its handlers get it
 * as `ctx.store` (also query handlers), or use `ListenerHandle.store`. Reads and writes are
 * synchronous. Uses `node:sqlite`.
 *
 * ```ts
 * const carts = shopDomain.readModel("carts", { store: "sqlite:carts.db" });
 * carts.on("ItemAdded", (data: { qty: number }, evt, ctx) => {
 *   const row = ctx.store!.get<{ items: number }>("carts", evt.aggregateId) ?? { items: 0 };
 *   ctx.store!.set("carts", evt.aggregateId, { items: row.items + data.qty });
 * });
 * carts.query("GetCart", (data: { id: string }, _qry, ctx) => ctx.store!.get("carts", data.id));
 * ```
 */
export class Store {
  private readonly db: DatabaseSync;
  /** The global position up to which the stored data is complete: the last committed batch. */
  committed: number;

  private constructor(db: DatabaseSync, committed: number) {
    this.db = db;
    this.committed = committed;
  }

  /**
   * Opens `url` (`sqlite:<path>`, a bare path, or `memory:`). If the stored data was built at
   * another `schema`, it is cleared and the read model rebuilds from the first event.
   */
  static async open(url: string, schema: string): Promise<Store> {
    // Loaded only when a read model has a store: node:sqlite still prints an experimental warning.
    const { DatabaseSync } = await import("node:sqlite");
    const path = url === "memory:" ? ":memory:" : url.replace(/^sqlite:/, "");
    const db = new DatabaseSync(path);
    db.exec(`PRAGMA journal_mode = WAL;
      PRAGMA secure_delete = ON;
      CREATE TABLE IF NOT EXISTS data (ns TEXT NOT NULL, key TEXT NOT NULL, value TEXT NOT NULL, PRIMARY KEY (ns, key));
      CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);`);
    const meta = (key: string) => (db.prepare("SELECT value FROM meta WHERE key = ?").get(key) as { value: string } | undefined)?.value;
    if (meta("schema") !== schema) {
      db.exec("BEGIN");
      db.exec("DELETE FROM data");
      db.exec("DELETE FROM meta WHERE key LIKE 'position:%'");
      db.prepare("INSERT OR REPLACE INTO meta (key, value) VALUES ('schema', ?), ('position', '0')").run(schema);
      db.exec("COMMIT");
    }
    const store = new Store(db, Number(meta("position") ?? 0));
    // A crash after a batch that erased a subject, before the log was emptied, left the removed
    // values' old pages in it: empty it before anything else.
    store.checkpoint();
    return store;
  }

  /** The value under `key` in namespace `ns`, or `undefined` if there is none. */
  get<T = unknown>(ns: string, key: string): T | undefined {
    const row = this.db.prepare("SELECT value FROM data WHERE ns = ? AND key = ?").get(ns, key) as { value: string } | undefined;
    return row ? (JSON.parse(row.value) as T) : undefined;
  }

  /** Stores `value` (as JSON) under `key` in namespace `ns`, replacing a value there. */
  set(ns: string, key: string, value: unknown): void {
    this.db.prepare("INSERT OR REPLACE INTO data (ns, key, value) VALUES (?, ?, ?)").run(ns, key, JSON.stringify(value));
  }

  /** Removes the value under `key` in namespace `ns`, if there is one. */
  remove(ns: string, key: string): void {
    this.db.prepare("DELETE FROM data WHERE ns = ? AND key = ?").run(ns, key);
  }

  /** Every key and value of a namespace, ordered by key. */
  all<T = unknown>(ns: string): [string, T][] {
    const rows = this.db.prepare("SELECT key, value FROM data WHERE ns = ? ORDER BY key").all(ns) as { key: string; value: string }[];
    return rows.map((r) => [r.key, JSON.parse(r.value) as T]);
  }

  /** Clears a namespace, or all data. */
  reset(ns?: string): void {
    if (ns === undefined) this.db.exec("DELETE FROM data");
    else this.db.prepare("DELETE FROM data WHERE ns = ?").run(ns);
  }

  /** @internal Starts a batch. */
  begin(): void {
    this.db.exec("BEGIN");
  }

  /**
   * @internal The position up to which the data is complete for the events of partition `part`:
   * every partition has its own positions. `""`: a kernel without partitions.
   */
  committedFor(part: string): number {
    if (part === "") return this.committed;
    const row = this.db.prepare("SELECT value FROM meta WHERE key = ?").get(`position:${part}`) as { value: string } | undefined;
    return Number(row?.value ?? 0);
  }

  /**
   * @internal Ends a batch: its data and `position` (of partition `part`, see `committedFor`)
   * take effect together. After a batch with an erasure (`erased`), the log of old pages is
   * emptied too: what the batch removed must not stay on disk.
   */
  commit(position: number, erased = false, part = ""): void {
    if (part === "") {
      this.db.prepare("UPDATE meta SET value = ? WHERE key = 'position'").run(String(position));
    } else {
      this.db.prepare("INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)").run(`position:${part}`, String(position));
    }
    this.db.exec("COMMIT");
    if (part === "") this.committed = position;
    if (erased) this.checkpoint();
  }

  /** @internal Marks where an event's writes begin within a batch, so they can be dropped alone. */
  savepoint(): void {
    this.db.exec("SAVEPOINT event");
  }

  /** @internal Keeps the event's writes (see `savepoint`). */
  release(): void {
    this.db.exec("RELEASE event");
  }

  /** @internal Drops the event's writes, keeping the rest of the batch (see `savepoint`). */
  rollbackEvent(): void {
    this.db.exec("ROLLBACK TO event; RELEASE event");
  }

  /** @internal Drops a failed batch. */
  rollback(): void {
    if (this.db.isTransaction) this.db.exec("ROLLBACK");
  }

  /** Closes the database, leaving no old pages behind. */
  close(): void {
    if (!this.db.isOpen) return;
    this.checkpoint();
    this.db.close();
  }

  /**
   * Writes the write-ahead log into the database file and truncates it: until then, the log
   * keeps old page images, values deleted since included (`secure_delete` clears only the
   * database file).
   */
  private checkpoint(): void {
    this.db.exec("PRAGMA wal_checkpoint(TRUNCATE)");
  }
}
