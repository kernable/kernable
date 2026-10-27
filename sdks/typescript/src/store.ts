// A read model's store: a key-value store with namespaces in a SQLite file (or in memory). The listener writes its data and its position in one
// transaction per batch, so after a crash every event has taken effect exactly once, and a
// restart continues where it left off instead of replaying everything.

import type { DatabaseSync } from "node:sqlite";

export class Store {
  private readonly db: DatabaseSync;
  /** The position up to which the stored data is complete. */
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
      db.prepare("INSERT OR REPLACE INTO meta (key, value) VALUES ('schema', ?), ('position', '0')").run(schema);
      db.exec("COMMIT");
    }
    return new Store(db, Number(meta("position") ?? 0));
  }

  get<T = unknown>(ns: string, key: string): T | undefined {
    const row = this.db.prepare("SELECT value FROM data WHERE ns = ? AND key = ?").get(ns, key) as { value: string } | undefined;
    return row ? (JSON.parse(row.value) as T) : undefined;
  }

  set(ns: string, key: string, value: unknown): void {
    this.db.prepare("INSERT OR REPLACE INTO data (ns, key, value) VALUES (?, ?, ?)").run(ns, key, JSON.stringify(value));
  }

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

  /** @internal Ends a batch: its data and `position` take effect together. */
  commit(position: number): void {
    this.db.prepare("UPDATE meta SET value = ? WHERE key = 'position'").run(String(position));
    this.db.exec("COMMIT");
    this.committed = position;
  }

  /** @internal Drops a failed batch. */
  rollback(): void {
    if (this.db.isTransaction) this.db.exec("ROLLBACK");
  }

  close(): void {
    this.db.close();
  }
}
