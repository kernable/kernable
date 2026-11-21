// Kit parity: the same scenario through the TypeScript, Python, Rust and Go kits, with the kit's
// domains hosted in one language and the scenario driven through another's KitApi, every pair.
// What each run leaves (the step results, the mails, every stored event) is normalized and must
// be the same as the TypeScript/TypeScript run's, byte for byte. See README.md.
//
//   node --conditions=source --no-warnings kits/parity/run.ts [ts,py,rs,go] [ts,py,rs,go]
//
// (the first list: host languages, the second: driver languages; default all). `cargo build -p
// kernable-kernel`, `cargo build -p kernable-kit --example parity` and the Go runner (`go build -o
// target/debug/examples/parity-go ./internal/kitparity` in sdks/go) first; the Python SDK's venv
// (`make sdk-python-venv` or as in the Makefile). Results land in target/parity/.

import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import { Kernable, type StoredEvent } from "../../sdks/typescript/src/index.ts";
import { TestKernel } from "../../sdks/typescript/test/kernel.ts";

const ROOT = new URL("../../", import.meta.url).pathname;
const KITS = join(ROOT, "kits");
const OUT = join(ROOT, "target", "parity");
const SCENARIO = join(KITS, "parity", "scenario.json");
const KERNABLE = join(ROOT, "target", process.env.KERNABLE_BIN ?? "debug", "kernable");
const PYTHON = join(ROOT, "sdks", "python", ".venv", "bin", "python");

type Lang = "ts" | "py" | "rs" | "go";
const LANGS: Lang[] = ["ts", "py", "rs", "go"];
const hosts = (process.argv[2]?.split(",") ?? LANGS) as Lang[];
const drivers = (process.argv[3]?.split(",") ?? LANGS) as Lang[];

function command(lang: Lang, role: "host" | "driver"): { cmd: string; args: string[]; cwd: string; env?: Record<string, string> } {
  switch (lang) {
    case "ts":
      return { cmd: "node", args: ["--conditions=source", "--no-warnings", "test/parity.ts", role], cwd: join(KITS, "typescript") };
    case "py":
      return { cmd: PYTHON, args: ["-m", "tests.parity", role], cwd: join(KITS, "python"), env: { PYTHONPATH: `${join(ROOT, "sdks", "python")}:${join(KITS, "python")}` } };
    case "rs":
      return { cmd: join(ROOT, "target", "debug", "examples", "parity"), args: [role], cwd: join(KITS, "rust") };
    case "go":
      return { cmd: join(ROOT, "target", "debug", "examples", "parity-go"), args: [role], cwd: join(ROOT, "sdks", "go") };
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

function startHost(lang: Lang, env: Record<string, string>, log: string): Promise<ChildProcess> {
  const c = command(lang, "host");
  const child = spawn(c.cmd, c.args, { cwd: c.cwd, env: { ...process.env, ...env, ...c.env }, stdio: ["ignore", "pipe", "pipe"] });
  let err = "";
  child.stderr!.on("data", (d) => {
    err += d;
    writeFileSync(log, err);
  });
  return new Promise((resolve, reject) => {
    let out = "";
    const timer = setTimeout(() => reject(new Error(`${lang} host not ready in 60 s:\n${err}`)), 60_000);
    child.stdout!.on("data", (d) => {
      out += d;
      if (out.includes("ready\n")) {
        clearTimeout(timer);
        resolve(child);
      }
    });
    child.on("exit", (code) => {
      clearTimeout(timer);
      reject(new Error(`${lang} host exited (${code}):\n${err}`));
    });
  });
}

async function stopHost(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null) return;
  child.removeAllListeners("exit");
  const exited = new Promise((r) => child.once("exit", r));
  child.kill("SIGTERM");
  await Promise.race([exited, sleep(5000)]);
  if (child.exitCode === null) child.kill("SIGKILL");
}

async function runPair(host: Lang, driverLang: Lang): Promise<{ transcript: unknown[]; mails: unknown[]; events: StoredEvent[] }> {
  const kernel = new TestKernel(["*"], [], { processes: true });
  await kernel.start();
  try {
    const cli = (...args: string[]) => {
      const r = kernel.cli(...args);
      if (!r.ok) throw new Error(`kernable ${args.join(" ")}: ${r.out}`);
    };
    cli("keys", "new", "kit");
    cli(
      "grant", "root.key", "kit.pub",
      "run=tenancy.*,account.*,identity.*,group.*,invitation.*,shop.*",
      ...["submit", "subscribe", "listen", "query", "answer", "check", "vault_read", "vault_erase", "manage", "reserved_order", "cache"].map((a) => `${a}=*`),
      "privileged=bypass_authorization",
      "process=*",
    );
    const env = {
      PARITY_URL: kernel.url,
      PARITY_KEY: join(kernel.dir, "kit.key"),
      PARITY_DIR: kernel.dir,
      PARITY_SCENARIO: SCENARIO,
      PARITY_OUT: join(kernel.dir, "transcript.json"),
      PARITY_KERNABLE: KERNABLE,
      PARITY_STORE: kernel.store,
      KERNABLE_LOG: process.env.KERNABLE_LOG ?? "warn",
    };
    const logs = join(OUT, `${host}-${driverLang}`);
    mkdirSync(logs, { recursive: true });
    let child = await startHost(host, env, join(logs, "host.log"));

    // The driver; a `restart` file from it asks for the host to be restarted.
    const c = command(driverLang, "driver");
    const driverChild = spawn(c.cmd, c.args, { cwd: c.cwd, env: { ...process.env, ...env, ...c.env }, stdio: ["ignore", "pipe", "pipe"] });
    let driverErr = "";
    driverChild.stderr!.on("data", (d) => (driverErr += d));
    driverChild.stdout!.on("data", (d) => (driverErr += d));
    const driverDone = new Promise<number | null>((r) => driverChild.on("exit", r));
    let finished = false;
    let code: number | null = null;
    void driverDone.then((x) => {
      finished = true;
      code = x;
    });
    const deadline = Date.now() + 15 * 60_000;
    while (!finished) {
      if (existsSync(join(kernel.dir, "restart"))) {
        await stopHost(child);
        child = await startHost(host, env, join(logs, "host-2.log"));
        rmSync(join(kernel.dir, "restart"));
      }
      if (Date.now() > deadline) {
        driverChild.kill("SIGKILL");
        throw new Error(`driver ${driverLang} did not finish in 15 min`);
      }
      await sleep(25);
    }
    writeFileSync(join(logs, "driver.log"), driverErr);
    if (code !== 0) throw new Error(`driver ${driverLang} exited ${code}:\n${driverErr.slice(-4000)}`);

    // Every stored event, once nothing new came for 1.5 s (reactors still at work end first).
    const reader = new Kernable({ url: kernel.url, key: join(kernel.dir, "kit.key") });
    const events: StoredEvent[] = [];
    let last = Date.now();
    const sub = reader.subscribe({ after: 0 }, (e) => {
      events.push(e);
      last = Date.now();
    });
    while (Date.now() - last < 1500) await sleep(100);
    sub.stop();
    await sub.done;
    reader.close();
    await stopHost(child);

    const transcript = JSON.parse(readFileSync(join(kernel.dir, "transcript.json"), "utf8"));
    const mailsFile = join(kernel.dir, "mails.jsonl");
    const mails = existsSync(mailsFile) ? readFileSync(mailsFile, "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l)) : [];
    return { transcript, mails, events };
  } finally {
    await kernel.dispose();
  }
}

// --- Normalizing: values that differ on every run become placeholders ---------------------------

const UUID = /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi;
const ARGON2 = /\$argon2id\$[^"\s]+/g;
const JWT = /eyJ[\w-]+\.[\w-]+\.[\w-]+/g;
const RANDOM_RUN = /[A-Za-z0-9_-]{16,}/g;
/** Fields whose string values are random as a whole. */
const RANDOM_FIELDS = new Set([
  "token", "secret", "clientSecret", "accessToken", "refreshToken", "challenge", "keyId", "key", "uri", "url", "callback", "recoveryCodes",
  "txt", "linkId", "sessionId", "sid", "jti", "nonce", "state", "credentialId", "publicKey", "codeVerifier", "hash", "tokenId", "rawId", "userHandle", "signature", "authenticatorData", "attestationObject", "clientDataJSON",
]);

class Normalizer {
  private readonly names = new Map<string, string>();
  /** Random values found in random fields, longest first: replaced wherever they appear. */
  private known: string[] = [];
  private next = 1;

  private name(raw: string): string {
    let n = this.names.get(raw);
    if (!n) {
      n = `<${this.next++}>`;
      this.names.set(raw, n);
    }
    return n;
  }

  /** First pass: the values of random fields, whole and in their dot-separated parts. */
  collect(v: unknown, field?: string): void {
    if (typeof v === "string") {
      // A URL's query parameter values (codes, states, nonces) are random too.
      const query = /^https?:\/\/[^\s?]*\?(\S+)$/.exec(v);
      if (query) {
        for (const pair of query[1].split("&")) {
          const value = decodeURIComponent(pair.split("=")[1] ?? "");
          if (value.length >= 8 && !/^https?:/.test(value) && !this.known.includes(value)) this.known.push(value);
        }
      }
      if (!field || !RANDOM_FIELDS.has(field) || v.length < 8) return;
      for (const part of [v, ...v.split(".")]) {
        if (part.length >= 8 && !UUID.test(part) && !this.known.includes(part)) this.known.push(part);
        UUID.lastIndex = 0;
      }
      return;
    }
    if (Array.isArray(v)) for (const x of v) this.collect(x, field);
    else if (v && typeof v === "object" && !(v instanceof Date)) for (const [k, x] of Object.entries(v)) this.collect(x, k);
  }

  /** Placeholders for every random part of `s`. */
  str(s: string, field?: string): string {
    if (this.names.has(s)) return this.names.get(s)!;
    // A URL's query parameters in a fixed order: libraries build them in their own.
    if (/^https?:\/\/[^\s]*\?/.test(s)) {
      const [base, q] = s.split("?", 2);
      s = `${base}?${q.split("&").sort().join("&")}`;
    }
    let out = s.replace(/http:\/\/127\.0\.0\.1:\d+/g, "<local>").replace(ARGON2, "<argon2>").replace(JWT, "<jwt>").replace(UUID, (m) => this.name(m.toLowerCase()));
    if (this.known.length) {
      // Named in the order they appear, then replaced longest first.
      const hits = this.known.filter((k) => out.includes(k)).map((k) => ({ k, i: out.indexOf(k) }));
      for (const h of hits.sort((a, b) => a.i - b.i || b.k.length - a.k.length)) this.name(h.k);
      this.known.sort((a, b) => b.length - a.length);
      for (const k of this.known) if (out.includes(k)) out = out.split(k).join(this.name(k));
    }
    out = out.replace(RANDOM_RUN, (m) => (/\d/.test(m) && /[A-Za-z]/.test(m)) || m.length >= 32 ? this.name(m) : m);
    if (field && RANDOM_FIELDS.has(field) && out === s && s.length >= 8) return this.name(s);
    return out;
  }

  value(v: unknown, field?: string): unknown {
    if (typeof v === "string") return this.str(v, field);
    if (typeof v === "number") {
      if (v > 1.5e12 && v < 3e12) return "<time>";
      if (v > 4e7 && v < 1.2e8 && Number.isInteger(v)) return "<totp-step>";
      if (v > 1.5e9 && v < 3e9 && (field === "iat" || field === "exp" || field === "at")) return "<time-s>";
      return v;
    }
    if (Array.isArray(v)) return v.map((x) => this.value(x, field));
    if (v && typeof v === "object") {
      if (v instanceof Date) return "<time>";
      // Keys in a fixed order: JSON objects are unordered.
      return Object.fromEntries(Object.entries(v).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)).map(([k, x]) => [this.str(k), this.value(x, k)]));
    }
    return v;
  }
}

/** An event as compared: what it is and says, not where and when it landed. */
function eventView(e: StoredEvent): Record<string, unknown> {
  const request = Object.fromEntries(Object.entries(e.request ?? {}).filter(([k, v]) => !["correlationId", "causationId", "traceId", "expectedVersion"].includes(k) && v !== undefined && v !== ""));
  return {
    stream: `${e.domain}.${e.aggregateType}/${e.aggregateId}`, tenant: e.tenant, type: e.type, schema: e.schema,
    redacted: [...(e.redacted ?? [])].sort(), data: e.data, request,
  };
}

function normalize(run: { transcript: unknown[]; mails: unknown[]; events: StoredEvent[] }): unknown {
  const n = new Normalizer();
  n.collect(run.transcript);
  n.collect(run.mails);
  n.collect(run.events.map((e) => e.data));
  const transcript = n.value(run.transcript);
  const mails = n.value(run.mails);
  // Events by stream, so reactors' timing across streams does not count; streams sorted by what
  // they hold once named.
  const streams = new Map<string, Record<string, unknown>[]>();
  for (const e of run.events) {
    const v = eventView(e);
    const key = v.stream as string;
    if (!streams.has(key)) streams.set(key, []);
    streams.get(key)!.push(v);
  }
  // Name in an order that does not depend on timing: streams whose names are known first, sorted.
  const ordered = [...streams.entries()].map(([key, evs]) => ({ key, evs, known: n.str(key) }));
  ordered.sort((a, b) => {
    const ka = /<\d+>/.test(a.known) || !/[0-9a-f]{8}-/.test(a.key) ? a.known : `~${a.evs[0].type}`;
    const kb = /<\d+>/.test(b.known) || !/[0-9a-f]{8}-/.test(b.key) ? b.known : `~${b.evs[0].type}`;
    return ka < kb ? -1 : ka > kb ? 1 : 0;
  });
  const events: Record<string, unknown> = {};
  for (const { key, evs } of ordered) events[n.str(key)] = evs.map((e) => n.value({ ...e, stream: undefined }));
  const sortedEvents = Object.fromEntries(Object.entries(events).sort(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0)));
  return { transcript, mails, events: sortedEvents };
}

// --- Main ----------------------------------------------------------------------------------------

mkdirSync(OUT, { recursive: true });
const results = new Map<string, string>();
const failures: string[] = [];
for (const h of hosts) {
  for (const d of drivers) {
    const name = `${h}-${d}`;
    const started = Date.now();
    try {
      const run = await runPair(h, d);
      const json = JSON.stringify(normalize(run), null, 1);
      writeFileSync(join(OUT, `${name}.json`), json);
      writeFileSync(join(OUT, `${name}.raw.json`), JSON.stringify(run, null, 1));
      results.set(name, json);
      const unexpected = (run.transcript as { unexpected?: unknown; step: number; op: string; result: unknown }[]).filter((s) => "unexpected" in s);
      for (const s of unexpected) failures.push(`${name}: step ${s.step} (${s.op}) expected ${JSON.stringify(s.unexpected)}, got ${JSON.stringify(s.result)}`);
      console.log(`${name}: ${run.transcript.length} steps, ${run.events.length} events, ${run.mails.length} mails, ${unexpected.length} unexpected (${((Date.now() - started) / 1000).toFixed(1)} s)`);
    } catch (e) {
      failures.push(`${name}: ${(e as Error).message}`);
      console.log(`${name}: FAILED ${(e as Error).message.split("\n")[0]}`);
    }
  }
}

// The baseline: this run's TypeScript/TypeScript, else the one an earlier run left.
const saved = join(OUT, "ts-ts.json");
const base = results.get("ts-ts") ?? (existsSync(saved) ? readFileSync(saved, "utf8") : [...results.values()][0]);
const baseName = results.has("ts-ts") || existsSync(saved) ? "ts-ts" : [...results.keys()][0];
for (const [name, json] of results) {
  rmSync(join(OUT, `${name}.diff`), { force: true });
  if (name === baseName || json === base) continue;
  let diff = "";
  try {
    execFileSync("diff", ["-u", join(OUT, `${baseName}.json`), join(OUT, `${name}.json`)], { encoding: "utf8" });
  } catch (e) {
    diff = (e as { stdout: string }).stdout;
  }
  writeFileSync(join(OUT, `${name}.diff`), diff);
  failures.push(`${name}: differs from ${baseName} (${diff.split("\n").filter((l) => /^[-+][^-+]/.test(l)).length} lines, ${join(OUT, `${name}.diff`)})`);
}

if (failures.length) {
  console.log(`\n${failures.length} problem(s):\n${failures.join("\n")}`);
  process.exit(1);
}
console.log(`\nall ${results.size} runs equal to ${baseName}`);
