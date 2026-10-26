// Test helper: runs the real kernel binary (`cargo build` first) in a temp dir.

import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createConnection, createServer } from "node:net";
import { join } from "node:path";

import { Key } from "../src/index.ts";
import type { TlsFiles } from "../src/rpc.ts";

// KERNABLE_BIN=release uses release binaries, e.g. for measurements.
const BIN = new URL(`../../../target/${process.env.KERNABLE_BIN ?? "debug"}/`, import.meta.url).pathname;

export interface TestKernelOptions {
  /** Listen on 127.0.0.1 with mutual TLS instead of a Unix socket; see `tlsFiles`. */
  tls?: boolean;
  /** Serve Prometheus metrics; see `metricsUrl`. */
  metrics?: boolean;
}

/** A free TCP port on 127.0.0.1. */
export function freePort(): Promise<number> {
  return new Promise((resolve) => {
    const s = createServer().listen(0, "127.0.0.1", () => {
      const { port } = s.address() as { port: number };
      s.close(() => resolve(port));
    });
  });
}

export class TestKernel {
  readonly dir = mkdtempSync("/tmp/kn-"); // short: Unix socket paths are limited to ~104 bytes
  readonly socket = join(this.dir, "k.sock");
  url = `unix:${this.socket}`;
  /** Client TLS files for `app` when started with `tls`. */
  readonly tlsFiles?: TlsFiles;
  metricsUrl?: string;
  private readonly options: TestKernelOptions;
  private readonly topLevel: string;
  private readonly tables: string;
  private configured = false;
  /** One key for both submitting commands and running the worker. */
  readonly appKey = join(this.dir, "app.key");
  /** Backend under test: KERNABLE_TEST_STORE=file selects the file store, else SQLite. */
  readonly store = process.env.KERNABLE_TEST_STORE === "file" ? `file:${join(this.dir, "k.log")}` : `sqlite:${join(this.dir, "k.db")}`;
  private process?: ChildProcess;

  /** `commandTypes` is the worker allowlist (`orders.*`, `orders.PlaceOrder`). `extraConfig` adds top-level TOML lines, e.g. `lease_ttl_ms = 500`. */
  constructor(commandTypes: string[], extraConfig = "", options: TestKernelOptions = {}) {
    this.options = options;
    if (options.tls) {
      execFileSync(join(BIN, "kernable"), ["certs", "new", this.dir, "app"]);
      const file = (name: string) => join(this.dir, name);
      this.tlsFiles = { ca: file("ca.tls.crt"), cert: file("app.tls.crt"), key: file("app.tls.key") };
    }
    const write = (name: string) => {
      const seed = randomBytes(32).toString("hex");
      writeFileSync(join(this.dir, `${name}.key`), seed);
      writeFileSync(join(this.dir, `${name}.pub`), Key.fromHex(seed).id);
      return Key.fromHex(seed).id;
    };
    write("kernel");
    const app = write("app");
    this.topLevel = `store = "${this.store}"
kernel_key = "kernel.key"
checkpoint_interval_ms = 50
${extraConfig}
`;
    this.tables = `
[clients]
"${app}" = ["*"]

[workers]
"${app}" = ${JSON.stringify(commandTypes)}

[subscribers]
"${app}" = ["*"]

[listeners]
"${app}" = ["*"]

[queriers]
"${app}" = ["*"]

[query_handlers]
"${app}" = ["*"]
`;
  }

  /** Picks ports on the first start and keeps them, so clients can reconnect after a restart. */
  private async configure(): Promise<void> {
    if (this.configured) return;
    this.configured = true;
    let topLevel = this.topLevel;
    let tls = "";
    if (this.options.tls) {
      this.url = `127.0.0.1:${await freePort()}`;
      tls = `[tls]\ncert = "kernel.tls.crt"\nkey = "kernel.tls.key"\nclient_ca = "ca.tls.crt"\n`;
    }
    topLevel += `listen = "${this.url}"\n`;
    if (this.options.metrics) {
      const metrics = `127.0.0.1:${await freePort()}`;
      this.metricsUrl = `http://${metrics}/metrics`;
      topLevel += `metrics_listen = "${metrics}"\n`;
    }
    writeFileSync(join(this.dir, "kernable.toml"), topLevel + tls + this.tables);
  }

  async start(): Promise<void> {
    await this.configure();
    this.process = spawn(join(BIN, "kernable-kernel"), [join(this.dir, "kernable.toml")], { stdio: "ignore" });
    for (let i = 0; i < 200; i++) {
      if (await accepts(this.url)) return;
      await new Promise((r) => setTimeout(r, 20));
    }
    throw new Error("kernel did not start");
  }

  async kill(signal: NodeJS.Signals = "SIGTERM"): Promise<void> {
    const p = this.process;
    if (!p || p.exitCode !== null || p.signalCode !== null) return;
    p.kill(signal);
    await once(p, "exit");
  }

  /** Output of `kernable verify` on the event log. */
  verify(): string {
    return execFileSync(join(BIN, "kernable"), ["verify", this.store, join(this.dir, "kernel.pub")], {
      encoding: "utf8",
    });
  }

  async dispose(): Promise<void> {
    await this.kill("SIGKILL");
    rmSync(this.dir, { recursive: true, force: true });
  }
}

function accepts(url: string): Promise<boolean> {
  const [host, port] = url.split(":");
  const target = url.startsWith("unix:") ? { path: url.slice("unix:".length) } : { host, port: Number(port) };
  return new Promise((resolve) => {
    const s = createConnection(target);
    s.once("connect", () => (s.end(), resolve(true)));
    s.once("error", () => resolve(false));
  });
}
