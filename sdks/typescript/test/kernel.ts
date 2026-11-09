// Test helper: runs the real kernel binary (`cargo build` first) in a temp dir.

import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createConnection, createServer } from "node:net";
import { join } from "node:path";

import type { TlsFiles } from "../src/rpc.ts";

// KERNABLE_BIN=release uses release binaries, e.g. for measurements.
const BIN = new URL(`../../../target/${process.env.KERNABLE_BIN ?? "debug"}/`, import.meta.url).pathname;

export interface TestKernelOptions {
  /** Listen on 127.0.0.1 with mutual TLS instead of a Unix socket; see `tlsFiles`. */
  tls?: boolean;
  /** Serve Prometheus metrics; see `metricsUrl`. */
  metrics?: boolean;
  /** Event and command types whose vault values the app key may read (default all). */
  vaultReaders?: string[];
  /** Domains (`shop.*`) whose files and cache the app key may use (default all). */
  stores?: string[];
  /** Grant the app key `process` for every process (a kernel with `ProcessService`). */
  processes?: boolean;
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
  readonly socket = join(this.dir, "kernable.sock");
  url = `unix:${this.socket}`;
  /** Client TLS files for `app` when started with `tls`. */
  readonly tlsFiles?: TlsFiles;
  metricsUrl?: string;
  private readonly options: TestKernelOptions;
  private readonly flags: string[];
  private configured = false;
  /** One key for both submitting commands and running the worker; its grant lies next to it. */
  readonly appKey = join(this.dir, "app.key");
  /** Backend under test: KERNABLE_TEST_STORE=file selects the file store, else SQLite. */
  readonly store = process.env.KERNABLE_TEST_STORE === "file" ? `file:${join(this.dir, "k.log")}` : `sqlite:${join(this.dir, "k.db")}`;
  private process?: ChildProcess;
  private watchdog?: ChildProcess;

  /**
   * The app key may run `commandTypes` (`orders.*`, `orders.PlaceOrder`) and do everything else.
   * `extraFlags` go to `kernable start`, e.g. `["--lease-ttl-ms", "500"]`.
   */
  constructor(commandTypes: string[], extraFlags: string[] = [], options: TestKernelOptions = {}) {
    this.options = options;
    const kernable = (...args: string[]) => execFileSync(join(BIN, "kernable"), args, { cwd: this.dir, encoding: "utf8" });
    if (options.tls) {
      kernable("certs", "new", this.dir, "app");
      const file = (name: string) => join(this.dir, name);
      this.tlsFiles = { ca: file("ca.tls.crt"), cert: file("app.tls.crt"), key: file("app.tls.key") };
    }
    for (const name of ["root", "kernel", "app"]) kernable("keys", "new", name);
    kernable(
      "grant", "root.key", "app.pub",
      `run=${commandTypes.join(",")}`,
      ...["submit", "subscribe", "listen", "query", "answer", "check", "vault_erase", "manage"].map((action) => `${action}=*`),
      `vault_read=${(options.vaultReaders ?? ["*"]).join(",")}`,
      ...["data_read", "data_write", "cache"].map((action) => `${action}=${(options.stores ?? ["*"]).join(",")}`),
      ...(options.processes ? ["process=*"] : []),
    );
    writeFileSync(join(this.dir, "vault.key"), randomBytes(32).toString("hex"));
    this.flags = [
      "--root", "root.pub",
      "--store", this.store,
      "--checkpoint-interval-ms", "50",
      "--vault-key", "vault.key",
      ...extraFlags,
    ];
  }

  /** Picks ports on the first start and keeps them, so clients can reconnect after a restart. */
  private async configure(): Promise<void> {
    if (this.configured) return;
    this.configured = true;
    if (this.options.tls) {
      this.url = `127.0.0.1:${await freePort()}`;
      this.flags.push("--tls-cert", "kernel.tls.crt", "--tls-key", "kernel.tls.key", "--tls-client-ca", "ca.tls.crt");
    }
    this.flags.push("--listen", this.url);
    if (this.options.metrics) {
      const metrics = `127.0.0.1:${await freePort()}`;
      this.metricsUrl = `http://${metrics}/metrics`;
      this.flags.push("--metrics-listen", metrics);
    }
  }

  async start(): Promise<void> {
    await this.configure();
    this.process = spawn(join(BIN, "kernable"), ["start", ...this.flags], { cwd: this.dir, stdio: "ignore" });
    this.watchdog = watchdog(this.process.pid!);
    for (let i = 0; i < 200; i++) {
      if (await accepts(this.url)) return;
      await new Promise((r) => setTimeout(r, 20));
    }
    throw new Error("kernel did not start");
  }

  async kill(signal: NodeJS.Signals = "SIGTERM"): Promise<void> {
    const p = this.process;
    if (p && p.exitCode === null && p.signalCode === null) {
      p.kill(signal);
      await once(p, "exit");
    }
    this.watchdog?.kill("SIGKILL");
    this.watchdog = undefined;
  }

  /** Runs `kernable <args>` in the kernel's directory: whether it succeeded, and its stdout. */
  cli(...args: string[]): { ok: boolean; out: string } {
    try {
      return { ok: true, out: execFileSync(join(BIN, "kernable"), args, { cwd: this.dir, encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] }) };
    } catch (err) {
      return { ok: false, out: String((err as { stdout?: string }).stdout ?? "") };
    }
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

/**
 * A shell reading a pipe from this process. However this process ends (a test runner's
 * timeout, Ctrl-C, a crash), the pipe closes and the shell kills the kernel `pid`; kill the
 * watchdog once the kernel is gone. It does not keep this process alive.
 */
function watchdog(pid: number): ChildProcess {
  const w = spawn("sh", ["-c", `read _; kill -9 ${pid} 2>/dev/null`], { stdio: ["pipe", "ignore", "ignore"] });
  w.unref();
  (w.stdin as unknown as { unref(): void }).unref();
  return w;
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
