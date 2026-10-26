// Test helper: runs the real kernel binary (`cargo build` first) in a temp dir.

import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import { randomBytes } from "node:crypto";
import { once } from "node:events";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { createConnection } from "node:net";
import { join } from "node:path";

import { Key } from "../src/index.ts";

const BIN = new URL("../../../target/debug/", import.meta.url).pathname;

export class TestKernel {
  readonly dir = mkdtempSync("/tmp/kn-"); // short: Unix socket paths are limited to ~104 bytes
  readonly socket = join(this.dir, "k.sock");
  readonly url = `unix:${this.socket}`;
  /** One key for both submitting commands and running the worker. */
  readonly appKey = join(this.dir, "app.key");
  /** Backend under test: KERNABLE_TEST_STORE=file selects the file store, else SQLite. */
  readonly store = process.env.KERNABLE_TEST_STORE === "file" ? `file:${join(this.dir, "k.log")}` : `sqlite:${join(this.dir, "k.db")}`;
  private process?: ChildProcess;

  /** `extraConfig` adds top-level TOML lines, e.g. `lease_ttl_ms = 500`. */
  constructor(commandTypes: string[], extraConfig = "") {
    const write = (name: string) => {
      const seed = randomBytes(32).toString("hex");
      writeFileSync(join(this.dir, `${name}.key`), seed);
      writeFileSync(join(this.dir, `${name}.pub`), Key.fromHex(seed).id);
      return Key.fromHex(seed).id;
    };
    write("kernel");
    const app = write("app");
    writeFileSync(
      join(this.dir, "kernable.toml"),
      `store = "${this.store}"
listen = "${this.url}"
kernel_key = "kernel.key"
checkpoint_interval_ms = 50
${extraConfig}

[clients]
"${app}" = ["*"]

[workers]
"${app}" = ${JSON.stringify(commandTypes)}

[subscribers]
"${app}" = ["*"]
`,
    );
  }

  async start(): Promise<void> {
    this.process = spawn(join(BIN, "kernable-kernel"), [join(this.dir, "kernable.toml")], { stdio: "ignore" });
    for (let i = 0; i < 200; i++) {
      if (await accepts(this.socket)) return;
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

function accepts(path: string): Promise<boolean> {
  return new Promise((resolve) => {
    const s = createConnection({ path });
    s.once("connect", () => (s.end(), resolve(true)));
    s.once("error", () => resolve(false));
  });
}
