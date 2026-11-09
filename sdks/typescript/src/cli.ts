#!/usr/bin/env node
// kernable-sdk: the SDK's own tool. `npx @kernable/sdk add kit [dir]` copies the kit of this SDK's
// version into your project: tenancy, accounts, identities, groups, invitations and
// authorization as source that is yours to change.

import { cpSync, existsSync, mkdirSync, readFileSync, readdirSync, statSync, writeFileSync } from "node:fs";
import { dirname, join, relative } from "node:path";
import { fileURLToPath } from "node:url";

const here = dirname(fileURLToPath(import.meta.url));
const sdk = join(here, "..");
const version = (JSON.parse(readFileSync(join(sdk, "package.json"), "utf8")) as { version: string }).version;

/** The kit's files: packed into the SDK (`kit/`), or in the repository when run from a checkout. */
function kitSource(): { kit: string; fixtures: string; license: string } | null {
  const packed = join(sdk, "kit");
  if (existsSync(join(packed, "src"))) return { kit: packed, fixtures: join(packed, "fixtures"), license: join(packed, "LICENSE") };
  const repo = join(sdk, "..", "..", "kits");
  if (existsSync(join(repo, "typescript", "src"))) return { kit: join(repo, "typescript"), fixtures: join(repo, "fixtures"), license: join(repo, "LICENSE") };
  return null;
}

/** What the copy takes along, relative to the kit: the sources, the example, the contract and its runners. */
const PARTS = ["src", "example", join("test", "unit"), "README.md"];

function header(file: string): string {
  return `// From the kernable kit ${version} (MIT, see LICENSE): yours to change.\n`;
}

function copyTree(from: string, to: string, written: string[]): void {
  for (const name of readdirSync(from)) {
    const source = join(from, name);
    const target = join(to, name);
    if (statSync(source).isDirectory()) {
      mkdirSync(target, { recursive: true });
      copyTree(source, target, written);
    } else if (name.endsWith(".ts")) {
      const text = readFileSync(source, "utf8");
      writeFileSync(target, text.startsWith("// From the kernable kit") ? text : header(name) + text);
      written.push(target);
    } else {
      cpSync(source, target);
      written.push(target);
    }
  }
}

function addKit(dir: string, force: boolean): number {
  const source = kitSource();
  if (!source) {
    process.stderr.write("kernable-sdk: this SDK carries no kit\n");
    return 1;
  }
  if (existsSync(dir) && readdirSync(dir).length > 0 && !force) {
    process.stderr.write(`kernable-sdk: ${dir} is not empty (the kit is copied, not merged); choose another directory, or --force\n`);
    return 1;
  }
  const written: string[] = [];
  for (const part of PARTS) {
    const from = join(source.kit, part);
    if (!existsSync(from)) continue;
    if (statSync(from).isDirectory()) {
      mkdirSync(join(dir, part), { recursive: true });
      copyTree(from, join(dir, part), written);
    } else {
      mkdirSync(dir, { recursive: true });
      cpSync(from, join(dir, part));
      written.push(join(dir, part));
    }
  }
  mkdirSync(join(dir, "fixtures"), { recursive: true });
  copyTree(source.fixtures, join(dir, "fixtures"), written);
  cpSync(source.license, join(dir, "LICENSE"));
  const kitPackage = JSON.parse(readFileSync(join(source.kit, "package.json"), "utf8")) as { dependencies: Record<string, string> };
  const deps = Object.entries(kitPackage.dependencies)
    .filter(([name]) => name !== "@kernable/sdk")
    .map(([name, range]) => `${name}@${range}`);
  process.stdout.write(
    `The kernable kit ${version} is in ${dir} (${written.length + 1} files), yours to change.\n` +
      `Add its dependencies:  npm install ${deps.join(" ")}\n` +
      `Its contract:          node --test ${relative(process.cwd(), join(dir, "test", "unit"))}/*.test.ts\n` +
      `Then see ${join(dir, "README.md")}.\n`,
  );
  return 0;
}

function main(args: string[]): number {
  const force = args.includes("--force");
  const rest = args.filter((a) => a !== "--force");
  if (rest[0] === "add" && rest[1] === "kit") return addKit(rest[2] ?? "kit", force);
  if (rest[0] === "--version") {
    process.stdout.write(`${version}\n`);
    return 0;
  }
  process.stderr.write("usage: kernable-sdk add kit [dir] [--force]   copies the kit into dir (default: kit)\n");
  return rest.length === 0 || rest[0] === "--help" ? 0 : 2;
}

process.exitCode = main(process.argv.slice(2));
