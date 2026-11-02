// Checks the built site for attribute names a browser refuses to set (`<td 2="">`). Markdown can
// produce them without an error: `{qty 2}` after an element is read as its attributes, and the
// page then fails in the browser ("Invalid qualified name: '2'"). Run after `vitepress build`.

import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

const DIST = new URL("../.vitepress/dist", import.meta.url).pathname;
// A start tag, its attributes with their values quoted or not.
const TAG = /<([a-zA-Z][^\s/>]*)((?:\s+[^\s"'>/=]+(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s"'=<>`]+))?)*)\s*\/?>/g;
const ATTRIBUTE = /\s+([^\s"'>/=]+)(?:\s*=\s*(?:"[^"]*"|'[^']*'|[^\s"'=<>`]+))?/g;
const VALID = /^[A-Za-z_:][-A-Za-z0-9_:.]*$/;

function* pages(dir: string): Generator<string> {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    const path = join(dir, entry.name);
    if (entry.isDirectory()) yield* pages(path);
    else if (entry.name.endsWith(".html")) yield path;
  }
}

const invalid: string[] = [];
for (const page of pages(DIST)) {
  for (const [, tag, attributes] of readFileSync(page, "utf8").matchAll(TAG)) {
    for (const [, name] of attributes!.matchAll(ATTRIBUTE)) {
      if (!VALID.test(name!)) invalid.push(`${page.slice(DIST.length + 1)}: <${tag} ${name}>`);
    }
  }
}
if (invalid.length) {
  console.error(`invalid attribute names (markdown read as attributes? write \`{...}\` inside code):\n${invalid.join("\n")}`);
  process.exit(1);
}
