// `/llms-full.txt`: the whole documentation as one Markdown file, for LLMs. The pages follow
// the sidebar; code imported with `<<< @/…` and `<!--@include: …-->` is inlined, and animated
// figures are left out. The dev server serves it; the build writes it next to the pages.

import { readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import type { Plugin } from "vite";

type Item = { text?: string; link?: string; items?: Item[] };

const SITE = "https://kernable.com/docs";

/** The pages' links in sidebar order. */
function links(items: Item[]): string[] {
  return items.flatMap((i) => [...(i.link ? [i.link] : []), ...links(i.items ?? [])]);
}

/** `/concepts/` → concepts/index.md, `/concepts/data` → concepts/data.md. */
const fileOf = (link: string) => (link.endsWith("/") ? `${link.slice(1)}index.md` : `${link.slice(1)}.md`);

/** The lines of `file` between `#region name` and `#endregion name`, or all of it, without region markers. */
function region(file: string, name?: string): string {
  const lines = readFileSync(file, "utf8").split("\n");
  const marker = /^\s*(\/\/|#)\s*#?(end)?region\b/;
  let inside = !name;
  const out: string[] = [];
  for (const line of lines) {
    if (name && new RegExp(`^\\s*(//|#)\\s*#?region ${name}\\s*$`).test(line)) inside = true;
    else if (name && new RegExp(`^\\s*(//|#)\\s*#?endregion ${name}\\s*$`).test(line)) inside = false;
    else if (inside && !marker.test(line)) out.push(line);
  }
  return out.join("\n").replace(/\n+$/, "");
}

/** One page as plain Markdown. */
function page(root: string, link: string): string {
  const path = join(root, fileOf(link));
  const text = readFileSync(path, "utf8")
    .replace(/^---\n[\s\S]*?\n---\n/, "")
    .replace(/<!--@include:\s*(.+?)\s*-->/g, (_, rel: string) => readFileSync(join(dirname(path), rel), "utf8"))
    .split("\n")
    .flatMap((line) => {
      const code = line.match(/^\s*<<<\s+@\/([^#{\s]+)(?:#([\w-]+))?(?:\{(\w+)[^}]*\})?(?:\s+\[(.+)\])?/);
      if (code) {
        const [, file, name, lang, title] = code;
        const ext = lang ?? file.split(".").pop() ?? "";
        return [...(title ? [`${title}:`] : []), "```" + ext, region(join(root, file), name), "```"];
      }
      if (/^\s*<FlowDiagram\b/.test(line)) return [];
      if (/^:{3,}/.test(line)) return [];
      return [line];
    })
    .join("\n");
  return `<!-- ${SITE}${link} -->\n\n${text.trim()}\n`;
}

export function llmsFull(root: string, sidebar: Item[]): string {
  const intro = `# kernable documentation\n\nEvery page of ${SITE}, in the order of its sidebar. For a short guide to building on kernable, see ${SITE}/kernable.AGENTS.md.\n\n**Status:** kernable 0.1: released and usable in production; the API may still change before 1.0, and the release notes say how.\n`;
  return [intro, ...links(sidebar).map((l) => page(root, l))].join("\n\n---\n\n");
}

/** Serves `/docs/llms-full.txt` in the dev server (under the base). */
export function llmsPlugin(root: string, sidebar: Item[]): Plugin {
  return {
    name: "kernable-llms-full",
    configureServer(server) {
      server.middlewares.use("/docs/llms-full.txt", (_req, res) => {
        res.setHeader("content-type", "text/plain; charset=utf-8");
        res.end(llmsFull(root, sidebar));
      });
    },
  };
}

/** Writes `/llms-full.txt` into the built site. */
export function writeLlmsFull(root: string, outDir: string, sidebar: Item[]): void {
  writeFileSync(join(outDir, "llms-full.txt"), llmsFull(root, sidebar));
}
