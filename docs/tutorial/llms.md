# For LLMs and coding agents

Starting a project on kernable with an AI assistant or a coding agent? Give it kernable's rules before it writes code. Three files exist for that:

| File | What it is | Use it |
|---|---|---|
| <a href="/kernable.AGENTS.md" target="_blank"><code>kernable.AGENTS.md</code></a> | a compact guide written as instructions: the mental model, project layout, every pattern with code, the rules, the typical errors and how to verify | copy it into your project as `AGENTS.md`, `CLAUDE.md` or your tool's context file |
| [`/llms.txt`](/llms.txt) | a short index of this documentation, following the [llms.txt](https://llmstxt.org) convention | tools that look for it find it at the site's root |
| [`/llms-full.txt`](/llms-full.txt) | the whole documentation in one Markdown file, with all code examples inlined | give it to an assistant that should know everything, or that answers questions about kernable |

## Start a project with an agent

1. Copy `kernable.AGENTS.md` into the new project, as `AGENTS.md` (or `CLAUDE.md`):
   ```sh
   curl -fsSL https://kernable.com/kernable.AGENTS.md -o AGENTS.md
   ```
2. Start a kernel for development in its own terminal: `kernable start --dev`. Its UI at http://127.0.0.1:7234 shows what the agent built.
3. Describe your domain to the agent in business terms: what is decided (aggregates, commands, events), what is read (queries), what happens as a consequence (reactors), who may do what (middleware), which data is personal.
4. Ask it to verify its work the way the file says: unit tests with `fold`, a run against the kernel, the UI.

The file is written for TypeScript, with a table of the Python and Rust spellings; the tutorials show all three in full.

## The guide

<<< @/public/kernable.AGENTS.md{md}
