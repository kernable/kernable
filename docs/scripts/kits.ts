// Generates the reference part of each kit page (docs/kits/generated/<domain>.md) from the
// TypeScript kit's sources, so the tables cannot drift from the code: per domain its aggregates
// with unique keys and facts, its commands with their fields, the events they write, who may
// send them and how they are refused, its events with their vault fields, its queries, and
// the read models and reactors that listen. Called by reference.ts (and its --check).

import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import ts from "typescript";

const KIT = new URL("../../kits/typescript/src/", import.meta.url).pathname;
export const KIT_DOMAINS = ["tenancy", "account", "identity", "group", "invitation"] as const;

/** The reserved listener orders (shared/constants.ts). */
const ORDERS: Record<string, string> = Object.fromEntries(
  [...(readFileSync(join(KIT, "shared/constants.ts"), "utf8").match(/export const ORDERS = \{([^}]*)\}/)?.[1] ?? "").matchAll(/(\w+): (-?\d+)/g)].map((m) => [m[1], m[2]]),
);

const cell = (s: string) => s.replace(/\\/g, "\\\\").replace(/\|/g, "\\|").replace(/\s+/g, " ").trim();
const code = (s: string) => `\`${cell(s)}\``;

function parse(path: string): ts.SourceFile | null {
  let text: string;
  try {
    text = readFileSync(path, "utf8");
  } catch {
    return null;
  }
  return ts.createSourceFile(path, text, ts.ScriptTarget.ES2022, true);
}

function walk(node: ts.Node, visit: (n: ts.Node) => void): void {
  visit(node);
  node.forEachChild((c) => walk(c, visit));
}

const doc = (node: ts.Node) =>
  ts.getJSDocCommentsAndTags(node).filter(ts.isJSDoc).map((d) => ts.getTextOfJSDocComment(d.comment) ?? "").join(" ").trim();

/** `{ email }` and `${d.tokenId}` in a vault subject, readable. */
function subject(expr: ts.Node | undefined, src: ts.SourceFile): string {
  if (!expr) return "";
  const body = ts.isArrowFunction(expr) ? expr.body : expr;
  const text = body.getText(src);
  if (/^id$/.test(text)) return "the aggregate";
  if (/^d\.(\w+)$/.test(text)) return `the ${text.slice(2)}`;
  return text.replace(/^`|`$/g, "").replace(/\$\{d\.(\w+)\}/g, "<$1>").replace(/\$\{id\}/g, "<id>");
}

interface Field { name: string; type: string; optional: boolean; doc: string }
interface Message {
  name: string;
  doc: string;
  fields: Field[];
  personal: { fields: string[]; subject: string }[];
  secret: string[];
  erases: string;
  answer: string;
  /** Queries: the aggregate it is about (`static target`). */
  target: string;
  node: ts.ClassDeclaration;
}

function messages(src: ts.SourceFile | null, base: "Event" | "Command" | "Query"): Message[] {
  if (!src) return [];
  const out: Message[] = [];
  for (const node of src.statements) {
    if (!ts.isClassDeclaration(node) || !node.name) continue;
    const ext = node.heritageClauses?.find((h) => h.token === ts.SyntaxKind.ExtendsKeyword)?.types[0];
    if (!ext || ext.expression.getText(src) !== base) continue;
    const m: Message = { name: node.name.text, doc: doc(node), fields: [], personal: [], secret: [], erases: "", answer: ext.typeArguments?.[0]?.getText(src) ?? "", target: "", node };
    for (const member of node.members) {
      if (!ts.isPropertyDeclaration(member) || !ts.isIdentifier(member.name)) continue;
      const isStatic = ts.getModifiers(member)?.some((x) => x.kind === ts.SyntaxKind.StaticKeyword);
      const name = member.name.text;
      if (!isStatic) {
        m.fields.push({ name, type: member.type?.getText(src) ?? "", optional: !!member.questionToken, doc: doc(member) });
      } else if (name === "personal" && member.initializer) {
        walk(member.initializer, (n) => {
          if (!ts.isObjectLiteralExpression(n)) return;
          const prop = (k: string) => n.properties.find((p): p is ts.PropertyAssignment => ts.isPropertyAssignment(p) && p.name.getText(src) === k);
          const fields = prop("fields")?.initializer;
          if (fields && ts.isArrayLiteralExpression(fields)) {
            m.personal.push({ fields: fields.elements.filter(ts.isStringLiteral).map((e) => e.text), subject: subject(prop("subject")?.initializer, src) });
          }
        });
      } else if (name === "secret" && member.initializer && ts.isArrayLiteralExpression(member.initializer)) {
        m.secret = member.initializer.elements.filter(ts.isStringLiteral).map((e) => e.text);
      } else if (name === "erases") {
        m.erases = subject(member.initializer, src);
      }
      else if (name === "target" && member.initializer && ts.isObjectLiteralExpression(member.initializer)) {
        const get = (k: string) => (member.initializer as ts.ObjectLiteralExpression).properties.find((p): p is ts.PropertyAssignment => ts.isPropertyAssignment(p) && p.name.getText(src) === k)?.initializer;
        const type = get("aggregateType");
        const field = get("field");
        if (type && field && ts.isStringLiteral(type) && ts.isStringLiteral(field)) m.target = `${type.text} in \`${field.text}\``;
      }
    }
    out.push(m);
  }
  return out;
}

/** Calls of the form `<receiver>.<method>(<Class>, handler, options)`. */
function registrations(src: ts.SourceFile | null, method: string): { receiver: string; cls: string; args: ts.NodeArray<ts.Expression> }[] {
  if (!src) return [];
  const out: { receiver: string; cls: string; args: ts.NodeArray<ts.Expression> }[] = [];
  walk(src, (n) => {
    if (!ts.isCallExpression(n) || !ts.isPropertyAccessExpression(n.expression) || n.expression.name.text !== method) return;
    const first = n.arguments[0];
    if (!first || !ts.isIdentifier(first)) return;
    let receiver = n.expression.expression;
    // In a chain (`domain.reactor("x").on(A).on(B)`), the receiver is the chain's head.
    while (ts.isCallExpression(receiver) && ts.isPropertyAccessExpression(receiver.expression)) receiver = receiver.expression.expression;
    out.push({ receiver: receiver.getText(src), cls: first.text, args: n.arguments });
  });
  return out;
}

/** The functions of a domain and of shared/, by name, to follow a handler into its helpers. */
function functionsOf(files: (ts.SourceFile | null)[]): Map<string, ts.Node> {
  const fns = new Map<string, ts.Node>();
  for (const src of files) {
    if (!src) continue;
    for (const s of src.statements) {
      if (ts.isFunctionDeclaration(s) && s.name && s.body) fns.set(s.name.text, s.body);
      if (ts.isVariableStatement(s)) {
        for (const d of s.declarationList.declarations) {
          if (ts.isIdentifier(d.name) && d.initializer && (ts.isArrowFunction(d.initializer) || ts.isFunctionExpression(d.initializer))) fns.set(d.name.text, d.initializer);
        }
      }
    }
  }
  return fns;
}

/** What a handler does: the classes it creates with `.new(…)` and the codes it rejects with, through its helpers. */
function effects(handler: ts.Node | undefined, fns: Map<string, ts.Node>): { created: Set<string>; codes: Set<string> } {
  const created = new Set<string>();
  const codes = new Set<string>();
  const seen = new Set<ts.Node>();
  const visit = (root: ts.Node) => {
    if (seen.has(root)) return;
    seen.add(root);
    walk(root, (n) => {
      if (ts.isCallExpression(n) && ts.isPropertyAccessExpression(n.expression) && n.expression.name.text === "new" && ts.isIdentifier(n.expression.expression)) {
        created.add(n.expression.expression.text);
      }
      if (ts.isNewExpression(n) && n.expression.getText() === "Rejected") {
        const c = n.arguments?.[1];
        if (c && ts.isStringLiteral(c)) codes.add(c.text);
      }
      if (ts.isCallExpression(n) && ts.isIdentifier(n.expression) && fns.has(n.expression.text)) visit(fns.get(n.expression.text)!);
    });
  };
  if (handler) visit(handler);
  return { created, codes };
}

/** Who may send what (authorization/levels.ts), with the comment beside each rule. */
function rules(): { rules: Map<string, { level: string; self: boolean; note: string }>; escalation: Set<string> } {
  const src = parse(join(KIT, "authorization/levels.ts"))!;
  const map = new Map<string, { level: string; self: boolean; note: string }>();
  const escalation = new Set<string>();
  walk(src, (n) => {
    if (ts.isVariableDeclaration(n) && n.name.getText(src) === "RULES" && n.initializer && ts.isObjectLiteralExpression(n.initializer)) {
      for (const p of n.initializer.properties) {
        if (!ts.isPropertyAssignment(p) || !ts.isObjectLiteralExpression(p.initializer)) continue;
        const get = (k: string) => (p.initializer as ts.ObjectLiteralExpression).properties.find((q): q is ts.PropertyAssignment => ts.isPropertyAssignment(q) && q.name.getText(src) === k)?.initializer.getText(src);
        const comment = ts.getTrailingCommentRanges(src.text, p.getEnd() + 1)?.[0];
        const note = comment ? src.text.slice(comment.pos, comment.end).replace(/^\/\/\s*/, "").replace(/: see policy\.ts$/, "") : "";
        map.set(JSON.parse(p.name.getText(src)), { level: JSON.parse(get("level") ?? '"group"'), self: get("self") === "true", note });
      }
    }
    if (ts.isVariableDeclaration(n) && n.name.getText(src) === "ESCALATION" && n.initializer && ts.isArrayLiteralExpression(n.initializer)) {
      for (const e of n.initializer.elements) if (ts.isStringLiteral(e)) escalation.add(e.text);
    }
  });
  return { rules: map, escalation };
}

/** What the bootstrap grants the API's own identity (its `api` group, bootstrap.ts). */
const API_PERMISSIONS = new Set(
  [...(readFileSync(join(KIT, "bootstrap.ts"), "utf8").match(/API_PERMISSIONS = \[([\s\S]*?)\];/)?.[1] ?? "").matchAll(/pattern: "([\w.]+)"/g)].map((m) => m[1]),
);

const LEVELS: Record<string, string> = {
  public: "anyone (the request carries its proof)",
  authenticated: "any signed-in account",
  internal: "only the kit's reactors",
  owners: "owners",
  platform: "the platform",
  group: "a group permission",
};

function who(type: string, r: ReturnType<typeof rules>): string {
  const rule = r.rules.get(type) ?? { level: "group", self: false, note: "" };
  let text = rule.level === "group" && API_PERMISSIONS.has(type) ? "the API (its `api` group)" : LEVELS[rule.level];
  if (rule.self) text += ", or the sender itself";
  if (r.escalation.has(type)) text += "; only with permissions the sender holds";
  if (rule.note) text += ` (${rule.note})`;
  return text;
}

function fieldList(m: Message): string {
  if (!m.fields.length) return "—";
  return m.fields
    .map((f) => {
      const marks = [m.secret.includes(f.name) ? "secret" : "", m.personal.some((p) => p.fields.includes(f.name)) ? "personal" : ""].filter(Boolean);
      return `${code(`${f.name}${f.optional ? "?" : ""}: ${f.type}`)}${marks.length ? ` (${marks.join(", ")})` : ""}`;
    })
    .join("<br>");
}

/** A query's answer: a named row type of the domain shown with its fields. */
function answer(text: string, types: Map<string, string[]>): string {
  const named = types.get(text);
  if (named) return code(`${text} { ${named.join(", ")} }`);
  return code(text || "—");
}

function typesOf(files: (ts.SourceFile | null)[]): Map<string, string[]> {
  const types = new Map<string, string[]>();
  for (const src of files) {
    if (!src) continue;
    for (const s of src.statements) {
      if (ts.isInterfaceDeclaration(s)) types.set(s.name.text, s.members.map((m) => m.name?.getText(src) ?? "").filter(Boolean));
    }
  }
  return types;
}

export function kitReference(domain: string): string {
  const file = (f: string) => parse(join(KIT, domain, f));
  const [domainTs, events, aggregates, commands, readmodels, queries, reactors] = ["domain.ts", "events.ts", "aggregates.ts", "commands.ts", "readmodels.ts", "queries.ts", "reactors.ts"].map(file);
  const shared = readdirSync(join(KIT, "shared")).map((f) => parse(join(KIT, "shared", f)));
  const fns = functionsOf([commands, aggregates, domainTs, ...shared]);
  const r = rules();
  const out: string[] = [];

  // Aggregates: variable → name, unique keys, facts.
  const aggregateNames = new Map<string, string>();
  const aggRows: string[] = [];
  if (aggregates) {
    for (const s of aggregates.statements) {
      if (!ts.isVariableStatement(s)) continue;
      for (const d of s.declarationList.declarations) {
        if (!d.initializer || !ts.isIdentifier(d.name)) continue;
        let name = "";
        const keys: string[] = [];
        const facts: string[] = [];
        walk(d.initializer, (n) => {
          if (!ts.isCallExpression(n) || !ts.isPropertyAccessExpression(n.expression)) return;
          const arg = n.arguments[0];
          if (!arg || !ts.isStringLiteral(arg)) return;
          const method = n.expression.name.text;
          if (method === "aggregate") name = arg.text;
          if (method === "unique") {
            const opts = n.arguments[2]?.getText(aggregates) ?? "";
            keys.push(code(arg.text) + (opts.includes('"global"') ? " (global)" : ""));
          }
          if (method === "fact") facts.push(code(arg.text));
        });
        if (!name) continue;
        aggregateNames.set(d.name.text, name);
        aggRows.push(`| ${code(`${domain}.${name}`)} | ${keys.reverse().join(", ") || "—"} | ${facts.reverse().join(", ") || "—"} |`);
      }
    }
  }
  const typeDecls = (domainTs?.statements ?? []).filter((s) => (ts.isTypeAliasDeclaration(s) || ts.isInterfaceDeclaration(s)) && ts.getModifiers(s)?.some((m) => m.kind === ts.SyntaxKind.ExportKeyword));
  if (typeDecls.length) {
    const text = typeDecls.map((s) => s.getText(domainTs!).replace(/^export /, "")).join("\n\n");
    out.push(`### Types\n\nThe types the tables below name, from \`${domain}/domain.ts\`:\n\n\`\`\`ts\n${text}\n\`\`\`\n`);
  }
  if (aggRows.length) {
    out.push(`### Aggregates\n\n| Aggregate | Unique keys | Facts |\n|---|---|---|\n${aggRows.join("\n")}\n`);
  }

  // Commands, in the order of the sources.
  const cmds = messages(commands, "Command");
  const handlers = new Map(registrations(commands, "command").map((x) => [x.cls, x]));
  if (cmds.length) {
    const rows = cmds.map((m) => {
      const reg = handlers.get(m.name);
      const { created, codes } = effects(reg?.args[1], fns);
      const writes = [...created].filter((c) => c !== m.name && /^[A-Z]/.test(c) && events && messages(events, "Event").some((e) => e.name === c));
      const requires: string[] = [];
      const opts = reg?.args[2];
      if (opts) {
        walk(opts, (n) => {
          if (!ts.isObjectLiteralExpression(n)) return;
          const get = (k: string) => n.properties.find((p): p is ts.PropertyAssignment => ts.isPropertyAssignment(p) && p.name.getText() === k)?.initializer;
          const fact = get("fact");
          if (!fact || !ts.isStringLiteral(fact)) return;
          const target = `${(get("domain") as ts.StringLiteral | undefined)?.text}.${(get("aggregateType") as ts.StringLiteral | undefined)?.text}`;
          const key = get("key");
          requires.push(get("absent") ? `no ${target} holds it as ${key && ts.isStringLiteral(key) ? key.text : "key"}` : `${target} is ${fact.text}`);
        });
      }
      const on = reg ? aggregateNames.get(reg.receiver) : undefined;
      const name = `${code(m.name)}${on ? `<br>on ${code(on)}` : ""}`;
      const refused = [...codes].sort().map(code).join(", ") + (requires.length ? `${codes.size ? "; " : ""}requires: ${requires.join("; ")}` : "");
      return `| ${name} | ${fieldList(m)} | ${writes.map(code).join(", ") || "—"} | ${who(`${domain}.${m.name}`, r)} | ${refused || "—"} |`;
    });
    out.push(
      `### Commands\n\nSecret fields leave the vault once the command completes; personal fields stay in the vault until their subject is erased.\n\n| Command | Fields | Writes | Who may send it | Refused with |\n|---|---|---|---|---|\n${rows.join("\n")}\n`,
    );
  }

  const evts = messages(events, "Event");
  if (evts.length) {
    const rows = evts.map((m) => {
      const vault = [
        ...m.personal.map((p) => `${p.fields.map(code).join(", ")} under ${code(p.subject)}`),
        m.erases ? `erases ${code(m.erases)}` : "",
      ].filter(Boolean);
      const fields = m.fields.map((f) => code(`${f.name}${f.optional ? "?" : ""}: ${f.type}`)).join("<br>") || "—";
      return `| ${code(m.name)} | ${fields} | ${vault.join("; ") || "—"} | ${cell(m.doc) || "—"} |`;
    });
    out.push(`### Events\n\n| Event | Fields | Vault | Note |\n|---|---|---|---|\n${rows.join("\n")}\n`);
  }

  const qs = messages(queries, "Query");
  if (qs.length) {
    const types = typesOf([readmodels, domainTs, queries]);
    const models = new Map<string, string>();
    if (readmodels) {
      walk(readmodels, (n) => {
        if (ts.isVariableDeclaration(n) && ts.isIdentifier(n.name) && n.initializer) {
          walk(n.initializer, (c) => {
            if (ts.isCallExpression(c) && ts.isPropertyAccessExpression(c.expression) && c.expression.name.text === "readModel" && ts.isStringLiteral(c.arguments[0])) {
              models.set((n.name as ts.Identifier).text, (c.arguments[0] as ts.StringLiteral).text);
            }
          });
        }
      });
    }
    const answeredBy = new Map(registrations(queries, "query").map((x) => [x.cls, models.get(x.receiver) ?? x.receiver]));
    const rows = qs.map((m) => `| ${code(m.name)}${answeredBy.get(m.name) ? `<br>by ${code(answeredBy.get(m.name)!)}` : ""} | ${fieldList(m)} | ${m.target ? `\`${m.target.split(" in ")[0]}\` in ${m.target.split(" in ")[1]}` : "—"} | ${answer(m.answer, types)} | ${who(`${domain}.${m.name}`, r)} |`);
    out.push(`### Queries\n\nA query about one aggregate names it (\`static target\`): the middleware decides on that aggregate as on a command's.\n\n| Query | Fields | About | Answer | Who may ask |\n|---|---|---|---|---|\n${rows.join("\n")}\n`);
  }

  // Listeners: read models and reactors, with the events they take and what reactors submit.
  const listenerRows: string[] = [];
  for (const [src, kind] of [[readmodels, "read model"], [reactors, "reactor"]] as const) {
    if (!src) continue;
    walk(src, (n) => {
      if (!ts.isCallExpression(n) || !ts.isPropertyAccessExpression(n.expression)) return;
      const method = n.expression.name.text;
      if ((method !== "reactor" && method !== "readModel") || !ts.isStringLiteral(n.arguments[0])) return;
      const name = n.arguments[0].text;
      const order = ORDERS[n.arguments[1]?.getText(src).match(/ORDERS\.(\w+)/)?.[1] ?? ""] ?? "";
      // Walk up the chain: each `.on(Event, handler)` above this call.
      const ons: { event: string; submits: Set<string> }[] = [];
      let node: ts.Node = n;
      while (node.parent && ts.isPropertyAccessExpression(node.parent) && ts.isCallExpression(node.parent.parent)) {
        const call = node.parent.parent;
        if (node.parent.name.text === "on" && ts.isIdentifier(call.arguments[0])) {
          const submits = new Set<string>();
          walk(call.arguments[1] ?? call, (c) => {
            if (ts.isCallExpression(c) && ts.isPropertyAccessExpression(c.expression) && c.expression.name.text === "submit") {
              const arg = c.arguments[0];
              if (arg && ts.isCallExpression(arg) && ts.isPropertyAccessExpression(arg.expression) && ts.isIdentifier(arg.expression.expression)) submits.add(arg.expression.expression.text);
            }
          });
          ons.push({ event: call.arguments[0].text, submits });
        }
        node = call;
      }
      if (kind === "reactor") {
        for (const on of ons) listenerRows.push(`| ${code(name)} (${kind}, order ${order}) | ${code(on.event)} | ${[...on.submits].map(code).join(", ") || "side effects only"} |`);
      } else {
        listenerRows.push(`| ${code(name)} (${kind}, order ${order}) | ${ons.map((o) => code(o.event)).join(", ")} | — |`);
      }
    });
  }
  if (listenerRows.length) {
    out.push(`### Listeners\n\n| Listener | On | Submits |\n|---|---|---|\n${listenerRows.join("\n")}\n`);
  }

  return `<!-- Generated by docs/scripts/reference.ts from kits/typescript/src/${domain}; do not edit. -->\n\n${out.join("\n")}`;
}
