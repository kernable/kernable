import { defineConfig } from "vitepress";
import { withMermaid } from "vitepress-plugin-mermaid";
import { fileURLToPath } from "node:url";

import { llmsPlugin, writeLlmsFull } from "./llms.ts";

const ROOT = fileURLToPath(new URL("..", import.meta.url));

const SIDEBAR = [
  {
    text: "Tutorials",
    items: [
      { text: "Getting started", link: "/tutorial/getting-started" },
      { text: "For LLMs and coding agents", link: "/tutorial/llms" },
      {
        text: "Build a shop",
        link: "/tutorial/shop/",
        items: [
          { text: "1. Run a kernel", link: "/tutorial/shop/1-kernel" },
          { text: "2. Aggregates and commands", link: "/tutorial/shop/2-commands" },
          { text: "3. Read models and queries", link: "/tutorial/shop/3-queries" },
          { text: "4. A reactor", link: "/tutorial/shop/4-reactor" },
          { text: "5. A second language", link: "/tutorial/shop/5-languages" },
          { text: "6. Authorization", link: "/tutorial/shop/6-authorization" },
          { text: "7. Personal data", link: "/tutorial/shop/7-personal-data" },
          { text: "8. Operate", link: "/tutorial/shop/8-operate" },
          { text: "9. Accounts and sign-in", link: "/tutorial/shop/9-sign-in" },
          { text: "10. A business process", link: "/tutorial/shop/10-process" },
        ],
      },
    ],
  },
  {
    text: "Concepts",
    items: [
      { text: "Overview", link: "/concepts/" },
      { text: "Architecture", link: "/concepts/architecture" },
      { text: "Event sourcing in kernable", link: "/concepts/event-sourcing" },
      { text: "Domains", link: "/concepts/domains" },
      { text: "How the pieces fit", link: "/concepts/pieces" },
      { text: "Order and consistency", link: "/concepts/ordering" },
      { text: "Unique keys and conditions", link: "/concepts/uniqueness" },
      { text: "Trust", link: "/concepts/trust" },
      { text: "Compliance", link: "/concepts/compliance" },
      { text: "Queries and middleware", link: "/concepts/queries-and-middleware" },
      { text: "Business processes", link: "/concepts/processes" },
      { text: "Data over time", link: "/concepts/data" },
      { text: "Failures and recovery", link: "/concepts/failures" },
      { text: "Compared with other systems", link: "/concepts/comparison" },
      { text: "What's open, what's kernable Cloud", link: "/concepts/open-and-cloud" },
    ],
  },
  {
    text: "How-to guides",
    items: [
      { text: "Overview", link: "/how-to/" },
      { text: "Deploy and upgrade", link: "/how-to/deploy" },
      { text: "Deployments and rollbacks", link: "/how-to/deployments" },
      { text: "Back up and restore", link: "/how-to/backup" },
      { text: "Move a kernel", link: "/how-to/export" },
      { text: "Previews and copies", link: "/how-to/previews" },
      { text: "Run a standby", link: "/how-to/standby" },
      { text: "Run read-only copies", link: "/how-to/readers" },
      { text: "Relays", link: "/how-to/relays" },
      { text: "Run a listener on several machines", link: "/how-to/instances" },
      { text: "Run a node from one file", link: "/how-to/node" },
      { text: "Run kernable on your own servers", link: "/how-to/self-host" },
      { text: "Share tenants across kernels", link: "/how-to/partitions" },
      { text: "Monitor kernable", link: "/how-to/monitor" },
      { text: "Mutual TLS with your own PKI", link: "/how-to/tls" },
      { text: "Keys per service", link: "/how-to/keys" },
      { text: "Change an event's format", link: "/how-to/event-versions" },
      { text: "Rebuild a read model", link: "/how-to/rebuild-read-model" },
      { text: "Remove a listener or aggregate", link: "/how-to/remove-listener" },
      { text: "Test your domain", link: "/how-to/test-domain" },
      { text: "Schedule a command", link: "/how-to/schedule" },
      { text: "Handle an erasure request", link: "/how-to/erasure" },
      { text: "Passwords and other secrets", link: "/how-to/secrets" },
      { text: "Store files and cache values", link: "/how-to/data-and-cache" },
      { text: "Write an SDK", link: "/how-to/new-sdk" },
    ],
  },
  {
    text: "Kits",
    items: [
      { text: "Overview", link: "/kits/" },
      { text: "Getting started", link: "/kits/getting-started" },
      { text: "Tenancy", link: "/kits/tenancy" },
      { text: "Accounts and sessions", link: "/kits/account" },
      { text: "Identities", link: "/kits/identity" },
      { text: "Groups", link: "/kits/group" },
      { text: "Invitations", link: "/kits/invitation" },
      { text: "Authorization", link: "/kits/authorization" },
      { text: "Your API: KitApi", link: "/kits/api" },
      { text: "Recipes", link: "/kits/recipes" },
    ],
  },
  {
    text: "Reference",
    items: [
      { text: "Overview", link: "/reference/" },
      { text: "kernable start", link: "/reference/configuration" },
      { text: "CLI", link: "/reference/cli" },
      { text: "TypeScript SDK", link: "/reference/sdk-typescript" },
      { text: "Python SDK", link: "/reference/sdk-python" },
      { text: "Rust SDK", link: "/reference/sdk-rust" },
      { text: "Protocol", link: "/reference/protocol" },
      { text: "Signatures", link: "/reference/signing" },
      { text: "Errors", link: "/reference/errors" },
      { text: "Metrics and health", link: "/reference/metrics" },
    ],
  },
];

export default withMermaid(
  defineConfig({
    title: "kernable",
    // Same form as the website's title: "kernable – …".
    titleTemplate: ":title – kernable",
    // Listed in kernable.com/robots.txt, next to the website's own sitemap.
    sitemap: { hostname: "https://kernable.com/docs/" },
    description: "Event sourcing and CQRS with a Rust kernel and business logic in any language",
    // Served at kernable.com/docs/, next to the website. `head` links do not get the base added.
    base: "/docs/",
    cleanUrls: true,
    head: [
      ["link", { rel: "icon", type: "image/png", href: "/docs/favicon.png" }],
      ["link", { rel: "apple-touch-icon", href: "/docs/apple-touch-icon.png" }],
    ],
    lastUpdated: false,
    // public/ holds files served as they are (kernable.AGENTS.md), not pages.
    srcExclude: ["examples/**", "scripts/**", "public/**", "kits/generated/**"],
    // The mermaid plugin pulls in CommonJS modules that the dev server must pre-bundle, or the
    // page stays blank in `make docs` (the production build is not affected).
    vite: { optimizeDeps: { include: ["mermaid", "fastdom"] }, plugins: [llmsPlugin(ROOT, SIDEBAR)] },
    // The whole documentation in one file for LLMs, next to the pages.
    buildEnd: (site) => writeLlmsFull(ROOT, site.outDir, SIDEBAR),
    themeConfig: {
      logo: { src: "/logo-nav.png", alt: "kernable" },
      siteTitle: false,
      search: { provider: "local" },
      nav: [
        { text: "Tutorials", link: "/tutorial/" },
        { text: "Concepts", link: "/concepts/" },
        { text: "How-to", link: "/how-to/" },
        { text: "Kits", link: "/kits/" },
        { text: "Reference", link: "/reference/" },
        // Back to the website; VitePress marks it as external.
        { text: "kernable.com", link: "https://kernable.com/" },
      ],
      sidebar: SIDEBAR,
      outline: [2, 3],
    },
  }),
);
