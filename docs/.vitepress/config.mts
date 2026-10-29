import { defineConfig } from "vitepress";
import { withMermaid } from "vitepress-plugin-mermaid";

export default withMermaid(
  defineConfig({
    title: "kernable",
    description: "Event sourcing and CQRS with a Rust kernel and business logic in any language",
    cleanUrls: true,
    head: [
      ["link", { rel: "icon", type: "image/png", href: "/favicon.png" }],
      ["link", { rel: "apple-touch-icon", href: "/apple-touch-icon.png" }],
    ],
    lastUpdated: false,
    srcExclude: ["examples/**", "scripts/**"],
    // The mermaid plugin pulls in CommonJS modules that the dev server must pre-bundle, or the
    // page stays blank in `make docs` (the production build is not affected).
    vite: { optimizeDeps: { include: ["mermaid", "fastdom"] } },
    themeConfig: {
      logo: { src: "/logo-nav.png", alt: "kernable" },
      siteTitle: false,
      search: { provider: "local" },
      nav: [
        { text: "Tutorials", link: "/tutorial/" },
        { text: "Concepts", link: "/concepts/" },
        { text: "How-to", link: "/how-to/" },
        { text: "Reference", link: "/reference/" },
      ],
      sidebar: [
        {
          text: "Tutorials",
          items: [
            { text: "Getting started", link: "/tutorial/getting-started" },
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
            { text: "Order and consistency", link: "/concepts/ordering" },
            { text: "Trust", link: "/concepts/trust" },
            { text: "Queries and middleware", link: "/concepts/queries-and-middleware" },
            { text: "Data over time", link: "/concepts/data" },
            { text: "Failures and recovery", link: "/concepts/failures" },
            { text: "Compared with other systems", link: "/concepts/comparison" },
          ],
        },
        {
          text: "How-to guides",
          items: [
            { text: "Overview", link: "/how-to/" },
            { text: "Deploy and upgrade", link: "/how-to/deploy" },
            { text: "Back up and restore", link: "/how-to/backup" },
            { text: "Monitor kernable", link: "/how-to/monitor" },
            { text: "Mutual TLS with your own PKI", link: "/how-to/tls" },
            { text: "Keys per service", link: "/how-to/keys" },
            { text: "Change an event's format", link: "/how-to/event-versions" },
            { text: "Rebuild a read model", link: "/how-to/rebuild-read-model" },
            { text: "Remove a listener or aggregate", link: "/how-to/remove-listener" },
            { text: "Test your domain", link: "/how-to/test-domain" },
            { text: "Handle an erasure request", link: "/how-to/erasure" },
            { text: "Passwords and other secrets", link: "/how-to/secrets" },
            { text: "Store files and cache values", link: "/how-to/data-and-cache" },
            { text: "Write an SDK", link: "/how-to/new-sdk" },
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
      ],
      outline: [2, 3],
    },
  }),
);
