import { defineConfig } from "vitepress";
import { withMermaid } from "vitepress-plugin-mermaid";

export default withMermaid(
  defineConfig({
    title: "kernable",
    description: "Event sourcing and CQRS with a Rust kernel and business logic in any language",
    cleanUrls: true,
    lastUpdated: false,
    srcExclude: ["examples/**", "scripts/**", "getting-started.old.md"],
    themeConfig: {
      search: { provider: "local" },
      nav: [
        { text: "Tutorials", link: "/tutorial/" },
        { text: "Concepts", link: "/concepts/" },
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
            { text: "Order and consistency", link: "/concepts/ordering" },
            { text: "Trust", link: "/concepts/trust" },
            { text: "Queries and middleware", link: "/concepts/queries-and-middleware" },
            { text: "Data over time", link: "/concepts/data" },
            { text: "Failures and recovery", link: "/concepts/failures" },
            { text: "Compared with other systems", link: "/concepts/comparison" },
          ],
        },
        {
          text: "Reference",
          items: [
            { text: "Overview", link: "/reference/" },
            { text: "kernable.toml", link: "/reference/configuration" },
            { text: "CLI", link: "/reference/cli" },
            { text: "TypeScript SDK", link: "/reference/sdk-typescript" },
            { text: "Python SDK", link: "/reference/sdk-python" },
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
