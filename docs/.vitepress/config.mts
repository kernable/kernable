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
        { text: "Tutorial", link: "/tutorial/getting-started" },
        { text: "Reference", link: "/reference/" },
      ],
      sidebar: [
        {
          text: "Tutorial",
          items: [{ text: "Getting started", link: "/tutorial/getting-started" }],
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
