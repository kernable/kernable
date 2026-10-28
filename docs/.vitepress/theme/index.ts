import DefaultTheme from "vitepress/theme";
import type { Theme } from "vitepress";
import Layout from "./Layout.vue";
import FlowDiagram from "./flow/FlowDiagram.vue";
import { useLanguageTabs } from "./languageTabs.ts";
import "./custom.css";

export default {
  extends: DefaultTheme,
  Layout,
  enhanceApp(ctx) {
    // Pages embed animated figures with <FlowDiagram name="…" />.
    ctx.app.component("FlowDiagram", FlowDiagram);
  },
  setup() {
    useLanguageTabs();
  },
} satisfies Theme;
