<script setup>
import DefaultTheme from 'vitepress/theme'
import { useData } from 'vitepress'
import AnimatedLogo from './AnimatedLogo.vue'
import FlowDiagram from './flow/FlowDiagram.vue'

const { Layout } = DefaultTheme
const { frontmatter } = useData()
</script>

<template>
  <Layout>
    <!-- on every page: kernable is not released yet -->
    <template #layout-top>
      <div class="kn-preview">
        <strong>Preview</strong>
        <span class="kn-preview-long">kernable is not released yet: these docs describe work in progress, and things may change before 1.0.</span>
        <span class="kn-preview-short">Not released yet.</span>
        <a href="https://kernable.com/#access">Request early access →</a>
      </div>
    </template>
    <!-- animated mark in the hero, home page only -->
    <template #home-hero-image>
      <AnimatedLogo v-if="frontmatter.layout === 'home'" />
    </template>
    <!-- kernable in one loop, below the features -->
    <template #home-features-after>
      <section v-if="frontmatter.layout === 'home'" class="kn-home-flow">
        <div class="container">
          <h2>How it works</h2>
          <FlowDiagram name="hero" loop bare />
          <p class="kn-credit">TypeScript, Python and Rust logos via Simple Icons; the Rust logo under CC BY-SA 4.0.</p>
        </div>
      </section>
    </template>
  </Layout>
</template>

<style>
/* The preview banner. VitePress keeps the nav and sidebar --vp-layout-top-height from the top of the
   window, so the banner stays fixed there too; otherwise it scrolls away and leaves a gap above the nav. */
:root {
  --vp-layout-top-height: 36px;
}
.kn-preview {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  z-index: var(--vp-z-index-layout-top);
  height: var(--vp-layout-top-height);
  display: flex;
  align-items: center;
  justify-content: center;
  gap: 10px;
  padding: 0 16px;
  font-size: 13px;
  white-space: nowrap;
  overflow: hidden;
  color: #e4e9f3;
  background: linear-gradient(100deg, rgba(0, 200, 240, 0.18), rgba(122, 77, 255, 0.22)), #1a2442;
}
.kn-preview strong {
  font-size: 11px;
  font-weight: 600;
  letter-spacing: 0.1em;
  text-transform: uppercase;
  color: #7fe3f5;
}
.kn-preview a {
  color: #fff;
  font-weight: 500;
  text-decoration: underline;
  text-underline-offset: 3px;
}
.kn-preview-short {
  display: none;
}
@media (max-width: 860px) {
  .kn-preview-long { display: none; }
  .kn-preview-short { display: inline; }
}
.kn-credit {
  margin: -16px 0 0;
  font-size: 11px;
  color: var(--vp-c-text-3);
}
/* same box as the feature cards above, so both share one left edge */
.kn-home-flow {
  margin-top: 48px;
  padding: 0 24px 24px;
}
.kn-home-flow .container {
  max-width: 1152px;
  margin: 0 auto;
}
.kn-home-flow h2 {
  font-size: 24px;
  font-weight: 600;
  letter-spacing: -0.01em;
  color: var(--vp-c-text-1);
}
@media (min-width: 640px) {
  .kn-home-flow { padding: 0 48px 24px; }
}
@media (min-width: 960px) {
  .kn-home-flow { padding: 0 64px 24px; }
}
</style>
