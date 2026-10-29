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
