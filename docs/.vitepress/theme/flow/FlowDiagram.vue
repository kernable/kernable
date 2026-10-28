<script setup>
import { ref, onMounted, onBeforeUnmount } from 'vue'
import { Graph, Runner, KIND, reducedMotion } from './engine'
import { diagrams } from './diagrams'
import { useFlowControls } from './useFlowControls'

const props = defineProps({
  name: { type: String, required: true },
  /* widen beyond the content column (for pages without aside) */
  wide: { type: Boolean, default: false },
  /* start automatically the first time the figure scrolls into view */
  autoplay: { type: Boolean, default: true }
})

const LEGEND = {
  cmd: 'Command',
  evt: 'Event',
  qry: 'Query',
  ok: 'Acknowledgement',
  kern: 'Kernel',
  sig: 'Signature',
  pii: 'Personal data',
  err: 'Rejection / mismatch'
}

const def = diagrams[props.name]
const legend = (def && def.legend) || []

const root = ref(null)
const svg = ref(null)

const c = useFlowControls()
const { playing, paused, caption, stepInfo, canBack, canNext, atEnd, back, next, playPause } = c
c.caption.value = def ? def.intro : `Unknown diagram: ${props.name}`

let graph = null
let runner = null
let observer = null

function build () {
  if (!svg.value || !def) return
  svg.value.innerHTML = ''
  graph = new Graph(svg.value, { w: def.w, h: def.h })
  def.build(graph)
  runner = new Runner(graph, def.steps(graph), { onSay: c.onSay })
  c.bind(graph, runner, def.intro, build)
}

onMounted(() => {
  if (!def) return
  build()
  if (props.autoplay && !reducedMotion()) {
    observer = new IntersectionObserver((entries) => {
      entries.forEach((e) => {
        if (e.isIntersecting) {
          observer.disconnect()
          observer = null
          c.playPause()
        }
      })
    }, { threshold: 0.5 })
    observer.observe(root.value)
  }
})

onBeforeUnmount(() => {
  if (observer) observer.disconnect()
  if (runner) runner.stop()
  if (graph) graph.destroy()
})
</script>

<template>
  <figure ref="root" class="kflow" :class="{ 'kflow-wide': wide }">
    <figcaption class="kflow-caption" aria-live="polite">{{ caption }}</figcaption>
    <div class="kflow-bar">
      <span class="kflow-legend">
        <span v-for="k in legend" :key="k" class="kflow-key">
          <i :style="{ background: KIND[k] }"></i>{{ LEGEND[k] || k }}
        </span>
      </span>
      <span class="kflow-actions">
        <span class="kflow-step">{{ stepInfo }}</span>
        <button type="button" class="kflow-btn" :disabled="!canBack" @click="back"
          title="Previous step" aria-label="Previous step">‹ Back</button>
        <button type="button" class="kflow-btn kflow-btn-play" @click="playPause">
          {{ playing ? (paused ? 'Resume' : 'Pause') : (atEnd ? 'Replay' : 'Play') }}
        </button>
        <button type="button" class="kflow-btn" :disabled="!canNext" @click="next"
          title="Next step" aria-label="Next step">Next ›</button>
      </span>
    </div>
    <div class="kflow-stage">
      <svg ref="svg" role="img" :aria-label="def ? def.intro : name"></svg>
    </div>
  </figure>
</template>

<style>
.kflow {
  margin: 24px 0;
  border: 1px solid var(--vp-c-divider);
  border-radius: 8px;
  background: var(--vp-c-bg-soft);
  overflow: hidden;
}
.kflow-wide {
  position: relative;
  left: 50%;
  transform: translateX(-50%);
  width: min(calc(100vw - 48px), 1120px);
  max-width: none;
}
@media (min-width: 960px) {
  .kflow-wide {
    width: min(calc(100vw - 272px - 96px), 1120px);
  }
}
.kflow-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-wrap: wrap;
  padding: 8px 14px;
  border-bottom: 1px solid var(--vp-c-divider);
}
.kflow-legend {
  display: flex;
  align-items: center;
  gap: 12px;
  flex-wrap: wrap;
}
.kflow-key {
  display: inline-flex;
  align-items: center;
  gap: 5px;
  font-size: 11px;
  color: var(--vp-c-text-3);
}
.kflow-key i {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  display: inline-block;
}
.kflow-actions {
  display: inline-flex;
  align-items: center;
  gap: 8px;
  margin-left: auto;
}
.kflow-step {
  font-size: 11px;
  color: var(--vp-c-text-3);
  font-variant-numeric: tabular-nums;
}
.kflow-btn {
  font-size: 12px;
  font-weight: 500;
  line-height: 1;
  padding: 5px 12px;
  border: 1px solid var(--vp-c-divider);
  border-radius: 20px;
  color: var(--vp-c-text-2);
  background: var(--vp-c-bg);
  transition: color 0.2s, border-color 0.2s;
}
.kflow-btn:hover:not(:disabled) {
  color: var(--vp-c-brand-1);
  border-color: var(--vp-c-brand-1);
}
.kflow-btn:disabled {
  opacity: 0.4;
  cursor: not-allowed;
}
.kflow-btn-play {
  min-width: 74px;
  color: var(--vp-c-brand-1);
  border-color: var(--vp-c-brand-1);
}
.kflow-stage {
  padding: 10px 12px 4px;
  overflow-x: auto;
}
.kflow-stage svg {
  display: block;
  width: 100%;
  height: auto;
}
/* on phones the figure keeps a readable size and scrolls sideways */
@media (max-width: 640px) {
  .kflow-stage svg {
    min-width: 600px;
  }
}
.kflow-caption {
  min-height: 42px;
  padding: 10px 14px;
  border-bottom: 1px solid var(--vp-c-divider);
  font-size: 13px;
  line-height: 1.5;
  color: var(--vp-c-text-2);
}

/* --- SVG parts --- */
.kflow-svg text {
  font-family: var(--vp-font-family-base);
  user-select: none;
}
.kflow-node,
.kflow-zone {
  transition: opacity 0.5s;
}
.kflow-hidden {
  opacity: 0;
}
.kflow-node .kflow-body {
  fill: var(--vp-c-bg);
  stroke: var(--vp-c-divider);
  stroke-width: 1;
  transition: stroke 0.25s;
}
.kflow-node .kflow-acc {
  fill: var(--kflow-accent);
  opacity: 0.85;
}
.kflow-node .kflow-lid {
  fill: var(--vp-c-bg);
  stroke: var(--kflow-accent);
  stroke-width: 1.2;
  transition: stroke 0.25s;
}
.kflow-node.hot .kflow-body {
  stroke: var(--kflow-accent);
  stroke-width: 1.6;
}
.kflow-node .kflow-label {
  fill: var(--vp-c-text-1);
  font-size: 13px;
  font-weight: 500;
}
.kflow-node .kflow-sub {
  fill: var(--vp-c-text-3);
  font-size: 10px;
}
.kflow-zone text {
  font-size: 10.5px;
}
.kflow-wire {
  fill: none;
  stroke: var(--vp-c-divider);
  stroke-width: 1.3;
  transition: stroke 0.3s, stroke-width 0.3s, opacity 0.5s;
}
.kflow-wire.hot {
  stroke-width: 2;
}
.kflow-packet-label {
  font-size: 10.5px;
  font-weight: 600;
  paint-order: stroke;
  stroke: var(--vp-c-bg-soft);
  stroke-width: 3px;
}
.kflow-note {
  font-size: 10.5px;
}
</style>
