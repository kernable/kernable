/* Transport controls shared by the animated diagrams (FlowDiagram).
   Drives an engine Runner with Play/Pause plus single-step Next/Back.

   Back is instant and deterministic: a step's only persistent effects are
   node sub-text (g.setSub) and what is shown (a step's `show`), so a step's
   resting state is reconstructed by restoring the initial snapshot and
   replaying the `show` lists and `after` callbacks up to it — no
   re-animation needed. */
import { ref, computed } from 'vue'

export function useFlowControls () {
  const playing = ref(false)
  const paused = ref(false)
  const caption = ref('')
  const stepInfo = ref('')
  const current = ref(-1) // index of the step currently shown (-1 = intro)
  const total = ref(0)

  const canBack = computed(() => current.value > -1)
  const canNext = computed(() => current.value < total.value - 1)
  const atEnd = computed(() => total.value > 0 && current.value >= total.value - 1)

  let graph = null
  let runner = null
  let intro = ''
  let initial = null
  let rebuild = null // () => void — recreates graph+runner and calls bind()

  /* runner is created with { onSay } wired to this */
  function onSay (text, i, n) {
    caption.value = text
    stepInfo.value = `${i + 1} / ${n}`
    current.value = i
  }

  /* call after each (re)build with the fresh graph + runner */
  function bind (g, r, introText, rebuildFn) {
    graph = g
    runner = r
    intro = introText || ''
    if (rebuildFn) rebuild = rebuildFn
    initial = g.snapshot()
    total.value = r.steps.length
    current.value = -1
    caption.value = intro
    stepInfo.value = ''
    playing.value = false
    paused.value = false
  }

  /* instant jump to the resting state of step `target` (-1 = intro) */
  function seek (target) {
    if (!runner || !graph) return
    runner.stop()
    playing.value = false
    paused.value = false
    graph.paused = false
    graph.coolWires()
    graph.clearFx()
    graph.restore(initial)
    for (let k = 0; k <= target; k++) {
      const st = runner.steps[k]
      if (st && st.show) graph.show(...st.show)
      if (st && st.after) st.after(graph)
    }
    graph.hotNodes(target < 0 ? [] : runner.steps[target].hot || [])
    caption.value = target < 0 ? intro : runner.steps[target].say
    stepInfo.value = target < 0 ? '' : `${target + 1} / ${runner.steps.length}`
    runner.i = target + 1
    current.value = target
  }

  function back () {
    if (!canBack.value) return
    seek(current.value - 1)
  }

  async function next () {
    if (!runner || !canNext.value) return
    runner.stop() // cancel any autoplay
    playing.value = false
    paused.value = false
    graph.paused = false
    graph.clearFx() // drop any stray packet from a cancelled run
    await runner.playStep()
    current.value = runner.i - 1
  }

  async function playPause () {
    if (!runner) return
    if (playing.value) {
      paused.value = !paused.value
      graph.paused = paused.value
      return
    }
    if (runner.i >= runner.steps.length) {
      if (rebuild) rebuild() // at the end → start over
    }
    playing.value = true
    paused.value = false
    graph.paused = false
    await runner.play()
    playing.value = false
    paused.value = false
    current.value = runner.i - 1
  }

  return {
    playing, paused, caption, stepInfo, current, total,
    canBack, canNext, atEnd,
    onSay, bind, seek, back, next, playPause
  }
}
