/* SVG flow-animation engine for the kernable docs.
   Every color is a VitePress theme variable, so figures follow the
   site palette in both light and dark mode without extra code. */

export const SVGNS = 'http://www.w3.org/2000/svg'

/* message/component kinds → theme colors */
export const KIND = {
  cmd: 'var(--vp-c-yellow-1)',
  evt: 'var(--vp-c-purple-1)',
  qry: 'var(--vp-c-indigo-1)',
  ok: 'var(--vp-c-green-1)',
  kern: 'var(--vp-c-brand-1)',
  sig: 'var(--vp-c-brand-2)',
  pii: 'var(--kn-c-personal)',
  err: 'var(--vp-c-red-1)',
  mut: 'var(--vp-c-text-3)'
}

export const reducedMotion = () =>
  typeof window !== 'undefined' &&
  window.matchMedia('(prefers-reduced-motion: reduce)').matches

const easeInOut = (t) => (t < 0.5 ? 2 * t * t : 1 - Math.pow(-2 * t + 2, 2) / 2)

export function svgEl (tag, attrs, parent) {
  const e = document.createElementNS(SVGNS, tag)
  for (const k in attrs) {
    if (k === 'style') Object.assign(e.style, attrs[k])
    else e.setAttribute(k, attrs[k])
  }
  if (parent) parent.appendChild(e)
  return e
}

/* polyline path with rounded corners */
export function roundedPath (pts, r = 12) {
  if (pts.length < 2) return ''
  let d = `M ${pts[0][0]} ${pts[0][1]}`
  for (let i = 1; i < pts.length - 1; i++) {
    const [x0, y0] = pts[i - 1]
    const [x1, y1] = pts[i]
    const [x2, y2] = pts[i + 1]
    const v1 = [x1 - x0, y1 - y0]
    const v2 = [x2 - x1, y2 - y1]
    const l1 = Math.hypot(...v1)
    const l2 = Math.hypot(...v2)
    const rr = Math.min(r, l1 / 2, l2 / 2)
    const pA = [x1 - (v1[0] / l1) * rr, y1 - (v1[1] / l1) * rr]
    const pB = [x1 + (v2[0] / l2) * rr, y1 + (v2[1] / l2) * rr]
    d += ` L ${pA[0]} ${pA[1]} Q ${x1} ${y1} ${pB[0]} ${pB[1]}`
  }
  const last = pts[pts.length - 1]
  d += ` L ${last[0]} ${last[1]}`
  return d
}

let uid = 0

export class Graph {
  constructor (svg, { w, h, flow = 1.45 } = {}) {
    this.svg = svg
    this.w = w
    this.h = h
    this.flow = flow
    this.paused = false
    this.reduced = reducedMotion()
    this.nodes = {}
    this.wires = {}
    this.parts = {} // id → the node, wire and zone with that id, for show/hide
    svg.setAttribute('viewBox', `0 0 ${w} ${h}`)
    svg.classList.add('kflow-svg')
    this.layerWires = svgEl('g', {}, svg)
    this.layerNodes = svgEl('g', {}, svg)
    this.layerFx = svgEl('g', {}, svg)
    const defs = svgEl('defs', {}, svg)
    this.markerId = `kflow-arr-${++uid}`
    const mk = svgEl('marker', {
      id: this.markerId, viewBox: '0 0 10 10', refX: 8, refY: 5,
      markerWidth: 6.5, markerHeight: 6.5, orient: 'auto-start-reverse'
    }, defs)
    svgEl('path', { d: 'M 0 1 L 9 5 L 0 9 z', style: { fill: 'var(--vp-c-text-3)' } }, mk)
    this.markerUrl = `url(#${this.markerId})`
  }

  destroy () {
    this.paused = true
    this._dead = true
  }

  zone (x, y, w, h, label, color, { id, hidden = false } = {}) {
    const g = svgEl('g', { class: 'kflow-zone' }, this.layerWires)
    if (id) this.part(id, g, hidden)
    svgEl('rect', {
      x, y, width: w, height: h, rx: 12, fill: 'none',
      'stroke-dasharray': '5 6', 'stroke-width': 1,
      style: { stroke: color || 'var(--vp-c-divider)' }
    }, g)
    if (label) {
      const t = svgEl('text', {
        x: x + 12, y: y + 18, 'font-size': 10.5, 'font-weight': 600,
        'letter-spacing': '.08em',
        style: { fill: color || 'var(--vp-c-text-3)' }
      }, g)
      t.textContent = label.toUpperCase()
    }
    return g
  }

  node (id, { x, y, w = 150, h = 50, label, sub, kind = 'mut', shape = 'box', hidden = false }) {
    const color = KIND[kind] || kind
    const g = svgEl('g', { class: 'kflow-node' }, this.layerNodes)
    this.part(id, g, hidden)
    g.style.setProperty('--kflow-accent', color)
    if (shape === 'store') {
      const ry = 9
      svgEl('path', {
        class: 'kflow-body',
        d: `M ${x} ${y + ry} A ${w / 2} ${ry} 0 0 1 ${x + w} ${y + ry} L ${x + w} ${y + h - ry} A ${w / 2} ${ry} 0 0 1 ${x} ${y + h - ry} Z`
      }, g)
      svgEl('ellipse', { class: 'kflow-lid', cx: x + w / 2, cy: y + ry, rx: w / 2, ry }, g)
    } else {
      svgEl('rect', { class: 'kflow-body', x, y, width: w, height: h, rx: 8 }, g)
      svgEl('rect', { class: 'kflow-acc', x, y: y + 8, width: 3, height: h - 16, rx: 1.5 }, g)
    }
    const ty = shape === 'store' ? y + h / 2 + 3 : y + (sub ? h / 2 - 2 : h / 2 + 4.5)
    const t1 = svgEl('text', { class: 'kflow-label', x: x + w / 2, y: ty, 'text-anchor': 'middle' }, g)
    t1.textContent = label
    if (sub) {
      const t2 = svgEl('text', {
        class: 'kflow-sub', x: x + w / 2,
        y: shape === 'store' ? ty + 14 : ty + 15, 'text-anchor': 'middle'
      }, g)
      t2.textContent = sub
    }
    this.nodes[id] = { g, x, y, w, h, sub: g.querySelector('.kflow-sub') }
    return this.nodes[id]
  }

  /* replace a node's sub-line (e.g. version counters) */
  setSub (id, text) {
    const n = this.nodes[id]
    if (n && n.sub) n.sub.textContent = text
  }

  /* fade nodes, wires and zones in or out (figures that grow step by step) */
  part (id, el, hidden) {
    ;(this.parts[id] ||= []).push(el)
    if (hidden) el.classList.add('kflow-hidden')
  }

  show (...ids) {
    ids.forEach((id) => (this.parts[id] || []).forEach((el) => el.classList.remove('kflow-hidden')))
  }

  hide (...ids) {
    ids.forEach((id) => (this.parts[id] || []).forEach((el) => el.classList.add('kflow-hidden')))
  }

  /* snapshot / restore node sub-lines and what is shown — used by step
     navigation to reconstruct a step's resting state without re-animating */
  snapshot () {
    const subs = {}
    for (const id in this.nodes) {
      const n = this.nodes[id]
      if (n.sub) subs[id] = n.sub.textContent
    }
    const hidden = Object.values(this.parts).flat().filter((el) => el.classList.contains('kflow-hidden'))
    return { subs, hidden }
  }

  restore ({ subs, hidden }) {
    for (const id in subs) this.setSub(id, subs[id])
    Object.values(this.parts).flat().forEach((el) => el.classList.toggle('kflow-hidden', hidden.includes(el)))
  }

  port (id, side, off = 0) {
    const n = this.nodes[id]
    const cx = n.x + n.w / 2
    const cy = n.y + n.h / 2
    switch (side) {
      case 'left': return [n.x, cy + off]
      case 'right': return [n.x + n.w, cy + off]
      case 'top': return [cx + off, n.y]
      case 'bottom': return [cx + off, n.y + n.h]
      default: return [cx, cy]
    }
  }

  wire (id, pts, { arrow = true, dashed = false, hidden = false } = {}) {
    const p = svgEl('path', { class: 'kflow-wire', d: roundedPath(pts) }, this.layerWires)
    this.part(id, p, hidden)
    if (arrow) p.setAttribute('marker-end', this.markerUrl)
    if (dashed) p.setAttribute('stroke-dasharray', '4 6')
    this.wires[id] = p
    return p
  }

  label (x, y, text, { kind, size = 10.5, anchor = 'middle' } = {}) {
    const t = svgEl('text', {
      class: 'kflow-note', x, y, 'font-size': size, 'text-anchor': anchor,
      style: { fill: kind ? (KIND[kind] || kind) : 'var(--vp-c-text-3)' }
    }, this.layerNodes)
    t.textContent = text
    return t
  }

  /* pausable wait, scaled by this.flow */
  wait (ms) {
    const D = this.reduced ? Math.min(ms, 100) : ms * (this.flow / 1.45)
    return new Promise((resolve) => {
      let elapsed = 0
      let last = performance.now()
      const tick = (now) => {
        if (this._dead) return resolve()
        const dt = now - last
        last = now
        if (!this.paused) elapsed += dt
        if (elapsed >= D) resolve()
        else requestAnimationFrame(tick)
      }
      requestAnimationFrame(tick)
    })
  }

  hotNodes (ids) {
    Object.values(this.nodes).forEach((n) => n.g.classList.remove('hot'))
    ;(ids || []).forEach((id) => this.nodes[id] && this.nodes[id].g.classList.add('hot'))
  }

  coolWires () {
    Object.values(this.wires).forEach((w) => {
      w.classList.remove('hot')
      w.style.stroke = ''
    })
  }

  clearFx () {
    while (this.layerFx.firstChild) this.layerFx.firstChild.remove()
  }

  /* animate a packet along a wire; resolves on arrival */
  packet (wireId, { kind = 'evt', label = '', dur = 1200, reverse = false, keepHot = true } = {}) {
    const path = this.wires[wireId]
    if (!path || this._dead) return Promise.resolve()
    const color = KIND[kind] || kind
    const len = path.getTotalLength()
    // Start at the wire's end: until the first frame, the packet would sit at the origin.
    const start = path.getPointAtLength(reverse ? len : 0)
    const g = svgEl('g', { transform: `translate(${start.x} ${start.y})` }, this.layerFx)
    const halo = svgEl('circle', { r: 10, opacity: 0.2, style: { fill: color } }, g)
    svgEl('circle', { r: 5, style: { fill: color } }, g)
    if (label) {
      const txt = svgEl('text', {
        class: 'kflow-packet-label', 'text-anchor': 'middle', y: -12,
        style: { fill: color }
      }, g)
      txt.textContent = label
    }
    path.classList.add('hot')
    path.style.stroke = color
    const D = this.reduced ? Math.min(dur, 150) : dur * (this.flow / 1.45)
    return new Promise((resolve) => {
      let elapsed = 0
      let last = performance.now()
      const tick = (now) => {
        if (this._dead) { g.remove(); return resolve() }
        const dt = now - last
        last = now
        if (!this.paused) elapsed += dt
        const t = Math.min(1, elapsed / D)
        const pos = path.getPointAtLength(len * easeInOut(reverse ? 1 - t : t))
        g.setAttribute('transform', `translate(${pos.x} ${pos.y})`)
        if (t < 1) requestAnimationFrame(tick)
        else {
          halo.setAttribute('opacity', 0.35)
          setTimeout(() => {
            g.remove()
            if (!keepHot) { path.classList.remove('hot'); path.style.stroke = '' }
            resolve()
          }, 120)
        }
      }
      requestAnimationFrame(tick)
    })
  }
}

/* Runner — plays a scripted list of steps on a graph.
   Each step: { say, show: [ids], hot: [ids], packets: [[wireId, opts]], run(g), pause, after(g) }
   `show` fades parts in as the step starts. */
export class Runner {
  constructor (graph, steps, { onSay, onDone } = {}) {
    this.g = graph
    this.steps = steps
    this.onSay = onSay || (() => {})
    this.onDone = onDone || (() => {})
    this.i = 0
    this.playing = false
    this.epoch = 0
  }

  /* play exactly the step at the cursor, then advance the cursor.
     Returns false at the end, or if cancelled (epoch changed) mid-step. */
  async playStep (ep) {
    if (this.i >= this.steps.length) return false
    const s = this.steps[this.i]
    if (s.show) this.g.show(...s.show)
    this.g.hotNodes(s.hot || [])
    if (s.say) this.onSay(s.say, this.i, this.steps.length)
    if (s.run) await s.run(this.g)
    if (s.packets && s.packets.length) {
      await Promise.all(s.packets.map(([wire, opts], idx) =>
        this.g.wait((opts && opts.delay) || idx * 120).then(() => this.g.packet(wire, opts))
      ))
    }
    if (!s.run && !(s.packets && s.packets.length)) await this.g.wait(s.pause || 900)
    if (ep != null && this.epoch !== ep) return false // interrupted — don't commit
    if (s.after) s.after(this.g)
    this.i++
    return true
  }

  async play () {
    if (this.playing) return
    if (this.i >= this.steps.length) this.reset()
    this.playing = true
    const ep = ++this.epoch
    while (this.playing && this.epoch === ep && this.i < this.steps.length && !this.g._dead) {
      const hold = this.steps[this.i].hold
      const ok = await this.playStep(ep)
      if (!ok || this.epoch !== ep) break
      await this.g.wait(hold != null ? hold : 500)
    }
    if (this.epoch === ep) {
      if (this.i >= this.steps.length) this.onDone()
      this.playing = false
    }
  }

  stop () { this.playing = false; this.epoch++ }

  reset () {
    this.playing = false
    this.i = 0
    this.g.hotNodes([])
    this.g.coolWires()
    this.g.clearFx()
  }
}
