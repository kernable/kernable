<script setup>
/* Animated kernable mark for the home hero.
   The "K" is redrawn as SVG from the kernable logo mark: a slanted stem and two
   arms that meet at a point inside it, separated from the stem by a gap in the
   page background. On load the stem rises and the arms slide in; afterwards
   the mark floats gently and a sheen crosses it now and then. Pure CSS, so it
   renders under SSR without a flash and stays still under prefers-reduced-motion. */

/* closed polygon with rounded corners */
function rounded (pts, r) {
  const n = pts.length
  let d = ''
  for (let i = 0; i < n; i++) {
    const [x0, y0] = pts[(i - 1 + n) % n]
    const [x1, y1] = pts[i]
    const [x2, y2] = pts[(i + 1) % n]
    const l1 = Math.hypot(x1 - x0, y1 - y0)
    const l2 = Math.hypot(x2 - x1, y2 - y1)
    const rr = Math.min(r, l1 / 2, l2 / 2)
    const a = [x1 - ((x1 - x0) / l1) * rr, y1 - ((y1 - y0) / l1) * rr]
    const b = [x1 + ((x2 - x1) / l2) * rr, y1 + ((y2 - y1) / l2) * rr]
    d += `${i ? 'L' : 'M'} ${a[0].toFixed(1)} ${a[1].toFixed(1)} Q ${x1} ${y1} ${b[0].toFixed(1)} ${b[1].toFixed(1)} `
  }
  return d + 'Z'
}

const stem = rounded([[175, 300], [540, 125], [540, 1110], [175, 910]], 60)
const upper = rounded([[372, 632], [820, 128], [1258, 128], [815, 632]], 46)
const lower = rounded([[372, 632], [815, 632], [1258, 1136], [820, 1136]], 46)
</script>

<template>
  <svg class="kn-logo" viewBox="145 60 1140 1140" role="img" aria-label="kernable">
    <defs>
      <linearGradient id="kn-logo-stem" gradientUnits="userSpaceOnUse" x1="360" y1="125" x2="360" y2="1110">
        <stop offset="0" class="kn-logo-stem-top" />
        <stop offset="1" class="kn-logo-stem-bottom" />
      </linearGradient>
      <linearGradient id="kn-logo-upper" gradientUnits="userSpaceOnUse" x1="420" y1="632" x2="1258" y2="128">
        <stop offset="0" stop-color="#1238f5" />
        <stop offset="1" stop-color="#00e5ff" />
      </linearGradient>
      <linearGradient id="kn-logo-lower" gradientUnits="userSpaceOnUse" x1="420" y1="632" x2="1240" y2="1136">
        <stop offset="0" stop-color="#2a0fb8" />
        <stop offset="0.45" stop-color="#5a2ff0" />
        <stop offset="1" stop-color="#a35cf7" />
      </linearGradient>
      <linearGradient id="kn-logo-sheen" x1="0" y1="0" x2="1" y2="0">
        <stop offset="0" stop-color="#fff" stop-opacity="0" />
        <stop offset="0.5" stop-color="#fff" stop-opacity="0.45" />
        <stop offset="1" stop-color="#fff" stop-opacity="0" />
      </linearGradient>
      <!-- the gap only cuts into the stem, never into the other arm -->
      <clipPath id="kn-logo-stem-clip">
        <path :d="stem" />
      </clipPath>
      <mask id="kn-logo-mask">
        <path :d="stem" fill="#fff" />
        <path :d="upper" fill="#fff" />
        <path :d="lower" fill="#fff" />
      </mask>
    </defs>
    <g class="kn-logo-float">
      <path class="kn-logo-stem" :d="stem" fill="url(#kn-logo-stem)" />
      <g class="kn-logo-upper">
        <path class="kn-logo-gap" :d="upper" clip-path="url(#kn-logo-stem-clip)" />
        <path :d="upper" fill="url(#kn-logo-upper)" />
      </g>
      <g class="kn-logo-lower">
        <path class="kn-logo-gap" :d="lower" clip-path="url(#kn-logo-stem-clip)" />
        <path :d="lower" fill="url(#kn-logo-lower)" />
      </g>
      <g mask="url(#kn-logo-mask)">
        <rect class="kn-logo-sheen" x="-200" y="60" width="320" height="1140" fill="url(#kn-logo-sheen)" />
      </g>
    </g>
  </svg>
</template>

<style>
.kn-logo {
  position: absolute;
  top: 50%;
  left: 50%;
  width: 192px;
  height: 192px;
  transform: translate(-50%, -50%);
  overflow: visible;
}
@media (min-width: 640px) {
  .kn-logo { width: 256px; height: 256px; }
}
@media (min-width: 960px) {
  .kn-logo { width: 300px; height: 300px; }
}

/* The stem is dark navy; in dark mode it is lifted so it still reads on the page. */
.kn-logo-stem-top { stop-color: #1e3a9a; }
.kn-logo-stem-bottom { stop-color: #0c1445; }
.dark .kn-logo-stem-top { stop-color: #3554cf; }
.dark .kn-logo-stem-bottom { stop-color: #1b2675; }

/* the gap between arms and stem is drawn in the page background */
.kn-logo-gap {
  fill: none;
  stroke: var(--vp-c-bg);
  stroke-width: 44;
  stroke-linejoin: round;
}

.kn-logo-stem,
.kn-logo-upper,
.kn-logo-lower {
  transform-box: fill-box;
  animation: kn-logo-in 0.8s cubic-bezier(0.2, 0.8, 0.2, 1) both;
}
.kn-logo-stem { --kn-from: translate(0, 90px); }
.kn-logo-upper { --kn-from: translate(160px, -160px); animation-delay: 0.3s; }
.kn-logo-lower { --kn-from: translate(160px, 160px); animation-delay: 0.45s; }

@keyframes kn-logo-in {
  from { opacity: 0; transform: var(--kn-from); }
  to { opacity: 1; transform: none; }
}

.kn-logo-float {
  animation: kn-logo-float 6s ease-in-out 1.4s infinite;
}
@keyframes kn-logo-float {
  0%, 100% { transform: translateY(0); }
  50% { transform: translateY(-24px); }
}

.kn-logo-sheen {
  animation: kn-logo-sheen 7s ease-in-out 1.6s infinite;
  transform: translateX(0);
}
@keyframes kn-logo-sheen {
  0% { transform: translateX(0); }
  30%, 100% { transform: translateX(1500px); }
}

@media (prefers-reduced-motion: reduce) {
  .kn-logo-stem,
  .kn-logo-upper,
  .kn-logo-lower,
  .kn-logo-float,
  .kn-logo-sheen {
    animation: none;
  }
  .kn-logo-sheen { display: none; }
}
</style>
