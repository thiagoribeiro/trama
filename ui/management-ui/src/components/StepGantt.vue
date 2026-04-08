<template>
  <div class="gantt" ref="containerRef">
    <div v-if="rows.length === 0" class="gantt__empty dim">No step timing data available.</div>
    <div v-else class="gantt__scroll">
      <svg :width="totalW" :height="totalH" class="gantt__svg">
        <!-- Grid lines -->
        <g class="gantt__grid">
          <line
            v-for="tick in timeTicks"
            :key="tick.px"
            :x1="LEFT + tick.px" :y1="0"
            :x2="LEFT + tick.px" :y2="chartH"
            stroke="#21262d" stroke-width="1"
          />
        </g>

        <!-- Rows -->
        <g v-for="(row, ri) in rows" :key="row.name">
          <!-- Lane background -->
          <rect
            :x="0" :y="ri * ROW_H"
            :width="totalW" :height="ROW_H - ROW_GAP"
            :fill="ri % 2 === 0 ? '#0d1117' : '#111820'"
          />
          <!-- Step name label -->
          <text
            :x="LEFT - 8" :y="ri * ROW_H + ROW_H / 2 - ROW_GAP / 2 + 1"
            text-anchor="end"
            dominant-baseline="middle"
            fill="#8b949e"
            font-size="11"
            font-family="ui-monospace, monospace"
          >{{ row.name }}</text>

          <!-- Bars for each attempt -->
          <g v-for="bar in row.bars" :key="`${bar.attempt}-${bar.phase}`">
            <rect
              :x="LEFT + bar.startPx"
              :y="ri * ROW_H + BAR_INSET"
              :width="Math.max(bar.widthPx, 2)"
              :height="BAR_H"
              :fill="bar.fill"
              :stroke="bar.stroke"
              stroke-width="1"
              rx="3"
              class="gantt__bar"
              @mouseenter="showTip($event, bar)"
              @mouseleave="hideTip"
            />
            <!-- Duration label inside bar (if wide enough) -->
            <text
              v-if="bar.widthPx > 36"
              :x="LEFT + bar.startPx + bar.widthPx / 2"
              :y="ri * ROW_H + BAR_INSET + BAR_H / 2 + 1"
              text-anchor="middle"
              dominant-baseline="middle"
              fill="#e2e8f0"
              font-size="9"
              font-family="ui-monospace, monospace"
              pointer-events="none"
            >{{ fmtMs(bar.durationMs) }}</text>
          </g>
        </g>

        <!-- Phase separator line (DOWN phase starts here, if any) -->
        <line
          v-if="downStartPx !== null"
          :x1="LEFT + downStartPx" :y1="0"
          :x2="LEFT + downStartPx" :y2="chartH"
          stroke="#e3b341" stroke-width="1" stroke-dasharray="4,3"
          opacity="0.5"
        />

        <!-- X axis -->
        <line :x1="LEFT" :y1="chartH" :x2="totalW - RIGHT" :y2="chartH" stroke="#30363d" stroke-width="1"/>
        <g v-for="tick in timeTicks" :key="`t${tick.px}`">
          <line
            :x1="LEFT + tick.px" :y1="chartH"
            :x2="LEFT + tick.px" :y2="chartH + 4"
            stroke="#30363d" stroke-width="1"
          />
          <text
            :x="LEFT + tick.px" :y="chartH + 14"
            text-anchor="middle"
            fill="#8b949e"
            font-size="9"
            font-family="ui-monospace, monospace"
          >{{ tick.label }}</text>
        </g>

        <!-- Total duration label -->
        <text
          :x="totalW - RIGHT" :y="chartH + 14"
          text-anchor="end"
          fill="#8b949e"
          font-size="9"
          font-family="ui-monospace, monospace"
        >{{ fmtMs(totalDurationMs) }}</text>
      </svg>
    </div>

    <!-- Tooltip -->
    <div
      v-if="tip"
      class="gantt__tip"
      :style="{ left: tip.x + 'px', top: tip.y + 'px' }"
    >
      <div class="gantt__tip-row"><span class="dim">step</span> {{ tip.name }}</div>
      <div class="gantt__tip-row"><span class="dim">phase</span> {{ tip.phase }}</div>
      <div v-if="tip.attempt !== undefined" class="gantt__tip-row"><span class="dim">attempt</span> #{{ tip.attempt }}</div>
      <div class="gantt__tip-row"><span class="dim">start</span> +{{ fmtMs(tip.startMs) }}</div>
      <div class="gantt__tip-row"><span class="dim">duration</span> {{ fmtMs(tip.durationMs) }}</div>
      <div v-if="tip.statusCode" class="gantt__tip-row"><span class="dim">status</span> {{ tip.statusCode }}</div>
      <div v-if="tip.url" class="gantt__tip-row gantt__tip-url dim">{{ tip.url }}</div>
    </div>
  </div>
</template>

<script setup>
import { ref, computed, onMounted, onUnmounted } from 'vue'

const props = defineProps({
  /** execution.startedAt ISO string — the zero point */
  startedAt: { type: String, default: null },
  /** steps array from GET /sagas/{id}/steps */
  steps: { type: Array, default: () => [] },
  /** calls array from GET /sagas/{id}/steps/calls */
  calls: { type: Array, default: () => [] },
})

// ── Layout constants ───────────────────────────────────────────────────────────

const LEFT    = 150   // label column width
const RIGHT   = 20    // right padding
const ROW_H   = 30    // row height (including gap)
const ROW_GAP = 4     // gap between rows
const BAR_H   = 18    // bar height
const BAR_INSET = (ROW_H - ROW_GAP - BAR_H) / 2  // vertical centering

const containerRef = ref(null)
const containerW   = ref(600)

const totalW  = computed(() => Math.max(containerW.value, LEFT + RIGHT + 200))
const chartW  = computed(() => totalW.value - LEFT - RIGHT)

// ── Build rows ────────────────────────────────────────────────────────────────

const execStart = computed(() => props.startedAt ? new Date(props.startedAt).getTime() : null)

// Use calls if available (better per-step timing), otherwise fall back to steps
const useCalls = computed(() => props.calls && props.calls.length > 0)

// Total duration: max createdAt across all records
const totalDurationMs = computed(() => {
  const t0 = execStart.value
  if (!t0) return 0
  let max = 0
  const records = useCalls.value ? props.calls : props.steps
  for (const r of records) {
    const end = new Date(r.createdAt).getTime() - t0
    if (end > max) max = end
  }
  return max || 1
})

function barColor(phase, success, error) {
  if (phase === 'DOWN') return { fill: '#2b1e00', stroke: '#e3b341' }
  if (success)          return { fill: '#0d2b0d', stroke: '#3fb950' }
  if (error)            return { fill: '#2d0a0a', stroke: '#f85149' }
  return                       { fill: '#1c2a4a', stroke: '#4a6fa5' }
}

const rows = computed(() => {
  const t0 = execStart.value
  if (!t0 || totalDurationMs.value === 0) return []

  const scale = chartW.value / totalDurationMs.value
  const rowMap = new Map()

  if (useCalls.value) {
    for (const c of props.calls) {
      const startMs  = c.stepStartedAt ? new Date(c.stepStartedAt).getTime() - t0 : 0
      const endMs    = new Date(c.createdAt).getTime() - t0
      const durationMs = endMs - startMs
      const { fill, stroke } = barColor(c.phase, c.success && !c.error, c.error)

      if (!rowMap.has(c.stepName)) rowMap.set(c.stepName, { name: c.stepName, bars: [] })
      rowMap.get(c.stepName).bars.push({
        startPx:    startMs * scale,
        widthPx:    Math.max(durationMs * scale, 2),
        durationMs,
        startMs,
        fill, stroke,
        phase:      c.phase,
        attempt:    c.attempt,
        statusCode: c.statusCode,
        url:        c.requestUrl,
        name:       c.stepName,
      })
    }
  } else {
    for (const s of props.steps) {
      const ref   = s.stepStartedAt ?? s.startedAt
      const startMs  = ref ? new Date(ref).getTime() - t0 : 0
      const endMs    = new Date(s.createdAt).getTime() - t0
      const durationMs = endMs - startMs
      const { fill, stroke } = barColor(s.phase, s.success, false)

      const key = `${s.stepName}::${s.phase}`
      if (!rowMap.has(key)) rowMap.set(key, { name: s.phase === 'DOWN' ? `${s.stepName} ↩` : s.stepName, bars: [] })
      rowMap.get(key).bars.push({
        startPx:    startMs * scale,
        widthPx:    Math.max(durationMs * scale, 2),
        durationMs,
        startMs,
        fill, stroke,
        phase:      s.phase,
        statusCode: s.statusCode,
        name:       s.stepName,
      })
    }
  }

  return [...rowMap.values()]
})

const chartH  = computed(() => rows.value.length * ROW_H)
const totalH  = computed(() => chartH.value + 24)

// ── DOWN phase separator ───────────────────────────────────────────────────────

const downStartPx = computed(() => {
  const t0 = execStart.value
  if (!t0) return null
  const scale = chartW.value / totalDurationMs.value
  let min = Infinity
  const records = useCalls.value ? props.calls : props.steps
  for (const r of records) {
    if (r.phase === 'DOWN') {
      const ref = r.stepStartedAt ?? r.startedAt
      const ms = ref ? new Date(ref).getTime() - t0 : Infinity
      if (ms < min) min = ms
    }
  }
  return min === Infinity ? null : min * scale
})

// ── Time axis ticks ───────────────────────────────────────────────────────────

const timeTicks = computed(() => {
  const total = totalDurationMs.value
  if (total === 0) return []
  const w = chartW.value

  // Choose a tick interval
  const targets = [50, 100, 200, 500, 1000, 2000, 5000, 10000, 30000, 60000]
  const minPxBetween = 60
  const interval = targets.find(t => (t / total) * w >= minPxBetween) ?? targets[targets.length - 1]

  const ticks = []
  for (let ms = interval; ms < total; ms += interval) {
    ticks.push({ px: (ms / total) * w, label: fmtMs(ms) })
  }
  return ticks
})

// ── Tooltip ────────────────────────────────────────────────────────────────────

const tip = ref(null)

function showTip(e, bar) {
  const rect = containerRef.value?.getBoundingClientRect()
  if (!rect) return
  tip.value = {
    x: e.clientX - rect.left + 12,
    y: e.clientY - rect.top  - 10,
    ...bar,
  }
}
function hideTip() { tip.value = null }

// ── Resize observer ────────────────────────────────────────────────────────────

let ro
onMounted(() => {
  if (containerRef.value) {
    ro = new ResizeObserver(([e]) => { containerW.value = e.contentRect.width })
    ro.observe(containerRef.value)
    containerW.value = containerRef.value.clientWidth
  }
})
onUnmounted(() => ro?.disconnect())

// ── Helpers ────────────────────────────────────────────────────────────────────

function fmtMs(ms) {
  if (ms == null) return '—'
  if (ms < 1000) return `${Math.round(ms)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}
</script>

<style scoped>
.gantt {
  position: relative;
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 6px;
  overflow: hidden;
}

.gantt__empty {
  padding: 1.5rem;
  font-size: 0.85rem;
}

.gantt__scroll {
  overflow-x: auto;
}

.gantt__svg {
  display: block;
  cursor: default;
}

.gantt__bar {
  cursor: pointer;
  transition: opacity 0.1s;
}
.gantt__bar:hover { opacity: 0.8; }

.gantt__tip {
  position: absolute;
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 6px;
  padding: 0.5rem 0.75rem;
  font-size: 0.78rem;
  pointer-events: none;
  z-index: 10;
  min-width: 160px;
  max-width: 280px;
}

.gantt__tip-row {
  display: flex;
  gap: 0.5rem;
  margin-bottom: 2px;
}

.gantt__tip-url {
  font-family: ui-monospace, monospace;
  font-size: 0.72rem;
  word-break: break-all;
  margin-top: 4px;
}

.dim { color: #8b949e; }
</style>
