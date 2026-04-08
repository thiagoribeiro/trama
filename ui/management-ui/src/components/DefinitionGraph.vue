<template>
  <div class="def-graph" ref="containerRef">
    <div v-if="parseError" class="def-graph__msg dim">{{ parseError }}</div>
    <div v-else-if="!snapshot" class="def-graph__msg dim">No graph definition.</div>
    <template v-else>
      <div class="def-graph__wrap" ref="wrapRef">
        <svg ref="svgRef" class="def-graph__svg" />
      </div>
      <div class="def-graph__controls">
        <button class="def-graph__btn" @click="fitView">Fit</button>
        <button class="def-graph__btn" @click="zoomBy(1.25)">+</button>
        <button class="def-graph__btn" @click="zoomBy(0.8)">−</button>
      </div>
      <div class="def-graph__legend">
        <span class="legend-dot legend-dot--ok"></span>completed
        <span class="legend-dot legend-dot--err"></span>failed
        <span class="legend-dot legend-dot--comp"></span>compensated
      </div>
    </template>
  </div>
</template>

<script setup>
import { ref, computed, watch, onMounted, onUnmounted, nextTick } from 'vue'
import { importDefinition } from '../../../definition-editor/js/io.js'

const props = defineProps({
  /** Raw definition object from API (SagaDefinitionResponse.definition) */
  definition: { type: Object, default: null },
  /** Step results array — used to colour nodes */
  steps: { type: Array, default: () => [] },
})

// ── Parse ──────────────────────────────────────────────────────────────────────

const snapshot  = ref(null)
const parseError = ref(null)

watch(() => props.definition, def => {
  if (!def) { snapshot.value = null; parseError.value = null; return }
  const result = importDefinition(def)
  if (!result.ok) {
    snapshot.value = null
    parseError.value = 'Could not render graph: ' + result.errors.join(', ')
  } else {
    parseError.value = null
    snapshot.value = result.snapshot
  }
}, { immediate: true })

// ── Node status from step results ─────────────────────────────────────────────

const nodeStatuses = computed(() => {
  const map = new Map()
  for (const step of props.steps) {
    const cur = map.get(step.stepName)
    if (step.phase === 'UP') {
      if (step.success) {
        if (!cur || cur === 'success') map.set(step.stepName, 'success')
      } else {
        map.set(step.stepName, 'failed')
      }
    } else if (step.phase === 'DOWN' && step.success) {
      if (cur !== 'failed') map.set(step.stepName, 'compensated')
    }
  }
  return map
})

// ── SVG refs + viewport ───────────────────────────────────────────────────────

const svgRef  = ref(null)
const wrapRef = ref(null)

let rootGroup = null
let vp = { tx: 80, ty: 60, scale: 1 }

function applyViewport() {
  if (rootGroup) rootGroup.setAttribute('transform', `translate(${vp.tx},${vp.ty}) scale(${vp.scale})`)
}

function zoomBy(factor) {
  if (!svgRef.value) return
  const rect = svgRef.value.getBoundingClientRect()
  const cx = rect.width / 2, cy = rect.height / 2
  vp.tx = cx - (cx - vp.tx) * factor
  vp.ty = cy - (cy - vp.ty) * factor
  vp.scale = Math.min(4, Math.max(0.1, vp.scale * factor))
  applyViewport()
}

function fitView() {
  if (!svgRef.value || !snapshot.value || snapshot.value.nodes.length === 0) return
  const nodes = snapshot.value.nodes
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity
  for (const n of nodes) {
    const w = n.kind === 'task' ? 190 : 160
    const h = n.kind === 'task' ?  64 :  84
    minX = Math.min(minX, n.x); minY = Math.min(minY, n.y)
    maxX = Math.max(maxX, n.x + w); maxY = Math.max(maxY, n.y + h)
  }
  const rect = svgRef.value.getBoundingClientRect()
  const pad = 60
  const scaleX = (rect.width  - pad * 2) / (maxX - minX || 1)
  const scaleY = (rect.height - pad * 2) / (maxY - minY || 1)
  vp.scale = Math.min(2, Math.max(0.1, Math.min(scaleX, scaleY)))
  vp.tx = pad - minX * vp.scale
  vp.ty = pad - minY * vp.scale
  applyViewport()
}

// ── Wheel zoom + pan ──────────────────────────────────────────────────────────

function onWheel(e) {
  e.preventDefault()
  const factor = e.deltaY > 0 ? 0.9 : 1.11
  const rect = svgRef.value.getBoundingClientRect()
  vp.tx = (e.clientX - rect.left) - ((e.clientX - rect.left) - vp.tx) * factor
  vp.ty = (e.clientY - rect.top)  - ((e.clientY - rect.top)  - vp.ty) * factor
  vp.scale = Math.min(4, Math.max(0.1, vp.scale * factor))
  applyViewport()
}

let panOrigin = null
function onPanStart(e) {
  if (e.target !== svgRef.value && e.target !== rootGroup) return
  panOrigin = { mx: e.clientX, my: e.clientY, tx: vp.tx, ty: vp.ty }
  const onMove = ev => {
    vp.tx = panOrigin.tx + ev.clientX - panOrigin.mx
    vp.ty = panOrigin.ty + ev.clientY - panOrigin.my
    applyViewport()
  }
  const onUp = () => { window.removeEventListener('mousemove', onMove); window.removeEventListener('mouseup', onUp) }
  window.addEventListener('mousemove', onMove)
  window.addEventListener('mouseup', onUp)
}

// ── Rendering ─────────────────────────────────────────────────────────────────

const NS = 'http://www.w3.org/2000/svg'
function el(tag) { return document.createElementNS(NS, tag) }
function sa(elem, attrs) { for (const [k, v] of Object.entries(attrs)) elem.setAttribute(k, v); return elem }
function trunc(s, max) { return s && s.length > max ? s.slice(0, max - 1) + '…' : (s || '') }

function buildDefs(svg) {
  const defs = el('defs')
  defs.innerHTML = `
    <marker id="dg-arrow" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
      <polygon points="0 0, 8 3, 0 6" fill="#4a6080"/>
    </marker>
    <marker id="dg-arrow-err" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
      <polygon points="0 0, 8 3, 0 6" fill="#ef4444"/>
    </marker>
  `
  svg.appendChild(defs)
}

function renderGraph(st, statuses) {
  const svg = svgRef.value
  if (!svg) return

  // Clear
  while (svg.firstChild) svg.removeChild(svg.firstChild)
  buildDefs(svg)

  rootGroup = el('g')
  rootGroup.id = 'dg-root'
  svg.appendChild(rootGroup)
  applyViewport()

  const nodeMap = new Map(st.nodes.map(n => [n.id, n]))
  const cycleSet = detectCycles(nodeMap)

  // Edges first
  const edgeGroup = el('g')
  renderEdges(edgeGroup, nodeMap, cycleSet)
  rootGroup.appendChild(edgeGroup)

  // Start pill
  if (st.entrypoint && nodeMap.has(st.entrypoint)) {
    renderStartPill(rootGroup, nodeMap.get(st.entrypoint))
  }

  // Nodes
  const terminals = computeTerminals(nodeMap)
  for (const [, node] of nodeMap) {
    renderNode(rootGroup, node, terminals.has(node.id), cycleSet.has(node.id), statuses.get(node.id))
  }
}

function computeTerminals(nodeMap) {
  const t = new Set()
  for (const [, n] of nodeMap) if (n.kind === 'task' && !n.next) t.add(n.id)
  return t
}

function detectCycles(nodeMap) {
  const ids = [...nodeMap.keys()]
  const indeg = new Map(ids.map(id => [id, 0]))
  const adj   = new Map(ids.map(id => [id, []]))
  for (const [, n] of nodeMap) {
    for (const t of nodeTargets(n)) {
      if (nodeMap.has(t)) { adj.get(n.id).push(t); indeg.set(t, indeg.get(t) + 1) }
    }
  }
  const queue = ids.filter(id => indeg.get(id) === 0), visited = new Set()
  while (queue.length) {
    const id = queue.shift(); visited.add(id)
    for (const t of adj.get(id)) { indeg.set(t, indeg.get(t) - 1); if (indeg.get(t) === 0) queue.push(t) }
  }
  return new Set(ids.filter(id => !visited.has(id)))
}

function nodeTargets(node) {
  if (node.kind === 'task') return node.next ? [node.next] : []
  if (node.kind === 'switch') return [...(node.cases || []).map(c => c.target), node.default].filter(Boolean)
  return []
}

function statusColors(status, kind) {
  if (status === 'success')     return { stroke: '#3fb950', fill: kind === 'task' ? '#0d2b0d' : '#0a2010' }
  if (status === 'failed')      return { stroke: '#f85149', fill: kind === 'task' ? '#2d0a0a' : '#2a0808' }
  if (status === 'compensated') return { stroke: '#e3b341', fill: kind === 'task' ? '#2b1e00' : '#251800' }
  return null
}

function renderNode(parent, node, terminal, inCycle, status) {
  const g = el('g')
  const sc = statusColors(status, node.kind)

  if (node.kind === 'task') {
    const W = 190, H = 64
    const stroke = inCycle ? '#ef4444' : sc ? sc.stroke : '#2d4070'
    const fill   = inCycle ? '#2d0a0a'  : sc ? sc.fill   : '#151f38'
    g.appendChild(sa(el('rect'), { x: node.x, y: node.y, width: W, height: H, rx: 8, fill, stroke, 'stroke-width': sc || inCycle ? 2 : 1 }))

    const badgeFill = node.mode === 'async' ? '#5b21b6' : '#065f46'
    g.appendChild(sa(el('rect'), { x: node.x + W - 54, y: node.y + 5, width: 50, height: 16, rx: 4, fill: badgeFill }))
    const bt = sa(el('text'), { x: node.x + W - 29, y: node.y + 17, 'text-anchor': 'middle', fill: '#d1fae5', 'font-size': 9, 'font-family': 'monospace' })
    bt.textContent = (node.mode || 'sync').toUpperCase()
    g.appendChild(bt)

    const nameText = sa(el('text'), { x: node.x + 10, y: node.y + H / 2 + 5, fill: sc ? sc.stroke : '#b0c4e8', 'font-size': 13, 'font-family': 'system-ui, sans-serif', 'font-weight': '600' })
    nameText.textContent = trunc(node.id, 18)
    g.appendChild(nameText)

    const rawUrl = node.request?.url
    if (rawUrl) {
      const urlText = sa(el('text'), { x: node.x + 10, y: node.y + H - 8, fill: '#4a6080', 'font-size': 9, 'font-family': 'monospace' })
      urlText.textContent = trunc(String(rawUrl).replace(/^https?:\/\//, ''), 28)
      g.appendChild(urlText)
    }

    if (terminal) {
      g.appendChild(sa(el('rect'), { x: node.x + W + 4, y: node.y + H / 2 - 8, width: 32, height: 14, rx: 3, fill: '#14532d' }))
      const et = sa(el('text'), { x: node.x + W + 20, y: node.y + H / 2 + 3, 'text-anchor': 'middle', fill: '#86efac', 'font-size': 9, 'font-family': 'monospace' })
      et.textContent = 'END'
      g.appendChild(et)
    }

    if (status) {
      const icon = status === 'success' ? '✓' : status === 'failed' ? '✗' : '↩'
      const badge = sa(el('text'), { x: node.x + 8, y: node.y - 4, fill: stroke, 'font-size': 11, 'font-weight': 'bold' })
      badge.textContent = icon
      g.appendChild(badge)
    }
  } else {
    const W = 160, H = 84, cx = node.x + W / 2, cy = node.y + H / 2
    const stroke = inCycle ? '#ef4444' : sc ? sc.stroke : '#6d3fd6'
    const fill   = inCycle ? '#2d0a0a'  : sc ? sc.fill   : '#1a0d38'
    g.appendChild(sa(el('polygon'), { points: `${cx},${node.y} ${node.x + W},${cy} ${cx},${node.y + H} ${node.x},${cy}`, fill, stroke, 'stroke-width': sc || inCycle ? 2 : 1 }))
    const label = sa(el('text'), { x: cx, y: cy + 5, 'text-anchor': 'middle', fill: sc ? sc.stroke : '#c4aaff', 'font-size': 12, 'font-family': 'system-ui, sans-serif', 'font-weight': '600' })
    label.textContent = trunc(node.id, 14)
    g.appendChild(label)
  }

  parent.appendChild(g)
}

function renderEdges(parent, nodeMap, cycleSet) {
  for (const [, node] of nodeMap) {
    if (node.kind === 'task' && node.next && nodeMap.has(node.next)) {
      const inCycle = cycleSet.has(node.id) && cycleSet.has(node.next)
      drawEdge(parent, outPort(node), inPort(nodeMap.get(node.next)), null, false, inCycle)
    } else if (node.kind === 'switch') {
      for (const c of (node.cases || [])) {
        if (c.target && nodeMap.has(c.target)) {
          const inCycle = cycleSet.has(node.id) && cycleSet.has(c.target)
          drawEdge(parent, switchOutPort(node), inPort(nodeMap.get(c.target)), c.name || '', false, inCycle)
        }
      }
      if (node.default && nodeMap.has(node.default)) {
        const inCycle = cycleSet.has(node.id) && cycleSet.has(node.default)
        drawEdge(parent, switchOutPort(node), inPort(nodeMap.get(node.default)), 'default', true, inCycle)
      }
    }
  }
}

function drawEdge(parent, src, dst, label, dashed, inCycle) {
  const dx = Math.abs(dst.x - src.x)
  const cp = Math.max(50, dx * 0.55)
  const d  = `M ${src.x} ${src.y} C ${src.x + cp} ${src.y} ${dst.x - cp} ${dst.y} ${dst.x} ${dst.y}`
  const col = inCycle ? '#ef4444' : dashed ? '#4a4060' : '#3a5080'
  const mrk = inCycle ? 'url(#dg-arrow-err)' : 'url(#dg-arrow)'
  parent.appendChild(sa(el('path'), { d, stroke: col, 'stroke-width': 1.5, fill: 'none', 'stroke-dasharray': dashed ? '5,3' : '', 'marker-end': mrk, 'pointer-events': 'none' }))
  if (label) {
    const mx = (src.x + dst.x) / 2, my = (src.y + dst.y) / 2
    const tw = label.length * 5.5 + 8
    parent.appendChild(sa(el('rect'), { x: mx - tw / 2, y: my - 9, width: tw, height: 16, rx: 3, fill: '#0d1117' }))
    const lt = sa(el('text'), { x: mx, y: my + 4, 'text-anchor': 'middle', fill: inCycle ? '#fca5a5' : '#6a7fa8', 'font-size': 9, 'font-family': 'monospace' })
    lt.textContent = label
    parent.appendChild(lt)
  }
}

function renderStartPill(parent, entryNode) {
  const ip = inPort(entryNode)
  const px = ip.x - 70
  const g = el('g')
  g.appendChild(sa(el('path'), { d: `M ${px + 25} ${ip.y} L ${ip.x} ${ip.y}`, stroke: '#1a7a4a', 'stroke-width': 1.5, fill: 'none', 'marker-end': 'url(#dg-arrow)' }))
  g.appendChild(sa(el('rect'), { x: px - 23, y: ip.y - 10, width: 48, height: 20, rx: 10, fill: '#14532d' }))
  const t = sa(el('text'), { x: px + 1, y: ip.y + 5, 'text-anchor': 'middle', fill: '#86efac', 'font-size': 10, 'font-family': 'monospace', 'font-weight': 'bold' })
  t.textContent = 'START'
  g.appendChild(t)
  parent.appendChild(g)
}

function inPort(node)       { return { x: node.x, y: node.y + nh(node) / 2 } }
function outPort(node)      { return { x: node.x + nw(node), y: node.y + nh(node) / 2 } }
function switchOutPort(node){ return { x: node.x + 160, y: node.y + 84 / 2 } }
function nw(node) { return node.kind === 'task' ? 190 : 160 }
function nh(node) { return node.kind === 'task' ?  64 :  84 }

// ── Lifecycle ─────────────────────────────────────────────────────────────────

function drawIfReady() {
  if (!snapshot.value || !svgRef.value) return
  renderGraph(snapshot.value, nodeStatuses.value)
  nextTick(fitView)
}

watch([snapshot, nodeStatuses], () => {
  nextTick(drawIfReady)
}, { deep: false })

onMounted(() => {
  nextTick(() => {
    if (svgRef.value) {
      svgRef.value.addEventListener('wheel', onWheel, { passive: false })
      svgRef.value.addEventListener('mousedown', onPanStart)
    }
    drawIfReady()
  })
})

onUnmounted(() => {
  if (svgRef.value) {
    svgRef.value.removeEventListener('wheel', onWheel)
    svgRef.value.removeEventListener('mousedown', onPanStart)
  }
})
</script>

<style scoped>
.def-graph {
  position: relative;
  display: flex;
  flex-direction: column;
  background: #0d1117;
  border: 1px solid #30363d;
  border-radius: 6px;
  overflow: hidden;
  min-height: 340px;
}

.def-graph__wrap {
  flex: 1;
  overflow: hidden;
  background: #0d1117;
  background-image: radial-gradient(circle, #1e2d50 1px, transparent 1px);
  background-size: 32px 32px;
}

.def-graph__svg {
  width: 100%;
  height: 100%;
  min-height: 320px;
  display: block;
}

.def-graph__msg {
  padding: 2rem;
  text-align: center;
  font-size: 0.85rem;
}

.def-graph__controls {
  position: absolute;
  top: 0.5rem;
  right: 0.5rem;
  display: flex;
  gap: 4px;
}

.def-graph__btn {
  background: #21262d;
  border: 1px solid #30363d;
  color: #c9d1d9;
  border-radius: 4px;
  padding: 2px 8px;
  font-size: 0.75rem;
  cursor: pointer;
}
.def-graph__btn:hover { background: #30363d; }

.def-graph__legend {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.35rem 0.75rem;
  font-size: 0.72rem;
  color: #8b949e;
  border-top: 1px solid #21262d;
}

.legend-dot {
  display: inline-block;
  width: 8px;
  height: 8px;
  border-radius: 50%;
  margin-left: 0.5rem;
}
.legend-dot--ok   { background: #3fb950; }
.legend-dot--err  { background: #f85149; }
.legend-dot--comp { background: #e3b341; }
</style>
