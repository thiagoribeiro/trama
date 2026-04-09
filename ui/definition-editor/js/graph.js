// graph.js — SVG canvas: rendering, zoom/pan, auto-layout, minimap

import * as state from './state.js';

const NODE_TASK_W = 190;
const NODE_TASK_H = 64;
const NODE_SWITCH_W = 160;
const NODE_SWITCH_H = 84;
const NODE_SLEEP_W = 190;
const NODE_SLEEP_H = 64;
const COL_W = 280;
const ROW_H = 150;

let svgEl, rootGroup, _bus;
let viewport = { tx: 80, ty: 100, scale: 1 };

// ── Init ──────────────────────────────────────────────────────────────────────

export function init(svg, bus) {
  svgEl = svg;
  _bus = bus;

  const defs = svgNS('defs');
  defs.innerHTML = `
    <marker id="arrow" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
      <polygon points="0 0, 8 3, 0 6" fill="#4a6080"/>
    </marker>
    <marker id="arrow-hi" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
      <polygon points="0 0, 8 3, 0 6" fill="#7c9ef8"/>
    </marker>
    <marker id="arrow-err" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
      <polygon points="0 0, 8 3, 0 6" fill="#ef4444"/>
    </marker>
    <filter id="glow" x="-20%" y="-20%" width="140%" height="140%">
      <feGaussianBlur stdDeviation="3" result="blur"/>
      <feMerge><feMergeNode in="blur"/><feMergeNode in="SourceGraphic"/></feMerge>
    </filter>
  `;
  svg.appendChild(defs);

  rootGroup = svgNS('g');
  rootGroup.id = 'canvas-root';
  svg.appendChild(rootGroup);

  applyViewport();

  svg.addEventListener('wheel', onWheel, { passive: false });
  svg.addEventListener('mousedown', onPanStart);
  svg.addEventListener('click', e => {
    if (e.target === svgEl || e.target === rootGroup) state.selectNode(null);
  });

  bus.addEventListener('state:changed', () => render(state.getState()));

  initMinimap();
}

function applyViewport() {
  rootGroup.setAttribute('transform',
    `translate(${viewport.tx}, ${viewport.ty}) scale(${viewport.scale})`);
  updateMinimap(state.getState());
}

function onWheel(e) {
  e.preventDefault();
  const factor = e.deltaY > 0 ? 0.9 : 1.11;
  const rect = svgEl.getBoundingClientRect();
  const mx = e.clientX - rect.left;
  const my = e.clientY - rect.top;
  viewport.tx = mx - (mx - viewport.tx) * factor;
  viewport.ty = my - (my - viewport.ty) * factor;
  viewport.scale = Math.min(3, Math.max(0.15, viewport.scale * factor));
  applyViewport();
}

let _panOrigin = null;
function onPanStart(e) {
  if (e.target !== svgEl && e.target !== rootGroup) return;
  _panOrigin = { mx: e.clientX, my: e.clientY, tx: viewport.tx, ty: viewport.ty };
  const onMove = ev => {
    viewport.tx = _panOrigin.tx + ev.clientX - _panOrigin.mx;
    viewport.ty = _panOrigin.ty + ev.clientY - _panOrigin.my;
    applyViewport();
  };
  const onUp = () => {
    window.removeEventListener('mousemove', onMove);
    window.removeEventListener('mouseup', onUp);
  };
  window.addEventListener('mousemove', onMove);
  window.addEventListener('mouseup', onUp);
}

export function screenToCanvas(sx, sy) {
  const rect = svgEl.getBoundingClientRect();
  return {
    x: (sx - rect.left - viewport.tx) / viewport.scale,
    y: (sy - rect.top  - viewport.ty) / viewport.scale,
  };
}

// ── Render ────────────────────────────────────────────────────────────────────

export function render(st) {
  while (rootGroup.firstChild) rootGroup.removeChild(rootGroup.firstChild);

  const { nodes, entrypoint, selectedNodeId } = st;
  const terminalSet = computeTerminals(nodes);
  const cycleSet    = detectCycles(nodes);
  const issues      = computeIssues(nodes);

  const edgeGroup = svgNS('g');
  renderEdges(edgeGroup, nodes, cycleSet);
  rootGroup.appendChild(edgeGroup);

  if (entrypoint && nodes.has(entrypoint)) {
    renderStartPill(rootGroup, nodes.get(entrypoint));
  }

  for (const [, node] of nodes) {
    renderNode(rootGroup, node,
      node.id === selectedNodeId,
      terminalSet.has(node.id),
      cycleSet.has(node.id),
      issues.get(node.id) ?? []);
  }

  updateMinimap(st);
}

function computeTerminals(nodes) {
  const t = new Set();
  for (const [, n] of nodes) {
    if ((n.kind === 'task' || n.kind === 'sleep') && !n.next) t.add(n.id);
  }
  return t;
}

// ── Cycle detection (Kahn's algorithm) ────────────────────────────────────────

function detectCycles(nodes) {
  const ids   = [...nodes.keys()];
  const indeg = new Map(ids.map(id => [id, 0]));
  const adj   = new Map(ids.map(id => [id, []]));

  for (const [, n] of nodes) {
    for (const t of nodeTargets(n)) {
      if (nodes.has(t)) {
        adj.get(n.id).push(t);
        indeg.set(t, indeg.get(t) + 1);
      }
    }
  }

  const queue   = ids.filter(id => indeg.get(id) === 0);
  const visited = new Set();
  while (queue.length) {
    const id = queue.shift();
    visited.add(id);
    for (const t of adj.get(id)) {
      indeg.set(t, indeg.get(t) - 1);
      if (indeg.get(t) === 0) queue.push(t);
    }
  }
  return new Set(ids.filter(id => !visited.has(id)));
}

// ── Validation issues ─────────────────────────────────────────────────────────

function computeIssues(nodes) {
  const map = new Map();
  for (const [, n] of nodes) {
    const list = [];
    if (n.kind === 'task') {
      if (!n.request?.url) list.push('Missing URL');
      if (n.mode === 'async' && !n.callback?.successWhen && !n.callback?.failureWhen)
        list.push('Async: no conditions defined');
    }
    if (n.kind === 'switch') {
      if (!n.default) list.push('No default target');
      if (n.cases.some(c => !c.when)) list.push('Case missing condition');
    }
    if (list.length) map.set(n.id, list);
  }
  return map;
}

// ── Node shapes ───────────────────────────────────────────────────────────────

function renderNode(parent, node, selected, terminal, inCycle, issues) {
  const g = svgNS('g');
  g.setAttribute('data-id', node.id);
  g.style.cursor = 'pointer';

  if (node.kind === 'task') drawTask(g, node, selected, terminal, inCycle, issues);
  else if (node.kind === 'sleep') drawSleep(g, node, selected, terminal, inCycle);
  else drawSwitch(g, node, selected, inCycle, issues);

  renderPorts(g, node);

  // Hover: slightly brighten the first shape child (rect or polygon)
  g.addEventListener('mouseenter', () => {
    const shape = g.querySelector('rect, polygon');
    if (shape && !selected) shape.style.filter = 'brightness(1.25)';
  });
  g.addEventListener('mouseleave', () => {
    const shape = g.querySelector('rect, polygon');
    if (shape) shape.style.filter = '';
  });

  parent.appendChild(g);
}

function drawTask(g, node, selected, terminal, inCycle, issues) {
  const { x, y } = node;
  const w = NODE_TASK_W, h = NODE_TASK_H;
  const hasIssues = issues.length > 0;
  const stroke = inCycle ? '#ef4444' : selected ? '#7c9ef8' : hasIssues ? '#f59e0b' : '#2d4070';
  const fill   = inCycle ? '#2d0a0a' : selected ? '#1d2d52' : '#151f38';

  const rect = sa(svgNS('rect'), {
    x, y, width: w, height: h, rx: 8, fill, stroke,
    'stroke-width': (selected || inCycle) ? 2 : hasIssues ? 1.5 : 1,
  });
  if (selected) rect.setAttribute('filter', 'url(#glow)');
  g.appendChild(rect);

  const badgeFill = node.mode === 'async' ? '#5b21b6' : '#065f46';
  g.appendChild(sa(svgNS('rect'), { x: x + w - 54, y: y + 5, width: 50, height: 16, rx: 4, fill: badgeFill }));
  const bt = sa(svgNS('text'), { x: x + w - 29, y: y + 17, 'text-anchor': 'middle', fill: '#d1fae5', 'font-size': 9, 'font-family': 'monospace' });
  bt.textContent = node.mode.toUpperCase();
  g.appendChild(bt);

  const idText = sa(svgNS('text'), {
    x: x + 10, y: y + h / 2 + 5,
    fill: selected ? '#c5d8ff' : '#b0c4e8',
    'font-size': 13, 'font-family': 'system-ui, sans-serif', 'font-weight': '600',
  });
  idText.textContent = trunc(node.id, 18);
  g.appendChild(idText);

  if (node.request?.url) {
    const urlText = sa(svgNS('text'), { x: x + 10, y: y + h - 8, fill: '#4a6080', 'font-size': 9, 'font-family': 'monospace' });
    urlText.textContent = trunc(String(node.request.url).replace(/^https?:\/\//, ''), 28);
    g.appendChild(urlText);
  }

  if (hasIssues) renderIssueBadge(g, x + w - 8, y - 6, issues.length);
  if (inCycle)   renderCycleBadge(g, x + 4, y - 6);

  if (terminal) {
    g.appendChild(sa(svgNS('rect'), { x: x + w + 4, y: y + h / 2 - 8, width: 32, height: 14, rx: 3, fill: '#14532d' }));
    const et = sa(svgNS('text'), { x: x + w + 20, y: y + h / 2 + 3, 'text-anchor': 'middle', fill: '#86efac', 'font-size': 9, 'font-family': 'monospace' });
    et.textContent = 'END';
    g.appendChild(et);
  }
}

function fmtDuration(ms) {
  if (ms >= 3_600_000) return `${(ms / 3_600_000).toFixed(1).replace(/\.0$/, '')}h`;
  if (ms >= 60_000)    return `${Math.round(ms / 60_000)}m`;
  return `${Math.round(ms / 1000)}s`;
}

function drawSleep(g, node, selected, terminal, inCycle) {
  const { x, y } = node;
  const w = NODE_SLEEP_W, h = NODE_SLEEP_H;
  const stroke = inCycle ? '#ef4444' : selected ? '#b088ff' : '#5b21b6';
  const fill   = inCycle ? '#2d0a0a' : selected ? '#2d1a50' : '#1a1040';

  const rect = sa(svgNS('rect'), {
    x, y, width: w, height: h, rx: 8, fill, stroke,
    'stroke-width': (selected || inCycle) ? 2 : 1,
  });
  if (selected) rect.setAttribute('filter', 'url(#glow)');
  g.appendChild(rect);

  // SLEEP badge (top-right)
  g.appendChild(sa(svgNS('rect'), { x: x + w - 58, y: y + 5, width: 54, height: 16, rx: 4, fill: '#4c1d95' }));
  const bt = sa(svgNS('text'), { x: x + w - 31, y: y + 17, 'text-anchor': 'middle', fill: '#ddd6fe', 'font-size': 9, 'font-family': 'monospace' });
  bt.textContent = 'SLEEP';
  g.appendChild(bt);

  // Node id
  const idText = sa(svgNS('text'), {
    x: x + 10, y: y + h / 2 + 5,
    fill: selected ? '#e0d0ff' : '#c4aaff',
    'font-size': 13, 'font-family': 'system-ui, sans-serif', 'font-weight': '600',
  });
  idText.textContent = trunc(node.id, 18);
  g.appendChild(idText);

  // Duration label (bottom-left)
  const durText = sa(svgNS('text'), { x: x + 10, y: y + h - 8, fill: '#7c3aed', 'font-size': 9, 'font-family': 'monospace' });
  durText.textContent = `⏱ ${fmtDuration(node.durationMillis ?? 0)}`;
  g.appendChild(durText);

  if (inCycle) renderCycleBadge(g, x + 4, y - 6);

  if (terminal) {
    g.appendChild(sa(svgNS('rect'), { x: x + w + 4, y: y + h / 2 - 8, width: 32, height: 14, rx: 3, fill: '#14532d' }));
    const et = sa(svgNS('text'), { x: x + w + 20, y: y + h / 2 + 3, 'text-anchor': 'middle', fill: '#86efac', 'font-size': 9, 'font-family': 'monospace' });
    et.textContent = 'END';
    g.appendChild(et);
  }
}

function drawSwitch(g, node, selected, inCycle, issues) {
  const { x, y } = node;
  const w = NODE_SWITCH_W, h = NODE_SWITCH_H;
  const cx = x + w / 2, cy = y + h / 2;
  const hasIssues = issues.length > 0;
  const stroke = inCycle ? '#ef4444' : selected ? '#b088ff' : hasIssues ? '#f59e0b' : '#6d3fd6';
  const fill   = inCycle ? '#2d0a0a' : selected ? '#2d1a50' : '#1a0d38';

  const diamond = sa(svgNS('polygon'), {
    points: `${cx},${y} ${x + w},${cy} ${cx},${y + h} ${x},${cy}`,
    fill, stroke, 'stroke-width': (selected || inCycle) ? 2 : hasIssues ? 1.5 : 1,
  });
  if (selected) diamond.setAttribute('filter', 'url(#glow)');
  g.appendChild(diamond);

  const label = sa(svgNS('text'), {
    x: cx, y: cy + 5, 'text-anchor': 'middle', 'dominant-baseline': 'middle',
    fill: selected ? '#e0d0ff' : '#c4aaff',
    'font-size': 12, 'font-family': 'system-ui, sans-serif', 'font-weight': '600',
  });
  label.textContent = trunc(node.id, 14);
  g.appendChild(label);

  if (hasIssues) renderIssueBadge(g, x + w - 8, y - 6, issues.length);
  if (inCycle)   renderCycleBadge(g, x + 4, y - 6);
}

function renderIssueBadge(g, x, y, count) {
  g.appendChild(sa(svgNS('circle'), { cx: x, cy: y, r: 8, fill: '#92400e' }));
  const t = sa(svgNS('text'), { x, y: y + 4, 'text-anchor': 'middle', fill: '#fef3c7', 'font-size': 9, 'font-weight': 'bold', 'font-family': 'monospace' });
  t.textContent = count > 1 ? `⚠${count}` : '⚠';
  g.appendChild(t);
}

function renderCycleBadge(g, x, y) {
  g.appendChild(sa(svgNS('circle'), { cx: x + 8, cy: y, r: 8, fill: '#7f1d1d' }));
  const t = sa(svgNS('text'), { x: x + 8, y: y + 4, 'text-anchor': 'middle', fill: '#fca5a5', 'font-size': 10, 'font-weight': 'bold', 'font-family': 'monospace' });
  t.textContent = '↺';
  g.appendChild(t);
}

// ── Port circles ───────────────────────────────────────────────────────────────

function renderPorts(g, node) {
  const ip = inputPort(node);
  const op = outputPort(node);
  const mkPort = (cx, cy, type) => {
    const c = sa(svgNS('circle'), { cx, cy, r: 5, fill: '#0d1117', stroke: type === 'out' ? '#3a6090' : '#253550', 'stroke-width': 1.5 });
    c.setAttribute('data-port-type', type);
    c.setAttribute('data-port-node', node.id);
    c.style.cursor = type === 'out' ? 'crosshair' : 'default';
    return c;
  };
  g.appendChild(mkPort(ip.x, ip.y, 'in'));
  g.appendChild(mkPort(op.x, op.y, 'out'));
}

// ── Start pill ────────────────────────────────────────────────────────────────

function renderStartPill(parent, entryNode) {
  const ip = inputPort(entryNode);
  const px = ip.x - 70, py = ip.y;
  const g = svgNS('g');
  g.appendChild(sa(svgNS('path'), { d: `M ${px + 25} ${py} L ${ip.x} ${ip.y}`, stroke: '#1a7a4a', 'stroke-width': 1.5, fill: 'none', 'marker-end': 'url(#arrow)' }));
  g.appendChild(sa(svgNS('rect'), { x: px - 23, y: py - 10, width: 48, height: 20, rx: 10, fill: '#14532d' }));
  const t = sa(svgNS('text'), { x: px + 1, y: py + 5, 'text-anchor': 'middle', fill: '#86efac', 'font-size': 10, 'font-family': 'monospace', 'font-weight': 'bold' });
  t.textContent = 'START';
  g.appendChild(t);
  parent.appendChild(g);
}

// ── Edges ─────────────────────────────────────────────────────────────────────

function renderEdges(parent, nodes, cycleSet) {
  for (const [, node] of nodes) {
    if ((node.kind === 'task' || node.kind === 'sleep') && node.next && nodes.has(node.next)) {
      const inCycle = cycleSet.has(node.id) && cycleSet.has(node.next);
      drawEdge(parent, outputPort(node), inputPort(nodes.get(node.next)), null, false, node.id, node.next, null, inCycle);
    } else if (node.kind === 'switch') {
      node.cases.forEach((c, i) => {
        if (c.target && nodes.has(c.target)) {
          const inCycle = cycleSet.has(node.id) && cycleSet.has(c.target);
          drawEdge(parent, switchPort(node), inputPort(nodes.get(c.target)), c.name || '', false, node.id, c.target, i, inCycle);
        }
      });
      if (node.default && nodes.has(node.default)) {
        const inCycle = cycleSet.has(node.id) && cycleSet.has(node.default);
        drawEdge(parent, switchPort(node), inputPort(nodes.get(node.default)), 'default', true, node.id, node.default, 'default', inCycle);
      }
    }
  }
}

function drawEdge(parent, src, dst, label, dashed, srcId, dstId, caseKey, inCycle) {
  const dx  = Math.abs(dst.x - src.x);
  const cp  = Math.max(50, dx * 0.55);
  const d   = `M ${src.x} ${src.y} C ${src.x + cp} ${src.y} ${dst.x - cp} ${dst.y} ${dst.x} ${dst.y}`;
  const col = inCycle ? '#ef4444' : dashed ? '#4a4060' : '#3a5080';
  const mrk = inCycle ? 'url(#arrow-err)' : 'url(#arrow)';

  parent.appendChild(sa(svgNS('path'), { d, stroke: col, 'stroke-width': 1.5, fill: 'none', 'stroke-dasharray': dashed ? '5,3' : '', 'marker-end': mrk, 'pointer-events': 'none' }));

  // Transparent wide hit-path for click-to-delete
  if (srcId != null) {
    const hit = sa(svgNS('path'), { d, stroke: 'transparent', 'stroke-width': 14, fill: 'none', cursor: 'pointer' });
    hit.setAttribute('data-edge-src', srcId);
    hit.setAttribute('data-edge-dst', String(dstId));
    hit.setAttribute('data-edge-case', caseKey != null ? String(caseKey) : '');
    parent.appendChild(hit);
  }

  if (label) {
    const mx = (src.x + dst.x) / 2, my = (src.y + dst.y) / 2;
    const tw = label.length * 5.5 + 8;
    parent.appendChild(sa(svgNS('rect'), { x: mx - tw / 2, y: my - 9, width: tw, height: 16, rx: 3, fill: '#0d1117' }));
    const lt = sa(svgNS('text'), { x: mx, y: my + 4, 'text-anchor': 'middle', fill: inCycle ? '#fca5a5' : '#6a7fa8', 'font-size': 9, 'font-family': 'monospace' });
    lt.textContent = label;
    parent.appendChild(lt);
  }
}

// ── Port positions (horizontal flow: left=in, right=out) ──────────────────────

function inputPort(node) {
  return { x: node.x, y: node.y + nh(node) / 2 };
}

function outputPort(node) {
  return { x: node.x + nw(node), y: node.y + nh(node) / 2 };
}

function switchPort(node) {
  // All switch outputs leave from the right vertex of the diamond
  return { x: node.x + NODE_SWITCH_W, y: node.y + NODE_SWITCH_H / 2 };
}

function nw(node) {
  if (node.kind === 'task')  return NODE_TASK_W;
  if (node.kind === 'sleep') return NODE_SLEEP_W;
  return NODE_SWITCH_W;
}
function nh(node) {
  if (node.kind === 'task')  return NODE_TASK_H;
  if (node.kind === 'sleep') return NODE_SLEEP_H;
  return NODE_SWITCH_H;
}

// ── Zoom helpers ──────────────────────────────────────────────────────────────

export function zoom(factor) {
  const rect = svgEl.getBoundingClientRect();
  const cx = rect.width / 2, cy = rect.height / 2;
  viewport.tx = cx - (cx - viewport.tx) * factor;
  viewport.ty = cy - (cy - viewport.ty) * factor;
  viewport.scale = Math.min(3, Math.max(0.15, viewport.scale * factor));
  applyViewport();
}

export function zoomFit(st) {
  if (!svgEl || st.nodes.size === 0) return;
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const [, n] of st.nodes) {
    minX = Math.min(minX, n.x); minY = Math.min(minY, n.y);
    maxX = Math.max(maxX, n.x + nw(n)); maxY = Math.max(maxY, n.y + nh(n));
  }
  const rect = svgEl.getBoundingClientRect();
  const pad = 80;
  const scaleX = (rect.width  - pad * 2) / (maxX - minX || 1);
  const scaleY = (rect.height - pad * 2) / (maxY - minY || 1);
  viewport.scale = Math.min(2, Math.max(0.15, Math.min(scaleX, scaleY)));
  viewport.tx = pad - minX * viewport.scale;
  viewport.ty = pad - minY * viewport.scale;
  applyViewport();
}

// ── Auto-layout ───────────────────────────────────────────────────────────────

export function autoLayout(st) {
  const nodes = st.nodes;
  if (nodes.size === 0) return;

  const ids   = [...nodes.keys()];
  const adj   = new Map(ids.map(id => [id, []]));
  const indeg = new Map(ids.map(id => [id, 0]));

  for (const [, n] of nodes) {
    for (const t of nodeTargets(n)) {
      if (nodes.has(t)) { adj.get(n.id).push(t); indeg.set(t, indeg.get(t) + 1); }
    }
  }

  const queue  = ids.filter(id => indeg.get(id) === 0);
  const sorted = [];
  while (queue.length) {
    const id = queue.shift(); sorted.push(id);
    for (const t of adj.get(id)) { indeg.set(t, indeg.get(t) - 1); if (indeg.get(t) === 0) queue.push(t); }
  }
  for (const id of ids) if (!sorted.includes(id)) sorted.push(id);

  const depth = new Map(ids.map(id => [id, 0]));
  for (const id of sorted) {
    const d = depth.get(id);
    for (const t of adj.get(id)) { if (depth.get(t) < d + 1) depth.set(t, d + 1); }
  }

  const cols = new Map();
  for (const id of sorted) {
    const col = depth.get(id) || 0;
    if (!cols.has(col)) cols.set(col, []);
    cols.get(col).push(id);
  }
  for (const [col, colIds] of cols) {
    colIds.forEach((id, row) => {
      const n = nodes.get(id);
      if (n) { n.x = col * COL_W + 80; n.y = row * ROW_H + 100; }
    });
  }
}

// ── Minimap ────────────────────────────────────────────────────────────────────

const MM_W = 180, MM_H = 110, MM_PAD = 8;
let minimapSvg;

function initMinimap() {
  const wrap = svgEl?.parentElement;
  if (!wrap) return;

  const container = document.createElement('div');
  container.id = 'minimap';

  minimapSvg = document.createElementNS('http://www.w3.org/2000/svg', 'svg');
  minimapSvg.setAttribute('width', MM_W);
  minimapSvg.setAttribute('height', MM_H);
  container.appendChild(minimapSvg);
  wrap.appendChild(container);

  minimapSvg.addEventListener('click', e => {
    const { scale, offX, offY, bounds } = mmBounds(state.getState().nodes);
    if (!bounds) return;
    const r  = minimapSvg.getBoundingClientRect();
    const cx = (e.clientX - r.left  - offX) / scale;
    const cy = (e.clientY - r.top   - offY) / scale;
    const sr = svgEl.getBoundingClientRect();
    viewport.tx = sr.width  / 2 - cx * viewport.scale;
    viewport.ty = sr.height / 2 - cy * viewport.scale;
    applyViewport();
    render(state.getState());
  });
}

function mmBounds(nodes) {
  const empty = { bounds: null, scale: 1, offX: MM_PAD, offY: MM_PAD };
  if (!nodes || nodes.size === 0) return empty;
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const [, n] of nodes) {
    minX = Math.min(minX, n.x); minY = Math.min(minY, n.y);
    maxX = Math.max(maxX, n.x + nw(n)); maxY = Math.max(maxY, n.y + nh(n));
  }
  const usW = MM_W - MM_PAD * 2, usH = MM_H - MM_PAD * 2;
  const scale = Math.min(usW / (maxX - minX || 1), usH / (maxY - minY || 1));
  const offX  = MM_PAD + (usW - (maxX - minX) * scale) / 2 - minX * scale;
  const offY  = MM_PAD + (usH - (maxY - minY) * scale) / 2 - minY * scale;
  return { bounds: { minX, minY, maxX, maxY }, scale, offX, offY };
}

function updateMinimap(st) {
  if (!minimapSvg) return;
  while (minimapSvg.firstChild) minimapSvg.removeChild(minimapSvg.firstChild);

  const { nodes } = st;
  const visible = nodes && nodes.size > 0;
  minimapSvg.parentElement.style.display = visible ? '' : 'none';
  if (!visible) return;

  const { scale, offX, offY } = mmBounds(nodes);
  const NS = 'http://www.w3.org/2000/svg';

  for (const [, n] of nodes) {
    const x = n.x * scale + offX, y = n.y * scale + offY;
    const w = nw(n) * scale,      h = nh(n) * scale;
    if (n.kind === 'task') {
      const r = document.createElementNS(NS, 'rect');
      Object.assign(r, {}); r.setAttribute('x', x); r.setAttribute('y', y);
      r.setAttribute('width', w); r.setAttribute('height', h);
      r.setAttribute('rx', 2); r.setAttribute('fill', '#1d2d52');
      r.setAttribute('stroke', '#2d4070'); r.setAttribute('stroke-width', 0.5);
      minimapSvg.appendChild(r);
    } else if (n.kind === 'sleep') {
      const r = document.createElementNS(NS, 'rect');
      r.setAttribute('x', x); r.setAttribute('y', y);
      r.setAttribute('width', w); r.setAttribute('height', h);
      r.setAttribute('rx', 2); r.setAttribute('fill', '#1a1040');
      r.setAttribute('stroke', '#5b21b6'); r.setAttribute('stroke-width', 0.5);
      minimapSvg.appendChild(r);
    } else {
      const cx = x + w / 2, cy = y + h / 2;
      const poly = document.createElementNS(NS, 'polygon');
      poly.setAttribute('points', `${cx},${y} ${x+w},${cy} ${cx},${y+h} ${x},${cy}`);
      poly.setAttribute('fill', '#1a0d38'); poly.setAttribute('stroke', '#6d3fd6');
      poly.setAttribute('stroke-width', 0.5);
      minimapSvg.appendChild(poly);
    }
  }

  if (svgEl) {
    const sr = svgEl.getBoundingClientRect();
    const vx = (-viewport.tx / viewport.scale) * scale + offX;
    const vy = (-viewport.ty / viewport.scale) * scale + offY;
    const vw = (sr.width  / viewport.scale) * scale;
    const vh = (sr.height / viewport.scale) * scale;
    const vp = document.createElementNS(NS, 'rect');
    vp.setAttribute('x', vx); vp.setAttribute('y', vy);
    vp.setAttribute('width', vw); vp.setAttribute('height', vh);
    vp.setAttribute('fill', 'rgba(74,128,232,0.08)');
    vp.setAttribute('stroke', '#4a80e8'); vp.setAttribute('stroke-width', 1);
    minimapSvg.appendChild(vp);
  }
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function nodeTargets(node) {
  if (node.kind === 'task' || node.kind === 'sleep') return node.next ? [node.next] : [];
  if (node.kind === 'switch')
    return [...node.cases.map(c => c.target), node.default].filter(Boolean);
  return [];
}

function svgNS(tag) {
  return document.createElementNS('http://www.w3.org/2000/svg', tag);
}

function sa(el, attrs) {
  for (const [k, v] of Object.entries(attrs)) el.setAttribute(k, v);
  return el;
}

function trunc(s, max) {
  if (!s) return '';
  return s.length > max ? s.slice(0, max - 1) + '…' : s;
}
