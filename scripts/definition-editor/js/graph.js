// graph.js — SVG canvas: rendering, zoom/pan, auto-layout

import * as state from './state.js';

const NODE_TASK_W = 190;
const NODE_TASK_H = 64;
const NODE_SWITCH_W = 160;
const NODE_SWITCH_H = 84;
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
}

function applyViewport() {
  rootGroup.setAttribute('transform',
    `translate(${viewport.tx}, ${viewport.ty}) scale(${viewport.scale})`);
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

  // Edges behind nodes
  const edgeGroup = svgNS('g');
  renderEdges(edgeGroup, nodes);
  rootGroup.appendChild(edgeGroup);

  // START pill
  if (entrypoint && nodes.has(entrypoint)) {
    renderStartPill(rootGroup, nodes.get(entrypoint));
  }

  // Nodes
  for (const [, node] of nodes) {
    renderNode(rootGroup, node, node.id === selectedNodeId, terminalSet.has(node.id));
  }
}

function computeTerminals(nodes) {
  const referenced = new Set();
  for (const [, n] of nodes) {
    if (n.kind === 'task' && n.next) referenced.add(n.next);
    if (n.kind === 'switch') {
      n.cases.forEach(c => c.target && referenced.add(c.target));
      if (n.default) referenced.add(n.default);
    }
  }
  const terminals = new Set();
  for (const [, n] of nodes) {
    if (n.kind === 'task' && !n.next) terminals.add(n.id);
  }
  return terminals;
}

// ── Node shapes ───────────────────────────────────────────────────────────────

function renderNode(parent, node, selected, terminal) {
  const g = svgNS('g');
  g.setAttribute('data-id', node.id);
  g.style.cursor = 'pointer';

  if (node.kind === 'task') drawTask(g, node, selected, terminal);
  else drawSwitch(g, node, selected);

  renderPorts(g, node);
  parent.appendChild(g);
}

function renderPorts(g, node) {
  const ip = inputPort(node);
  const op = outputPort(node);

  const mkPort = (cx, cy, type) => {
    const c = sa(svgNS('circle'), {
      cx, cy, r: 5,
      fill: '#0d1117',
      stroke: type === 'out' ? '#3a6090' : '#253550',
      'stroke-width': 1.5,
    });
    c.setAttribute('data-port-type', type);
    c.setAttribute('data-port-node', node.id);
    c.style.cursor = type === 'out' ? 'crosshair' : 'default';
    return c;
  };

  g.appendChild(mkPort(ip.x, ip.y, 'in'));
  g.appendChild(mkPort(op.x, op.y, 'out'));
}

function drawTask(g, node, selected, terminal) {
  const { x, y } = node;
  const w = NODE_TASK_W, h = NODE_TASK_H;

  const rect = sa(svgNS('rect'), {
    x, y, width: w, height: h, rx: 8,
    fill: selected ? '#1d2d52' : '#151f38',
    stroke: selected ? '#7c9ef8' : '#2d4070',
    'stroke-width': selected ? 2 : 1,
  });
  if (selected) rect.setAttribute('filter', 'url(#glow)');
  g.appendChild(rect);

  // Mode badge
  const badgeFill = node.mode === 'async' ? '#5b21b6' : '#065f46';
  const badge = sa(svgNS('rect'), { x: x + w - 54, y: y + 5, width: 50, height: 16, rx: 4, fill: badgeFill });
  const badgeText = sa(svgNS('text'), {
    x: x + w - 29, y: y + 17,
    'text-anchor': 'middle', fill: '#d1fae5', 'font-size': 9, 'font-family': 'monospace',
  });
  badgeText.textContent = node.mode.toUpperCase();
  g.appendChild(badge); g.appendChild(badgeText);

  // ID
  const idText = sa(svgNS('text'), {
    x: x + 10, y: y + h / 2 + 5,
    fill: selected ? '#c5d8ff' : '#b0c4e8',
    'font-size': 13, 'font-family': 'system-ui, sans-serif', 'font-weight': '600',
  });
  idText.textContent = trunc(node.id, 18);
  g.appendChild(idText);

  // URL hint
  if (node.request?.url) {
    const urlText = sa(svgNS('text'), {
      x: x + 10, y: y + h - 8,
      fill: '#4a6080', 'font-size': 9, 'font-family': 'monospace',
    });
    urlText.textContent = trunc(node.request.url.replace(/^https?:\/\//, ''), 30);
    g.appendChild(urlText);
  }

  // END badge
  if (terminal) {
    const eb = sa(svgNS('rect'), { x: x + w / 2 - 18, y: y + h + 5, width: 36, height: 14, rx: 3, fill: '#14532d' });
    const et = sa(svgNS('text'), {
      x: x + w / 2, y: y + h + 16,
      'text-anchor': 'middle', fill: '#86efac', 'font-size': 9, 'font-family': 'monospace',
    });
    et.textContent = 'END';
    g.appendChild(eb); g.appendChild(et);
  }
}

function drawSwitch(g, node, selected) {
  const { x, y } = node;
  const w = NODE_SWITCH_W, h = NODE_SWITCH_H;
  const cx = x + w / 2, cy = y + h / 2;

  const diamond = sa(svgNS('polygon'), {
    points: `${cx},${y} ${x + w},${cy} ${cx},${y + h} ${x},${cy}`,
    fill: selected ? '#2d1a50' : '#1a0d38',
    stroke: selected ? '#b088ff' : '#6d3fd6',
    'stroke-width': selected ? 2 : 1,
  });
  if (selected) diamond.setAttribute('filter', 'url(#glow)');
  g.appendChild(diamond);

  const label = sa(svgNS('text'), {
    x: cx, y: cy + 5,
    'text-anchor': 'middle', 'dominant-baseline': 'middle',
    fill: selected ? '#e0d0ff' : '#c4aaff',
    'font-size': 12, 'font-family': 'system-ui, sans-serif', 'font-weight': '600',
  });
  label.textContent = trunc(node.id, 14);
  g.appendChild(label);
}

// ── Start pill ────────────────────────────────────────────────────────────────

function renderStartPill(parent, entryNode) {
  const ip = inputPort(entryNode);
  const px = ip.x, py = ip.y - 55;

  const g = svgNS('g');

  const path = sa(svgNS('path'), {
    d: `M ${px} ${py + 12} C ${px} ${py + 35} ${ip.x} ${ip.y - 20} ${ip.x} ${ip.y}`,
    stroke: '#1a7a4a', 'stroke-width': 1.5, fill: 'none', 'marker-end': 'url(#arrow)',
  });
  const pill = sa(svgNS('rect'), { x: px - 24, y: py - 10, width: 48, height: 20, rx: 10, fill: '#14532d' });
  const text = sa(svgNS('text'), {
    x: px, y: py + 5,
    'text-anchor': 'middle', fill: '#86efac',
    'font-size': 10, 'font-family': 'monospace', 'font-weight': 'bold',
  });
  text.textContent = 'START';

  g.appendChild(path); g.appendChild(pill); g.appendChild(text);
  parent.appendChild(g);
}

// ── Edges ─────────────────────────────────────────────────────────────────────

function renderEdges(parent, nodes) {
  for (const [, node] of nodes) {
    if (node.kind === 'task' && node.next && nodes.has(node.next)) {
      drawEdge(parent, outputPort(node), inputPort(nodes.get(node.next)), null, false);
    } else if (node.kind === 'switch') {
      const total = node.cases.length + (node.default ? 1 : 0);
      let i = 0;
      for (const c of node.cases) {
        if (c.target && nodes.has(c.target)) {
          drawEdge(parent, switchPort(node, i, total), inputPort(nodes.get(c.target)), c.name || '', false);
        }
        i++;
      }
      if (node.default && nodes.has(node.default)) {
        drawEdge(parent, switchPort(node, i, total), inputPort(nodes.get(node.default)), 'default', true);
      }
    }
  }
}

function drawEdge(parent, src, dst, label, dashed) {
  const dy = Math.abs(dst.y - src.y);
  const cp = Math.max(50, dy * 0.55);

  const path = sa(svgNS('path'), {
    d: `M ${src.x} ${src.y} C ${src.x} ${src.y + cp} ${dst.x} ${dst.y - cp} ${dst.x} ${dst.y}`,
    stroke: dashed ? '#4a4060' : '#3a5080',
    'stroke-width': 1.5, fill: 'none',
    'stroke-dasharray': dashed ? '5,3' : '',
    'marker-end': 'url(#arrow)',
  });
  parent.appendChild(path);

  if (label) {
    const mx = (src.x + dst.x) / 2;
    const my = (src.y + dst.y) / 2;
    const pad = 4;
    const tw = label.length * 5.5 + pad * 2;
    const bg = sa(svgNS('rect'), { x: mx - tw / 2, y: my - 9, width: tw, height: 16, rx: 3, fill: '#0d1117' });
    const txt = sa(svgNS('text'), {
      x: mx, y: my + 4,
      'text-anchor': 'middle', fill: '#6a7fa8',
      'font-size': 9, 'font-family': 'monospace',
    });
    txt.textContent = label;
    parent.appendChild(bg); parent.appendChild(txt);
  }
}

// ── Port positions ─────────────────────────────────────────────────────────────

function inputPort(node) {
  return { x: node.x + nw(node) / 2, y: node.y };
}

function outputPort(node) {
  return { x: node.x + nw(node) / 2, y: node.y + nh(node) };
}

function switchPort(node, idx, total) {
  const cx = node.x + NODE_SWITCH_W / 2;
  const bottom = node.y + NODE_SWITCH_H;
  if (total <= 1) return { x: cx, y: bottom };
  const span = Math.min(NODE_SWITCH_W * 0.65, (total - 1) * 44);
  const step = span / (total - 1);
  return { x: cx - span / 2 + idx * step, y: bottom };
}

function nw(node) { return node.kind === 'task' ? NODE_TASK_W : NODE_SWITCH_W; }
function nh(node) { return node.kind === 'task' ? NODE_TASK_H : NODE_SWITCH_H; }

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
    const w = nw(n), h = nh(n);
    minX = Math.min(minX, n.x); minY = Math.min(minY, n.y);
    maxX = Math.max(maxX, n.x + w); maxY = Math.max(maxY, n.y + h);
  }
  const rect = svgEl.getBoundingClientRect();
  const pad = 60;
  const scaleX = (rect.width - pad * 2) / (maxX - minX || 1);
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

  const ids = [...nodes.keys()];
  const adj = new Map(ids.map(id => [id, []]));
  const indeg = new Map(ids.map(id => [id, 0]));

  for (const [, n] of nodes) {
    for (const t of targets(n)) {
      if (nodes.has(t)) {
        adj.get(n.id).push(t);
        indeg.set(t, indeg.get(t) + 1);
      }
    }
  }

  // Kahn topological sort
  const queue = ids.filter(id => indeg.get(id) === 0);
  const sorted = [];
  while (queue.length) {
    const id = queue.shift();
    sorted.push(id);
    for (const t of adj.get(id)) {
      indeg.set(t, indeg.get(t) - 1);
      if (indeg.get(t) === 0) queue.push(t);
    }
  }
  for (const id of ids) if (!sorted.includes(id)) sorted.push(id);

  // Assign column = max depth from entrypoint
  const depth = new Map(ids.map(id => [id, 0]));
  for (const id of sorted) {
    const d = depth.get(id);
    for (const t of adj.get(id)) {
      if (depth.get(t) < d + 1) depth.set(t, d + 1);
    }
  }

  // Group into columns
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

function targets(node) {
  if (node.kind === 'task') return node.next ? [node.next] : [];
  if (node.kind === 'switch') {
    return [...node.cases.map(c => c.target), node.default].filter(Boolean);
  }
  return [];
}

// ── Helpers ────────────────────────────────────────────────────────────────────

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
