// drag.js — Palette→canvas drop, node repositioning, port→edge connection drawing

import * as state from './state.js';
import { screenToCanvas } from './graph.js';

export function init() {
  const canvas = document.getElementById('canvas');

  // ── Palette → canvas ───────────────────────────────────────────────────────
  document.querySelectorAll('.palette-item').forEach(item => {
    item.addEventListener('dragstart', e => {
      e.dataTransfer.setData('kind', item.dataset.kind);
      e.dataTransfer.effectAllowed = 'copy';
    });
  });

  canvas.addEventListener('dragover', e => {
    e.preventDefault();
    e.dataTransfer.dropEffect = 'copy';
  });

  canvas.addEventListener('drop', e => {
    e.preventDefault();
    const kind = e.dataTransfer.getData('kind');
    if (!kind) return;
    const { x, y } = screenToCanvas(e.clientX, e.clientY);
    const id = state.addNode(kind, x - 80, y - 32);
    state.selectNode(id);
  });

  // ── Mousedown: port connection (priority) OR node repositioning ────────────
  canvas.addEventListener('mousedown', e => {
    // Priority 1: output port → start connection draw
    const portEl = e.target.closest('[data-port-type="out"]');
    if (portEl) {
      e.preventDefault();
      e.stopPropagation();
      startConnection(portEl);
      return;
    }

    // Priority 2: any node group → reposition
    const g = e.target.closest('[data-id]');
    if (!g) return;

    e.preventDefault();
    e.stopPropagation();

    const id = g.dataset.id;
    state.selectNode(id);

    let last = screenToCanvas(e.clientX, e.clientY);

    const onMove = ev => {
      const cur = screenToCanvas(ev.clientX, ev.clientY);
      state.moveNode(id, cur.x - last.x, cur.y - last.y);
      last = cur;
    };

    const onUp = () => {
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
    };

    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  });
}

// ── Connection drawing ─────────────────────────────────────────────────────────

function startConnection(portEl) {
  const srcId = portEl.getAttribute('data-port-node');
  // cx/cy are already in canvas coordinates (set by graph.js renderPorts)
  const srcX = parseFloat(portEl.getAttribute('cx'));
  const srcY = parseFloat(portEl.getAttribute('cy'));

  // Append draft path to canvas-root so it lives in canvas coordinate space
  const root = document.getElementById('canvas-root');
  const NS = 'http://www.w3.org/2000/svg';
  const draft = document.createElementNS(NS, 'path');
  draft.setAttribute('stroke', '#7c9ef8');
  draft.setAttribute('stroke-width', '2');
  draft.setAttribute('stroke-dasharray', '6,3');
  draft.setAttribute('fill', 'none');
  draft.setAttribute('pointer-events', 'none');
  root.appendChild(draft);

  // Highlight source port while dragging
  portEl.setAttribute('fill', '#4a80e8');
  portEl.setAttribute('stroke', '#7c9ef8');

  const onMove = ev => {
    const cur = screenToCanvas(ev.clientX, ev.clientY);
    const dy = cur.y - srcY;
    const cp = Math.max(40, Math.abs(dy) * 0.55);
    draft.setAttribute('d',
      `M ${srcX} ${srcY} C ${srcX} ${srcY + cp} ${cur.x} ${cur.y - cp} ${cur.x} ${cur.y}`
    );
  };

  const onUp = ev => {
    window.removeEventListener('mousemove', onMove);
    window.removeEventListener('mouseup', onUp);

    draft.remove();
    portEl.setAttribute('fill', '#0d1117');
    portEl.setAttribute('stroke', '#3a6090');

    // Find node under cursor (draft has pointer-events:none so it won't intercept)
    const el = document.elementFromPoint(ev.clientX, ev.clientY);
    const targetG = el?.closest('[data-id]');
    if (!targetG) return;
    const dstId = targetG.getAttribute('data-id');
    if (!dstId || dstId === srcId) return;

    const src = state.getNodes().get(srcId);
    if (!src) return;

    if (src.kind === 'task') {
      state.updateNode(srcId, { next: dstId });
    } else if (src.kind === 'switch') {
      // Add a new case pointing at the target; user fills in the condition
      state.updateNode(srcId, {
        cases: [...src.cases, { name: '', when: null, target: dstId }],
      });
    }
  };

  window.addEventListener('mousemove', onMove);
  window.addEventListener('mouseup', onUp);
}
