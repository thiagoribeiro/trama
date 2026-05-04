// drag.js — Palette→canvas drop, node repositioning, port→edge connection, edge deletion

import * as state from './state.js';
import { screenToCanvas } from './graph.js';

export function init(container) {
  const canvas = container.querySelector('#canvas');

  // ── Palette → canvas ───────────────────────────────────────────────────────
  container.querySelectorAll('.palette-item').forEach(item => {
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

  // ── Click on edge hit-path → delete connection ─────────────────────────────
  canvas.addEventListener('click', e => {
    const hit = e.target.closest('[data-edge-src]');
    if (!hit) return;
    const srcId   = hit.getAttribute('data-edge-src');
    const caseKey = hit.getAttribute('data-edge-case');
    const src = state.getNodes().get(srcId);
    if (!src) return;

    if (src.kind === 'task' || src.kind === 'sleep') {
      state.updateNode(srcId, { next: null });
    } else if (src.kind === 'switch') {
      if (caseKey === 'default') {
        state.updateNode(srcId, { default: null });
      } else {
        const idx = parseInt(caseKey);
        state.updateNode(srcId, { cases: src.cases.filter((_, i) => i !== idx) });
      }
    }
  });

  // ── Mousedown: output port (connection) OR node body (reposition) ──────────
  canvas.addEventListener('mousedown', e => {
    // Priority 1: output port → draw connection
    const portEl = e.target.closest('[data-port-type="out"]');
    if (portEl) {
      e.preventDefault();
      e.stopPropagation();
      startConnection(portEl, container);
      return;
    }

    // Priority 2: node group → reposition + select
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

function startConnection(portEl, container) {
  const srcId = portEl.getAttribute('data-port-node');
  const srcX  = parseFloat(portEl.getAttribute('cx'));
  const srcY  = parseFloat(portEl.getAttribute('cy'));

  const root  = container.querySelector('#canvas-root');
  const NS    = 'http://www.w3.org/2000/svg';
  const draft = document.createElementNS(NS, 'path');
  draft.setAttribute('stroke', '#7c9ef8');
  draft.setAttribute('stroke-width', '2');
  draft.setAttribute('stroke-dasharray', '6,3');
  draft.setAttribute('fill', 'none');
  draft.setAttribute('pointer-events', 'none');
  root.appendChild(draft);

  portEl.setAttribute('fill', '#4a80e8');
  portEl.setAttribute('stroke', '#7c9ef8');

  const onMove = ev => {
    const cur = screenToCanvas(ev.clientX, ev.clientY);
    const cp  = Math.max(40, Math.abs(cur.x - srcX) * 0.55);
    draft.setAttribute('d',
      `M ${srcX} ${srcY} C ${srcX + cp} ${srcY} ${cur.x - cp} ${cur.y} ${cur.x} ${cur.y}`
    );
  };

  const onUp = ev => {
    window.removeEventListener('mousemove', onMove);
    window.removeEventListener('mouseup', onUp);
    draft.remove();
    portEl.setAttribute('fill', '#0d1117');
    portEl.setAttribute('stroke', '#3a6090');

    const el = document.elementFromPoint(ev.clientX, ev.clientY);
    const targetG = el?.closest('[data-id]');
    if (!targetG) return;
    const dstId = targetG.getAttribute('data-id');
    if (!dstId || dstId === srcId) return;

    const src = state.getNodes().get(srcId);
    if (!src) return;

    if (src.kind === 'task' || src.kind === 'sleep') {
      state.updateNode(srcId, { next: dstId });
    } else if (src.kind === 'switch') {
      state.updateNode(srcId, {
        cases: [...src.cases, { name: '', when: null, target: dstId }],
      });
    }
  };

  window.addEventListener('mousemove', onMove);
  window.addEventListener('mouseup', onUp);
}
