// main.js — Bootstrap: event bus, module init, toolbar wiring

import * as state from './state.js';
import * as graph from './graph.js';
import * as drag from './drag.js';
import { exportDefinition, importDefinition } from './io.js';
import { autoLayout, zoomFit, zoom } from './graph.js';
import { initVSCodeBridge } from './vscode.js';
import * as globalPanel from './panels/global.js';
import * as properties from './panels/properties.js';

// ── Event bus ──────────────────────────────────────────────────────────────────

const bus = new EventTarget();

// ── Bootstrap ─────────────────────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  state.init(bus);

  const svg = document.getElementById('canvas');
  graph.init(svg, bus);
  drag.init();

  properties.init(document.getElementById('props-panel'), bus);
  globalPanel.init(document.getElementById('global-panel'), bus);

  wireToolbar();
  wireKeyboard();
  initVSCodeBridge();
});

// ── Toolbar ───────────────────────────────────────────────────────────────────

function wireToolbar() {
  document.getElementById('btn-settings').addEventListener('click', () => globalPanel.toggle());

  document.getElementById('btn-layout').addEventListener('click', () => {
    autoLayout(state.getState());
    bus.dispatchEvent(new CustomEvent('state:changed'));
  });

  document.getElementById('btn-export').addEventListener('click', openExportDialog);
  document.getElementById('btn-import').addEventListener('click', openImportDialog);

  document.getElementById('btn-zoom-in').addEventListener('click',  () => zoom(1.25));
  document.getElementById('btn-zoom-out').addEventListener('click', () => zoom(0.8));
  document.getElementById('btn-zoom-fit').addEventListener('click', () => zoomFit(state.getState()));

  // Templates dropdown
  document.getElementById('btn-templates').addEventListener('click', e => {
    const menu = document.getElementById('templates-menu');
    menu.classList.toggle('hidden');
    e.stopPropagation();
  });
  document.addEventListener('click', () => {
    document.getElementById('templates-menu')?.classList.add('hidden');
  });
  document.querySelectorAll('[data-template]').forEach(item => {
    item.addEventListener('click', () => {
      loadTemplate(item.dataset.template);
      document.getElementById('templates-menu').classList.add('hidden');
    });
  });
}

// ── Validation ────────────────────────────────────────────────────────────────

function validateDefinition(st) {
  const errors = [], warnings = [];
  const { nodes, entrypoint } = st;

  if (!entrypoint) errors.push('No entrypoint defined');
  else if (!nodes.has(entrypoint)) errors.push(`Entrypoint "${entrypoint}" does not exist`);

  // Cycle detection
  const ids   = [...nodes.keys()];
  const indeg = new Map(ids.map(id => [id, 0]));
  const adj   = new Map(ids.map(id => [id, []]));
  for (const [, n] of nodes) {
    const targets = n.kind === 'task' ? (n.next ? [n.next] : [])
      : [...n.cases.map(c => c.target), n.default].filter(Boolean);
    for (const t of targets) {
      if (nodes.has(t)) { adj.get(n.id).push(t); indeg.set(t, indeg.get(t) + 1); }
    }
  }
  const q = ids.filter(id => indeg.get(id) === 0); const vis = new Set();
  while (q.length) { const id = q.shift(); vis.add(id); for (const t of adj.get(id)) { indeg.set(t, indeg.get(t) - 1); if (indeg.get(t) === 0) q.push(t); } }
  const cycled = ids.filter(id => !vis.has(id));
  if (cycled.length) errors.push(`Cycle detected involving: ${cycled.join(', ')}`);

  for (const [, n] of nodes) {
    if (n.kind === 'task' && !n.request?.url)
      warnings.push(`"${n.id}": missing request URL`);
    if (n.kind === 'task' && n.mode === 'async' && !n.callback?.successWhen && !n.callback?.failureWhen)
      warnings.push(`"${n.id}": async node has no success/failure conditions`);
    if (n.kind === 'switch' && !n.default)
      warnings.push(`"${n.id}": switch has no default target`);
    if (n.kind === 'switch' && n.cases.some(c => !c.when))
      warnings.push(`"${n.id}": one or more cases are missing a condition`);
  }

  return { errors, warnings };
}

// ── Export dialog ─────────────────────────────────────────────────────────────

function openExportDialog() {
  const st = state.getState();
  const { errors, warnings } = validateDefinition(st);

  const issuesEl = document.getElementById('export-issues');
  issuesEl.innerHTML = '';
  issuesEl.className = '';

  if (errors.length || warnings.length) {
    if (errors.length) {
      const ul = document.createElement('ul');
      errors.forEach(e => { const li = document.createElement('li'); li.textContent = e; ul.appendChild(li); });
      issuesEl.appendChild(ul);
      issuesEl.className = 'export-issues--errors';
    }
    if (warnings.length) {
      const ul = document.createElement('ul');
      warnings.forEach(w => { const li = document.createElement('li'); li.textContent = w; ul.appendChild(li); });
      issuesEl.appendChild(ul);
      if (!errors.length) issuesEl.className = 'export-issues--warnings';
    }
  }

  const json = JSON.stringify(exportDefinition(st), null, 2);
  document.getElementById('export-content').value = json;
  document.getElementById('export-dialog').showModal();
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('export-copy').addEventListener('click', () => {
    const area = document.getElementById('export-content');
    navigator.clipboard?.writeText(area.value).catch(() => { area.select(); document.execCommand('copy'); });
    const btn = document.getElementById('export-copy');
    btn.textContent = 'Copied!';
    setTimeout(() => { btn.textContent = 'Copy'; }, 1500);
  });

  document.getElementById('export-download').addEventListener('click', () => {
    const area = document.getElementById('export-content');
    const name = (state.getState().meta.name || 'saga') + '.json';
    const blob = new Blob([area.value], { type: 'application/json' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url; a.download = name; a.click();
    URL.revokeObjectURL(url);
  });

  document.getElementById('export-dialog').addEventListener('click', e => {
    if (e.target === e.currentTarget) e.currentTarget.close();
  });
});

// ── Import dialog ─────────────────────────────────────────────────────────────

let _pendingImport = null;

function openImportDialog() {
  _pendingImport = null;
  document.getElementById('import-content').value = '';
  document.getElementById('import-error').textContent = '';
  document.getElementById('import-diff').innerHTML = '';
  document.getElementById('import-apply').disabled = true;
  document.getElementById('import-dialog').showModal();
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('import-file').addEventListener('change', e => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = ev => {
      document.getElementById('import-content').value = ev.target.result;
      parseAndPreviewImport();
    };
    reader.readAsText(file);
  });

  document.getElementById('import-content').addEventListener('input', parseAndPreviewImport);

  document.getElementById('import-apply').addEventListener('click', () => {
    if (!_pendingImport) return;
    state.replaceAll(_pendingImport);
    document.getElementById('import-dialog').close();
  });

  document.getElementById('import-dialog').addEventListener('click', e => {
    if (e.target === e.currentTarget) e.currentTarget.close();
  });
});

function parseAndPreviewImport() {
  const raw = document.getElementById('import-content').value.trim();
  const errEl  = document.getElementById('import-error');
  const diffEl = document.getElementById('import-diff');
  const applyBtn = document.getElementById('import-apply');

  errEl.textContent = '';
  diffEl.innerHTML  = '';
  applyBtn.disabled = true;
  _pendingImport    = null;

  if (!raw) return;

  const result = importDefinition(raw);
  if (!result.ok) {
    errEl.textContent = result.errors.join('\n');
    return;
  }

  _pendingImport = result.snapshot;

  // Diff summary
  const currentIds = new Set(state.getNodes().keys());
  const incomingIds = new Set(result.snapshot.nodes.map(n => n.id));
  const added   = [...incomingIds].filter(id => !currentIds.has(id));
  const removed = [...currentIds].filter(id => !incomingIds.has(id));
  const kept    = [...incomingIds].filter(id => currentIds.has(id));

  const lines = [];
  if (kept.length)    lines.push(`<span class="diff-kept">  ${kept.length} node(s) kept: ${kept.join(', ')}</span>`);
  if (added.length)   lines.push(`<span class="diff-added">+ ${added.length} added: ${added.join(', ')}</span>`);
  if (removed.length) lines.push(`<span class="diff-removed">− ${removed.length} removed: ${removed.join(', ')}</span>`);
  if (!lines.length)  lines.push(`<span class="diff-kept">  No changes to node set</span>`);

  diffEl.innerHTML = lines.join('<br>');
  applyBtn.disabled = false;
}

// ── Templates ─────────────────────────────────────────────────────────────────

const TEMPLATES = {
  'simple-chain': {
    name: 'simple-chain', version: 'v1',
    failureHandling: { type: 'retry', maxAttempts: 2, delayMillis: 500 },
    entrypoint: 'step-1',
    nodes: [
      { kind: 'task', id: 'step-1', action: { mode: 'sync', request: { url: '', verb: 'POST' } }, next: 'step-2' },
      { kind: 'task', id: 'step-2', action: { mode: 'sync', request: { url: '', verb: 'POST' } }, next: 'step-3' },
      { kind: 'task', id: 'step-3', action: { mode: 'sync', request: { url: '', verb: 'POST' } } },
    ],
  },

  'payment-split': {
    name: 'payment-saga', version: 'v1',
    failureHandling: { type: 'retry', maxAttempts: 3, delayMillis: 1000 },
    entrypoint: 'validate',
    nodes: [
      { kind: 'task',   id: 'validate', action: { mode: 'sync', request: { url: '', verb: 'POST' } }, next: 'choose-method' },
      { kind: 'switch', id: 'choose-method',
        cases: [
          { name: 'pix',  when: { '==': [{ var: 'payload.paymentMethod' }, 'pix']  }, target: 'pix-payment' },
          { name: 'card', when: { '==': [{ var: 'payload.paymentMethod' }, 'card'] }, target: 'card-payment' },
        ],
        default: 'pix-payment',
      },
      { kind: 'task', id: 'pix-payment',  action: { mode: 'sync',  request: { url: '', verb: 'POST' } }, next: 'notify' },
      { kind: 'task', id: 'card-payment', action: { mode: 'async', request: { url: '', verb: 'POST' },
        acceptedStatusCodes: [202],
        callback: { timeoutMillis: 60000, successWhen: null, failureWhen: null } }, next: 'notify' },
      { kind: 'task', id: 'notify', action: { mode: 'sync', request: { url: '', verb: 'POST' } } },
    ],
  },

  'async-callback': {
    name: 'async-saga', version: 'v1',
    failureHandling: { type: 'backoff', maxAttempts: 3, initialDelayMillis: 500, maxDelayMillis: 10000, multiplier: 2.0, jitterRatio: 0.1 },
    entrypoint: 'trigger',
    nodes: [
      { kind: 'task', id: 'trigger', action: {
        mode: 'async', request: { url: '', verb: 'POST' },
        acceptedStatusCodes: [202],
        callback: {
          timeoutMillis: 120000,
          successWhen: { '==': [{ var: 'payload.status' }, 'completed'] },
          failureWhen: { '==': [{ var: 'payload.status' }, 'failed'] },
        }
      }, next: 'notify' },
      { kind: 'task', id: 'notify', action: { mode: 'sync', request: { url: '', verb: 'POST' } } },
    ],
  },
};

function loadTemplate(name) {
  const tpl = TEMPLATES[name];
  if (!tpl) return;
  const result = importDefinition(tpl);
  if (result.ok) state.replaceAll(result.snapshot);
}

// ── Keyboard shortcuts ─────────────────────────────────────────────────────────

function wireKeyboard() {
  document.addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
      e.preventDefault();
      if (window.EDITOR?.save) window.EDITOR.save();
      else openExportDialog();
      return;
    }
    if (e.key === 'Escape') {
      globalPanel.hide();
      state.selectNode(null);
      return;
    }
    if ((e.key === 'Delete' || e.key === 'Backspace') &&
        !e.target.matches('input, textarea, select, [contenteditable]')) {
      const sel = state.getState().selectedNodeId;
      if (sel) state.deleteNode(sel);
    }
  });
}
