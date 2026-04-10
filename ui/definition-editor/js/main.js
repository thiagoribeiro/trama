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

// ── Embeddable mount API ───────────────────────────────────────────────────────

/**
 * Mount the editor into `container` (any DOM element that contains the
 * expected ids/classes from index.html / DefinitionEditorWidget.vue).
 * Returns { load(def), getDefinition(), destroy() }.
 */
export function mount(container) {
  state.init(bus);

  const $ = id => container.querySelector('#' + id);
  const svg = $('canvas');
  graph.init(svg, bus);
  drag.init(container);

  properties.init($('props-panel'), bus);
  globalPanel.init($('global-panel'), bus);

  const kbHandler = wireKeyboard();
  wireToolbar(container);
  wireDialogs(container);
  initVSCodeBridge();

  function load(def) {
    const result = importDefinition(def);
    if (result.ok) state.replaceAll(result.snapshot);
  }

  function getDefinition() {
    return exportDefinition(state.getState());
  }

  function destroy() {
    document.removeEventListener('keydown', kbHandler);
    document.removeEventListener('click', _templateMenuCloser);
  }

  return { load, getDefinition, destroy };
}

// ── Standalone bootstrap (index.html) ─────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  // Only auto-mount in the standalone editor context (canvas element must exist).
  // When imported by the Vue management UI, this guard prevents mounting into the wrong root.
  if (!document.getElementById('canvas')) return;
  const container = document.getElementById('app');
  if (!container) return;
  mount(container);
});

// ── Toolbar ───────────────────────────────────────────────────────────────────

let _templateMenuCloser = null;

function wireToolbar(container) {
  const $ = id => container.querySelector('#' + id);

  $('btn-settings').addEventListener('click', () => globalPanel.toggle());

  $('btn-layout').addEventListener('click', () => {
    autoLayout(state.getState());
    bus.dispatchEvent(new CustomEvent('state:changed'));
  });

  $('btn-export').addEventListener('click', () => openExportDialog(container));
  $('btn-import').addEventListener('click', () => openImportDialog(container));

  $('btn-zoom-in').addEventListener('click',  () => zoom(1.25));
  $('btn-zoom-out').addEventListener('click', () => zoom(0.8));
  $('btn-zoom-fit').addEventListener('click', () => zoomFit(state.getState()));

  // Templates dropdown
  $('btn-templates').addEventListener('click', e => {
    const menu = $('templates-menu');
    menu.classList.toggle('hidden');
    e.stopPropagation();
  });
  _templateMenuCloser = () => { container.querySelector('#templates-menu')?.classList.add('hidden'); };
  document.addEventListener('click', _templateMenuCloser);
  container.querySelectorAll('[data-template]').forEach(item => {
    item.addEventListener('click', () => {
      loadTemplate(item.dataset.template);
      container.querySelector('#templates-menu').classList.add('hidden');
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
    const targets = (n.kind === 'task' || n.kind === 'sleep') ? (n.next ? [n.next] : [])
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

function openExportDialog(container) {
  const $ = id => container.querySelector('#' + id);
  const st = state.getState();
  const { errors, warnings } = validateDefinition(st);

  const issuesEl = $('export-issues');
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
  $('export-content').value = json;
  $('export-dialog').showModal();
}

// ── Import dialog ─────────────────────────────────────────────────────────────

let _pendingImport = null;
let _importContainer = null;

function openImportDialog(container) {
  _importContainer = container;
  const $ = id => container.querySelector('#' + id);
  _pendingImport = null;
  $('import-content').value = '';
  $('import-error').textContent = '';
  $('import-diff').innerHTML = '';
  $('import-apply').disabled = true;
  $('import-dialog').showModal();
}

function wireDialogs(container) {
  const $ = id => container.querySelector('#' + id);

  // Export dialog
  $('export-copy').addEventListener('click', () => {
    const area = $('export-content');
    navigator.clipboard?.writeText(area.value).catch(() => { area.select(); document.execCommand('copy'); });
    const btn = $('export-copy');
    btn.textContent = 'Copied!';
    setTimeout(() => { btn.textContent = 'Copy'; }, 1500);
  });

  $('export-download').addEventListener('click', () => {
    const area = $('export-content');
    const name = (state.getState().meta.name || 'saga') + '.json';
    const blob = new Blob([area.value], { type: 'application/json' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    a.href = url; a.download = name; a.click();
    URL.revokeObjectURL(url);
  });

  $('export-dialog').addEventListener('click', e => {
    if (e.target === e.currentTarget) e.currentTarget.close();
  });

  $('export-close')?.addEventListener('click', () => $('export-dialog').close());
  $('import-close')?.addEventListener('click', () => $('import-dialog').close());
  $('import-close-cancel')?.addEventListener('click', () => $('import-dialog').close());

  // Import dialog
  $('import-file').addEventListener('change', e => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = ev => {
      $('import-content').value = ev.target.result;
      parseAndPreviewImport(container);
    };
    reader.readAsText(file);
  });

  $('import-content').addEventListener('input', () => parseAndPreviewImport(container));

  $('import-apply').addEventListener('click', () => {
    if (!_pendingImport) return;
    state.replaceAll(_pendingImport);
    $('import-dialog').close();
  });

  $('import-dialog').addEventListener('click', e => {
    if (e.target === e.currentTarget) e.currentTarget.close();
  });
}

function parseAndPreviewImport(container) {
  const $ = id => container.querySelector('#' + id);
  const raw = $('import-content').value.trim();
  const errEl  = $('import-error');
  const diffEl = $('import-diff');
  const applyBtn = $('import-apply');

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
  const handler = e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
      e.preventDefault();
      if (window.EDITOR?.save) window.EDITOR.save();
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
  };
  document.addEventListener('keydown', handler);
  return handler;
}
