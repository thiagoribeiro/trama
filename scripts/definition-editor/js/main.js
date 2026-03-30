// main.js — Bootstrap: event bus, module init, toolbar wiring

import * as state from './state.js';
import * as graph from './graph.js';
import * as drag from './drag.js';
import { exportDefinition, importDefinition } from './io.js';
import { autoLayout } from './graph.js';
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
  wireImportExport();
  wireKeyboard();
  initVSCodeBridge();
});

// ── Toolbar ───────────────────────────────────────────────────────────────────

function wireToolbar() {
  document.getElementById('btn-settings').addEventListener('click', () => {
    globalPanel.toggle();
  });

  document.getElementById('btn-layout').addEventListener('click', () => {
    autoLayout(state.getState());
    bus.dispatchEvent(new CustomEvent('state:changed'));
  });

  document.getElementById('btn-export').addEventListener('click', openExportDialog);
  document.getElementById('btn-import').addEventListener('click', openImportDialog);

  document.getElementById('btn-zoom-in').addEventListener('click', () => {
    graph.zoom(1.25);
  });

  document.getElementById('btn-zoom-out').addEventListener('click', () => {
    graph.zoom(0.8);
  });

  document.getElementById('btn-zoom-fit').addEventListener('click', () => {
    graph.zoomFit(state.getState());
  });
}

// ── Export dialog ─────────────────────────────────────────────────────────────

function openExportDialog() {
  const json = JSON.stringify(exportDefinition(state.getState()), null, 2);
  const dialog = document.getElementById('export-dialog');
  const area = document.getElementById('export-content');
  area.value = json;
  dialog.showModal();
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('export-copy').addEventListener('click', () => {
    const area = document.getElementById('export-content');
    navigator.clipboard?.writeText(area.value).catch(() => {
      area.select(); document.execCommand('copy');
    });
    const btn = document.getElementById('export-copy');
    btn.textContent = 'Copied!';
    setTimeout(() => { btn.textContent = 'Copy'; }, 1500);
  });

  document.getElementById('export-download').addEventListener('click', () => {
    const area = document.getElementById('export-content');
    const name = (state.getState().meta.name || 'saga') + '.json';
    const blob = new Blob([area.value], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url; a.download = name; a.click();
    URL.revokeObjectURL(url);
  });

  document.getElementById('export-dialog').addEventListener('click', e => {
    if (e.target === e.currentTarget) e.currentTarget.close();
  });
});

// ── Import dialog ─────────────────────────────────────────────────────────────

function openImportDialog() {
  const dialog = document.getElementById('import-dialog');
  document.getElementById('import-content').value = '';
  document.getElementById('import-error').textContent = '';
  dialog.showModal();
}

document.addEventListener('DOMContentLoaded', () => {
  document.getElementById('import-file').addEventListener('change', e => {
    const file = e.target.files[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = ev => { document.getElementById('import-content').value = ev.target.result; };
    reader.readAsText(file);
  });

  document.getElementById('import-apply').addEventListener('click', () => {
    const raw = document.getElementById('import-content').value.trim();
    if (!raw) return;
    const result = importDefinition(raw);
    if (!result.ok) {
      document.getElementById('import-error').textContent = result.errors.join('\n');
      return;
    }
    state.replaceAll(result.snapshot);
    document.getElementById('import-dialog').close();
  });

  document.getElementById('import-dialog').addEventListener('click', e => {
    if (e.target === e.currentTarget) e.currentTarget.close();
  });
});

// ── Keyboard shortcuts ─────────────────────────────────────────────────────────

function wireKeyboard() {
  document.addEventListener('keydown', e => {
    // Ctrl/Cmd+S → export (delegates to vscode bridge if in webview)
    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
      e.preventDefault();
      if (window.EDITOR?.save) window.EDITOR.save();
      else openExportDialog();
      return;
    }

    // Escape → close panels / deselect
    if (e.key === 'Escape') {
      globalPanel.hide();
      state.selectNode(null);
      return;
    }

    // Delete / Backspace → delete selected node (only if not in an input)
    if ((e.key === 'Delete' || e.key === 'Backspace') &&
        !e.target.matches('input, textarea, select, [contenteditable]')) {
      const sel = state.getState().selectedNodeId;
      if (sel) state.deleteNode(sel);
    }
  });
}
