// vscode.js — VSCode webview bridge (no-op when running standalone in browser)

import { exportDefinition } from './io.js';
import { importDefinition } from './io.js';
import * as state from './state.js';

export function initVSCodeBridge() {
  let vscode = null;
  try {
    // acquireVsCodeApi() is injected by VSCode into webview contexts.
    // It throws ReferenceError in a regular browser — that's the signal
    // we're running standalone.
    vscode = acquireVsCodeApi(); // eslint-disable-line no-undef
  } catch (_) {
    return; // standalone browser — nothing to do
  }

  // Expose save so the extension host can trigger it and so Ctrl+S works
  window.EDITOR = window.EDITOR || {};
  window.EDITOR.save = () => {
    const json = exportDefinition(state.getState());
    vscode.postMessage({ type: 'save', content: JSON.stringify(json, null, 2) });
  };

  // Receive messages from the extension host
  window.addEventListener('message', ({ data }) => {
    if (!data?.type) return;

    if (data.type === 'load') {
      const result = importDefinition(data.content);
      if (result.ok) state.replaceAll(result.snapshot);
    }

    if (data.type === 'theme') {
      document.documentElement.setAttribute('data-theme', data.kind ?? 'dark');
    }
  });

  // Keyboard shortcut: Ctrl/Cmd + S → save
  document.addEventListener('keydown', e => {
    if ((e.ctrlKey || e.metaKey) && e.key === 's') {
      e.preventDefault();
      window.EDITOR.save?.();
    }
  });

  // Signal readiness — extension responds with a 'load' message
  vscode.postMessage({ type: 'ready' });
}
