// panels/properties.js — Node property forms (Task + Switch)

import * as state from '../state.js';
import { createJsonLogicBuilder } from './jsonlogic.js';

let _panel, _bus;

export function init(panelEl, bus) {
  _panel = panelEl;
  _bus = bus;

  bus.addEventListener('node:selected', e => {
    const id = e.detail?.id;
    if (!id) { _panel.classList.add('hidden'); return; }
    const node = state.getNodes().get(id);
    if (!node) return;
    _panel.classList.remove('hidden');
    draw(node);
  });

  // Re-render on state change only if no input is focused (avoid clobbering user typing)
  bus.addEventListener('state:changed', () => {
    const sel = state.getState().selectedNodeId;
    if (!sel) return;
    const active = document.activeElement;
    const typingInPanel = active &&
      active.matches('input, textarea, select') &&
      active.closest('#props-panel');
    if (typingInPanel) return;
    const node = state.getNodes().get(sel);
    if (node) draw(node);
  });
}

function draw(node) {
  _panel.innerHTML = '';

  const header = d('div', 'props-header');
  const badge = d('span', `props-badge props-badge--${node.kind}`);
  badge.textContent = node.kind.toUpperCase();
  header.appendChild(badge);

  // Editable ID — rename propagates to all references
  const idInput = d('input', 'props-id-input');
  idInput.value = node.id;
  idInput.title = 'Click to rename node';
  idInput.addEventListener('change', () => {
    const ok = state.renameNode(node.id, idInput.value);
    if (!ok) {
      idInput.value = node.id; // revert — name already taken
      idInput.classList.add('props-id-input--error');
      setTimeout(() => idInput.classList.remove('props-id-input--error'), 1200);
    }
  });
  header.appendChild(idInput);
  _panel.appendChild(header);

  if (node.kind === 'task') drawTask(node);
  else drawSwitch(node);
}

// ── Task form ─────────────────────────────────────────────────────────────────

function drawTask(node) {
  // Entrypoint toggle
  const isEntry = state.getState().entrypoint === node.id;
  const entryBtn = d('button', 'btn btn--sm' + (isEntry ? ' btn--active' : ''));
  entryBtn.textContent = isEntry ? '★ Entrypoint' : '☆ Set as Entrypoint';
  entryBtn.onclick = () => state.setEntrypoint(node.id);
  _panel.appendChild(entryBtn);

  // Mode
  field(_panel, 'Mode', select(['sync', 'async'], node.mode, v => {
    state.updateNode(node.id, { mode: v });
  }));

  // Request section
  section(_panel, 'Request', true, sec => {
    field(sec, 'URL', tplInput(node.request?.url ?? '', v =>
      state.updateNode(node.id, { request: { url: v } })));
    field(sec, 'Verb', select(
      ['GET', 'POST', 'PUT', 'PATCH', 'DELETE', 'HEAD'],
      node.request?.verb ?? 'POST',
      v => state.updateNode(node.id, { request: { verb: v } })));
    field(sec, 'Headers', headersEditor(node.request?.headers ?? [], h =>
      state.updateNode(node.id, { request: { headers: h } })));
    field(sec, 'Body', tplTextarea(
      typeof node.request?.body === 'string' ? node.request.body
        : (node.request?.body ? JSON.stringify(node.request.body, null, 2) : ''),
      v => state.updateNode(node.id, { request: { body: v } })));
  });

  // Next node
  field(_panel, 'Next node', nodeSelect(node.next, node.id, v =>
    state.updateNode(node.id, { next: v })));

  // Async fields
  if (node.mode === 'async') {
    section(_panel, 'Async / Callback', true, sec => {
      field(sec, 'Accepted status codes', textInput(
        (node.acceptedStatusCodes ?? []).join(', '),
        v => state.updateNode(node.id, {
          acceptedStatusCodes: v.split(',').map(s => parseInt(s)).filter(n => !isNaN(n))
        }), 'e.g. 202'));

      field(sec, 'Callback timeout (ms)', textInput(
        String(node.callback?.timeoutMillis ?? 60000),
        v => state.updateNode(node.id, { callback: { timeoutMillis: parseInt(v) || 60000 } })));

      const nodeIds = [...state.getNodes().keys()].filter(id => id !== node.id);
      const schema  = state.getState().meta.payloadSchema ?? [];

      label(sec, 'Success when (json-logic)');
      const successDiv = d('div', 'jlb-wrap');
      sec.appendChild(successDiv);
      createJsonLogicBuilder(successDiv, node.callback?.successWhen, expr =>
        state.updateNode(node.id, { callback: { successWhen: expr } }), nodeIds, schema);

      label(sec, 'Failure when (json-logic)');
      const failureDiv = d('div', 'jlb-wrap');
      sec.appendChild(failureDiv);
      createJsonLogicBuilder(failureDiv, node.callback?.failureWhen, expr =>
        state.updateNode(node.id, { callback: { failureWhen: expr } }), nodeIds, schema);
    });
  }

  // Compensation
  section(_panel, 'Compensation (undo)', false, sec => {
    const toggle = checkbox('Enable compensation', !!node.compensation, enabled => {
      state.updateNode(node.id, {
        compensation: enabled ? { url: '', verb: 'POST', headers: [], body: '' } : null
      });
    });
    sec.appendChild(toggle);

    if (node.compensation) {
      field(sec, 'URL', tplInput(node.compensation.url ?? '', v =>
        state.updateNode(node.id, { compensation: { url: v } })));
      field(sec, 'Verb', select(['POST', 'PUT', 'DELETE', 'GET'], node.compensation.verb ?? 'POST', v =>
        state.updateNode(node.id, { compensation: { verb: v } })));
      field(sec, 'Body', tplTextarea(
        typeof node.compensation.body === 'string' ? node.compensation.body : '',
        v => state.updateNode(node.id, { compensation: { body: v } })));
    }
  });

  _panel.appendChild(deleteBtn(node));
}

// ── Switch form ───────────────────────────────────────────────────────────────

function drawSwitch(node) {
  const isEntry = state.getState().entrypoint === node.id;
  const entryBtn = d('button', 'btn btn--sm' + (isEntry ? ' btn--active' : ''));
  entryBtn.textContent = isEntry ? '★ Entrypoint' : '☆ Set as Entrypoint';
  entryBtn.onclick = () => state.setEntrypoint(node.id);
  _panel.appendChild(entryBtn);

  const nodeIds = [...state.getNodes().keys()].filter(id => id !== node.id);
  const schema  = state.getState().meta.payloadSchema ?? [];

  label(_panel, 'Cases');

  node.cases.forEach((c, i) => {
    const block = d('div', 'case-block');

    const caseHead = d('div', 'case-head');
    const caseLabel = d('span', '');
    caseLabel.textContent = `Case ${i + 1}`;
    const rmBtn = d('button', 'btn btn--icon');
    rmBtn.textContent = '×';
    rmBtn.title = 'Remove case';
    rmBtn.onclick = () => {
      const cases = node.cases.filter((_, j) => j !== i);
      state.updateNode(node.id, { cases });
    };
    caseHead.appendChild(caseLabel);
    caseHead.appendChild(rmBtn);
    block.appendChild(caseHead);

    field(block, 'Name', textInput(c.name ?? '', v => {
      const cases = [...node.cases];
      cases[i] = { ...cases[i], name: v };
      state.updateNode(node.id, { cases });
    }, 'optional label'));

    label(block, 'Condition (when)');
    const whenDiv = d('div', 'jlb-wrap');
    block.appendChild(whenDiv);
    createJsonLogicBuilder(whenDiv, c.when, expr => {
      const cases = [...node.cases];
      cases[i] = { ...cases[i], when: expr };
      state.updateNode(node.id, { cases });
    }, nodeIds, schema);

    field(block, 'Target node', nodeSelect(c.target, node.id, v => {
      const cases = [...node.cases];
      cases[i] = { ...cases[i], target: v };
      state.updateNode(node.id, { cases });
    }));

    _panel.appendChild(block);
  });

  const addCaseBtn = d('button', 'btn btn--sm');
  addCaseBtn.textContent = '+ Add case';
  addCaseBtn.onclick = () => {
    state.updateNode(node.id, { cases: [...node.cases, { name: '', when: null, target: null }] });
  };
  _panel.appendChild(addCaseBtn);

  field(_panel, 'Default target', nodeSelect(node.default, node.id, v =>
    state.updateNode(node.id, { default: v })));

  _panel.appendChild(deleteBtn(node));
}

// ── Widgets ───────────────────────────────────────────────────────────────────

function deleteBtn(node) {
  const btn = d('button', 'btn btn--danger btn--sm');
  btn.textContent = '⌫ Delete node';
  btn.style.marginTop = '20px';
  btn.onclick = () => {
    if (confirm(`Delete "${node.id}"?`)) state.deleteNode(node.id);
  };
  return btn;
}

function field(parent, labelText, inputEl) {
  const wrap = d('div', 'prop-field');
  const lbl = d('div', 'prop-label');
  lbl.textContent = labelText;
  wrap.appendChild(lbl);
  wrap.appendChild(inputEl);
  parent.appendChild(wrap);
}

function label(parent, text) {
  const lbl = d('div', 'prop-label');
  lbl.textContent = text;
  parent.appendChild(lbl);
}

function section(parent, title, openByDefault, buildFn) {
  const det = document.createElement('details');
  if (openByDefault) det.open = true;
  const sum = document.createElement('summary');
  sum.className = 'prop-section';
  sum.textContent = title;
  det.appendChild(sum);
  buildFn(det);
  parent.appendChild(det);
}

function textInput(value, onChange, placeholder = '') {
  const input = d('input', 'prop-input');
  input.type = 'text';
  input.value = value ?? '';
  input.placeholder = placeholder;
  input.addEventListener('change', () => onChange(input.value));
  return input;
}

function select(options, value, onChange) {
  const sel = d('select', 'prop-input');
  options.forEach(opt => {
    const o = document.createElement('option');
    o.value = opt; o.textContent = opt; o.selected = opt === value;
    sel.appendChild(o);
  });
  sel.addEventListener('change', () => onChange(sel.value));
  return sel;
}

const TEMPLATE_HINTS = ['{{payload.}}', '{{runtime.callback.url}}', '{{runtime.callback.token}}'];

function tplInput(value, onChange) {
  const wrap = d('div', 'tpl-wrap');
  const input = d('input', 'prop-input');
  input.type = 'text';
  input.value = value ?? '';
  input.addEventListener('change', () => onChange(input.value));

  const hints = d('div', 'tpl-hints');
  TEMPLATE_HINTS.forEach(hint => {
    const btn = d('button', 'tpl-btn');
    btn.textContent = hint;
    btn.title = 'Insert at cursor';
    btn.onclick = e => {
      e.preventDefault();
      const pos = input.selectionStart ?? input.value.length;
      input.value = input.value.slice(0, pos) + hint + input.value.slice(pos);
      onChange(input.value);
      input.focus();
      input.setSelectionRange(pos + hint.length, pos + hint.length);
    };
    hints.appendChild(btn);
  });

  wrap.appendChild(input);
  wrap.appendChild(hints);
  return wrap;
}

function tplTextarea(value, onChange) {
  const wrap = d('div', 'tpl-wrap');
  const ta = d('textarea', 'prop-textarea');
  ta.rows = 5;
  ta.spellcheck = false;
  ta.value = value ?? '';
  ta.addEventListener('change', () => onChange(ta.value));

  const hints = d('div', 'tpl-hints');
  TEMPLATE_HINTS.forEach(hint => {
    const btn = d('button', 'tpl-btn');
    btn.textContent = hint;
    btn.onclick = e => {
      e.preventDefault();
      const pos = ta.selectionStart ?? ta.value.length;
      ta.setRangeText(hint, pos, pos, 'end');
      onChange(ta.value);
    };
    hints.appendChild(btn);
  });

  wrap.appendChild(ta);
  wrap.appendChild(hints);
  return wrap;
}

function headersEditor(headers, onChange) {
  const wrap = d('div', 'headers-wrap');

  const redraw = () => {
    wrap.innerHTML = '';
    headers.forEach((h, i) => {
      const row = d('div', 'header-row');
      const keyIn = d('input', 'prop-input hdr-key');
      keyIn.type = 'text'; keyIn.placeholder = 'Name'; keyIn.value = h.key;
      keyIn.oninput = () => { headers[i].key = keyIn.value; onChange([...headers]); };
      const valIn = d('input', 'prop-input hdr-val');
      valIn.type = 'text'; valIn.placeholder = 'Value'; valIn.value = h.value;
      valIn.oninput = () => { headers[i].value = valIn.value; onChange([...headers]); };
      const rm = d('button', 'btn btn--icon');
      rm.textContent = '×';
      rm.onclick = () => { headers.splice(i, 1); onChange([...headers]); redraw(); };
      row.appendChild(keyIn); row.appendChild(valIn); row.appendChild(rm);
      wrap.appendChild(row);
    });
    const addBtn = d('button', 'btn btn--sm');
    addBtn.textContent = '+ Header';
    addBtn.onclick = () => { headers.push({ key: '', value: '' }); onChange([...headers]); redraw(); };
    wrap.appendChild(addBtn);
  };

  redraw();
  return wrap;
}

function nodeSelect(currentId, excludeId, onChange) {
  const sel = d('select', 'prop-input');
  const none = document.createElement('option');
  none.value = ''; none.textContent = '— none —'; none.selected = !currentId;
  sel.appendChild(none);
  for (const [id] of state.getNodes()) {
    if (id === excludeId) continue;
    const o = document.createElement('option');
    o.value = id; o.textContent = id; o.selected = id === currentId;
    sel.appendChild(o);
  }
  sel.addEventListener('change', () => onChange(sel.value || null));
  return sel;
}

function checkbox(labelText, checked, onChange) {
  const wrap = d('label', 'checkbox-wrap');
  const input = d('input', '');
  input.type = 'checkbox';
  input.checked = checked;
  input.onchange = () => onChange(input.checked);
  const span = document.createElement('span');
  span.textContent = labelText;
  wrap.appendChild(input);
  wrap.appendChild(span);
  return wrap;
}

function d(tag, cls) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  return e;
}
