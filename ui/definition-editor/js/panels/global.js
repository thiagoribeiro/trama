// panels/global.js — Global saga settings panel

import * as state from '../state.js';

let _panel, _bus;

export function init(panelEl, bus) {
  _panel = panelEl;
  _bus = bus;
  bus.addEventListener('state:changed', () => {
    if (!_panel.classList.contains('hidden') &&
        !document.activeElement?.closest('#global-panel')) draw();
  });
}

export function show() { _panel.classList.remove('hidden'); draw(); }
export function hide() { _panel.classList.add('hidden'); }
export function toggle() { _panel.classList.contains('hidden') ? show() : hide(); }

function draw() {
  _panel.innerHTML = '';
  const { meta, nodes, entrypoint } = state.getState();

  // Header
  const header = d('div', 'global-header');
  const title = d('span', 'global-title');
  title.textContent = 'Saga Settings';
  const closeBtn = d('button', 'btn btn--icon');
  closeBtn.textContent = '×';
  closeBtn.onclick = hide;
  header.appendChild(title);
  header.appendChild(closeBtn);
  _panel.appendChild(header);

  // Saga identity
  field(_panel, 'Name', textIn(meta.name ?? '', v => state.setMeta({ name: v })));
  field(_panel, 'Version', textIn(meta.version ?? 'v1', v => state.setMeta({ version: v })));

  // Entrypoint selector
  field(_panel, 'Entrypoint', (() => {
    const sel = d('select', 'prop-input');
    const none = document.createElement('option');
    none.value = ''; none.textContent = '— none —'; none.selected = !entrypoint;
    sel.appendChild(none);
    for (const [id] of nodes) {
      const o = document.createElement('option');
      o.value = id; o.textContent = id; o.selected = id === entrypoint;
      sel.appendChild(o);
    }
    sel.onchange = () => state.setEntrypoint(sel.value || null);
    return sel;
  })());

  // Failure handling
  section(_panel, 'Failure Handling', true, sec => {
    const fh = meta.failureHandling ?? { type: 'retry', maxAttempts: 2, delayMillis: 500 };

    field(sec, 'Type', (() => {
      const sel = d('select', 'prop-input');
      ['retry', 'backoff'].forEach(v => {
        const o = document.createElement('option');
        o.value = v; o.textContent = v; o.selected = v === fh.type;
        sel.appendChild(o);
      });
      sel.onchange = () => state.setMeta({ failureHandling: { ...fh, type: sel.value } });
      return sel;
    })());

    field(sec, 'Max Attempts', numIn(fh.maxAttempts ?? 2,
      v => state.setMeta({ failureHandling: { ...fh, maxAttempts: v } })));

    if (fh.type === 'retry') {
      field(sec, 'Delay (ms)', numIn(fh.delayMillis ?? 500,
        v => state.setMeta({ failureHandling: { ...fh, delayMillis: v } })));
    } else {
      field(sec, 'Initial delay (ms)', numIn(fh.initialDelayMillis ?? 500,
        v => state.setMeta({ failureHandling: { ...fh, initialDelayMillis: v } })));
      field(sec, 'Max delay (ms)', numIn(fh.maxDelayMillis ?? 30000,
        v => state.setMeta({ failureHandling: { ...fh, maxDelayMillis: v } })));
      field(sec, 'Multiplier', floatIn(fh.multiplier ?? 2.0,
        v => state.setMeta({ failureHandling: { ...fh, multiplier: v } })));
      field(sec, 'Jitter ratio', floatIn(fh.jitterRatio ?? 0.0,
        v => state.setMeta({ failureHandling: { ...fh, jitterRatio: v } })));
    }
  });

  // onSuccessCallback
  callbackSection(_panel, 'On Success Callback', meta.onSuccessCallback,
    cb => state.setMeta({ onSuccessCallback: cb }));

  // onFailureCallback
  callbackSection(_panel, 'On Failure Callback', meta.onFailureCallback,
    cb => state.setMeta({ onFailureCallback: cb }));

  // Payload schema — defines expected input fields, used as autocomplete hints
  section(_panel, 'Payload Schema', false, sec => {
    const schema = meta.payloadSchema ?? [];

    const redraw = () => {
      // remove all children except the summary
      while (sec.children.length > 1) sec.removeChild(sec.lastChild);
      schema.forEach((field, i) => {
        const row = d('div', 'schema-row');

        const nameIn = d('input', 'prop-input schema-name');
        nameIn.type = 'text'; nameIn.placeholder = 'fieldName'; nameIn.value = field.name;
        nameIn.oninput = () => { schema[i].name = nameIn.value; state.setMeta({ payloadSchema: [...schema] }); };

        const typeIn = d('select', 'prop-input schema-type');
        ['string', 'number', 'boolean', 'object', 'array'].forEach(t => {
          const o = document.createElement('option');
          o.value = t; o.textContent = t; o.selected = t === field.type;
          typeIn.appendChild(o);
        });
        typeIn.onchange = () => { schema[i].type = typeIn.value; state.setMeta({ payloadSchema: [...schema] }); };

        const rm = d('button', 'btn btn--icon');
        rm.textContent = '×';
        rm.onclick = () => { schema.splice(i, 1); state.setMeta({ payloadSchema: [...schema] }); redraw(); };

        row.appendChild(nameIn); row.appendChild(typeIn); row.appendChild(rm);
        sec.appendChild(row);
      });

      const addBtn = d('button', 'btn btn--sm');
      addBtn.textContent = '+ Field';
      addBtn.onclick = () => { schema.push({ name: '', type: 'string' }); state.setMeta({ payloadSchema: [...schema] }); redraw(); };
      sec.appendChild(addBtn);
    };
    redraw();
  });
}

function callbackSection(parent, title, cb, onChange) {
  section(parent, title, false, sec => {
    const toggle = checkbox('Enable', !!cb, enabled => {
      onChange(enabled ? { url: '', verb: 'POST', headers: [], body: '' } : null);
    });
    sec.appendChild(toggle);

    if (cb) {
      field(sec, 'URL', textIn(cb.url ?? '', v => onChange({ ...cb, url: v })));
      field(sec, 'Verb', (() => {
        const sel = d('select', 'prop-input');
        ['POST', 'PUT', 'GET'].forEach(v => {
          const o = document.createElement('option');
          o.value = v; o.textContent = v; o.selected = v === (cb.verb ?? 'POST');
          sel.appendChild(o);
        });
        sel.onchange = () => onChange({ ...cb, verb: sel.value });
        return sel;
      })());
      field(sec, 'Body', (() => {
        const ta = d('textarea', 'prop-textarea');
        ta.rows = 3; ta.spellcheck = false;
        ta.value = typeof cb.body === 'string' ? cb.body
          : (cb.body ? JSON.stringify(cb.body, null, 2) : '');
        ta.onchange = () => onChange({ ...cb, body: ta.value });
        return ta;
      })());
    }
  });
}

// ── Helpers ────────────────────────────────────────────────────────────────────

function field(parent, labelText, inputEl) {
  const wrap = d('div', 'prop-field');
  const lbl = d('div', 'prop-label'); lbl.textContent = labelText;
  wrap.appendChild(lbl); wrap.appendChild(inputEl);
  parent.appendChild(wrap);
}

function section(parent, title, open, buildFn) {
  const det = document.createElement('details');
  if (open) det.open = true;
  const sum = document.createElement('summary');
  sum.className = 'prop-section'; sum.textContent = title;
  det.appendChild(sum); buildFn(det); parent.appendChild(det);
}

function textIn(value, onChange) {
  const input = d('input', 'prop-input');
  input.type = 'text'; input.value = value ?? '';
  input.onchange = () => onChange(input.value);
  return input;
}

function numIn(value, onChange) {
  const input = d('input', 'prop-input');
  input.type = 'number'; input.value = String(value ?? 0);
  input.onchange = () => onChange(parseInt(input.value) || 0);
  return input;
}

function floatIn(value, onChange) {
  const input = d('input', 'prop-input');
  input.type = 'number'; input.step = '0.1'; input.value = String(value ?? 0);
  input.onchange = () => onChange(parseFloat(input.value) || 0);
  return input;
}

function checkbox(labelText, checked, onChange) {
  const wrap = d('label', 'checkbox-wrap');
  const input = document.createElement('input');
  input.type = 'checkbox'; input.checked = checked;
  input.onchange = () => onChange(input.checked);
  const span = document.createElement('span');
  span.textContent = labelText;
  wrap.appendChild(input); wrap.appendChild(span);
  return wrap;
}

function d(tag, cls) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  return e;
}
