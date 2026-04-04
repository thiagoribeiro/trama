// panels/jsonlogic.js — Visual json-logic expression builder

const OPS = [
  { v: '==',  l: '==' },
  { v: '!=',  l: '!=' },
  { v: '<',   l: '<'  },
  { v: '<=',  l: '<=' },
  { v: '>',   l: '>'  },
  { v: '>=',  l: '>=' },
  { v: 'in',  l: 'in  (comma list)' },
  { v: '!',   l: '!   (not, unary)' },
];

let _uid = 0;

/**
 * Creates a json-logic builder UI inside `container`.
 *
 * @param {HTMLElement}          container     - Where to render the builder
 * @param {object|null}          initialExpr  - Existing json-logic object (or null)
 * @param {function}             onChange     - Called with the new json-logic object on every change
 * @param {string[]}             nodeIds      - Current graph node IDs (for path suggestions)
 * @param {{ name, type }[]}     payloadSchema - Known payload fields for autocomplete
 */
export function createJsonLogicBuilder(container, initialExpr, onChange, nodeIds = [], payloadSchema = []) {
  const uid = ++_uid;

  let st = { mode: 'visual', combinator: 'and', conditions: [] };
  if (initialExpr != null) {
    const parsed = toVisual(initialExpr);
    if (parsed) st = parsed;
    else st = { mode: 'raw', combinator: 'and', conditions: [], rawJson: JSON.stringify(initialExpr, null, 2) };
  }

  function emit() {
    if (st.mode === 'visual') {
      onChange(toJsonLogic(st));
    } else {
      try { onChange(JSON.parse(st.rawJson)); } catch { /* invalid — don't emit */ }
    }
  }

  function _getRule() {
    if (st.mode === 'visual') return toJsonLogic(st);
    return JSON.parse(st.rawJson || 'null');
  }

  function draw() {
    container.innerHTML = '';
    container.className = 'jlb';

    // ── Mode toggle ──────────────────────────────────────────────────────────
    const bar = d('div', 'jlb-bar');
    for (const m of ['visual', 'raw']) {
      const btn = d('button', 'jlb-mode' + (st.mode === m ? ' active' : ''));
      btn.textContent = m === 'visual' ? 'Visual' : 'Raw JSON';
      btn.onclick = () => {
        if (m === 'raw' && st.mode === 'visual')
          st.rawJson = JSON.stringify(toJsonLogic(st), null, 2);
        else if (m === 'visual' && st.mode === 'raw') {
          try {
            const p = toVisual(JSON.parse(st.rawJson));
            if (p) st = p;
          } catch { /* stay raw */ }
        }
        st.mode = m;
        draw();
      };
      bar.appendChild(btn);
    }
    container.appendChild(bar);

    if (st.mode === 'visual') drawVisual();
    else drawRaw();
  }

  // ── Visual mode ─────────────────────────────────────────────────────────────
  function drawVisual() {
    if (st.conditions.length > 1) {
      const combRow = d('div', 'jlb-comb');
      combRow.appendChild(t('Combine: '));
      for (const c of ['and', 'or']) {
        const btn = d('button', 'jlb-comb-btn' + (st.combinator === c ? ' active' : ''));
        btn.textContent = c.toUpperCase();
        btn.onclick = () => { st.combinator = c; emit(); draw(); };
        combRow.appendChild(btn);
      }
      container.appendChild(combRow);
    }

    const list = d('div', 'jlb-list');
    st.conditions.forEach((cond, i) => list.appendChild(drawRow(cond, i)));
    container.appendChild(list);

    if (st.conditions.length === 0) {
      const hint = d('div', 'jlb-hint');
      hint.textContent = 'Click "+ Condition" to add a condition';
      container.appendChild(hint);
    }

    const addBtn = d('button', 'jlb-add');
    addBtn.textContent = '+ Condition';
    addBtn.onclick = () => {
      st.conditions.push({ _id: Math.random(), path: '', op: '==', value: '' });
      draw();
    };
    container.appendChild(addBtn);
  }

  function drawRow(cond, i) {
    const row = d('div', 'jlb-row');
    const listId = `jlb-dl-${uid}-${i}`;

    // Datalist for path suggestions
    const dl = document.createElement('datalist');
    dl.id = listId;
    const suggestions = [
      ...payloadSchema.map(f => `payload.${f.name}`),
      'payload.',
      'prev.body.',
      ...nodeIds.map(id => `nodes.${id}.response.body.`),
    ];
    suggestions.forEach(s => { const o = document.createElement('option'); o.value = s; dl.appendChild(o); });
    row.appendChild(dl);

    // Path input
    const pathIn = d('input', 'jlb-path');
    pathIn.type = 'text';
    pathIn.placeholder = 'payload.field';
    pathIn.value = cond.path;
    pathIn.setAttribute('list', listId);
    pathIn.oninput = () => { cond.path = pathIn.value; emit(); };
    row.appendChild(pathIn);

    // Operator
    const opSel = d('select', 'jlb-op');
    OPS.forEach(({ v, l }) => {
      const o = document.createElement('option');
      o.value = v; o.textContent = l; o.selected = v === cond.op;
      opSel.appendChild(o);
    });
    opSel.onchange = () => { cond.op = opSel.value; emit(); draw(); };
    row.appendChild(opSel);

    // Value input (hidden for unary !)
    const valIn = d('input', 'jlb-value');
    valIn.type = 'text';
    valIn.placeholder = cond.op === 'in' ? 'a, b, c' : 'value';
    valIn.value = cond.value;
    valIn.style.display = cond.op === '!' ? 'none' : '';
    valIn.oninput = () => { cond.value = valIn.value; emit(); };
    row.appendChild(valIn);

    // Remove
    const rm = d('button', 'jlb-rm');
    rm.textContent = '×';
    rm.title = 'Remove condition';
    rm.onclick = () => { st.conditions.splice(i, 1); emit(); draw(); };
    row.appendChild(rm);

    return row;
  }

  // ── Raw mode ─────────────────────────────────────────────────────────────────
  function drawRaw() {
    const ta = d('textarea', 'jlb-raw');
    ta.rows = 5;
    ta.spellcheck = false;
    ta.value = st.rawJson ?? '';
    ta.oninput = () => {
      st.rawJson = ta.value;
      try { onChange(JSON.parse(ta.value)); ta.style.outline = ''; } catch { ta.style.outline = '1px solid #f87171'; }
    };
    container.appendChild(ta);

    const valBtn = d('button', 'jlb-validate');
    valBtn.textContent = 'Validate';
    valBtn.onclick = () => {
      try { JSON.parse(ta.value); ta.style.outline = '1px solid #4ade80'; }
      catch (e) { ta.style.outline = '1px solid #f87171'; alert('JSON error: ' + e.message); }
    };
    container.appendChild(valBtn);
  }

  draw();
  drawTestSandbox(container, _getRule);
}

// ── Test sandbox ───────────────────────────────────────────────────────────────

function drawTestSandbox(container, _getRule) {
  // Requires window.jsonLogic (json-logic-js loaded from CDN)
  // Renders inline below the builder — collapsed by default.
  const det = document.createElement('details');
  const sum = document.createElement('summary');
  sum.className = 'jlb-test-summary';
  sum.textContent = 'Test condition';
  det.appendChild(sum);

  const body = document.createElement('div');
  body.className = 'jlb-test-body';

  const payloadLabel = document.createElement('div');
  payloadLabel.className = 'jlb-test-label';
  payloadLabel.textContent = 'Sample payload JSON:';
  body.appendChild(payloadLabel);

  // Pre-fill sample from schema if available
  const samplePayload = {};
  // We don't have payloadSchema here directly, but we can infer from suggestions
  const ta = document.createElement('textarea');
  ta.className = 'jlb-raw';
  ta.rows = 3;
  ta.spellcheck = false;
  ta.placeholder = '{"paymentMethod": "pix"}';
  body.appendChild(ta);

  const runBtn = document.createElement('button');
  runBtn.className = 'jlb-validate';
  runBtn.textContent = 'Evaluate';

  const resultEl = document.createElement('div');
  resultEl.className = 'jlb-test-result';

  runBtn.onclick = () => {
    if (!window.jsonLogic) {
      resultEl.textContent = '⚠ json-logic-js not loaded';
      resultEl.className = 'jlb-test-result jlb-test-warn';
      return;
    }
    let data;
    try { data = JSON.parse(ta.value || '{}'); }
    catch (e) { resultEl.textContent = `JSON error: ${e.message}`; resultEl.className = 'jlb-test-result jlb-test-fail'; return; }

    // Get current expression — build from visual or raw state
    let rule;
    try {
      // Retrieve the last emitted expression by evaluating the builder state
      // We re-compute here from the outer closure's `st` and helper functions
      rule = _getRule();
    } catch (e) {
      resultEl.textContent = `Rule error: ${e.message}`;
      resultEl.className = 'jlb-test-result jlb-test-fail';
      return;
    }

    if (rule == null) {
      resultEl.textContent = '⚠ No conditions defined';
      resultEl.className = 'jlb-test-result jlb-test-warn';
      return;
    }

    try {
      const result = window.jsonLogic.apply(rule, data);
      resultEl.textContent = result ? '✓ true' : '✗ false';
      resultEl.className = `jlb-test-result ${result ? 'jlb-test-pass' : 'jlb-test-fail'}`;
    } catch (e) {
      resultEl.textContent = `Eval error: ${e.message}`;
      resultEl.className = 'jlb-test-result jlb-test-fail';
    }
  };

  body.appendChild(runBtn);
  body.appendChild(resultEl);
  det.appendChild(body);
  container.appendChild(det);
}

// ── Serialise visual → json-logic ──────────────────────────────────────────────

function toJsonLogic(st) {
  const conds = st.conditions.map(serCond).filter(Boolean);
  if (conds.length === 0) return null;
  if (conds.length === 1) return conds[0];
  return { [st.combinator]: conds };
}

function serCond(c) {
  if (!c.path) return null;
  const ref = { var: c.path };
  if (c.op === '!') return { '!': ref };
  if (c.op === 'in') {
    const items = c.value.split(',').map(s => s.trim()).filter(Boolean);
    return { in: [ref, items] };
  }
  return { [c.op]: [ref, coerce(c.value)] };
}

function coerce(v) {
  if (v === 'true') return true;
  if (v === 'false') return false;
  if (v === 'null') return null;
  const n = Number(v);
  return (!isNaN(n) && v.trim() !== '') ? n : v;
}

// ── Parse json-logic → visual state ──────────────────────────────────────────

function toVisual(expr) {
  if (!expr) return { mode: 'visual', combinator: 'and', conditions: [] };

  // Compound AND/OR
  for (const comb of ['and', 'or']) {
    if (Array.isArray(expr[comb])) {
      const conditions = [];
      for (const sub of expr[comb]) {
        const row = parseSingleCond(sub);
        if (!row) return null;
        conditions.push(row);
      }
      return { mode: 'visual', combinator: comb, conditions };
    }
  }

  // Single condition
  const row = parseSingleCond(expr);
  if (row) return { mode: 'visual', combinator: 'and', conditions: [row] };
  return null;
}

function parseSingleCond(expr) {
  if (!expr || typeof expr !== 'object') return null;

  // Unary !
  if (expr['!'] != null) {
    const path = expr['!']?.var;
    if (path) return { _id: Math.random(), path, op: '!', value: '' };
  }

  for (const op of ['==', '!=', '<', '<=', '>', '>=', 'in']) {
    if (expr[op] !== undefined) {
      const args = expr[op];
      if (!Array.isArray(args) || !args[0]?.var) return null;
      const path = args[0].var;
      const value = op === 'in'
        ? (Array.isArray(args[1]) ? args[1].join(', ') : '')
        : String(args[1] ?? '');
      return { _id: Math.random(), path, op, value };
    }
  }

  return null;
}

// ── DOM helpers ────────────────────────────────────────────────────────────────

function d(tag, cls) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  return e;
}

function t(text) {
  return document.createTextNode(text);
}
