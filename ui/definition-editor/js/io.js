// io.js — Import / Export

// ── Export ────────────────────────────────────────────────────────────────────

export function exportDefinition(st) {
  const { meta, nodes, entrypoint } = st;

  const exportedNodes = [];
  for (const [, node] of nodes) {
    if (node.kind === 'task') exportedNodes.push(exportTask(node));
    else if (node.kind === 'switch') exportedNodes.push(exportSwitch(node));
    else if (node.kind === 'sleep') exportedNodes.push(exportSleep(node));
  }

  const def = {
    name: meta.name || 'my-saga',
    version: meta.version || 'v1',
    failureHandling: meta.failureHandling || { type: 'retry', maxAttempts: 2, delayMillis: 500 },
    entrypoint,
    nodes: exportedNodes,
  };

  if (meta.onSuccessCallback) def.onSuccessCallback = exportHttpCall(meta.onSuccessCallback);
  if (meta.onFailureCallback) def.onFailureCallback = exportHttpCall(meta.onFailureCallback);

  return def;
}

function exportTask(node) {
  const out = {
    kind: 'task',
    id: node.id,
    action: {
      mode: node.mode,
      request: exportHttpCall(node.request),
    },
  };

  if (node.mode === 'async') {
    if (node.acceptedStatusCodes?.length)
      out.action.acceptedStatusCodes = node.acceptedStatusCodes;
    if (node.callback) {
      out.action.callback = { timeoutMillis: node.callback.timeoutMillis || 60000 };
      if (node.callback.successWhen) out.action.callback.successWhen = node.callback.successWhen;
      if (node.callback.failureWhen) out.action.callback.failureWhen = node.callback.failureWhen;
    }
  } else {
    if (node.request?.successStatusCodes?.length)
      out.action.successStatusCodes = node.request.successStatusCodes;
  }

  if (node.compensation) out.compensation = exportHttpCall(node.compensation);
  if (node.next) out.next = node.next;

  return out;
}

function exportSwitch(node) {
  return {
    kind: 'switch',
    id: node.id,
    cases: node.cases
      .filter(c => c.when && c.target)
      .map(c => ({ ...(c.name ? { name: c.name } : {}), when: c.when, target: c.target })),
    default: node.default,
  };
}

function exportHttpCall(call) {
  if (!call) return null;
  const out = { url: call.url || '', verb: call.verb || 'POST' };
  if (call.headers?.length)
    out.headers = Object.fromEntries(call.headers.filter(h => h.key).map(h => [h.key, h.value]));
  if (call.body) out.body = tryParseJson(call.body);
  return out;
}

function tryParseJson(v) {
  if (typeof v !== 'string') return v;
  try { return JSON.parse(v); } catch { return v; }
}

// ── Import ────────────────────────────────────────────────────────────────────

export function importDefinition(input) {
  let def;
  try {
    def = typeof input === 'string' ? JSON.parse(input) : input;
  } catch (e) {
    return { ok: false, errors: [`Invalid JSON: ${e.message}`] };
  }

  const errors = [];
  if (!def.name) errors.push('Missing "name"');
  if (!def.entrypoint) errors.push('Missing "entrypoint"');
  if (!Array.isArray(def.nodes)) errors.push('"nodes" must be an array');
  if (errors.length) return { ok: false, errors };

  const nodes = def.nodes.map(n => {
    if (n.kind === 'task') return importTask(n);
    if (n.kind === 'switch') return importSwitch(n);
    if (n.kind === 'sleep') return importSleep(n);
    return null;
  }).filter(Boolean);

  assignPositions(nodes, def.entrypoint);

  return {
    ok: true,
    snapshot: {
      meta: {
        name: def.name,
        version: def.version || 'v1',
        failureHandling: def.failureHandling || { type: 'retry', maxAttempts: 2, delayMillis: 500 },
        onSuccessCallback: def.onSuccessCallback ? importHttpCall(def.onSuccessCallback) : null,
        onFailureCallback: def.onFailureCallback ? importHttpCall(def.onFailureCallback) : null,
      },
      nodes,
      entrypoint: def.entrypoint,
    },
  };
}

function importTask(n) {
  return {
    kind: 'task',
    id: n.id,
    x: 0, y: 0,
    mode: n.action?.mode || 'sync',
    request: importHttpCall(n.action?.request),
    acceptedStatusCodes: n.action?.acceptedStatusCodes || [],
    callback: {
      timeoutMillis: n.action?.callback?.timeoutMillis || 60000,
      successWhen: n.action?.callback?.successWhen || null,
      failureWhen: n.action?.callback?.failureWhen || null,
    },
    compensation: n.compensation ? importHttpCall(n.compensation) : null,
    next: n.next || null,
  };
}

function importSwitch(n) {
  return {
    kind: 'switch',
    id: n.id,
    x: 0, y: 0,
    cases: (n.cases || []).map(c => ({ name: c.name || '', when: c.when || null, target: c.target || null })),
    default: n.default || null,
  };
}

function importSleep(n) {
  return {
    kind: 'sleep',
    id: n.id,
    x: 0, y: 0,
    durationMillis: n.durationMillis ?? 0,
    next: n.next || null,
  };
}

function exportSleep(node) {
  const out = { kind: 'sleep', id: node.id, durationMillis: node.durationMillis };
  if (node.next) out.next = node.next;
  return out;
}

function importHttpCall(call) {
  if (!call) return { url: '', verb: 'POST', headers: [], body: '', successStatusCodes: [] };
  return {
    url: call.url || '',
    verb: call.verb || 'POST',
    headers: call.headers
      ? Object.entries(call.headers).map(([key, value]) => ({ key, value: String(value) }))
      : [],
    body: call.body != null
      ? (typeof call.body === 'string' ? call.body : JSON.stringify(call.body, null, 2))
      : '',
    successStatusCodes: call.successStatusCodes || [],
  };
}

// ── Auto-layout (used on import) ───────────────────────────────────────────────

function assignPositions(nodes, entrypoint) {
  const COL_W = 280, ROW_H = 150;
  const byId = new Map(nodes.map(n => [n.id, n]));
  const adj = new Map(nodes.map(n => [n.id, []]));
  const indeg = new Map(nodes.map(n => [n.id, 0]));

  for (const n of nodes) {
    for (const t of nodeTargets(n)) {
      if (byId.has(t)) {
        adj.get(n.id).push(t);
        indeg.set(t, indeg.get(t) + 1);
      }
    }
  }

  // BFS from entrypoint
  const visited = new Set();
  const queue = byId.has(entrypoint) ? [entrypoint] : [];
  const sorted = [];
  while (queue.length) {
    const id = queue.shift();
    if (visited.has(id)) continue;
    visited.add(id); sorted.push(id);
    for (const t of adj.get(id) || []) queue.push(t);
  }
  for (const n of nodes) if (!visited.has(n.id)) sorted.push(n.id);

  // Depth assignment
  const depth = new Map(nodes.map(n => [n.id, 0]));
  for (const id of sorted) {
    const d = depth.get(id) || 0;
    for (const t of adj.get(id) || []) {
      if ((depth.get(t) || 0) < d + 1) depth.set(t, d + 1);
    }
  }

  // Group by column, assign positions
  const cols = new Map();
  for (const id of sorted) {
    const col = depth.get(id) || 0;
    if (!cols.has(col)) cols.set(col, []);
    cols.get(col).push(id);
  }
  for (const [col, colIds] of cols) {
    colIds.forEach((id, row) => {
      const n = byId.get(id);
      if (n) { n.x = col * COL_W + 80; n.y = row * ROW_H + 100; }
    });
  }
}

function nodeTargets(node) {
  if (node.kind === 'task') return node.next ? [node.next] : [];
  if (node.kind === 'sleep') return node.next ? [node.next] : [];
  if (node.kind === 'switch')
    return [...node.cases.map(c => c.target), node.default].filter(Boolean);
  return [];
}
