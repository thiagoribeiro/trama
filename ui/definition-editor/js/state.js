// state.js — Single source of truth for the editor

let _bus = null;
let _nodeCounter = 0;

const _state = {
  meta: {
    name: 'my-saga',
    version: 'v1',
    failureHandling: { type: 'retry', maxAttempts: 2, delayMillis: 500 },
    onSuccessCallback: null,
    onFailureCallback: null,
    payloadSchema: [],   // [{ name: string, type: string }]
  },
  nodes: new Map(),
  entrypoint: null,
  selectedNodeId: null,
};

export function init(bus) {
  _bus = bus;
}

export function getState() {
  return _state;
}

export function getNodes() {
  return _state.nodes;
}

function notify() {
  _bus.dispatchEvent(new CustomEvent('state:changed'));
}

export function addNode(kind, x, y) {
  const id = `${kind}-${++_nodeCounter}`;
  let node;
  if (kind === 'task') {
    node = {
      kind: 'task', id, x, y,
      mode: 'sync',
      request: { url: '', verb: 'POST', headers: [], body: '', successStatusCodes: [] },
      acceptedStatusCodes: [],
      callback: { timeoutMillis: 60000, successWhen: null, failureWhen: null },
      compensation: null,
      next: null,
    };
  } else if (kind === 'switch') {
    node = { kind: 'switch', id, x, y, cases: [], default: null };
  } else if (kind === 'sleep') {
    node = { kind: 'sleep', id, x, y, durationMillis: 60000, next: null };
  }
  _state.nodes.set(id, node);
  if (!_state.entrypoint) _state.entrypoint = id;
  notify();
  return id;
}

/** Rename a node ID and propagate all references. Returns false if newId is already taken. */
export function renameNode(oldId, newId) {
  newId = newId.trim();
  if (!newId || oldId === newId) return true;
  if (_state.nodes.has(newId)) return false;
  const node = _state.nodes.get(oldId);
  if (!node) return false;
  node.id = newId;
  _state.nodes = new Map(
    [..._state.nodes.entries()].map(([k, v]) => [k === oldId ? newId : k, v])
  );
  for (const [, n] of _state.nodes) {
    if ((n.kind === 'task' || n.kind === 'sleep') && n.next === oldId) n.next = newId;
    if (n.kind === 'switch') {
      n.cases = n.cases.map(c => ({ ...c, target: c.target === oldId ? newId : c.target }));
      if (n.default === oldId) n.default = newId;
    }
  }
  if (_state.entrypoint === oldId) _state.entrypoint = newId;
  if (_state.selectedNodeId === oldId) _state.selectedNodeId = newId;
  notify();
  return true;
}

export function updateNode(id, patch) {
  const node = _state.nodes.get(id);
  if (!node) return;
  deepPatch(node, patch);
  notify();
}

export function deleteNode(id) {
  _state.nodes.delete(id);
  for (const [, node] of _state.nodes) {
    if ((node.kind === 'task' || node.kind === 'sleep') && node.next === id) node.next = null;
    if (node.kind === 'switch') {
      node.cases = node.cases.filter(c => c.target !== id);
      if (node.default === id) node.default = null;
    }
  }
  if (_state.entrypoint === id) {
    _state.entrypoint = _state.nodes.size > 0 ? _state.nodes.keys().next().value : null;
  }
  if (_state.selectedNodeId === id) {
    _state.selectedNodeId = null;
    _bus.dispatchEvent(new CustomEvent('node:selected', { detail: { id: null } }));
  }
  notify();
}

export function moveNode(id, dx, dy) {
  const node = _state.nodes.get(id);
  if (!node) return;
  node.x += dx;
  node.y += dy;
  notify();
}

export function setEntrypoint(id) {
  _state.entrypoint = id;
  notify();
}

export function selectNode(id) {
  _state.selectedNodeId = id;
  _bus.dispatchEvent(new CustomEvent('node:selected', { detail: { id } }));
  // Don't call notify() here — graph already highlights via state:changed from elsewhere
  // But we do need a render pass for highlight
  _bus.dispatchEvent(new CustomEvent('state:changed'));
}

export function setMeta(patch) {
  Object.assign(_state.meta, patch);
  notify();
}

export function replaceAll(snapshot) {
  _state.meta = { payloadSchema: [], ...snapshot.meta };
  _state.nodes = new Map(snapshot.nodes.map(n => [n.id, n]));
  _state.entrypoint = snapshot.entrypoint;
  _state.selectedNodeId = null;
  _nodeCounter = _state.nodes.size;
  _bus.dispatchEvent(new CustomEvent('node:selected', { detail: { id: null } }));
  notify();
}

export function deepPatch(target, patch) {
  for (const [k, v] of Object.entries(patch)) {
    if (
      v !== null && typeof v === 'object' && !Array.isArray(v) &&
      typeof target[k] === 'object' && target[k] !== null && !Array.isArray(target[k])
    ) {
      deepPatch(target[k], v);
    } else {
      target[k] = v;
    }
  }
}
