<template>
  <div class="timeline">
    <div v-if="!steps.length" class="empty">No step results.</div>
    <div
      v-for="step in steps"
      :key="step.id"
      class="step"
      :class="stepClass(step)"
    >
      <div class="step__header" @click="toggle(step.id)">
        <span class="step__icon">{{ stepIcon(step) }}</span>
        <span class="step__name">{{ step.stepName }}</span>
        <span class="step__phase dim">{{ step.phase }}</span>
        <span v-if="step.statusCode" class="step__code" :class="codeClass(step.statusCode)">
          {{ step.statusCode }}
        </span>
        <span class="step__latency dim">{{ fmtLatency(step.latencyMs) }}</span>
        <span class="step__toggle dim">{{ expanded.has(step.id) ? '▲' : '▼' }}</span>
      </div>

      <div v-if="expanded.has(step.id)" class="step__body">
        <div class="step__meta">
          <span class="dim">Started:</span> {{ fmtDate(step.stepStartedAt ?? step.startedAt) }}
          <span class="dim">Recorded:</span> {{ fmtDate(step.createdAt) }}
        </div>

        <!-- HTTP call attempts (not shown for inbound CALLBACK steps) -->
        <div v-if="step.phase !== 'CALLBACK'" class="step__calls-label">HTTP Calls</div>
        <div v-if="step.phase !== 'CALLBACK' && stepCalls(step).length" class="step__calls">
          <div
            v-for="call in stepCalls(step)"
            :key="call.id"
            class="call"
            :class="callClass(call)"
          >
            <div class="call__header">
              <span class="call__attempt dim">attempt #{{ call.attempt }}</span>
              <span v-if="call.statusCode" class="call__code" :class="codeClass(call.statusCode)">{{ call.statusCode }}</span>
              <span class="call__latency dim">{{ fmtLatency(call.latencyMs) }}</span>
              <span class="call__time dim">{{ fmtDate(call.stepStartedAt) }}</span>
            </div>
            <div v-if="call.requestUrl" class="call__url">{{ call.requestUrl }}</div>
            <div v-if="call.error" class="call__error">{{ call.error }}</div>
            <div v-if="call.requestBody !== null && call.requestBody !== undefined" class="call__section">
              <div class="step__label">Request</div>
              <pre class="step__code-block">{{ fmtJson(call.requestBody) }}</pre>
            </div>
            <div v-if="call.responseBody !== null && call.responseBody !== undefined" class="call__section">
              <div class="step__label">Response</div>
              <pre class="step__code-block">{{ fmtJson(call.responseBody) }}</pre>
            </div>
          </div>
        </div>
        <div v-else-if="step.phase === 'CALLBACK' && step.responseBody !== null && step.responseBody !== undefined">
          <div class="step__label">Callback body</div>
          <pre class="step__code-block">{{ fmtJson(step.responseBody) }}</pre>
        </div>
        <div v-else-if="step.phase === 'CALLBACK' && !step.success" class="dim" style="font-size:0.8rem">Callback timed out — no body received.</div>
        <div v-else-if="step.phase === 'CALLBACK'" class="dim" style="font-size:0.8rem">Callback received (no body).</div>
        <div v-else-if="step.responseBody !== null && step.responseBody !== undefined">
          <div class="step__label">Response body</div>
          <pre class="step__code-block">{{ fmtJson(step.responseBody) }}</pre>
        </div>
        <div v-else class="dim" style="font-size:0.8rem">No call records.</div>
      </div>
    </div>
  </div>
</template>

<script setup>
import { ref } from 'vue'

const props = defineProps({
  steps: { type: Array, default: () => [] },
  calls: { type: Array, default: () => [] },
})

function stepCalls(step) {
  return props.calls.filter(c => c.stepName === step.stepName && c.phase === step.phase)
}

const expanded = ref(new Set())

function toggle(id) {
  if (expanded.value.has(id)) expanded.value.delete(id)
  else expanded.value.add(id)
  expanded.value = new Set(expanded.value)
}

function stepIcon(step) {
  if (step.phase === 'CALLBACK') return step.success ? '↙' : '✗'
  if (!step.success) return '✗'
  return step.phase === 'UP' ? '▶' : '↩'
}

function stepClass(step) {
  if (step.phase === 'CALLBACK') return step.success ? 'step--callback' : 'step--failed'
  if (!step.success) return 'step--failed'
  return step.phase === 'UP' ? 'step--success' : 'step--down'
}

function codeClass(code) {
  if (code >= 200 && code < 300) return 'code--ok'
  if (code >= 400) return 'code--err'
  return ''
}

function fmtLatency(ms) {
  if (ms == null) return ''
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

function fmtDate(iso) {
  return iso ? new Date(iso).toLocaleTimeString() : '—'
}

function callClass(call) {
  if (call.error) return 'call--error'
  if (call.statusCode && call.statusCode >= 400) return 'call--error'
  return 'call--ok'
}

function fmtJson(val) {
  try { return JSON.stringify(val, null, 2) } catch { return String(val) }
}
</script>

<style scoped>
.timeline { display: flex; flex-direction: column; gap: 4px; }

.step {
  border: 1px solid #30363d;
  border-radius: 6px;
  overflow: hidden;
}
.step--success  { border-left: 3px solid #3fb950; }
.step--failed   { border-left: 3px solid #f85149; }
.step--down     { border-left: 3px solid #e3b341; }
.step--callback { border-left: 3px solid #58a6ff; }

.step__header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  cursor: pointer;
  background: #161b22;
  user-select: none;
}
.step__header:hover { background: #21262d; }

.step__icon { font-size: 0.75rem; width: 1rem; text-align: center; }
.step__name { font-family: monospace; font-size: 0.85rem; flex: 1; }
.step__phase { font-size: 0.7rem; }
.step__code { font-size: 0.75rem; font-weight: 600; }
.step__latency { font-size: 0.75rem; margin-left: auto; }
.step__toggle { font-size: 0.65rem; }

.code--ok  { color: #3fb950; }
.code--err { color: #f85149; }

.step__body {
  padding: 0.75rem;
  background: #0d1117;
  border-top: 1px solid #30363d;
}

.step__meta {
  font-size: 0.75rem;
  color: #8b949e;
  display: flex;
  gap: 1rem;
  margin-bottom: 0.5rem;
}

.step__label {
  font-size: 0.75rem;
  color: #8b949e;
  margin-bottom: 0.25rem;
}

.step__code-block {
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 4px;
  padding: 0.5rem;
  font-size: 0.78rem;
  overflow-x: auto;
  max-height: 300px;
  overflow-y: auto;
  white-space: pre-wrap;
  word-break: break-all;
}

.step__calls-label {
  font-size: 0.75rem;
  color: #8b949e;
  margin: 0.5rem 0 0.25rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.step__calls { display: flex; flex-direction: column; gap: 6px; }

.call {
  border: 1px solid #30363d;
  border-radius: 4px;
  overflow: hidden;
  border-left: 2px solid #30363d;
}
.call--ok    { border-left-color: #3fb950; }
.call--error { border-left-color: #f85149; }

.call__header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.35rem 0.6rem;
  background: #161b22;
  font-size: 0.75rem;
}

.call__attempt { font-family: monospace; }
.call__code { font-weight: 600; }
.call__latency { margin-left: auto; }
.call__time { font-size: 0.7rem; }

.call__url {
  font-family: monospace;
  font-size: 0.75rem;
  padding: 0.25rem 0.6rem;
  background: #0d1117;
  color: #79c0ff;
  word-break: break-all;
}

.call__error {
  font-size: 0.75rem;
  padding: 0.25rem 0.6rem;
  background: #0d1117;
  color: #f85149;
  font-family: monospace;
}

.call__section {
  padding: 0.4rem 0.6rem;
  background: #0d1117;
}
</style>
