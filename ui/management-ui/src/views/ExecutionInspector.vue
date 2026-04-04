<template>
  <div class="inspector">
    <header class="page-header">
      <div class="header-left">
        <RouterLink to="/executions" class="btn">← Back</RouterLink>
        <h1 class="mono">{{ execution?.name ?? 'Loading…' }}</h1>
        <StatusBadge v-if="execution" :status="execution.status" />
      </div>
      <button
        v-if="execution?.status === 'FAILED'"
        class="btn btn--primary"
        @click="retry"
        :disabled="retrying"
      >
        {{ retrying ? 'Retrying…' : 'Retry' }}
      </button>
    </header>

    <div v-if="error" class="alert alert--error">{{ error }}</div>
    <div v-if="retryMsg" class="alert alert--success">{{ retryMsg }}</div>

    <div v-if="execution" class="meta">
      <span class="dim">ID:</span> <span class="mono">{{ execution.id }}</span>
      <span class="dim">Version:</span> <span class="mono">{{ execution.version }}</span>
      <span class="dim">Started:</span> {{ fmtDate(execution.startedAt) }}
      <span v-if="execution.completedAt" class="dim">Completed:</span>
      <span v-if="execution.completedAt">{{ fmtDate(execution.completedAt) }}</span>
      <span class="dim">Duration:</span> {{ duration(execution.startedAt, execution.completedAt) }}
    </div>

    <div v-if="execution?.failureDescription" class="alert alert--error failure">
      {{ execution.failureDescription }}
    </div>

    <div v-if="loading" class="loading">Loading…</div>

    <div v-else class="inspector__body">
      <!-- Definition Graph -->
      <section v-if="definition" class="graph-section">
        <h2>Definition Graph</h2>
        <DefinitionGraph :definition="definition" :steps="steps" />
      </section>

      <!-- Step Timeline -->
      <section class="timeline-section">
        <h2>Steps</h2>
        <StepTimeline :steps="steps" :calls="calls" />
      </section>
    </div>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import StatusBadge from '../components/StatusBadge.vue'
import StepTimeline from '../components/StepTimeline.vue'
import DefinitionGraph from '../components/DefinitionGraph.vue'
import { api } from '../api/index.js'

const route = useRoute()

const execution  = ref(null)
const steps      = ref([])
const calls      = ref([])
const definition = ref(null)
const loading    = ref(true)
const error      = ref(null)
const retrying   = ref(false)
const retryMsg   = ref(null)

onMounted(load)

async function load() {
  loading.value = true
  error.value = null
  try {
    const detail = await api.getExecutionDetail(route.params.id)
    execution.value  = detail?.execution ?? null
    steps.value      = detail?.steps ?? []
    calls.value      = detail?.calls ?? []
    // detail.definition is SagaDefinitionResponse; the graph uses .definition (the inner object)
    definition.value = detail?.definition?.definition ?? null
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

async function retry() {
  retrying.value = true
  retryMsg.value = null
  error.value = null
  try {
    await api.retryExecution(route.params.id)
    retryMsg.value = 'Retry queued. Refreshing…'
    setTimeout(load, 2000)
  } catch (e) {
    error.value = e.message
  } finally {
    retrying.value = false
  }
}

function fmtDate(iso) {
  return iso ? new Date(iso).toLocaleString() : '—'
}

function duration(start, end) {
  if (!start) return '—'
  const ms = new Date(end ?? Date.now()) - new Date(start)
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${Math.floor(ms / 60000)}m ${Math.floor((ms % 60000) / 1000)}s`
}
</script>

<style scoped>
.inspector { display: flex; flex-direction: column; gap: 1rem; }

.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.header-left { display: flex; align-items: center; gap: 0.75rem; }
h1 { font-size: 1.25rem; font-weight: 600; }
h2 { font-size: 1rem; font-weight: 600; margin-bottom: 0.75rem; color: #8b949e; }

.meta {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem 1.25rem;
  font-size: 0.85rem;
  background: #161b22;
  border: 1px solid #30363d;
  border-radius: 6px;
  padding: 0.75rem 1rem;
}

.failure { font-family: monospace; font-size: 0.85rem; }

.inspector__body { display: flex; flex-direction: column; gap: 1.5rem; }

.graph-section { display: flex; flex-direction: column; }
.graph-section .def-graph { height: 360px; }

.timeline-section { flex: 1; }
</style>
