<template>
  <div>
    <header class="page-header">
      <h1>Executions</h1>
      <button class="btn" @click="load" :disabled="loading">Refresh</button>
    </header>

    <!-- Filters -->
    <div class="filters">
      <input v-model="filterName" class="input" placeholder="Filter by name…" @keydown.enter="load" />
      <select v-model="filterStatus" class="select">
        <option value="">All statuses</option>
        <option>IN_PROGRESS</option>
        <option>WAITING_CALLBACK</option>
        <option>SLEEPING</option>
        <option>SUCCEEDED</option>
        <option>FAILED</option>
        <option>CORRUPTED</option>
      </select>
      <input v-model="searchId" class="input" placeholder="Jump to execution ID…" @keydown.enter="jumpToId" />
      <button class="btn btn--primary" @click="load">Search</button>
    </div>

    <div v-if="error" class="alert alert--error">{{ error }}</div>
    <div v-if="loading" class="loading">Loading…</div>

    <table v-else-if="executions.length" class="table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Name</th>
          <th>Version</th>
          <th>Status</th>
          <th>Started</th>
          <th>Duration</th>
        </tr>
      </thead>
      <tbody>
        <tr
          v-for="ex in executions"
          :key="ex.id"
          class="clickable"
          @click="$router.push(`/executions/${ex.id}`)"
        >
          <td class="mono dim">{{ ex.id.slice(0, 8) }}…</td>
          <td class="mono">{{ ex.name }}</td>
          <td class="mono dim">{{ ex.version }}</td>
          <td><StatusBadge :status="ex.status" /></td>
          <td class="dim">{{ fmtDate(ex.startedAt) }}</td>
          <td class="dim">{{ duration(ex.startedAt, ex.completedAt) }}</td>
        </tr>
      </tbody>
    </table>

    <p v-else-if="!loading" class="empty">No executions found.</p>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import StatusBadge from '../components/StatusBadge.vue'
import { api } from '../api/index.js'

const router = useRouter()
const executions = ref([])
const loading = ref(false)
const error = ref(null)
const filterName = ref('')
const filterStatus = ref('')
const searchId = ref('')

onMounted(load)

async function load() {
  loading.value = true
  error.value = null
  try {
    const params = {}
    if (filterName.value) params.name = filterName.value
    if (filterStatus.value) params.status = filterStatus.value
    executions.value = await api.listExecutions(params) ?? []
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

function jumpToId() {
  const id = searchId.value.trim()
  if (id) router.push(`/executions/${id}`)
}

function fmtDate(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
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
.page-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 1.5rem; }
h1 { font-size: 1.5rem; font-weight: 600; }
.filters { display: flex; gap: 0.5rem; margin-bottom: 1.25rem; flex-wrap: wrap; }
</style>
