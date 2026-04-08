<template>
  <div>
    <header class="page-header">
      <h1>Definitions</h1>
      <RouterLink to="/definitions/new" class="btn btn--primary">+ New Definition</RouterLink>
    </header>

    <div v-if="error" class="alert alert--error">{{ error }}</div>

    <div v-if="loading" class="loading">Loading…</div>

    <table v-else-if="definitions.length" class="table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Version</th>
          <th>Updated</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="def in definitions" :key="def.id">
          <td class="mono">{{ def.name }}</td>
          <td class="mono">{{ def.version }}</td>
          <td class="dim">{{ fmtDate(def.updatedAt) }}</td>
          <td class="actions">
            <RouterLink :to="`/definitions/${def.id}`" class="btn btn--sm">Edit</RouterLink>
            <button class="btn btn--sm btn--run" @click="openRunModal(def)">Run</button>
            <button class="btn btn--sm btn--danger" @click="confirmDelete(def)">Delete</button>
          </td>
        </tr>
      </tbody>
    </table>

    <p v-else class="empty">No definitions found. Create one to get started.</p>

    <!-- Delete confirm dialog -->
    <dialog ref="deleteDialog" class="dialog">
      <div class="dialog__body">
        <p>Delete <strong class="mono">{{ pending?.name }} {{ pending?.version }}</strong>?</p>
        <p class="dim">This cannot be undone.</p>
        <div class="dialog__actions">
          <button class="btn" @click="deleteDialog.close()">Cancel</button>
          <button class="btn btn--danger" @click="doDelete">Delete</button>
        </div>
      </div>
    </dialog>

    <!-- Run dialog -->
    <dialog ref="runDialog" class="dialog">
      <div class="dialog__body">
        <p>Run <strong class="mono">{{ pending?.name }} {{ pending?.version }}</strong></p>
        <label class="field-label">Payload JSON</label>
        <textarea v-model="runPayload" class="textarea" rows="6" placeholder="{}"></textarea>
        <div v-if="runError" class="alert alert--error">{{ runError }}</div>
        <div class="dialog__actions">
          <button class="btn" @click="runDialog.close()">Cancel</button>
          <button class="btn btn--primary" @click="doRun" :disabled="running">
            {{ running ? 'Running…' : 'Run' }}
          </button>
        </div>
      </div>
    </dialog>
  </div>
</template>

<script setup>
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { api } from '../api/index.js'

const router = useRouter()
const definitions = ref([])
const loading = ref(true)
const error = ref(null)
const pending = ref(null)
const deleteDialog = ref(null)
const runDialog = ref(null)
const runPayload = ref('{}')
const runError = ref(null)
const running = ref(false)

onMounted(load)

async function load() {
  loading.value = true
  error.value = null
  try {
    definitions.value = await api.listDefinitions() ?? []
  } catch (e) {
    error.value = e.message
  } finally {
    loading.value = false
  }
}

function confirmDelete(def) {
  pending.value = def
  deleteDialog.value.showModal()
}

async function doDelete() {
  try {
    await api.deleteDefinition(pending.value.id)
    deleteDialog.value.close()
    await load()
  } catch (e) {
    error.value = e.message
    deleteDialog.value.close()
  }
}

function openRunModal(def) {
  pending.value = def
  runPayload.value = '{}'
  runError.value = null
  runDialog.value.showModal()
}

async function doRun() {
  runError.value = null
  let payload
  try {
    payload = JSON.parse(runPayload.value)
  } catch {
    runError.value = 'Invalid JSON payload'
    return
  }
  running.value = true
  try {
    const res = await api.runDefinition(pending.value.name, pending.value.version, payload)
    runDialog.value.close()
    router.push(`/executions/${res.id}`)
  } catch (e) {
    runError.value = e.message
  } finally {
    running.value = false
  }
}

function fmtDate(iso) {
  if (!iso) return '—'
  return new Date(iso).toLocaleString()
}
</script>

<style scoped>
.page-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 1.5rem; }
h1 { font-size: 1.5rem; font-weight: 600; }
</style>
