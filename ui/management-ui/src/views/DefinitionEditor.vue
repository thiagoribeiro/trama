<template>
  <div class="editor-page">

    <!-- Workspace bar — replaces the old page header -->
    <header class="workspace-bar">
      <nav class="breadcrumb">
        <RouterLink to="/definitions" class="bc-back" title="All definitions">←</RouterLink>
        <span class="bc-sep">›</span>
        <span class="bc-name">{{ defName ?? 'New Definition' }}</span>
        <span v-if="defVersion" class="bc-version">{{ defVersion }}</span>
        <span v-if="isNew" class="bc-version bc-version--new">new</span>
      </nav>

      <div class="workspace-actions">
        <!-- Transient feedback -->
        <Transition name="fade">
          <span v-if="saveSuccess" class="ws-saved">✓ Saved</span>
        </Transition>
        <span v-if="apiError" class="ws-error" :title="apiError">⚠ Error</span>

        <button
          v-if="!isNew"
          class="btn btn--ghost"
          @click="reload"
          :disabled="loading"
          title="Reload from server"
        >↺</button>

        <button
          class="btn btn--primary"
          @click="saveToApi"
          :disabled="saving"
        >{{ saving ? 'Saving…' : 'Save' }}</button>
      </div>
    </header>

    <!-- Full-screen error banner (only for load failures) -->
    <div v-if="apiError && !saveSuccess" class="alert alert--error alert--slim">{{ apiError }}</div>

    <DefinitionEditorWidget ref="widget" class="editor-widget-fill" />
  </div>
</template>

<script setup>
import { ref, computed } from 'vue'
import { useRoute } from 'vue-router'
import { onMounted } from 'vue'
import DefinitionEditorWidget from '../components/DefinitionEditorWidget.vue'
import { api } from '../api/index.js'

const route   = useRoute()
const widget  = ref(null)
const saving  = ref(false)
const loading = ref(false)
const apiError    = ref(null)
const saveSuccess = ref(false)
const defName     = ref(null)
const defVersion  = ref(null)

const isNew = computed(() => !route.params.id)

onMounted(async () => {
  if (!isNew.value) await loadDefinition()
})

async function loadDefinition() {
  loading.value = true
  apiError.value = null
  try {
    const def = await api.getDefinition(route.params.id)
    if (!def) return
    defName.value    = def.name
    defVersion.value = def.version
    requestAnimationFrame(() => widget.value?.load(def.definition))
  } catch (e) {
    apiError.value = e.message
  } finally {
    loading.value = false
  }
}

async function reload() {
  if (isNew.value) return
  await loadDefinition()
}

async function saveToApi() {
  const def = widget.value?.getDefinition()
  if (!def) {
    apiError.value = 'Nothing to save — build a definition first.'
    return
  }
  saving.value = true
  apiError.value = null
  saveSuccess.value = false
  try {
    if (isNew.value) {
      await api.createDefinition(def)
    } else {
      await api.updateDefinition(route.params.id, def)
    }
    saveSuccess.value = true
    setTimeout(() => { saveSuccess.value = false }, 4000)
  } catch (e) {
    apiError.value = e.message
  } finally {
    saving.value = false
  }
}
</script>

<style scoped>
.editor-page {
  display: flex;
  flex-direction: column;
  height: 100%;
  gap: 0;
}

/* ── Workspace bar ──────────────────────────────────────────────────────────── */
.workspace-bar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0 1rem;
  height: 44px;
  flex-shrink: 0;
  background: #161b22;
  border-bottom: 1px solid #21262d;
}

.breadcrumb {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.85rem;
  min-width: 0;
}

.bc-back {
  color: #8b949e;
  text-decoration: none;
  font-size: 1rem;
  line-height: 1;
  padding: 2px 4px;
  border-radius: 4px;
  transition: color 0.12s;
}
.bc-back:hover { color: #e2e8f0; }

.bc-sep { color: #30363d; }

.bc-name {
  font-weight: 600;
  color: #e2e8f0;
  font-size: 0.9rem;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  max-width: 280px;
}

.bc-version {
  font-size: 0.72rem;
  font-family: ui-monospace, monospace;
  background: #1f2d3d;
  color: #58a6ff;
  border: 1px solid #1f4068;
  padding: 1px 6px;
  border-radius: 10px;
  flex-shrink: 0;
}

.bc-version--new {
  background: #1a3521;
  color: #3fb950;
  border-color: #1a4731;
}

/* ── Actions ────────────────────────────────────────────────────────────────── */
.workspace-actions {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  flex-shrink: 0;
}

.ws-saved {
  font-size: 0.78rem;
  color: #3fb950;
  font-weight: 500;
}

.ws-error {
  font-size: 0.78rem;
  color: #f85149;
  cursor: help;
}

.btn--ghost {
  background: transparent;
  border-color: transparent;
  color: #8b949e;
  padding: 0.3rem 0.5rem;
  font-size: 1rem;
}
.btn--ghost:hover {
  color: #e2e8f0;
  background: #21262d;
  border-color: #30363d;
}
.btn--ghost:disabled { opacity: 0.4; cursor: default; }

/* ── Alert slim ─────────────────────────────────────────────────────────────── */
.alert--slim {
  padding: 0.4rem 1rem;
  border-radius: 0;
  border-left: none;
  border-right: none;
  border-top: none;
  font-size: 0.8rem;
  flex-shrink: 0;
}

/* ── Canvas fill ─────────────────────────────────────────────────────────────── */
.editor-widget-fill {
  flex: 1;
  min-height: 0;
  overflow: hidden;
}

/* ── Fade transition for "Saved" ────────────────────────────────────────────── */
.fade-enter-active, .fade-leave-active { transition: opacity 0.4s; }
.fade-enter-from, .fade-leave-to { opacity: 0; }
</style>
