<template>
  <div class="editor-page">
    <header class="page-header">
      <h1>{{ isNew ? 'New Definition' : 'Edit Definition' }}</h1>
      <div class="header-actions">
        <RouterLink to="/definitions" class="btn">Back</RouterLink>
        <button class="btn btn--primary" @click="saveToApi" :disabled="saving">
          {{ saving ? 'Saving…' : 'Save to API' }}
        </button>
      </div>
    </header>

    <div v-if="apiError" class="alert alert--error">{{ apiError }}</div>
    <div v-if="saveSuccess" class="alert alert--success">Saved successfully.</div>

    <!-- The definition editor iframe keeps all its original JS intact -->
    <iframe
      ref="editorFrame"
      :src="editorUrl"
      class="editor-iframe"
      @load="onFrameLoad"
    />
  </div>
</template>

<script setup>
import { ref, computed, onMounted } from 'vue'
import { useRoute } from 'vue-router'
import { api } from '../api/index.js'

const route = useRoute()
const editorFrame = ref(null)
const saving = ref(false)
const apiError = ref(null)
const saveSuccess = ref(false)

const isNew = computed(() => !route.params.id)

// Point to the definition-editor served from the same origin.
// In dev the BFF proxy isn't needed for the editor files — Vite serves them.
const editorUrl = computed(() => {
  const base = import.meta.env.DEV
    ? '/definition-editor/index.html'
    : '/definition-editor/index.html'
  if (!isNew.value) return `${base}?definitionId=${route.params.id}`
  return base
})

function onFrameLoad() {
  if (!isNew.value && editorFrame.value) {
    loadDefinitionIntoEditor()
  }
}

async function loadDefinitionIntoEditor() {
  try {
    const def = await api.getDefinition(route.params.id)
    if (!def) return
    const win = editorFrame.value?.contentWindow
    if (win?.loadDefinition) {
      win.loadDefinition(def.definition)
    }
  } catch (e) {
    apiError.value = e.message
  }
}

async function saveToApi() {
  const win = editorFrame.value?.contentWindow
  if (!win?.exportDefinition) {
    apiError.value = 'Editor not ready — try again in a moment.'
    return
  }
  const def = win.exportDefinition()
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
    setTimeout(() => { saveSuccess.value = false }, 3000)
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
}
.page-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 1rem;
  flex-shrink: 0;
}
h1 { font-size: 1.5rem; font-weight: 600; }
.header-actions { display: flex; gap: 0.5rem; }
.editor-iframe {
  flex: 1;
  border: 1px solid #30363d;
  border-radius: 6px;
  background: #0d1117;
  min-height: 0;
}
</style>
