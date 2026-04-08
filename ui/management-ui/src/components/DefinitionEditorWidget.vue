<template>
  <div class="editor-widget" ref="editorRoot">

    <!-- ── Toolbar ────────────────────────────────────────────────────────── -->
    <header id="toolbar">
      <button class="btn btn--sm" id="btn-import" title="Import JSON definition">↑ Import</button>
      <button class="btn btn--sm" id="btn-export" title="Export JSON definition">↓ Export</button>

      <div class="templates-wrap">
        <button class="btn btn--sm" id="btn-templates" title="Start from a template">Templates ▾</button>
        <div id="templates-menu" class="templates-menu hidden">
          <div class="templates-menu-item" data-template="simple-chain">
            <strong>Simple Chain</strong>
            <span>3 tasks in sequence</span>
          </div>
          <div class="templates-menu-item" data-template="payment-split">
            <strong>Payment Split</strong>
            <span>validate → switch (pix / card) → notify</span>
          </div>
          <div class="templates-menu-item" data-template="async-callback">
            <strong>Async + Callback</strong>
            <span>async trigger with success/failure conditions</span>
          </div>
        </div>
      </div>

      <div class="sep"></div>

      <button class="btn btn--sm" id="btn-layout" title="Auto-layout graph">⊞ Layout</button>
      <button class="btn btn--icon" id="btn-zoom-in"  title="Zoom in">+</button>
      <button class="btn btn--icon" id="btn-zoom-out" title="Zoom out">−</button>
      <button class="btn btn--icon" id="btn-zoom-fit" title="Fit to screen">⊡</button>

      <div class="spacer"></div>

      <button class="btn btn--sm" id="btn-settings" title="Saga global settings">⚙ Settings</button>
    </header>

    <!-- ── Workspace ─────────────────────────────────────────────────────── -->
    <div id="workspace">

      <!-- Palette -->
      <aside id="palette">
        <div class="palette-label">Add Node</div>
        <div class="palette-item" data-kind="task" draggable="true" title="HTTP task node — drag onto canvas">
          <span class="pi-icon">▭</span> Task
        </div>
        <div class="palette-item" data-kind="switch" draggable="true" title="Conditional switch node — drag onto canvas">
          <span class="pi-icon">◇</span> Switch
        </div>

        <div class="palette-divider"></div>

        <div class="palette-label">Node States</div>
        <div class="state-item">
          <span class="state-swatch state-swatch--amber">⚠</span>
          <span>Has issues</span>
        </div>
        <div class="state-item">
          <span class="state-swatch state-swatch--red">↺</span>
          <span>In a cycle</span>
        </div>
        <div class="state-item">
          <span class="state-swatch state-swatch--green">END</span>
          <span>Terminal</span>
        </div>
      </aside>

      <!-- Canvas -->
      <div id="canvas-wrap">
        <svg id="canvas" xmlns="http://www.w3.org/2000/svg"></svg>
        <div id="canvas-empty-hint">Drag a node from the palette, or pick a template</div>
      </div>

      <!-- Properties panel -->
      <aside id="props-panel" class="hidden"></aside>

      <!-- Global settings panel -->
      <aside id="global-panel" class="hidden"></aside>

    </div><!-- #workspace -->

    <!-- ── Export dialog ──────────────────────────────────────────────────── -->
    <dialog id="export-dialog">
      <div class="dialog-header">
        <span class="dialog-title">Export Definition JSON</span>
        <button class="btn btn--icon" id="export-close">×</button>
      </div>
      <div class="dialog-body">
        <div id="export-issues"></div>
        <textarea id="export-content" readonly spellcheck="false"></textarea>
      </div>
      <div class="dialog-footer">
        <button class="btn" id="export-copy">Copy</button>
        <button class="btn btn--primary" id="export-download">Download</button>
      </div>
    </dialog>

    <!-- ── Import dialog ──────────────────────────────────────────────────── -->
    <dialog id="import-dialog">
      <div class="dialog-header">
        <span class="dialog-title">Import Definition JSON</span>
        <button class="btn btn--icon" id="import-close">×</button>
      </div>
      <div class="dialog-body">
        <div class="import-file-row">
          <span class="import-file-label">Load from file:</span>
          <input type="file" id="import-file" accept=".json,application/json" />
        </div>
        <textarea id="import-content" placeholder="…or paste JSON here" spellcheck="false"></textarea>
        <div id="import-error"></div>
        <div id="import-diff"></div>
      </div>
      <div class="dialog-footer">
        <button class="btn" id="import-close-cancel">Cancel</button>
        <button class="btn btn--primary" id="import-apply" disabled>Import</button>
      </div>
    </dialog>

  </div><!-- .editor-widget -->
</template>

<script setup>
import { ref, onMounted, onUnmounted } from 'vue'
import { mount } from '@editor/js/main.js'

// Make json-logic-js available as window.jsonLogic for the condition test sandbox
import jsonLogic from 'json-logic-js'
if (!window.jsonLogic) window.jsonLogic = jsonLogic

const editorRoot = ref(null)
let editorApi = null

onMounted(() => {
  // Wire the canvas-empty-hint visibility toggle
  const hint = editorRoot.value.querySelector('#canvas-empty-hint')
  editorRoot.value.querySelector('#canvas').addEventListener('DOMSubtreeModified', () => {
    hint.style.display = editorRoot.value.querySelector('#canvas [data-id]') ? 'none' : ''
  }, { passive: true })

  // Mount editor into our root element
  editorApi = mount(editorRoot.value)
})

onUnmounted(() => {
  editorApi?.destroy()
})

/** Load a definition object into the editor, replacing current state */
function load(def) {
  editorApi?.load(def)
}

/** Return the current definition as a plain object, or null if not ready */
function getDefinition() {
  return editorApi?.getDefinition() ?? null
}

defineExpose({ load, getDefinition })
</script>

<style>
/* Import the editor's own styles globally (not scoped, to reach SVG internals) */
@import url('../../../definition-editor/css/editor.css');
</style>

<style scoped>
.editor-widget {
  display: flex;
  flex-direction: column;
  height: 100%;
  overflow: hidden;
}
</style>
