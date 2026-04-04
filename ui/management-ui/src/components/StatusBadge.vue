<template>
  <span class="badge" :class="cls">{{ status }}</span>
</template>

<script setup>
const props = defineProps({ status: String })

const cls = {
  SUCCEEDED: 'badge--green',
  COMPLETED: 'badge--green',
  FAILED: 'badge--red',
  IN_PROGRESS: 'badge--blue',
  WAITING_CALLBACK: 'badge--amber',
  CORRUPTED: 'badge--red',
}[props.status] ?? 'badge--grey'
</script>

<style scoped>
.badge {
  display: inline-block;
  padding: 0.15em 0.55em;
  border-radius: 1em;
  font-size: 0.75rem;
  font-weight: 600;
  letter-spacing: 0.03em;
}
.badge--green  { background: #1a4731; color: #3fb950; }
.badge--red    { background: #4a1515; color: #f85149; }
.badge--blue   { background: #1a3a5c; color: #58a6ff; }
.badge--amber  { background: #4a3000; color: #e3b341; }
.badge--grey   { background: #21262d; color: #8b949e; }
</style>
