<template>
  <div class="layout">
    <nav class="sidebar">
      <div class="logo">trama</div>
      <RouterLink to="/definitions" class="nav-link">
        <span class="icon">&#9649;</span> Definitions
      </RouterLink>
      <RouterLink to="/executions" class="nav-link">
        <span class="icon">&#9654;</span> Executions
      </RouterLink>
    </nav>
    <main class="content">
      <RouterView />
    </main>
  </div>
</template>

<style>
.layout {
  display: flex;
  height: 100vh;
  overflow: hidden;
}

.sidebar {
  width: 200px;
  flex-shrink: 0;
  background: #161b22;
  border-right: 1px solid #30363d;
  display: flex;
  flex-direction: column;
  padding: 1rem 0;
}

.logo {
  font-size: 1.25rem;
  font-weight: 700;
  letter-spacing: 0.1em;
  color: #58a6ff;
  padding: 0 1.25rem 1.5rem;
  border-bottom: 1px solid #30363d;
  margin-bottom: 1rem;
}

.nav-link {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.6rem 1.25rem;
  color: #8b949e;
  text-decoration: none;
  font-size: 0.9rem;
  transition: background 0.15s, color 0.15s;
}

.nav-link:hover,
.nav-link.router-link-active {
  background: #21262d;
  color: #e2e8f0;
}

.icon { font-style: normal; font-size: 0.75rem; }

.content {
  flex: 1;
  overflow-y: auto;
  padding: 1.5rem;
}
</style>
