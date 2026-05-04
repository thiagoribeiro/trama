import { createRouter, createWebHashHistory } from 'vue-router'
import DefinitionsList from '../views/DefinitionsList.vue'
import DefinitionEditor from '../views/DefinitionEditor.vue'
import ExecutionsList from '../views/ExecutionsList.vue'
import ExecutionInspector from '../views/ExecutionInspector.vue'

export default createRouter({
  history: createWebHashHistory(),
  routes: [
    { path: '/', redirect: '/definitions' },
    { path: '/definitions', component: DefinitionsList },
    { path: '/definitions/new', component: DefinitionEditor },
    { path: '/definitions/:id', component: DefinitionEditor },
    { path: '/executions', component: ExecutionsList },
    { path: '/executions/:id', component: ExecutionInspector },
  ],
})
