import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { createReadStream, existsSync } from 'fs'
import { resolve, extname } from 'path'

const EDITOR_DIR = resolve(__dirname, '../definition-editor')
const MIME = { '.html': 'text/html', '.css': 'text/css', '.js': 'application/javascript' }

export default defineConfig({
  plugins: [
    vue(),
    {
      name: 'serve-definition-editor',
      configureServer(server) {
        server.middlewares.use((req, res, next) => {
          const PREFIX = '/definition-editor'
          if (!req.url?.startsWith(PREFIX)) return next()
          const sub = req.url.slice(PREFIX.length).split('?')[0] || '/index.html'
          const file = resolve(EDITOR_DIR, sub.replace(/^\//, ''))
          if (!existsSync(file)) return next()
          res.writeHead(200, { 'Content-Type': MIME[extname(file)] ?? 'text/plain' })
          createReadStream(file).pipe(res)
        })
      },
    },
  ],
  server: {
    proxy: {
      '/api': {
        target: 'http://localhost:9000',
        changeOrigin: true,
      },
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
  },
})
