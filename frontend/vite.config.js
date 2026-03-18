import { defineConfig, loadEnv } from 'vite'
import react from '@vitejs/plugin-react'
import { fileURLToPath, URL } from 'node:url'

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, fileURLToPath(new URL('.', import.meta.url)), '')
  return {
    plugins: [react()],
    define: {
      'import.meta.env.BACKEND_SERVICE_URL': JSON.stringify(env.BACKEND_SERVICE_URL || '')
    }
  }
})
