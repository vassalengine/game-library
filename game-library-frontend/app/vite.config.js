import { defineConfig } from 'vitest/config';
import { svelte } from '@sveltejs/vite-plugin-svelte'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [svelte()],
  base: '/library',
  build: {
    sourcemap: true,
    rollupOptions: {
      input: {
        root: './root.html',
        new: './new.html',
        projects: './projects.html',
        project: './project.html',
        publishers: './publishers.html',
        search: './search.html',
        tags: './tags.html',
        flags: './flags.html'
      }
    }
  },
  resolve: process.env.VITEST ? {
    conditions: ['browser']
  } : undefined
});
