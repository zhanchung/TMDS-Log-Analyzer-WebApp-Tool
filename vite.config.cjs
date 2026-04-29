const { defineConfig } = require("vite");
const react = require("@vitejs/plugin-react");
const { resolve } = require("node:path");

module.exports = defineConfig({
  root: resolve(__dirname, "app/renderer"),
  plugins: [react()],
  resolve: {
    alias: {
      "@shared": resolve(__dirname, "app/shared"),
      "@app": resolve(__dirname, "app/renderer/src"),
    },
  },
  define: {
    __TMDS_BUILD_VERSION__: JSON.stringify("dev"),
    __TMDS_BUILD_AT__: JSON.stringify(new Date().toISOString()),
  },
  build: {
    outDir: resolve(__dirname, "dist/renderer"),
    emptyOutDir: true,
  },
});
