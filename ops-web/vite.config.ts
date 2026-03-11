import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    proxy: {
      "/ops": "http://localhost:8000",
      "/health": "http://localhost:8000",
      "/metrics": "http://localhost:8000",
      "/svc/broker": {
        target: "http://localhost:8001",
        rewrite: (path) => path.replace(/^\/svc\/broker/, ""),
      },
      "/svc/trading": {
        target: "http://localhost:8002",
        rewrite: (path) => path.replace(/^\/svc\/trading/, ""),
      },
      "/svc/market": {
        target: "http://localhost:8003",
        rewrite: (path) => path.replace(/^\/svc\/market/, ""),
      },
      "/svc/replay": {
        target: "http://localhost:8004",
        rewrite: (path) => path.replace(/^\/svc\/replay/, ""),
      },
      "/svc/shadow": {
        target: "http://localhost:8005",
        rewrite: (path) => path.replace(/^\/svc\/shadow/, ""),
      },
    },
  },
});
