import { defineConfig } from "vite";
import vue from "@vitejs/plugin-vue";

const opsApiTarget = process.env.OPS_API_PROXY_TARGET ?? "http://localhost:8000";
const brokerGatewayTarget = process.env.BROKER_GATEWAY_PROXY_TARGET ?? "http://localhost:8001";
const tradingCoreTarget = process.env.TRADING_CORE_PROXY_TARGET ?? "http://localhost:8002";
const marketIntelTarget = process.env.MARKET_INTEL_PROXY_TARGET ?? "http://localhost:8003";
const replayRunnerTarget = process.env.REPLAY_RUNNER_PROXY_TARGET ?? "http://localhost:8004";
const shadowLiveTarget = process.env.SHADOW_LIVE_PROXY_TARGET ?? "http://localhost:8005";
const dataIngestTarget = process.env.DATA_INGEST_PROXY_TARGET ?? "http://localhost:8006";

export default defineConfig({
  plugins: [vue()],
  server: {
    port: 5173,
    proxy: {
      "/ops": opsApiTarget,
      "/health": opsApiTarget,
      "/metrics": opsApiTarget,
      "/svc/broker": {
        target: brokerGatewayTarget,
        rewrite: (path) => path.replace(/^\/svc\/broker/, ""),
      },
      "/svc/trading": {
        target: tradingCoreTarget,
        rewrite: (path) => path.replace(/^\/svc\/trading/, ""),
      },
      "/svc/market": {
        target: marketIntelTarget,
        rewrite: (path) => path.replace(/^\/svc\/market/, ""),
      },
      "/svc/replay": {
        target: replayRunnerTarget,
        rewrite: (path) => path.replace(/^\/svc\/replay/, ""),
      },
      "/svc/shadow": {
        target: shadowLiveTarget,
        rewrite: (path) => path.replace(/^\/svc\/shadow/, ""),
      },
      "/svc/data": {
        target: dataIngestTarget,
        rewrite: (path) => path.replace(/^\/svc\/data/, ""),
      },
    },
  },
});
