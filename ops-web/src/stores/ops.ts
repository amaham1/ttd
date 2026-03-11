import { defineStore } from "pinia";

import type {
  BreakState,
  BrokerSnapshot,
  DashboardSummary,
  MarketIntelSnapshot,
  OrderTrace,
  PositionState,
  ReplaySnapshot,
  ReplayJobState,
  RiskFlagState,
  ShadowLiveSnapshot,
  ShadowLoopSnapshot,
  StrategyState,
  TradingSnapshot,
} from "../types";

async function getJson<T>(url: string, fallback: T): Promise<T> {
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`request failed: ${response.status}`);
    }
    return (await response.json()) as T;
  } catch {
    return fallback;
  }
}

export const useOpsStore = defineStore("ops", {
  state: () => ({
    loading: false,
    summary: null as DashboardSummary | null,
    strategies: [] as StrategyState[],
    breaks: [] as BreakState[],
    positions: [] as PositionState[],
    riskFlags: [] as RiskFlagState[],
    replayJobs: [] as ReplayJobState[],
    orderDemo: null as OrderTrace | null,
    brokerSnapshot: null as BrokerSnapshot | null,
    tradingSnapshot: null as TradingSnapshot | null,
    marketSnapshot: null as MarketIntelSnapshot | null,
    replaySnapshot: null as ReplaySnapshot | null,
    shadowSnapshot: null as ShadowLiveSnapshot | null,
    shadowLoopSnapshot: null as ShadowLoopSnapshot | null,
  }),
  actions: {
    async refreshAll() {
      this.loading = true;
      const [
        summary,
        strategies,
        breaks,
        positions,
        riskFlags,
        replayJobs,
        orderDemo,
        brokerSnapshot,
        tradingSnapshot,
        marketSnapshot,
        replaySnapshot,
        shadowSnapshot,
        shadowLoopSnapshot,
      ] =
        await Promise.all([
          getJson<DashboardSummary>("/ops/summary", {
            mode: { mode: "NORMAL", updated_at_utc: new Date().toISOString() },
            strategy_enabled_count: 2,
            blocked_symbol_count: 0,
            open_break_count: 1,
            replay_job_count: 1,
            risk_flag_count: 2,
            active_position_count: 2,
          }),
          getJson<StrategyState[]>("/ops/strategies", []),
          getJson<BreakState[]>("/ops/reconciliation-breaks", []),
          getJson<PositionState[]>("/ops/positions", []),
          getJson<RiskFlagState[]>("/ops/risk-flags", []),
          getJson<ReplayJobState[]>("/ops/replay-jobs", []),
          getJson<OrderTrace | null>("/ops/orders/order-demo", null),
          getJson<BrokerSnapshot | null>("/svc/broker/snapshot", null),
          getJson<TradingSnapshot | null>("/svc/trading/snapshot", null),
          getJson<MarketIntelSnapshot | null>("/svc/market/snapshot", null),
          getJson<ReplaySnapshot | null>("/svc/replay/snapshot", null),
          getJson<ShadowLiveSnapshot | null>("/svc/shadow/snapshot", null),
          getJson<ShadowLoopSnapshot | null>("/svc/shadow/loop/status", null),
        ]);

      this.summary = summary;
      this.strategies = strategies;
      this.breaks = breaks;
      this.positions = positions;
      this.riskFlags = riskFlags;
      this.replayJobs = replayJobs;
      this.orderDemo = orderDemo;
      this.brokerSnapshot = brokerSnapshot;
      this.tradingSnapshot = tradingSnapshot;
      this.marketSnapshot = marketSnapshot;
      this.replaySnapshot = replaySnapshot;
      this.shadowSnapshot = shadowSnapshot;
      this.shadowLoopSnapshot = shadowLoopSnapshot;
      this.loading = false;
    },
    async queueReplayJob() {
      const today = new Date().toISOString().slice(0, 10);
      await fetch("/ops/replay-jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ trading_date: today, scenario: "manual-console" }),
      });
      await this.refreshAll();
    },
  },
});
