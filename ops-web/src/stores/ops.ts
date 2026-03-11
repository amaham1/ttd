import { defineStore } from "pinia";

import type {
  BreakState,
  BrokerSnapshot,
  CandidateDecisionRecord,
  DashboardSummary,
  DisclosureRuleDefinition,
  ExecutionPlan,
  LiveControlState,
  MarketIntelSnapshot,
  PositionState,
  ReplaySnapshot,
  ResearchReportSnapshot,
  ReplayJobState,
  RiskFlagState,
  ShadowLiveMetricsSummary,
  ShadowLiveSnapshot,
  ShadowLoopSnapshot,
  StrategyState,
  TradingSnapshot,
} from "../types";

function defaultSummary(): DashboardSummary {
  return {
    mode: { mode: "NORMAL", updated_at_utc: new Date().toISOString() },
    strategy_enabled_count: 0,
    blocked_symbol_count: 0,
    open_break_count: 0,
    replay_job_count: 0,
    risk_flag_count: 0,
    active_position_count: 0,
  };
}

function defaultLiveControls(): LiveControlState {
  return {
    max_order_value_krw: 500000,
    auto_loop_interval_seconds: 60,
    autonomous_loop_enabled: false,
    updated_at_utc: new Date().toISOString(),
  };
}

async function getJson<T>(url: string, fallback: T): Promise<T> {
  try {
    const response = await fetch(url);
    if (!response.ok) {
      throw new Error(`요청 실패 (${response.status})`);
    }
    return (await response.json()) as T;
  } catch {
    return fallback;
  }
}

function translateErrorMessage(message: string): string {
  const mapping: Record<string, string> = {
    "Failed to update live controls.": "실거래 제어값 저장에 실패했습니다.",
    "Failed to start autonomous loop.": "자동 루프 시작에 실패했습니다.",
    "Failed to stop autonomous loop.": "자동 루프 정지에 실패했습니다.",
  };
  return mapping[message] ?? message.replace("request failed", "요청 실패");
}

async function readErrorMessage(response: Response): Promise<string> {
  try {
    const errorBody = (await response.json()) as { detail?: string };
    if (errorBody?.detail) {
      return translateErrorMessage(errorBody.detail);
    }
  } catch {
    // Ignore JSON parsing failures and fall back to status text.
  }
  return translateErrorMessage(response.statusText || `요청 실패 (${response.status})`);
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
    brokerSnapshot: null as BrokerSnapshot | null,
    tradingSnapshot: null as TradingSnapshot | null,
    marketSnapshot: null as MarketIntelSnapshot | null,
    replaySnapshot: null as ReplaySnapshot | null,
    shadowSnapshot: null as ShadowLiveSnapshot | null,
    shadowLoopSnapshot: null as ShadowLoopSnapshot | null,
    candidateDecisions: [] as CandidateDecisionRecord[],
    disclosureRules: [] as DisclosureRuleDefinition[],
    latestPlan: null as ExecutionPlan | null,
    researchReport: null as ResearchReportSnapshot | null,
    shadowMetricsSummary: null as ShadowLiveMetricsSummary | null,
    liveControls: null as LiveControlState | null,
    loopActionPending: false,
    loopActionError: null as string | null,
  }),
  actions: {
    async refreshAll() {
      this.loading = true;
      try {
        const [
          summary,
          strategies,
          breaks,
          positions,
          riskFlags,
          replayJobs,
          brokerSnapshot,
          tradingSnapshot,
          marketSnapshot,
          replaySnapshot,
          shadowSnapshot,
          shadowLoopSnapshot,
          candidateDecisions,
          disclosureRules,
          latestPlan,
          liveControls,
        ] = await Promise.all([
          getJson<DashboardSummary>("/ops/summary", this.summary ?? defaultSummary()),
          getJson<StrategyState[]>("/ops/strategies", [...this.strategies]),
          getJson<BreakState[]>("/ops/reconciliation-breaks", [...this.breaks]),
          getJson<PositionState[]>("/ops/positions", [...this.positions]),
          getJson<RiskFlagState[]>("/ops/risk-flags", [...this.riskFlags]),
          getJson<ReplayJobState[]>("/ops/replay-jobs", [...this.replayJobs]),
          getJson<BrokerSnapshot | null>("/svc/broker/snapshot", this.brokerSnapshot),
          getJson<TradingSnapshot | null>("/svc/trading/snapshot", this.tradingSnapshot),
          getJson<MarketIntelSnapshot | null>("/svc/market/snapshot", this.marketSnapshot),
          getJson<ReplaySnapshot | null>("/svc/replay/snapshot", this.replaySnapshot),
          getJson<ShadowLiveSnapshot | null>("/svc/shadow/snapshot", this.shadowSnapshot),
          getJson<ShadowLoopSnapshot | null>("/svc/shadow/loop/status", this.shadowLoopSnapshot),
          getJson<CandidateDecisionRecord[]>("/ops/candidate-decisions", [...this.candidateDecisions]),
          getJson<DisclosureRuleDefinition[]>("/ops/rules/disclosure", [...this.disclosureRules]),
          getJson<ExecutionPlan | null>("/svc/shadow/plan/latest", this.latestPlan),
          getJson<LiveControlState>("/ops/live-controls", this.liveControls ?? defaultLiveControls()),
        ]);

        this.summary = summary;
        this.strategies = strategies;
        this.breaks = breaks;
        this.positions = positions;
        this.riskFlags = riskFlags;
        this.replayJobs = replayJobs;
        this.brokerSnapshot = brokerSnapshot;
        this.tradingSnapshot = tradingSnapshot;
        this.marketSnapshot = marketSnapshot;
        this.replaySnapshot = replaySnapshot;
        this.shadowSnapshot = shadowSnapshot;
        this.shadowLoopSnapshot = shadowLoopSnapshot;
        this.candidateDecisions = candidateDecisions;
        this.disclosureRules = disclosureRules;
        this.latestPlan = latestPlan;
        this.liveControls = liveControls;
      } finally {
        this.loading = false;
      }
    },
    async refreshResearch() {
      this.loading = true;
      try {
        const [researchReport, shadowMetricsSummary] = await Promise.all([
          getJson<ResearchReportSnapshot | null>(
            "/svc/data/reports/research",
            this.researchReport,
          ),
          getJson<ShadowLiveMetricsSummary | null>(
            "/svc/data/runtime/shadow-live/summary",
            this.shadowMetricsSummary,
          ),
        ]);
        this.researchReport = researchReport;
        this.shadowMetricsSummary = shadowMetricsSummary;
      } finally {
        this.loading = false;
      }
    },
    async queueReplayJob() {
      const today = new Date().toISOString().slice(0, 10);
      await fetch("/ops/replay-jobs", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ trading_date: today, scenario: "수동-콘솔" }),
      });
      await this.refreshAll();
    },
    async runShadowSample() {
      const response = await fetch("/svc/shadow/run/sample", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ execute_live: false, persist: true }),
      });
      if (response.ok) {
        this.latestPlan = (await response.json()) as ExecutionPlan;
      }
      await this.refreshAll();
    },
    async updateLiveControls(controls: {
      maxOrderValueKrW: number;
      autoLoopIntervalSeconds: number;
    }) {
      this.loopActionPending = true;
      this.loopActionError = null;
      try {
        const response = await fetch("/ops/live-controls", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            max_order_value_krw: Math.max(Math.trunc(controls.maxOrderValueKrW), 1),
            auto_loop_interval_seconds: Math.max(
              Math.trunc(controls.autoLoopIntervalSeconds),
              1,
            ),
            actor: "ops-console",
            reason_code: "DASHBOARD_UPDATE",
          }),
        });
        if (!response.ok) {
          throw new Error(await readErrorMessage(response));
        }
        this.liveControls = (await response.json()) as LiveControlState;
      } catch (error) {
        this.loopActionError =
          error instanceof Error ? translateErrorMessage(error.message) : "실거래 제어값 저장에 실패했습니다.";
        throw error;
      } finally {
        this.loopActionPending = false;
      }
    },
    async startAutonomousLoop(options?: {
      intervalSeconds?: number;
      maxOrderValueKrW?: number;
    }) {
      this.loopActionPending = true;
      this.loopActionError = null;
      try {
        const intervalSeconds = Math.max(
          Math.trunc(
            options?.intervalSeconds ??
              this.liveControls?.auto_loop_interval_seconds ??
              60,
          ),
          1,
        );
        const maxOrderValueKrW = Math.max(
          Math.trunc(
            options?.maxOrderValueKrW ??
              this.liveControls?.max_order_value_krw ??
              500000,
          ),
          1,
        );
        const response = await fetch("/ops/live-loop/start", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            interval_seconds: intervalSeconds,
            max_order_value_krw: maxOrderValueKrW,
            actor: "ops-console",
            reason_code: "DASHBOARD_START",
          }),
        });
        if (!response.ok) {
          throw new Error(await readErrorMessage(response));
        }
        const startResult = (await response.json()) as {
          live_control: LiveControlState;
          loop: ShadowLoopSnapshot;
        };
        this.liveControls = startResult.live_control;
        this.shadowLoopSnapshot = startResult.loop;
        await this.refreshAll();
      } catch (error) {
        this.loopActionError =
          error instanceof Error ? translateErrorMessage(error.message) : "자동 루프 시작에 실패했습니다.";
        throw error;
      } finally {
        this.loopActionPending = false;
      }
    },
    async stopAutonomousLoop() {
      this.loopActionPending = true;
      this.loopActionError = null;
      try {
        const response = await fetch("/ops/live-loop/stop", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            actor: "ops-console",
            reason_code: "DASHBOARD_STOP",
          }),
        });
        if (!response.ok) {
          throw new Error(await readErrorMessage(response));
        }
        const stopResult = (await response.json()) as {
          live_control: LiveControlState;
          loop: ShadowLoopSnapshot;
        };
        this.liveControls = stopResult.live_control;
        this.shadowLoopSnapshot = stopResult.loop;
        await this.refreshAll();
      } catch (error) {
        this.loopActionError =
          error instanceof Error ? translateErrorMessage(error.message) : "자동 루프 정지에 실패했습니다.";
        throw error;
      } finally {
        this.loopActionPending = false;
      }
    },
  },
});
