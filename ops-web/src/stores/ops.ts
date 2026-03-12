import { defineStore } from "pinia";

import type {
  BreakState,
  BrokerExecutionFill,
  BrokerOmsOrder,
  BrokerOrderNotice,
  BrokerSnapshot,
  CandidateDecisionRecord,
  ControlPlaneAuditEvent,
  DashboardSummary,
  DisclosureRuleDefinition,
  ExecutionPlan,
  InstrumentLookupEntry,
  LiveControlState,
  MarketIntelSnapshot,
  MarketPipelineDiagnostics,
  PortfolioDiagnostics,
  PositionState,
  ReplaySnapshot,
  ResearchReportSnapshot,
  ReplayJobState,
  RiskFlagState,
  SelectorDiagnostics,
  ShadowLiveMetricsSummary,
  ShadowLiveSnapshot,
  ShadowLoopSnapshot,
  StrategyState,
  TradingSnapshot,
} from "../types";
import {
  formatInstrumentLabel,
  translateReasonText,
} from "../utils/display";

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
    "Failed to resolve reconciliation break.": "정합성 브레이크 해소에 실패했습니다.",
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
    dashboardPending: false,
    summary: null as DashboardSummary | null,
    strategies: [] as StrategyState[],
    breaks: [] as BreakState[],
    positions: [] as PositionState[],
    riskFlags: [] as RiskFlagState[],
    replayJobs: [] as ReplayJobState[],
    brokerSnapshot: null as BrokerSnapshot | null,
    omsOrders: [] as BrokerOmsOrder[],
    executionFills: [] as BrokerExecutionFill[],
    brokerOrderNotices: [] as BrokerOrderNotice[],
    tradingSnapshot: null as TradingSnapshot | null,
    marketSnapshot: null as MarketIntelSnapshot | null,
    marketDiagnostics: null as MarketPipelineDiagnostics | null,
    selectorDiagnostics: null as SelectorDiagnostics | null,
    portfolioDiagnostics: null as PortfolioDiagnostics | null,
    replaySnapshot: null as ReplaySnapshot | null,
    shadowSnapshot: null as ShadowLiveSnapshot | null,
    shadowLoopSnapshot: null as ShadowLoopSnapshot | null,
    candidateDecisions: [] as CandidateDecisionRecord[],
    disclosureRules: [] as DisclosureRuleDefinition[],
    latestPlan: null as ExecutionPlan | null,
    controlPlaneAuditEvents: [] as ControlPlaneAuditEvent[],
    researchReport: null as ResearchReportSnapshot | null,
    shadowMetricsSummary: null as ShadowLiveMetricsSummary | null,
    shadowRunAuditSummary: null as ShadowLiveMetricsSummary | null,
    liveControls: null as LiveControlState | null,
    instrumentLookups: {} as Record<string, InstrumentLookupEntry>,
    orderMonitorPending: false,
    loopActionPending: false,
    loopActionError: null as string | null,
  }),
  actions: {
    collectReferencedSymbols(): string[] {
      const symbols = new Set<string>();
      const addSymbol = (value?: string | null) => {
        const normalized = String(value ?? "").trim();
        if (normalized.length > 0) {
          symbols.add(normalized);
        }
      };

      for (const position of this.positions) {
        addSymbol(position.symbol);
      }
      for (const riskFlag of this.riskFlags) {
        addSymbol(riskFlag.symbol);
      }
      for (const decision of this.candidateDecisions) {
        addSymbol(decision.source_symbol);
      }
      for (const order of this.omsOrders) {
        addSymbol(order.instrument_id);
      }
      for (const fill of this.executionFills) {
        addSymbol(fill.instrument_id);
      }
      for (const notice of this.brokerOrderNotices) {
        addSymbol(notice.STCK_SHRN_ISCD);
      }
      addSymbol(this.latestPlan?.planned_order?.instrument_id as string | undefined);

      for (const decision of this.marketDiagnostics?.recent_candidate_decisions ?? []) {
        addSymbol(decision.source_symbol);
      }
      for (const event of this.marketDiagnostics?.recent_structured_events ?? []) {
        addSymbol(event.instrument_id);
      }
      for (const cluster of this.marketDiagnostics?.recent_event_clusters ?? []) {
        addSymbol(cluster.instrument_id);
      }
      for (const trigger of this.marketDiagnostics?.recent_watchlist_triggers ?? []) {
        addSymbol(trigger.instrument_id);
      }
      for (const candidate of this.marketDiagnostics?.recent_candidates ?? []) {
        addSymbol(candidate.instrument_id);
      }

      for (const trigger of this.selectorDiagnostics?.recent_watchlist ?? []) {
        addSymbol(trigger.instrument_id);
      }
      for (const selection of this.selectorDiagnostics?.recent_selections ?? []) {
        addSymbol(selection.instrument_id);
      }
      for (const candidate of this.selectorDiagnostics?.recent_trade_candidates ?? []) {
        addSymbol(candidate.instrument_id);
      }

      for (const selection of this.portfolioDiagnostics?.recent_selected_decisions ?? []) {
        addSymbol(selection.instrument_id);
      }
      for (const candidate of this.portfolioDiagnostics?.recent_trade_candidates ?? []) {
        addSymbol(candidate.instrument_id);
      }
      for (const symbol of this.portfolioDiagnostics?.portfolio?.selected_symbols ?? []) {
        addSymbol(symbol);
      }

      for (const run of this.shadowRunAuditSummary?.recent_runs ?? []) {
        addSymbol(run.instrument_id);
      }
      for (const row of this.researchReport?.instrument_leaderboard ?? []) {
        addSymbol(row.instrument_id);
      }
      for (const run of this.shadowMetricsSummary?.recent_runs ?? []) {
        addSymbol(run.instrument_id);
      }

      return [...symbols];
    },
    async refreshInstrumentLookups(symbols?: string[]) {
      const requestedSymbols = [...new Set((symbols ?? this.collectReferencedSymbols()).map((value) => String(value ?? "").trim()).filter((value) => value.length > 0))];
      if (requestedSymbols.length === 0) {
        return;
      }
      const params = new URLSearchParams();
      for (const symbol of requestedSymbols) {
        params.append("symbol", symbol);
      }
      const lookups = await getJson<InstrumentLookupEntry[]>(
        `/ops/instrument-names?${params.toString()}`,
        [],
      );
      if (!Array.isArray(lookups)) {
        return;
      }
      const nextLookupMap = { ...this.instrumentLookups };
      for (const lookup of lookups) {
        const normalized = String(lookup?.symbol ?? "").trim();
        if (!normalized) {
          continue;
        }
        nextLookupMap[normalized] = lookup;
      }
      this.instrumentLookups = nextLookupMap;
    },
    resolveInstrumentName(symbol?: string | null, fallbackName?: string | null): string | null {
      const normalized = String(symbol ?? "").trim();
      if (normalized.length === 0) {
        const fallback = String(fallbackName ?? "").trim();
        return fallback.length > 0 ? fallback : null;
      }
      const cachedName = String(this.instrumentLookups[normalized]?.name ?? "").trim();
      if (cachedName.length > 0) {
        return cachedName;
      }
      const fallback = String(fallbackName ?? "").trim();
      return fallback.length > 0 ? fallback : null;
    },
    formatInstrumentLabel(symbol?: string | null, fallbackName?: string | null): string {
      return formatInstrumentLabel(symbol, fallbackName, {
        resolveInstrumentName: (value) => this.resolveInstrumentName(value),
      });
    },
    translateText(value?: string | null): string {
      return translateReasonText(value, {
        resolveInstrumentName: (symbol) => this.resolveInstrumentName(symbol),
      });
    },
    async refreshDashboard() {
      if (this.dashboardPending) {
        return;
      }
      this.dashboardPending = true;
      try {
        const [
          summary,
          breaks,
          positions,
          riskFlags,
          brokerSnapshot,
          omsOrders,
          executionFills,
          brokerOrderNotices,
          tradingSnapshot,
          marketSnapshot,
          marketDiagnostics,
          selectorDiagnostics,
          portfolioDiagnostics,
          replaySnapshot,
          shadowSnapshot,
          shadowLoopSnapshot,
          candidateDecisions,
          latestPlan,
          controlPlaneAuditEvents,
          shadowRunAuditSummary,
          liveControls,
        ] = await Promise.all([
          getJson<DashboardSummary>("/ops/summary", this.summary ?? defaultSummary()),
          getJson<BreakState[]>("/ops/reconciliation-breaks", [...this.breaks]),
          getJson<PositionState[]>("/ops/positions", [...this.positions]),
          getJson<RiskFlagState[]>("/ops/risk-flags", [...this.riskFlags]),
          getJson<BrokerSnapshot | null>("/svc/broker/snapshot", this.brokerSnapshot),
          getJson<BrokerOmsOrder[]>("/svc/broker/oms/orders?limit=20", [...this.omsOrders]),
          getJson<BrokerExecutionFill[]>(
            "/svc/broker/oms/fills?limit=20",
            [...this.executionFills],
          ),
          getJson<BrokerOrderNotice[]>(
            "/svc/broker/ws/order-notices?limit=10",
            [...this.brokerOrderNotices],
          ),
          getJson<TradingSnapshot | null>("/svc/trading/snapshot", this.tradingSnapshot),
          getJson<MarketIntelSnapshot | null>("/svc/market/snapshot", this.marketSnapshot),
          getJson<MarketPipelineDiagnostics | null>(
            "/svc/market/diagnostics/pipeline?limit=15",
            this.marketDiagnostics,
          ),
          getJson<SelectorDiagnostics | null>(
            "/svc/selector/selector/diagnostics?limit=15",
            this.selectorDiagnostics,
          ),
          getJson<PortfolioDiagnostics | null>(
            "/svc/portfolio/portfolio/diagnostics?limit=15",
            this.portfolioDiagnostics,
          ),
          getJson<ReplaySnapshot | null>("/svc/replay/snapshot", this.replaySnapshot),
          getJson<ShadowLiveSnapshot | null>("/svc/shadow/snapshot", this.shadowSnapshot),
          getJson<ShadowLoopSnapshot | null>(
            "/svc/shadow/loop/status",
            this.shadowLoopSnapshot,
          ),
          getJson<CandidateDecisionRecord[]>(
            "/ops/candidate-decisions",
            [...this.candidateDecisions],
          ),
          getJson<ExecutionPlan | null>("/svc/shadow/plan/latest", this.latestPlan),
          getJson<ControlPlaneAuditEvent[]>(
            "/ops/audit-log?limit=20",
            [...this.controlPlaneAuditEvents],
          ),
          getJson<ShadowLiveMetricsSummary | null>(
            "/svc/shadow/run-audit/summary?recent_limit=15",
            this.shadowRunAuditSummary,
          ),
          getJson<LiveControlState>(
            "/ops/live-controls",
            this.liveControls ?? defaultLiveControls(),
          ),
        ]);

        this.summary = summary;
        this.breaks = breaks;
        this.positions = positions;
        this.riskFlags = riskFlags;
        this.brokerSnapshot = brokerSnapshot;
        this.omsOrders = omsOrders;
        this.executionFills = executionFills;
        this.brokerOrderNotices = brokerOrderNotices;
        this.tradingSnapshot = tradingSnapshot;
        this.marketSnapshot = marketSnapshot;
        this.marketDiagnostics = marketDiagnostics;
        this.selectorDiagnostics = selectorDiagnostics;
        this.portfolioDiagnostics = portfolioDiagnostics;
        this.replaySnapshot = replaySnapshot;
        this.shadowSnapshot = shadowSnapshot;
        this.shadowLoopSnapshot = shadowLoopSnapshot;
        this.candidateDecisions = candidateDecisions;
        this.latestPlan = latestPlan;
        this.controlPlaneAuditEvents = controlPlaneAuditEvents;
        this.shadowRunAuditSummary = shadowRunAuditSummary;
        this.liveControls = liveControls;
        await this.refreshInstrumentLookups();
      } finally {
        this.dashboardPending = false;
      }
    },
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
          omsOrders,
          executionFills,
          brokerOrderNotices,
          tradingSnapshot,
          marketSnapshot,
          marketDiagnostics,
          selectorDiagnostics,
          portfolioDiagnostics,
          replaySnapshot,
          shadowSnapshot,
          shadowLoopSnapshot,
          candidateDecisions,
          disclosureRules,
          latestPlan,
          controlPlaneAuditEvents,
          shadowRunAuditSummary,
          liveControls,
        ] = await Promise.all([
          getJson<DashboardSummary>("/ops/summary", this.summary ?? defaultSummary()),
          getJson<StrategyState[]>("/ops/strategies", [...this.strategies]),
          getJson<BreakState[]>("/ops/reconciliation-breaks", [...this.breaks]),
          getJson<PositionState[]>("/ops/positions", [...this.positions]),
          getJson<RiskFlagState[]>("/ops/risk-flags", [...this.riskFlags]),
          getJson<ReplayJobState[]>("/ops/replay-jobs", [...this.replayJobs]),
          getJson<BrokerSnapshot | null>("/svc/broker/snapshot", this.brokerSnapshot),
          getJson<BrokerOmsOrder[]>("/svc/broker/oms/orders?limit=20", [...this.omsOrders]),
          getJson<BrokerExecutionFill[]>(
            "/svc/broker/oms/fills?limit=20",
            [...this.executionFills],
          ),
          getJson<BrokerOrderNotice[]>(
            "/svc/broker/ws/order-notices?limit=10",
            [...this.brokerOrderNotices],
          ),
          getJson<TradingSnapshot | null>("/svc/trading/snapshot", this.tradingSnapshot),
          getJson<MarketIntelSnapshot | null>("/svc/market/snapshot", this.marketSnapshot),
          getJson<MarketPipelineDiagnostics | null>(
            "/svc/market/diagnostics/pipeline?limit=15",
            this.marketDiagnostics,
          ),
          getJson<SelectorDiagnostics | null>(
            "/svc/selector/selector/diagnostics?limit=15",
            this.selectorDiagnostics,
          ),
          getJson<PortfolioDiagnostics | null>(
            "/svc/portfolio/portfolio/diagnostics?limit=15",
            this.portfolioDiagnostics,
          ),
          getJson<ReplaySnapshot | null>("/svc/replay/snapshot", this.replaySnapshot),
          getJson<ShadowLiveSnapshot | null>("/svc/shadow/snapshot", this.shadowSnapshot),
          getJson<ShadowLoopSnapshot | null>("/svc/shadow/loop/status", this.shadowLoopSnapshot),
          getJson<CandidateDecisionRecord[]>("/ops/candidate-decisions", [...this.candidateDecisions]),
          getJson<DisclosureRuleDefinition[]>("/ops/rules/disclosure", [...this.disclosureRules]),
          getJson<ExecutionPlan | null>("/svc/shadow/plan/latest", this.latestPlan),
          getJson<ControlPlaneAuditEvent[]>(
            "/ops/audit-log?limit=20",
            [...this.controlPlaneAuditEvents],
          ),
          getJson<ShadowLiveMetricsSummary | null>(
            "/svc/shadow/run-audit/summary?recent_limit=15",
            this.shadowRunAuditSummary,
          ),
          getJson<LiveControlState>("/ops/live-controls", this.liveControls ?? defaultLiveControls()),
        ]);

        this.summary = summary;
        this.strategies = strategies;
        this.breaks = breaks;
        this.positions = positions;
        this.riskFlags = riskFlags;
        this.replayJobs = replayJobs;
        this.brokerSnapshot = brokerSnapshot;
        this.omsOrders = omsOrders;
        this.executionFills = executionFills;
        this.brokerOrderNotices = brokerOrderNotices;
        this.tradingSnapshot = tradingSnapshot;
        this.marketSnapshot = marketSnapshot;
        this.marketDiagnostics = marketDiagnostics;
        this.selectorDiagnostics = selectorDiagnostics;
        this.portfolioDiagnostics = portfolioDiagnostics;
        this.replaySnapshot = replaySnapshot;
        this.shadowSnapshot = shadowSnapshot;
        this.shadowLoopSnapshot = shadowLoopSnapshot;
        this.candidateDecisions = candidateDecisions;
        this.disclosureRules = disclosureRules;
        this.latestPlan = latestPlan;
        this.controlPlaneAuditEvents = controlPlaneAuditEvents;
        this.shadowRunAuditSummary = shadowRunAuditSummary;
        this.liveControls = liveControls;
        await this.refreshInstrumentLookups();
      } finally {
        this.loading = false;
      }
    },
    async refreshOrderMonitor() {
      if (this.orderMonitorPending) {
        return;
      }
      this.orderMonitorPending = true;
      try {
        const [
          omsOrders,
          executionFills,
          brokerOrderNotices,
          positions,
          latestPlan,
          candidateDecisions,
          shadowSnapshot,
          shadowLoopSnapshot,
        ] = await Promise.all([
          getJson<BrokerOmsOrder[]>("/svc/broker/oms/orders?limit=20", [...this.omsOrders]),
          getJson<BrokerExecutionFill[]>(
            "/svc/broker/oms/fills?limit=20",
            [...this.executionFills],
          ),
          getJson<BrokerOrderNotice[]>(
            "/svc/broker/ws/order-notices?limit=10",
            [...this.brokerOrderNotices],
          ),
          getJson<PositionState[]>("/ops/positions", [...this.positions]),
          getJson<ExecutionPlan | null>("/svc/shadow/plan/latest", this.latestPlan),
          getJson<CandidateDecisionRecord[]>(
            "/ops/candidate-decisions",
            [...this.candidateDecisions],
          ),
          getJson<ShadowLiveSnapshot | null>("/svc/shadow/snapshot", this.shadowSnapshot),
          getJson<ShadowLoopSnapshot | null>(
            "/svc/shadow/loop/status",
            this.shadowLoopSnapshot,
          ),
        ]);
        this.omsOrders = omsOrders;
        this.executionFills = executionFills;
        this.brokerOrderNotices = brokerOrderNotices;
        this.positions = positions;
        this.latestPlan = latestPlan;
        this.candidateDecisions = candidateDecisions;
        this.shadowSnapshot = shadowSnapshot;
        this.shadowLoopSnapshot = shadowLoopSnapshot;
        await this.refreshInstrumentLookups();
      } finally {
        this.orderMonitorPending = false;
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
        await this.refreshInstrumentLookups();
      } finally {
        this.loading = false;
      }
    },
    async resolveReconciliationBreak(breakId: string) {
      const normalizedBreakId = String(breakId ?? "").trim();
      if (!normalizedBreakId) {
        throw new Error("정합성 브레이크 ID가 비어 있습니다.");
      }

      const response = await fetch(
        `/svc/broker/reconciliation/breaks/${encodeURIComponent(normalizedBreakId)}/resolve`,
        {
          method: "POST",
        },
      );
      if (!response.ok) {
        throw new Error(await readErrorMessage(response));
      }
      await this.refreshDashboard();
      return (await response.json()) as Record<string, unknown>;
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
