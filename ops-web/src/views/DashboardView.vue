<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";

import AutonomousLoopPanel from "../components/AutonomousLoopPanel.vue";
import PipelineDiagnosticsPanel from "../components/PipelineDiagnosticsPanel.vue";
import StrategyGuidancePanel from "../components/StrategyGuidancePanel.vue";
import { useOpsStore } from "../stores/ops";
import {
  formatBooleanWord,
  formatBreakId,
  formatCandidateStatus,
  formatCommonCode,
  formatCurrencyKrw,
  formatDateTimeLocal,
  formatOperationMode,
  formatOrderSide,
  formatOrderState,
  formatQuoteBasis,
  formatRiskFlagType,
  formatRuleId,
  formatSeverity,
  formatVenueHint,
} from "../utils/display";

type Tone = "ok" | "alert" | "neutral";

interface ActivityItem {
  key: string;
  happenedAt: string;
  kind: string;
  headline: string;
  detail: string;
  tone: Tone;
}

interface HealthItem {
  key: string;
  label: string;
  value: string;
  detail: string;
  tone: Tone;
}

interface IssueItem {
  key: string;
  title: string;
  detail: string;
  stamp?: string | null;
  tone: Tone;
}

const store = useOpsStore();
const autoRefreshEnabled = ref(true);
const autoRefreshSeconds = 5;

let refreshTimer: number | null = null;

function parseTime(value?: string | null): number {
  if (!value) {
    return 0;
  }
  const parsed = Date.parse(value);
  return Number.isNaN(parsed) ? 0 : parsed;
}

function badgeToneClass(tone: Tone): string {
  if (tone === "ok") {
    return "badge badge-ok";
  }
  if (tone === "alert") {
    return "badge badge-alert";
  }
  return "badge badge-neutral";
}

function payloadString(
  payload: Record<string, unknown> | null | undefined,
  key: string,
): string | null {
  const value = payload?.[key];
  if (typeof value !== "string") {
    return null;
  }
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

function payloadNumber(
  payload: Record<string, unknown> | null | undefined,
  key: string,
): number | null {
  const value = payload?.[key];
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function payloadRecord(
  payload: Record<string, unknown> | null | undefined,
  key: string,
): Record<string, unknown> | null {
  const value = payload?.[key];
  if (value && typeof value === "object" && !Array.isArray(value)) {
    return value as Record<string, unknown>;
  }
  return null;
}

function shadowRunDetail(run: {
  status?: string | null;
  reason?: string | null;
  payload?: Record<string, unknown> | null;
}): string {
  const blockingDetail =
    run.reason ??
    payloadString(run.payload, "risk_reason_summary") ??
    payloadString(run.payload, "selection_reason");
  const successDetail =
    payloadString(run.payload, "selection_reason") ??
    payloadString(run.payload, "risk_reason_summary") ??
    run.reason;
  const preferBlockingReason = ["BLOCKED", "NO_TRADE", "ERROR", "PLANNED_ONLY"].includes(
    String(run.status ?? "").toUpperCase(),
  );
  return translatedText(
    (preferBlockingReason ? blockingDetail : successDetail) ?? "최근 루프 상세 사유 없음",
  );
}

function formatDiagnosticKey(value?: string | null): string {
  const mapping: Record<string, string> = {
    missing_context: "문맥 누락",
    hard_block: "하드블록",
    tradeability: "매매 불가",
    low_confidence: "저신뢰도",
    stale_cluster: "신선도 만료",
    vendor_fail_closed: "시세 벤더 차단",
    outside_top_n: "상위 순위 밖",
    non_positive_alpha: "순알파 0 이하",
  };
  const normalized = value?.trim() ?? "";
  if (!normalized) {
    return "-";
  }
  return mapping[normalized] ?? formatCommonCode(normalized);
}

function formatCountMap(
  counts: Record<string, number> | null | undefined,
  maxItems = 3,
): string {
  const entries = Object.entries(counts ?? {})
    .filter(([, count]) => Number.isFinite(count) && count > 0)
    .sort((left, right) => right[1] - left[1])
    .slice(0, maxItems);
  if (entries.length === 0) {
    return "없음";
  }
  return entries
    .map(([label, count]) => `${formatDiagnosticKey(label)} ${count}건`)
    .join(" / ");
}

function formatAuditAction(action?: string | null): string {
  const mapping: Record<string, string> = {
    SET_LIVE_CONTROL: "라이브 제어 변경",
    SET_LOOP_SCHEDULER: "루프 스케줄 변경",
    TRIGGER_KILL_SWITCH: "킬 스위치 작동",
    RELEASE_KILL_SWITCH: "킬 스위치 해제",
    SET_STRATEGY: "전략 상태 변경",
    SET_SYMBOL_BLOCK: "종목 차단 변경",
  };
  return mapping[action ?? ""] ?? action ?? "제어 이벤트";
}

function formatAuditDetail(event: {
  actor?: string | null;
  reason_code?: string | null;
  after?: Record<string, unknown> | null;
}): string {
  const parts: string[] = [];
  if (event.actor) {
    parts.push(`주체 ${event.actor}`);
  }
  if (event.reason_code) {
    parts.push(`사유 ${event.reason_code}`);
  }
  const after = event.after ?? {};
  const maxOrderValue = after.max_order_value_krw;
  const autoLoop = after.autonomous_loop_enabled;
  if (typeof maxOrderValue === "number") {
    parts.push(`한도 ${formatCurrencyKrw(maxOrderValue)}`);
  }
  if (typeof autoLoop === "boolean") {
    parts.push(`자동 루프 ${autoLoop ? "활성" : "비활성"}`);
  }
  return parts.join(" / ") || "상세 제어 정보 없음";
}

function instrumentLabel(symbol?: string | null): string {
  return store.formatInstrumentLabel(symbol);
}

function translatedText(value?: string | null): string {
  return store.translateText(value);
}

function auditActionLabel(action?: string | null): string {
  const mapping: Record<string, string> = {
    SET_LIVE_CONTROL: "라이브 제어 변경",
    ACQUIRE_LOOP_LEASE: "자동 루프 시작",
    RELEASE_LOOP_LEASE: "자동 루프 중지",
    SUSPEND_LOOP_RUNTIME: "자동 루프 일시 중지",
    ACTIVATE_KILL_SWITCH: "킬 스위치 작동",
    TRIGGER_KILL_SWITCH: "킬 스위치 작동",
    RELEASE_KILL_SWITCH: "킬 스위치 해제",
    SET_STRATEGY_ENABLED: "전략 상태 변경",
    SET_STRATEGY: "전략 상태 변경",
    SET_SYMBOL_BLOCK: "종목 차단 변경",
  };
  return mapping[action ?? ""] ?? translatedText(action);
}

function auditDetailText(event: {
  actor?: string | null;
  reason_code?: string | null;
  after?: Record<string, unknown> | null;
}): string {
  const parts: string[] = [];
  if (event.actor) {
    parts.push(`주체 ${event.actor}`);
  }
  if (event.reason_code) {
    parts.push(`사유 ${translatedText(event.reason_code)}`);
  }
  const after = event.after ?? {};
  const maxOrderValue = after.max_order_value_krw;
  const autoLoop = after.autonomous_loop_enabled;
  if (typeof maxOrderValue === "number") {
    parts.push(`한도 ${formatCurrencyKrw(maxOrderValue)}`);
  }
  if (typeof autoLoop === "boolean") {
    parts.push(`자동 루프 ${autoLoop ? "활성" : "비활성"}`);
  }
  return parts.join(" / ") || "상세 제어 정보 없음";
}

function candidateTone(status?: string | null): Tone {
  if (status === "SELECTED" || status === "SUBMITTED" || status === "VERIFIED") {
    return "ok";
  }
  if (status?.includes("REJECTED") || status === "BLOCKED" || status === "ERROR") {
    return "alert";
  }
  return "neutral";
}

async function refreshDashboard() {
  await store.refreshDashboard();
}

function stopAutoRefresh() {
  if (refreshTimer !== null) {
    window.clearInterval(refreshTimer);
    refreshTimer = null;
  }
}

function startAutoRefresh() {
  stopAutoRefresh();
  refreshTimer = window.setInterval(() => {
    void store.refreshDashboard();
  }, autoRefreshSeconds * 1000);
}

watch(
  autoRefreshEnabled,
  (enabled) => {
    if (enabled) {
      startAutoRefresh();
      return;
    }
    stopAutoRefresh();
  },
  { immediate: true },
);

onMounted(async () => {
  await refreshDashboard();
});

onBeforeUnmount(() => {
  stopAutoRefresh();
});

const marketDecisionRows = computed(
  () => store.marketDiagnostics?.recent_candidate_decisions ?? store.candidateDecisions,
);

const latestMarketEvents = computed(
  () => store.marketDiagnostics?.recent_structured_events ?? [],
);

const latestSelectorWatchlist = computed(
  () =>
    store.selectorDiagnostics?.recent_watchlist ??
    store.marketDiagnostics?.recent_watchlist_triggers ??
    [],
);

const latestSelectorSelections = computed(
  () => store.selectorDiagnostics?.recent_selections ?? [],
);

const latestSelectorTradeCandidates = computed(
  () => store.selectorDiagnostics?.recent_trade_candidates ?? [],
);

const latestPortfolioSelections = computed(
  () => store.portfolioDiagnostics?.recent_selected_decisions ?? [],
);

const latestPortfolioTradeCandidates = computed(
  () => store.portfolioDiagnostics?.recent_trade_candidates ?? [],
);

const orderedDecisions = computed(() =>
  [...marketDecisionRows.value].sort(
    (left, right) => parseTime(right.created_at_utc) - parseTime(left.created_at_utc),
  ),
);

const primaryDecision = computed(
  () =>
    orderedDecisions.value.find((decision) => decision.candidate_status === "SELECTED") ??
    orderedDecisions.value[0] ??
    null,
);

const recentDecisions = computed(() => orderedDecisions.value.slice(0, 5));

const liveLoopRunning = computed(
  () => Boolean(store.shadowLoopSnapshot?.running && store.shadowLoopSnapshot?.execute_live),
);

const latestShadowRun = computed(
  () => store.shadowRunAuditSummary?.recent_runs?.[0] ?? null,
);

const latestShadowPayload = computed(
  () => latestShadowRun.value?.payload ?? null,
);

const latestShadowOrder = computed(() =>
  payloadRecord(latestShadowPayload.value, "planned_order"),
);

const focusSymbol = computed(
  () =>
    latestShadowRun.value?.instrument_id ??
    primaryDecision.value?.source_symbol ??
    store.latestPlan?.planned_order?.instrument_id ??
    null,
);

const focusReportName = computed(
  () =>
    payloadString(latestShadowPayload.value, "source_report_name") ??
    primaryDecision.value?.source_report_name ??
    store.latestPlan?.source_report_name ??
    null,
);

const focusRuleId = computed(
  () =>
    payloadString(latestShadowPayload.value, "matched_rule_id") ??
    store.latestPlan?.matched_rule_id ??
    primaryDecision.value?.matched_positive_rule_id ??
    primaryDecision.value?.matched_block_rule_id ??
    null,
);

const focusSelectionReason = computed(
  () =>
    payloadString(latestShadowPayload.value, "selection_reason") ??
    primaryDecision.value?.selection_reason ??
    primaryDecision.value?.rejection_reason ??
    store.latestPlan?.selection_reason ??
    null,
);

const focusRiskReason = computed(
  () =>
    payloadString(latestShadowPayload.value, "risk_reason_summary") ??
    latestShadowRun.value?.reason ??
    store.latestPlan?.risk_reason_summary ??
    store.shadowSnapshot?.last_execution_reason ??
    store.latestPlan?.reason ??
    null,
);

const focusQuoteBasis = computed(
  () =>
    payloadString(latestShadowPayload.value, "quote_basis") ??
    store.latestPlan?.quote_basis ??
    null,
);

const focusSelectedPrice = computed(
  () =>
    payloadNumber(latestShadowPayload.value, "selected_price_krw") ??
    store.latestPlan?.selected_price_krw ??
    null,
);

const focusPlannedQty = computed(() => {
  const value = latestShadowOrder.value?.qty;
  if (typeof value === "number" && Number.isFinite(value)) {
    return value;
  }
  if (typeof value === "string" && value.trim().length > 0) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return store.latestPlan?.planned_order?.qty ?? null;
});

const focusVenue = computed(() => {
  const value = latestShadowOrder.value?.venue_hint;
  if (typeof value === "string" && value.trim().length > 0) {
    return value;
  }
  return store.latestPlan?.planned_order?.venue_hint ?? null;
});

const pipelineStages = computed(() => [
  {
    label: "공시 입력",
    value: `${store.marketDiagnostics?.pipeline_counts.disclosure_input_count ?? 0}건`,
    detail: formatCountMap(store.marketDiagnostics?.status_counts),
    tone:
      (store.marketDiagnostics?.pipeline_counts.disclosure_input_count ?? 0) > 0
        ? "ok"
        : "neutral",
  },
  {
    label: "구조화 이벤트",
    value: `${store.marketDiagnostics?.pipeline_counts.structured_event_count ?? 0}건`,
    detail: formatCountMap(store.marketDiagnostics?.event_type_counts),
    tone:
      (store.marketDiagnostics?.pipeline_counts.structured_event_count ?? 0) > 0
        ? "ok"
        : "neutral",
  },
  {
    label: "워치리스트",
    value: `${store.selectorDiagnostics?.diagnostics.input_watchlist_count ?? 0}건`,
    detail: formatCountMap(
      store.selectorDiagnostics?.diagnostics.watchlist_reason_counts ??
        store.marketDiagnostics?.watchlist_reason_counts,
    ),
    tone:
      (store.selectorDiagnostics?.diagnostics.input_watchlist_count ?? 0) > 0
        ? "ok"
        : "neutral",
  },
  {
    label: "selector 선택",
    value: `${store.selectorDiagnostics?.diagnostics.selected_topn_count ?? 0}건`,
    detail: formatCountMap(store.selectorDiagnostics?.diagnostics.filter_counts),
    tone:
      (store.selectorDiagnostics?.diagnostics.selected_topn_count ?? 0) > 0
        ? "ok"
        : "neutral",
  },
  {
    label: "portfolio 편입",
    value: `${store.portfolioDiagnostics?.diagnostics.chosen_count ?? 0}건`,
    detail: formatCountMap({
      non_positive_alpha: store.portfolioDiagnostics?.diagnostics.dropped_non_positive_count ?? 0,
      sector_cap: store.portfolioDiagnostics?.diagnostics.sector_cap_rejection_count ?? 0,
    }),
    tone:
      (store.portfolioDiagnostics?.diagnostics.chosen_count ?? 0) > 0
        ? "ok"
        : "neutral",
  },
  {
    label: "실행 후보",
    value: `${store.portfolioDiagnostics?.diagnostics.matched_trade_candidate_count ?? 0}건`,
    detail: `shadow 최근 결과 ${formatCandidateStatus(
      latestShadowRun.value?.status ?? store.shadowLoopSnapshot?.last_result_status,
    )}`,
    tone:
      (store.portfolioDiagnostics?.diagnostics.matched_trade_candidate_count ?? 0) > 0
        ? "ok"
        : candidateTone(latestShadowRun.value?.status ?? store.shadowLoopSnapshot?.last_result_status),
  },
]);

const currentBlockerDiagnosis = computed(() => {
  if ((store.marketDiagnostics?.pipeline_counts.disclosure_input_count ?? 0) === 0) {
    return store.marketDiagnostics?.summary_note ?? "오늘 공시 입력이 아직 없어 후보 생성이 시작되지 않았습니다.";
  }
  if ((store.marketDiagnostics?.pipeline_counts.structured_event_count ?? 0) === 0) {
    return store.marketDiagnostics?.summary_note ?? "공시는 있었지만 구조화 이벤트가 생성되지 않았습니다.";
  }
  if ((store.selectorDiagnostics?.diagnostics.input_watchlist_count ?? 0) === 0) {
    return store.selectorDiagnostics?.diagnostics.summary_note ?? "selector 입력 워치리스트가 비어 있습니다.";
  }
  if ((store.selectorDiagnostics?.diagnostics.selected_topn_count ?? 0) === 0) {
    return store.selectorDiagnostics?.diagnostics.summary_note ?? "selector 단계에서 거래 후보가 생성되지 않았습니다.";
  }
  if ((store.portfolioDiagnostics?.diagnostics.chosen_count ?? 0) === 0) {
    return store.portfolioDiagnostics?.diagnostics.summary_note ?? "portfolio 단계에서 편입 종목이 없습니다.";
  }
  if ((store.portfolioDiagnostics?.diagnostics.matched_trade_candidate_count ?? 0) === 0) {
    return store.portfolioDiagnostics?.diagnostics.summary_note ?? "portfolio 편입 종목이 실행 후보로 연결되지 않았습니다.";
  }
  return (
    focusRiskReason.value ??
    "후보 생성과 실행 체인이 살아 있으며, 현재는 최신 루프 조건에 따라 대기 중입니다."
  );
});

const thoughtStats = computed(() => [
  {
    label: "최근 루프 결과",
    value: formatCandidateStatus(
      latestShadowRun.value?.status ?? store.shadowLoopSnapshot?.last_result_status,
    ),
    detail: `완료 ${formatDateTimeLocal(
      latestShadowRun.value?.created_at_utc ?? store.shadowLoopSnapshot?.last_finished_at_utc,
    )}`,
  },
  {
    label: "최근 heartbeat",
    value: store.shadowLoopSnapshot?.lease_stale ? "지연" : "정상",
    detail: formatDateTimeLocal(store.shadowLoopSnapshot?.heartbeat_at_utc),
  },
  {
    label: "실행 모드",
    value:
      latestShadowRun.value?.execute_live || store.shadowLoopSnapshot?.execute_live
        ? "실거래"
        : "모의",
    detail: `루프 ${store.shadowLoopSnapshot?.running ? "실행 중" : "대기"}`,
  },
  {
    label: "후보 / 종목",
    value: instrumentLabel(focusSymbol.value),
    detail: `후보 ${latestShadowRun.value?.candidate_id ?? store.latestPlan?.candidate_id ?? "-"}`,
  },
  {
    label: "선택 가격",
    value: formatCurrencyKrw(focusSelectedPrice.value),
    detail: `기준 ${formatQuoteBasis(focusQuoteBasis.value)}`,
  },
  {
    label: "주문 수량",
    value: focusPlannedQty.value !== null && focusPlannedQty.value !== undefined
      ? `${focusPlannedQty.value}주`
      : "-",
    detail: `시장 ${formatVenueHint(focusVenue.value)}`,
  },
]);

const topStats = computed(() => [
  {
    label: "운영 모드",
    value: formatOperationMode(store.summary?.mode.mode),
    detail: `업데이트 ${formatDateTimeLocal(store.summary?.mode.updated_at_utc)}`,
    tone: store.summary?.mode.mode === "KILL_SWITCH" ? "alert" : "ok",
  },
  {
    label: "자동 루프",
    value: liveLoopRunning.value ? "실행 중" : "중지",
    detail: `주기 ${store.liveControls?.auto_loop_interval_seconds ?? 60}초`,
    tone: liveLoopRunning.value ? "ok" : "neutral",
  },
  {
    label: "최대 투자 한도",
    value: formatCurrencyKrw(store.liveControls?.max_order_value_krw ?? 500000),
    detail: `목표 ${store.shadowLoopSnapshot?.desired_running ? "가동" : "정지"}`,
    tone: "neutral" as Tone,
  },
  {
    label: "실시간 후보",
    value: `${store.shadowSnapshot?.candidate_count ?? 0}건`,
    detail: `마지막 동기화 ${formatDateTimeLocal(store.shadowSnapshot?.last_sync_utc)}`,
    tone:
      (store.shadowSnapshot?.candidate_count ?? 0) > 0 || liveLoopRunning.value ? "ok" : "neutral",
  },
  {
    label: "미해결 브레이크",
    value: `${store.breaks.length}건`,
    detail: `리스크 플래그 ${store.riskFlags.length}건`,
    tone: store.breaks.length > 0 || store.riskFlags.length > 0 ? "alert" : "ok",
  },
  {
    label: "주문 제출",
    value: `${store.shadowSnapshot?.submitted_count ?? 0}건`,
    detail: `최근 상태 ${formatCandidateStatus(store.shadowSnapshot?.last_execution_status)}`,
    tone: (store.shadowSnapshot?.submitted_count ?? 0) > 0 ? "ok" : "neutral",
  },
  {
    label: "보유 포지션",
    value: `${store.positions.length}종목`,
    detail: `예상 현금 ${formatCurrencyKrw(store.tradingSnapshot?.projected_cash_krw)}`,
    tone: store.positions.length > 0 ? "ok" : "neutral",
  },
  {
    label: "브로커 상태",
    value: formatCommonCode(store.brokerSnapshot?.current_mode),
    detail: `WS 승인 ${formatBooleanWord(store.brokerSnapshot?.ws_approval_ready ?? false)}`,
    tone: store.brokerSnapshot?.degraded_reason ? "alert" : "ok",
  },
]);

const healthItems = computed<HealthItem[]>(() => [
  {
    key: "broker",
    label: "브로커 게이트웨이",
    value: formatCommonCode(store.brokerSnapshot?.current_mode),
    detail: `REST 인증 ${formatBooleanWord(
      store.brokerSnapshot?.rest_token_ready ?? false,
    )} / WS 승인 ${formatBooleanWord(store.brokerSnapshot?.ws_approval_ready ?? false)}`,
    tone: store.brokerSnapshot?.degraded_reason ? "alert" : "ok",
  },
  {
    key: "shadow",
    label: "섀도 라이브 루프",
    value: liveLoopRunning.value ? "실행 중" : formatCommonCode(store.shadowLoopSnapshot?.last_result_status),
    detail: `실행 ${store.shadowLoopSnapshot?.run_count ?? 0}회 / 마지막 종료 ${formatDateTimeLocal(
      store.shadowLoopSnapshot?.last_finished_at_utc,
    )}`,
    tone: store.shadowLoopSnapshot?.last_error ? "alert" : liveLoopRunning.value ? "ok" : "neutral",
  },
  {
    key: "market",
    label: "마켓 인텔",
    value: formatCommonCode(store.marketSnapshot?.parser_mode),
    detail: `백로그 ${store.marketSnapshot?.disclosure_backlog ?? 0}건 / 저신뢰 ${
      store.marketSnapshot?.low_confidence_count ?? 0
    }건`,
    tone: (store.marketSnapshot?.low_confidence_count ?? 0) > 0 ? "alert" : "ok",
  },
  {
    key: "trading",
    label: "트레이딩 코어",
    value: formatOperationMode(store.tradingSnapshot?.operation_mode),
    detail: `오픈 브레이크 ${store.tradingSnapshot?.open_breaks ?? 0}건 / 포지션 ${
      store.tradingSnapshot?.projected_positions ?? 0
    }건`,
    tone: (store.tradingSnapshot?.open_breaks ?? 0) > 0 ? "alert" : "ok",
  },
  {
    key: "replay",
    label: "리플레이 러너",
    value: formatCommonCode(store.replaySnapshot?.worker_mode),
    detail: `활성 작업 ${store.replaySnapshot?.active_jobs ?? 0}건 / 최근 ${formatDateTimeLocal(
      store.replaySnapshot?.last_run_utc,
    )}`,
    tone: (store.replaySnapshot?.active_jobs ?? 0) > 0 ? "neutral" : "ok",
  },
]);

const activeIssues = computed<IssueItem[]>(() => {
  const items: IssueItem[] = [];

  if (store.shadowLoopSnapshot?.last_error) {
    items.push({
      key: "loop-error",
      title: "루프 오류",
      detail: translatedText(store.shadowLoopSnapshot.last_error),
      stamp: store.shadowLoopSnapshot.last_finished_at_utc,
      tone: "alert",
    });
  }

  const riskReasons =
    store.latestPlan?.risk_reason_summary
      ?.split(",")
      .map((item) => item.trim())
      .filter((item) => item.length > 0) ?? [];

  for (const reason of riskReasons.slice(0, 3)) {
    items.push({
      key: `plan-risk-${reason}`,
      title: "현재 주문 차단 사유",
      detail: translatedText(reason),
      tone: "alert",
    });
  }

  for (const item of store.breaks.slice(0, 4)) {
    items.push({
      key: `break-${item.break_id}`,
      title: `${formatSeverity(item.severity)} 브레이크`,
      detail: formatBreakId(item.break_id),
      stamp: item.detected_at_utc,
      tone: "alert",
    });
  }

  for (const item of store.riskFlags.slice(0, 4)) {
    items.push({
      key: `risk-${item.symbol}-${item.flag_type}`,
      title: `${instrumentLabel(item.symbol)} 리스크`,
      detail: translatedText(item.flag_type),
      stamp: item.updated_at_utc,
      tone: item.hard_block ? "alert" : "neutral",
    });
  }

  return items.slice(0, 8);
});

const activityFeed = computed<ActivityItem[]>(() => {
  const items: ActivityItem[] = [];

  if (store.shadowLoopSnapshot?.heartbeat_at_utc || store.shadowLoopSnapshot?.last_finished_at_utc) {
    items.push({
      key: "loop-heartbeat",
      happenedAt:
        store.shadowLoopSnapshot?.heartbeat_at_utc ??
        store.shadowLoopSnapshot?.last_finished_at_utc ??
        "",
      kind: "루프 상태",
      headline: `${
        store.shadowLoopSnapshot?.running ? "루프 실행 중" : "루프 대기"
      } / ${formatCandidateStatus(store.shadowLoopSnapshot?.last_result_status)}`,
      detail: `heartbeat ${formatDateTimeLocal(
        store.shadowLoopSnapshot?.heartbeat_at_utc,
      )} / 누적 ${store.shadowLoopSnapshot?.run_count ?? 0}회`,
      tone: store.shadowLoopSnapshot?.last_error ? "alert" : store.shadowLoopSnapshot?.running ? "ok" : "neutral",
    });
  }

  for (const run of store.shadowRunAuditSummary?.recent_runs ?? []) {
    items.push({
      key: `shadow-run-${run.run_id}`,
      happenedAt: run.created_at_utc,
      kind: "루프 실행",
      headline: `${instrumentLabel(run.instrument_id)} ${formatCandidateStatus(run.status)}`,
      detail: shadowRunDetail(run),
      tone:
        run.status === "SUBMITTED" || run.status === "VERIFIED"
          ? "ok"
          : run.status === "BLOCKED" || run.status === "ERROR"
            ? "alert"
            : "neutral",
    });
  }

  for (const event of store.controlPlaneAuditEvents.slice(0, 8)) {
    items.push({
      key: `audit-${event.event_id}`,
      happenedAt: event.created_at_utc,
      kind: "제어 이벤트",
      headline: auditActionLabel(event.action),
      detail: auditDetailText(event),
      tone:
        event.action === "TRIGGER_KILL_SWITCH"
          ? "alert"
          : event.action === "SET_LIVE_CONTROL" || event.action === "SET_LOOP_SCHEDULER"
            ? "ok"
            : "neutral",
    });
  }

  for (const decision of orderedDecisions.value.slice(0, 6)) {
    items.push({
      key: `decision-${decision.decision_id}`,
      happenedAt: decision.created_at_utc,
      kind: "판단",
      headline: `${instrumentLabel(decision.source_symbol)} ${formatCandidateStatus(decision.candidate_status)}`,
      detail: translatedText(
        decision.selection_reason ??
        decision.rejection_reason ??
        decision.ranking_reason ??
        decision.source_report_name ??
        "판단 사유 없음",
      ),
      tone: candidateTone(decision.candidate_status),
    });
  }

  for (const order of store.omsOrders.slice(0, 6)) {
    items.push({
      key: `order-${order.internal_order_id}`,
      happenedAt: order.last_event_at_utc,
      kind: "OMS 주문",
      headline: `${instrumentLabel(order.instrument_id)} ${formatOrderSide(order.side_code)} ${formatOrderState(
        order.order_state_code,
      )}`,
      detail: `총 ${order.filled_qty + order.working_qty}주 / 체결 ${order.filled_qty}주 / 평균 ${formatCurrencyKrw(
        order.avg_fill_price,
      )}`,
      tone:
        order.order_state_code === "FILLED" || order.order_state_code === "ACKED"
          ? "ok"
          : order.order_state_code === "REJECTED"
            ? "alert"
            : "neutral",
    });
  }

  for (const fill of store.executionFills.slice(0, 6)) {
    items.push({
      key: `fill-${fill.internal_order_id}-${fill.broker_trade_id ?? fill.fill_ts_utc}`,
      happenedAt: fill.fill_ts_utc,
      kind: "체결",
      headline: `${instrumentLabel(fill.instrument_id)} ${formatOrderSide(fill.side_code)} ${fill.fill_qty}주`,
      detail: `체결가 ${formatCurrencyKrw(fill.fill_price)} / 주문번호 ${fill.broker_order_no ?? "-"}`,
      tone: "ok",
    });
  }

  for (const notice of store.brokerOrderNotices.slice(0, 6)) {
    items.push({
      key: `notice-${notice.ODER_NO ?? notice.internal_order_id ?? notice.received_at_utc}`,
      happenedAt: notice.received_at_utc ?? "",
      kind: "브로커 알림",
      headline: `${instrumentLabel(notice.STCK_SHRN_ISCD)} ${
        notice.is_fill ? "체결 통보" : "주문 통보"
      }`,
      detail: `주문번호 ${notice.ODER_NO ?? "-"} / 수량 ${
        notice.CNTG_QTY ?? notice.ORD_QTY ?? "-"
      }주 / 가격 ${notice.CNTG_UNPR ?? notice.ORD_UNPR ?? "-"}`,
      tone: notice.REJECT_YN === "Y" ? "alert" : notice.is_fill ? "ok" : "neutral",
    });
  }

  return items
    .sort((left, right) => parseTime(right.happenedAt) - parseTime(left.happenedAt))
    .slice(0, 12);
});
</script>

<template>
  <section class="stack dashboard-stack">
    <AutonomousLoopPanel />
    <PipelineDiagnosticsPanel />

    <article class="panel compact-panel">
      <div class="panel-header">
        <div>
          <h3>실시간 운영 보드</h3>
          <p class="helper-copy">
            시스템이 지금 무엇을 보고, 왜 판단했고, 실제로 어떤 주문과 체결이 나왔는지 한 화면에서
            추적합니다.
          </p>
        </div>
        <div class="panel-tools">
          <button
            class="secondary-button"
            :disabled="store.dashboardPending"
            @click="refreshDashboard"
          >
            지금 갱신
          </button>
          <button class="secondary-button" @click="autoRefreshEnabled = !autoRefreshEnabled">
            {{ autoRefreshEnabled ? `${autoRefreshSeconds}초 자동 갱신 켜짐` : "자동 갱신 꺼짐" }}
          </button>
        </div>
      </div>

      <div class="compact-stat-grid">
        <div
          v-for="item in topStats"
          :key="item.label"
          class="compact-stat"
        >
          <span>{{ item.label }}</span>
          <strong>{{ item.value }}</strong>
          <p :class="['compact-stat-detail', item.tone === 'alert' ? 'compact-stat-detail-alert' : '']">
            {{ item.detail }}
          </p>
        </div>
      </div>
    </article>

    <article class="panel compact-panel">
      <div class="panel-header">
        <div>
          <h3>현재 루프 생각과 행동</h3>
          <p class="helper-copy">
            마지막 루프가 무엇을 봤고, 왜 막혔거나 왜 주문하려 했는지 바로 읽을 수 있습니다.
          </p>
        </div>
        <span :class="badgeToneClass(candidateTone(latestShadowRun?.status ?? store.shadowLoopSnapshot?.last_result_status))">
          {{ formatCandidateStatus(latestShadowRun?.status ?? store.shadowLoopSnapshot?.last_result_status) }}
        </span>
      </div>

      <div class="compact-stat-grid compact-stat-grid-6">
        <div v-for="item in thoughtStats" :key="item.label" class="compact-stat">
          <span>{{ item.label }}</span>
          <strong>{{ item.value }}</strong>
          <p class="compact-stat-detail">{{ item.detail }}</p>
        </div>
      </div>

      <div class="chip-row">
        <span class="tag">보고서 {{ focusReportName ?? "-" }}</span>
        <span class="tag">규칙 {{ formatRuleId(focusRuleId) }}</span>
        <span class="tag">최근 후보 {{ latestShadowRun?.candidate_id ?? store.latestPlan?.candidate_id ?? "-" }}</span>
        <span class="tag">최근 주문 {{ store.shadowSnapshot?.last_internal_order_id ?? "-" }}</span>
      </div>

      <div class="callout-grid">
        <div class="callout">
          <p class="detail-label">판단 근거</p>
          <strong>{{ translatedText(focusSelectionReason ?? "최근 판단 근거가 아직 없습니다.") }}</strong>
        </div>
        <div class="callout">
          <p class="detail-label">행동 또는 차단 이유</p>
          <strong>{{ translatedText(focusRiskReason ?? "최근 행동 사유가 아직 없습니다.") }}</strong>
        </div>
      </div>
    </article>

    <div class="dashboard-grid">
      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>지금 시스템이 보는 것</h3>
            <p class="helper-copy">가장 최근 후보, 선택 이유, 주문 계획, 차단 이유</p>
          </div>
          <span :class="badgeToneClass(candidateTone(primaryDecision?.candidate_status))">
            {{ formatCandidateStatus(primaryDecision?.candidate_status) }}
          </span>
        </div>

        <div v-if="primaryDecision" class="focus-block">
          <div class="focus-header">
            <div>
              <strong>{{ instrumentLabel(primaryDecision.source_symbol) }}</strong>
              <p>{{ primaryDecision.source_report_name ?? "최근 후보 없음" }}</p>
            </div>
            <div class="focus-meta">
              <span class="tag">{{ formatDateTimeLocal(primaryDecision.created_at_utc) }}</span>
              <span class="tag">
                {{ formatRuleId(primaryDecision.matched_positive_rule_id ?? primaryDecision.matched_block_rule_id) }}
              </span>
            </div>
          </div>

          <div class="kv-grid">
            <div class="kv-cell">
              <span>후보 상태</span>
              <strong>{{ formatCandidateStatus(primaryDecision.candidate_status) }}</strong>
            </div>
            <div class="kv-cell">
              <span>최근 실행</span>
              <strong>{{ formatCandidateStatus(store.shadowSnapshot?.last_execution_status) }}</strong>
            </div>
            <div class="kv-cell">
              <span>주문 수량</span>
              <strong>{{ store.latestPlan?.planned_order?.qty ?? "-" }}</strong>
            </div>
            <div class="kv-cell">
              <span>선택 가격</span>
              <strong>{{ formatCurrencyKrw(store.latestPlan?.selected_price_krw) }}</strong>
            </div>
            <div class="kv-cell">
              <span>가격 기준</span>
              <strong>{{ formatQuoteBasis(store.latestPlan?.quote_basis) }}</strong>
            </div>
            <div class="kv-cell">
              <span>시장 힌트</span>
              <strong>{{ formatVenueHint(store.latestPlan?.planned_order?.venue_hint as string | undefined) }}</strong>
            </div>
          </div>

          <div class="dense-callouts">
            <div class="callout">
              <p class="detail-label">판단 이유</p>
              <strong>
                {{
                  translatedText(
                    primaryDecision.selection_reason ??
                      primaryDecision.rejection_reason ??
                      primaryDecision.ranking_reason ??
                      "최근 판단 설명이 없습니다.",
                  )
                }}
              </strong>
            </div>
            <div class="callout">
              <p class="detail-label">주문 또는 차단 이유</p>
              <strong>
                {{
                  translatedText(
                    store.latestPlan?.risk_reason_summary ??
                      store.latestPlan?.price_reason ??
                      store.shadowSnapshot?.last_execution_reason ??
                      store.latestPlan?.reason ??
                      "최근 주문 근거가 없습니다.",
                  )
                }}
              </strong>
            </div>
          </div>
        </div>
        <p v-else class="helper-copy">아직 후보 판단 기록이 없습니다.</p>

        <div class="subpanel">
          <div class="panel-header">
            <h4>최근 판단 흐름</h4>
            <span>{{ recentDecisions.length }}건</span>
          </div>
          <div v-if="recentDecisions.length" class="mini-list">
            <div
              v-for="decision in recentDecisions"
              :key="decision.decision_id"
              class="mini-row"
            >
              <div>
                <strong>{{ instrumentLabel(decision.source_symbol) }}</strong>
                <p>
                  {{
                    translatedText(
                      decision.selection_reason ??
                        decision.rejection_reason ??
                        decision.source_report_name ??
                        "-",
                    )
                  }}
                </p>
              </div>
              <div class="mini-meta">
                <span :class="badgeToneClass(candidateTone(decision.candidate_status))">
                  {{ formatCandidateStatus(decision.candidate_status) }}
                </span>
                <span class="monospace">{{ formatDateTimeLocal(decision.created_at_utc) }}</span>
              </div>
            </div>
          </div>
          <p v-else class="helper-copy">표시할 최근 판단이 없습니다.</p>
        </div>
      </article>

      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>실시간 운영 로그</h3>
            <p class="helper-copy">루프 실행, 제어 변경, 판단, 주문, 체결, 브로커 통보를 시간순으로 묶었습니다.</p>
          </div>
          <span>{{ activityFeed.length }}건</span>
        </div>

        <div v-if="activityFeed.length" class="timeline">
          <div v-for="item in activityFeed" :key="item.key" class="timeline-row">
            <div class="timeline-stamp">{{ formatDateTimeLocal(item.happenedAt) }}</div>
            <div class="timeline-body">
              <div class="timeline-headline">
                <strong>{{ item.headline }}</strong>
                <span :class="badgeToneClass(item.tone)">{{ item.kind }}</span>
              </div>
              <p>{{ item.detail }}</p>
            </div>
          </div>
        </div>
        <p v-else class="helper-copy">아직 집계된 실시간 활동이 없습니다.</p>
      </article>

      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>막고 있는 이유와 운영 헬스</h3>
            <p class="helper-copy">지금 주문을 막는 요소와 각 서비스 상태를 같이 봅니다.</p>
          </div>
          <span>{{ activeIssues.length }}건</span>
        </div>

        <div class="health-grid">
          <div
            v-for="item in healthItems"
            :key="item.key"
            class="health-card"
          >
            <div class="health-card-head">
              <span>{{ item.label }}</span>
              <span :class="badgeToneClass(item.tone)">{{ item.value }}</span>
            </div>
            <p>{{ item.detail }}</p>
          </div>
        </div>

        <div class="subpanel">
          <div class="panel-header">
            <h4>현재 주의할 항목</h4>
            <span>{{ activeIssues.length }}</span>
          </div>
          <div v-if="activeIssues.length" class="mini-list">
            <div v-for="item in activeIssues" :key="item.key" class="mini-row">
              <div>
                <strong>{{ item.title }}</strong>
                <p>{{ item.detail }}</p>
              </div>
              <div class="mini-meta">
                <span :class="badgeToneClass(item.tone)">
                  {{ item.tone === "alert" ? "주의" : "확인" }}
                </span>
                <span class="monospace">{{ formatDateTimeLocal(item.stamp) }}</span>
              </div>
            </div>
          </div>
          <p v-else class="helper-copy">현재 눈에 띄는 차단 사유나 경고가 없습니다.</p>
        </div>
      </article>
    </div>

    <div class="dashboard-lower-grid">
      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>보유 포지션</h3>
            <p class="helper-copy">현재 실제 보유 수량과 평가 금액, 미실현 손익</p>
          </div>
          <span>{{ store.positions.length }}종목</span>
        </div>

        <div class="table-wrap">
          <table v-if="store.positions.length" class="table dense-table">
            <thead>
              <tr>
                <th>종목</th>
                <th>수량</th>
                <th>평가 금액</th>
                <th>미실현 손익</th>
                <th>업데이트</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="position in store.positions.slice(0, 8)" :key="position.symbol">
                <td>{{ instrumentLabel(position.symbol) }}</td>
                <td>{{ position.net_qty }}</td>
                <td>{{ formatCurrencyKrw(position.market_value_krw) }}</td>
                <td>{{ formatCurrencyKrw(position.unrealized_pnl_krw) }}</td>
                <td>{{ formatDateTimeLocal(position.updated_at_utc) }}</td>
              </tr>
            </tbody>
          </table>
          <p v-else class="helper-copy">현재 표시할 보유 포지션이 없습니다.</p>
        </div>
      </article>

      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>최근 주문 결과</h3>
            <p class="helper-copy">OMS 주문 상태와 최근 체결을 함께 봅니다.</p>
          </div>
          <span>{{ store.omsOrders.length + store.executionFills.length }}건</span>
        </div>

        <div class="subpanel">
          <div class="panel-header">
            <h4>OMS 주문</h4>
            <span>{{ store.omsOrders.length }}</span>
          </div>
          <div class="table-wrap">
            <table v-if="store.omsOrders.length" class="table dense-table">
              <thead>
                <tr>
                  <th>시각</th>
                  <th>종목</th>
                  <th>구분</th>
                  <th>상태</th>
                  <th>체결</th>
                  <th>주문번호</th>
                </tr>
              </thead>
              <tbody>
                <tr v-for="order in store.omsOrders.slice(0, 5)" :key="order.internal_order_id">
                  <td>{{ formatDateTimeLocal(order.last_event_at_utc) }}</td>
                  <td>{{ instrumentLabel(order.instrument_id) }}</td>
                  <td>{{ formatOrderSide(order.side_code) }}</td>
                  <td>{{ formatOrderState(order.order_state_code) }}</td>
                  <td>{{ order.filled_qty }}/{{ order.filled_qty + order.working_qty }}</td>
                  <td class="monospace">{{ order.broker_order_no ?? "-" }}</td>
                </tr>
              </tbody>
            </table>
            <p v-else class="helper-copy">아직 생성된 OMS 주문이 없습니다.</p>
          </div>
        </div>

        <div class="subpanel">
          <div class="panel-header">
            <h4>최근 체결</h4>
            <span>{{ store.executionFills.length }}</span>
          </div>
          <div class="table-wrap">
            <table v-if="store.executionFills.length" class="table dense-table">
              <thead>
                <tr>
                  <th>시각</th>
                  <th>종목</th>
                  <th>구분</th>
                  <th>수량</th>
                  <th>체결가</th>
                  <th>주문번호</th>
                </tr>
              </thead>
              <tbody>
                <tr
                  v-for="fill in store.executionFills.slice(0, 5)"
                  :key="`${fill.internal_order_id}-${fill.broker_trade_id ?? fill.fill_ts_utc}`"
                >
                  <td>{{ formatDateTimeLocal(fill.fill_ts_utc) }}</td>
                  <td>{{ instrumentLabel(fill.instrument_id) }}</td>
                  <td>{{ formatOrderSide(fill.side_code) }}</td>
                  <td>{{ fill.fill_qty }}</td>
                  <td>{{ formatCurrencyKrw(fill.fill_price) }}</td>
                  <td class="monospace">{{ fill.broker_order_no ?? "-" }}</td>
                </tr>
              </tbody>
            </table>
            <p v-else class="helper-copy">아직 확인된 체결 내역이 없습니다.</p>
          </div>
        </div>
      </article>
    </div>

    <StrategyGuidancePanel />
  </section>
</template>
