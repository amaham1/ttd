<script setup lang="ts">
import { computed } from "vue";

import AutonomousLoopPanel from "../components/AutonomousLoopPanel.vue";
import { useOpsStore } from "../stores/ops";
import {
  formatCandidateStatus,
  formatCurrencyKrw,
  formatCommonCode,
  formatOperationMode,
  formatQuoteBasis,
  formatRiskFlagType,
  formatRuleId,
  formatSeverity,
} from "../utils/display";

const store = useOpsStore();

const selectedDecision = computed(() =>
  store.candidateDecisions.find((decision) => decision.candidate_status === "SELECTED") ??
  store.candidateDecisions[0] ??
  null,
);

const cards = computed(() => {
  const summary = store.summary;
  if (!summary) {
    return [];
  }
  return [
    { label: "활성 전략", value: summary.strategy_enabled_count },
    { label: "미해결 브레이크", value: summary.open_break_count },
    { label: "리스크 플래그", value: summary.risk_flag_count },
    { label: "보유 종목", value: summary.active_position_count },
    { label: "리플레이 작업", value: summary.replay_job_count },
    { label: "차단 종목", value: summary.blocked_symbol_count },
  ];
});
</script>

<template>
  <section class="stack">
    <AutonomousLoopPanel />

    <div class="hero-card">
      <div>
        <p class="eyebrow">시스템 상태</p>
        <h3>{{ formatOperationMode(store.summary?.mode.mode) }}</h3>
        <p class="hero-copy">
          브로커 상태, 리스크 게이트, 후보 선정, 최근 주문 계획을 한 번에 확인해
          자동 루프 투입 전에 빠르게 점검할 수 있게 구성했습니다.
        </p>
      </div>
      <div class="hero-metric">
        <span>최근 갱신</span>
        <strong>{{ store.summary?.mode.updated_at_utc ?? "-" }}</strong>
      </div>
    </div>

    <div class="metric-grid">
      <article v-for="card in cards" :key="card.label" class="metric-card">
        <p class="metric-label">{{ card.label }}</p>
        <strong class="metric-value">{{ card.value }}</strong>
      </article>
    </div>

    <div class="two-column">
      <article class="panel">
        <div class="panel-header">
          <h3>리스크 플래그</h3>
          <span>{{ store.riskFlags.length }}</span>
        </div>
        <div v-if="store.riskFlags.length" class="list">
          <div v-for="flag in store.riskFlags" :key="`${flag.symbol}:${flag.flag_type}`" class="list-row">
            <div>
              <strong>{{ flag.symbol }}</strong>
              <p>{{ formatRiskFlagType(flag.flag_type) }}</p>
            </div>
            <span :class="['badge', flag.hard_block ? 'badge-alert' : 'badge-neutral']">
              {{ formatSeverity(flag.severity) }}
            </span>
          </div>
        </div>
        <p v-else class="helper-copy">현재 활성 리스크 플래그가 없습니다.</p>
      </article>

      <article class="panel">
        <div class="panel-header">
          <h3>보유 포지션</h3>
          <span>{{ store.positions.length }}</span>
        </div>
        <div v-if="store.positions.length" class="list">
          <div v-for="position in store.positions" :key="position.symbol" class="list-row">
            <div>
              <strong>{{ position.symbol }}</strong>
              <p>{{ position.net_qty }}주</p>
            </div>
            <span class="monospace">{{ formatCurrencyKrw(position.market_value_krw) }}</span>
          </div>
        </div>
        <p v-else class="helper-copy">현재 보유 포지션이 없습니다.</p>
      </article>
    </div>

    <div class="two-column">
      <article class="panel">
        <div class="panel-header">
          <h3>선정 후보 요약</h3>
          <span>{{ formatCandidateStatus(selectedDecision?.candidate_status) }}</span>
        </div>
        <div v-if="selectedDecision" class="stack compact-stack">
          <div class="list-row list-row-wide emphasis-row">
            <div>
              <strong>{{ selectedDecision.source_symbol ?? "-" }}</strong>
              <p>{{ selectedDecision.source_report_name ?? "-" }}</p>
            </div>
            <div class="list-meta">
              <span class="badge badge-ok">
                {{ formatRuleId(selectedDecision.matched_positive_rule_id ?? selectedDecision.matched_block_rule_id) }}
              </span>
              <span class="monospace">{{ selectedDecision.source_receipt_no ?? "-" }}</span>
            </div>
          </div>
          <div class="callout callout-positive">
            <p class="detail-label">선정 이유</p>
            <strong>{{ selectedDecision.selection_reason ?? selectedDecision.rejection_reason ?? "-" }}</strong>
          </div>
        </div>
        <p v-else class="helper-copy">후보 설명 기록이 아직 없습니다.</p>
      </article>

      <article class="panel">
        <div class="panel-header">
          <h3>서비스 상태</h3>
          <span>실시간</span>
        </div>
        <div class="list">
          <div class="list-row">
            <div>
              <strong>브로커 게이트웨이</strong>
              <p>{{ formatCommonCode(store.brokerSnapshot?.current_mode) }}</p>
            </div>
            <span class="monospace">예산 {{ store.brokerSnapshot?.pending_rate_budget ?? "-" }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>트레이딩 코어</strong>
              <p>{{ formatOperationMode(store.tradingSnapshot?.operation_mode) }}</p>
            </div>
            <span class="monospace">{{ formatCurrencyKrw(store.tradingSnapshot?.projected_cash_krw) }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>마켓 인텔</strong>
              <p>{{ formatCommonCode(store.marketSnapshot?.parser_mode) }}</p>
            </div>
            <span class="monospace">적체 {{ store.marketSnapshot?.disclosure_backlog ?? "-" }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>리플레이 러너</strong>
              <p>{{ formatCommonCode(store.replaySnapshot?.worker_mode) }}</p>
            </div>
            <span class="monospace">실행 중 {{ store.replaySnapshot?.active_jobs ?? "-" }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>섀도우 라이브</strong>
              <p>{{ formatCandidateStatus(store.shadowSnapshot?.last_execution_status ?? store.shadowSnapshot?.mode) }}</p>
            </div>
            <span class="monospace">루프 {{ store.shadowLoopSnapshot?.running ? "실행 중" : "중지" }}</span>
          </div>
        </div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <h3>운영 확인 항목</h3>
          <span>체크</span>
        </div>
        <div class="list">
          <div class="list-row">
            <div>
              <strong>저신뢰 공시</strong>
              <p>수동 검토가 필요한 공시 적체입니다.</p>
            </div>
            <span class="badge badge-neutral">{{ store.marketSnapshot?.low_confidence_count ?? 0 }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>미해결 브레이크</strong>
              <p>값이 0이 아니면 신규 진입을 멈추고 원인을 먼저 확인해야 합니다.</p>
            </div>
            <span class="badge badge-alert">{{ store.summary?.open_break_count ?? 0 }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>리플레이 검증 범위</strong>
              <p>현재까지 누적된 재현 검증 작업 수입니다.</p>
            </div>
            <span class="badge badge-ok">{{ store.summary?.replay_job_count ?? 0 }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>최근 주문 근거</strong>
              <p>{{ store.latestPlan?.price_reason ?? "아직 생성된 주문 계획이 없습니다." }}</p>
            </div>
            <span :class="['badge', store.latestPlan ? 'badge-ok' : 'badge-neutral']">
              {{ formatQuoteBasis(store.latestPlan?.quote_basis) }}
            </span>
          </div>
        </div>
      </article>
    </div>
  </section>
</template>
