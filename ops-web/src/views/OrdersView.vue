<script setup lang="ts">
import { computed } from "vue";

import { useOpsStore } from "../stores/ops";
import {
  formatCandidateStatus,
  formatCurrencyKrw,
  formatQuoteBasis,
  formatRuleId,
  formatVenueHint,
} from "../utils/display";

const store = useOpsStore();

const selectedDecision = computed(() =>
  store.candidateDecisions.find((decision) => decision.candidate_id === store.latestPlan?.candidate_id) ??
  store.candidateDecisions.find((decision) => decision.candidate_status === "SELECTED") ??
  null,
);
</script>

<template>
  <section class="stack">
    <article class="panel">
      <div class="panel-header">
        <h3>최근 주문 계획 근거</h3>
        <span>{{ formatCandidateStatus(store.latestPlan?.status) }}</span>
      </div>
      <div v-if="store.latestPlan" class="detail-grid">
        <div>
          <p class="detail-label">후보 ID</p>
          <strong class="monospace">{{ store.latestPlan.candidate_id }}</strong>
        </div>
        <div>
          <p class="detail-label">규칙 ID</p>
          <strong class="monospace">{{ formatRuleId(store.latestPlan.matched_rule_id) }}</strong>
        </div>
        <div>
          <p class="detail-label">가격 기준</p>
          <strong>{{ formatQuoteBasis(store.latestPlan.quote_basis) }}</strong>
        </div>
        <div>
          <p class="detail-label">선택 가격</p>
          <strong>{{ formatCurrencyKrw(store.latestPlan.selected_price_krw) }}</strong>
        </div>
        <div>
          <p class="detail-label">주문 수량</p>
          <strong>{{ store.latestPlan.planned_order?.qty ?? "-" }}</strong>
        </div>
        <div>
          <p class="detail-label">거래 시장</p>
          <strong>{{ formatVenueHint(store.latestPlan.planned_order?.venue_hint as string | undefined) }}</strong>
        </div>
        <div class="detail-span-2">
          <p class="detail-label">가격 결정 이유</p>
          <strong>{{ store.latestPlan.price_reason ?? "-" }}</strong>
        </div>
        <div class="detail-span-2">
          <p class="detail-label">수량 결정 이유</p>
          <strong>{{ store.latestPlan.quantity_reason ?? "-" }}</strong>
        </div>
        <div class="detail-span-2">
          <p class="detail-label">리스크 요약</p>
          <strong>{{ store.latestPlan.risk_reason_summary ?? "-" }}</strong>
        </div>
      </div>
      <p v-else class="helper-copy">아직 생성된 주문 계획이 없습니다.</p>
    </article>

    <article class="panel">
      <div class="panel-header">
        <h3>후보 문맥</h3>
        <span>{{ formatCandidateStatus(selectedDecision?.candidate_status) }}</span>
      </div>
      <div v-if="selectedDecision" class="detail-grid">
        <div>
          <p class="detail-label">종목 코드</p>
          <strong class="monospace">{{ selectedDecision.source_symbol ?? "-" }}</strong>
        </div>
        <div>
          <p class="detail-label">접수 번호</p>
          <strong class="monospace">{{ selectedDecision.source_receipt_no ?? "-" }}</strong>
        </div>
        <div class="detail-span-2">
          <p class="detail-label">공시명</p>
          <strong>{{ selectedDecision.source_report_name ?? "-" }}</strong>
        </div>
        <div class="detail-span-2">
          <p class="detail-label">후보 판단 이유</p>
          <strong>{{ selectedDecision.selection_reason ?? selectedDecision.rejection_reason ?? "-" }}</strong>
        </div>
      </div>
      <p v-else class="helper-copy">연결된 후보 설명이 없습니다.</p>
    </article>

    <article class="panel">
      <div class="panel-header">
        <h3>보유 포지션</h3>
        <span>{{ store.positions.length }}</span>
      </div>
      <table v-if="store.positions.length" class="table">
        <thead>
          <tr>
            <th>종목</th>
            <th>수량</th>
            <th>평균 단가</th>
            <th>평가 금액</th>
            <th>평가 손익</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="position in store.positions" :key="position.symbol">
            <td>{{ position.symbol }}</td>
            <td>{{ position.net_qty }}</td>
            <td>{{ formatCurrencyKrw(position.avg_cost_krw) }}</td>
            <td>{{ formatCurrencyKrw(position.market_value_krw) }}</td>
            <td>{{ formatCurrencyKrw(position.unrealized_pnl_krw) }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="helper-copy">현재 표시할 보유 포지션이 없습니다.</p>
    </article>
  </section>
</template>
