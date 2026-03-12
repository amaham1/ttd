<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";

import { useOpsStore } from "../stores/ops";
import {
  formatCandidateStatus,
  formatCurrencyKrw,
  formatDateTimeLocal,
  formatOrderSide,
  formatOrderState,
  formatQuoteBasis,
  formatRuleId,
  formatVenueHint,
} from "../utils/display";

type Tone = "ok" | "alert" | "neutral";

interface OrderFeedItem {
  key: string;
  happenedAt: string;
  kind: string;
  headline: string;
  detail: string;
  tone: Tone;
}

const store = useOpsStore();
const autoRefreshEnabled = ref(true);
const autoRefreshSeconds = 5;

let refreshTimer: number | null = null;

function instrumentLabel(symbol?: string | null): string {
  return store.formatInstrumentLabel(symbol);
}

function translatedText(value?: string | null): string {
  return store.translateText(value);
}

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

function decisionTone(status?: string | null): Tone {
  if (status === "SELECTED" || status === "SUBMITTED" || status === "VERIFIED") {
    return "ok";
  }
  if (status?.includes("REJECTED") || status === "BLOCKED" || status === "ERROR") {
    return "alert";
  }
  return "neutral";
}

const orderedDecisions = computed(() =>
  [...store.candidateDecisions].sort(
    (left, right) => parseTime(right.created_at_utc) - parseTime(left.created_at_utc),
  ),
);

const selectedDecision = computed(
  () =>
    orderedDecisions.value.find(
      (decision) => decision.candidate_id === store.latestPlan?.candidate_id,
    ) ??
    orderedDecisions.value.find((decision) => decision.candidate_status === "SELECTED") ??
    orderedDecisions.value[0] ??
    null,
);

const latestFill = computed(() => store.executionFills[0] ?? null);
const latestNotice = computed(() => store.brokerOrderNotices[0] ?? null);
const rejectedNoticeCount = computed(
  () => store.brokerOrderNotices.filter((notice) => notice.REJECT_YN === "Y").length,
);

const orderStats = computed(() => [
  {
    label: "최근 주문 ID",
    value: store.shadowSnapshot?.last_internal_order_id ?? "-",
    detail: `브로커 주문번호 ${store.shadowSnapshot?.last_broker_order_no ?? "-"}`,
  },
  {
    label: "최근 실행 상태",
    value: formatCandidateStatus(store.shadowSnapshot?.last_execution_status),
    detail: `누적 제출 ${store.shadowSnapshot?.submitted_count ?? 0}건`,
  },
  {
    label: "최근 체결",
    value: latestFill.value
      ? `${instrumentLabel(latestFill.value.instrument_id)} ${latestFill.value.fill_qty}주`
      : "없음",
    detail: latestFill.value
      ? `${formatOrderSide(latestFill.value.side_code)} / ${formatCurrencyKrw(
          latestFill.value.fill_price,
        )}`
      : "아직 체결이 없습니다.",
  },
  {
    label: "브로커 알림",
    value: `${store.brokerOrderNotices.length}건`,
    detail: `거부 ${rejectedNoticeCount.value}건`,
  },
  {
    label: "OMS 주문",
    value: `${store.omsOrders.length}건`,
    detail: `체결 ${store.executionFills.length}건`,
  },
  {
    label: "보유 포지션",
    value: `${store.positions.length}종목`,
    detail: `예상 현금 ${formatCurrencyKrw(store.tradingSnapshot?.projected_cash_krw)}`,
  },
]);

const orderActivityFeed = computed<OrderFeedItem[]>(() => {
  const items: OrderFeedItem[] = [];

  for (const decision of orderedDecisions.value.slice(0, 4)) {
    items.push({
      key: `decision-${decision.decision_id}`,
      happenedAt: decision.created_at_utc,
      kind: "판단",
      headline: `${instrumentLabel(decision.source_symbol)} ${formatCandidateStatus(decision.candidate_status)}`,
      detail: translatedText(
        decision.selection_reason ??
          decision.rejection_reason ??
          decision.source_report_name ??
          "판단 설명 없음",
      ),
      tone: decisionTone(decision.candidate_status),
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
      headline: `${instrumentLabel(notice.STCK_SHRN_ISCD)} ${notice.is_fill ? "체결 통보" : "주문 통보"}`,
      detail: `주문번호 ${notice.ODER_NO ?? "-"} / 수량 ${
        notice.CNTG_QTY ?? notice.ORD_QTY ?? "-"
      }주 / 가격 ${notice.CNTG_UNPR ?? notice.ORD_UNPR ?? "-"}`,
      tone: notice.REJECT_YN === "Y" ? "alert" : notice.is_fill ? "ok" : "neutral",
    });
  }

  return items
    .sort((left, right) => parseTime(right.happenedAt) - parseTime(left.happenedAt))
    .slice(0, 14);
});

async function refreshMonitor() {
  await store.refreshOrderMonitor();
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
    void store.refreshOrderMonitor();
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
  await refreshMonitor();
});

onBeforeUnmount(() => {
  stopAutoRefresh();
});
</script>

<template>
  <section class="stack dashboard-stack">
    <article class="panel compact-panel">
      <div class="panel-header">
        <div>
          <h3>실시간 주문 보드</h3>
          <p class="helper-copy">
            판단, 주문 계획, OMS 상태, 체결, 브로커 알림을 한 화면에서 추적합니다.
          </p>
        </div>
        <div class="panel-tools">
          <button
            class="secondary-button"
            :disabled="store.orderMonitorPending"
            @click="refreshMonitor"
          >
            지금 갱신
          </button>
          <button class="secondary-button" @click="autoRefreshEnabled = !autoRefreshEnabled">
            {{ autoRefreshEnabled ? `${autoRefreshSeconds}초 자동 갱신 켜짐` : "자동 갱신 꺼짐" }}
          </button>
        </div>
      </div>

      <div class="compact-stat-grid compact-stat-grid-6">
        <div v-for="item in orderStats" :key="item.label" class="compact-stat">
          <span>{{ item.label }}</span>
          <strong>{{ item.value }}</strong>
          <p class="compact-stat-detail">{{ item.detail }}</p>
        </div>
      </div>
    </article>

    <div class="ops-grid-3">
      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>주문 판단과 계획</h3>
            <p class="helper-copy">최근 후보 판단과 현재 주문 계획의 근거</p>
          </div>
          <span :class="badgeToneClass(decisionTone(selectedDecision?.candidate_status))">
            {{ formatCandidateStatus(selectedDecision?.candidate_status ?? store.latestPlan?.status) }}
          </span>
        </div>

        <div v-if="selectedDecision" class="focus-block">
          <div class="focus-header">
            <div>
              <strong>{{ instrumentLabel(selectedDecision.source_symbol) }}</strong>
              <p>{{ selectedDecision.source_report_name ?? "최근 판단 없음" }}</p>
            </div>
            <div class="focus-meta">
              <span class="tag">{{ formatDateTimeLocal(selectedDecision.created_at_utc) }}</span>
              <span class="tag">
                {{ formatRuleId(selectedDecision.matched_positive_rule_id ?? selectedDecision.matched_block_rule_id) }}
              </span>
            </div>
          </div>

          <div class="kv-grid">
            <div class="kv-cell">
              <span>후보 상태</span>
              <strong>{{ formatCandidateStatus(selectedDecision.candidate_status) }}</strong>
            </div>
            <div class="kv-cell">
              <span>주문 상태</span>
              <strong>{{ formatCandidateStatus(store.latestPlan?.status) }}</strong>
            </div>
            <div class="kv-cell">
              <span>가격 기준</span>
              <strong>{{ formatQuoteBasis(store.latestPlan?.quote_basis) }}</strong>
            </div>
            <div class="kv-cell">
              <span>선택 가격</span>
              <strong>{{ formatCurrencyKrw(store.latestPlan?.selected_price_krw) }}</strong>
            </div>
            <div class="kv-cell">
              <span>주문 수량</span>
              <strong>{{ store.latestPlan?.planned_order?.qty ?? "-" }}</strong>
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
                    selectedDecision.selection_reason ??
                      selectedDecision.rejection_reason ??
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
                      store.latestPlan?.reason ??
                      "최근 주문 근거가 없습니다.",
                  )
                }}
              </strong>
            </div>
          </div>
        </div>
        <p v-else class="helper-copy">표시할 최근 판단이 없습니다.</p>
      </article>

      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>실시간 주문 타임라인</h3>
            <p class="helper-copy">판단부터 브로커 통보까지 시간순으로 추적</p>
          </div>
          <span>{{ orderActivityFeed.length }}건</span>
        </div>

        <div v-if="orderActivityFeed.length" class="timeline">
          <div v-for="item in orderActivityFeed" :key="item.key" class="timeline-row">
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
        <p v-else class="helper-copy">아직 집계된 주문 활동이 없습니다.</p>
      </article>

      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>체결과 브로커 확인</h3>
            <p class="helper-copy">최근 체결과 브로커 수신 결과를 빠르게 점검</p>
          </div>
          <span>{{ store.executionFills.length + store.brokerOrderNotices.length }}건</span>
        </div>

        <div class="subpanel-stack">
          <div class="subpanel">
            <div class="panel-header">
              <h4>최근 체결</h4>
              <span>{{ latestFill ? "있음" : "없음" }}</span>
            </div>
            <div v-if="latestFill" class="kv-grid">
              <div class="kv-cell">
                <span>종목</span>
                <strong>{{ instrumentLabel(latestFill.instrument_id) }}</strong>
              </div>
              <div class="kv-cell">
                <span>구분</span>
                <strong>{{ formatOrderSide(latestFill.side_code) }}</strong>
              </div>
              <div class="kv-cell">
                <span>수량</span>
                <strong>{{ latestFill.fill_qty }}주</strong>
              </div>
              <div class="kv-cell">
                <span>체결가</span>
                <strong>{{ formatCurrencyKrw(latestFill.fill_price) }}</strong>
              </div>
            </div>
            <p v-else class="helper-copy">아직 확인된 체결이 없습니다.</p>
          </div>

          <div class="subpanel">
            <div class="panel-header">
              <h4>최근 브로커 알림</h4>
              <span>{{ latestNotice ? "있음" : "없음" }}</span>
            </div>
            <div v-if="latestNotice" class="kv-grid">
              <div class="kv-cell">
                <span>종목</span>
                <strong>{{ instrumentLabel(latestNotice.STCK_SHRN_ISCD) }}</strong>
              </div>
              <div class="kv-cell">
                <span>상태</span>
                <strong>
                  {{
                    latestNotice.REJECT_YN === "Y"
                      ? "거부"
                      : latestNotice.is_fill
                        ? "체결 통보"
                        : "주문 통보"
                  }}
                </strong>
              </div>
              <div class="kv-cell">
                <span>수량</span>
                <strong>{{ latestNotice.CNTG_QTY ?? latestNotice.ORD_QTY ?? "-" }}주</strong>
              </div>
              <div class="kv-cell">
                <span>주문번호</span>
                <strong>{{ latestNotice.ODER_NO ?? "-" }}</strong>
              </div>
            </div>
            <p v-else class="helper-copy">아직 수신된 브로커 알림이 없습니다.</p>
          </div>
        </div>
      </article>
    </div>

    <div class="ops-grid-2">
      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>보유 포지션</h3>
            <p class="helper-copy">현재 실제 보유 수량과 평가 손익</p>
          </div>
          <span>{{ store.positions.length }}종목</span>
        </div>

        <div class="table-wrap">
          <table v-if="store.positions.length" class="table dense-table">
            <thead>
              <tr>
                <th>종목</th>
                <th>수량</th>
                <th>평균 단가</th>
                <th>평가 금액</th>
                <th>미실현 손익</th>
              </tr>
            </thead>
            <tbody>
              <tr v-for="position in store.positions.slice(0, 8)" :key="position.symbol">
                <td>{{ instrumentLabel(position.symbol) }}</td>
                <td>{{ position.net_qty }}</td>
                <td>{{ formatCurrencyKrw(position.avg_cost_krw) }}</td>
                <td>{{ formatCurrencyKrw(position.market_value_krw) }}</td>
                <td>{{ formatCurrencyKrw(position.unrealized_pnl_krw) }}</td>
              </tr>
            </tbody>
          </table>
          <p v-else class="helper-copy">현재 표시할 보유 포지션이 없습니다.</p>
        </div>
      </article>

      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>상세 주문 / 체결 / 알림</h3>
            <p class="helper-copy">운영자가 바로 비교할 수 있게 최근 5건씩 압축 표시</p>
          </div>
          <span>{{ store.omsOrders.length + store.executionFills.length + store.brokerOrderNotices.length }}건</span>
        </div>

        <div class="subpanel-stack">
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
                  </tr>
                </thead>
                <tbody>
                  <tr v-for="order in store.omsOrders.slice(0, 5)" :key="order.internal_order_id">
                    <td>{{ formatDateTimeLocal(order.last_event_at_utc) }}</td>
                    <td>{{ instrumentLabel(order.instrument_id) }}</td>
                    <td>{{ formatOrderSide(order.side_code) }}</td>
                    <td>{{ formatOrderState(order.order_state_code) }}</td>
                    <td>{{ order.filled_qty }}/{{ order.filled_qty + order.working_qty }}</td>
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
                  </tr>
                </tbody>
              </table>
              <p v-else class="helper-copy">아직 확인된 체결 내역이 없습니다.</p>
            </div>
          </div>

          <div class="subpanel">
            <div class="panel-header">
              <h4>브로커 알림</h4>
              <span>{{ store.brokerOrderNotices.length }}</span>
            </div>
            <div class="table-wrap">
              <table v-if="store.brokerOrderNotices.length" class="table dense-table">
                <thead>
                  <tr>
                    <th>시각</th>
                    <th>종목</th>
                    <th>상태</th>
                    <th>수량</th>
                    <th>주문번호</th>
                  </tr>
                </thead>
                <tbody>
                  <tr
                    v-for="notice in store.brokerOrderNotices.slice(0, 5)"
                    :key="`${notice.ODER_NO ?? notice.ORGN_ODNO ?? 'notice'}-${notice.received_at_utc ?? notice.STCK_CNTG_HOUR}`"
                  >
                    <td>{{ formatDateTimeLocal(notice.received_at_utc ?? null) }}</td>
                    <td>{{ instrumentLabel(notice.STCK_SHRN_ISCD) }}</td>
                    <td>
                      {{
                        notice.REJECT_YN === "Y"
                          ? "거부"
                          : notice.is_fill
                            ? "체결 통보"
                            : "주문 통보"
                      }}
                    </td>
                    <td>{{ notice.CNTG_QTY ?? notice.ORD_QTY ?? "-" }}</td>
                    <td class="monospace">{{ notice.ODER_NO ?? "-" }}</td>
                  </tr>
                </tbody>
              </table>
              <p v-else class="helper-copy">아직 수신된 브로커 알림이 없습니다.</p>
            </div>
          </div>
        </div>
      </article>
    </div>
  </section>
</template>
