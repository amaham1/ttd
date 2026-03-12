<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from "vue";

import { useOpsStore } from "../stores/ops";
import {
  formatBreakId,
  formatBreakScope,
  formatCandidateStatus,
  formatCommonCode,
  formatDateTimeLocal,
  formatRiskFlagType,
  formatSeverity,
  formatSourceSystem,
} from "../utils/display";

type Tone = "ok" | "alert" | "neutral";

interface AlertFeedItem {
  key: string;
  happenedAt: string;
  title: string;
  detail: string;
  tone: Tone;
}

const store = useOpsStore();
const autoRefreshEnabled = ref(true);
const autoRefreshSeconds = 5;
const resolvingBreakId = ref<string | null>(null);
const breakActionError = ref<string | null>(null);

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

async function refreshBreaks() {
  await store.refreshDashboard();
}

async function resolveBreak(breakId: string) {
  const normalizedBreakId = String(breakId ?? "").trim();
  if (!normalizedBreakId || resolvingBreakId.value) {
    return;
  }

  resolvingBreakId.value = normalizedBreakId;
  breakActionError.value = null;
  try {
    await store.resolveReconciliationBreak(normalizedBreakId);
  } catch (error) {
    breakActionError.value = error instanceof Error ? error.message : "정합성 브레이크 해소에 실패했습니다.";
  } finally {
    resolvingBreakId.value = null;
  }
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
  await refreshBreaks();
});

onBeforeUnmount(() => {
  stopAutoRefresh();
});

const hardBlockFlags = computed(() => store.riskFlags.filter((item) => item.hard_block));

const breakStats = computed(() => [
  {
    label: "미해결 브레이크",
    value: `${store.breaks.length}건`,
    detail: store.breaks.length > 0 ? "주문 재개 전 원인 확인 필요" : "현재 열린 브레이크 없음",
  },
  {
    label: "하드 블록",
    value: `${hardBlockFlags.value.length}건`,
    detail: `전체 리스크 플래그 ${store.riskFlags.length}건`,
  },
  {
    label: "최근 실행 상태",
    value: formatCandidateStatus(store.shadowSnapshot?.last_execution_status),
    detail: translatedText(store.shadowSnapshot?.last_execution_reason ?? "최근 실행 사유 없음"),
  },
  {
    label: "루프 최근 결과",
    value: formatCommonCode(store.shadowLoopSnapshot?.last_result_status),
    detail: `최근 종료 ${formatDateTimeLocal(store.shadowLoopSnapshot?.last_finished_at_utc)}`,
  },
  {
    label: "브로커 상태",
    value: formatCommonCode(store.brokerSnapshot?.current_mode),
    detail: translatedText(store.brokerSnapshot?.degraded_reason ?? "성능 저하 신호 없음"),
  },
  {
    label: "마켓 인텔",
    value: formatCommonCode(store.marketSnapshot?.parser_mode),
    detail: `저신뢰 ${store.marketSnapshot?.low_confidence_count ?? 0}건`,
  },
]);

const guardReasons = computed(() =>
  (store.latestPlan?.risk_reason_summary ?? "")
    .split(",")
    .map((item) => item.trim())
    .filter((item) => item.length > 0),
);

const actionItems = computed(() => {
  const items: { key: string; title: string; detail: string; tone: Tone }[] = [];

  if (store.breaks.length > 0) {
    items.push({
      key: "break-gate",
      title: "브레이크가 열려 있습니다",
      detail: "브레이크가 0건이 될 때까지 신규 진입보다 원인 확인과 정합성 점검이 우선입니다.",
      tone: "alert",
    });
  }

  if (hardBlockFlags.value.length > 0) {
    items.push({
      key: "hard-block",
      title: "하드 블록 리스크가 있습니다",
      detail: "킬 스위치, 세션 가드, WS 중단, 지연 호가 같은 플래그는 주문을 직접 막습니다.",
      tone: "alert",
    });
  }

  for (const reason of guardReasons.value.slice(0, 3)) {
    items.push({
      key: `guard-${reason}`,
      title: "현재 주문 차단 사유",
      detail: translatedText(reason),
      tone: "alert",
    });
  }

  if (items.length === 0) {
    items.push({
      key: "clear",
      title: "즉시 조치가 필요한 경고는 없습니다",
      detail: "그래도 주문 전에는 브레이크, 리스크 플래그, 브로커 WS 상태를 같이 확인하는 편이 안전합니다.",
      tone: "ok",
    });
  }

  return items;
});

const alertTimeline = computed<AlertFeedItem[]>(() => {
  const items: AlertFeedItem[] = [];

  for (const item of store.breaks) {
    items.push({
      key: `break-${item.break_id}`,
      happenedAt: item.detected_at_utc,
      title: `${formatSeverity(item.severity)} 브레이크`,
      detail: `${formatBreakId(item.break_id)} / ${formatBreakScope(item.scope)}`,
      tone: "alert",
    });
  }

  for (const item of store.riskFlags) {
    items.push({
      key: `risk-${item.symbol}-${item.flag_type}`,
      happenedAt: item.updated_at_utc,
      title: `${instrumentLabel(item.symbol)} 리스크`,
      detail: `${formatSourceSystem(item.source_system)} / ${formatRiskFlagType(item.flag_type)}`,
      tone: item.hard_block ? "alert" : "neutral",
    });
  }

  return items
    .sort((left, right) => parseTime(right.happenedAt) - parseTime(left.happenedAt))
    .slice(0, 12);
});
</script>

<template>
  <section class="stack dashboard-stack">
    <article class="panel compact-panel">
      <div class="panel-header">
        <div>
          <h3>브레이크 / 리스크 보드</h3>
          <p class="helper-copy">
            지금 무엇이 주문을 막고 있는지, 어떤 조치를 먼저 해야 하는지 한 화면에서 봅니다.
          </p>
        </div>
        <div class="panel-tools">
          <button
            class="secondary-button"
            :disabled="store.dashboardPending"
            @click="refreshBreaks"
          >
            지금 갱신
          </button>
          <button class="secondary-button" @click="autoRefreshEnabled = !autoRefreshEnabled">
            {{ autoRefreshEnabled ? `${autoRefreshSeconds}초 자동 갱신 켜짐` : "자동 갱신 꺼짐" }}
          </button>
        </div>
      </div>

      <div class="compact-stat-grid compact-stat-grid-6">
        <div v-for="item in breakStats" :key="item.label" class="compact-stat">
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
            <h3>미해결 브레이크</h3>
            <p class="helper-copy">정합성 불일치와 주문 중단 요소</p>
          </div>
          <span>{{ store.breaks.length }}건</span>
        </div>

        <p v-if="breakActionError" class="helper-copy text-danger">{{ breakActionError }}</p>

        <div v-if="store.breaks.length" class="mini-list">
          <div v-for="item in store.breaks" :key="item.break_id" class="mini-row">
            <div>
              <strong>{{ formatBreakId(item.break_id) }}</strong>
              <p>{{ formatBreakScope(item.scope) }}</p>
            </div>
            <div class="mini-meta">
              <span class="badge badge-alert">{{ formatSeverity(item.severity) }}</span>
              <span class="monospace">{{ formatDateTimeLocal(item.detected_at_utc) }}</span>
              <button
                class="secondary-button"
                :disabled="resolvingBreakId !== null"
                @click="resolveBreak(item.break_id)"
              >
                {{ resolvingBreakId === item.break_id ? "해소 중..." : "해소" }}
              </button>
            </div>
          </div>
        </div>
        <p v-else class="helper-copy">현재 감지된 정합성 브레이크가 없습니다.</p>
      </article>

      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>리스크 플래그</h3>
            <p class="helper-copy">주문을 차단하거나 주의가 필요한 실시간 플래그</p>
          </div>
          <span>{{ store.riskFlags.length }}건</span>
        </div>

        <div v-if="store.riskFlags.length" class="mini-list">
          <div
            v-for="item in store.riskFlags"
            :key="`${item.symbol}-${item.flag_type}`"
            class="mini-row"
          >
            <div>
              <strong>{{ instrumentLabel(item.symbol) }}</strong>
              <p>{{ formatSourceSystem(item.source_system) }} / {{ formatRiskFlagType(item.flag_type) }}</p>
            </div>
            <div class="mini-meta">
              <span :class="badgeToneClass(item.hard_block ? 'alert' : 'neutral')">
                {{ item.hard_block ? "하드 블록" : formatSeverity(item.severity) }}
              </span>
              <span class="monospace">{{ formatDateTimeLocal(item.updated_at_utc) }}</span>
            </div>
          </div>
        </div>
        <p v-else class="helper-copy">현재 감지된 리스크 플래그가 없습니다.</p>
      </article>

      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>지금 먼저 볼 것</h3>
            <p class="helper-copy">운영자가 우선 순위대로 확인할 항목</p>
          </div>
          <span>{{ actionItems.length }}개</span>
        </div>

        <div class="mini-list">
          <div v-for="item in actionItems" :key="item.key" class="mini-row">
            <div>
              <strong>{{ item.title }}</strong>
              <p>{{ item.detail }}</p>
            </div>
            <div class="mini-meta">
              <span :class="badgeToneClass(item.tone)">
                {{ item.tone === "alert" ? "우선 확인" : "정상" }}
              </span>
            </div>
          </div>
        </div>
      </article>
    </div>

    <div class="ops-grid-2">
      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>실시간 경고 타임라인</h3>
            <p class="helper-copy">브레이크와 리스크 플래그를 시간순으로 묶었습니다.</p>
          </div>
          <span>{{ alertTimeline.length }}건</span>
        </div>

        <div v-if="alertTimeline.length" class="timeline">
          <div v-for="item in alertTimeline" :key="item.key" class="timeline-row">
            <div class="timeline-stamp">{{ formatDateTimeLocal(item.happenedAt) }}</div>
            <div class="timeline-body">
              <div class="timeline-headline">
                <strong>{{ item.title }}</strong>
                <span :class="badgeToneClass(item.tone)">
                  {{ item.tone === "alert" ? "경고" : "확인" }}
                </span>
              </div>
              <p>{{ item.detail }}</p>
            </div>
          </div>
        </div>
        <p v-else class="helper-copy">표시할 경고 타임라인이 없습니다.</p>
      </article>

      <article class="panel compact-panel">
        <div class="panel-header">
          <div>
            <h3>운영 참고 상태</h3>
            <p class="helper-copy">브레이크를 볼 때 같이 확인하면 좋은 현재 상태</p>
          </div>
          <span>참고</span>
        </div>

        <div class="health-grid">
          <div class="health-card">
            <div class="health-card-head">
              <span>최근 실행</span>
              <span :class="badgeToneClass(store.shadowSnapshot?.last_execution_status ? 'alert' : 'neutral')">
                {{ formatCandidateStatus(store.shadowSnapshot?.last_execution_status) }}
              </span>
            </div>
            <p>{{ translatedText(store.shadowSnapshot?.last_execution_reason ?? "최근 실행 사유가 없습니다.") }}</p>
          </div>

          <div class="health-card">
            <div class="health-card-head">
              <span>루프 최근 결과</span>
              <span :class="badgeToneClass(store.shadowLoopSnapshot?.last_error ? 'alert' : 'neutral')">
                {{ formatCommonCode(store.shadowLoopSnapshot?.last_result_status) }}
              </span>
            </div>
            <p>{{ translatedText(store.shadowLoopSnapshot?.last_error ?? "최근 루프 오류가 없습니다.") }}</p>
          </div>

          <div class="health-card">
            <div class="health-card-head">
              <span>브로커 상태</span>
              <span :class="badgeToneClass(store.brokerSnapshot?.degraded_reason ? 'alert' : 'ok')">
                {{ formatCommonCode(store.brokerSnapshot?.current_mode) }}
              </span>
            </div>
            <p>{{ translatedText(store.brokerSnapshot?.degraded_reason ?? "브로커 성능 저하 신호가 없습니다.") }}</p>
          </div>
        </div>
      </article>
    </div>
  </section>
</template>
