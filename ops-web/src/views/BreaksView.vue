<script setup lang="ts">
import { useOpsStore } from "../stores/ops";
import {
  formatBreakId,
  formatBreakScope,
  formatCommonCode,
  formatRiskFlagType,
  formatSeverity,
  formatSourceSystem,
} from "../utils/display";

const store = useOpsStore();
</script>

<template>
  <section class="stack">
    <article class="panel">
      <div class="panel-header">
        <h3>정합성 브레이크</h3>
        <span>{{ store.breaks.length }}</span>
      </div>
      <div v-if="store.breaks.length" class="list">
        <div v-for="item in store.breaks" :key="item.break_id" class="list-row list-row-wide">
          <div>
            <strong>{{ formatBreakId(item.break_id) }}</strong>
            <p>{{ formatBreakScope(item.scope) }}</p>
          </div>
          <div class="list-meta">
            <span class="badge badge-alert">{{ formatSeverity(item.severity) }}</span>
            <span class="monospace">{{ formatCommonCode(item.status) }}</span>
          </div>
        </div>
      </div>
      <p v-else class="helper-copy">현재 감지된 정합성 브레이크가 없습니다.</p>
    </article>

    <article class="panel">
      <div class="panel-header">
        <h3>리스크 플래그 원본</h3>
        <span>{{ store.riskFlags.length }}</span>
      </div>
      <div v-if="store.riskFlags.length" class="list">
        <div v-for="item in store.riskFlags" :key="`${item.symbol}:${item.flag_type}`" class="list-row list-row-wide">
          <div>
            <strong>{{ item.symbol }}</strong>
            <p>{{ formatSourceSystem(item.source_system) }} / {{ formatRiskFlagType(item.flag_type) }}</p>
          </div>
          <div class="list-meta">
            <span :class="['badge', item.hard_block ? 'badge-alert' : 'badge-neutral']">
              {{ formatSeverity(item.severity) }}
            </span>
            <span class="monospace">{{ item.updated_at_utc }}</span>
          </div>
        </div>
      </div>
      <p v-else class="helper-copy">현재 수집된 리스크 플래그가 없습니다.</p>
    </article>
  </section>
</template>
