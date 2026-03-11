<script setup lang="ts">
import { useOpsStore } from "../stores/ops";
import { formatStrategyEnabled, formatStrategyId } from "../utils/display";

const store = useOpsStore();
</script>

<template>
  <section class="stack">
    <article class="panel">
      <div class="panel-header">
        <h3>전략 제어</h3>
        <span>{{ store.strategies.length }}</span>
      </div>
      <div v-if="store.strategies.length" class="list">
        <div v-for="strategy in store.strategies" :key="strategy.strategy_id" class="list-row">
          <div>
            <strong>{{ formatStrategyId(strategy.strategy_id) }}</strong>
            <p>{{ strategy.updated_at_utc }}</p>
          </div>
          <span :class="['badge', strategy.enabled ? 'badge-ok' : 'badge-neutral']">
            {{ formatStrategyEnabled(strategy.enabled) }}
          </span>
        </div>
      </div>
      <p v-else class="helper-copy">등록된 전략이 없습니다.</p>
    </article>
  </section>
</template>
