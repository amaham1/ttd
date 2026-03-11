<script setup lang="ts">
import { useOpsStore } from "../stores/ops";

const store = useOpsStore();
</script>

<template>
  <section class="stack">
    <article class="panel">
      <div class="panel-header">
        <h3>Strategy Controls</h3>
        <span>{{ store.strategies.length }}</span>
      </div>
      <div class="list">
        <div v-for="strategy in store.strategies" :key="strategy.strategy_id" class="list-row">
          <div>
            <strong>{{ strategy.strategy_id }}</strong>
            <p>{{ strategy.updated_at_utc }}</p>
          </div>
          <span :class="['badge', strategy.enabled ? 'badge-ok' : 'badge-neutral']">
            {{ strategy.enabled ? "ENABLED" : "DISABLED" }}
          </span>
        </div>
      </div>
    </article>
  </section>
</template>
