<script setup lang="ts">
import { useOpsStore } from "../stores/ops";

const store = useOpsStore();
</script>

<template>
  <section class="stack">
    <article class="panel">
      <div class="panel-header">
        <h3>Reconciliation Breaks</h3>
        <span>{{ store.breaks.length }}</span>
      </div>
      <div class="list">
        <div v-for="item in store.breaks" :key="item.break_id" class="list-row list-row-wide">
          <div>
            <strong>{{ item.break_id }}</strong>
            <p>{{ item.scope }}</p>
          </div>
          <div class="list-meta">
            <span class="badge badge-alert">{{ item.severity }}</span>
            <span class="monospace">{{ item.status }}</span>
          </div>
        </div>
      </div>
    </article>

    <article class="panel">
      <div class="panel-header">
        <h3>Risk Flag Feed</h3>
        <span>{{ store.riskFlags.length }}</span>
      </div>
      <div class="list">
        <div v-for="item in store.riskFlags" :key="`${item.symbol}:${item.flag_type}`" class="list-row list-row-wide">
          <div>
            <strong>{{ item.symbol }}</strong>
            <p>{{ item.source_system }} / {{ item.flag_type }}</p>
          </div>
          <div class="list-meta">
            <span :class="['badge', item.hard_block ? 'badge-alert' : 'badge-neutral']">
              {{ item.severity }}
            </span>
            <span class="monospace">{{ item.updated_at_utc }}</span>
          </div>
        </div>
      </div>
    </article>
  </section>
</template>
