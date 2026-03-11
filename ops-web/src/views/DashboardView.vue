<script setup lang="ts">
import { computed } from "vue";

import { useOpsStore } from "../stores/ops";

const store = useOpsStore();

const cards = computed(() => {
  const summary = store.summary;
  if (!summary) {
    return [];
  }
  return [
    { label: "Enabled Strategies", value: summary.strategy_enabled_count },
    { label: "Open Breaks", value: summary.open_break_count },
    { label: "Risk Flags", value: summary.risk_flag_count },
    { label: "Active Positions", value: summary.active_position_count },
    { label: "Replay Jobs", value: summary.replay_job_count },
    { label: "Blocked Symbols", value: summary.blocked_symbol_count },
  ];
});
</script>

<template>
  <section class="stack">
    <div class="hero-card">
      <div>
        <p class="eyebrow">System Mode</p>
        <h3>{{ store.summary?.mode.mode ?? "LOADING" }}</h3>
        <p class="hero-copy">
          Reconciliation, risk, and broker state are surfaced here first so the
          operator can freeze entry before bad data propagates into OMS.
        </p>
      </div>
      <div class="hero-metric">
        <span>Updated</span>
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
          <h3>Risk Flags</h3>
          <span>{{ store.riskFlags.length }}</span>
        </div>
        <div class="list">
          <div v-for="flag in store.riskFlags" :key="`${flag.symbol}:${flag.flag_type}`" class="list-row">
            <div>
              <strong>{{ flag.symbol }}</strong>
              <p>{{ flag.flag_type }}</p>
            </div>
            <span :class="['badge', flag.hard_block ? 'badge-alert' : 'badge-neutral']">
              {{ flag.severity }}
            </span>
          </div>
        </div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <h3>Live Positions</h3>
          <span>{{ store.positions.length }}</span>
        </div>
        <div class="list">
          <div v-for="position in store.positions" :key="position.symbol" class="list-row">
            <div>
              <strong>{{ position.symbol }}</strong>
              <p>{{ position.net_qty }} shares</p>
            </div>
            <span class="monospace">{{ position.market_value_krw.toLocaleString() }} KRW</span>
          </div>
        </div>
      </article>
    </div>

    <div class="two-column">
      <article class="panel">
        <div class="panel-header">
          <h3>Service Snapshots</h3>
          <span>Live</span>
        </div>
        <div class="list">
          <div class="list-row">
            <div>
              <strong>Broker Gateway</strong>
              <p>{{ store.brokerSnapshot?.current_mode ?? "UNAVAILABLE" }}</p>
            </div>
            <span class="monospace">budget {{ store.brokerSnapshot?.pending_rate_budget ?? "-" }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>Trading Core</strong>
              <p>{{ store.tradingSnapshot?.operation_mode ?? "UNAVAILABLE" }}</p>
            </div>
            <span class="monospace">{{ store.tradingSnapshot?.projected_cash_krw?.toLocaleString?.() ?? "-" }} KRW</span>
          </div>
          <div class="list-row">
            <div>
              <strong>Market Intel</strong>
              <p>{{ store.marketSnapshot?.parser_mode ?? "UNAVAILABLE" }}</p>
            </div>
            <span class="monospace">backlog {{ store.marketSnapshot?.disclosure_backlog ?? "-" }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>Replay Runner</strong>
              <p>{{ store.replaySnapshot?.worker_mode ?? "UNAVAILABLE" }}</p>
            </div>
            <span class="monospace">active {{ store.replaySnapshot?.active_jobs ?? "-" }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>Shadow Live</strong>
              <p>{{ store.shadowSnapshot?.last_execution_status ?? store.shadowSnapshot?.mode ?? "UNAVAILABLE" }}</p>
            </div>
            <span class="monospace">
              loop {{ store.shadowLoopSnapshot?.running ? "RUNNING" : "STOPPED" }}
            </span>
          </div>
        </div>
      </article>

      <article class="panel">
        <div class="panel-header">
          <h3>Attention Queue</h3>
          <span>Operator</span>
        </div>
        <div class="list">
          <div class="list-row">
            <div>
              <strong>Low-confidence disclosures</strong>
              <p>Parser fallback and manual review candidates.</p>
            </div>
            <span class="badge badge-neutral">{{ store.marketSnapshot?.low_confidence_count ?? 0 }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>Open reconciliation breaks</strong>
              <p>Freeze entry when this count is non-zero.</p>
            </div>
            <span class="badge badge-alert">{{ store.summary?.open_break_count ?? 0 }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>Replay coverage</strong>
              <p>Use replay bundles before promotion to live size.</p>
            </div>
            <span class="badge badge-ok">{{ store.summary?.replay_job_count ?? 0 }}</span>
          </div>
          <div class="list-row">
            <div>
              <strong>Shadow loop status</strong>
              <p>Automated candidate evaluation and order planning worker.</p>
            </div>
            <span :class="['badge', store.shadowLoopSnapshot?.running ? 'badge-ok' : 'badge-neutral']">
              {{ store.shadowLoopSnapshot?.running ? "RUNNING" : "STOPPED" }}
            </span>
          </div>
        </div>
      </article>
    </div>
  </section>
</template>
