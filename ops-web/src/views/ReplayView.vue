<script setup lang="ts">
import { useOpsStore } from "../stores/ops";

const store = useOpsStore();
</script>

<template>
  <section class="stack">
    <article class="hero-card">
      <div>
        <p class="eyebrow">Replay Control</p>
        <h3>Scenario injection for broker gaps and delayed ACKs</h3>
        <p class="hero-copy">
          Queue a new replay package to validate reconciliation, risk freeze,
          and projection rebuild behavior.
        </p>
      </div>
      <button class="primary-button" @click="store.queueReplayJob()">Queue Replay</button>
    </article>

    <article class="panel">
      <div class="panel-header">
        <h3>Replay Jobs</h3>
        <span>{{ store.replayJobs.length }}</span>
      </div>
      <table class="table">
        <thead>
          <tr>
            <th>Replay Job</th>
            <th>Trading Date</th>
            <th>Scenario</th>
            <th>Status</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="job in store.replayJobs" :key="job.replay_job_id">
            <td class="monospace">{{ job.replay_job_id }}</td>
            <td>{{ job.trading_date }}</td>
            <td>{{ job.scenario }}</td>
            <td>{{ job.status }}</td>
          </tr>
        </tbody>
      </table>
    </article>
  </section>
</template>
