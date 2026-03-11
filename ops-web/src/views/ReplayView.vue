<script setup lang="ts">
import { useOpsStore } from "../stores/ops";
import { formatCommonCode, formatReplayJobId, formatReplayScenario } from "../utils/display";

const store = useOpsStore();
</script>

<template>
  <section class="stack">
    <article class="hero-card">
      <div>
        <p class="eyebrow">리플레이 제어</p>
        <h3>브로커 공백, 지연 응답, 정합성 복구 시나리오 검증</h3>
        <p class="hero-copy">
          저장된 리플레이 패키지를 기준으로 정합성 복구와 리스크 연동 흐름을 재현해
          실제 자동 루프 전에 점검할 수 있습니다.
        </p>
      </div>
      <button class="primary-button" @click="store.queueReplayJob()">리플레이 등록</button>
    </article>

    <article class="panel">
      <div class="panel-header">
        <h3>리플레이 작업</h3>
        <span>{{ store.replayJobs.length }}</span>
      </div>
      <table v-if="store.replayJobs.length" class="table">
        <thead>
          <tr>
            <th>작업 ID</th>
            <th>거래일</th>
            <th>시나리오</th>
            <th>상태</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="job in store.replayJobs" :key="job.replay_job_id">
            <td class="monospace">{{ formatReplayJobId(job.replay_job_id) }}</td>
            <td>{{ job.trading_date }}</td>
            <td>{{ formatReplayScenario(job.scenario) }}</td>
            <td>{{ formatCommonCode(job.status) }}</td>
          </tr>
        </tbody>
      </table>
      <p v-else class="helper-copy">등록된 리플레이 작업이 없습니다.</p>
    </article>
  </section>
</template>
