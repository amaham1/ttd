<script setup lang="ts">
import { onMounted } from "vue";
import { RouterLink, RouterView } from "vue-router";

import { useOpsStore } from "./stores/ops";
import { formatOperationMode } from "./utils/display";

const store = useOpsStore();

onMounted(async () => {
  await store.refreshAll();
});

const navItems = [
  { label: "대시보드", to: "/dashboard" },
  { label: "설명 가능성", to: "/explainability" },
  { label: "리서치", to: "/research" },
  { label: "주문", to: "/orders" },
  { label: "브레이크", to: "/breaks" },
  { label: "전략", to: "/strategies" },
  { label: "리플레이", to: "/replay" },
];
</script>

<template>
  <div class="shell">
    <aside class="sidebar">
      <div>
        <p class="eyebrow">한국투자증권 자동매매</p>
        <h1>운영 콘솔</h1>
        <p class="sidebar-copy">
          브로커 상태, 리스크 게이트, 자동 루프, 리플레이, 설명 가능성, 리서치 지표를
          한 화면에서 확인합니다.
        </p>
      </div>
      <nav class="nav">
        <RouterLink
          v-for="item in navItems"
          :key="item.to"
          :to="item.to"
          class="nav-link"
          active-class="nav-link-active"
        >
          {{ item.label }}
        </RouterLink>
      </nav>
      <div class="status-chip">
        <span class="status-dot"></span>
        {{ formatOperationMode(store.summary?.mode.mode) || "불러오는 중" }}
      </div>
    </aside>
    <main class="content">
      <header class="topbar">
        <div>
          <p class="eyebrow">운영 관리</p>
          <h2>한국 주식 통합 제어 화면</h2>
        </div>
        <button class="primary-button" :disabled="store.loading" @click="store.refreshAll">
          새로고침
        </button>
      </header>
      <RouterView />
    </main>
  </div>
</template>
