<script setup lang="ts">
import { onMounted } from "vue";
import { RouterLink, RouterView } from "vue-router";

import { useOpsStore } from "./stores/ops";

const store = useOpsStore();

onMounted(async () => {
  await store.refreshAll();
});

const navItems = [
  { label: "Dashboard", to: "/dashboard" },
  { label: "Orders", to: "/orders" },
  { label: "Breaks", to: "/breaks" },
  { label: "Strategies", to: "/strategies" },
  { label: "Replay", to: "/replay" },
];
</script>

<template>
  <div class="shell">
    <aside class="sidebar">
      <div>
        <p class="eyebrow">KIS AI Trading</p>
        <h1>Ops Console</h1>
        <p class="sidebar-copy">
          Live control surface for KIS broker health, risk gates, replay, and
          reconciliation.
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
        {{ store.summary?.mode.mode ?? "LOADING" }}
      </div>
    </aside>
    <main class="content">
      <header class="topbar">
        <div>
          <p class="eyebrow">Operator View</p>
          <h2>Domestic equities v1 control plane</h2>
        </div>
        <button class="primary-button" :disabled="store.loading" @click="store.refreshAll">
          Refresh
        </button>
      </header>
      <RouterView />
    </main>
  </div>
</template>
