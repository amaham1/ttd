import { createRouter, createWebHistory } from "vue-router";

import BreaksView from "./views/BreaksView.vue";
import DashboardView from "./views/DashboardView.vue";
import OrdersView from "./views/OrdersView.vue";
import ReplayView from "./views/ReplayView.vue";
import StrategiesView from "./views/StrategiesView.vue";

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/", redirect: "/dashboard" },
    { path: "/dashboard", component: DashboardView },
    { path: "/orders", component: OrdersView },
    { path: "/breaks", component: BreaksView },
    { path: "/strategies", component: StrategiesView },
    { path: "/replay", component: ReplayView },
  ],
});
