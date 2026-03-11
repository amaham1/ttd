import { createRouter, createWebHistory } from "vue-router";

import BreaksView from "./views/BreaksView.vue";
import DashboardView from "./views/DashboardView.vue";
import ExplainabilityView from "./views/ExplainabilityView.vue";
import OrdersView from "./views/OrdersView.vue";
import ResearchView from "./views/ResearchView.vue";
import ReplayView from "./views/ReplayView.vue";
import StrategiesView from "./views/StrategiesView.vue";

export const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: "/", redirect: "/dashboard" },
    { path: "/dashboard", component: DashboardView },
    { path: "/explainability", component: ExplainabilityView },
    { path: "/research", component: ResearchView },
    { path: "/orders", component: OrdersView },
    { path: "/breaks", component: BreaksView },
    { path: "/strategies", component: StrategiesView },
    { path: "/replay", component: ReplayView },
  ],
});
