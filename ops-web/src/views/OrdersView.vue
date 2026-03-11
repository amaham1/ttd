<script setup lang="ts">
import { useOpsStore } from "../stores/ops";

const store = useOpsStore();
</script>

<template>
  <section class="stack">
    <article class="panel">
      <div class="panel-header">
        <h3>Representative Order Trace</h3>
        <span>{{ store.orderDemo?.state ?? "UNKNOWN" }}</span>
      </div>
      <div v-if="store.orderDemo" class="detail-grid">
        <div>
          <p class="detail-label">Order ID</p>
          <strong class="monospace">{{ store.orderDemo.order_id }}</strong>
        </div>
        <div>
          <p class="detail-label">Client Order</p>
          <strong class="monospace">{{ store.orderDemo.client_order_id }}</strong>
        </div>
        <div>
          <p class="detail-label">Broker Order</p>
          <strong class="monospace">{{ store.orderDemo.broker_order_no ?? "-" }}</strong>
        </div>
        <div>
          <p class="detail-label">Updated</p>
          <strong class="monospace">{{ store.orderDemo.updated_at_utc }}</strong>
        </div>
      </div>
    </article>

    <article class="panel">
      <div class="panel-header">
        <h3>Projected Positions</h3>
        <span>{{ store.positions.length }}</span>
      </div>
      <table class="table">
        <thead>
          <tr>
            <th>Symbol</th>
            <th>Net Qty</th>
            <th>Avg Cost</th>
            <th>Market Value</th>
            <th>UPnL</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="position in store.positions" :key="position.symbol">
            <td>{{ position.symbol }}</td>
            <td>{{ position.net_qty }}</td>
            <td>{{ position.avg_cost_krw.toLocaleString() }}</td>
            <td>{{ position.market_value_krw.toLocaleString() }}</td>
            <td>{{ position.unrealized_pnl_krw.toLocaleString() }}</td>
          </tr>
        </tbody>
      </table>
    </article>
  </section>
</template>
