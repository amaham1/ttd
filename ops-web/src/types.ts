export interface OperationModeState {
  mode: string;
  reason?: string | null;
  updated_at_utc: string;
}

export interface DashboardSummary {
  mode: OperationModeState;
  strategy_enabled_count: number;
  blocked_symbol_count: number;
  open_break_count: number;
  replay_job_count: number;
  risk_flag_count: number;
  active_position_count: number;
}

export interface StrategyState {
  strategy_id: string;
  enabled: boolean;
  updated_at_utc: string;
}

export interface BreakState {
  break_id: string;
  scope: string;
  severity: string;
  status: string;
  detected_at_utc: string;
}

export interface OrderTrace {
  order_id: string;
  state: string;
  client_order_id: string;
  broker_order_no?: string | null;
  updated_at_utc: string;
}

export interface PositionState {
  symbol: string;
  net_qty: number;
  avg_cost_krw: number;
  market_value_krw: number;
  unrealized_pnl_krw: number;
  updated_at_utc: string;
}

export interface RiskFlagState {
  symbol: string;
  flag_type: string;
  severity: string;
  hard_block: boolean;
  source_system: string;
  updated_at_utc: string;
}

export interface ReplayJobState {
  replay_job_id: string;
  trading_date: string;
  status: string;
  scenario: string;
  created_at_utc: string;
}

export interface BrokerSnapshot {
  rest_token_ready: boolean;
  ws_approval_ready: boolean;
  last_rest_auth_utc?: string | null;
  last_ws_auth_utc?: string | null;
  current_mode: string;
  pending_rate_budget: number;
  degraded_reason?: string | null;
}

export interface TradingSnapshot {
  order_count: number;
  open_breaks: number;
  projected_positions: number;
  projected_cash_krw: number;
  operation_mode: string;
}

export interface MarketIntelSnapshot {
  disclosure_backlog: number;
  parser_mode: string;
  low_confidence_count: number;
  last_disclosure_utc?: string | null;
}

export interface ReplaySnapshot {
  active_jobs: number;
  last_run_utc?: string | null;
  worker_mode: string;
}

export interface ShadowLiveSnapshot {
  mode: string;
  candidate_count: number;
  fill_match_rate: number;
  last_sync_utc: string;
  last_candidate_id?: string | null;
  last_intent_id?: string | null;
  last_internal_order_id?: string | null;
  last_broker_order_no?: string | null;
  last_execution_status?: string | null;
  last_execution_reason?: string | null;
}

export interface ShadowLoopSnapshot {
  running: boolean;
  execute_live: boolean;
  persist: boolean;
  interval_seconds: number;
  run_count: number;
  last_started_at_utc?: string | null;
  last_finished_at_utc?: string | null;
  last_result_status?: string | null;
  last_error?: string | null;
}
