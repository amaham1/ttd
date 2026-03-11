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
  no_trade_count: number;
  blocked_count: number;
  submitted_count: number;
  stale_data_incident_count: number;
  duplicate_order_incident_count: number;
  selector_mismatch_incident_count: number;
}

export interface ShadowLoopSnapshot {
  running: boolean;
  execute_live: boolean;
  persist: boolean;
  interval_seconds: number;
  run_count: number;
  desired_running?: boolean;
  owner_id?: string | null;
  lease_expires_at_utc?: string | null;
  heartbeat_at_utc?: string | null;
  lease_stale?: boolean;
  restored_from_durable?: boolean;
  last_started_at_utc?: string | null;
  last_finished_at_utc?: string | null;
  last_result_status?: string | null;
  last_error?: string | null;
}

export interface LiveControlState {
  max_order_value_krw: number;
  auto_loop_interval_seconds: number;
  autonomous_loop_enabled: boolean;
  updated_at_utc: string;
}

export interface CandidateDecisionRecord {
  decision_id: string;
  candidate_id?: string | null;
  source_receipt_no?: string | null;
  source_report_name?: string | null;
  source_symbol?: string | null;
  matched_positive_rule_id?: string | null;
  matched_block_rule_id?: string | null;
  candidate_status: string;
  selection_reason?: string | null;
  rejection_reason?: string | null;
  ranking_score?: number | null;
  ranking_reason?: string | null;
  decision_payload_json: Record<string, unknown>;
  created_at_utc: string;
}

export interface DisclosureRuleDefinition {
  rule_id: string;
  rule_type: string;
  rule_name: string;
  match_field: string;
  match_pattern: string;
  decision_effect: string;
  reason_template: string;
  enabled: boolean;
  priority: number;
}

export interface ExecutionPlan {
  candidate_id: string;
  intent_id?: string | null;
  planned_order?: {
    price?: number | null;
    qty?: number | null;
    venue_hint?: string | null;
    [key: string]: unknown;
  } | null;
  selected_price_krw?: number | null;
  quote_basis?: string | null;
  price_source_value?: number | null;
  execute_live: boolean;
  persisted: boolean;
  selection_reason?: string | null;
  matched_rule_id?: string | null;
  source_report_name?: string | null;
  source_receipt_no?: string | null;
  price_reason?: string | null;
  quantity_reason?: string | null;
  risk_reason_summary?: string | null;
  risk_state?: Record<string, unknown> | null;
  broker_response?: Record<string, unknown> | null;
  normalized_ack?: Record<string, unknown> | null;
  persistence_error?: string | null;
  status: string;
  reason?: string | null;
}

export interface WalkForwardFoldReport {
  fold_id: string;
  train_start_utc: string;
  train_end_utc: string;
  validation_start_utc: string;
  validation_end_utc: string;
  train_count: number;
  validation_count: number;
  mean_return_bps: number;
  mean_payoff_score: number;
  hit_rate: number;
  net_sharpe_proxy: number;
  max_drawdown_bps: number;
}

export interface ModelTrainingReport {
  training_id: string;
  model_version: string;
  algorithm_family: string;
  trained_experts: string[];
  training_row_count: number;
  fold_reports: WalkForwardFoldReport[];
  metrics: Record<string, number>;
  artifact_refs: Record<string, string>;
  created_at_utc: string;
}

export interface ResearchInstrumentReport {
  instrument_id: string;
  event_family: string;
  sample_count: number;
  avg_forward_return_bps: number;
  avg_payoff_score: number;
  worst_drawdown_bps: number;
}

export interface ShadowLiveRunAuditRecord {
  run_id: string;
  candidate_id?: string | null;
  instrument_id?: string | null;
  execute_live: boolean;
  persisted: boolean;
  status: string;
  reason?: string | null;
  promotion_required: boolean;
  promotion_approved?: boolean | null;
  stale_data_incident: boolean;
  duplicate_order_incident: boolean;
  selector_mismatch_incident: boolean;
  payload: Record<string, unknown>;
  created_at_utc: string;
}

export interface ShadowLiveMetricsSummary {
  run_count: number;
  no_trade_count: number;
  blocked_count: number;
  submitted_count: number;
  live_attempt_count: number;
  stale_data_incident_count: number;
  duplicate_order_incident_count: number;
  selector_mismatch_incident_count: number;
  promotion_block_count: number;
  latest_run_at_utc?: string | null;
  recent_runs: ShadowLiveRunAuditRecord[];
}

export interface ResearchReportSnapshot {
  generated_at_utc: string;
  latest_training_report?: ModelTrainingReport | null;
  training_history: ModelTrainingReport[];
  instrument_leaderboard: ResearchInstrumentReport[];
  event_family_leaderboard: Array<Record<string, unknown>>;
  fold_reports: WalkForwardFoldReport[];
  data_counts: Record<string, number>;
}
