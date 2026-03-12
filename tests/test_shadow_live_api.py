import asyncio
from datetime import UTC, date, datetime, timedelta

from fastapi.testclient import TestClient
from pytest import MonkeyPatch

from apps.data_ingest import service as data_ingest_service_module
from apps.market_intel.service import market_intel_service
from apps.ops_api.store import store
from apps.shadow_live import service as shadow_service_module
from apps.shadow_live.service import BrokerGatewayRequestError, ExecutionPlan
from apps.shadow_live.main import app
from libs.adapters.openai_parser import TradeDecisionOverlayResult
from libs.contracts.messages import (
    ExecutionReadiness,
    MacroSeriesPointRecord,
    ShadowLiveMetricsSummary,
    ShadowLiveRunAuditRecord,
)
from libs.db.repositories import InstrumentProfileSnapshot
from libs.domain.enums import OrderSide


client = TestClient(app)


async def fake_select_candidate():
    return market_intel_service.sample_candidates()[0].model_copy(
        update={
            "expire_ts_utc": datetime.now(UTC) + timedelta(minutes=30),
            "cooldown_key": "005930:BUYBACK:20990101",
        }
    )


def test_shadow_live_plans_order_without_live_execution() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    persisted_audits = []

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 1.5384,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_query_balance():
        return {"output1": [], "output2": [{"tot_evlu_amt": "6400000", "dnca_tot_amt": "6400000"}]}

    def fake_persist_shadow_live_run(audit):
        persisted_audits.append(audit)

    async def fake_build_execution_readiness(
        *,
        candidate,
        execute_live=False,
        risk_state=None,
        market_snapshot_ok=True,
        vendor_healthy=True,
    ):
        return ExecutionReadiness(
            account_id=candidate.account_scope,
            strategy_id=candidate.strategy_id,
            instrument_id=candidate.instrument_id,
            execution_side=candidate.side,
            max_allowed_notional_krw=candidate.target_notional_krw,
        )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_build_execution_readiness",
        fake_build_execution_readiness,
    )
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "refresh_live_risk_state", fake_refresh_live_risk_state)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_price", fake_query_price)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_asking_price", fake_query_asking_price)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_balance", fake_query_balance)
    monkeypatch.setattr(
        data_ingest_service_module.data_ingest_service,
        "persist_shadow_live_run",
        fake_persist_shadow_live_run,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["quote_basis"] == "BEST_ASK"
        assert body["planned_order"]["price"] == 70200
        assert body["planned_order"]["qty"] >= 1
        assert body["matched_rule_id"] == "disclosure.positive.buyback"
        assert body["selection_reason"] is not None
        assert body["price_reason"] is not None
        assert body["quantity_reason"] is not None
        assert len(persisted_audits) == 1
        assert persisted_audits[0].status == "PLANNED_ONLY"

        latest_plan_response = client.get("/plan/latest")
        assert latest_plan_response.status_code == 200
        assert latest_plan_response.json()["candidate_id"] == body["candidate_id"]
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_submits_order_when_live_execution_enabled() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 1.5384,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_query_balance():
        return {"output1": [], "output2": [{"tot_evlu_amt": "6400000", "dnca_tot_amt": "6400000"}]}

    async def fake_submit_cash_order(payload):
        return {
            "rt_cd": "0",
            "output": {
                "ODNO": "8300019999",
                "ORD_TMD": "103000",
                "EXCG_ID_DVSN_CD": payload["excg_id_dvsn_cd"],
            },
        }

    async def fake_normalize_order_ack(payload):
        return {
            "event": {
                "broker_order_no": payload["output"]["ODNO"],
            }
        }

    async def fake_build_execution_readiness(
        *,
        candidate,
        execute_live=False,
        risk_state=None,
        market_snapshot_ok=True,
        vendor_healthy=True,
    ):
        return ExecutionReadiness(
            account_id=candidate.account_scope,
            strategy_id=candidate.strategy_id,
            instrument_id=candidate.instrument_id,
            execution_side=candidate.side,
            max_allowed_notional_krw=candidate.target_notional_krw,
        )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_build_execution_readiness",
        fake_build_execution_readiness,
    )
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "refresh_live_risk_state", fake_refresh_live_risk_state)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_price", fake_query_price)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_asking_price", fake_query_asking_price)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "query_balance", fake_query_balance)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "submit_cash_order", fake_submit_cash_order)
    monkeypatch.setattr(shadow_service_module.shadow_live_service.gateway_client, "normalize_order_ack", fake_normalize_order_ack)
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "SUBMITTED"
        assert body["broker_response"]["output"]["ODNO"] == "8300019999"
        assert body["normalized_ack"]["event"]["broker_order_no"] == "8300019999"
        assert body["source_receipt_no"] == "20260311000123"
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_skips_expired_portfolio_candidates() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    expired_candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-expired-buy",
            "expire_ts_utc": datetime.now(UTC) - timedelta(minutes=1),
            "cooldown_key": "expired-buy:1",
        }
    )

    async def fake_fetch_portfolio_candidates_with_status(*, force_refresh=False):
        return [expired_candidate], True

    async def fake_query_balance():
        return {"output1": [], "output2": [{"tot_evlu_amt": "6400000", "dnca_tot_amt": "6400000"}]}

    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_portfolio_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "NO_TRADE"
        assert body["reason"] == "ALL_PORTFOLIO_CANDIDATES_EXPIRED:count=1"
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_skips_excluded_exit_symbol_and_plans_buy_candidate() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    service = shadow_service_module.shadow_live_service
    buy_candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-buy-after-excluded-exit",
            "instrument_id": "056090",
            "cooldown_key": "buy-after-excluded-exit:1",
            "expire_ts_utc": datetime.now(UTC) + timedelta(minutes=30),
        }
    )

    async def fake_fetch_portfolio_candidates_with_status(*, force_refresh=False):
        return [buy_candidate], True

    async def fake_query_balance():
        return {
            "output1": [
                {
                    "pdno": "005935",
                    "hldg_qty": "42",
                    "pchs_avg_pric": "136200",
                    "prpr": "135500",
                    "evlu_amt": "5691000",
                    "evlu_pfls_rt": "-0.51",
                }
            ],
            "output2": [{"tot_evlu_amt": "6400000", "dnca_tot_amt": "709000"}],
        }

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 1.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        assert symbol == "056090"
        return {"output": {"stck_prpr": "3130"}}

    async def fake_query_asking_price(*, symbol, market_div):
        assert symbol == "056090"
        return {"output1": {"askp1": "3135", "bidp1": "3130"}}

    async def fake_build_execution_readiness(
        *,
        candidate,
        execute_live=False,
        risk_state=None,
        market_snapshot_ok=True,
        vendor_healthy=True,
    ):
        return ExecutionReadiness(
            account_id=candidate.account_scope,
            strategy_id=candidate.strategy_id,
            instrument_id=candidate.instrument_id,
            execution_side=candidate.side,
            max_allowed_notional_krw=candidate.target_notional_krw,
        )

    monkeypatch.setattr(service.settings, "kis_live_excluded_symbols", "005935")
    monkeypatch.setattr(service, "_fetch_portfolio_candidates_with_status", fake_fetch_portfolio_candidates_with_status)
    monkeypatch.setattr(service, "_build_execution_readiness", fake_build_execution_readiness)
    monkeypatch.setattr(service.gateway_client, "query_balance", fake_query_balance)
    monkeypatch.setattr(service.gateway_client, "refresh_live_risk_state", fake_refresh_live_risk_state)
    monkeypatch.setattr(service.gateway_client, "query_price", fake_query_price)
    monkeypatch.setattr(service.gateway_client, "query_asking_price", fake_query_asking_price)
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["planned_order"]["side"] == "buy"
        assert body["planned_order"]["instrument_id"] == "056090"
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_buy_rate_limit_blocks_third_live_order_within_60_seconds() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    service = shadow_service_module.shadow_live_service
    submitted_payloads: list[dict] = []
    buy_candidates = [
        market_intel_service.sample_candidates()[0].model_copy(
            update={
                "candidate_id": f"candidate-buy-rate-{index}",
                "instrument_id": f"90{index:04d}",
                "cooldown_key": f"buy-rate:{index}",
                "expire_ts_utc": datetime.now(UTC) + timedelta(minutes=30),
            }
        )
        for index in range(1, 4)
    ]

    async def fake_select_run_candidate(*, execute_live: bool):
        return buy_candidates.pop(0), None

    async def fake_build_execution_readiness(
        *,
        candidate,
        execute_live=False,
        risk_state=None,
        market_snapshot_ok=True,
        vendor_healthy=True,
    ):
        return ExecutionReadiness(
            account_id=candidate.account_scope,
            strategy_id=candidate.strategy_id,
            instrument_id=candidate.instrument_id,
            execution_side=candidate.side,
            max_allowed_notional_krw=candidate.target_notional_krw,
        )

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.25,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_submit_cash_order(payload):
        submitted_payloads.append(payload)
        return {
            "rt_cd": "0",
            "output": {
                "ODNO": f"83000199{len(submitted_payloads)}",
                "ORD_TMD": "103000",
                "EXCG_ID_DVSN_CD": payload["excg_id_dvsn_cd"],
            },
        }

    async def fake_normalize_order_ack(payload):
        return {"event": {"broker_order_no": payload["output"]["ODNO"]}}

    monkeypatch.setattr(service.settings, "shadow_live_side_rate_limit_window_seconds", 60)
    monkeypatch.setattr(service.settings, "shadow_live_side_rate_limit_max_orders", 2)
    monkeypatch.setattr(service, "_select_run_candidate", fake_select_run_candidate)
    monkeypatch.setattr(service, "_build_execution_readiness", fake_build_execution_readiness)
    monkeypatch.setattr(service.gateway_client, "refresh_live_risk_state", fake_refresh_live_risk_state)
    monkeypatch.setattr(service.gateway_client, "query_price", fake_query_price)
    monkeypatch.setattr(service.gateway_client, "query_asking_price", fake_query_asking_price)
    monkeypatch.setattr(service.gateway_client, "submit_cash_order", fake_submit_cash_order)
    monkeypatch.setattr(service.gateway_client, "normalize_order_ack", fake_normalize_order_ack)
    try:
        responses = [
            client.post("/run/sample", json={"execute_live": True, "persist": False})
            for _ in range(3)
        ]

        bodies = [response.json() for response in responses]
        assert [response.status_code for response in responses] == [200, 200, 200]
        assert [body["status"] for body in bodies] == ["SUBMITTED", "SUBMITTED", "BLOCKED"]
        assert bodies[2]["reason"] == (
            "ORDER_SIDE_RATE_LIMIT_EXCEEDED:side=buy,window_seconds=60,current_count=2,max_count=2"
        )
        assert len(submitted_payloads) == 2
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_sell_rate_limit_blocks_third_live_order_within_60_seconds() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    service = shadow_service_module.shadow_live_service
    submitted_payloads: list[dict] = []
    sell_template = market_intel_service.sample_candidates()[0]
    sell_candidates = [
        sell_template.model_copy(
            update={
                "candidate_id": f"candidate-sell-rate-{index}",
                "instrument_id": f"80{index:04d}",
                "strategy_id": "close-only-defense",
                "side": OrderSide.SELL,
                "target_qty_override": 3,
                "target_notional_krw": 210000,
                "cooldown_key": f"sell-rate:{index}",
                "expire_ts_utc": datetime.now(UTC) + timedelta(minutes=10),
            }
        )
        for index in range(1, 4)
    ]

    async def fake_select_run_candidate(*, execute_live: bool):
        return sell_candidates.pop(0), None

    async def fake_build_execution_readiness(
        *,
        candidate,
        execute_live=False,
        risk_state=None,
        market_snapshot_ok=True,
        vendor_healthy=True,
    ):
        return ExecutionReadiness(
            account_id=candidate.account_scope,
            strategy_id=candidate.strategy_id,
            instrument_id=candidate.instrument_id,
            execution_side=candidate.side,
            max_allowed_notional_krw=candidate.target_notional_krw,
        )

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.1,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_submit_cash_order(payload):
        submitted_payloads.append(payload)
        return {
            "rt_cd": "0",
            "output": {
                "ODNO": f"84000199{len(submitted_payloads)}",
                "ORD_TMD": "103500",
                "EXCG_ID_DVSN_CD": payload["excg_id_dvsn_cd"],
            },
        }

    async def fake_normalize_order_ack(payload):
        return {"event": {"broker_order_no": payload["output"]["ODNO"]}}

    monkeypatch.setattr(service.settings, "shadow_live_side_rate_limit_window_seconds", 60)
    monkeypatch.setattr(service.settings, "shadow_live_side_rate_limit_max_orders", 2)
    monkeypatch.setattr(service, "_select_run_candidate", fake_select_run_candidate)
    monkeypatch.setattr(service, "_build_execution_readiness", fake_build_execution_readiness)
    monkeypatch.setattr(service.gateway_client, "refresh_live_risk_state", fake_refresh_live_risk_state)
    monkeypatch.setattr(service.gateway_client, "query_price", fake_query_price)
    monkeypatch.setattr(service.gateway_client, "query_asking_price", fake_query_asking_price)
    monkeypatch.setattr(service.gateway_client, "submit_cash_order", fake_submit_cash_order)
    monkeypatch.setattr(service.gateway_client, "normalize_order_ack", fake_normalize_order_ack)
    try:
        responses = [
            client.post("/run/sample", json={"execute_live": True, "persist": False})
            for _ in range(3)
        ]

        bodies = [response.json() for response in responses]
        assert [response.status_code for response in responses] == [200, 200, 200]
        assert [body["status"] for body in bodies] == ["SUBMITTED", "SUBMITTED", "BLOCKED"]
        assert bodies[2]["reason"] == (
            "ORDER_SIDE_RATE_LIMIT_EXCEEDED:side=sell,window_seconds=60,current_count=2,max_count=2"
        )
        assert len(submitted_payloads) == 2
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_prioritizes_exit_candidate_and_plans_sell_despite_entry_pause() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6200000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 4.6154,
            "entry_paused": True,
            "live_pause_reason": "DAILY_LOSS_LIMIT_4.50_PCT_BREACHED",
        }

    async def fake_query_price(*, symbol, market_div):
        assert symbol == "123456"
        return {"output": {"stck_prpr": "9300"}}

    async def fake_query_asking_price(*, symbol, market_div):
        assert symbol == "123456"
        return {"output1": {"askp1": "9310", "bidp1": "9290"}}

    async def fake_query_balance():
        return {
            "output1": [
                {
                    "pdno": "123456",
                    "hldg_qty": "10",
                    "pchs_avg_pric": "10000",
                    "prpr": "9300",
                    "evlu_amt": "93000",
                    "evlu_pfls_rt": "-7.0",
                }
            ],
            "output2": [{"tot_evlu_amt": "6200000", "dnca_tot_amt": "6107000"}],
        }

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["quote_basis"] == "BEST_BID"
        assert body["matched_rule_id"] == "exit.stop_loss.hard"
        assert "EXIT_STOP_LOSS" in body["selection_reason"]
        assert body["planned_order"]["side"] == "sell"
        assert body["planned_order"]["instrument_id"] == "123456"
        assert body["planned_order"]["qty"] == 10
        assert body["planned_order"]["price"] == 9290
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_exits_position_removed_from_live_portfolio() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    live_candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-other-live",
            "instrument_id": "222222",
            "target_notional_krw": 500000,
        }
    )

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6500000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        assert symbol == "111111"
        return {"output": {"stck_prpr": "10100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        assert symbol == "111111"
        return {"output1": {"askp1": "10110", "bidp1": "10090"}}

    async def fake_query_balance():
        return {
            "output1": [
                {
                    "pdno": "111111",
                    "hldg_qty": "6",
                    "pchs_avg_pric": "10000",
                    "prpr": "10100",
                    "evlu_amt": "60600",
                    "evlu_pfls_rt": "1.0",
                }
            ],
            "output2": [{"tot_evlu_amt": "6500000", "dnca_tot_amt": "6439400"}],
        }

    async def fake_fetch_portfolio_candidates_with_status(*, force_refresh=False):
        return [live_candidate], True

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_portfolio_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["matched_rule_id"] == "exit.portfolio_removed"
        assert body["planned_order"]["side"] == "sell"
        assert body["planned_order"]["instrument_id"] == "111111"
        assert body["planned_order"]["qty"] == 6
        assert "EXIT_PORTFOLIO_REMOVED" in body["selection_reason"]
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_scales_down_when_position_exceeds_live_target() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    live_candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-scale-down-live",
            "instrument_id": "333333",
            "target_notional_krw": 30000,
            "selection_confidence": 0.82,
            "expected_edge_bps": 51.0,
            "ranking_score": 18.0,
            "sector_name": "Technology",
        }
    )

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6500000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        assert symbol == "333333"
        return {"output": {"stck_prpr": "10000"}}

    async def fake_query_asking_price(*, symbol, market_div):
        assert symbol == "333333"
        return {"output1": {"askp1": "10010", "bidp1": "9990"}}

    async def fake_query_balance():
        return {
            "output1": [
                {
                    "pdno": "333333",
                    "hldg_qty": "10",
                    "pchs_avg_pric": "9800",
                    "prpr": "10000",
                    "evlu_amt": "100000",
                    "evlu_pfls_rt": "2.0408",
                }
            ],
            "output2": [{"tot_evlu_amt": "6500000", "dnca_tot_amt": "6400000"}],
        }

    async def fake_fetch_portfolio_candidates_with_status(*, force_refresh=False):
        return [live_candidate], True

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_portfolio_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["matched_rule_id"] == "exit.rebalance.scale_down"
        assert body["planned_order"]["side"] == "sell"
        assert body["planned_order"]["instrument_id"] == "333333"
        assert body["planned_order"]["qty"] == 7
        assert "desired_qty=3" in body["selection_reason"]
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_trims_position_on_macro_headwind_even_when_still_selected() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    live_candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-macro-trim-live",
            "instrument_id": "555555",
            "target_notional_krw": 100000,
            "selection_confidence": 0.7,
            "expected_edge_bps": 30.0,
            "ranking_score": 16.0,
            "sector_name": "Transport",
            "thematic_tags": ["oil-sensitive", "cyclical"],
            "cross_asset_impact_score": -0.32,
            "thematic_alignment_score": 0.08,
            "macro_headwind_score": 0.63,
        }
    )

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6500000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        assert symbol == "555555"
        return {"output": {"stck_prpr": "10000"}}

    async def fake_query_asking_price(*, symbol, market_div):
        assert symbol == "555555"
        return {"output1": {"askp1": "10010", "bidp1": "9990"}}

    async def fake_query_balance():
        return {
            "output1": [
                {
                    "pdno": "555555",
                    "hldg_qty": "8",
                    "pchs_avg_pric": "9800",
                    "prpr": "10000",
                    "evlu_amt": "80000",
                    "evlu_pfls_rt": "2.0408",
                }
            ],
            "output2": [{"tot_evlu_amt": "6500000", "dnca_tot_amt": "6420000"}],
        }

    async def fake_fetch_portfolio_candidates_with_status(*, force_refresh=False):
        return [live_candidate], True

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_portfolio_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["matched_rule_id"] == "exit.macro_headwind.trim"
        assert body["planned_order"]["side"] == "sell"
        assert body["planned_order"]["qty"] == 3
        assert "macro_headwind=0.63" in body["selection_reason"]
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_fully_exits_position_on_severe_macro_headwind() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    live_candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-macro-full-live",
            "instrument_id": "666666",
            "target_notional_krw": 70000,
            "selection_confidence": 0.58,
            "expected_edge_bps": 15.0,
            "ranking_score": 8.0,
            "sector_name": "Chemicals",
            "thematic_tags": ["oil-sensitive"],
            "cross_asset_impact_score": -0.45,
            "thematic_alignment_score": 0.04,
            "macro_headwind_score": 0.8,
        }
    )

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6500000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        assert symbol == "666666"
        return {"output": {"stck_prpr": "10000"}}

    async def fake_query_asking_price(*, symbol, market_div):
        assert symbol == "666666"
        return {"output1": {"askp1": "10010", "bidp1": "9990"}}

    async def fake_query_balance():
        return {
            "output1": [
                {
                    "pdno": "666666",
                    "hldg_qty": "7",
                    "pchs_avg_pric": "9900",
                    "prpr": "10000",
                    "evlu_amt": "70000",
                    "evlu_pfls_rt": "1.0101",
                }
            ],
            "output2": [{"tot_evlu_amt": "6500000", "dnca_tot_amt": "6430000"}],
        }

    async def fake_fetch_portfolio_candidates_with_status(*, force_refresh=False):
        return [live_candidate], True

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_portfolio_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["matched_rule_id"] == "exit.macro_headwind.full"
        assert body["planned_order"]["side"] == "sell"
        assert body["planned_order"]["qty"] == 7
        assert "EXIT_MACRO_HEADWIND_FULL" in body["selection_reason"]
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_uses_persisted_profile_for_macro_exit_without_live_candidate() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6500000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        assert symbol == "777777"
        return {"output": {"stck_prpr": "10000"}}

    async def fake_query_asking_price(*, symbol, market_div):
        assert symbol == "777777"
        return {"output1": {"askp1": "10010", "bidp1": "9990"}}

    async def fake_query_balance():
        return {
            "output1": [
                {
                    "pdno": "777777",
                    "hldg_qty": "5",
                    "pchs_avg_pric": "9950",
                    "prpr": "10000",
                    "evlu_amt": "50000",
                    "evlu_pfls_rt": "0.5025",
                    "buy_dt": "20260301",
                }
            ],
            "output2": [{"tot_evlu_amt": "6500000", "dnca_tot_amt": "6450000"}],
        }

    async def fake_fetch_portfolio_candidates_with_status(*, force_refresh=False):
        return [], False

    async def fake_select_none():
        return None

    persisted_profile = InstrumentProfileSnapshot(
        instrument_id="777777",
        issuer_name="Profile Backed Transport",
        sector_name="Transportation",
        oil_up_beta=-0.8,
        usdkrw_up_beta=-0.35,
        rates_up_beta=-0.05,
        china_growth_beta=0.0,
        domestic_demand_beta=0.2,
        export_beta=0.0,
        thematic_tags=["oil-sensitive", "consumer"],
        rationale="Persisted transport sensitivity profile",
        confidence_score=0.82,
        used_fallback=False,
        source_event_family="CONTRACT",
        source_event_type="SUPPLY_CONTRACT",
        source_report_name="단일판매·공급계약체결",
        source_receipt_no="20260312000001",
        source_summary_text="Transport operator signed a supply agreement but remains oil-sensitive.",
        created_at_utc=None,
        updated_at_utc=None,
    )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_none)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_portfolio_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.repository,
        "get_instrument_profile",
        lambda instrument_id: persisted_profile if instrument_id == "777777" else None,
    )
    monkeypatch.setattr(
        data_ingest_service_module.data_ingest_service,
        "latest_macro_points",
        lambda: [
            MacroSeriesPointRecord(series_id="DCOILWTICO", observation_date=date(2026, 3, 10), value=70.0),
            MacroSeriesPointRecord(series_id="DCOILWTICO", observation_date=date(2026, 3, 11), value=82.0),
            MacroSeriesPointRecord(series_id="DEXKOUS", observation_date=date(2026, 3, 10), value=1300.0),
            MacroSeriesPointRecord(series_id="DEXKOUS", observation_date=date(2026, 3, 11), value=1360.0),
            MacroSeriesPointRecord(series_id="DGS10", observation_date=date(2026, 3, 10), value=4.0),
            MacroSeriesPointRecord(series_id="DGS10", observation_date=date(2026, 3, 11), value=4.2),
        ],
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["matched_rule_id"] == "exit.macro_profile.full"
        assert body["planned_order"]["side"] == "sell"
        assert body["planned_order"]["qty"] == 5
        assert "EXIT_MACRO_PROFILE_FULL" in body["selection_reason"]
        assert "profile_source=DB_PROFILE" in body["selection_reason"]
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_can_exit_from_gemini_overlay_when_other_rules_do_not_fire() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    live_candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-gemini-exit",
            "instrument_id": "888888",
            "target_notional_krw": 90000,
            "selection_confidence": 0.74,
            "expected_edge_bps": 22.0,
            "ranking_score": 10.0,
            "sector_name": "Technology",
            "thematic_tags": ["technology"],
            "cross_asset_impact_score": 0.05,
            "thematic_alignment_score": 0.42,
            "macro_headwind_score": 0.34,
        }
    )

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6500000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        assert symbol == "888888"
        return {"output": {"stck_prpr": "10000"}}

    async def fake_query_asking_price(*, symbol, market_div):
        assert symbol == "888888"
        return {"output1": {"askp1": "10010", "bidp1": "9990"}}

    async def fake_query_balance():
        return {
            "output1": [
                {
                    "pdno": "888888",
                    "hldg_qty": "10",
                    "pchs_avg_pric": "10020",
                    "prpr": "10000",
                    "evlu_amt": "100000",
                    "evlu_pfls_rt": "-0.1996",
                    "buy_dt": "20260310",
                }
            ],
            "output2": [{"tot_evlu_amt": "6500000", "dnca_tot_amt": "6400000"}],
        }

    async def fake_fetch_portfolio_candidates_with_status(*, force_refresh=False):
        return [live_candidate], True

    async def fake_overlay(**kwargs):
        return TradeDecisionOverlayResult(
            action_bias="TRIM",
            alpha_adjust_bps=24.0,
            confidence_adjust=0.08,
            position_scale=0.6,
            holding_days_adjust=-2,
            exit_urgency_score=0.76,
            thesis_quality_score=0.34,
            crowding_risk_score=0.41,
            signal_decay_score=0.73,
            hard_block=False,
            rationale="Catalyst faded and near-term upside weakened.",
            confidence=0.87,
            used_fallback=False,
        )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_portfolio_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.parser_client,
        "infer_trade_decision_overlay",
        fake_overlay,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["matched_rule_id"] == "exit.gemini.trim"
        assert body["planned_order"]["side"] == "sell"
        assert body["planned_order"]["qty"] == 5
        assert "EXIT_GEMINI_TRIM" in body["selection_reason"]
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_does_not_force_portfolio_exit_when_candidate_fetch_fails() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6500000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        if symbol == "005930":
            return {"output": {"stck_prpr": "70100"}}
        return {"output": {"stck_prpr": "10100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        if symbol == "005930":
            return {"output1": {"askp1": "70200", "bidp1": "70100"}}
        return {"output1": {"askp1": "10110", "bidp1": "10090"}}

    async def fake_query_balance():
        return {
            "output1": [
                {
                    "pdno": "444444",
                    "hldg_qty": "4",
                    "pchs_avg_pric": "10000",
                    "prpr": "10100",
                    "evlu_amt": "40400",
                    "evlu_pfls_rt": "1.0",
                }
            ],
            "output2": [{"tot_evlu_amt": "6500000", "dnca_tot_amt": "6459600"}],
        }

    async def fake_fetch_portfolio_candidates_with_status(*, force_refresh=False):
        return [], False

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_portfolio_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["planned_order"]["side"] == "buy"
        assert body["planned_order"]["instrument_id"] == "005930"
        assert body["matched_rule_id"] == "disclosure.positive.buyback"
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_falls_through_to_next_buy_candidate_when_first_one_is_blocked() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    sample = market_intel_service.sample_candidates()[0]
    first_candidate = sample.model_copy(
        update={
            "candidate_id": "candidate-blocked-first",
            "instrument_id": "005930",
            "cooldown_key": "005930:fallback:first",
            "expire_ts_utc": datetime.now(UTC) + timedelta(minutes=30),
        }
    )
    second_candidate = sample.model_copy(
        update={
            "candidate_id": "candidate-fallback-second",
            "instrument_id": "000660",
            "cooldown_key": "000660:fallback:second",
            "expire_ts_utc": datetime.now(UTC) + timedelta(minutes=30),
            "source_receipt_no": "20260311000999",
        }
    )

    async def fake_fetch_portfolio_candidates_with_status(*, force_refresh=False):
        return [first_candidate, second_candidate], True

    async def fake_select_exit_candidate(*, live_candidates, portfolio_candidates_ok):
        return None

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_build_execution_readiness(
        *,
        candidate,
        execute_live=False,
        risk_state=None,
        market_snapshot_ok=True,
        vendor_healthy=True,
    ):
        return ExecutionReadiness(
            account_id=candidate.account_scope,
            strategy_id=candidate.strategy_id,
            instrument_id=candidate.instrument_id,
            execution_side=candidate.side,
            max_allowed_notional_krw=candidate.target_notional_krw,
        )

    async def fake_resolve_quote_context(*, candidate, execute_live, venue_hint):
        if candidate.instrument_id == "005930":
            return (
                {
                    "selected_price_krw": 70200,
                    "quote_basis": "BEST_ASK",
                    "spread_bps": 14.0,
                },
                True,
                True,
                {
                    "entry_allowed": False,
                    "reason_codes": ["SYMBOL_QUOTE_UNAVAILABLE"],
                },
            )
        return (
            {
                "selected_price_krw": 198000,
                "quote_basis": "BEST_ASK",
                "spread_bps": 8.0,
            },
            True,
            True,
            {
                "entry_allowed": True,
                "reason_codes": [],
            },
        )

    def fake_persist_shadow_live_run(_audit):
        return None

    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_portfolio_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_select_exit_candidate",
        fake_select_exit_candidate,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_build_execution_readiness",
        fake_build_execution_readiness,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_resolve_quote_context",
        fake_resolve_quote_context,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        data_ingest_service_module.data_ingest_service,
        "persist_shadow_live_run",
        fake_persist_shadow_live_run,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})

        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "PLANNED_ONLY"
        assert body["candidate_id"] == "candidate-fallback-second"
        assert body["planned_order"]["instrument_id"] == "000660"
        assert "fallback_attempts=" in body["selection_reason"]
        assert "005930:SYMBOL_QUOTE_UNAVAILABLE" in body["selection_reason"]
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_returns_no_trade_without_fallback_candidate() -> None:
    monkeypatch = MonkeyPatch()

    async def no_candidate():
        return None

    async def fake_select_exit_candidate(*, live_candidates, portfolio_candidates_ok):
        return None

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", no_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_select_exit_candidate",
        fake_select_exit_candidate,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "NO_TRADE"
        assert body["candidate_id"] == ""
    finally:
        monkeypatch.undo()


def test_shadow_live_reports_reason_when_portfolio_has_no_actionable_candidates() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()

    async def fake_fetch_portfolio_candidates_with_status(*, force_refresh=False):
        return [], True

    async def fake_fetch_portfolio_selected_count():
        return 2

    async def fake_select_exit_candidate(*, live_candidates, portfolio_candidates_ok):
        return None

    def fake_persist_shadow_live_run(_audit):
        return None

    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_portfolio_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_selected_count",
        fake_fetch_portfolio_selected_count,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_select_exit_candidate",
        fake_select_exit_candidate,
    )
    monkeypatch.setattr(
        data_ingest_service_module.data_ingest_service,
        "persist_shadow_live_run",
        fake_persist_shadow_live_run,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": False, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "NO_TRADE"
        assert body["reason"] == "PORTFOLIO_SELECTED_WITHOUT_ACTIONABLE_CANDIDATES:selected_count=2"
        assert body["risk_reason_summary"] == body["reason"]
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_run_audit_summary_endpoint() -> None:
    monkeypatch = MonkeyPatch()

    def fake_summary(*, window_days=None, recent_limit=10):
        return ShadowLiveMetricsSummary(
            run_count=3,
            no_trade_count=1,
            blocked_count=1,
            submitted_count=1,
            live_attempt_count=2,
            stale_data_incident_count=0,
            duplicate_order_incident_count=0,
            selector_mismatch_incident_count=0,
            promotion_block_count=0,
            latest_run_at_utc="2026-03-11T23:14:16Z",
            recent_runs=[
                ShadowLiveRunAuditRecord(
                    run_id="shadow-run-1",
                    candidate_id="candidate-1",
                    instrument_id="005930",
                    execute_live=True,
                    persisted=True,
                    status="BLOCKED",
                    reason="SESSION_OUTSIDE_ENTRY_WINDOW",
                    payload={
                        "selection_reason": "실적 개선 후보",
                        "risk_reason_summary": "SESSION_OUTSIDE_ENTRY_WINDOW",
                    },
                )
            ],
        )

    monkeypatch.setattr(
        data_ingest_service_module.data_ingest_service,
        "shadow_live_metrics_summary",
        fake_summary,
    )
    try:
        response = client.get("/run-audit/summary?recent_limit=5")

        assert response.status_code == 200
        body = response.json()
        assert body["run_count"] == 3
        assert body["recent_runs"][0]["status"] == "BLOCKED"
        assert body["recent_runs"][0]["payload"]["selection_reason"] == "실적 개선 후보"
    finally:
        monkeypatch.undo()


def test_reset_runtime_cancels_existing_loop_task() -> None:
    async def scenario():
        shadow_service_module.shadow_live_service._loop_stop_event = asyncio.Event()
        task = asyncio.create_task(asyncio.sleep(30))
        shadow_service_module.shadow_live_service._loop_task = task
        shadow_service_module.shadow_live_service.reset_runtime()
        await asyncio.sleep(0)
        return task.cancelled(), shadow_service_module.shadow_live_service._loop_task is None

    cancelled, cleared = asyncio.run(scenario())

    assert cancelled is True
    assert cleared is True


def test_shadow_live_blocks_live_execution_when_promotion_is_not_ready() -> None:
    monkeypatch = MonkeyPatch()

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 1.5384,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_promotion_readiness():
        return {
            "approved": False,
            "reason_codes": ["INSUFFICIENT_SHADOW_RUNS"],
        }

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_promotion_readiness",
        fake_promotion_readiness,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        True,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "BLOCKED"
        assert "INSUFFICIENT_SHADOW_RUNS" in body["reason"]
        assert body["promotion_readiness"]["approved"] is False
    finally:
        monkeypatch.undo()


def test_shadow_live_returns_blocked_when_broker_gateway_rejects_order() -> None:
    monkeypatch = MonkeyPatch()

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_submit_cash_order(payload):
        raise BrokerGatewayRequestError(
            status_code=409,
            detail="live trading is not armed",
            payload={"detail": "live trading is not armed"},
        )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "submit_cash_order",
        fake_submit_cash_order,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "BLOCKED"
        assert "BROKER_GATEWAY_REJECTED:live trading is not armed" == body["reason"]
        assert body["broker_response"]["detail"] == "live trading is not armed"
    finally:
        monkeypatch.undo()


def test_shadow_live_blocks_micro_test_when_share_price_exceeds_cap() -> None:
    monkeypatch = MonkeyPatch()
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-expensive-cap-test",
            "cooldown_key": "005930:BUYBACK:20990101",
        }
    )

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_fetch_candidates(*, force_refresh=False):
        return [candidate]

    async def fake_fetch_candidates_with_status(*, force_refresh=False):
        return [candidate], True

    async def fake_selected_count():
        return 1

    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates",
        fake_fetch_candidates,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_selected_count",
        fake_selected_count,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_max_order_value_krw",
        5_000,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "kis_live_allowed_symbols",
        "005930",
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "NO_TRADE"
        assert body["reason"].startswith("MICRO_TEST_PRICE_CAP_EXCEEDED:")
    finally:
        monkeypatch.undo()


def test_shadow_live_submits_micro_test_single_share_within_cap() -> None:
    monkeypatch = MonkeyPatch()
    expensive_candidate = market_intel_service.sample_candidates()[0]
    cheap_candidate = expensive_candidate.model_copy(
        update={
            "candidate_id": "candidate-cheap",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20260311",
            "source_receipt_no": "20260311000999",
        }
    )

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        if symbol == "123456":
            return {"output": {"stck_prpr": "4890"}}
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        if symbol == "123456":
            return {"output1": {"askp1": "4900", "bidp1": "4890"}}
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_query_balance():
        return {"output1": [], "output2": [{"tot_evlu_amt": "6400000", "dnca_tot_amt": "6400000"}]}

    async def fake_fetch_candidates(*, force_refresh=False):
        return [expensive_candidate, cheap_candidate]

    async def fake_fetch_candidates_with_status(*, force_refresh=False):
        return [expensive_candidate, cheap_candidate], True

    async def fake_selected_count():
        return 2

    async def fake_submit_cash_order(payload):
        return {
            "rt_cd": "0",
            "output": {
                "ODNO": "8300019998",
                "ORD_TMD": "103001",
                "EXCG_ID_DVSN_CD": payload["excg_id_dvsn_cd"],
            },
        }

    async def fake_normalize_order_ack(payload):
        return {
            "event": {
                "broker_order_no": payload["output"]["ODNO"],
            }
        }

    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates",
        fake_fetch_candidates,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_selected_count",
        fake_selected_count,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "submit_cash_order",
        fake_submit_cash_order,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "normalize_order_ack",
        fake_normalize_order_ack,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_max_order_value_krw",
        5_000,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "kis_live_allowed_symbols",
        "005930,123456",
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "SUBMITTED"
        assert body["candidate_id"] == "candidate-cheap"
        assert body["planned_order"]["qty"] == 1
        assert body["planned_order"]["price"] == 4900
        assert body["planned_order"]["qty"] * body["planned_order"]["price"] <= 5_000
    finally:
        monkeypatch.undo()


def test_micro_test_candidate_preview_endpoint_suggests_allowlist_update() -> None:
    monkeypatch = MonkeyPatch()
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-preview",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20260311",
            "source_receipt_no": "20260311001001",
        }
    )

    async def fake_fetch_candidates(*, force_refresh=False):
        return [candidate]

    async def fake_selected_count():
        return 1

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "4890"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "4900", "bidp1": "4890"}}

    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates",
        fake_fetch_candidates,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_selected_count",
        fake_selected_count,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_max_order_value_krw",
        5_000,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "kis_live_allowed_symbols",
        "005930",
    )
    try:
        response = client.get("/preview/micro-test-candidates")
        assert response.status_code == 200
        body = response.json()
        assert body["configured_allowed_symbols"] == ["005930"]
        assert body["suggested_allowed_symbols"] == ["123456"]
        assert body["live_risk_state"]["entry_paused"] is False
        assert body["candidates"][0]["instrument_id"] == "123456"
        assert body["candidates"][0]["eligible_now"] is False
        assert body["candidates"][0]["eligible_if_allowlisted"] is True
        assert body["candidates"][0]["selected_price_krw"] == 4900
        assert body["candidates"][0]["proposed_qty"] == 1
        assert body["candidates"][0]["proposed_order_value_krw"] == 4900
    finally:
        monkeypatch.undo()


def test_shadow_live_returns_no_trade_when_no_whitelisted_candidate_meets_micro_cap() -> None:
    monkeypatch = MonkeyPatch()
    expensive_candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-expensive-1",
            "cooldown_key": "005930:BUYBACK:20990102",
        }
    )
    second_expensive = expensive_candidate.model_copy(
        update={
            "candidate_id": "candidate-expensive-2",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20260311",
        }
    )

    async def fake_fetch_candidates(*, force_refresh=False):
        return [expensive_candidate, second_expensive]

    async def fake_fetch_candidates_with_status(*, force_refresh=False):
        return [expensive_candidate, second_expensive], True

    async def fake_selected_count():
        return 2

    async def fake_query_price(*, symbol, market_div):
        if symbol == "123456":
            return {"output": {"stck_prpr": "5100"}}
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        if symbol == "123456":
            return {"output1": {"askp1": "5200", "bidp1": "5100"}}
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_query_balance():
        return {"output1": [], "output2": [{"tot_evlu_amt": "6400000", "dnca_tot_amt": "6400000"}]}

    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates",
        fake_fetch_candidates,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_candidates_with_status",
        fake_fetch_candidates_with_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service,
        "_fetch_portfolio_selected_count",
        fake_selected_count,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_max_order_value_krw",
        5_000,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "kis_live_allowed_symbols",
        "005930,123456",
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "NO_TRADE"
        assert body["reason"].startswith("MICRO_TEST_PRICE_CAP_EXCEEDED:")
    finally:
        monkeypatch.undo()


def test_post_trade_verification_endpoint_reports_verified_fill() -> None:
    monkeypatch = MonkeyPatch()
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-verify",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20260311",
            "source_receipt_no": "20260311001002",
        }
    )

    async def fake_select_candidate():
        return candidate

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "4890"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "4900", "bidp1": "4890"}}

    async def fake_submit_cash_order(payload):
        return {
            "rt_cd": "0",
            "output": {
                "ODNO": "8300011000",
                "ORD_TMD": "103101",
                "EXCG_ID_DVSN_CD": payload["excg_id_dvsn_cd"],
            },
        }

    async def fake_normalize_order_ack(payload):
        return {
            "event": {
                "broker_order_no": payload["output"]["ODNO"],
            }
        }

    async def fake_query_daily_ccld(*, symbol, broker_order_no, venue_code="KRX", trading_date=None):
        return {
            "output1": [
                {
                    "ODNO": broker_order_no,
                    "PDNO": symbol,
                    "tot_ccld_qty": "1",
                    "avg_prvs": "4900",
                }
            ]
        }

    async def fake_query_balance():
        return {
            "output1": [{"pdno": "123456", "hldg_qty": "1"}],
            "output2": [{"tot_evlu_amt": "6500000", "dnca_tot_amt": "6495100"}],
        }

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "submit_cash_order",
        fake_submit_cash_order,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "normalize_order_ack",
        fake_normalize_order_ack,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_daily_ccld",
        fake_query_daily_ccld,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        submit_response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert submit_response.status_code == 200
        assert submit_response.json()["status"] == "SUBMITTED"

        verify_response = client.get("/verify/post-trade/latest")
        assert verify_response.status_code == 200
        body = verify_response.json()
        assert body["status"] == "VERIFIED"
        assert body["broker_order_no"] == "8300011000"
        assert body["matched_order_count"] == 1
        assert body["matched_fill_qty"] == 1
        assert body["matched_avg_fill_price_krw"] == 4900.0
        assert body["balance_position_qty"] == 1
    finally:
        monkeypatch.undo()


def test_shadow_live_rejects_live_loop_start_in_micro_test_mode() -> None:
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_run_once_only",
        True,
    )
    try:
        response = client.post(
            "/loop/start",
            json={"interval_seconds": 15, "execute_live": True, "persist": False},
        )
        assert response.status_code == 400
        assert "only via /run/sample" in response.json()["detail"]
    finally:
        monkeypatch.undo()


def test_shadow_live_allows_live_loop_start_when_autonomous_control_is_enabled() -> None:
    monkeypatch = MonkeyPatch()
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_run_once_only",
        True,
    )
    store.set_live_control(
        max_order_value_krw=500000,
        auto_loop_interval_seconds=15,
        autonomous_loop_enabled=True,
        actor="ops-console",
        reason_code="ENABLE_AUTONOMOUS",
    )

    async def fake_run_once(*, execute_live, persist):
        shadow_service_module.shadow_live_service._loop_stop_event.set()
        return ExecutionPlan(
            candidate_id="",
            intent_id=None,
            planned_order=None,
            selected_price_krw=None,
            quote_basis=None,
            price_source_value=None,
            execute_live=execute_live,
            persisted=persist,
            status="NO_TRADE",
            reason="AUTONOMOUS_LOOP_TEST",
            risk_reason_summary="AUTONOMOUS_LOOP_TEST",
        )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "run_once", fake_run_once)
    try:
        async def orchestrate():
            snapshot = await shadow_service_module.shadow_live_service.start_loop(
                interval_seconds=1,
                execute_live=True,
                persist=True,
            )
            await asyncio.sleep(0)
            running_snapshot = shadow_service_module.shadow_live_service.loop_snapshot()
            await shadow_service_module.shadow_live_service.stop_loop()
            return snapshot, running_snapshot

        snapshot, running_snapshot = asyncio.run(orchestrate())
        assert snapshot.desired_running is True
        assert running_snapshot.execute_live is True
    finally:
        monkeypatch.undo()


def test_shadow_live_verify_sell_reports_zero_balance_after_full_exit() -> None:
    monkeypatch = MonkeyPatch()
    shadow_service_module.shadow_live_service.reset_runtime()
    balance_call_count = {"count": 0}

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        assert symbol == "654321"
        return {"output": {"stck_prpr": "9300"}}

    async def fake_query_asking_price(*, symbol, market_div):
        assert symbol == "654321"
        return {"output1": {"askp1": "9310", "bidp1": "9290"}}

    async def fake_query_balance():
        balance_call_count["count"] += 1
        if balance_call_count["count"] == 1:
            return {
                "output1": [
                    {
                        "pdno": "654321",
                        "hldg_qty": "10",
                        "pchs_avg_pric": "10000",
                        "prpr": "9300",
                        "evlu_amt": "93000",
                        "evlu_pfls_rt": "-7.0",
                    }
                ],
                "output2": [{"tot_evlu_amt": "6400000", "dnca_tot_amt": "6307000"}],
            }
        return {
            "output1": [],
            "output2": [{"tot_evlu_amt": "6400000", "dnca_tot_amt": "6400000"}],
        }

    async def fake_submit_cash_order(payload):
        assert payload["ord_dv"] == "sell"
        return {
            "rt_cd": "0",
            "output": {
                "ODNO": "8300099999",
                "ORD_TMD": "103500",
                "EXCG_ID_DVSN_CD": payload["excg_id_dvsn_cd"],
            },
        }

    async def fake_normalize_order_ack(payload):
        return {
            "event": {
                "broker_order_no": payload["output"]["ODNO"],
            }
        }

    async def fake_query_daily_ccld(*, symbol, broker_order_no, venue_code="KRX", trading_date=None):
        return {
            "output1": [
                {
                    "ODNO": broker_order_no,
                    "PDNO": symbol,
                    "tot_ccld_qty": "10",
                    "avg_prvs": "9290",
                }
            ]
        }

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "submit_cash_order",
        fake_submit_cash_order,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "normalize_order_ack",
        fake_normalize_order_ack,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_daily_ccld",
        fake_query_daily_ccld,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        submit_response = client.post("/run/sample", json={"execute_live": True, "persist": False})

        assert submit_response.status_code == 200
        submit_body = submit_response.json()
        assert submit_body["status"] == "SUBMITTED"
        assert submit_body["planned_order"]["side"] == "sell"
        assert submit_body["planned_order"]["qty"] == 10

        verify_response = client.get("/verify/post-trade/latest")

        assert verify_response.status_code == 200
        body = verify_response.json()
        assert body["status"] == "VERIFIED"
        assert body["matched_fill_qty"] == 10
        assert body["balance_position_qty"] == 0
    finally:
        shadow_service_module.shadow_live_service.reset_runtime()
        monkeypatch.undo()


def test_shadow_live_loop_control_endpoints() -> None:
    monkeypatch = MonkeyPatch()

    async def fake_start_loop(*, interval_seconds, execute_live, persist):
        return {
            "running": True,
            "execute_live": execute_live,
            "persist": persist,
            "interval_seconds": interval_seconds,
            "run_count": 0,
            "last_started_at_utc": None,
            "last_finished_at_utc": None,
            "last_result_status": None,
            "last_error": None,
        }

    async def fake_stop_loop():
        return {
            "running": False,
            "execute_live": False,
            "persist": True,
            "interval_seconds": 60,
            "run_count": 0,
            "last_started_at_utc": None,
            "last_finished_at_utc": None,
            "last_result_status": None,
            "last_error": None,
        }

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "start_loop", fake_start_loop)
    monkeypatch.setattr(shadow_service_module.shadow_live_service, "stop_loop", fake_stop_loop)
    try:
        start_response = client.post(
            "/loop/start",
            json={"interval_seconds": 15, "execute_live": False, "persist": True},
        )
        assert start_response.status_code == 200
        assert start_response.json()["running"] is True
        assert start_response.json()["interval_seconds"] == 15

        stop_response = client.post("/loop/stop")
        assert stop_response.status_code == 200
        assert stop_response.json()["running"] is False
    finally:
        monkeypatch.undo()


def test_shadow_live_live_run_uses_ws_market_guard_and_persists_session_state() -> None:
    monkeypatch = MonkeyPatch()
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-ws-live",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20990101",
            "source_receipt_no": "20260311002001",
        }
    )

    async def fake_select_candidate():
        return candidate

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_ws_status():
        return {
            "running": True,
            "symbols": ["123456"],
            "venue": "KRX",
            "include_fill_notice": True,
            "include_market_status": True,
        }

    async def fake_market_guard(*, symbol, venue="KRX"):
        return {
            "instrument_id": symbol,
            "venue": venue,
            "entry_allowed": True,
            "reason_codes": [],
            "ws_running": True,
            "ws_market_data_stale": False,
            "session_open": True,
            "halt_detected": False,
            "latest_quote_at_utc": "2026-03-11T00:31:00+00:00",
            "latest_trade_at_utc": "2026-03-11T00:31:00+00:00",
            "quote_snapshot": {
                "instrument_id": symbol,
                "best_ask_px": 4900,
                "best_bid_px": 4890,
                "exchange_ts_utc": "2026-03-11T00:31:00+00:00",
            },
            "trade_snapshot": {
                "instrument_id": symbol,
                "last_price": 4890,
                "received_ts_utc": "2026-03-11T00:31:00+00:00",
            },
            "market_status_snapshot": {
                "venue": venue,
                "status_code": "OPEN",
            },
        }

    async def fake_query_balance():
        return {"output1": [], "output2": [{"tot_evlu_amt": "6400000", "dnca_tot_amt": "6400000"}]}

    async def fake_submit_cash_order(payload):
        return {
            "rt_cd": "0",
            "output": {
                "ODNO": "8300012000",
                "ORD_TMD": "103000",
                "EXCG_ID_DVSN_CD": payload["excg_id_dvsn_cd"],
            },
        }

    async def fake_normalize_order_ack(payload):
        return {
            "event": {
                "broker_order_no": payload["output"]["ODNO"],
            }
        }

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "ws_status",
        fake_ws_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "market_guard",
        fake_market_guard,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "submit_cash_order",
        fake_submit_cash_order,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "normalize_order_ack",
        fake_normalize_order_ack,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "shadow_live_require_ws_live_market_data",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "SUBMITTED"
        assert body["quote_basis"] == "WS_BEST_ASK"
        assert body["planned_order"]["price"] == 4900
        assert store.sessions["KRX"].session_code == "OPEN"
        assert store.sessions["KRX"].entry_allowed is True
    finally:
        monkeypatch.undo()


def test_shadow_live_blocks_live_execution_when_ws_market_guard_blocks_entry() -> None:
    monkeypatch = MonkeyPatch()
    candidate = market_intel_service.sample_candidates()[0].model_copy(
        update={
            "candidate_id": "candidate-ws-blocked",
            "instrument_id": "123456",
            "cooldown_key": "123456:BUYBACK:20990101",
            "source_receipt_no": "20260311002002",
        }
    )

    async def fake_select_candidate():
        return candidate

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_ws_status():
        return {
            "running": True,
            "symbols": ["123456"],
            "venue": "KRX",
            "include_fill_notice": True,
            "include_market_status": True,
        }

    async def fake_market_guard(*, symbol, venue="KRX"):
        return {
            "instrument_id": symbol,
            "venue": venue,
            "entry_allowed": False,
            "reason_codes": ["TRADING_HALT_ACTIVE"],
            "ws_running": True,
            "ws_market_data_stale": False,
            "session_open": True,
            "halt_detected": True,
            "latest_quote_at_utc": "2026-03-11T00:31:00+00:00",
            "latest_trade_at_utc": "2026-03-11T00:31:00+00:00",
            "quote_snapshot": {
                "instrument_id": symbol,
                "best_ask_px": 4900,
                "best_bid_px": 4890,
                "exchange_ts_utc": "2026-03-11T00:31:00+00:00",
            },
            "trade_snapshot": {
                "instrument_id": symbol,
                "last_price": 4890,
                "received_ts_utc": "2026-03-11T00:31:00+00:00",
            },
            "market_status_snapshot": {
                "venue": venue,
                "status_code": "HALT",
            },
        }

    async def fake_query_balance():
        return {"output1": [], "output2": [{"tot_evlu_amt": "6400000", "dnca_tot_amt": "6400000"}]}

    async def fail_submit_cash_order(_payload):
        raise AssertionError("submit_cash_order should not be called when market guard blocks entry")

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "ws_status",
        fake_ws_status,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "market_guard",
        fake_market_guard,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "submit_cash_order",
        fail_submit_cash_order,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "shadow_live_require_ws_live_market_data",
        True,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "BLOCKED"
        assert "TRADING_HALT_ACTIVE" in body["reason"]
        assert store.sessions["KRX"].session_code == "HALTED"
        assert store.sessions["KRX"].entry_allowed is False
    finally:
        monkeypatch.undo()


def test_shadow_live_restore_loop_if_needed_resumes_durable_loop_state() -> None:
    monkeypatch = MonkeyPatch()
    owner_id = "shadow-live-previous"
    store.acquire_loop_lease(
        loop_id="shadow-live-main",
        service_name="shadow-live",
        owner_id=owner_id,
        interval_seconds=5,
        execute_live=False,
        persist=True,
        ttl_seconds=60.0,
        actor=owner_id,
        reason_code="TEST_START",
    )
    store.release_loop_lease(
        loop_id="shadow-live-main",
        owner_id=owner_id,
        desired_running=True,
        reason_code="TEST_SUSPEND",
    )

    async def fake_run_once(*, execute_live, persist):
        shadow_service_module.shadow_live_service._loop_stop_event.set()
        return ExecutionPlan(
            candidate_id="",
            intent_id=None,
            planned_order=None,
            selected_price_krw=None,
            quote_basis=None,
            price_source_value=None,
            execute_live=execute_live,
            persisted=persist,
            status="NO_TRADE",
            reason="AUTO_RESUME_TEST",
            risk_reason_summary="AUTO_RESUME_TEST",
        )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "run_once", fake_run_once)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "shadow_live_loop_auto_resume",
        True,
    )

    async def orchestrate():
        snapshot = await shadow_service_module.shadow_live_service.restore_loop_if_needed()
        await asyncio.sleep(0)
        running_snapshot = shadow_service_module.shadow_live_service.loop_snapshot()
        await shadow_service_module.shadow_live_service.stop_loop()
        return snapshot, running_snapshot

    try:
        restored_snapshot, running_snapshot = asyncio.run(orchestrate())
        assert restored_snapshot.desired_running is True
        assert running_snapshot.restored_from_durable is True
        assert running_snapshot.run_count >= 1
    finally:
        monkeypatch.undo()


def test_shadow_live_uses_runtime_live_control_cap_for_live_orders() -> None:
    monkeypatch = MonkeyPatch()

    async def fake_refresh_live_risk_state():
        return {
            "current_total_equity_krw": 6400000,
            "baseline_total_equity_krw": 6500000,
            "daily_loss_pct": 0.0,
            "entry_paused": False,
            "live_pause_reason": None,
        }

    async def fake_query_price(*, symbol, market_div):
        return {"output": {"stck_prpr": "70100"}}

    async def fake_query_asking_price(*, symbol, market_div):
        return {"output1": {"askp1": "70200", "bidp1": "70100"}}

    async def fake_query_balance():
        return {"output1": [], "output2": [{"tot_evlu_amt": "6400000", "dnca_tot_amt": "6400000"}]}

    store.set_live_control(
        max_order_value_krw=50000,
        auto_loop_interval_seconds=60,
        actor="test",
        reason_code="LOW_CAP",
    )

    monkeypatch.setattr(shadow_service_module.shadow_live_service, "_select_candidate", fake_select_candidate)
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "refresh_live_risk_state",
        fake_refresh_live_risk_state,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_price",
        fake_query_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_asking_price",
        fake_query_asking_price,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.gateway_client,
        "query_balance",
        fake_query_balance,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "trading_micro_test_mode_enabled",
        False,
    )
    monkeypatch.setattr(
        shadow_service_module.shadow_live_service.settings,
        "selector_live_require_promotion_approval",
        False,
    )
    try:
        response = client.post("/run/sample", json={"execute_live": True, "persist": False})
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "BLOCKED"
        assert "exceeds sizing limit 50,000KRW" in body["reason"]
    finally:
        monkeypatch.undo()
