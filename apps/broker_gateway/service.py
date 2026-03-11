from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

from libs.adapters.kis import KISHttpBrokerGateway
from libs.adapters.kis_mapper import map_fill_notice, map_order_ack
from libs.adapters.minio_store import MinioObjectStore
from libs.adapters.nats import NatsEventBus
from libs.config.settings import get_settings
from libs.db.base import SessionLocal
from libs.db.repositories import TradingRepository
from libs.domain.enums import MessageType
from libs.domain.enums import Environment
from libs.services.common_stock_universe import CommonStockUniverseError
from libs.services.common_stock_universe import CommonStockUniverseService
from libs.services.event_pipeline import EventPipelineService
from libs.services.raw_event_service import RawEventService


@dataclass(slots=True)
class BrokerSessionState:
    rest_token_ready: bool
    ws_approval_ready: bool
    last_rest_auth_utc: datetime | None
    last_ws_auth_utc: datetime | None
    current_mode: str
    pending_rate_budget: int
    degraded_reason: str | None = None
    allowed_envs: list[str] | None = None
    live_trading_enabled: bool = False
    live_trading_armed: bool = False
    live_trading_armed_by: str | None = None
    live_trading_armed_at_utc: datetime | None = None
    last_total_equity_krw: int | None = None
    baseline_total_equity_krw: int | None = None
    daily_loss_pct: float | None = None
    entry_paused: bool = False
    live_pause_reason: str | None = None
    common_stock_universe_count: int | None = None


class LiveTradingGuardError(RuntimeError):
    pass


@dataclass(slots=True)
class BrokerGatewayRuntime:
    adapter: KISHttpBrokerGateway = field(default_factory=KISHttpBrokerGateway)
    repository: TradingRepository = field(default_factory=lambda: TradingRepository(SessionLocal))
    raw_event_service: RawEventService = field(
        default_factory=lambda: RawEventService(
            repository=TradingRepository(SessionLocal),
            object_store=MinioObjectStore(),
        )
    )
    event_pipeline: EventPipelineService = field(
        default_factory=lambda: EventPipelineService(event_bus=NatsEventBus())
    )
    common_stock_universe: CommonStockUniverseService = field(default_factory=CommonStockUniverseService)
    last_rest_auth_utc: datetime | None = None
    last_ws_auth_utc: datetime | None = None
    live_trading_armed: bool = False
    live_trading_armed_by: str | None = None
    live_trading_armed_at_utc: datetime | None = None
    last_total_equity_krw: int | None = None
    baseline_total_equity_krw: int | None = None
    daily_loss_pct: float | None = None
    entry_paused: bool = False
    live_pause_reason: str | None = None

    def snapshot(self) -> BrokerSessionState:
        settings = get_settings()
        common_stock_snapshot = self.common_stock_universe.snapshot()
        return BrokerSessionState(
            rest_token_ready=self.last_rest_auth_utc is not None,
            ws_approval_ready=self.last_ws_auth_utc is not None,
            last_rest_auth_utc=self.last_rest_auth_utc,
            last_ws_auth_utc=self.last_ws_auth_utc,
            current_mode="NORMAL",
            pending_rate_budget=42,
            allowed_envs=["prod"] if not settings.kis_enable_paper else ["prod", "vps"],
            live_trading_enabled=settings.kis_live_trading_enabled,
            live_trading_armed=self.live_trading_armed,
            live_trading_armed_by=self.live_trading_armed_by,
            live_trading_armed_at_utc=self.live_trading_armed_at_utc,
            last_total_equity_krw=self.last_total_equity_krw,
            baseline_total_equity_krw=self.baseline_total_equity_krw,
            daily_loss_pct=self.daily_loss_pct,
            entry_paused=self.entry_paused,
            live_pause_reason=self.live_pause_reason,
            common_stock_universe_count=common_stock_snapshot.symbol_count or None,
        )

    def _ensure_allowed_env(self, env: Environment) -> None:
        settings = get_settings()
        if env == Environment.VPS and not settings.kis_enable_paper:
            raise LiveTradingGuardError("paper trading is disabled; use env=prod only")

    def _ensure_expected_account(self, payload: dict[str, Any]) -> None:
        settings = get_settings()
        cano = str(payload.get("cano", ""))
        product_code = str(payload.get("acnt_prdt_cd", ""))
        if settings.kis_account_no and cano and cano != settings.kis_account_no:
            raise LiveTradingGuardError("payload cano does not match configured live account")
        if settings.kis_account_product_code and product_code and product_code != settings.kis_account_product_code:
            raise LiveTradingGuardError("payload acnt_prdt_cd does not match configured live account")

    def _balance_query_payload(self) -> dict[str, Any]:
        settings = get_settings()
        return {
            "env": Environment.PROD,
            "cano": settings.kis_account_no,
            "acnt_prdt_cd": settings.kis_account_product_code,
            "afhr_flpr_yn": "N",
            "ofl_yn": "",
            "inqr_dvsn": "02",
            "unpr_dvsn": "01",
            "fund_sttl_icld_yn": "N",
            "fncg_amt_auto_rdpt_yn": "N",
            "prcs_dvsn": "00",
            "ctx_area_fk100": "",
            "ctx_area_nk100": "",
        }

    def _parse_live_risk_metrics(self, payload: dict[str, Any]) -> tuple[int, int, float]:
        summary = (payload.get("output2") or [{}])[0]
        current_total = int(float(summary.get("tot_evlu_amt") or summary.get("nass_amt") or 0))
        baseline_total = int(float(summary.get("bfdy_tot_asst_evlu_amt") or current_total or 0))
        if baseline_total <= 0:
            daily_loss_pct = 0.0
        else:
            daily_loss_pct = max(((baseline_total - current_total) / baseline_total) * 100.0, 0.0)
        return current_total, baseline_total, daily_loss_pct

    async def refresh_live_risk_state(self) -> dict[str, Any]:
        settings = get_settings()
        payload = await self.adapter.query_balance(self._balance_query_payload())
        current_total, baseline_total, daily_loss_pct = self._parse_live_risk_metrics(payload)
        self.last_total_equity_krw = current_total
        self.baseline_total_equity_krw = baseline_total
        self.daily_loss_pct = daily_loss_pct

        pause_reasons: list[str] = []
        if settings.kis_live_min_total_equity_krw > 0 and current_total <= settings.kis_live_min_total_equity_krw:
            pause_reasons.append(f"TOTAL_EQUITY_AT_OR_BELOW_{settings.kis_live_min_total_equity_krw}")
        if settings.kis_live_daily_loss_limit_pct > 0 and daily_loss_pct >= settings.kis_live_daily_loss_limit_pct:
            pause_reasons.append(f"DAILY_LOSS_LIMIT_{settings.kis_live_daily_loss_limit_pct:.2f}_PCT_BREACHED")

        self.entry_paused = bool(pause_reasons)
        self.live_pause_reason = ",".join(pause_reasons) if pause_reasons else None
        if self.entry_paused:
            self.live_trading_armed = False

        return {
            "current_total_equity_krw": current_total,
            "baseline_total_equity_krw": baseline_total,
            "daily_loss_pct": round(daily_loss_pct, 4),
            "entry_paused": self.entry_paused,
            "live_pause_reason": self.live_pause_reason,
        }

    async def _ensure_common_stock_symbol(self, symbol: str) -> None:
        settings = get_settings()
        if not settings.kis_live_common_stock_only:
            return
        try:
            is_common = await self.common_stock_universe.is_common_stock(symbol)
        except CommonStockUniverseError as exc:
            raise LiveTradingGuardError(f"failed to verify common stock universe: {exc}") from exc
        if not is_common:
            raise LiveTradingGuardError(f"symbol {symbol} is not recognized as a regular listed stock")

    async def _ensure_live_order_allowed(self, payload: dict[str, Any]) -> None:
        settings = get_settings()
        if not settings.kis_live_trading_enabled:
            raise LiveTradingGuardError("live trading is disabled in configuration")
        risk_state = await self.refresh_live_risk_state()
        if risk_state["entry_paused"]:
            raise LiveTradingGuardError(f"live entry paused: {risk_state['live_pause_reason']}")
        if settings.kis_live_require_arm and not self.live_trading_armed:
            raise LiveTradingGuardError("live trading is not armed")
        self._ensure_expected_account(payload)

        symbol = str(payload.get("pdno", "")).strip()
        allowed_symbols = settings.kis_live_allowed_symbol_list
        if allowed_symbols and symbol and symbol not in allowed_symbols:
            raise LiveTradingGuardError(f"symbol {symbol} is not in KIS_LIVE_ALLOWED_SYMBOLS")
        if symbol:
            await self._ensure_common_stock_symbol(symbol)

        qty = int(payload.get("ord_qty", 0))
        price = int(float(payload.get("ord_unpr", 0)))
        order_value = qty * price
        if settings.kis_live_max_order_value_krw > 0 and order_value > settings.kis_live_max_order_value_krw:
            raise LiveTradingGuardError(
                f"order value {order_value} exceeds KIS_LIVE_MAX_ORDER_VALUE_KRW={settings.kis_live_max_order_value_krw}"
            )

    async def arm_live_trading(self, armed_by: str, reason: str | None = None) -> dict[str, Any]:
        settings = get_settings()
        if not settings.kis_live_trading_enabled:
            raise LiveTradingGuardError("set KIS_LIVE_TRADING_ENABLED=true before arming live trading")
        await self.refresh_live_risk_state()
        if self.entry_paused and self.live_pause_reason:
            raise LiveTradingGuardError(f"cannot arm while entry is paused: {self.live_pause_reason}")
        self.live_trading_armed = True
        self.live_trading_armed_by = armed_by if reason is None else f"{armed_by}:{reason}"
        self.live_trading_armed_at_utc = datetime.now(UTC)
        return {
            "live_trading_enabled": settings.kis_live_trading_enabled,
            "live_trading_armed": self.live_trading_armed,
            "live_trading_armed_by": self.live_trading_armed_by,
            "live_trading_armed_at_utc": self.live_trading_armed_at_utc,
        }

    def disarm_live_trading(self, disarmed_by: str, reason: str | None = None) -> dict[str, Any]:
        self.live_trading_armed = False
        self.live_trading_armed_by = disarmed_by if reason is None else f"{disarmed_by}:{reason}"
        self.live_trading_armed_at_utc = datetime.now(UTC)
        return {
            "live_trading_enabled": get_settings().kis_live_trading_enabled,
            "live_trading_armed": self.live_trading_armed,
            "live_trading_armed_by": self.live_trading_armed_by,
            "live_trading_armed_at_utc": self.live_trading_armed_at_utc,
        }

    async def issue_rest_token(self, env: Environment) -> dict[str, Any]:
        self._ensure_allowed_env(env)
        token = await self.adapter.issue_rest_token(env)
        self.last_rest_auth_utc = datetime.now(UTC)
        return {
            "access_token_prefix": token.access_token[:8],
            "expires_at_utc": token.expires_at_utc,
            "token_type": token.token_type,
        }

    async def issue_ws_approval(self, env: Environment) -> dict[str, Any]:
        self._ensure_allowed_env(env)
        approval = await self.adapter.issue_ws_approval(env)
        self.last_ws_auth_utc = approval.issued_at_utc
        return {
            "approval_key_prefix": approval.approval_key[:8],
            "issued_at_utc": approval.issued_at_utc,
        }

    async def submit_cash_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        if env == Environment.PROD:
            await self._ensure_live_order_allowed(payload)
        return await self.adapter.submit_cash_order(payload)

    async def submit_cancel_replace(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        self._ensure_expected_account(payload)
        rvse_cncl_dvsn_cd = str(payload.get("rvse_cncl_dvsn_cd", ""))
        if env == Environment.PROD and rvse_cncl_dvsn_cd != "02":
            await self._ensure_live_order_allowed(payload)
        return await self.adapter.submit_cancel_replace(payload)

    async def query_psbl_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        self._ensure_expected_account(payload)
        return await self.adapter.query_psbl_order(payload)

    async def query_price(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        return await self.adapter.query_price(payload)

    async def query_asking_price(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        return await self.adapter.query_asking_price(payload)

    async def query_balance(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        self._ensure_expected_account(payload)
        response = await self.adapter.query_balance(payload)
        if env == Environment.PROD:
            try:
                current_total, baseline_total, daily_loss_pct = self._parse_live_risk_metrics(response)
                self.last_total_equity_krw = current_total
                self.baseline_total_equity_krw = baseline_total
                self.daily_loss_pct = daily_loss_pct
                settings = get_settings()
                self.entry_paused = (
                    (settings.kis_live_min_total_equity_krw > 0 and current_total <= settings.kis_live_min_total_equity_krw)
                    or (
                        settings.kis_live_daily_loss_limit_pct > 0
                        and daily_loss_pct >= settings.kis_live_daily_loss_limit_pct
                    )
                )
                if self.entry_paused:
                    reasons = []
                    if current_total <= settings.kis_live_min_total_equity_krw:
                        reasons.append(f"TOTAL_EQUITY_AT_OR_BELOW_{settings.kis_live_min_total_equity_krw}")
                    if daily_loss_pct >= settings.kis_live_daily_loss_limit_pct:
                        reasons.append(
                            f"DAILY_LOSS_LIMIT_{settings.kis_live_daily_loss_limit_pct:.2f}_PCT_BREACHED"
                        )
                    self.live_pause_reason = ",".join(reasons)
                    self.live_trading_armed = False
                else:
                    self.live_pause_reason = None
            except Exception:
                pass
        return response

    async def query_daily_ccld(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        self._ensure_allowed_env(env)
        self._ensure_expected_account(payload)
        return await self.adapter.query_daily_ccld(payload)

    async def normalize_order_ack(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            receipt = self.raw_event_service.store(
                source_system_code="KIS",
                channel_code="REST",
                endpoint_code="order-cash",
                payload_json=payload,
                source_object_id=str(payload.get("output", {}).get("ODNO") or payload.get("msg_cd") or "order-ack"),
                venue_code=payload.get("output", {}).get("EXCG_ID_DVSN_CD"),
            )
        except Exception:
            fallback_raw_service = RawEventService()
            receipt = fallback_raw_service.store(
                source_system_code="KIS",
                channel_code="REST",
                endpoint_code="order-cash",
                payload_json=payload,
                source_object_id=str(payload.get("output", {}).get("ODNO") or payload.get("msg_cd") or "order-ack"),
                venue_code=payload.get("output", {}).get("EXCG_ID_DVSN_CD"),
            )
        event = map_order_ack(
            payload=payload,
            internal_order_id=payload["internal_order_id"],
            client_order_id=payload["client_order_id"],
            raw_ref=f"raw:{receipt.checksum[:12]}",
            venue=payload.get("output", {}).get("EXCG_ID_DVSN_CD"),
        )
        try:
            envelope = await self.event_pipeline.publish_order_ack(event)
        except Exception:
            self.event_pipeline = EventPipelineService()
            envelope = await self.event_pipeline.publish_order_ack(event)
        return {
            "raw_receipt": {
                "checksum": receipt.checksum,
                "stored_at_utc": receipt.stored_at_utc,
            },
            "event": event,
            "envelope": envelope.envelope,
            "message_type": MessageType.EVENT,
        }

    async def normalize_fill_notice(self, payload: dict[str, Any]) -> dict[str, Any]:
        try:
            receipt = self.raw_event_service.store(
                source_system_code="KIS",
                channel_code="WS",
                endpoint_code="fill-notice",
                payload_json=payload,
                source_object_id=str(payload.get("ODER_NO") or payload.get("oder_no") or "fill-notice"),
            )
        except Exception:
            fallback_raw_service = RawEventService()
            receipt = fallback_raw_service.store(
                source_system_code="KIS",
                channel_code="WS",
                endpoint_code="fill-notice",
                payload_json=payload,
                source_object_id=str(payload.get("ODER_NO") or payload.get("oder_no") or "fill-notice"),
            )
        event = map_fill_notice(
            payload=payload,
            internal_order_id=payload["internal_order_id"],
            account_id=payload["account_id"],
            raw_ref=f"raw:{receipt.checksum[:12]}",
        )
        try:
            envelope = await self.event_pipeline.publish_fill(event)
        except Exception:
            self.event_pipeline = EventPipelineService()
            envelope = await self.event_pipeline.publish_fill(event)
        return {
            "raw_receipt": {
                "checksum": receipt.checksum,
                "stored_at_utc": receipt.stored_at_utc,
            },
            "event": event,
            "envelope": envelope.envelope,
        }

    async def close(self) -> None:
        await self.adapter.close()
        await self.common_stock_universe.close()
        if self.event_pipeline.event_bus is not None:
            await self.event_pipeline.event_bus.close()


runtime = BrokerGatewayRuntime()
