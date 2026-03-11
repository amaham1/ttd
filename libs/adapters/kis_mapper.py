from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from libs.contracts.messages import FillEvent, MarketTick, OrderAckEvent, QuoteL1
from libs.domain.enums import OrderSide


def _parse_ts(value: str | None, *, fallback_date: datetime | None = None) -> datetime:
    fallback = fallback_date or datetime.now(UTC)
    if not value:
        return fallback
    value = value.strip()
    try:
        if len(value) == 6:
            return fallback.replace(
                hour=int(value[0:2]),
                minute=int(value[2:4]),
                second=int(value[4:6]),
                microsecond=0,
            )
        if len(value) == 14:
            return datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
    except ValueError:
        return fallback
    return fallback


def map_order_ack(
    *,
    payload: dict[str, Any],
    internal_order_id: str,
    client_order_id: str,
    raw_ref: str | None = None,
    venue: str | None = None,
) -> OrderAckEvent:
    output = payload.get("output", payload)
    broker_order_no = output.get("ODNO") or output.get("odno") or ""
    ack_ts = _parse_ts(output.get("ORD_TMD") or output.get("ord_tmd"))
    return OrderAckEvent(
        internal_order_id=internal_order_id,
        client_order_id=client_order_id,
        broker_order_no=str(broker_order_no),
        broker_status_code=payload.get("msg_cd") or payload.get("rt_cd"),
        ack_ts_utc=ack_ts,
        venue=venue or output.get("EXCG_ID_DVSN_CD") or output.get("excg_id_dvsn_cd"),
        raw_ref=raw_ref,
    )


def map_fill_notice(
    *,
    payload: dict[str, Any],
    internal_order_id: str,
    account_id: str,
    raw_ref: str | None = None,
) -> FillEvent:
    side_code = str(payload.get("SELN_BYOV_CLS") or payload.get("seln_byov_cls") or "")
    side = OrderSide.BUY if side_code == "02" else OrderSide.SELL
    return FillEvent(
        internal_order_id=internal_order_id,
        broker_order_no=str(payload.get("ODER_NO") or payload.get("oder_no") or ""),
        broker_trade_id=(payload.get("CNTG_SEQ") or payload.get("cntg_seq") or None),
        account_id=account_id,
        instrument_id=str(payload.get("STCK_SHRN_ISCD") or payload.get("stck_shrn_iscd") or ""),
        side=side,
        venue=payload.get("ORD_EXG_GB") or payload.get("ord_exg_gb"),
        fill_ts_utc=_parse_ts(payload.get("STCK_CNTG_HOUR") or payload.get("stck_cntg_hour")),
        price=int(payload.get("CNTG_UNPR") or payload.get("cntg_unpr") or 0),
        qty=int(payload.get("CNTG_QTY") or payload.get("cntg_qty") or 0),
        raw_ref=raw_ref,
    )


def map_trade_tick(
    *,
    payload: dict[str, Any],
    instrument_id: str | None = None,
    venue: str = "KRX",
    raw_ref: str | None = None,
) -> MarketTick:
    return MarketTick(
        instrument_id=instrument_id or str(payload.get("MKSC_SHRN_ISCD") or payload.get("mksc_shrn_iscd") or ""),
        venue=venue,
        exchange_ts_utc=_parse_ts(payload.get("STCK_CNTG_HOUR") or payload.get("stck_cntg_hour")),
        received_ts_utc=datetime.now(UTC),
        last_price=int(payload.get("STCK_PRPR") or payload.get("stck_prpr") or 0),
        last_qty=int(payload.get("CNTG_VOL") or payload.get("cntg_vol") or 0),
        cum_volume=int(payload.get("ACML_VOL") or payload.get("acml_vol") or 0),
        trade_strength=float(payload.get("CTTR") or payload.get("cttr") or 0) if (payload.get("CTTR") or payload.get("cttr")) else None,
        raw_ref=raw_ref,
    )


def map_quote_l1(
    *,
    payload: dict[str, Any],
    instrument_id: str | None = None,
    venue: str = "KRX",
    raw_ref: str | None = None,
) -> QuoteL1:
    bid = int(payload.get("BIDP1") or payload.get("bidp1") or 0)
    ask = int(payload.get("ASKP1") or payload.get("askp1") or 0)
    spread_bps = None
    if bid > 0 and ask > 0:
        mid = (bid + ask) / 2
        if mid > 0:
            spread_bps = ((ask - bid) / mid) * 10_000
    return QuoteL1(
        instrument_id=instrument_id or str(payload.get("MKSC_SHRN_ISCD") or payload.get("mksc_shrn_iscd") or ""),
        venue=venue,
        exchange_ts_utc=_parse_ts(payload.get("STCK_CNTG_HOUR") or payload.get("stck_cntg_hour")),
        best_bid_px=bid,
        best_bid_qty=int(payload.get("BIDP_RSQN1") or payload.get("bidp_rsqn1") or 0),
        best_ask_px=ask,
        best_ask_qty=int(payload.get("ASKP_RSQN1") or payload.get("askp_rsqn1") or 0),
        total_bid_qty=int(payload.get("TOTAL_BIDP_RSQN") or payload.get("total_bidp_rsqn") or 0),
        total_ask_qty=int(payload.get("TOTAL_ASKP_RSQN") or payload.get("total_askp_rsqn") or 0),
        spread_bps=spread_bps,
        raw_ref=raw_ref,
    )
