from libs.adapters.kis_mapper import map_fill_notice, map_order_ack, map_quote_l1, map_trade_tick


def test_map_order_ack_extracts_order_number_and_time() -> None:
    event = map_order_ack(
        payload={
            "msg_cd": "APBK0013",
            "output": {"ODNO": "8300012345", "ORD_TMD": "101530", "EXCG_ID_DVSN_CD": "KRX"},
        },
        internal_order_id="order-1",
        client_order_id="client-1",
        raw_ref="raw:abc123",
    )

    assert event.broker_order_no == "8300012345"
    assert event.venue == "KRX"
    assert event.raw_ref == "raw:abc123"


def test_map_fill_notice_maps_buy_side_and_qty() -> None:
    event = map_fill_notice(
        payload={
            "ODER_NO": "8300012345",
            "SELN_BYOV_CLS": "02",
            "STCK_SHRN_ISCD": "005930",
            "STCK_CNTG_HOUR": "102000",
            "CNTG_UNPR": "70100",
            "CNTG_QTY": "3",
        },
        internal_order_id="order-1",
        account_id="default",
    )

    assert event.instrument_id == "005930"
    assert event.qty == 3
    assert event.price == 70100
    assert event.side.value == "buy"


def test_map_trade_tick_and_quote() -> None:
    tick = map_trade_tick(
        payload={"MKSC_SHRN_ISCD": "005930", "STCK_CNTG_HOUR": "102100", "STCK_PRPR": "70200", "CNTG_VOL": "7", "ACML_VOL": "100"},
        raw_ref="raw:t1",
    )
    quote = map_quote_l1(
        payload={"MKSC_SHRN_ISCD": "005930", "ASKP1": "70200", "BIDP1": "70100", "ASKP_RSQN1": "4", "BIDP_RSQN1": "6"},
        raw_ref="raw:q1",
    )

    assert tick.last_price == 70200
    assert tick.raw_ref == "raw:t1"
    assert quote.best_ask_px == 70200
    assert quote.best_bid_px == 70100
    assert quote.spread_bps is not None
