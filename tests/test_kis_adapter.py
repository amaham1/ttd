from __future__ import annotations

import httpx
import pytest

from libs.adapters.kis import KISHttpBrokerGateway
from libs.config.settings import Settings
from libs.domain.enums import Environment


@pytest.mark.asyncio
async def test_issue_rest_token_uses_expected_endpoint() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/oauth2/tokenP"
        payload = {"access_token": "token-123", "expires_in": 3600, "token_type": "Bearer"}
        return httpx.Response(200, json=payload)

    settings = Settings(
        kis_base_url="https://prod.example.com",
        kis_vts_base_url="https://paper.example.com",
        kis_app_key="real-key",
        kis_app_secret="real-secret",
        kis_paper_app_key="paper-key",
        kis_paper_app_secret="paper-secret",
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    gateway = KISHttpBrokerGateway(settings=settings, client=client)

    token = await gateway.issue_rest_token(Environment.PROD)

    assert token.access_token == "token-123"
    await gateway.close()


@pytest.mark.asyncio
async def test_submit_cash_order_issues_hashkey_and_order_request() -> None:
    seen_paths: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        if request.url.path == "/oauth2/tokenP":
            return httpx.Response(200, json={"access_token": "token-123", "expires_in": 3600})
        if request.url.path == "/uapi/hashkey":
            return httpx.Response(200, json={"HASH": "hash-123"})
        assert request.url.path == "/uapi/domestic-stock/v1/trading/order-cash"
        assert request.headers["tr_id"] == "VTTC0012U"
        assert request.headers["hashkey"] == "hash-123"
        return httpx.Response(200, json={"rt_cd": "0", "output": {"ODNO": "8300012345"}})

    settings = Settings(
        kis_base_url="https://prod.example.com",
        kis_vts_base_url="https://paper.example.com",
        kis_app_key="real-key",
        kis_app_secret="real-secret",
        kis_paper_app_key="paper-key",
        kis_paper_app_secret="paper-secret",
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    gateway = KISHttpBrokerGateway(settings=settings, client=client)

    response = await gateway.submit_cash_order(
        {
            "env": Environment.VPS,
            "ord_dv": "buy",
            "cano": "12345678",
            "acnt_prdt_cd": "01",
            "pdno": "005930",
            "ord_dvsn": "00",
            "ord_qty": "1",
            "ord_unpr": "70000",
            "excg_id_dvsn_cd": "KRX",
        }
    )

    assert response["output"]["ODNO"] == "8300012345"
    assert seen_paths == ["/oauth2/tokenP", "/uapi/hashkey", "/uapi/domestic-stock/v1/trading/order-cash"]
    await gateway.close()


@pytest.mark.asyncio
async def test_query_price_and_asking_price_use_expected_endpoints() -> None:
    seen_paths: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        seen_paths.append(request.url.path)
        if request.url.path == "/oauth2/tokenP":
            return httpx.Response(200, json={"access_token": "token-123", "expires_in": 3600})
        if request.url.path == "/uapi/domestic-stock/v1/quotations/inquire-price":
            assert request.headers["tr_id"] == "FHKST01010100"
            return httpx.Response(200, json={"output": {"stck_prpr": "70100"}})
        assert request.url.path == "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn"
        assert request.headers["tr_id"] == "FHKST01010200"
        return httpx.Response(200, json={"output1": {"askp1": "70200", "bidp1": "70100"}})

    settings = Settings(
        kis_base_url="https://prod.example.com",
        kis_vts_base_url="https://paper.example.com",
        kis_app_key="real-key",
        kis_app_secret="real-secret",
        kis_paper_app_key="paper-key",
        kis_paper_app_secret="paper-secret",
    )
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    gateway = KISHttpBrokerGateway(settings=settings, client=client)

    price_response = await gateway.query_price(
        {"env": Environment.PROD, "fid_cond_mrkt_div_code": "J", "fid_input_iscd": "005930"}
    )
    asking_response = await gateway.query_asking_price(
        {"env": Environment.PROD, "fid_cond_mrkt_div_code": "J", "fid_input_iscd": "005930"}
    )

    assert price_response["output"]["stck_prpr"] == "70100"
    assert asking_response["output1"]["askp1"] == "70200"
    assert seen_paths == [
        "/oauth2/tokenP",
        "/uapi/domestic-stock/v1/quotations/inquire-price",
        "/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
    ]
    await gateway.close()
