from __future__ import annotations

import json
from abc import ABC, abstractmethod
from base64 import b64decode
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import websockets
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad

from libs.config.settings import Settings, get_settings
from libs.domain.enums import Environment


class KISAPIError(RuntimeError):
    def __init__(self, message: str, *, status_code: int | None = None, payload: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.payload = payload or {}


@dataclass(slots=True)
class KISToken:
    access_token: str
    expires_at_utc: datetime
    token_type: str = "Bearer"


@dataclass(slots=True)
class KISApproval:
    approval_key: str
    issued_at_utc: datetime


@dataclass(slots=True)
class KISHashKey:
    hashkey: str
    issued_at_utc: datetime


@dataclass(slots=True)
class KISWebSocketSubscription:
    tr_id: str
    tr_key: str


def _aes_cbc_base64_dec(key: str, iv: str, cipher_text: str) -> str:
    cipher = AES.new(key.encode("utf-8"), AES.MODE_CBC, iv.encode("utf-8"))
    return unpad(cipher.decrypt(b64decode(cipher_text)), AES.block_size).decode("utf-8")


def _parse_kis_ws_control_message(raw: str) -> dict[str, Any]:
    payload = json.loads(raw)
    header = payload.get("header", {})
    body = payload.get("body", {})
    output = body.get("output", {})
    return {
        "tr_id": header.get("tr_id", ""),
        "tr_key": header.get("tr_key", ""),
        "encrypt": header.get("encrypt", "N"),
        "rt_cd": body.get("rt_cd"),
        "msg_cd": body.get("msg_cd"),
        "msg1": body.get("msg1"),
        "iv": output.get("iv"),
        "key": output.get("key"),
        "is_pingpong": header.get("tr_id") == "PINGPONG",
    }


def _environment_name(env: Environment) -> str:
    return "real" if env == Environment.PROD else "demo"


def _resolve_credentials(settings: Settings, env: Environment) -> tuple[str, str]:
    if env == Environment.PROD:
        return settings.kis_app_key, settings.kis_app_secret
    return settings.kis_paper_app_key, settings.kis_paper_app_secret


def _resolve_base_url(settings: Settings, env: Environment) -> str:
    return settings.kis_base_url if env == Environment.PROD else settings.kis_vts_base_url


def _resolve_ws_url(settings: Settings, env: Environment) -> str:
    return settings.kis_ws_url if env == Environment.PROD else settings.kis_vts_ws_url


class KISBrokerGateway(ABC):
    @abstractmethod
    async def issue_rest_token(self, env: Environment) -> KISToken:
        raise NotImplementedError

    @abstractmethod
    async def issue_ws_approval(self, env: Environment) -> KISApproval:
        raise NotImplementedError

    @abstractmethod
    async def submit_cash_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def submit_cancel_replace(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def query_psbl_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def query_balance(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def query_daily_ccld(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def query_price(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def query_asking_price(self, payload: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def subscribe_quote(self, symbol: str, venue: str = "KRX") -> None:
        raise NotImplementedError

    @abstractmethod
    async def subscribe_trade(self, symbol: str, venue: str = "KRX") -> None:
        raise NotImplementedError

    @abstractmethod
    async def subscribe_fill_notice(self, symbol: str = "") -> None:
        raise NotImplementedError

    @abstractmethod
    async def subscribe_market_status(self, symbol: str = "", venue: str = "KRX") -> None:
        raise NotImplementedError

    @abstractmethod
    async def recv_ws_message(self) -> dict[str, Any]:
        raise NotImplementedError

    @abstractmethod
    async def send_ws_pong(self, payload: str | bytes | None = None) -> None:
        raise NotImplementedError

    @abstractmethod
    async def close_ws(self) -> None:
        raise NotImplementedError


class KISHttpBrokerGateway(KISBrokerGateway):
    def __init__(
        self,
        settings: Settings | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.settings = settings or get_settings()
        self.client = client or httpx.AsyncClient(timeout=self.settings.kis_request_timeout_seconds)
        self._token_cache: dict[Environment, KISToken] = {}
        self._approval_cache: dict[Environment, KISApproval] = {}
        self._ws_session: websockets.WebSocketClientProtocol | None = None
        self._ws_crypto: dict[str, tuple[str, str]] = {}
        self._subscriptions: list[KISWebSocketSubscription] = []

    async def close(self) -> None:
        await self.close_ws()
        await self.client.aclose()

    async def close_ws(self) -> None:
        if self._ws_session is not None:
            await self._ws_session.close()
            self._ws_session = None
        self._ws_crypto = {}
        self._subscriptions = []

    async def issue_rest_token(self, env: Environment) -> KISToken:
        cached = self._token_cache.get(env)
        if cached and cached.expires_at_utc > datetime.now(UTC) + timedelta(minutes=5):
            return cached

        app_key, app_secret = _resolve_credentials(self.settings, env)
        response = await self.client.post(
            f"{_resolve_base_url(self.settings, env)}/oauth2/tokenP",
            headers={"content-type": "application/json"},
            json={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "appsecret": app_secret,
            },
        )
        payload = response.json()
        if response.status_code != 200 or "access_token" not in payload:
            raise KISAPIError("failed to issue KIS REST token", status_code=response.status_code, payload=payload)

        expires_at = datetime.now(UTC) + timedelta(seconds=int(payload.get("expires_in", 86_400)))
        token = KISToken(
            access_token=payload["access_token"],
            expires_at_utc=expires_at,
            token_type=payload.get("token_type", "Bearer"),
        )
        self._token_cache[env] = token
        return token

    async def issue_ws_approval(self, env: Environment) -> KISApproval:
        cached = self._approval_cache.get(env)
        if cached and cached.issued_at_utc > datetime.now(UTC) - timedelta(hours=12):
            return cached

        app_key, app_secret = _resolve_credentials(self.settings, env)
        response = await self.client.post(
            f"{_resolve_base_url(self.settings, env)}/oauth2/Approval",
            headers={"content-type": "application/json"},
            json={
                "grant_type": "client_credentials",
                "appkey": app_key,
                "secretkey": app_secret,
            },
        )
        payload = response.json()
        approval_key = payload.get("approval_key")
        if response.status_code != 200 or not approval_key:
            raise KISAPIError("failed to issue KIS websocket approval", status_code=response.status_code, payload=payload)

        approval = KISApproval(approval_key=approval_key, issued_at_utc=datetime.now(UTC))
        self._approval_cache[env] = approval
        return approval

    async def issue_hashkey(self, env: Environment, body: dict[str, Any]) -> KISHashKey:
        app_key, app_secret = _resolve_credentials(self.settings, env)
        response = await self.client.post(
            f"{_resolve_base_url(self.settings, env)}/uapi/hashkey",
            headers={
                "content-type": "application/json",
                "appkey": app_key,
                "appsecret": app_secret,
            },
            json=body,
        )
        payload = response.json()
        hashkey = payload.get("HASH")
        if response.status_code != 200 or not hashkey:
            raise KISAPIError("failed to issue KIS hashkey", status_code=response.status_code, payload=payload)
        return KISHashKey(hashkey=hashkey, issued_at_utc=datetime.now(UTC))

    async def _authorized_request(
        self,
        *,
        env: Environment,
        method: str,
        path: str,
        tr_id: str,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        require_hashkey: bool = False,
    ) -> dict[str, Any]:
        token = await self.issue_rest_token(env)
        app_key, app_secret = _resolve_credentials(self.settings, env)
        headers = {
            "authorization": f"{token.token_type} {token.access_token}",
            "appkey": app_key,
            "appsecret": app_secret,
            "tr_id": tr_id,
            "custtype": self.settings.kis_customer_type,
        }
        if json_body is not None:
            headers["content-type"] = "application/json"
        if require_hashkey and json_body is not None:
            headers["hashkey"] = (await self.issue_hashkey(env, json_body)).hashkey

        response = await self.client.request(
            method,
            f"{_resolve_base_url(self.settings, env)}{path}",
            headers=headers,
            params=params,
            json=json_body,
        )
        payload = response.json()
        if response.status_code != 200:
            raise KISAPIError("KIS request failed", status_code=response.status_code, payload=payload)
        return payload

    async def submit_cash_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.VPS)
        env = env if isinstance(env, Environment) else Environment(env)
        side = payload["ord_dv"]
        tr_id_map = {
            (Environment.PROD, "buy"): "TTTC0012U",
            (Environment.PROD, "sell"): "TTTC0011U",
            (Environment.VPS, "buy"): "VTTC0012U",
            (Environment.VPS, "sell"): "VTTC0011U",
        }
        tr_id = tr_id_map[(env, side)]
        body = {
            "CANO": payload["cano"],
            "ACNT_PRDT_CD": payload["acnt_prdt_cd"],
            "PDNO": payload["pdno"],
            "ORD_DVSN": payload["ord_dvsn"],
            "ORD_QTY": str(payload["ord_qty"]),
            "ORD_UNPR": str(payload["ord_unpr"]),
            "EXCG_ID_DVSN_CD": payload.get("excg_id_dvsn_cd", "KRX"),
            "SLL_TYPE": payload.get("sll_type", ""),
            "CNDT_PRIC": payload.get("cndt_pric", ""),
        }
        return await self._authorized_request(
            env=env,
            method="POST",
            path="/uapi/domestic-stock/v1/trading/order-cash",
            tr_id=tr_id,
            json_body=body,
            require_hashkey=True,
        )

    async def submit_cancel_replace(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.VPS)
        env = env if isinstance(env, Environment) else Environment(env)
        tr_id = "TTTC0013U" if env == Environment.PROD else "VTTC0013U"
        body = {
            "CANO": payload["cano"],
            "ACNT_PRDT_CD": payload["acnt_prdt_cd"],
            "KRX_FWDG_ORD_ORGNO": payload["krx_fwdg_ord_orgno"],
            "ORGN_ODNO": payload["orgn_odno"],
            "ORD_DVSN": payload["ord_dvsn"],
            "RVSE_CNCL_DVSN_CD": payload["rvse_cncl_dvsn_cd"],
            "ORD_QTY": str(payload["ord_qty"]),
            "ORD_UNPR": str(payload["ord_unpr"]),
            "QTY_ALL_ORD_YN": payload["qty_all_ord_yn"],
            "EXCG_ID_DVSN_CD": payload.get("excg_id_dvsn_cd", "KRX"),
        }
        if payload.get("cndt_pric"):
            body["CNDT_PRIC"] = str(payload["cndt_pric"])
        return await self._authorized_request(
            env=env,
            method="POST",
            path="/uapi/domestic-stock/v1/trading/order-rvsecncl",
            tr_id=tr_id,
            json_body=body,
            require_hashkey=True,
        )

    async def query_psbl_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.VPS)
        env = env if isinstance(env, Environment) else Environment(env)
        tr_id = "TTTC8908R" if env == Environment.PROD else "VTTC8908R"
        params = {
            "CANO": payload["cano"],
            "ACNT_PRDT_CD": payload["acnt_prdt_cd"],
            "PDNO": payload["pdno"],
            "ORD_UNPR": str(payload["ord_unpr"]),
            "ORD_DVSN": payload["ord_dvsn"],
            "CMA_EVLU_AMT_ICLD_YN": payload.get("cma_evlu_amt_icld_yn", "N"),
            "OVRS_ICLD_YN": payload.get("ovrs_icld_yn", "N"),
        }
        return await self._authorized_request(
            env=env,
            method="GET",
            path="/uapi/domestic-stock/v1/trading/inquire-psbl-order",
            tr_id=tr_id,
            params=params,
        )

    async def query_balance(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.VPS)
        env = env if isinstance(env, Environment) else Environment(env)
        tr_id = "TTTC8434R" if env == Environment.PROD else "VTTC8434R"
        params = {
            "CANO": payload["cano"],
            "ACNT_PRDT_CD": payload["acnt_prdt_cd"],
            "AFHR_FLPR_YN": payload.get("afhr_flpr_yn", "N"),
            "OFL_YN": payload.get("ofl_yn", ""),
            "INQR_DVSN": payload.get("inqr_dvsn", "02"),
            "UNPR_DVSN": payload.get("unpr_dvsn", "01"),
            "FUND_STTL_ICLD_YN": payload.get("fund_sttl_icld_yn", "N"),
            "FNCG_AMT_AUTO_RDPT_YN": payload.get("fncg_amt_auto_rdpt_yn", "N"),
            "PRCS_DVSN": payload.get("prcs_dvsn", "00"),
            "CTX_AREA_FK100": payload.get("ctx_area_fk100", ""),
            "CTX_AREA_NK100": payload.get("ctx_area_nk100", ""),
        }
        return await self._authorized_request(
            env=env,
            method="GET",
            path="/uapi/domestic-stock/v1/trading/inquire-balance",
            tr_id=tr_id,
            params=params,
        )

    async def query_daily_ccld(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.VPS)
        env = env if isinstance(env, Environment) else Environment(env)
        pd_dv = payload.get("pd_dv", "inner")
        tr_id_map = {
            (Environment.PROD, "before"): "CTSC9215R",
            (Environment.PROD, "inner"): "TTTC0081R",
            (Environment.VPS, "before"): "VTSC9215R",
            (Environment.VPS, "inner"): "VTTC0081R",
        }
        tr_id = tr_id_map[(env, pd_dv)]
        params = {
            "CANO": payload["cano"],
            "ACNT_PRDT_CD": payload["acnt_prdt_cd"],
            "INQR_STRT_DT": payload["inqr_strt_dt"],
            "INQR_END_DT": payload["inqr_end_dt"],
            "SLL_BUY_DVSN_CD": payload.get("sll_buy_dvsn_cd", "00"),
            "PDNO": payload.get("pdno", ""),
            "CCLD_DVSN": payload.get("ccld_dvsn", "00"),
            "INQR_DVSN": payload.get("inqr_dvsn", "00"),
            "INQR_DVSN_3": payload.get("inqr_dvsn_3", "00"),
            "ORD_GNO_BRNO": payload.get("ord_gno_brno", ""),
            "ODNO": payload.get("odno", ""),
            "INQR_DVSN_1": payload.get("inqr_dvsn_1", ""),
            "CTX_AREA_FK100": payload.get("ctx_area_fk100", ""),
            "CTX_AREA_NK100": payload.get("ctx_area_nk100", ""),
            "EXCG_ID_DVSN_CD": payload.get("excg_id_dvsn_cd", "KRX"),
        }
        return await self._authorized_request(
            env=env,
            method="GET",
            path="/uapi/domestic-stock/v1/trading/inquire-daily-ccld",
            tr_id=tr_id,
            params=params,
        )

    async def query_price(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        tr_id = "FHKST01010100"
        params = {
            "FID_COND_MRKT_DIV_CODE": payload.get("fid_cond_mrkt_div_code", "J"),
            "FID_INPUT_ISCD": payload["fid_input_iscd"],
        }
        return await self._authorized_request(
            env=env,
            method="GET",
            path="/uapi/domestic-stock/v1/quotations/inquire-price",
            tr_id=tr_id,
            params=params,
        )

    async def query_asking_price(self, payload: dict[str, Any]) -> dict[str, Any]:
        env = payload.get("env", Environment.PROD)
        env = env if isinstance(env, Environment) else Environment(env)
        tr_id = "FHKST01010200"
        params = {
            "FID_COND_MRKT_DIV_CODE": payload.get("fid_cond_mrkt_div_code", "J"),
            "FID_INPUT_ISCD": payload["fid_input_iscd"],
        }
        return await self._authorized_request(
            env=env,
            method="GET",
            path="/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn",
            tr_id=tr_id,
            params=params,
        )

    async def _ensure_ws_session(self, env: Environment) -> websockets.WebSocketClientProtocol:
        if self._ws_session is not None and not getattr(self._ws_session, "closed", False):
            return self._ws_session
        approval = await self.issue_ws_approval(env)
        self._ws_session = await websockets.connect(_resolve_ws_url(self.settings, env), ping_interval=30, ping_timeout=10)
        self._current_approval = approval
        return self._ws_session

    async def _ws_subscribe(self, env: Environment, tr_id: str, tr_key: str) -> None:
        ws = await self._ensure_ws_session(env)
        approval = await self.issue_ws_approval(env)
        message = {
            "header": {
                "approval_key": approval.approval_key,
                "custtype": self.settings.kis_customer_type,
                "tr_type": "1",
                "content-type": "utf-8",
            },
            "body": {"input": {"tr_id": tr_id, "tr_key": tr_key}},
        }
        self._subscriptions.append(KISWebSocketSubscription(tr_id=tr_id, tr_key=tr_key))
        await ws.send(json.dumps(message))

    async def recv_ws_message(self) -> dict[str, Any]:
        if self._ws_session is None:
            raise KISAPIError("websocket session is not connected")
        raw = await self._ws_session.recv()
        if isinstance(raw, bytes):
            raw = raw.decode("utf-8")
        if raw.startswith("{"):
            control = _parse_kis_ws_control_message(raw)
            if control["iv"] and control["key"]:
                self._ws_crypto[control["tr_id"]] = (control["key"], control["iv"])
            return {"type": "control", "payload": control}
        parts = raw.split("|", maxsplit=3)
        if len(parts) < 4:
            return {"type": "unknown", "payload": raw}
        encrypted, tr_id, count, payload = parts
        if encrypted == "1" and tr_id in self._ws_crypto:
            key, iv = self._ws_crypto[tr_id]
            payload = _aes_cbc_base64_dec(key, iv, payload)
        return {
            "type": "stream",
            "tr_id": tr_id,
            "count": int(count),
            "payload": payload,
        }

    async def send_ws_pong(self, payload: str | bytes | None = None) -> None:
        if self._ws_session is None:
            raise KISAPIError("websocket session is not connected")
        pong_payload = payload.encode("utf-8") if isinstance(payload, str) else payload
        await self._ws_session.pong(pong_payload)

    async def subscribe_quote(self, symbol: str, venue: str = "KRX", env: Environment | None = None) -> None:
        env = env or (Environment.VPS if self.settings.kis_enable_paper else Environment.PROD)
        tr_id = {"KRX": "H0STASP0", "NXT": "H0NXASP0", "TOTAL": "H0UNASP0"}.get(venue, "H0STASP0")
        await self._ws_subscribe(env, tr_id, symbol)

    async def subscribe_trade(self, symbol: str, venue: str = "KRX", env: Environment | None = None) -> None:
        env = env or (Environment.VPS if self.settings.kis_enable_paper else Environment.PROD)
        tr_id = {"KRX": "H0STCNT0", "NXT": "H0NXCNT0", "TOTAL": "H0UNCNT0"}.get(venue, "H0STCNT0")
        await self._ws_subscribe(env, tr_id, symbol)

    async def subscribe_fill_notice(self, symbol: str = "", env: Environment | None = None) -> None:
        env = env or (Environment.VPS if self.settings.kis_enable_paper else Environment.PROD)
        tr_id = "H0STCNI9" if env == Environment.VPS else "H0STCNI0"
        tr_key = self.settings.kis_hts_id if not symbol else symbol
        await self._ws_subscribe(env, tr_id, tr_key)

    async def subscribe_market_status(
        self,
        symbol: str = "",
        venue: str = "KRX",
        env: Environment | None = None,
    ) -> None:
        env = env or (Environment.VPS if self.settings.kis_enable_paper else Environment.PROD)
        tr_id = {"KRX": "H0STMKO0", "NXT": "H0NXMKO0", "TOTAL": "H0UNMKO0"}.get(venue, "H0STMKO0")
        tr_key = symbol or venue
        await self._ws_subscribe(env, tr_id, tr_key)


class StubKISBrokerGateway(KISBrokerGateway):
    async def issue_rest_token(self, env: Environment) -> KISToken:
        return KISToken(access_token=f"{_environment_name(env)}-token", expires_at_utc=datetime.now(UTC))

    async def issue_ws_approval(self, env: Environment) -> KISApproval:
        return KISApproval(approval_key=f"{_environment_name(env)}-approval", issued_at_utc=datetime.now(UTC))

    async def submit_cash_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"status": "accepted", "payload": payload}

    async def submit_cancel_replace(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"status": "accepted", "payload": payload}

    async def query_psbl_order(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"status": "ok", "payload": payload}

    async def query_balance(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"status": "ok", "payload": payload}

    async def query_daily_ccld(self, payload: dict[str, Any]) -> dict[str, Any]:
        return {"status": "ok", "payload": payload}

    async def subscribe_quote(self, symbol: str, venue: str = "KRX", env: Environment | None = None) -> None:
        return None

    async def subscribe_trade(self, symbol: str, venue: str = "KRX", env: Environment | None = None) -> None:
        return None

    async def subscribe_fill_notice(self, symbol: str = "", env: Environment | None = None) -> None:
        return None

    async def subscribe_market_status(
        self,
        symbol: str = "",
        venue: str = "KRX",
        env: Environment | None = None,
    ) -> None:
        return None

    async def recv_ws_message(self) -> dict[str, Any]:
        return {"type": "control", "payload": {"tr_id": "PINGPONG", "is_pingpong": True}}

    async def send_ws_pong(self, payload: str | bytes | None = None) -> None:
        return None

    async def close_ws(self) -> None:
        return None
