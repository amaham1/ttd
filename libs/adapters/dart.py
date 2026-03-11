from __future__ import annotations

import io
import zipfile
from dataclasses import dataclass
from datetime import date
from xml.etree import ElementTree

import httpx

from libs.config.settings import Settings, get_settings


class OpenDARTError(RuntimeError):
    pass


@dataclass(slots=True)
class DartCorpCode:
    corp_code: str
    corp_name: str
    stock_code: str | None
    modify_date: str | None


class OpenDARTClient:
    def __init__(self, settings: Settings | None = None, client: httpx.AsyncClient | None = None) -> None:
        self.settings = settings or get_settings()
        self.client = client or httpx.AsyncClient(timeout=20.0)

    async def close(self) -> None:
        await self.client.aclose()

    async def download_corp_codes(self) -> list[DartCorpCode]:
        response = await self.client.get(
            f"{self.settings.opendart_base_url}/corpCode.xml",
            params={"crtfc_key": self.settings.opendart_api_key},
        )
        if response.status_code != 200:
            raise OpenDARTError("failed to download corp codes")
        with zipfile.ZipFile(io.BytesIO(response.content)) as zipped:
            xml_name = zipped.namelist()[0]
            xml_bytes = zipped.read(xml_name)
        root = ElementTree.fromstring(xml_bytes)
        result: list[DartCorpCode] = []
        for entry in root.findall(".//list"):
            stock_code = (entry.findtext("stock_code") or "").strip()
            result.append(
                DartCorpCode(
                    corp_code=(entry.findtext("corp_code") or "").strip(),
                    corp_name=(entry.findtext("corp_name") or "").strip(),
                    stock_code=stock_code or None,
                    modify_date=(entry.findtext("modify_date") or "").strip() or None,
                )
            )
        return result

    async def list_disclosures(self, begin: date, end: date, corp_code: str | None = None) -> dict:
        params = {
            "crtfc_key": self.settings.opendart_api_key,
            "bgn_de": begin.strftime("%Y%m%d"),
            "end_de": end.strftime("%Y%m%d"),
            "page_no": 1,
            "page_count": 100,
        }
        if corp_code:
            params["corp_code"] = corp_code
        response = await self.client.get(f"{self.settings.opendart_base_url}/list.json", params=params)
        payload = response.json()
        if response.status_code != 200 or payload.get("status") not in {"000", "013"}:
            raise OpenDARTError(f"OpenDART list failed: {payload}")
        return payload

    async def fetch_document(self, rcept_no: str) -> bytes:
        response = await self.client.get(
            f"{self.settings.opendart_base_url}/document.xml",
            params={"crtfc_key": self.settings.opendart_api_key, "rcept_no": rcept_no},
        )
        if response.status_code != 200:
            raise OpenDARTError("failed to fetch OpenDART document")
        return response.content
