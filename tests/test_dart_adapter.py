from __future__ import annotations

import io
import zipfile

import httpx
import pytest

from libs.adapters.dart import OpenDARTClient
from libs.config.settings import Settings


def _corp_code_zip() -> bytes:
    xml = """<?xml version="1.0" encoding="UTF-8"?>
    <result>
      <list>
        <corp_code>00126380</corp_code>
        <corp_name>Samsung Electronics</corp_name>
        <stock_code>005930</stock_code>
        <modify_date>20250101</modify_date>
      </list>
    </result>
    """.encode("utf-8")
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zipped:
        zipped.writestr("CORPCODE.xml", xml)
    return buffer.getvalue()


@pytest.mark.asyncio
async def test_download_corp_codes_parses_zip_payload() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=_corp_code_zip())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    settings = Settings(opendart_api_key="dart-key")
    dart = OpenDARTClient(settings=settings, client=client)

    corp_codes = await dart.download_corp_codes()

    assert corp_codes[0].corp_code == "00126380"
    assert corp_codes[0].stock_code == "005930"
    await dart.close()
