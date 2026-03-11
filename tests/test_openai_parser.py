from __future__ import annotations

import pytest

from libs.adapters.openai_parser import OpenAIParserClient
from libs.config.settings import Settings


@pytest.mark.asyncio
async def test_openai_parser_fallback_blocks_dilution_event() -> None:
    parser = OpenAIParserClient(settings=Settings(openai_api_key=""))

    result = await parser.parse_disclosure("회사는 제3자배정 유상증자를 결정했다.")

    assert result.used_fallback is True
    assert result.structured["event_type"] == "EQUITY_DILUTION"
    assert result.structured["tradeability"] == "BLOCK"
    await parser.close()
