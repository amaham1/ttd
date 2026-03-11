from __future__ import annotations

import asyncio
from datetime import datetime

from libs.adapters.dart import OpenDARTClient
from libs.adapters.kis import KISHttpBrokerGateway
from libs.config.settings import get_settings
from libs.domain.enums import Environment


def _mask(value: str, keep: int = 6) -> str:
    if len(value) <= keep:
        return "*" * len(value)
    return value[:keep] + "..."


async def main() -> None:
    settings = get_settings()
    kis = KISHttpBrokerGateway(settings=settings)
    dart = OpenDARTClient(settings=settings)
    try:
        print("[1] KIS REST token")
        token = await kis.issue_rest_token(Environment.PROD)
        print(f"    token: {_mask(token.access_token)} expires={token.expires_at_utc.isoformat()}")

        print("[2] KIS WS approval")
        approval = await kis.issue_ws_approval(Environment.PROD)
        print(f"    approval: {_mask(approval.approval_key)} issued={approval.issued_at_utc.isoformat()}")

        print("[3] KIS balance query")
        balance = await kis.query_balance(
            {
                "env": Environment.PROD,
                "cano": settings.kis_account_no,
                "acnt_prdt_cd": settings.kis_account_product_code,
                "afhr_flpr_yn": "N",
                "inqr_dvsn": "02",
                "unpr_dvsn": "01",
                "fund_sttl_icld_yn": "N",
                "fncg_amt_auto_rdpt_yn": "N",
                "prcs_dvsn": "00",
                "ctx_area_fk100": "",
                "ctx_area_nk100": "",
            }
        )
        print(f"    rt_cd={balance.get('rt_cd')} msg_cd={balance.get('msg_cd')} msg1={balance.get('msg1')}")
        output2 = balance.get("output2") or []
        if output2:
            summary = output2[0] if isinstance(output2, list) else output2
            print(
                "    summary:",
                {
                    "dnca_tot_amt": summary.get("dnca_tot_amt"),
                    "tot_evlu_amt": summary.get("tot_evlu_amt"),
                    "evlu_pfls_smtl_amt": summary.get("evlu_pfls_smtl_amt"),
                },
            )

        print("[4] OpenDART corp codes")
        corp_codes = await dart.download_corp_codes()
        print(f"    count={len(corp_codes)} sample={corp_codes[0].corp_code}:{corp_codes[0].corp_name}")

        print("[5] OpenDART daily list")
        today = datetime.now().date()
        disclosures = await dart.list_disclosures(today, today)
        print(
            "    status=",
            disclosures.get("status"),
            "total_count=",
            disclosures.get("total_count"),
        )
    finally:
        await kis.close()
        await dart.close()


if __name__ == "__main__":
    asyncio.run(main())
