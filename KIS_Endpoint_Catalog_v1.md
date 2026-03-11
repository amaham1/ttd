# KIS Endpoint Catalog v1

기준일: 2026-03-11  
범위: 대한민국 주식 자동매매 시스템 구현에 직접 필요한 한국투자증권(KIS) Open API 핵심 엔드포인트 정리  
목적: 기존 설계 문서의 추상 레벨을 실제 구현 가능한 브로커 어댑터 명세 수준으로 내리는 것

---

## 1. 문서 성격

이 문서는 "모든 KIS API" 목록이 아니다. 아래 구현 범위에 필요한 핵심 경로만 우선 정리한다.

- 인증
- 웹소켓 접속키
- 현금 주문
- 정정/취소
- 매수가능조회
- 잔고조회
- 일별 주문체결조회
- 실시간 체결/호가/장운영정보

다음 항목은 일부 사실, 일부 구현 추론을 포함한다.

- `확인됨`: 공식 샘플 코드 또는 공식 문서에서 직접 확인
- `추론`: 샘플 구조상 높은 확률로 맞지만 실계좌 샘플로 재검증 필요

---

## 2. 소스

이 문서는 아래 공식 자료를 기준으로 작성했다.

- [KIS API 포털](https://apiportal.koreainvestment.com/)
- [KIS 공식 GitHub 저장소](https://github.com/koreainvestment/open-trading-api)
- [기존계좌 API 신청 문서](https://apiportal.koreainvestment.com/provider-doc2)
- 로컬 복제본:
  - [README.md](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/README.md)
  - [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py)
  - [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py)
  - [examples_user/domestic_stock/domestic_stock_functions_ws.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions_ws.py)
  - [kis_devlp.yaml](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/kis_devlp.yaml)

---

## 3. 환경 및 접속 정보

### 3.1 Base URL

| 환경 | REST base URL | WS base URL | 확인 상태 |
|---|---|---|---|
| 실전 | `https://openapi.koreainvestment.com:9443` | `ws://ops.koreainvestment.com:21000` | 확인됨 |
| 모의 | `https://openapivts.koreainvestment.com:29443` | `ws://ops.koreainvestment.com:31000` | 확인됨 |

근거:

- [kis_devlp.yaml](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/kis_devlp.yaml)

### 3.2 계정/환경 설정 키

필수 설정 항목:

- `my_app`, `my_sec`
- `paper_app`, `paper_sec`
- `my_htsid`
- `my_acct_stock`, `my_paper_stock`
- `my_prod`

주의:

- HTS ID는 체결통보와 조건검색 목록 확인 등 일부 기능에 사용된다.
- 실전과 모의는 키셋이 분리된다.
- 브로커 어댑터는 `broker_env=prod|vps`와 계좌상품코드(`01`, `03`, `08`, `22`, `29`)를 함께 관리해야 한다.

근거:

- [README.md](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/README.md)
- [kis_devlp.yaml](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/kis_devlp.yaml)

주의:

- 위 경로 중 README의 설정 가이드는 `~/KIS/config/kis_devlp.yaml` 사용을 전제한다.

---

## 4. 인증 및 세션 엔드포인트

### 4.1 REST 접근 토큰 발급

| 항목 | 값 |
|---|---|
| Method | `POST` |
| Path | `/oauth2/tokenP` |
| 실전/모의 | 둘 다 사용 |
| Body | `grant_type`, `appkey`, `appsecret` |
| 주요 응답 | `access_token`, `access_token_token_expired` |
| 내부 용도 | 주문/조회 REST 호출 공통 인증 |
| 확인 상태 | 확인됨 |

구현 메모:

- 샘플 코드는 토큰을 일별 파일에 캐시한다.
- 샘플 주석은 "유효기간 1일, 6시간 이내 재발급 시 기존 토큰 유지"라고 적고 있다.
- 동시에 포털 문서 예시에는 `access_token_token_expired` 필드가 노출되므로, 시스템은 고정 TTL이 아니라 응답 기반 `expires_at`으로 관리하는 편이 안전하다.

근거:

- [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py):192
- [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py):215

### 4.2 WS 접속키 발급

| 항목 | 값 |
|---|---|
| Method | `POST` |
| Path | `/oauth2/Approval` |
| 실전/모의 | 둘 다 사용 |
| Body | `grant_type`, `appkey`, `secretkey` |
| 주요 응답 | `approval_key` |
| 내부 용도 | 웹소켓 구독용 세션 초기화 |
| 확인 상태 | 확인됨 |

구현 메모:

- REST 토큰과 별도 수명주기로 관리해야 한다.
- 샘플 코드에서도 `auth()`와 `auth_ws()`가 분리되어 있다.

근거:

- [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py):475

### 4.3 Hashkey 발급

| 항목 | 값 |
|---|---|
| Method | `POST` |
| Path | `/uapi/hashkey` |
| 실전/모의 | REST 주문 계열에서 사용 가능 |
| Body | 원본 POST body |
| 주요 응답 | `HASH` |
| 내부 용도 | 주문 body 무결성 헤더 |
| 확인 상태 | 확인됨 |

구현 메모:

- 공식 샘플 코드는 "현재는 hash key 필수 사항 아님, 생략가능"이라고 적고 있다.
- 하지만 구현에서는 endpoint별 정책값으로 보유하는 것이 안전하다.

근거:

- [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py):269

---

## 5. REST 공통 규칙

### 5.1 공통 헤더

샘플 코드 기준 공통 헤더:

- `authorization: Bearer <token>`
- `appkey`
- `appsecret`
- `tr_id`
- `custtype: P`
- `tr_cont`

근거:

- [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py):418

### 5.2 성공/실패 판정

샘플 기준 1차 판정은 다음과 같다.

- HTTP `200`
- body의 `rt_cd == "0"`

실패 시 주요 필드:

- `msg_cd`
- `msg1`

근거:

- [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py):320

### 5.3 POST body 규칙

공식 샘플은 주문 POST body의 key를 대문자로 구성한다.

예:

- `CANO`
- `ACNT_PRDT_CD`
- `PDNO`
- `ORD_QTY`

근거:

- [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py):10120

### 5.4 실전/모의 TR ID 차이

샘플 공통 규칙:

- 실전 TR ID가 `T`, `J`, `C`로 시작하는 경우
- 모의투자 모드에서 내부 래퍼가 `V...`로 자동 치환하기도 한다

하지만 실제 함수들은 명시적으로 실전/모의 TR ID를 나누어 적고 있다. 따라서 설계상으로도 "명시적 TR ID 맵"을 따로 관리하는 것이 좋다.

근거:

- [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py):421

---

## 6. 핵심 주문/계좌 엔드포인트

## 6.1 현금 주문

| 항목 | 값 |
|---|---|
| 기능 | 현금 매수/매도 주문 |
| Method | `POST` |
| Path | `/uapi/domestic-stock/v1/trading/order-cash` |
| 실전 TR ID | 매도 `TTTC0011U`, 매수 `TTTC0012U` |
| 모의 TR ID | 매도 `VTTC0011U`, 매수 `VTTC0012U` |
| 필수 Body | `CANO`, `ACNT_PRDT_CD`, `PDNO`, `ORD_DVSN`, `ORD_QTY`, `ORD_UNPR`, `EXCG_ID_DVSN_CD` |
| 선택 Body | `SLL_TYPE`, `CNDT_PRIC` |
| 응답 핵심 필드 | `KRX_FWDG_ORD_ORGNO`, `ODNO`, `ORD_TMD` |
| 확인 상태 | 확인됨 |

구현 메모:

- `ORD_QTY`, `ORD_UNPR`는 문자열로 전달해야 한다.
- `EXCG_ID_DVSN_CD`는 샘플에서 `KRX`, `NXT`, `SOR` 사용 가능성이 드러난다.
- 이 REST 성공은 "브로커 접수 응답"이지, 최종 working/fill을 보장하지 않는다.

근거:

- [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py):10097
- [examples_llm/domestic_stock/order_cash/chk_order_cash.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_llm/domestic_stock/order_cash/chk_order_cash.py):18

## 6.2 정정/취소 주문

| 항목 | 값 |
|---|---|
| 기능 | 기존 주문 정정/취소 |
| Method | `POST` |
| Path | `/uapi/domestic-stock/v1/trading/order-rvsecncl` |
| 실전 TR ID | `TTTC0013U` |
| 모의 TR ID | `VTTC0013U` |
| 필수 Body | `CANO`, `ACNT_PRDT_CD`, `KRX_FWDG_ORD_ORGNO`, `ORGN_ODNO`, `ORD_DVSN`, `RVSE_CNCL_DVSN_CD`, `ORD_QTY`, `ORD_UNPR`, `QTY_ALL_ORD_YN`, `EXCG_ID_DVSN_CD` |
| 선택 Body | `CNDT_PRIC` |
| 응답 핵심 필드 | `krx_fwdg_ord_orgno`, `odno`, `ord_tmd` |
| 확인 상태 | 확인됨 |

구현 메모:

- 샘플은 정정/취소 전 `주식정정취소가능주문조회`를 먼저 호출하라고 안내한다.
- 응답 필드가 현금 주문 응답과 달리 소문자 예시로 보이므로, 어댑터에서 필드명 대소문자 정규화가 필요하다.

근거:

- [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py):10771
- [examples_llm/domestic_stock/order_rvsecncl/chk_order_rvsecncl.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_llm/domestic_stock/order_rvsecncl/chk_order_rvsecncl.py):16

## 6.3 정정취소 가능 주문 조회

| 항목 | 값 |
|---|---|
| 기능 | 정정/취소 가능 주문 확인 |
| Method | `GET` |
| Path | `/uapi/domestic-stock/v1/trading/inquire-psbl-rvsecncl` |
| 실전 TR ID | `TTTC0084R` |
| 모의 TR ID | 샘플에 별도 분기 없음 |
| 필수 Query | `CANO`, `ACNT_PRDT_CD`, `INQR_DVSN_1`, `INQR_DVSN_2` |
| 응답 핵심 | 가능 수량(`psbl_qty`) 포함 배열 |
| 확인 상태 | 확인됨 |

구현 메모:

- 주문 정정/취소 요청 직전의 사전 검증 API로 쓰는 것이 안전하다.

근거:

- [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py):6308

## 6.4 매수가능 조회

| 항목 | 값 |
|---|---|
| 기능 | 주문 가능 수량/금액 조회 |
| Method | `GET` |
| Path | `/uapi/domestic-stock/v1/trading/inquire-psbl-order` |
| 실전 TR ID | `TTTC8908R` |
| 모의 TR ID | `VTTC8908R` |
| 필수 Query | `CANO`, `ACNT_PRDT_CD`, `PDNO`, `ORD_UNPR`, `ORD_DVSN`, `CMA_EVLU_AMT_ICLD_YN`, `OVRS_ICLD_YN` |
| 응답 핵심 | `nrcvb_buy_amt`, `max_buy_amt`, `nrcvb_buy_qty`, `max_buy_qty` 류 |
| 확인 상태 | 부분 확인됨 |

구현 메모:

- 샘플은 가능 수량 확인 시 `ORD_DVSN:01(시장가)` 또는 실제 주문과 동일 주문구분으로 확인하라고 강조한다.
- 가능 금액과 가능 수량은 risk gate/portfolio sizing 이전에 반드시 질의하는 편이 안전하다.

근거:

- [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py):6206

## 6.5 잔고 조회

| 항목 | 값 |
|---|---|
| 기능 | 현재 잔고/평가/포지션 조회 |
| Method | `GET` |
| Path | `/uapi/domestic-stock/v1/trading/inquire-balance` |
| 실전 TR ID | `TTTC8434R` |
| 모의 TR ID | `VTTC8434R` |
| 필수 Query | `CANO`, `ACNT_PRDT_CD`, `AFHR_FLPR_YN`, `INQR_DVSN`, `UNPR_DVSN`, `FUND_STTL_ICLD_YN`, `FNCG_AMT_AUTO_RDPT_YN`, `PRCS_DVSN` |
| 연속조회 키 | `CTX_AREA_FK100`, `CTX_AREA_NK100`, header `tr_cont` |
| 응답 구조 | `output1`, `output2`, `ctx_area_fk100`, `ctx_area_nk100` |
| 확인 상태 | 확인됨 |

구현 메모:

- 실전은 1회 최대 50건, 모의는 최대 20건으로 샘플이 설명한다.
- `AFHR_FLPR_YN`는 `N`, `Y`, `X`를 사용하며 `X`는 NXT 조회 의미를 갖는다.
- 당일 전량매도 종목도 수량 0으로 남을 수 있으므로, 체결 정합성의 최종 진실로 단독 사용하면 안 된다.

근거:

- [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py):3679

## 6.6 일별 주문체결 조회

| 항목 | 값 |
|---|---|
| 기능 | 체결/미체결, 과거 주문 이력 조회 |
| Method | `GET` |
| Path | `/uapi/domestic-stock/v1/trading/inquire-daily-ccld` |
| 실전 TR ID | 3개월 이전 `CTSC9215R`, 3개월 이내 `TTTC0081R` |
| 모의 TR ID | 3개월 이전 `VTSC9215R`, 3개월 이내 `VTTC0081R` |
| 필수 Query | `CANO`, `ACNT_PRDT_CD`, `INQR_STRT_DT`, `INQR_END_DT`, `SLL_BUY_DVSN_CD`, `CCLD_DVSN`, `INQR_DVSN`, `INQR_DVSN_3` |
| 선택 Query | `PDNO`, `ODNO`, `ORD_GNO_BRNO`, `INQR_DVSN_1`, `EXCG_ID_DVSN_CD` |
| 연속조회 키 | `CTX_AREA_FK100`, `CTX_AREA_NK100`, header `tr_cont` |
| 응답 구조 | `output1`, `output2` |
| 확인 상태 | 확인됨 |

구현 메모:

- 실전 1회 최대 100건, 모의 1회 최대 15건.
- 샘플은 3개월 이전 조회는 장 종료 이후나 짧은 조회 구간으로 권장한다.
- 이 API는 REST 주문 응답 누락, WS 체결 누락, restart 후 복구에 매우 중요하다.

근거:

- [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py):4120

## 6.7 현재가 조회

| 항목 | 값 |
|---|---|
| 기능 | 현재가/기본 시세 조회 |
| Method | `GET` |
| Path | `/uapi/domestic-stock/v1/quotations/inquire-price` |
| TR ID | `FHKST01010100` |
| 필수 Query | `FID_COND_MRKT_DIV_CODE`, `FID_INPUT_ISCD` |
| 확인 상태 | 확인됨 |

구현 메모:

- 온라인 엔진의 핵심 입력은 보통 WS를 쓰되, 장전 초기화와 장애 시 보정용 REST 현재가가 유용하다.

근거:

- [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py):6097

## 6.8 예상체결 포함 호가 조회

| 항목 | 값 |
|---|---|
| 기능 | 호가 + 예상체결 정보 조회 |
| Method | `GET` |
| Path | `/uapi/domestic-stock/v1/quotations/inquire-asking-price-exp-ccn` |
| TR ID | `FHKST01010200` |
| 필수 Query | `FID_COND_MRKT_DIV_CODE`, `FID_INPUT_ISCD` |
| 응답 구조 | `output1` 호가, `output2` 예상체결 |
| 확인 상태 | 확인됨 |

근거:

- [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py):3614

## 6.9 영업일/장시간 조회

| 항목 | 값 |
|---|---|
| 기능 | 시장 시간/영업일 참고 |
| Method | `GET` |
| Path | `/uapi/domestic-stock/v1/quotations/market-time` |
| TR ID | `HHMCM000002C0` |
| 확인 상태 | 확인됨 |

주의:

- 샘플 주석에는 "국내선물 영업일조회"로 적혀 있어 이름과 기능 설명이 혼재한다.
- 운영 시 authoritative session source로 쓰려면 실제 응답 샘플을 별도 수집해 검증해야 한다.

근거:

- [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py):9623

---

## 7. 웹소켓 엔드포인트 및 TR ID

## 7.1 공통 WS 요청 구조

샘플 기준 WS 구독 메시지 구조:

- header
  - `approval_key`
  - `tr_type`
  - `custtype`
- body.input
  - `tr_id`
  - `tr_key`

근거:

- [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py):513

## 7.2 실시간 호가

| 기능 | TR ID | 비고 |
|---|---|---|
| KRX 호가 | `H0STASP0` | 확인됨 |
| NXT 호가 | `H0NXASP0` | 확인됨 |
| 통합 호가 | `H0UNASP0` | 확인됨 |

근거:

- [examples_user/domestic_stock/domestic_stock_functions_ws.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions_ws.py):15

## 7.3 실시간 체결가

| 기능 | TR ID | 비고 |
|---|---|---|
| KRX 체결가 | `H0STCNT0` | 확인됨 |
| NXT 체결가 | `H0NXCNT0` | 확인됨 |
| 통합 체결가 | `H0UNCNT0` | 확인됨 |

근거:

- [examples_user/domestic_stock/domestic_stock_functions_ws.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions_ws.py):323

## 7.4 실시간 체결통보

| 기능 | 실전 TR ID | 모의 TR ID | 비고 |
|---|---|---|---|
| 주문/정정/취소/거부 접수 + 체결통보 | `H0STCNI0` | `H0STCNI9` | 확인됨 |

핵심 필드:

- `ODER_NO`
- `OODER_NO`
- `CNTG_QTY`
- `CNTG_UNPR`
- `STCK_CNTG_HOUR`
- `RFUS_YN`
- `CNTG_YN`
- `ACPT_YN`
- `ORD_EXG_GB`
- `ODER_PRC`

중요 규칙:

- `CNTG_YN == 2`이면 체결통보
- `CNTG_YN == 1`이면 주문/정정/취소/거부 접수 통보
- 메시지는 AES256 key/iv로 복호화해야 한다

근거:

- [examples_user/domestic_stock/domestic_stock_functions_ws.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions_ws.py):389

## 7.5 실시간 장운영정보

| 기능 | TR ID | 비고 |
|---|---|---|
| KRX 장운영정보 | `H0STMKO0` | 확인됨 |
| NXT 장운영정보 | `H0NXMKO0` | 확인됨 |
| 통합 장운영정보 | `H0UNMKO0` | 확인됨 |

핵심 필드:

- `TRHT_YN`
- `TR_SUSP_REAS_CNTT`
- `MKOP_CLS_CODE`
- `ANTC_MKOP_CLS_CODE`
- `ISCD_STAT_CLS_CODE`
- `VI_CLS_CODE`
- `OVTM_VI_CLS_CODE`
- `EXCH_CLS_CODE`

구현 메모:

- 샘플은 KRX 장운영정보가 VI 발동/해제 시 수신된다고 설명한다.
- 따라서 장운영정보는 단순 시계열이 아니라 거래정지/VI와 결합된 리스크 이벤트 소스로 써야 한다.

근거:

- [examples_user/domestic_stock/domestic_stock_functions_ws.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions_ws.py):1099
- [examples_llm/domestic_stock/market_status_krx/market_status_krx.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_llm/domestic_stock/market_status_krx/market_status_krx.py):17

## 7.6 시간외 시세

| 기능 | TR ID | 비고 |
|---|---|---|
| 시간외 KRX 호가 | `H0STOAA0` | 확인됨 |
| 시간외 KRX 체결가 | `H0STOUP0` | 확인됨 |
| 시간외 KRX 예상체결 | `H0STOAC0` | 확인됨 |

근거:

- [examples_user/domestic_stock/domestic_stock_functions_ws.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions_ws.py):1659

---

## 8. 구현용 Canonical 매핑 포인트

## 8.1 Order Submit Command

KIS 매핑 필수 필드:

- `CANO` -> `account_id`
- `ACNT_PRDT_CD` -> `account_product_code`
- `PDNO` -> `instrument_id`
- `ORD_DVSN` -> `order_type/tif style`
- `ORD_QTY` -> `qty`
- `ORD_UNPR` -> `price`
- `EXCG_ID_DVSN_CD` -> `venue_hint`

## 8.2 REST 주문 응답

KIS 매핑 필수 필드:

- `KRX_FWDG_ORD_ORGNO` 또는 `krx_fwdg_ord_orgno` -> `broker_org_no`
- `ODNO` 또는 `odno` -> `broker_order_no`
- `ORD_TMD` 또는 `ord_tmd` -> `broker_accept_ts`

정책:

- 필드 대소문자 차이는 어댑터에서 정규화한다.

## 8.3 WS 체결통보

KIS 매핑 필수 필드:

- `ODER_NO` -> `broker_order_no`
- `OODER_NO` -> `parent_or_original_order_no`
- `STCK_SHRN_ISCD` -> `instrument_id`
- `CNTG_QTY` -> `fill_qty` 또는 `notice_qty`
- `CNTG_UNPR` -> `fill_price` 또는 `notice_price`
- `STCK_CNTG_HOUR` -> `event_ts_local`
- `RFUS_YN` -> `rejected_flag`
- `CNTG_YN` -> `is_fill`
- `ACPT_YN` -> `accepted_flag`
- `ORD_EXG_GB` -> `venue`
- `ODER_PRC` -> `order_price`

## 8.4 Position / Cash / Order Recovery

권장 우선순위:

1. WS 체결통보
2. 일별 주문체결 조회
3. 잔고 조회

이유:

- WS는 가장 빠르다.
- 일별 주문체결 조회는 복구용으로 가장 직접적이다.
- 잔고 조회는 보유수량 0 잔고가 D+2까지 남을 수 있어 원장의 직접 대체재로는 약하다.

---

## 9. 구현 시 반드시 외부화할 값

하드코딩 금지 항목:

- `broker_env -> base_url/ws_url`
- `TR ID map`
- `approval/token refresh lead time`
- `hashkey required endpoints`
- `REST rate limit profile`
- `WS reconnect profile`
- `EXCG_ID_DVSN_CD` allowed values
- `market session policy`

---

## 10. 아직 남은 검증 항목

아래는 샘플 구조로는 강하게 시사되지만, 실계좌 또는 포털 응답 샘플로 최종 확인이 필요하다.

1. `order-cash`와 `order-rvsecncl` 응답 필드 대소문자 일관성
2. `ORD_DVSN`의 전체 enum 표
3. `EXCG_ID_DVSN_CD=KRX|NXT|SOR|ALL`의 실제 주문 지원 범위
4. 모의투자에서 `NXT`, `SOR`가 동일하게 시뮬레이션되는지 여부
5. `market-time` 응답이 장운영 authoritative source로 충분한지 여부
6. hashkey가 실제 운영에서 필수인 엔드포인트 목록
7. 체결통보 구독에 HTS ID가 반드시 필요한 기능 경계

---

## 11. 다음 문서와의 연결

이 문서는 아래 문서와 함께 읽어야 한다.

- [KIS_AI_Trading_Doc_Supplement_2026-03-11.md](/C:/Users/MMM/Documents/New%20project/KIS_AI_Trading_Doc_Supplement_2026-03-11.md)
- [KIS_Status_and_Error_Map_v1.md](/C:/Users/MMM/Documents/New%20project/KIS_Status_and_Error_Map_v1.md)
