# KIS Status and Error Map v1

기준일: 2026-03-11  
목적: 한국투자증권 Open API의 원시 응답/통보를 내부 canonical 상태와 오류 체계로 매핑하기 위한 기준 문서

---

## 1. 문서 성격

이 문서는 두 층으로 구성된다.

1. `확인된 사실`
2. `구현용 추론`

이유:

- KIS 공식 샘플은 호출 구조와 주요 필드는 잘 보여주지만
- 모든 상태 코드 enum과 실제 업무 의미를 완전히 표 형태로 주지는 않는다.

따라서 이 문서는 "당장 구현 가능한 최소 안전 규칙"을 먼저 고정하고, 실거래 샘플 수집 후 세부 enum을 좁혀가도록 설계한다.

---

## 2. 소스

- [KIS API 포털](https://apiportal.koreainvestment.com/)
- [KIS 공식 GitHub 저장소](https://github.com/koreainvestment/open-trading-api)
- 로컬 복제본:
  - [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py)
  - [examples_user/domestic_stock/domestic_stock_functions.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions.py)
  - [examples_user/domestic_stock/domestic_stock_functions_ws.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/domestic_stock/domestic_stock_functions_ws.py)
  - [examples_llm/domestic_stock/order_cash/chk_order_cash.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_llm/domestic_stock/order_cash/chk_order_cash.py)
  - [examples_llm/domestic_stock/order_rvsecncl/chk_order_rvsecncl.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_llm/domestic_stock/order_rvsecncl/chk_order_rvsecncl.py)

---

## 3. 공통 원시 응답 구조

## 3.1 REST 응답 공통

샘플 코드 기준 공통 REST 응답 판정 필드:

- `rt_cd`
- `msg_cd`
- `msg1`
- `output` 또는 `output1`, `output2`

기본 판정:

- HTTP `200` 이면서 `rt_cd == "0"` -> 성공
- 그 외 -> 실패

근거:

- [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py):320

## 3.2 WS 시스템 응답 공통

샘플 코드 기준 WS 시스템 응답 구조:

- `header.tr_id`
- `header.tr_key`
- `header.encrypt`
- `body.rt_cd`
- `body.msg1`
- `body.output.iv`
- `body.output.key`

주요 의미:

- `rt_cd == "0"` -> 구독/제어 메시지 성공
- `PINGPONG` -> heartbeat 성격의 제어 메시지
- `output.iv`, `output.key` -> AES256 복호화용 재료

근거:

- [examples_user/kis_auth.py](/C:/Users/MMM/Documents/New%20project/kis-open-trading-api/examples_user/kis_auth.py):538

---

## 4. Canonical Error Category

KIS raw 에러는 그대로 저장하되, 내부에서는 아래 범주로 먼저 묶는다.

| Canonical Error Category | 판정 기준 | 예시 원시 신호 |
|---|---|---|
| `HTTP_TRANSPORT_ERROR` | HTTP status != 200 | 네트워크/게이트웨이/서버 에러 |
| `BROKER_BUSINESS_ERROR` | HTTP 200 + `rt_cd != "0"` | `msg_cd`, `msg1` 포함 비즈니스 오류 |
| `WS_CONTROL_ERROR` | WS 시스템 응답에서 `rt_cd != "0"` | 구독 실패, 권한 오류 |
| `WS_STREAM_GAP` | heartbeat 누락, 메시지 공백, 재접속 루프 | WS stale, disconnect |
| `FIELD_MAPPING_ERROR` | 응답은 성공이나 필수 필드 누락/타입 불일치 | output 파싱 실패 |
| `UNKNOWN_BROKER_STATUS` | 상태코드 또는 플래그 의미 미정의 | 새 enum 등장 |

정책:

- 내부 로직은 raw `msg_cd`, `msg1`를 직접 if-else로 쓰지 않는다.
- 먼저 canonical category로 매핑하고, raw는 audit에 남긴다.

---

## 5. 세션/인증 상태 맵

## 5.1 REST 인증 상태

| Raw Source | Canonical State | 근거 |
|---|---|---|
| `/oauth2/tokenP` 성공 | `REST_AUTH_READY` | 확인됨 |
| 토큰 만료 시각 임박 | `REST_AUTH_EXPIRING` | 추론 |
| REST 401 또는 auth 실패 | `REST_AUTH_FAILED` | 확인됨 |
| 재발급 반복 실패 | `REST_AUTH_DEGRADED` | 추론 |

구현 규칙:

- `REST_AUTH_FAILED` 시 신규 주문 진입 금지
- 조회 API만 살아 있어도 `EXIT_ONLY` 또는 `ENTRY_FROZEN` 판단 가능

## 5.2 WS 인증 상태

| Raw Source | Canonical State | 근거 |
|---|---|---|
| `/oauth2/Approval` 성공 | `WS_AUTH_READY` | 확인됨 |
| approval 재발급 필요 | `WS_AUTH_EXPIRING` | 추론 |
| approval 발급 실패 | `WS_AUTH_FAILED` | 확인됨 |
| approval은 정상이나 heartbeat 누락 | `WS_STREAM_DEGRADED` | 확인됨 |

---

## 6. 시장/세션 상태 맵

KIS는 장운영정보 WS에서 아래 필드를 노출한다.

- `TRHT_YN`
- `TR_SUSP_REAS_CNTT`
- `MKOP_CLS_CODE`
- `ANTC_MKOP_CLS_CODE`
- `ISCD_STAT_CLS_CODE`
- `VI_CLS_CODE`
- `OVTM_VI_CLS_CODE`
- `EXCH_CLS_CODE`

문제:

- 샘플 코드에는 각 코드의 완전한 enum 사전이 없다.

따라서 v1에서는 아래 수준으로만 고정한다.

| Raw Field | Canonical 의미 | 상태 |
|---|---|---|
| `TRHT_YN` true/active | `TRADING_HALTED` | 확인됨 |
| `TR_SUSP_REAS_CNTT` 비어있지 않음 | `HALT_REASON_PRESENT` | 확인됨 |
| `VI_CLS_CODE` 비정상 값 | `VI_ACTIVE_OR_SPECIAL` | 추론 |
| `EXCH_CLS_CODE` | `venue=KRX|NXT|UNIFIED` | 추론 |
| `MKOP_CLS_CODE` | `session_code` raw 유지 | 확인됨 |
| `ANTC_MKOP_CLS_CODE` | `anticipated_session_code` raw 유지 | 확인됨 |

정책:

- `MKOP_CLS_CODE`는 지금 단계에서 raw 코드 그대로 저장
- 별도 `ref_kis_market_status_map` 테이블을 만들어 실데이터 수집 후 enum을 확정

---

## 7. 주문 상태 맵

## 7.1 내부 Canonical 주문 상태

내부 OMS는 아래 상태를 사용한다.

- `CREATED`
- `SUBMITTING`
- `REST_ACCEPTED`
- `ACKED`
- `WORKING`
- `PARTIALLY_FILLED`
- `FILLED`
- `PENDING_CANCEL`
- `CANCELED`
- `PENDING_REPLACE`
- `REPLACED`
- `REJECTED`
- `RECON_HOLD`
- `BROKEN`

`REST_ACCEPTED`를 별도로 두는 이유:

- KIS는 REST 주문 응답과 WS 접수/체결통보가 분리된다.
- REST 성공 직후를 곧바로 `WORKING`으로 보면 race condition에서 꼬인다.

## 7.2 주문 제출 REST 응답 매핑

### 성공 케이스

| Raw 조건 | Canonical Event | Canonical State | 근거 |
|---|---|---|---|
| HTTP 200 + `rt_cd == "0"` + `ODNO` 존재 | `OrderRestAccepted` | `REST_ACCEPTED` | 확인됨 |
| HTTP 200 + `rt_cd == "0"` + org/order/time 존재 | `BrokerOrderReferenceIssued` | `REST_ACCEPTED` | 확인됨 |

응답에서 추출할 값:

- `KRX_FWDG_ORD_ORGNO` or `krx_fwdg_ord_orgno`
- `ODNO` or `odno`
- `ORD_TMD` or `ord_tmd`

주의:

- REST 성공은 최종 체결/주문유지 상태를 의미하지 않는다.
- 이후 WS `체결통보` 또는 조회 복구가 도착해야 `ACKED/WORKING/FILLED` 판단이 안정적이다.

### 실패 케이스

| Raw 조건 | Canonical Event | Canonical State |
|---|---|---|
| HTTP != 200 | `OrderSubmitTransportFailed` | `REJECTED` or `RECON_HOLD` |
| HTTP 200 + `rt_cd != "0"` | `OrderSubmitBrokerRejected` | `REJECTED` |

판정 규칙:

- 서버가 주문을 받지 못했다고 확신 가능하면 `REJECTED`
- 주문 접수 여부가 불명확하면 `RECON_HOLD`

이 구분은 ACK 지연 런북과 연결해야 한다.

## 7.3 WS 체결통보 매핑

KIS 체결통보 WS 핵심 필드:

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

샘플이 직접 밝히는 사실:

- `CNTG_YN == 2` -> 체결통보
- `CNTG_YN == 1` -> 주문/정정/취소/거부 접수 통보

이를 기준으로 아래처럼 매핑한다.

| Raw 조건 | Canonical Event | Canonical State | 근거 |
|---|---|---|---|
| `CNTG_YN == 2` | `FillReceived` | `PARTIALLY_FILLED` or `FILLED` | 확인됨 |
| `CNTG_YN == 1` + `RFUS_YN`이 거부 의미 | `OrderRejected` | `REJECTED` | 추론 |
| `CNTG_YN == 1` + `ACPT_YN`이 접수 의미 | `OrderAcked` | `ACKED` | 추론 |
| `CNTG_YN == 1` + 정정/취소 관련 표시 | `OrderCanceled` or `OrderReplaced` | terminal or live | 추론 |

여기서 `RFUS_YN`, `ACPT_YN`, `RCTF_CLS`, `ODER_KIND`의 값 사전은 실계좌 샘플 수집 후 확정해야 한다.

## 7.4 Fill 상태 판정

KIS 체결통보에는 누적/증분 의미가 명시적으로 완전히 설명되지 않는다. 따라서 v1 규칙은 아래처럼 둔다.

1. WS 체결통보에서 broker trade unique key가 확인되면 그것을 1순위 멱등키로 사용
2. broker trade id가 없다면 `ODER_NO + STCK_CNTG_HOUR + CNTG_QTY + CNTG_UNPR` 조합을 임시 멱등키로 사용
3. 일별 주문체결조회와 대조하여 최종 보정

주의:

- 위 2번은 임시 규칙이다.
- 실계좌 샘플 확보 후 더 강한 키가 나오면 교체해야 한다.

## 7.5 정정/취소 상태 판정

정정/취소는 REST 응답 성공만으로 terminal 처리하지 않는다.

규칙:

1. `order-rvsecncl` REST 성공 -> `PENDING_CANCEL` 또는 `PENDING_REPLACE`
2. 이후 WS 접수/거부/체결 추가도 정상 시나리오로 허용
3. 취소 요청 후 fill 추가 도착 가능
4. 일별 주문체결조회로 최종 확정

---

## 8. 포지션/현금 상태 맵

## 8.1 잔고 조회(`inquire-balance`)

샘플이 직접 밝히는 사실:

- 실전 1회 최대 50건
- 모의 1회 최대 20건
- 당일 전량매도 잔고도 보유수량 0으로 남을 수 있으며 D+2 이후 사라질 수 있음

따라서 매핑 원칙은 아래와 같다.

| Raw Source | Canonical Role |
|---|---|
| WS 체결통보 | intraday primary ledger trigger |
| `inquire-daily-ccld` | order/fill 복구 primary |
| `inquire-balance` | current projection 대조용 |

정책:

- `inquire-balance`만으로 체결원장 재구성 금지
- `0주 잔고`는 즉시 포지션 종료 확정 신호로 사용 금지

## 8.2 매수가능조회(`inquire-psbl-order`)

이 API는 현금 current state보다 "주문 가능"을 직접 알려준다.

권장 매핑:

- `available_cash_proxy`
- `max_buy_qty_proxy`
- `risk_precheck_snapshot`

정책:

- 주문 직전 risk gate에서 재질의 가능
- portfolio engine 계산보다 브로커 가능금액이 우선

---

## 9. 에러 처리 우선순위

실제 운영 시 아래 우선순위로 판정한다.

1. HTTP 전송 실패
2. REST `rt_cd != "0"`
3. WS 시스템응답 실패
4. 필드 매핑 실패
5. 상태 해석 불가

이유:

- 1~3은 브로커 측 명시 신호
- 4~5는 내부 구현 문제일 가능성이 높다

---

## 10. Canonical Error Severity

| Canonical Error | Severity | 기본 운영 행동 |
|---|---|---|
| `HTTP_TRANSPORT_ERROR` | `HIGH` | 재시도 + 신규진입 제한 검토 |
| `BROKER_BUSINESS_ERROR` | `MEDIUM/HIGH` | 주문 거절, raw 보존 |
| `WS_CONTROL_ERROR` | `HIGH` | 구독 재시도, stale 감시 강화 |
| `WS_STREAM_GAP` | `CRITICAL` | 신규진입 차단 |
| `FIELD_MAPPING_ERROR` | `HIGH` | DLQ + 어댑터 경보 |
| `UNKNOWN_BROKER_STATUS` | `HIGH` | 안전측 차단 |

---

## 11. 구현 테이블 권장안

실제 코드에는 아래 기준 테이블을 두는 것이 좋다.

### 11.1 `ref_kis_rest_error_map`

권장 컬럼:

- `msg_cd`
- `msg_text_pattern`
- `http_status`
- `canonical_error_category`
- `retryable`
- `freeze_recommended`
- `notes`

### 11.2 `ref_kis_ws_status_map`

권장 컬럼:

- `tr_id`
- `field_name`
- `raw_value`
- `semantic_meaning`
- `canonical_event`
- `canonical_state`
- `confidence`

### 11.3 `ref_kis_order_response_field_map`

권장 컬럼:

- `api_name`
- `raw_field_name`
- `normalized_field_name`
- `canonical_field_name`
- `required`
- `type_rule`

---

## 12. v1에서 확정하지 않은 것

다음 항목은 아직 raw 값 사전이 부족해서 "테이블 설계만 고정"하고 값은 유보한다.

1. `MKOP_CLS_CODE` enum 전체
2. `ANTC_MKOP_CLS_CODE` enum 전체
3. `ISCD_STAT_CLS_CODE` enum 전체
4. `VI_CLS_CODE` enum 전체
5. `RFUS_YN` raw 값 전체
6. `ACPT_YN` raw 값 전체
7. `RCTF_CLS`, `ODER_KIND`, `ODER_COND` 전체 업무 의미
8. 주문구분 `ORD_DVSN` 전체 enum

---

## 13. 당장 코드에 반영할 최소 규칙

아래는 지금 바로 구현해도 되는 최소 규칙이다.

1. REST는 `HTTP 200 + rt_cd == "0"`만 성공으로 본다.
2. REST 주문 성공은 `REST_ACCEPTED`까지만 올리고, 최종 상태는 WS 또는 조회 복구로 확정한다.
3. WS `CNTG_YN == 2`는 fill로 본다.
4. WS `CNTG_YN == 1`은 접수/거부/정정/취소 계열 notice로 본다.
5. `inquire-daily-ccld`는 restart/recovery의 핵심 대조 API로 사용한다.
6. `inquire-balance`는 projection 대조용이지 fill ledger 재구성의 단독 근거로 쓰지 않는다.
7. 알 수 없는 코드가 나오면 안전측으로 `UNKNOWN_BROKER_STATUS` 처리 후 신규 진입을 제한한다.

---

## 14. 연결 문서

- [KIS_Endpoint_Catalog_v1.md](/C:/Users/MMM/Documents/New%20project/KIS_Endpoint_Catalog_v1.md)
- [KIS_AI_Trading_Doc_Supplement_2026-03-11.md](/C:/Users/MMM/Documents/New%20project/KIS_AI_Trading_Doc_Supplement_2026-03-11.md)
