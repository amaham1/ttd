# KIS AI Trading 문서 보강 메모 (2026-03-11)

이 문서는 아래 기존 설계 문서를 공식 자료로 보강하기 위한 addendum이다.

- `KIS_AI_Trading_Master_Spec_v2.md`
- `KIS_AI_Trading_11_Detailed_Event_Contracts_v2.md`
- `KIS_AI_Trading_12_Detailed_Operations_Runbook_v2.md`
- `KIS_AI_Trading_13_API_Field_Mapping_Guide_v2.md`
- `KIS_AI_Trading_14_Config_and_Policy_Catalog_v2.md`

목적은 두 가지다.

1. 문서의 큰 설계 방향은 유지하되, 실제 한국투자증권 Open API, OpenDART, 복수시장 제약을 더 정확히 반영한다.
2. 구현 직전에 반드시 확정해야 할 브로커별 세부 항목을 명시한다.

---

## 1. 총평

기존 문서의 큰 방향은 타당하다.

- AI와 주문 경로 분리
- raw 보존
- append-only 원장 + current projection 분리
- 리스크 게이트 중앙화
- ACK/fill race, stale feed, reconciliation break를 정상 운영 시나리오로 간주
- 리플레이, 섀도우 라이브, 운영 런북을 필수 산출물로 둠

이 방향은 실제 실거래 시스템 설계로서 맞다.

다만 실제 KIS API 구현 기준으로는 아직 다음이 부족하다.

- KIS 엔드포인트별 확정 매핑표
- KIS 상태 코드/오류 코드의 canonical 매핑표
- REST 인증과 WS 접속키의 별도 수명주기 관리 명세
- 실전/모의 환경 차이 명세
- 복수시장/NXT를 시스템이 어디까지 직접 제어할 수 있는지에 대한 capability 확인

---

## 2. KIS 공식 자료로 보강된 핵심 사실

### 2.1 REST 인증과 WS 인증은 분리해서 봐야 한다

한국투자증권 공식 샘플 저장소는 REST 인증용 `auth()`와 웹소켓 접속용 `auth_ws()`를 분리해서 사용한다.

의미:

- 주문/조회용 access token 관리와
- 실시간 시세/체결통보용 websocket approval/session 관리를

하나의 세션 객체로 뭉개면 안 된다.

문서 반영 포인트:

- `BrokerSessionStarted`를 단일 이벤트로 두되, payload에 `channel=REST|WS`를 강제해야 한다.
- `BrokerSessionDegraded`도 REST 장애와 WS stale를 분리해서 기록해야 한다.
- 리스크 게이트는 `access token 정상`과 `WS 체결/시세 freshness 정상`을 별개 판정으로 다뤄야 한다.

### 2.2 실전/모의투자 App Key는 분리된다

공식 샘플 저장소는 실전 투자용과 모의 투자용 App Key/App Secret을 분리해 안내한다.

의미:

- shadow/paper/live 환경 분리 원칙은 문서상 선언만으로 끝나면 안 된다.
- 설정 레지스트리에서 `broker_env=prod|paper|dev`와 키 세트를 명시적으로 분리해야 한다.

문서 반영 포인트:

- 정책 카탈로그에 `broker environment policy`를 추가한다.
- 운영 런북 장전 체크리스트에 "현재 키셋과 대상 계좌 환경 일치 여부"를 넣는다.

### 2.3 HTS ID 의존성이 있다

공식 샘플 저장소는 체결통보와 조건검색 목록 조회 등 일부 기능에서 `HTS ID` 사용을 언급한다.

의미:

- KIS 어댑터 설정에는 단순 `app_key/app_secret/account` 외에 `hts_id`도 별도 관리 대상일 가능성이 높다.
- 체결통보/조건검색/실시간 구독 기능을 붙일 때 `hts_id` 누락을 단순 설정 오류로 분류하면 안 되고, 세션 준비 조건으로 봐야 한다.

문서 반영 포인트:

- `BrokerSessionStarted` payload에 `approval_scope`만 둘 것이 아니라 `credential_profile_id` 또는 `hts_id_present` 같은 운영 진단용 필드를 추가한다.
- 장전 체크리스트에 `HTS ID configured`를 넣는다.

### 2.4 KIS 운영 공지는 자주 바뀐다

KIS API 포털 공지에는 2026-02-24 웹소켓 무한루프 호출 자동 차단 공지와 2026-02-25 API 호출 유량 안내가 노출된다.

의미:

- rate budget과 reconnect backoff는 문서상의 좋은 습관이 아니라 필수 운영 제약이다.
- aggressive reconnect나 무한 재시도는 계정 차단 리스크가 있다.

문서 반영 포인트:

- 정책 카탈로그의 `브로커 세션 정책`에 아래 필드를 추가한다.
  - `rest_rate_limit_profile`
  - `ws_subscribe_rate_limit_profile`
  - `max_reconnect_attempts_per_window`
  - `forced_cooldown_after_block_sec`
- 런북에 "자동 차단 의심" 시나리오를 별도 추가한다.

### 2.5 Hashkey 사용 여부를 주문 경로 명세에 명확히 넣어야 한다

KIS 포털 카테고리/샘플 자료에는 Hashkey 발급 및 사용 흐름이 존재한다.

의미:

- 주문 제출 어댑터는 단순 REST POST 래퍼가 아니라,
  - 토큰 확보
  - 필요 시 hashkey 발급
  - TR ID 선택
  - 실전/모의 도메인 선택
  - 요청 헤더 구성
  - 응답 해석
의 순서를 강제하는 capability로 설계해야 한다.

문서 반영 포인트:

- `OrderSubmitRequested -> Broker Connector` 구간에 `transport_precheck` 단계를 추가한다.
- 어댑터 contract test에 "hashkey 누락", "TR ID 불일치", "실전/모의 도메인 혼동" 케이스를 포함한다.

### 2.6 토큰 유효기간은 구현 직전 재확인이 필요하다

기존 문서 10장에는 access token 24시간 가정이 들어가 있지만, 2026-03-11 기준 KIS 포털의 기존계좌 API 신청 문서에는 `access_token_token_expired`가 약 7776000초(90일)로 보이는 응답 예시가 있다. 반면 공식 샘플/운영 가이드에서는 토큰 재발급 제한과 refresh 흐름도 함께 언급된다.

의미:

- 계좌 유형, 적용 API 묶음, 문서 버전에 따라 토큰 정책이 다르거나 표현이 다를 수 있다.
- 따라서 "토큰은 24시간"을 시스템 불변 전제로 박아 넣으면 위험하다.

문서 반영 포인트:

- 토큰 만료 정책은 코드 상수로 박지 말고 `Broker Auth Policy`로 외부화한다.
- 실제 구현 전 반드시 사용 계정 유형 기준 공식 문서/응답으로 재확인한다.
- 운영 모니터링은 고정 TTL 가정보다 `actual expires_at` 기반이어야 한다.

---

## 3. OpenDART 공식 자료로 보강된 핵심 사실

### 3.1 corpCode 파일은 별도 기준정보 파이프라인으로 취급해야 한다

OpenDART 공식 corpCode 가이드는 corp code와 stock code를 XML ZIP 형태로 제공한다.

의미:

- corpCode는 단순 정적 CSV가 아니라 버전 있는 기준정보다.
- 종목 마스터와 DART corp 매핑은 장전 체크의 일부가 아니라 별도 동기화 파이프라인이 필요하다.

문서 반영 포인트:

- `Symbol Master` 하위에 `corp_code_snapshot_version`을 둔다.
- `ref_instrument`에 `dart_corp_code_effective_from/effective_to` 또는 snapshot 참조값을 두는 것이 좋다.

### 3.2 OpenDART 호출 제한은 parser backlog와 직접 연결된다

OpenDART 공식 오류 가이드는 일반적으로 요청 한도 초과 시 `020` 오류를 안내하고, 일부 API는 조회 가능한 회사 수 제한도 존재한다.

의미:

- 장중 공시 burst 상황에서 무조건 원문 재조회하는 구조는 위험하다.
- `DisclosureDetected`와 `FetchDisclosureDocument` 사이에 우선순위 큐가 필요하다는 기존 문서 방향이 맞다.

문서 반영 포인트:

- 정책 카탈로그의 `Parsing Policy`에 `document_fetch_priority_rule`, `opendart_rate_budget`, `max_refetch_attempts`를 추가한다.
- 런북 6.6 시나리오에 `OpenDART rate-limit hit`을 명시적으로 추가한다.

### 3.3 공시시각, 수집시각, 거래반영가능시각은 분리해야 한다

기존 문서도 이 방향을 잡고 있지만, OpenDART 공식 자료 특성상 실제 공시 접수시각과 시스템 수집시각, 파서 완료시각이 다를 수 있다.

문서 반영 포인트:

- `DisclosureDetected`와 `DisclosureParsed`에 공통적으로 아래 시각을 분리 저장한다.
  - `disclosure_ts_utc`
  - `detected_ts_utc`
  - `document_fetched_ts_utc`
  - `parsed_ts_utc`
  - `available_ts_utc`

---

## 4. 복수시장/NXT 관련 보강

### 4.1 복수시장 체계는 이미 운영 현실이다

금융위원회 공식 자료 기준, 2025-03-04부터 대체거래소(ATS, NXT)가 출범했고 복수시장 환경과 최선집행 체계가 강조된다.

의미:

- 메인 스펙이 `KRX/NXT`, `venue-aware execution`, `Market/Venue State 중앙화`를 넣은 것은 방향상 맞다.

### 4.2 하지만 시스템이 직접 venue를 완전히 제어할 수 있는지는 별도 확인이 필요하다

최선집행과 SOR는 기본적으로 증권사 수준의 집행 체계와 연결된다. 따라서 내부 시스템이 "어느 시장에 낼지"를 직접 완전 제어할 수 있는지, 아니면 "허용 세션/주문 공격성/가격 제한"만 제어할 수 있는지는 KIS API capability 확인이 필요하다.

의미:

- 현재 문서의 `Execution Router`는 너무 이른 단계에서 "직접 venue routing"을 전제할 위험이 있다.

권장 수정:

- `Execution Router`를 두 단계로 분리해서 문서화한다.
  - `Execution Policy Selector`: 공격성, limit/marketable limit, cancel-repost, slice 여부 결정
  - `Venue Selector`: 브로커 API가 실제 지원하는 범위 내에서만 활성화

실행 규칙:

- KIS API가 venue hint만 지원하면 hint 기반으로 축소 구현
- venue 직접지정 미지원이면 `VenueState`는 리스크/세션 판정에만 사용
- 향후 capability 확인 후 확장

### 4.3 세션 시간표는 코드 상수보다 외부 정책으로 관리해야 한다

복수시장 체계에서는 KRX와 NXT의 pre/main/after 구간, 허용 주문 유형, 공격성 정책이 달라질 수 있다.

문서 반영 포인트:

- `Venue Policy`를 정적 enum이 아니라 데이터 테이블로 관리한다.
- `VenueStateUpdated`는 거래소 공식 시간표를 직접 계산하지 말고, authoritative session service에서만 발행한다는 기존 원칙을 유지한다.

---

## 5. 기존 문서별 구체 보강안

## 5.1 메인 스펙 보강안

추가할 섹션:

- `Broker Capability Matrix`
  - 주문 가능 기능
  - 정정/취소 지원 형태
  - 실시간 체결통보 지원 여부
  - venue 제어 가능 범위
  - 모의투자와 실전 차이

- `Authentication & Session Model`
  - REST token
  - WS approval/session
  - HTS ID
  - hashkey
  - 실전/모의 키셋

- `Broker Constraint Registry`
  - rate limit
  - reconnect policy
  - 운영 공지 기반 강제 변경 사항

수정이 필요한 기존 가정:

- access token 유효기간을 고정 24시간으로 단정하지 말 것
- Execution Router에서 venue 직접 라우팅을 기본 가정으로 두지 말 것

## 5.2 이벤트 계약서 보강안

추가 이벤트 후보:

- `BrokerAuthTokenIssued`
- `BrokerAuthTokenRefreshFailed`
- `BrokerWsApprovalIssued`
- `BrokerRateLimitApproaching`
- `BrokerRateLimitBlocked`
- `DisclosureFetchDeferred`

기존 이벤트 payload 추가 권장:

- `BrokerSessionStarted`
  - `environment`
  - `credential_profile_id`
  - `hts_id_present`

- `OrderSubmitRequested`
  - `broker_env`
  - `tr_id_hint`
  - `hashkey_required`

- `OrderAcked`
  - `ack_source` (`REST_RESPONSE` or `WS_NOTICE` or `QUERY_RECOVERY`)

- `FillReceived`
  - `fill_source`
  - `cum_fill_qty_if_present`
  - `fill_delta_qty_if_present`

## 5.3 운영 런북 보강안

추가 시나리오:

- KIS 호출 유량 초과 또는 자동 차단 의심
- 실전/모의 환경 키 혼용
- HTS ID 누락 또는 체결통보 구독 실패
- WS approval 재발급 실패
- 공시 원문 fetch backlog와 DART rate-limit 동시 발생

장전 체크리스트 추가:

- 현재 환경(`prod|paper|dev`) 확인
- App Key/App Secret 세트 확인
- HTS ID 확인
- websocket approval/session 생성 확인
- KIS 포털 최근 운영 공지 확인

## 5.4 API 필드 매핑 가이드 보강안

13번 문서는 현재 "원칙" 문서다. 실제 구현에는 다음 산출물이 추가되어야 한다.

- `KIS_ORDER_SUBMIT_REST_MAP`
- `KIS_ORDER_ACK_REST_MAP`
- `KIS_ORDER_ACK_WS_MAP`
- `KIS_FILL_WS_MAP`
- `KIS_OPEN_ORDER_QUERY_MAP`
- `KIS_POSITION_QUERY_MAP`
- `KIS_AVAILABLE_CASH_QUERY_MAP`
- `KIS_TICK_WS_MAP`
- `KIS_QUOTE_WS_MAP`
- `KIS_SESSION_STATE_MAP`
- `KIS_ERROR_CODE_MAP`
- `KIS_BROKER_STATUS_MAP`

각 표에는 반드시 아래가 들어가야 한다.

- external endpoint/topic
- method / TR ID / channel
- request header fields
- request body fields
- response body fields
- success / reject / retryable 판정 기준
- raw-to-canonical 변환 규칙
- 멱등 키 후보
- projection 복구용 우선순위

## 5.5 정책 카탈로그 보강안

새 정책 범주 추가 권장:

- `Broker Auth Policy`
  - token_refresh_lead_sec
  - ws_approval_refresh_lead_sec
  - token_issue_cooldown_sec
  - credential_profile_id

- `Broker Transport Policy`
  - tr_id_map_version
  - hashkey_required_endpoints
  - reconnect_backoff_profile
  - rate_limit_profile
  - auto_block_recovery_policy

- `Environment Isolation Policy`
  - prod_keys_allowed_hosts
  - paper_keys_allowed_hosts
  - replay_env_broker_access_disabled

---

## 6. 구현 우선순위 보정

기존 스프린트 순서는 대체로 맞다. 다만 KIS 실연동 기준으로는 아래 순서를 더 권장한다.

1. `Auth/Session/Broker Config Registry`
2. `Raw Event Log + Broker Connector Skeleton`
3. `OrderSubmitRequested -> OrderAcked/Rejected -> FillReceived`
4. `Open order recovery + query reconciliation`
5. `Position/Cash projection`
6. `Venue State + Risk Gate`
7. `DART/KIND/KRX adapters`
8. `Disclosure parser + Feature + Signal`
9. `Execution optimization`

이유:

- 브로커 세션과 주문 원장이 먼저 안정화되지 않으면 AI/시그널이 앞서도 실거래 품질이 나오지 않는다.
- KIS 쪽 운영 제약은 전략보다 어댑터와 OMS에서 먼저 흡수해야 한다.

---

## 7. 구현 직전 반드시 확정할 질문

아래는 문서 보강 후에도 실제 구현 전에 확인해야 하는 질문이다.

1. 현재 사용할 KIS 계정 유형에서 access token 실제 만료 정책은 무엇인가.
2. 실시간 체결통보 구독에 필요한 값은 정확히 무엇인가.
3. HTS ID가 필요한 기능 범위는 어디까지인가.
4. 주문 제출 시 hashkey가 필요한 endpoint 범위는 어디까지인가.
5. 실전과 모의에서 주문 응답 포맷, 에러 코드, 체결통보 포맷이 완전히 같은가.
6. KIS API가 venue 직접지정, venue hint, 또는 통합집행 중 어느 모델을 제공하는가.
7. 주문 조회/잔고 조회가 projection 복구의 authoritative source가 될 수 있는가, 아니면 보조 대조 수단인가.
8. 체결통보에서 수수료/세금이 즉시 제공되는가, 아니면 후행 조회가 필요한가.
9. 장전/장후/복수시장 구간별 허용 주문 타입이 API 차원에서 어떻게 제한되는가.
10. KIS 운영 공지 변경을 자동 감시해야 하는가.

---

## 8. 권장 추가 산출물

실제 개발을 시작하기 전 아래 문서를 추가로 만드는 것을 권장한다.

1. `KIS_Broker_Adapter_Contract_v1.md`
2. `KIS_Endpoint_Catalog_v1.md`
3. `KIS_Status_and_Error_Map_v1.md`
4. `KIS_Order_Lifecycle_Reconciliation_v1.md`
5. `KIS_Runtime_Config_Profile_v1.md`

---

## 9. 참고한 공식 자료

아래 자료는 2026-03-11 기준 확인했다.

- 한국투자증권 API 포털 메인: https://apiportal.koreainvestment.com/
- 한국투자증권 공식 샘플 저장소: https://github.com/koreainvestment/open-trading-api
- KIS 포털 기존계좌 API 신청 문서: https://apiportal.koreainvestment.com/provider-doc2
- KIS 포털 카테고리/공지 검색:
  - https://apiportal.koreainvestment.com/apiservice-category
  - https://apiportal.koreainvestment.com/about-openapi
  - https://apiportal.koreainvestment.com/community-qna
- OpenDART corpCode 가이드: https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS001&apiId=2019018
- OpenDART 오류코드 가이드: https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DE001&apiId=AE00004
- OpenDART 공시검색(list) 가이드: https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS002&apiId=2019001
- 금융위원회 복수시장/NXT 관련 보도자료:
  - https://www.fsc.go.kr/no010101/83953
  - https://www.fsc.go.kr/no010101/83005

주의:

- KIS 포털 공지와 샘플 저장소는 수시로 바뀔 수 있다.
- 인증 정책, 호출 유량, websocket 제한, HTS ID 요구사항은 구현 직전 반드시 다시 확인해야 한다.
