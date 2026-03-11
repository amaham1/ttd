from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
    )

    app_name: str = Field(default="KIS AI Trading", alias="APP_NAME")
    app_env: str = Field(default="local", alias="APP_ENV")
    app_timezone: str = Field(default="Asia/Seoul", alias="APP_TIMEZONE")
    app_log_level: str = Field(default="INFO", alias="APP_LOG_LEVEL")

    ops_api_host: str = Field(default="0.0.0.0", alias="OPS_API_HOST")
    ops_api_port: int = Field(default=8000, alias="OPS_API_PORT")
    ops_api_url: str = Field(default="http://localhost:8000", alias="OPS_API_URL")
    ops_state_path: str = Field(default="storage/ops/ops_state.json", alias="OPS_STATE_PATH")
    ops_state_audit_limit: int = Field(default=200, alias="OPS_STATE_AUDIT_LIMIT")
    broker_gateway_port: int = Field(default=8001, alias="BROKER_GATEWAY_PORT")
    broker_gateway_url: str = Field(default="http://localhost:8001", alias="BROKER_GATEWAY_URL")
    trading_core_port: int = Field(default=8002, alias="TRADING_CORE_PORT")
    trading_core_url: str = Field(default="http://localhost:8002", alias="TRADING_CORE_URL")
    market_intel_port: int = Field(default=8003, alias="MARKET_INTEL_PORT")
    market_intel_url: str = Field(default="http://localhost:8003", alias="MARKET_INTEL_URL")
    replay_runner_port: int = Field(default=8004, alias="REPLAY_RUNNER_PORT")
    shadow_live_port: int = Field(default=8005, alias="SHADOW_LIVE_PORT")
    shadow_live_url: str = Field(default="http://localhost:8005", alias="SHADOW_LIVE_URL")
    data_ingest_port: int = Field(default=8006, alias="DATA_INGEST_PORT")
    feature_pipeline_port: int = Field(default=8007, alias="FEATURE_PIPELINE_PORT")
    selector_engine_port: int = Field(default=8008, alias="SELECTOR_ENGINE_PORT")
    selector_engine_url: str = Field(default="http://localhost:8008", alias="SELECTOR_ENGINE_URL")
    portfolio_engine_port: int = Field(default=8009, alias="PORTFOLIO_ENGINE_PORT")
    portfolio_engine_url: str = Field(default="http://localhost:8009", alias="PORTFOLIO_ENGINE_URL")

    mariadb_host: str = Field(default="localhost", alias="MARIADB_HOST")
    mariadb_port: int = Field(default=3306, alias="MARIADB_PORT")
    mariadb_db: str = Field(default="kis_ai_trading", alias="MARIADB_DB")
    mariadb_user: str = Field(default="kis", alias="MARIADB_USER")
    mariadb_password: str = Field(default="kis_password", alias="MARIADB_PASSWORD")

    redis_url: str = Field(default="redis://localhost:6379/0", alias="REDIS_URL")
    nats_url: str = Field(default="nats://localhost:4222", alias="NATS_URL")
    nats_stream_name: str = Field(default="KIS_EVENTS", alias="NATS_STREAM_NAME")
    nats_stream_subjects: str = Field(default="evt.>", alias="NATS_STREAM_SUBJECTS")

    minio_endpoint: str = Field(default="localhost:9000", alias="MINIO_ENDPOINT")
    minio_access_key: str = Field(default="minioadmin", alias="MINIO_ACCESS_KEY")
    minio_secret_key: str = Field(default="minioadmin", alias="MINIO_SECRET_KEY")
    minio_secure: bool = Field(default=False, alias="MINIO_SECURE")
    minio_bucket_raw: str = Field(default="raw-events", alias="MINIO_BUCKET_RAW")
    minio_bucket_evidence: str = Field(default="evidence-bundles", alias="MINIO_BUCKET_EVIDENCE")
    minio_bucket_replay: str = Field(default="replay-packages", alias="MINIO_BUCKET_REPLAY")
    minio_bucket_research: str = Field(default="research-parquet", alias="MINIO_BUCKET_RESEARCH")

    clickhouse_host: str = Field(default="localhost", alias="CLICKHOUSE_HOST")
    clickhouse_port: int = Field(default=8123, alias="CLICKHOUSE_PORT")
    clickhouse_database: str = Field(default="kis_ai_trading", alias="CLICKHOUSE_DATABASE")
    clickhouse_user: str = Field(default="default", alias="CLICKHOUSE_USER")
    clickhouse_password: str = Field(default="", alias="CLICKHOUSE_PASSWORD")

    kis_base_url: str = Field(default="", alias="KIS_BASE_URL")
    kis_ws_url: str = Field(default="", alias="KIS_WS_URL")
    kis_vts_base_url: str = Field(default="", alias="KIS_VTS_BASE_URL")
    kis_vts_ws_url: str = Field(default="", alias="KIS_VTS_WS_URL")
    kis_app_key: str = Field(default="", alias="KIS_APP_KEY")
    kis_app_secret: str = Field(default="", alias="KIS_APP_SECRET")
    kis_paper_app_key: str = Field(default="", alias="KIS_PAPER_APP_KEY")
    kis_paper_app_secret: str = Field(default="", alias="KIS_PAPER_APP_SECRET")
    kis_customer_type: str = Field(default="P", alias="KIS_CUSTOMER_TYPE")
    kis_hts_id: str = Field(default="", alias="KIS_HTS_ID")
    kis_account_no: str = Field(default="", alias="KIS_ACCOUNT_NO")
    kis_account_product_code: str = Field(default="01", alias="KIS_ACCOUNT_PRODUCT_CODE")
    kis_request_timeout_seconds: float = Field(default=10.0, alias="KIS_REQUEST_TIMEOUT_SECONDS")
    kis_ws_heartbeat_timeout_seconds: float = Field(
        default=20.0,
        alias="KIS_WS_HEARTBEAT_TIMEOUT_SECONDS",
    )
    kis_ws_reconnect_delay_seconds: float = Field(
        default=3.0,
        alias="KIS_WS_RECONNECT_DELAY_SECONDS",
    )
    kis_ws_stale_after_seconds: float = Field(
        default=15.0,
        alias="KIS_WS_STALE_AFTER_SECONDS",
    )
    kis_enable_paper: bool = Field(default=False, alias="KIS_ENABLE_PAPER")
    kis_live_trading_enabled: bool = Field(default=False, alias="KIS_LIVE_TRADING_ENABLED")
    kis_live_require_arm: bool = Field(default=True, alias="KIS_LIVE_REQUIRE_ARM")
    kis_live_max_order_value_krw: int = Field(default=0, alias="KIS_LIVE_MAX_ORDER_VALUE_KRW")
    kis_live_allowed_symbols: str = Field(default="", alias="KIS_LIVE_ALLOWED_SYMBOLS")
    kis_live_daily_loss_limit_pct: float = Field(default=5.0, alias="KIS_LIVE_DAILY_LOSS_LIMIT_PCT")
    kis_live_min_total_equity_krw: int = Field(default=5000000, alias="KIS_LIVE_MIN_TOTAL_EQUITY_KRW")
    kis_live_common_stock_only: bool = Field(default=True, alias="KIS_LIVE_COMMON_STOCK_ONLY")
    trading_live_entry_session_start_local_time: str = Field(
        default="09:05",
        alias="TRADING_LIVE_ENTRY_SESSION_START_LOCAL_TIME",
    )
    trading_live_entry_session_end_local_time: str = Field(
        default="15:15",
        alias="TRADING_LIVE_ENTRY_SESSION_END_LOCAL_TIME",
    )

    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4.1-mini", alias="OPENAI_MODEL")
    opendart_api_key: str = Field(default="", alias="OPENDART_API_KEY")
    opendart_base_url: str = Field(default="https://opendart.fss.or.kr/api", alias="OPENDART_BASE_URL")
    news_provider_order: str = Field(default="NAVER,RSS,GDELT", alias="NEWS_PROVIDER_ORDER")
    google_news_rss_enabled: bool = Field(default=True, alias="GOOGLE_NEWS_RSS_ENABLED")
    google_news_rss_base_url: str = Field(
        default="https://news.google.com/rss/search",
        alias="GOOGLE_NEWS_RSS_BASE_URL",
    )
    google_news_rss_hl: str = Field(default="ko", alias="GOOGLE_NEWS_RSS_HL")
    google_news_rss_gl: str = Field(default="KR", alias="GOOGLE_NEWS_RSS_GL")
    google_news_rss_ceid: str = Field(default="KR:ko", alias="GOOGLE_NEWS_RSS_CEID")
    google_news_rss_include_source_query: bool = Field(
        default=True,
        alias="GOOGLE_NEWS_RSS_INCLUDE_SOURCE_QUERY",
    )
    google_news_rss_source_sites: str = Field(
        default="reuters.com,apnews.com,bbc.com,cnbc.com,marketwatch.com",
        alias="GOOGLE_NEWS_RSS_SOURCE_SITES",
    )
    news_rss_feed_urls: str = Field(default="", alias="NEWS_RSS_FEED_URLS")
    naver_search_base_url: str = Field(
        default="https://openapi.naver.com/v1/search",
        alias="NAVER_SEARCH_BASE_URL",
    )
    naver_search_client_id: str = Field(default="", alias="NAVER_SEARCH_CLIENT_ID")
    naver_search_client_secret: str = Field(
        default="",
        alias="NAVER_SEARCH_CLIENT_SECRET",
    )
    gdelt_doc_api_url: str = Field(default="https://api.gdeltproject.org/api/v2/doc/doc", alias="GDELT_DOC_API_URL")
    gdelt_min_interval_seconds: float = Field(default=5.0, alias="GDELT_MIN_INTERVAL_SECONDS")
    fred_api_base_url: str = Field(default="https://api.stlouisfed.org/fred", alias="FRED_API_BASE_URL")
    fred_api_key: str = Field(default="", alias="FRED_API_KEY")
    news_provider_stop_after_min_results: bool = Field(
        default=True,
        alias="NEWS_PROVIDER_STOP_AFTER_MIN_RESULTS",
    )
    news_min_results_to_stop: int = Field(default=3, alias="NEWS_MIN_RESULTS_TO_STOP")
    news_research_query_limit_per_symbol: int = Field(
        default=3,
        alias="NEWS_RESEARCH_QUERY_LIMIT_PER_SYMBOL",
    )

    selector_top_n: int = Field(default=20, alias="SELECTOR_TOP_N")
    portfolio_target_size: int = Field(default=8, alias="PORTFOLIO_TARGET_SIZE")
    selector_watchlist_size: int = Field(default=50, alias="SELECTOR_WATCHLIST_SIZE")
    selector_confidence_floor: float = Field(default=0.55, alias="SELECTOR_CONFIDENCE_FLOOR")
    selector_event_freshness_minutes: int = Field(default=720, alias="SELECTOR_EVENT_FRESHNESS_MINUTES")
    selector_post_entry_cooldown_minutes: int = Field(default=240, alias="SELECTOR_POST_ENTRY_COOLDOWN_MINUTES")
    selector_max_single_name_weight: float = Field(default=0.15, alias="SELECTOR_MAX_SINGLE_NAME_WEIGHT")
    selector_max_sector_weight: float = Field(default=0.30, alias="SELECTOR_MAX_SECTOR_WEIGHT")
    selector_max_new_positions_per_day: int = Field(default=8, alias="SELECTOR_MAX_NEW_POSITIONS_PER_DAY")
    selector_participation_rate_cap: float = Field(default=0.05, alias="SELECTOR_PARTICIPATION_RATE_CAP")
    selector_default_account_scope: str = Field(default="default", alias="SELECTOR_DEFAULT_ACCOUNT_SCOPE")
    selector_default_strategy_id: str = Field(default="event-swing-topn", alias="SELECTOR_DEFAULT_STRATEGY_ID")
    selector_default_holding_days: int = Field(default=5, alias="SELECTOR_DEFAULT_HOLDING_DAYS")
    selector_live_realtime_recalc_minutes: int = Field(default=1, alias="SELECTOR_LIVE_REALTIME_RECALC_MINUTES")
    selector_fail_closed_on_vendor_error: bool = Field(default=True, alias="SELECTOR_FAIL_CLOSED_ON_VENDOR_ERROR")
    selector_promotion_min_training_rows: int = Field(default=24, alias="SELECTOR_PROMOTION_MIN_TRAINING_ROWS")
    selector_promotion_min_mean_payoff_score: float = Field(
        default=10.0,
        alias="SELECTOR_PROMOTION_MIN_MEAN_PAYOFF_SCORE",
    )
    selector_promotion_min_hit_rate: float = Field(default=0.52, alias="SELECTOR_PROMOTION_MIN_HIT_RATE")
    selector_promotion_min_net_sharpe_proxy: float = Field(
        default=0.08,
        alias="SELECTOR_PROMOTION_MIN_NET_SHARPE_PROXY",
    )
    selector_promotion_max_drawdown_bps: float = Field(
        default=-900.0,
        alias="SELECTOR_PROMOTION_MAX_DRAWDOWN_BPS",
    )
    selector_promotion_required_shadow_runs: int = Field(
        default=20,
        alias="SELECTOR_PROMOTION_REQUIRED_SHADOW_RUNS",
    )
    selector_promotion_required_payoff_lift: float = Field(
        default=0.0,
        alias="SELECTOR_PROMOTION_REQUIRED_PAYOFF_LIFT",
    )
    selector_promotion_required_sharpe_lift: float = Field(
        default=0.0,
        alias="SELECTOR_PROMOTION_REQUIRED_SHARPE_LIFT",
    )
    selector_runtime_stage: str = Field(default="PAPER", alias="SELECTOR_RUNTIME_STAGE")
    selector_live_target_stage: str = Field(
        default="TINY_CAPITAL",
        alias="SELECTOR_LIVE_TARGET_STAGE",
    )
    selector_live_require_promotion_approval: bool = Field(
        default=False,
        alias="SELECTOR_LIVE_REQUIRE_PROMOTION_APPROVAL",
    )
    trading_proxy_price_krw: int = Field(default=80000, alias="TRADING_PROXY_PRICE_KRW")
    trading_min_target_notional_krw: int = Field(
        default=100000,
        alias="TRADING_MIN_TARGET_NOTIONAL_KRW",
    )
    trading_max_target_notional_krw: int = Field(
        default=1500000,
        alias="TRADING_MAX_TARGET_NOTIONAL_KRW",
    )
    trading_reference_edge_bps: float = Field(
        default=45.0,
        alias="TRADING_REFERENCE_EDGE_BPS",
    )
    trading_reference_penalty_bps: float = Field(
        default=80.0,
        alias="TRADING_REFERENCE_PENALTY_BPS",
    )
    trading_single_share_overshoot_tolerance_pct: float = Field(
        default=5.0,
        alias="TRADING_SINGLE_SHARE_OVERSHOOT_TOLERANCE_PCT",
    )
    trading_micro_test_mode_enabled: bool = Field(
        default=False,
        alias="TRADING_MICRO_TEST_MODE_ENABLED",
    )
    trading_micro_test_max_order_value_krw: int = Field(
        default=5000,
        alias="TRADING_MICRO_TEST_MAX_ORDER_VALUE_KRW",
    )
    trading_micro_test_require_allowed_symbols: bool = Field(
        default=True,
        alias="TRADING_MICRO_TEST_REQUIRE_ALLOWED_SYMBOLS",
    )
    trading_micro_test_run_once_only: bool = Field(
        default=True,
        alias="TRADING_MICRO_TEST_RUN_ONCE_ONLY",
    )
    trading_micro_test_max_spread_bps: float = Field(
        default=0.0,
        alias="TRADING_MICRO_TEST_MAX_SPREAD_BPS",
    )
    shadow_live_loop_auto_resume: bool = Field(
        default=True,
        alias="SHADOW_LIVE_LOOP_AUTO_RESUME",
    )
    shadow_live_loop_lease_ttl_seconds: float = Field(
        default=180.0,
        alias="SHADOW_LIVE_LOOP_LEASE_TTL_SECONDS",
    )
    shadow_live_loop_watchdog_grace_seconds: float = Field(
        default=240.0,
        alias="SHADOW_LIVE_LOOP_WATCHDOG_GRACE_SECONDS",
    )
    shadow_live_require_ws_live_market_data: bool = Field(
        default=True,
        alias="SHADOW_LIVE_REQUIRE_WS_LIVE_MARKET_DATA",
    )

    @property
    def database_url(self) -> str:
        return (
            f"mysql+pymysql://{self.mariadb_user}:{self.mariadb_password}"
            f"@{self.mariadb_host}:{self.mariadb_port}/{self.mariadb_db}"
        )

    @property
    def clickhouse_http_url(self) -> str:
        return (
            f"http://{self.clickhouse_host}:{self.clickhouse_port}"
        )

    @property
    def nats_stream_subject_list(self) -> list[str]:
        return [subject.strip() for subject in self.nats_stream_subjects.split(",") if subject.strip()]

    @property
    def kis_live_allowed_symbol_list(self) -> list[str]:
        return [symbol.strip() for symbol in self.kis_live_allowed_symbols.split(",") if symbol.strip()]

    @property
    def news_provider_list(self) -> list[str]:
        return [
            provider.strip().upper()
            for provider in self.news_provider_order.split(",")
            if provider.strip()
        ]

    @property
    def news_rss_feed_url_list(self) -> list[str]:
        return [url.strip() for url in self.news_rss_feed_urls.split(",") if url.strip()]

    @property
    def google_news_rss_source_site_list(self) -> list[str]:
        return [site.strip() for site in self.google_news_rss_source_sites.split(",") if site.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
