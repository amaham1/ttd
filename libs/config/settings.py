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
    broker_gateway_port: int = Field(default=8001, alias="BROKER_GATEWAY_PORT")
    broker_gateway_url: str = Field(default="http://localhost:8001", alias="BROKER_GATEWAY_URL")
    trading_core_port: int = Field(default=8002, alias="TRADING_CORE_PORT")
    market_intel_port: int = Field(default=8003, alias="MARKET_INTEL_PORT")
    replay_runner_port: int = Field(default=8004, alias="REPLAY_RUNNER_PORT")
    shadow_live_port: int = Field(default=8005, alias="SHADOW_LIVE_PORT")

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
    kis_enable_paper: bool = Field(default=False, alias="KIS_ENABLE_PAPER")
    kis_live_trading_enabled: bool = Field(default=False, alias="KIS_LIVE_TRADING_ENABLED")
    kis_live_require_arm: bool = Field(default=True, alias="KIS_LIVE_REQUIRE_ARM")
    kis_live_max_order_value_krw: int = Field(default=0, alias="KIS_LIVE_MAX_ORDER_VALUE_KRW")
    kis_live_allowed_symbols: str = Field(default="", alias="KIS_LIVE_ALLOWED_SYMBOLS")
    kis_live_daily_loss_limit_pct: float = Field(default=5.0, alias="KIS_LIVE_DAILY_LOSS_LIMIT_PCT")
    kis_live_min_total_equity_krw: int = Field(default=5000000, alias="KIS_LIVE_MIN_TOTAL_EQUITY_KRW")
    kis_live_common_stock_only: bool = Field(default=True, alias="KIS_LIVE_COMMON_STOCK_ONLY")

    openai_api_key: str = Field(default="", alias="OPENAI_API_KEY")
    openai_model: str = Field(default="gpt-4.1-mini", alias="OPENAI_MODEL")
    opendart_api_key: str = Field(default="", alias="OPENDART_API_KEY")
    opendart_base_url: str = Field(default="https://opendart.fss.or.kr/api", alias="OPENDART_BASE_URL")

    @property
    def database_url(self) -> str:
        return (
            f"mysql+pymysql://{self.mariadb_user}:{self.mariadb_password}"
            f"@{self.mariadb_host}:{self.mariadb_port}/{self.mariadb_db}"
        )

    @property
    def nats_stream_subject_list(self) -> list[str]:
        return [subject.strip() for subject in self.nats_stream_subjects.split(",") if subject.strip()]

    @property
    def kis_live_allowed_symbol_list(self) -> list[str]:
        return [symbol.strip() for symbol in self.kis_live_allowed_symbols.split(",") if symbol.strip()]


@lru_cache
def get_settings() -> Settings:
    return Settings()
