"""配置管理"""

from pydantic_settings import BaseSettings
from pydantic import Field
from typing import Optional


class Settings(BaseSettings):
    # Slurm REST API
    slurm_rest_url: str = Field("http://192.168.112.180:6820", alias="SLURM_REST_URL")
    slurm_api_version: str = Field("v0.0.37", alias="SLURM_API_VERSION")
    slurm_user_name: str = Field("djs", alias="SLURM_USER_NAME")
    slurm_user_token: str = Field("", alias="SLURM_USER_TOKEN")

    # SSH（用于 sacct/squeue/sinfo 备用采集）
    ssh_host: str = Field("192.168.112.182", alias="SSH_HOST")
    ssh_port: int = Field(22, alias="SSH_PORT")
    ssh_user: str = Field("djs", alias="SSH_USER")
    ssh_password: Optional[str] = Field(None, alias="SSH_PASSWORD")
    ssh_key_path: Optional[str] = Field(None, alias="SSH_KEY_PATH")
    ssh_connect_timeout: int = Field(10, alias="SSH_CONNECT_TIMEOUT")

    # 算力节点 SSH 默认认证（node_ip_map 中未单独配置用户/密码时使用）
    node_ssh_user: str = Field("djs", alias="NODE_SSH_USER")
    node_ssh_password: Optional[str] = Field(None, alias="NODE_SSH_PASSWORD")

    # PostgreSQL
    postgres_host: str = Field("localhost", alias="POSTGRES_HOST")
    postgres_port: int = Field(5432, alias="POSTGRES_PORT")
    postgres_db: str = Field("slurm_monitor", alias="POSTGRES_DB")
    postgres_user: str = Field("slurm_monitor", alias="POSTGRES_USER")
    postgres_password: str = Field("", alias="POSTGRES_PASSWORD")

    @property
    def postgres_dsn(self) -> str:
        return (
            f"postgresql+psycopg2://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    # Redis
    redis_host: str = Field("localhost", alias="REDIS_HOST")
    redis_port: int = Field(6379, alias="REDIS_PORT")
    redis_password: Optional[str] = Field(None, alias="REDIS_PASSWORD")
    redis_db: int = Field(0, alias="REDIS_DB")

    @property
    def redis_url(self) -> str:
        auth = f":{self.redis_password}@" if self.redis_password else ""
        return f"redis://{auth}{self.redis_host}:{self.redis_port}/{self.redis_db}"

    # 采集间隔
    collector_interval_realtime: int = Field(30, alias="COLLECTOR_INTERVAL_REALTIME")
    collector_interval_history: int = Field(300, alias="COLLECTOR_INTERVAL_HISTORY")
    # 历史数据回溯时间（秒），用于处理结算延迟
    collector_history_lookback: int = Field(3600, alias="COLLECTOR_HISTORY_LOOKBACK")

    # Web 服务
    web_host: str = Field("0.0.0.0", alias="WEB_HOST")
    web_port: int = Field(8000, alias="WEB_PORT")
    web_debug: bool = Field(False, alias="WEB_DEBUG")

    # 日志
    log_level: str = Field("INFO", alias="LOG_LEVEL")
    log_file: Optional[str] = Field(None, alias="LOG_FILE")

    # 时区
    timezone: str = Field("Asia/Shanghai", alias="TIMEZONE")

    # 项目组配置页管理员认证（明文，内部简单管控）
    admin_user: str = Field("admin", alias="ADMIN_USER")
    admin_pass: str = Field("admin123", alias="ADMIN_PASS")

    model_config = {"env_file": ".env", "populate_by_name": True}


settings = Settings()
