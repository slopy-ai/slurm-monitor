"""Slurm REST API 客户端

封装 slurmrestd 的只读查询。作为主要数据源，当 user 字段为空时由 SSH sacct 补全。
"""

import logging
from typing import Any

import requests

from ..config import settings

logger = logging.getLogger(__name__)


class SlurmRestClient:
    """slurmrestd 只读客户端，自动重试认证失败。"""

    def __init__(self):
        self.base_url = f"{settings.slurm_rest_url.rstrip('/')}"
        self.api = settings.slurm_api_version
        self.session = requests.Session()
        self._token: str | None = None

    def _headers(self) -> dict[str, str]:
        return {
            "X-SLURM-USER-NAME": settings.slurm_user_name,
            "X-SLURM-USER-TOKEN": settings.slurm_user_token,
            "Accept": "application/json",
        }

    def _get(self, path: str, params: dict | None = None) -> dict[str, Any]:
        url = f"{self.base_url}{path}"
        resp = self.session.get(url, headers=self._headers(), params=params, timeout=30)
        resp.raise_for_status()
        data = resp.json()
        # slurmrestd 在 HTTP 200 下仍可能在 errors 数组里报错
        if data.get("errors"):
            errs = data["errors"]
            logger.warning("slurmrestd %s returned errors: %s", path, errs)
        return data

    # ---- 实时接口 ----

    def nodes(self) -> list[dict]:
        """节点状态与资源容量、分配。"""
        data = self._get(f"/slurm/{self.api}/nodes")
        return data.get("nodes", []) or []

    def jobs(self) -> list[dict]:
        """实时作业（运行中与排队）。"""
        data = self._get(f"/slurm/{self.api}/jobs")
        return data.get("jobs", []) or []

    def partitions(self) -> list[dict]:
        data = self._get(f"/slurm/{self.api}/partitions")
        return data.get("partitions", []) or []

    def diag(self) -> dict:
        """调度器统计：调度周期、队列、回填等。"""
        data = self._get(f"/slurm/{self.api}/diag")
        return data.get("statistics", {}) or {}

    # ---- 历史接口 ----

    def jobs_db(self, start_ts: int | None = None, end_ts: int | None = None) -> list[dict]:
        """slurmdbd 历史作业。start/end 为 Unix 时间戳。"""
        params: dict = {}
        if start_ts is not None:
            params["start_time"] = start_ts
        if end_ts is not None:
            params["end_time"] = end_ts
        data = self._get(f"/slurmdb/{self.api}/jobs", params=params or None)
        return data.get("jobs", []) or []

    def tres(self) -> list[dict]:
        """TRES 类型定义，用于解析资源字符串。"""
        data = self._get(f"/slurmdb/{self.api}/tres")
        return data.get("tres", []) or []


rest_client = SlurmRestClient()
