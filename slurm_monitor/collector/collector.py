"""主采集器

协调 REST API 和 SSH (sacct/squeue/sinfo) 的数据采集，实现：
1. 实时：节点状态 + 运行中/排队作业（优先 REST API）
2. 历史：sacct 增量同步，补全 REST API 用户信息缺失
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

from ..collector.rest_client import rest_client
from ..collector.ssh_client import sacct_jobs, sacct_job_user, squeue_jobs, sinfo_nodes
from ..collector.gpu_collector import fetch_gpu_frame, agent_rows_to_db
from ..storage.writer import load_enabled_node_ip_map
from ..parser.sacct import parse_sacct_output, DEFAULT_FIELDS
from ..parser.tres import parse_tres
from ..storage.db import init_db
from ..storage.writer import (
    upsert_jobs_from_sacct,
    save_gpu_snapshots,
    save_node_host_stats,
    upsert_jobs_from_rest,
    save_node_snapshot,
    patch_job_user,
    aggregate_node_daily,
    purge_old_snapshots,
)
from ..storage.node_health import update_node_health
from ..config import settings

logger = logging.getLogger(__name__)



class SlurmCollector:
    """采集器主类，按配置间隔轮询 Slurm 数据并持久化。"""

    def __init__(self):
        self._last_history_sync: Optional[datetime] = None
        self._last_hourly_agg: Optional[datetime] = None
        self._last_purge: Optional[datetime] = None
        # 节点名 -> (连续失败次数, 下次允许重试的 monotonic 时间)
        self._gpu_backoff: dict[str, tuple[int, float]] = {}
        # 节点名 -> 上次写入 DB 时的健康状态（'ok'/'degraded'/'error'/'unknown'）
        # 仅在状态转变时写 DB，避免正常采集每轮产生无效 upsert
        self._gpu_node_status: dict[str, str] = {}

    # ---- 实时采集 ----

    def collect_realtime(self) -> dict:
        """采集节点、实时作业、分区。返回汇总供缓存写入。"""
        result: dict = {}

        try:
            nodes = rest_client.nodes()
            save_node_snapshot(nodes)
            result["nodes"] = nodes
            logger.info("collected %d nodes", len(nodes))
        except Exception as exc:
            logger.warning("nodes collection failed: %s", exc)
            result["nodes"] = []

        try:
            jobs = rest_client.jobs()
            missing_user_ids = [
                j.get("job_id") for j in jobs
                if not j.get("user_name") and not j.get("user_id")
            ]
            result["jobs"] = jobs
            result["jobs_ok"] = True
            result["missing_user_job_ids"] = missing_user_ids
            logger.info("collected %d realtime jobs (%d missing user)", len(jobs), len(missing_user_ids))
        except Exception as exc:
            logger.warning("jobs collection failed: %s", exc)
            result["jobs"] = []
            result["missing_user_job_ids"] = []

        return result

    # ---- GPU 实测采集（agent.py 注入，从 node_ip_map 读取节点配置） ----

    def collect_gpu(self) -> None:
        """轮询 node_ip_map 中启用的算力节点，注入 agent 采集 GPU 数据，写入 GpuSnapshot。

        节点连续失败时指数退避（30s→…→15min），避免对认证失败的账号高频重试导致被锁定。
        """
        try:
            nodes = load_enabled_node_ip_map()
        except Exception as exc:
            logger.warning("failed to load node_ip_map: %s", exc)
            return

        now = time.monotonic()
        for node in nodes:
            name = node["node_name"]
            fails, next_try = self._gpu_backoff.get(name, (0, 0.0))
            if now < next_try:
                continue
            try:
                frame = fetch_gpu_frame(
                    ip=node["ip"],
                    port=node["port"],
                    ssh_user=node["ssh_user"] or settings.node_ssh_user,
                    ssh_password=node["ssh_password"] or settings.node_ssh_password,
                )
            except Exception as exc:
                fails += 1
                delay = min(settings.collector_interval_realtime * 2 ** (fails - 1), 900)
                self._gpu_backoff[name] = (fails, now + delay)
                logger.warning("GPU collect failed for %s (%s), fail #%d, retry in %ds: %s",
                               name, node["ip"], fails, delay, exc)
                # 更新节点健康状态（只在首次失败或状态已是 ok 时写 DB，避免每次失败都写）
                prev_status = self._gpu_node_status.get(name, 'unknown')
                new_status = 'error' if fails >= 3 else 'degraded'
                if prev_status != new_status:
                    update_node_health(
                        node_name=name,
                        node_type='compute',
                        status=new_status,
                        consecutive_failures=fails,
                        last_failure_at=datetime.now(timezone.utc),
                        last_error_msg=str(exc),
                        next_retry_at=datetime.fromtimestamp(now + delay, tz=timezone.utc),
                    )
                    self._gpu_node_status[name] = new_status
                continue

            if fails:
                logger.info("GPU collect for %s recovered after %d failures", name, fails)
            self._gpu_backoff.pop(name, None)

            # 仅在状态从非 ok 转为 ok 时写 DB（正常采集不产生额外写入）
            if self._gpu_node_status.get(name) != 'ok':
                update_node_health(
                    node_name=name,
                    node_type='compute',
                    status='ok',
                    consecutive_failures=0,
                    last_success_at=datetime.now(timezone.utc),
                    last_error_msg=None,
                )
                self._gpu_node_status[name] = 'ok'

            gpus = frame.get("gpus", [])
            if gpus:
                rows = agent_rows_to_db(name, gpus)
                save_gpu_snapshots(name, rows)
                logger.info("collected %d GPU snapshots from %s", len(rows), name)
            save_node_host_stats(name, frame.get("host") or {})

        # 节点被删除/停用后清理退避状态
        active = {n["node_name"] for n in nodes}
        for stale in set(self._gpu_backoff) - active:
            self._gpu_backoff.pop(stale, None)

    # ---- 用户信息补全 ----

    def backfill_users_via_sacct(self, job_ids: list[int]) -> None:
        """为 REST API 用户字段为空的作业通过 sacct 补全用户信息。"""
        if not job_ids:
            return
        try:
            raw = sacct_job_user(job_ids)
        except Exception as exc:
            logger.warning("sacct user backfill failed: %s", exc)
            return

        for line in raw.splitlines():
            if not line.strip():
                continue
            parts = line.split("|")
            if len(parts) < 3:
                continue
            raw_job_id, user, uid_str = parts[0], parts[1], parts[2]
            if "." in raw_job_id:
                continue
            try:
                job_id = int(raw_job_id.split("_")[0])
            except ValueError:
                continue
            if user and user not in ("", "None"):
                uid = int(uid_str) if uid_str.isdigit() else None
                patch_job_user(job_id, user, uid)

    # ---- 历史同步 ----

    def sync_history(self, lookback_seconds: Optional[int] = None) -> int:
        """从 sacct 增量同步历史作业。返回写入条数。"""
        if lookback_seconds is None:
            lookback_seconds = settings.collector_history_lookback

        if self._last_history_sync is None:
            start = datetime.now(timezone.utc) - timedelta(days=30)
        else:
            start = self._last_history_sync - timedelta(seconds=lookback_seconds)

        start_str = start.strftime("%Y-%m-%d")
        logger.info("syncing history from %s", start_str)

        try:
            raw = sacct_jobs(start=start_str)
        except Exception as exc:
            logger.error("sacct history sync failed: %s", exc)
            return 0

        records = parse_sacct_output(raw, DEFAULT_FIELDS)
        written = upsert_jobs_from_sacct(records)
        self._last_history_sync = datetime.now(timezone.utc)
        logger.info("history sync wrote %d records", written)
        return written

    # ---- 天级聚合 ----

    def run_daily_aggregation(self) -> None:
        """将昨天的 NodeSnapshot 聚合到 NodeAggStat(1d)。每小时触发一次（幂等）。"""
        now = datetime.now(timezone.utc)
        yesterday = (now - timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        today = now.replace(hour=0, minute=0, second=0, microsecond=0)

        try:
            from ..storage.db import get_session
            from ..storage.models import NodeSnapshot
            with get_session() as session:
                node_names = [
                    r[0] for r in session.query(NodeSnapshot.node_name).distinct().all()
                ]
            from ..storage.writer import aggregate_node_daily
            for name in node_names:
                aggregate_node_daily(name, yesterday)
                aggregate_node_daily(name, today)  # 今天也实时聚合（幂等）
            logger.info("daily aggregation done for %d nodes", len(node_names))
        except Exception as exc:
            logger.error("daily aggregation failed: %s", exc)

    # ---- 主循环 ----

    def run_once(self) -> None:
        """执行一次完整采集周期。"""
        now = datetime.now(timezone.utc)

        # 实时采集（每轮都做）
        rt = self.collect_realtime()
        try:
            upsert_jobs_from_rest(rt.get("jobs", []), prune_pending=rt.get("jobs_ok", False))
        except Exception as exc:
            logger.warning("upsert realtime jobs failed: %s", exc)
        if rt.get("missing_user_job_ids"):
            self.backfill_users_via_sacct(rt["missing_user_job_ids"])

        # GPU 实测采集（Phase 2）
        self.collect_gpu()

        # 历史同步（按配置间隔）
        if (
            self._last_history_sync is None
            or (now - self._last_history_sync).total_seconds() >= settings.collector_interval_history
        ):
            self.sync_history()

        # 天级聚合（每小时触发一次，幂等）
        current_hour = now.replace(minute=0, second=0, microsecond=0)
        if self._last_hourly_agg is None or self._last_hourly_agg < current_hour:
            self.run_daily_aggregation()
            self._last_hourly_agg = current_hour

        # 过期数据清理（每天一次）
        if (
            self._last_purge is None
            or (now - self._last_purge).total_seconds() >= 86400
        ):
            try:
                purge_old_snapshots()
            except Exception as exc:
                logger.warning("purge failed: %s", exc)
            self._last_purge = now

    def run(self) -> None:
        """持续运行采集主循环。"""
        logger.info("initializing database")
        init_db()
        logger.info("starting collector (realtime=%ds, history=%ds)",
                    settings.collector_interval_realtime, settings.collector_interval_history)

        while True:
            t0 = time.monotonic()
            try:
                self.run_once()
            except Exception as exc:
                logger.exception("collector run_once failed: %s", exc)
            elapsed = time.monotonic() - t0
            sleep_for = max(0, settings.collector_interval_realtime - elapsed)
            logger.debug("sleeping %.1fs before next cycle", sleep_for)
            time.sleep(sleep_for)
