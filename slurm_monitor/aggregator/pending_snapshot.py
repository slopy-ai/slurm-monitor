"""排队作业快照采集器

每 5 分钟采集一次当前排队作业的统计信息。
用于生成排队等待趋势、P90 等待时长、原因分布等。
"""

import json
import logging
from datetime import datetime, timedelta, timezone
from collections import Counter
from typing import List, Dict, Any

from sqlalchemy.dialects.postgresql import insert

from ..storage.db import get_session
from ..storage.models import Job, PendingJobSnapshot

logger = logging.getLogger(__name__)


class PendingJobSnapshotCollector:
    """排队作业快照采集器"""

    def collect_snapshot(self):
        """采集当前排队作业快照"""
        snapshot_ts = datetime.now(timezone.utc)

        with get_session() as session:
            # 查询所有排队作业
            pending_jobs = session.query(Job).filter(
                Job.state == "PENDING"
            ).all()

            if not pending_jobs:
                logger.info("当前无排队作业")
                # 仍然记录快照（数据为 0）
                snapshot = self._create_empty_snapshot(snapshot_ts)
            else:
                snapshot = self._analyze_pending_jobs(pending_jobs, snapshot_ts)

            # 写入数据库
            stmt = insert(PendingJobSnapshot).values(snapshot)
            session.execute(stmt)
            session.commit()

            logger.info(
                f"已采集排队快照: {snapshot['pending_total']} 个作业, "
                f"平均等待 {snapshot['avg_wait_minutes']:.1f} 分钟"
            )

    def _create_empty_snapshot(self, snapshot_ts: datetime) -> Dict[str, Any]:
        """创建空快照"""
        return {
            "snapshot_ts": snapshot_ts,
            "pending_total": 0,
            "pending_gpu_demand": 0,
            "avg_wait_minutes": 0.0,
            "p50_wait_minutes": 0.0,
            "p90_wait_minutes": 0.0,
            "max_wait_minutes": 0.0,
            "long_pending_count": 0,
            "reason_distribution": json.dumps({}),
            "top_users": json.dumps([]),
            "top_nodes": json.dumps([]),
        }

    def _analyze_pending_jobs(self, jobs: List[Job], snapshot_ts: datetime) -> Dict[str, Any]:
        """分析排队作业"""
        now = snapshot_ts

        # 计算等待时长
        wait_times = []
        for job in jobs:
            if job.submit_time:
                # 确保 submit_time 是 timezone-aware（如果是 naive，假定为 UTC）
                submit_time = job.submit_time
                if submit_time.tzinfo is None:
                    submit_time = submit_time.replace(tzinfo=timezone.utc)

                wait_seconds = (now - submit_time).total_seconds()
                wait_minutes = wait_seconds / 60
                wait_times.append(wait_minutes)

        wait_times_sorted = sorted(wait_times)
        n = len(wait_times_sorted)

        avg_wait = sum(wait_times) / n if wait_times else 0
        p50_wait = wait_times_sorted[n // 2] if n > 0 else 0
        p90_wait = wait_times_sorted[int(n * 0.9)] if n > 0 else 0
        max_wait = max(wait_times) if wait_times else 0

        # 统计长时间排队（> 30 分钟）
        long_pending_count = sum(1 for wt in wait_times if wt > 30)

        # 统计 GPU 需求
        total_gpu_demand = sum(job.req_gpus or 0 for job in jobs)

        # 等待原因分布（pending_reason 由 collector 从 slurmrestd 写入；尚未采集到的记为 Unknown）
        reason_dist = dict(Counter(job.pending_reason or "Unknown" for job in jobs))

        # Top 用户
        user_counter = Counter(job.user for job in jobs if job.user)
        top_users = [
            {"user": user, "pending": count}
            for user, count in user_counter.most_common(5)
        ]

        # Top 节点（目标节点）
        node_counter = Counter()
        for job in jobs:
            if job.node_list:
                # 简化处理，实际可能是节点列表
                node_counter[job.node_list] += 1

        top_nodes = [
            {"node": node, "pending": count}
            for node, count in node_counter.most_common(5)
        ]

        return {
            "snapshot_ts": snapshot_ts,
            "pending_total": len(jobs),
            "pending_gpu_demand": total_gpu_demand,
            "avg_wait_minutes": round(avg_wait, 2),
            "p50_wait_minutes": round(p50_wait, 2),
            "p90_wait_minutes": round(p90_wait, 2),
            "max_wait_minutes": round(max_wait, 2),
            "long_pending_count": long_pending_count,
            "reason_distribution": json.dumps(reason_dist, ensure_ascii=False),
            "top_users": json.dumps(top_users, ensure_ascii=False),
            "top_nodes": json.dumps(top_nodes, ensure_ascii=False),
        }

    def cleanup_old_snapshots(self, days: int = 30):
        """清理过期快照

        Args:
            days: 保留天数
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=days)

        with get_session() as session:
            deleted = session.query(PendingJobSnapshot).filter(
                PendingJobSnapshot.snapshot_ts < cutoff
            ).delete()
            session.commit()

            if deleted > 0:
                logger.info(f"清理排队快照过期数据: {deleted} 条")
