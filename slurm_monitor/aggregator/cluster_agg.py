"""集群级数据聚合器

定时从 NodeSnapshot 和 Job 表聚合生成集群级统计数据。
生成多粒度聚合数据：5m / 15m / 1h / 1d。
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any

from sqlalchemy import func, text
from sqlalchemy.dialects.postgresql import insert

from ..storage.db import get_session
from ..storage.models import NodeSnapshot, Job, ClusterAggStat, GpuSnapshot

logger = logging.getLogger(__name__)


class ClusterAggregator:
    """集群聚合器"""

    # 粒度配置：粒度名 -> (interval 字符串, 保留天数)
    GRANULARITIES = {
        "5m": ("5 minutes", 7),      # 5分钟粒度保留 7 天
        "15m": ("15 minutes", 30),   # 15分钟粒度保留 30 天
        "1h": ("1 hour", 90),        # 1小时粒度保留 90 天
        "1d": ("1 day", 365),        # 1天粒度保留 1 年
    }

    def aggregate_recent(self, granularity: str = "5m", hours: int = 1):
        """聚合最近 N 小时的数据

        Args:
            granularity: 粒度 (5m/15m/1h/1d)
            hours: 回溯小时数
        """
        if granularity not in self.GRANULARITIES:
            logger.error(f"不支持的粒度: {granularity}")
            return

        interval_str, _ = self.GRANULARITIES[granularity]
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=hours)

        logger.info(f"聚合集群数据: {granularity} 粒度, {start_dt} ~ {end_dt}")

        with get_session() as session:
            # 1. 聚合节点快照数据
            # 先按 (bucket_ts, node_name) 求均值（消除同一节点同一桶内的多次采样），
            # 再跨节点 SUM，得到集群级总量（避免 AVG 把多节点的值平均掉）。
            node_agg = session.execute(text(f"""
                SELECT
                    bucket_ts,
                    SUM(gpu_total)        AS gpu_total,
                    SUM(gpu_used)         AS gpu_used,
                    SUM(cpu_total)        AS cpu_total,
                    SUM(cpu_used)         AS cpu_used,
                    SUM(mem_total_gb)     AS mem_total_gb,
                    SUM(mem_used_gb)      AS mem_used_gb,
                    COUNT(*)              AS node_count,
                    SUM(nodes_idle)       AS nodes_idle,
                    SUM(nodes_mixed)      AS nodes_mixed,
                    SUM(nodes_allocated)  AS nodes_allocated,
                    SUM(nodes_down)       AS nodes_down,
                    SUM(sample_count)     AS sample_count
                FROM (
                    SELECT
                        date_bin(
                            interval '{interval_str}',
                            collected_at,
                            TIMESTAMP '2000-01-01'
                        )                                              AS bucket_ts,
                        node_name,
                        AVG(gres_total)                                AS gpu_total,
                        AVG(gres_alloc)                                AS gpu_used,
                        AVG(total_cpus)                                AS cpu_total,
                        AVG(alloc_cpus)                                AS cpu_used,
                        AVG(total_memory_mb) / 1024.0                 AS mem_total_gb,
                        AVG(alloc_memory_mb) / 1024.0                 AS mem_used_gb,
                        1                                              AS node_count,
                        MAX(CASE WHEN state = 'idle'                  THEN 1 ELSE 0 END) AS nodes_idle,
                        MAX(CASE WHEN state = 'mixed'                 THEN 1 ELSE 0 END) AS nodes_mixed,
                        MAX(CASE WHEN state = 'allocated'             THEN 1 ELSE 0 END) AS nodes_allocated,
                        MAX(CASE WHEN state LIKE '%down%'
                                  OR state LIKE '%drain%'             THEN 1 ELSE 0 END) AS nodes_down,
                        COUNT(*)                                       AS sample_count
                    FROM node_snapshots
                    WHERE collected_at >= :start AND collected_at < :end
                    GROUP BY bucket_ts, node_name
                ) AS per_node
                GROUP BY bucket_ts
                ORDER BY bucket_ts
            """), {"start": start_dt, "end": end_dt}).fetchall()

            # 2. 获取 GPU 平均利用率（如果有采集数据）
            gpu_util_map = {}
            gpu_util_rows = session.execute(text(f"""
                SELECT
                    date_bin(
                        interval '{interval_str}',
                        collected_at,
                        TIMESTAMP '2000-01-01'
                    ) AS bucket_ts,
                    AVG(util_pct) AS avg_util
                FROM gpu_snapshots
                WHERE collected_at >= :start AND collected_at < :end
                  AND util_pct IS NOT NULL
                GROUP BY bucket_ts
            """), {"start": start_dt, "end": end_dt}).fetchall()

            for row in gpu_util_rows:
                gpu_util_map[row.bucket_ts] = row.avg_util

            # 3. 聚合作业数据（按时间桶统计运行/排队作业数）
            # 注意：这里简化处理，统计每个桶内的作业快照
            # 实际应该按照作业的生命周期计算在该时间桶内的平均作业数
            job_agg_map = {}
            for bucket_row in node_agg:
                bucket_ts = bucket_row.bucket_ts
                bucket_end = bucket_ts + timedelta(seconds=self._interval_to_seconds(interval_str))

                # 统计该时间段内的作业数（简化版）
                running_count = session.query(Job).filter(
                    Job.state == "RUNNING",
                    Job.start_time <= bucket_end,
                    (Job.end_time.is_(None) | (Job.end_time >= bucket_ts))
                ).count()

                pending_count = session.query(Job).filter(
                    Job.state == "PENDING",
                    Job.submit_time <= bucket_end
                ).count()

                job_agg_map[bucket_ts] = {
                    "running": running_count,
                    "pending": pending_count,
                }

            # 4. 合并数据并写入
            records = []
            for row in node_agg:
                bucket_ts = row.bucket_ts
                jobs = job_agg_map.get(bucket_ts, {"running": 0, "pending": 0})

                record = {
                    "bucket_ts": bucket_ts,
                    "granularity": granularity,
                    "gpu_total": int(row.gpu_total or 0),
                    "gpu_used": int(row.gpu_used or 0),
                    "gpu_util_avg": float(gpu_util_map.get(bucket_ts, 0)) if bucket_ts in gpu_util_map else None,
                    "cpu_total": int(row.cpu_total or 0),
                    "cpu_used": int(row.cpu_used or 0),
                    "mem_total_gb": float(row.mem_total_gb or 0),
                    "mem_used_gb": float(row.mem_used_gb or 0),
                    "job_running": jobs["running"],
                    "job_pending": jobs["pending"],
                    "nodes_total": int(row.node_count or 0),
                    "nodes_idle": int(row.nodes_idle or 0),
                    "nodes_mixed": int(row.nodes_mixed or 0),
                    "nodes_allocated": int(row.nodes_allocated or 0),
                    "nodes_down": int(row.nodes_down or 0),
                    "sample_count": int(row.sample_count or 0),
                }
                records.append(record)

            # 使用 upsert 避免重复
            if records:
                stmt = insert(ClusterAggStat).values(records)
                stmt = stmt.on_conflict_do_update(
                    constraint="uq_cluster_agg",
                    set_={
                        "gpu_total": stmt.excluded.gpu_total,
                        "gpu_used": stmt.excluded.gpu_used,
                        "gpu_util_avg": stmt.excluded.gpu_util_avg,
                        "cpu_total": stmt.excluded.cpu_total,
                        "cpu_used": stmt.excluded.cpu_used,
                        "mem_total_gb": stmt.excluded.mem_total_gb,
                        "mem_used_gb": stmt.excluded.mem_used_gb,
                        "job_running": stmt.excluded.job_running,
                        "job_pending": stmt.excluded.job_pending,
                        "nodes_total": stmt.excluded.nodes_total,
                        "nodes_idle": stmt.excluded.nodes_idle,
                        "nodes_mixed": stmt.excluded.nodes_mixed,
                        "nodes_allocated": stmt.excluded.nodes_allocated,
                        "nodes_down": stmt.excluded.nodes_down,
                        "sample_count": stmt.excluded.sample_count,
                    }
                )
                session.execute(stmt)
                session.commit()
                logger.info(f"已聚合 {len(records)} 个时间桶的数据")

    def _interval_to_seconds(self, interval_str: str) -> int:
        """将 interval 字符串转换为秒数"""
        parts = interval_str.split()
        value = int(parts[0])
        unit = parts[1]

        if unit.startswith("minute"):
            return value * 60
        elif unit.startswith("hour"):
            return value * 3600
        elif unit.startswith("day"):
            return value * 86400
        else:
            return 300  # 默认 5 分钟

    def cleanup_old_data(self, granularity: str):
        """清理过期数据

        Args:
            granularity: 粒度 (5m/15m/1h/1d)
        """
        if granularity not in self.GRANULARITIES:
            return

        _, retain_days = self.GRANULARITIES[granularity]
        cutoff = datetime.now(timezone.utc) - timedelta(days=retain_days)

        with get_session() as session:
            deleted = session.query(ClusterAggStat).filter(
                ClusterAggStat.granularity == granularity,
                ClusterAggStat.bucket_ts < cutoff
            ).delete()
            session.commit()

            if deleted > 0:
                logger.info(f"清理 {granularity} 粒度过期数据: {deleted} 条")

    def aggregate_all_granularities(self):
        """聚合所有粒度的数据"""
        # 5m 粒度：最近 1 小时
        self.aggregate_recent("5m", hours=1)

        # 15m 粒度：最近 6 小时
        self.aggregate_recent("15m", hours=6)

        # 1h 粒度：最近 24 小时
        self.aggregate_recent("1h", hours=24)

        # 1d 粒度：最近 7 天
        self.aggregate_recent("1d", hours=24 * 7)

        # 清理过期数据
        for granularity in self.GRANULARITIES:
            self.cleanup_old_data(granularity)
