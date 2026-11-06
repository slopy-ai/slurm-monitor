"""新版 API 路由 - 符合前端接口文档规范

按照 Slurm监控前端接口文档.md 实现统一响应格式和路径结构。
"""

from __future__ import annotations

import csv
import io
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional, List, Dict, Any
from zoneinfo import ZoneInfo

from fastapi import APIRouter, Query, HTTPException
from fastapi.responses import Response
from sqlalchemy import func, desc, text

from ..storage.db import get_session
from ..storage.models import Job, JobHistory, NodeSnapshot, NodeAggStat, GpuSnapshot
from ..config import settings
from ..storage.cache import cache

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api", tags=["slurm-monitor"])

# 缓存 TTL（秒）- 优化后：延长 TTL 减少缓存失效，支持多用户复用
TTL_SUMMARY = 30
TTL_TREND = 60
TTL_NODES = 45         # 30 → 45（减少节点列表缓存失效频率）
TTL_NODE_DETAIL = 30
TTL_PENDING = 30       # 15 → 30（排队作业变化不频繁）
TTL_META = 300         # 60 → 300（元数据变化少）
TTL_GPU_LIVE = 10      # GPU 实时帧：采集间隔 30s，短缓存即可
TTL_RGTOP_FRAME = 10   # rgtop 的批量接口，与 gpu-live 同样语义和 TTL
TTL_TOP_USERS = 120    # 新增：cluster/top-users 缓存 2 分钟（计算较慢）


def _now_utc() -> datetime:
    """当前 UTC 时间，timezone-aware。"""
    return datetime.now(timezone.utc)


def _to_naive(dt: Optional[datetime]) -> Optional[datetime]:
    """将 datetime 转为 naive（去掉 tzinfo），与数据库中的 naive 时间兼容。"""
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def _seconds_since(dt: Optional[datetime]) -> Optional[float]:
    """计算从 dt 到现在的秒数，兼容 naive/aware datetime。"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        # 数据库返回的 naive datetime，按 UTC 处理
        now = datetime.utcnow()
    else:
        now = _now_utc()
    return (now - dt).total_seconds()

# 粒度映射
GRANULARITY_MAP = {
    "5s": "5 seconds",
    "30s": "30 seconds",
    "1m": "1 minute",
    "5m": "5 minutes",
    "15m": "15 minutes",
    "1h": "1 hour",
    "1d": "1 day",
}


def success_response(data: Any) -> Dict[str, Any]:
    """统一成功响应格式"""
    return {"code": 0, "data": data}


def error_response(code: int, message: str) -> Dict[str, Any]:
    """统一错误响应格式"""
    return {"code": code, "message": message}


def _to_iso(dt: Optional[datetime]) -> Optional[str]:
    """转换为 YYYY-MM-DD HH:mm:ss 格式"""
    if not dt:
        return None
    # 转换为本地时区（东八区）
    local_tz = timezone(timedelta(hours=8))
    local_dt = dt.astimezone(local_tz)
    return local_dt.strftime("%Y-%m-%d %H:%M:%S")


# ==================== 三、总览页 ====================

@router.get("/cluster/health")
def cluster_health():
    """集群连接健康状态（算力节点 + 登录机）"""
    cache_key = "cluster_health"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    from ..storage.node_health import get_cluster_health
    health = get_cluster_health()

    # 格式化返回数据
    data = {
        "login_node": None,
        "compute_nodes": []
    }

    if health["login_node"]:
        ln = health["login_node"]
        data["login_node"] = {
            "status": ln["status"],
            "consecutive_failures": ln["consecutive_failures"],
            "last_success_at": _to_iso(ln["last_success_at"]),
            "last_failure_at": _to_iso(ln["last_failure_at"]),
            "last_error_msg": ln["last_error_msg"],
        }

    for cn in health["compute_nodes"]:
        data["compute_nodes"].append({
            "name": cn["name"],
            "status": cn["status"],
            "consecutive_failures": cn["consecutive_failures"],
            "last_success_at": _to_iso(cn["last_success_at"]),
            "last_failure_at": _to_iso(cn["last_failure_at"]),
            "last_error_msg": cn["last_error_msg"],
            "next_retry_at": _to_iso(cn["next_retry_at"]),
        })

    cache.set(cache_key, data, 10)  # 短缓存10秒
    return success_response(data)


@router.get("/cluster/top-pending")
def cluster_top_pending(
    limit: int = Query(10, ge=5, le=50)
):
    """排队作业等待时长 TOP（总览页用）

    实时快照，按当前 PENDING 作业的等待时长从高到低排列。
    等待时长 = 当前时间 - submit_time。

    Returns:
        {
            "top_pending": [
                {
                    "job_id": 1234,
                    "job_label": "1234",
                    "user": "zhangsan",
                    "job_name": "train_bert",
                    "partition": "gpu",
                    "req_gpus": 4,
                    "wait_sec": 7200,
                    "wait_fmt": "2时0分",
                    "reason": "Resources",
                    "submit_time": "2025-01-08 10:00:00"
                }
            ],
            "ts": "2025-01-08 12:00:00"
        }
    """
    cache_key = f"top_pending:{limit}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        jobs = session.query(Job).filter(
            Job.state == "PENDING",
            Job.submit_time.isnot(None),
        ).order_by(Job.submit_time).limit(limit * 2).all()  # 多取一些，过滤掉 submit_time 异常的

        now_naive = datetime.utcnow()

        def fmt_wait(sec: float) -> str:
            sec = int(sec)
            d, r = divmod(sec, 86400)
            h, r = divmod(r, 3600)
            m = r // 60
            if d:
                return f"{d}天{h}时{m}分"
            if h:
                return f"{h}时{m}分"
            return f"{m}分"

        rows = []
        for j in jobs:
            st = j.submit_time
            if st is None:
                continue
            wait_sec = (now_naive - st).total_seconds()
            if wait_sec < 0:
                continue

            # 数组子任务标签（141_54 格式）
            if j.array_task_id is not None and j.array_job_id is not None:
                job_label = f"{j.array_job_id}_{j.array_task_id}"
            else:
                job_label = str(j.job_id)

            rows.append({
                "job_id": j.job_id,
                "job_label": job_label,
                "user": j.user or "—",
                "job_name": j.job_name or "—",
                "partition": j.partition or "—",
                "req_gpus": j.req_gpus or 0,
                "wait_sec": int(wait_sec),
                "wait_fmt": fmt_wait(wait_sec),
                "reason": j.pending_reason or "—",
                "submit_time": _to_iso(st),
            })
            if len(rows) >= limit:
                break

        # 按等待时长降序
        rows.sort(key=lambda x: x["wait_sec"], reverse=True)

        data = {
            "top_pending": rows[:limit],
            "ts": _to_iso(datetime.now(timezone.utc)),
        }

        cache.set(cache_key, data, 15)  # 15s 缓存，与排队列表同口径
        return success_response(data)


@router.get("/cluster/top-users")
def cluster_top_users(
    range: str = Query("7d", regex="^(1d|7d|30d|all)$"),
    metric: str = Query("gpu_hours", regex="^(gpu_hours|job_count|gpu_util|gpu_util_asc)$"),
    limit: int = Query(10, ge=5, le=50)
):
    """全局用户 Top 榜单（总览页用）

    Args:
        range: 时间范围（1d/7d/30d/all）
        metric: 排序指标（gpu_hours=GPU时、job_count=作业数、gpu_util=平均GPU利用率）
        limit: 返回前 N 名

    Returns:
        {
            "range": "7d",
            "metric": "gpu_hours",
            "top_users": [
                {
                    "user": "zhangsan",
                    "gpu_hours": 1234.5,
                    "job_count": 89,
                    "gpu_util_avg": 78.5,
                    "success_rate": 95.2
                }
            ]
        }
    """
    cache_key = f"top_users:{range}:{metric}:{limit}"
    data = cache.get_or_compute(
        cache_key, TTL_TOP_USERS,
        lambda: _load_top_users(range, metric, limit),
    )
    return success_response(data)


def _load_top_users(range: str, metric: str, limit: int) -> Dict[str, Any]:
    """计算 Top 用户榜单（供缓存回源使用）。"""
    range_start = _range_start_naive(range)

    with get_session() as session:
        # 1) 每用户基础统计：GPU 卡时 / 作业数 / 成功数
        #    数据源 job_history（已结束作业持久化表）；jobs 表只保留当前运行/排队作业，
        #    按时间范围统计已结束作业必须用 job_history。
        user_stats_rows = session.execute(text("""
            SELECT
                jh."user",
                SUM(COALESCE(jh.alloc_gpus, 0) * COALESCE(jh.elapsed_seconds, 0)) / 3600.0 AS gpu_hours,
                COUNT(*) AS job_count,
                SUM(CASE WHEN jh.state = 'COMPLETED' THEN 1 ELSE 0 END) AS job_success
            FROM job_history jh
            WHERE (jh.state IN ('COMPLETED', 'FAILED', 'TIMEOUT') OR jh.state LIKE 'CANCELLED%')
              AND (:range_start IS NULL OR jh.submit_time >= :range_start)
              AND jh."user" IS NOT NULL
            GROUP BY jh."user"
        """), {"range_start": range_start}).all()

        if not user_stats_rows:
            return {"range": range, "metric": metric, "top_users": []}

        # 2) 每用户 GPU 利用率加权聚合（拆分为两个查询，避免大表 UNION ALL）
        #    查询 2a：从 job_efficiency_analysis 获取（已分析完成的作业）
        util_analyzed = session.execute(text("""
            SELECT jh."user",
                   SUM(COALESCE(jh.alloc_gpus, 0) * COALESCE(jh.elapsed_seconds, 0) * jea.gpu_util_avg) AS w_sum,
                   SUM(COALESCE(jh.alloc_gpus, 0) * COALESCE(jh.elapsed_seconds, 0)) AS wt_sum
            FROM job_history jh
            JOIN job_efficiency_analysis jea ON jea.job_id = jh.job_id
            WHERE jea.gpu_util_avg IS NOT NULL
              AND (jh.state IN ('COMPLETED', 'FAILED', 'TIMEOUT') OR jh.state LIKE 'CANCELLED%')
              AND (:range_start IS NULL OR jh.submit_time >= :range_start)
              AND jh."user" IS NOT NULL
            GROUP BY jh."user"
        """), {"range_start": range_start}).all()

        #    查询 2b：从 gpu_snapshots 计算（未分析的作业）
        util_unanalyzed = session.execute(text("""
            SELECT jh."user",
                   SUM(COALESCE(jh.alloc_gpus, 0) * COALESCE(jh.elapsed_seconds, 0) * gs_avg.util_avg) AS w_sum,
                   SUM(COALESCE(jh.alloc_gpus, 0) * COALESCE(jh.elapsed_seconds, 0)) AS wt_sum
            FROM job_history jh
            JOIN (
                SELECT job_id, AVG(util_pct) AS util_avg
                FROM gpu_snapshots
                WHERE job_id IS NOT NULL AND util_pct IS NOT NULL
                GROUP BY job_id
            ) gs_avg ON gs_avg.job_id = jh.job_id
            LEFT JOIN job_efficiency_analysis jea ON jea.job_id = jh.job_id
            WHERE jea.job_id IS NULL
              AND (jh.state IN ('COMPLETED', 'FAILED', 'TIMEOUT') OR jh.state LIKE 'CANCELLED%')
              AND (:range_start IS NULL OR jh.submit_time >= :range_start)
              AND jh."user" IS NOT NULL
            GROUP BY jh."user"
        """), {"range_start": range_start}).all()

        # 合并两个查询结果
        util_map = {}
        for rows in [util_analyzed, util_unanalyzed]:
            for r in rows:
                if r.user not in util_map:
                    util_map[r.user] = {"w_sum": 0.0, "wt_sum": 0.0}
                util_map[r.user]["w_sum"] += float(r.w_sum or 0)
                util_map[r.user]["wt_sum"] += float(r.wt_sum or 0)

        # 计算加权利用率
        for user in util_map:
            wt_sum = util_map[user]["wt_sum"]
            util_map[user] = round(util_map[user]["w_sum"] / wt_sum, 1) if wt_sum > 0 else None

        # 3) 组装结果
        top_users = []
        for row in user_stats_rows:
            user = row.user
            job_count = int(row.job_count or 0)
            top_users.append({
                "user": user,
                "gpu_hours": round(float(row.gpu_hours or 0), 1),
                "job_count": job_count,
                "gpu_util_avg": util_map.get(user),
                "success_rate": round(100.0 * (row.job_success or 0) / job_count, 1) if job_count else 0,
            })

        # 4) 排序
        if metric == "gpu_hours":
            top_users.sort(key=lambda x: x["gpu_hours"], reverse=True)
        elif metric == "job_count":
            top_users.sort(key=lambda x: x["job_count"], reverse=True)
        elif metric == "gpu_util":
            # GPU 利用率从高到低，None 排在后面
            top_users.sort(key=lambda x: (x["gpu_util_avg"] if x["gpu_util_avg"] is not None else -1), reverse=True)
        elif metric == "gpu_util_asc":
            # GPU 利用率从低到高，None 排在最后
            top_users.sort(key=lambda x: (x["gpu_util_avg"] if x["gpu_util_avg"] is not None else 9999))

        data = {
            "range": range,
            "metric": metric,
            "top_users": top_users[:limit],
        }

        return data


@router.get("/cluster/summary")
def cluster_summary():
    """集群当前态汇总 - 3.1 接口"""
    cache_key = "cluster_summary"
    data = cache.get_or_compute(cache_key, TTL_SUMMARY, _load_cluster_summary)
    return success_response(data)


def _load_cluster_summary() -> Dict[str, Any]:
    """计算集群当前态汇总（供缓存回源使用）。"""
    with get_session() as session:
        # 获取最新快照时间点
        latest = session.query(NodeSnapshot).order_by(desc(NodeSnapshot.collected_at)).first()
        if not latest:
            return {
                "ts": _to_iso(datetime.now(timezone.utc)),
                "gpu": {"total": 0, "used": 0, "utilization_avg": 0},
                "cpu": {"total": 0, "used": 0},
                "mem": {"total_gb": 0, "used_gb": 0},
                "jobs": {"running": 0, "pending": 0},
                "nodes": {"total": 0, "idle": 0, "mixed": 0, "allocated": 0, "down": 0},
            }

        # 同一时间点的所有节点快照
        snapshots = session.query(NodeSnapshot).filter(
            NodeSnapshot.collected_at == latest.collected_at
        ).all()

        # 汇总 GPU/CPU/内存
        total_gpu = sum(s.gres_total or 0 for s in snapshots)
        used_gpu = sum(s.gres_alloc or 0 for s in snapshots)
        total_cpu = sum(s.total_cpus or 0 for s in snapshots)
        used_cpu = sum(s.alloc_cpus or 0 for s in snapshots)

        # GPU 平均利用率：各节点最近一帧里有进程占用的卡的平均值（帧超过 5 分钟视为过期）
        util_row = session.execute(text("""
            SELECT AVG(g.util_pct) AS util
            FROM gpu_snapshots g
            JOIN (SELECT node_name, MAX(collected_at) AS m FROM gpu_snapshots GROUP BY node_name) x
              ON g.node_name = x.node_name AND g.collected_at = x.m
            WHERE x.m > (NOW() AT TIME ZONE 'UTC') - INTERVAL '5 minutes'
              AND g.proc_json IS NOT NULL AND g.proc_json <> '[]'
        """)).fetchone()
        gpu_util_avg = round(float(util_row.util), 1) if util_row and util_row.util is not None else None

        # 内存：有节点实测的用实测，否则回退 Slurm 分配值
        # 一次批量取所有节点最近一条主机指标，避免逐节点查询
        from ..storage.models import NodeHostStat
        host_rows = session.query(NodeHostStat).filter(
            NodeHostStat.node_name.in_([s.node_name for s in snapshots]),
        ).order_by(NodeHostStat.node_name, desc(NodeHostStat.collected_at)).all()
        host_map = {}
        for h in host_rows:
            if h.node_name in host_map:
                continue
            if (_seconds_since(h.collected_at) or 1e9) <= HOST_STAT_FRESH_SECONDS:
                host_map[h.node_name] = h

        total_mem_mb = used_mem_mb = 0
        for s in snapshots:
            host = host_map.get(s.node_name)
            if host and host.mem_total_mb and host.mem_used_mb is not None:
                total_mem_mb += host.mem_total_mb
                used_mem_mb += host.mem_used_mb
            else:
                total_mem_mb += s.total_memory_mb or 0
                used_mem_mb += s.alloc_memory_mb or 0

        # 作业统计（一次分组查询，避免两次 count）
        job_counts = dict(
            session.query(Job.state, func.count(Job.id))
            .filter(Job.state.in_(["RUNNING", "PENDING"]))
            .group_by(Job.state).all()
        )
        running_jobs = job_counts.get("RUNNING", 0)
        pending_jobs = job_counts.get("PENDING", 0)

        # 节点状态统计
        node_states = {"idle": 0, "mixed": 0, "allocated": 0, "down": 0}
        for s in snapshots:
            state = (s.state or "unknown").lower()
            if "down" in state or "drain" in state:
                node_states["down"] += 1
            elif state == "idle":
                node_states["idle"] += 1
            elif state == "allocated":
                node_states["allocated"] += 1
            elif state == "mixed":
                node_states["mixed"] += 1

        data = {
            "ts": _to_iso(latest.collected_at),
            "gpu": {
                "total": total_gpu,
                "used": used_gpu,
                "utilization_avg": gpu_util_avg,
            },
            "cpu": {
                "total": total_cpu,
                "used": used_cpu,
            },
            "mem": {
                "total_gb": round(total_mem_mb / 1024, 1),
                "used_gb": round(used_mem_mb / 1024, 1),
            },
            "jobs": {
                "running": running_jobs,
                "pending": pending_jobs,
            },
            "nodes": {
                "total": len(snapshots),
                **node_states,
            },
        }

        return data


@router.get("/cluster/trend")
def cluster_trend(
    range: str = Query("5m", regex="^(1m|5m|15m|1h|1d)$"),
    date: Optional[str] = None,
    metrics: Optional[str] = None,
):
    """集群级历史趋势 - 3.2 接口"""
    cache_key = f"cluster_trend:{range}:{date}:{metrics}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    selected_metrics = (metrics or "gpu_used,gpu_util,job_running,job_pending").split(",")
    target_date = date or datetime.now(timezone.utc).strftime("%Y-%m-%d")

    with get_session() as session:
        from ..storage.models import ClusterAggStat

        # 查询指定日期的聚合数据
        try:
            date_start = datetime.fromisoformat(target_date).replace(tzinfo=timezone.utc)
            date_end = date_start + timedelta(days=1)
        except ValueError:
            date_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            date_end = date_start + timedelta(days=1)

        rows = session.query(ClusterAggStat).filter(
            ClusterAggStat.granularity == range,
            ClusterAggStat.bucket_ts >= date_start,
            ClusterAggStat.bucket_ts < date_end
        ).order_by(ClusterAggStat.bucket_ts).all()

        times = []
        metric_data = {m: [] for m in selected_metrics}

        for row in rows:
            # 格式化时间
            local_tz = timezone(timedelta(hours=8))
            local_time = row.bucket_ts.astimezone(local_tz)
            time_str = local_time.strftime("%H:%M") if range != "1d" else local_time.strftime("%m-%d")
            times.append(time_str)

            # 提取指标
            if "gpu_used" in metric_data:
                metric_data["gpu_used"].append(row.gpu_used or 0)
            if "gpu_util" in metric_data:
                metric_data["gpu_util"].append(round(row.gpu_util_avg, 1) if row.gpu_util_avg else 0)
            if "job_running" in metric_data:
                metric_data["job_running"].append(row.job_running or 0)
            if "job_pending" in metric_data:
                metric_data["job_pending"].append(row.job_pending or 0)

    data = {
        "range": range,
        "date": target_date,
        "times": times,
        **metric_data
    }

    cache.set(cache_key, data, TTL_TREND)
    return success_response(data)


@router.get("/nodes")
def nodes_list(
    partition: Optional[str] = None,
    state: Optional[str] = None,
    sort: str = Query("gpu_util", regex="^(gpu_util|load|pending|name)$"),
    trend_range: str = Query("1h", regex="^(1h|1d)$"),
):
    """节点列表 - 3.3 接口（优化版：批量查询避免 N+1）"""
    # 缓存键不含 sort：排序在缓存数据上做，多个用户排序/刷新间隔不同也复用同一份查询结果
    cache_key = f"nodes:{partition}:{state}:{trend_range}"

    def load_nodes() -> List[Dict[str, Any]]:
        return _load_nodes(partition, state, trend_range)

    nodes_data = cache.get_or_compute(cache_key, TTL_NODES, load_nodes)
    return success_response(_sort_nodes(nodes_data, sort))


def _load_nodes(partition: Optional[str], state: Optional[str], trend_range: str) -> List[Dict[str, Any]]:
    """查询节点列表原始数据（不含排序），供缓存回源使用。"""
    with get_session() as session:
        # 获取最新快照
        latest = session.query(NodeSnapshot).order_by(desc(NodeSnapshot.collected_at)).first()
        if not latest:
            return []

        snapshots = session.query(NodeSnapshot).filter(
            NodeSnapshot.collected_at == latest.collected_at
        ).all()

        # 预过滤节点（避免对不需要的节点执行批量查询）
        filtered_snapshots = []
        for s in snapshots:
            # 分区过滤
            if partition and partition != "all" and (s.partition or "") != partition:
                continue
            # 状态过滤
            node_state = (s.state or "").lower()
            if state and state != "all":
                state_filter = state.split(",")
                if not any(sf.lower() in node_state for sf in state_filter):
                    continue
            filtered_snapshots.append(s)

        if not filtered_snapshots:
            return success_response([])

        node_names = [s.node_name for s in filtered_snapshots]
        gpu_node_names = [s.node_name for s in filtered_snapshots if s.gres_total and s.gres_total > 0]

        # ========== 批量查询 1：最新 GPU 帧（所有 GPU 节点）==========
        # 语义同 _latest_gpu_frame：每节点取最近一帧（该时刻的全部卡），新鲜度由调用方判断
        gpu_frames_map = {}  # node_name -> {"age_seconds": ..., "gpus": [...]}
        if gpu_node_names:
            gpu_frames_query = text("""
                WITH latest AS (
                    SELECT node_name, max(collected_at) AS ts
                    FROM gpu_snapshots
                    WHERE node_name = ANY(:nodes)
                    GROUP BY node_name
                )
                SELECT g.node_name, g.gpu_index, g.util_pct, g.proc_json,
                       EXTRACT(EPOCH FROM ((NOW() AT TIME ZONE 'UTC') - g.collected_at)) AS age_seconds
                FROM gpu_snapshots g
                JOIN latest l ON l.node_name = g.node_name AND l.ts = g.collected_at
                ORDER BY g.node_name, g.gpu_index
            """)
            gpu_frames_raw = session.execute(gpu_frames_query, {"nodes": gpu_node_names}).all()
            for row in gpu_frames_raw:
                frame = gpu_frames_map.setdefault(
                    row.node_name, {"age_seconds": float(row.age_seconds or 0), "gpus": []}
                )
                try:
                    procs = json.loads(row.proc_json) if row.proc_json else []
                except ValueError:
                    procs = []
                frame["gpus"].append({"util_pct": row.util_pct, "procs": procs})

        # ========== 批量查询 2：GPU 趋势（所有 GPU 节点）==========
        trend_map = {}  # node_name -> {ago: value}
        if gpu_node_names:
            bucket, count = (300, 12) if trend_range == "1h" else (3600, 24)
            span = bucket * count
            trend_query = text("""
                SELECT node_name,
                       floor(extract(epoch FROM ((NOW() AT TIME ZONE 'UTC') - collected_at)) / :b)::int AS ago,
                       avg(util_pct) AS v
                FROM gpu_snapshots
                WHERE node_name = ANY(:nodes)
                  AND util_pct IS NOT NULL
                  AND collected_at > (NOW() AT TIME ZONE 'UTC') - make_interval(secs => :span)
                GROUP BY node_name, ago
            """)
            trend_rows = session.execute(trend_query, {
                "nodes": gpu_node_names, "b": bucket, "span": span
            }).all()
            for row in trend_rows:
                if row.node_name not in trend_map:
                    trend_map[row.node_name] = {}
                trend_map[row.node_name][row.ago] = round(float(row.v), 1)

        # ========== 批量查询 3：主机状态（所有节点，每节点取最新一条）==========
        from ..storage.models import NodeHostStat
        host_rows = session.query(NodeHostStat).filter(
            NodeHostStat.node_name.in_(node_names),
        ).order_by(NodeHostStat.node_name, desc(NodeHostStat.collected_at)).all()
        host_map = {}
        for h in host_rows:
            if h.node_name in host_map:
                continue  # 已取到该节点最新一条
            if (_seconds_since(h.collected_at) or 1e9) <= HOST_STAT_FRESH_SECONDS:
                host_map[h.node_name] = h

        # ========== 批量查询 4：待排队作业（一次取全量分组，Python 侧按节点归属）==========
        # 语义同 _node_pending_filter：node_list 里出现该节点名，或未指定节点且分区相同
        pending_rows = session.execute(text("""
            SELECT COALESCE(j.partition, '') AS partition, COALESCE(j.node_list, '') AS node_list, COUNT(*) AS cnt
            FROM jobs j
            WHERE j.state = 'PENDING'
            GROUP BY j.partition, j.node_list
        """)).all()
        pending_groups = [
            (r.partition, r.node_list.lower(), int(r.cnt))
            for r in pending_rows
        ]

        # ========== 主循环：组装数据（无数据库查询）==========
        nodes_data = []
        bucket_count = 12 if trend_range == "1h" else 24
        for s in filtered_snapshots:
            node_state = (s.state or "").lower()

            # GPU 利用率
            gpu_util = None
            if s.gres_total and s.gres_total > 0:
                frame = gpu_frames_map.get(s.node_name)
                if frame and frame["age_seconds"] <= HOST_STAT_FRESH_SECONDS:
                    occupied = [g["util_pct"] for g in frame["gpus"] if g["procs"] and g["util_pct"] is not None]
                    gpu_util = round(sum(occupied) / len(occupied)) if occupied else 0

            # GPU 趋势
            trend_data = []
            if s.node_name in trend_map:
                by_ago = trend_map[s.node_name]
                trend_data = [by_ago.get(i) for i in range(bucket_count - 1, -1, -1) if i in by_ago]

            # 主机状态
            host = host_map.get(s.node_name)

            # 排队作业数：node_list 里出现该节点名，或未指定节点且分区相同（与 _node_pending_filter 等价）
            lname = (s.node_name or "").lower()
            pending_jobs = 0
            for p_part, p_nodes, p_cnt in pending_groups:
                if p_nodes:
                    if lname in p_nodes:
                        pending_jobs += p_cnt
                elif s.partition and p_part == s.partition:
                    pending_jobs += p_cnt

            # 节点状态归一化
            if "down" in node_state or "drain" in node_state:
                state_norm = "down"
            elif node_state == "idle":
                state_norm = "idle"
            elif node_state == "allocated":
                state_norm = "allocated"
            else:
                state_norm = "mixed"

            nodes_data.append({
                "name": s.node_name,
                "partition": s.partition,
                "state": state_norm,
                "gpu": {
                    "total": s.gres_total or 0,
                    "used": s.gres_alloc or 0,
                    "util_pct": gpu_util,  # 占用卡的平均利用率，未采集到为 None
                },
                "cpu": {
                    "total": s.total_cpus or 0,
                    "used": s.alloc_cpus or 0,
                    "util_pct": host.cpu_util_pct if host else None,
                },
                # 内存优先用节点实测，没有时回退 Slurm 分配值（该集群 AllocMem 常为 0）
                "mem": {
                    "total_gb": round(((host.mem_total_mb if host and host.mem_total_mb else s.total_memory_mb) or 0) / 1024, 1),
                    "used_gb": round(((host.mem_used_mb if host and host.mem_used_mb is not None else s.alloc_memory_mb) or 0) / 1024, 1),
                },
                # 负载 = 1 分钟 load average / 核数 × 100（Slurm 的 cpu_load 不是百分比，不能直接用）
                "load": round(host.load1 / host.cpu_count * 100, 1) if host and host.load1 is not None and host.cpu_count else None,
                "pending_jobs": pending_jobs,
                "trend": trend_data,
            })

        return nodes_data


def _sort_nodes(nodes_data: List[Dict[str, Any]], sort: str) -> List[Dict[str, Any]]:
    """按指定维度排序节点列表（在缓存副本上操作，不改动缓存里的原始顺序）。"""
    result = list(nodes_data)
    if sort == "gpu_util":
        result.sort(
            key=lambda n: (n["gpu"]["util_pct"] if n["gpu"]["util_pct"] is not None else -1,
                           n["gpu"]["used"] / max(n["gpu"]["total"], 1)),
            reverse=True,
        )
    elif sort == "load":
        result.sort(key=lambda n: n["load"] if n["load"] is not None else -1, reverse=True)
    elif sort == "pending":
        result.sort(key=lambda n: n["pending_jobs"], reverse=True)
    elif sort == "name":
        result.sort(key=lambda n: n["name"])
    return result


# ==================== 四、节点详情页 ====================

@router.get("/node/{name}")
def node_info(name: str):
    """节点基本信息 - 4.1 接口"""
    cache_key = f"node_info:{name}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        snap = session.query(NodeSnapshot).filter(
            NodeSnapshot.node_name == name
        ).order_by(desc(NodeSnapshot.collected_at)).first()

        if not snap:
            return error_response(40401, f"节点 {name} 不存在")

        running = session.query(Job).filter(
            Job.state == "RUNNING",
            Job.node_list.ilike(f"%{name}%")
        ).count()

        pending = session.query(Job).filter(
            Job.state == "PENDING",
            _node_pending_filter(snap.partition, name),
        ).count()

        host = _fresh_host_stat(session, name)
        frame = _latest_gpu_frame(session, name)
        occupied = [g["util_pct"] for g in (frame["gpus"] if frame else []) if g["procs"] and g["util_pct"] is not None]

        state_norm = (snap.state or "").lower()
        if "down" in state_norm or "drain" in state_norm:
            state_norm = "down"
        elif state_norm == "idle":
            state_norm = "idle"
        elif state_norm == "allocated":
            state_norm = "allocated"
        else:
            state_norm = "mixed"

        data = {
            "name": name,
            "partition": snap.partition,
            "state": state_norm,
            "gpu": {
                "total": snap.gres_total or 0,
                "used": snap.gres_alloc or 0,
                # 仅统计有进程占用的卡，与总览口径一致；未采集到时为 None
                "utilization_avg": round(sum(occupied) / len(occupied)) if occupied else None,
            },
            "cpu": {
                "total": snap.total_cpus or 0,
                "used": snap.alloc_cpus or 0,
                "util_pct": host.cpu_util_pct if host else None,
                "load1": host.load1 if host else None,
            },
            # 内存优先用节点实测（agent），没有实测时回退到 Slurm 分配值（该集群 AllocMem 常为 0）
            "mem": {
                "total_gb": round(((host.mem_total_mb if host and host.mem_total_mb else snap.total_memory_mb) or 0) / 1024, 1),
                "used_gb": round(((host.mem_used_mb if host and host.mem_used_mb is not None else snap.alloc_memory_mb) or 0) / 1024, 1),
                "source": "host" if host and host.mem_used_mb is not None else "slurm",
            },
            "job_count": {
                "running": running,
                "pending": pending,
            },
        }

        cache.set(cache_key, data, TTL_NODE_DETAIL)
        return success_response(data)


# 趋势指标 → (表, SQL 聚合表达式)。GPU 指标来自 gpu_snapshots，主机指标来自 node_host_stats
_TREND_METRICS = {
    "gpu_util": ("gpu_snapshots", "avg(util_pct)"),
    "gpu_mem":  ("gpu_snapshots", "sum(mem_used_mb) * 100.0 / nullif(sum(mem_total_mb), 0)"),
    "gpu_power": ("gpu_snapshots", "avg(power_w)"),
    "gpu_temp": ("gpu_snapshots", "avg(temp_c)"),
    "load":     ("node_host_stats", "avg(load1 * 100.0 / nullif(cpu_count, 0))"),   # 1 分钟负载 / 核数
    "mem":      ("node_host_stats", "avg(mem_used_mb * 100.0 / nullif(mem_total_mb, 0))"),
    "cpu_util": ("node_host_stats", "avg(cpu_util_pct)"),
}
_TREND_BUCKETS = {"1m": 60, "5m": 300, "15m": 900, "1h": 3600, "1d": 3600}  # 1d：按天对比，用小时点


def _local_day_bounds_utc(day: str) -> tuple[datetime, datetime]:
    """本地时区（settings.timezone）自然日 [00:00, 24:00) 对应的 naive UTC 区间。"""
    tz = ZoneInfo(settings.timezone)
    start_local = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=tz)
    start = start_local.astimezone(timezone.utc).replace(tzinfo=None)
    return start, start + timedelta(days=1)


@router.get("/node/{name}/trend")
def node_trend(
    name: str,
    granularity: str = Query("5m", alias="range", pattern="^(1m|5m|15m|1h|1d)$"),  # 不能直接叫 range：会遮住内置 range()
    date_a: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),
    date_b: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    metrics: Optional[str] = None,
    gpu: Optional[str] = Query(None, description="GPU 卡序号，缺省/all 表示全部卡（仅影响 GPU 指标）"),
):
    """节点历史曲线 - 4.2 接口（按本地自然日切片，支持两天叠加对比）

    返回 times（一天内的时间轴 HH:MM）和 series[指标][日期] = 与 times 对齐的数组，无数据的点为 null。
    """
    selected = [m for m in (metrics or "gpu_util,gpu_mem,load,mem").split(",") if m in _TREND_METRICS]
    if not selected:
        return error_response(40001, f"metrics 取值范围: {','.join(_TREND_METRICS)}")
    gpu_idx = int(gpu) if gpu and gpu.isdigit() else None
    cache_key = f"node_trend:{name}:{granularity}:{date_a}:{date_b}:{','.join(selected)}:{gpu_idx}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    bucket = _TREND_BUCKETS[granularity]
    n_points = 86400 // bucket
    times = [f"{(i * bucket) // 3600:02d}:{((i * bucket) % 3600) // 60:02d}" for i in range(n_points)]
    days = [d for d in (date_a, date_b) if d]
    series: Dict[str, Dict[str, list]] = {m: {} for m in selected}

    try:
        bounds = {d: _local_day_bounds_utc(d) for d in days}
    except ValueError:
        return error_response(40001, "日期格式应为 YYYY-MM-DD")

    with get_session() as session:
        for m in selected:
            table, expr = _TREND_METRICS[m]
            card_filter = " AND gpu_index = :gpu" if table == "gpu_snapshots" and gpu_idx is not None else ""
            for d in days:
                start, end = bounds[d]
                params = {"n": name, "s": start, "e": end, "b": bucket, "gpu": gpu_idx}
                rows = session.execute(text(f"""
                    SELECT floor(extract(epoch FROM (collected_at - :s)) / :b)::int AS bk, {expr} AS v
                    FROM {table}
                    WHERE node_name = :n AND collected_at >= :s AND collected_at < :e{card_filter}
                    GROUP BY bk
                """), params).all()
                values: list = [None] * n_points
                for bk, v in rows:
                    if v is not None and 0 <= bk < n_points:
                        values[bk] = round(float(v), 1)
                series[m][d] = values

    data = {"range": granularity, "bucket_seconds": bucket, "dates": days, "times": times, "series": series}
    cache.set(cache_key, data, TTL_TREND)
    return success_response(data)


_USER_RANGE_DAYS = {"1d": 1, "7d": 7, "30d": 30, "90d": 90}


def _range_start_naive(range_: Optional[str]) -> Optional[datetime]:
    """时间范围起点（naive UTC，与库对齐）；all/None 表示不限。"""
    days = _USER_RANGE_DAYS.get(range_ or "")
    return (datetime.now(timezone.utc) - timedelta(days=days)).replace(tzinfo=None) if days else None


def _job_label(j) -> str:
    """作业显示号：数组子任务显示为 父作业号_子任务号（如 141_54），其余为作业号。"""
    if getattr(j, "array_job_id", None) and getattr(j, "array_task_id", None) is not None:
        return f"{j.array_job_id}_{j.array_task_id}"
    return str(j.job_id)


# 不参与调度排序的排队原因（依赖未满足/被挂起/定时启动），这类作业没有有意义的排队位次
_UNSCHEDULABLE_REASONS = {
    "Dependency", "DependencyNeverSatisfied", "JobHeldUser", "JobHeldAdmin", "BeginTime", "JobArrayTaskLimit",
}


def _pending_rank_map(session) -> Dict[int, int]:
    """排队位次：同分区内按优先级降序、提交时间升序排名，从 1 开始。

    只统计可被调度的作业；优先级尚未采集到时排在最后。
    """
    rows = session.query(
        Job.job_id, Job.partition, Job.priority, Job.submit_time, Job.pending_reason
    ).filter(Job.state == "PENDING").all()
    by_part: Dict[Optional[str], list] = {}
    for r in rows:
        if r.pending_reason in _UNSCHEDULABLE_REASONS:
            continue
        by_part.setdefault(r.partition, []).append(r)
    ranks: Dict[int, int] = {}
    for items in by_part.values():
        items.sort(key=lambda r: (-(r.priority or 0), r.submit_time or datetime.max))
        for i, r in enumerate(items, 1):
            ranks[r.job_id] = i
    return ranks


HOST_STAT_FRESH_SECONDS = 300  # 主机指标超过 5 分钟没更新视为过期，回退到 Slurm 分配值


def _fresh_host_stat(session, node_name: str):
    """节点最近一条主机指标（agent 采集）；没有或已过期返回 None。"""
    from ..storage.models import NodeHostStat
    row = session.query(NodeHostStat).filter(
        NodeHostStat.node_name == node_name
    ).order_by(desc(NodeHostStat.collected_at)).first()
    if row and (_seconds_since(row.collected_at) or 1e9) <= HOST_STAT_FRESH_SECONDS:
        return row
    return None


def _node_pending_filter(partition: Optional[str], name: str):
    """可能落在该节点的排队作业：明确指定了该节点，或没指定节点且分区相同。"""
    from sqlalchemy import or_, and_
    cond = Job.node_list.ilike(f"%{name}%")
    if partition:
        cond = or_(cond, and_(or_(Job.node_list.is_(None), Job.node_list == ""), Job.partition == partition))
    return cond


def _gpu_job_stats(session, job_ids) -> Dict[int, Dict[str, float]]:
    """按作业号汇总 GPU 实测：平均利用率、单卡显存峰值（来自 gpu_snapshots.job_id，agent 解析）。"""
    ids = [int(i) for i in job_ids if i]
    if not ids:
        return {}
    rows = session.execute(text("""
        SELECT job_id, AVG(util_pct) AS util, MAX(mem_used_mb) AS mem_peak
        FROM gpu_snapshots
        WHERE job_id = ANY(:ids) AND util_pct IS NOT NULL
        GROUP BY job_id
    """), {"ids": ids}).all()
    return {
        r.job_id: {
            "util": round(float(r.util), 1),
            "mem_peak_gb": round(float(r.mem_peak) / 1024, 1) if r.mem_peak is not None else None,
        }
        for r in rows
    }


def _latest_gpu_frame(session, node_name: str) -> Optional[Dict[str, Any]]:
    """取某节点最近一帧 GPU 快照（web 与 rgtop 共用）。

    返回 {node, collected_at, age_seconds, gpus: [...]}，无数据返回 None。
    """
    latest = session.query(func.max(GpuSnapshot.collected_at)).filter(
        GpuSnapshot.node_name == node_name
    ).scalar()
    if latest is None:
        return None

    rows = session.query(GpuSnapshot).filter(
        GpuSnapshot.node_name == node_name,
        GpuSnapshot.collected_at == latest,
    ).order_by(GpuSnapshot.gpu_index).all()

    gpus = []
    for g in rows:
        try:
            procs = json.loads(g.proc_json) if g.proc_json else []
        except ValueError:
            procs = []
        gpus.append({
            "idx": g.gpu_index,
            "uuid": g.gpu_uuid,
            "name": g.gpu_name,
            "util_pct": g.util_pct,
            "mem_used_mb": g.mem_used_mb,
            "mem_total_mb": g.mem_total_mb,
            "temp_c": g.temp_c,
            "power_w": g.power_w,
            "power_limit_w": g.power_limit_w,
            "job_id": int(g.job_id) if g.job_id else None,  # 该卡所属 Slurm 作业（agent 经 slurmstepd 解析）
            "procs": procs,
        })
    host = _fresh_host_stat(session, node_name)
    return {
        "node": node_name,
        "collected_at": _to_iso(latest),
        "age_seconds": round(_seconds_since(latest) or 0, 1),
        "gpus": gpus,
        # 主机指标（CPU/内存/负载），节点没有主机数据或已过期时为 null
        "host": {
            "cpu_count": host.cpu_count,
            "cpu_util_pct": host.cpu_util_pct,
            "load1": host.load1, "load5": host.load5, "load15": host.load15,
            "mem_total_mb": host.mem_total_mb,
            "mem_used_mb": host.mem_used_mb,
        } if host else None,
    }


@router.get("/node/{name}/gpus")
def node_gpus(name: str):
    """GPU 明细表 - 4.3 接口（数据来自 gpu_snapshots，agent.py 采集）"""
    cache_key = f"node_gpus:{name}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        frame = _latest_gpu_frame(session, name)
        if not frame:
            return success_response([])

        # 卡所属作业的已运行时长
        used_time: Dict[int, str] = {}
        job_ids = {g["job_id"] for g in frame["gpus"] if g["job_id"]}
        if job_ids:
            for j in session.query(Job).filter(Job.job_id.in_(job_ids), Job.state == "RUNNING").all():
                elapsed = _seconds_since(j.start_time) if j.start_time else None
                if elapsed is not None:
                    used_time[j.job_id] = f"{int(elapsed // 3600):02d}:{int(elapsed % 3600 // 60):02d}:{int(elapsed % 60):02d}"

        gpu_data = []
        for g in frame["gpus"]:
            # 空闲：利用率 <1% 且没有进程占用
            idle = (g["util_pct"] or 0) < 1 and not g["procs"]
            users = sorted({p["user"] for p in g["procs"] if p.get("user")})
            gpu_data.append({
                "gpu_id": g["idx"],
                "state": "idle" if idle else "used",
                "job_id": g["job_id"],
                "user": ",".join(users) or None,
                "used_time": used_time.get(g["job_id"]) if g["job_id"] else None,
                "name": g["name"],
                "util_pct": g["util_pct"],
                "mem_used_mb": g["mem_used_mb"],
                "mem_total_mb": g["mem_total_mb"],
                "temp_c": g["temp_c"],
                "power_w": g["power_w"],
                "power_limit_w": g["power_limit_w"],
                "procs": g["procs"],
                "collected_at": frame["collected_at"],
                "age_seconds": frame["age_seconds"],
            })

        cache.set(cache_key, gpu_data, TTL_GPU_LIVE)
        return success_response(gpu_data)


@router.get("/gpu-live")
def gpu_live(node: Optional[str] = Query(None, description="只返回指定节点，逗号分隔")):
    """全部（或指定）节点最近一帧 GPU 数据，供 rgtop 等命令行工具读取，无需 SSH 到算力节点。"""
    wanted = [n.strip() for n in node.split(",") if n.strip()] if node else None
    cache_key = f"gpu_live:{','.join(sorted(wanted)) if wanted else '*'}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        names = wanted or [
            r[0] for r in session.query(GpuSnapshot.node_name).distinct().order_by(GpuSnapshot.node_name).all()
        ]
        frames = [f for f in (_latest_gpu_frame(session, n) for n in names) if f]

    data = {"nodes": frames}
    cache.set(cache_key, data, TTL_GPU_LIVE)
    return success_response(data)


@router.get("/gpu-live/{name}")
def gpu_live_node(name: str):
    """单节点最近一帧 GPU 数据。"""
    with get_session() as session:
        frame = _latest_gpu_frame(session, name)
    if not frame:
        raise HTTPException(status_code=404, detail=f"no GPU data for node {name}")
    return success_response(frame)


# ==================== rgtop 客户端接口 ====================

def _to_epoch(dt: Optional[datetime]) -> Optional[int]:
    """转 Unix 秒。数据库里存的是 naive UTC，按 UTC 解释。"""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _rgtop_node_list(session, wanted: Optional[List[str]]) -> List[str]:
    """要返回的节点：显式指定的原样返回；否则取启用节点 ∪ 有 GPU 数据的节点。

    并上 node_ip_map 的启用节点，是为了让「掉线但曾采集过」的节点仍出现在列表里
    并带 error，而不是静默消失（rgtop 会把后者当成节点被删除）。
    """
    if wanted:
        return wanted

    names = {r[0] for r in session.execute(
        text("SELECT DISTINCT node_name FROM gpu_snapshots")
    ).all()}
    try:
        names |= {r[0] for r in session.execute(
            text("SELECT node_name FROM node_ip_map WHERE enabled = 1")
        ).all()}
    except Exception:  # node_ip_map 不存在时退回只看快照
        logger.debug("node_ip_map unavailable, listing nodes from gpu_snapshots only")
    return sorted(names)


def _rgtop_gpu_rows(session, nodes: List[str], all_nodes: bool):
    """一次取回所有节点各自的最新一帧（DISTINCT ON + JOIN，避免按节点 N+1）。"""
    sql = """
        WITH latest AS (
            SELECT DISTINCT ON (node_name) node_name, collected_at
            FROM gpu_snapshots
            {where}
            ORDER BY node_name, collected_at DESC
        )
        SELECT g.node_name, g.gpu_index, g.gpu_uuid, g.gpu_name, g.job_id,
               g.util_pct, g.mem_used_mb, g.mem_total_mb,
               g.power_w, g.power_limit_w, g.temp_c, g.proc_json, g.collected_at
        FROM gpu_snapshots g
        JOIN latest l ON g.node_name = l.node_name AND g.collected_at = l.collected_at
        ORDER BY g.node_name, g.gpu_index
    """
    if all_nodes:
        return session.execute(text(sql.format(where=""))).all()
    return session.execute(
        text(sql.format(where="WHERE node_name = ANY(:nodes)")), {"nodes": nodes}
    ).all()


def _rgtop_host_rows(session, nodes: List[str], all_nodes: bool) -> Dict[str, Any]:
    """每节点最近一条主机指标，一次查询（DISTINCT ON，走 (node_name, collected_at DESC) 索引）。"""
    sql = """
        SELECT DISTINCT ON (node_name)
               node_name, cpu_count, cpu_util_pct, load1, load5, load15,
               mem_total_mb, mem_used_mb, mem_avail_mb, mem_cache_mb,
               swap_total_mb, swap_free_mb, cpu_cores, cpu_model, sockets,
               cpu_temp_c, uptime_s, collected_at
        FROM node_host_stats
        {where}
        ORDER BY node_name, collected_at DESC
    """
    try:
        if all_nodes:
            rows = session.execute(text(sql.format(where=""))).all()
        else:
            rows = session.execute(
                text(sql.format(where="WHERE node_name = ANY(:nodes)")), {"nodes": nodes}
            ).all()
    except Exception:  # 旧库还没迁移出新列时，主机段整体降级为 null
        logger.warning("node_host_stats query failed, host section omitted", exc_info=True)
        return {}
    return {r.node_name: r for r in rows}


def _rgtop_host_payload(h) -> Dict[str, Any]:
    """主机指标 → rgtop 的 CPU 行 / 每核条带 / 内存细分所需的形状。"""
    return {
        "cpu_count": h.cpu_count,
        "cpu_util_pct": h.cpu_util_pct,
        "load1": h.load1, "load5": h.load5, "load15": h.load15,
        "mem_total_mb": h.mem_total_mb,
        "mem_used_mb": h.mem_used_mb,
        "mem_avail_mb": h.mem_avail_mb,
        "mem_cache_mb": h.mem_cache_mb,
        "swap_total_mb": h.swap_total_mb,
        "swap_free_mb": h.swap_free_mb,
        "cpu_cores": h.cpu_cores,       # base36，每位 0..35 对应 0..100%
        "cpu_model": h.cpu_model,
        "sockets": h.sockets,
        "cpu_temp_c": h.cpu_temp_c,
        "uptime_s": h.uptime_s,
        "collected_at": _to_iso(h.collected_at),
        "collected_at_epoch": _to_epoch(h.collected_at),
        "age_seconds": round(_seconds_since(h.collected_at) or 0, 1),
    }


@router.get("/rgtop/frame")
def rgtop_frame(
    node: Optional[str] = Query(None, description="只返回指定节点，逗号分隔；不传则返回全部"),
    procs: int = Query(1, ge=0, le=1, description="1=带每卡进程列表；0=省略（省带宽）"),
):
    """rgtop 用的批量帧：一次返回多个节点各自的最新帧，供 HTTP 后端替代 SSH 采集。

    与 /api/gpu-live 的区别（后者保持不变，Web 前端在用）：
    - 批量查询（DISTINCT ON），不是按节点 N+1
    - procs 开关：rgtop 关掉 USER 列/进程面板时不需要进程数据
    - 每个节点独立的 error 字段，区分「无数据」「节点故障」，客户端据此画错误卡片
    - collected_at_epoch：客户端可据此自己外推数据年龄（服务端算的 age_seconds 会随时间失真）
    """
    wanted = [n.strip() for n in node.split(",") if n.strip()] if node else None
    cache_key = f"rgtop_frame:{','.join(sorted(wanted)) if wanted else '*'}:{procs}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        nodes = _rgtop_node_list(session, wanted)
        gpu_rows = _rgtop_gpu_rows(session, nodes, all_nodes=not wanted)
        host_rows = _rgtop_host_rows(session, nodes, all_nodes=not wanted)

    by_node: Dict[str, List[Any]] = {}
    for r in gpu_rows:
        by_node.setdefault(r.node_name, []).append(r)

    frames = []
    for name in nodes:
        rows = by_node.get(name, [])
        gpus = []
        for g in rows:
            gpu = {
                "idx": g.gpu_index,
                "uuid": g.gpu_uuid,
                "name": g.gpu_name,
                "util_pct": g.util_pct,
                "mem_used_mb": g.mem_used_mb,
                "mem_total_mb": g.mem_total_mb,
                "temp_c": g.temp_c,
                "power_w": g.power_w,
                "power_limit_w": g.power_limit_w,
                "job_id": int(g.job_id) if g.job_id else None,
            }
            if procs:
                try:
                    gpu["procs"] = json.loads(g.proc_json) if g.proc_json else []
                except ValueError:
                    gpu["procs"] = []
            gpus.append(gpu)

        # 节点级错误：rgtop 用它区分「这台机器没数据」和「服务端出问题」
        error = None
        if not rows:
            error = "no GPU data collected for this node"
        elif all(g["util_pct"] is None and g["mem_used_mb"] is None for g in gpus):
            error = "agent collected no usable GPU metrics"

        frames.append({
            "node": name,
            "error": error,
            "collected_at": _to_iso(rows[0].collected_at) if rows else None,
            "collected_at_epoch": _to_epoch(rows[0].collected_at) if rows else None,
            "age_seconds": round(_seconds_since(rows[0].collected_at) or 0, 1) if rows else None,
            "gpus": gpus,
            "host": _rgtop_host_payload(host_rows[name]) if name in host_rows else None,
        })

    data = {
        "server": "slurm-monitor",
        "generated_at": _to_iso(_now_utc()),
        "generated_at_epoch": _to_epoch(_now_utc()),
        "procs": bool(procs),
        "nodes": frames,
    }
    cache.set(cache_key, data, TTL_RGTOP_FRAME)
    return success_response(data)


@router.get("/rgtop/frame/{name}")
def rgtop_frame_node(name: str, procs: int = Query(1, ge=0, le=1)):
    """单节点版本；节点没有任何 GPU 数据时返回 404（与 /api/gpu-live/{name} 一致）。"""
    result = rgtop_frame(node=name, procs=procs)
    frames = result["data"]["nodes"]
    if not frames or frames[0]["error"] == "no GPU data collected for this node":
        raise HTTPException(status_code=404, detail=f"no GPU data for node {name}")
    return result


@router.get("/node/{name}/jobs")
def node_jobs(name: str, state: Optional[str] = None):
    """节点作业列表 - 4.4 接口"""
    cache_key = f"node_jobs:{name}:{state}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        # 运行作业
        running = session.query(Job).filter(
            Job.state == "RUNNING",
            Job.node_list.ilike(f"%{name}%")
        ).all()

        # 作业级 GPU 利用率：agent 把每张卡对应到所属 Slurm 作业，这里取该作业所有卡的平均
        frame = _latest_gpu_frame(session, name)
        job_util: Dict[int, List[int]] = {}
        if frame:
            for g in frame["gpus"]:
                if g["job_id"] and g["util_pct"] is not None:
                    job_util.setdefault(g["job_id"], []).append(g["util_pct"])

        running_data = []
        for j in running:
            utils = job_util.get(j.job_id)
            gpu_util_avg = round(sum(utils) / len(utils)) if utils else None

            duration = None
            if j.start_time:
                elapsed = _seconds_since(j.start_time) or 0
                hours = int(elapsed // 3600)
                minutes = int((elapsed % 3600) // 60)
                seconds = int(elapsed % 60)
                duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            running_data.append({
                "job_id": int(j.job_id) if j.job_id else None,
                "job_label": _job_label(j),
                "user": j.user,
                "partition": j.partition,
                "gpu_num": j.alloc_gpus or 0,
                "cpu_num": j.alloc_cpus or 0,
                "mem_gb": round((j.alloc_mem_mb or j.req_mem_mb or 0) / 1024),  # AllocTRES 无 mem，回退 ReqTRES
                "start_time": _to_iso(j.start_time),
                "duration": duration,
                "gpu_util_avg": gpu_util_avg,
            })

        # 排队作业
        node_part = session.query(NodeSnapshot.partition).filter(
            NodeSnapshot.node_name == name
        ).order_by(desc(NodeSnapshot.collected_at)).limit(1).scalar()
        pending = session.query(Job).filter(
            Job.state == "PENDING",
            _node_pending_filter(node_part, name),
        ).all()

        ranks = _pending_rank_map(session)
        pending_data = []
        for j in pending:
            wait_duration = None
            if j.submit_time:
                elapsed = _seconds_since(j.submit_time) or 0
                hours = int(elapsed // 3600)
                minutes = int((elapsed % 3600) // 60)
                seconds = int(elapsed % 60)
                wait_duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            pending_data.append({
                "job_id": int(j.job_id) if j.job_id else None,
                "job_label": _job_label(j),
                "user": j.user,
                "submit_time": _to_iso(j.submit_time),
                "wait_duration": wait_duration,
                "partition": j.partition,
                "req_gpu": j.req_gpus or 0,
                "req_cpu": j.req_cpus or 0,
                "req_mem_gb": round((j.req_mem_mb or 0) / 1024),
                "reason": j.pending_reason,
                "priority": j.priority,
                "queue_pos": ranks.get(j.job_id),
            })

        data = {
            "running": running_data,
            "pending": pending_data,
        }

        cache.set(cache_key, data, TTL_PENDING)
        return success_response(data)


# ==================== 五、排队作业页 ====================

@router.get("/jobs/pending")
def jobs_pending(
    user: Optional[str] = None,
    partition: Optional[str] = None,
    node: Optional[str] = None,
    reason: Optional[str] = None,
    gpu_req: Optional[str] = None,
    keyword: Optional[str] = None,
    sort: str = Query("wait_desc", regex="^(wait_desc|priority|submit_time)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    """排队作业列表 - 5.1 接口"""
    cache_key = f"jobs_pending:{user}:{partition}:{node}:{reason}:{gpu_req}:{keyword}:{sort}:{page}:{page_size}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        query = session.query(Job).filter(Job.state == "PENDING")

        # 过滤条件
        if user and user != "all":
            query = query.filter(Job.user == user)
        if partition and partition != "all":
            query = query.filter(Job.partition == partition)
        if node and node != "all":
            query = query.filter(Job.node_list.ilike(f"%{node}%"))
        if reason and reason != "all":
            query = query.filter(Job.pending_reason == reason)
        if keyword:
            query = query.filter(Job.job_id.ilike(f"%{keyword}%"))
        if gpu_req and gpu_req != "all":
            if gpu_req == "8+":
                query = query.filter(Job.req_gpus >= 8)
            else:
                query = query.filter(Job.req_gpus == int(gpu_req))

        # 排序
        if sort == "wait_desc":
            query = query.order_by(Job.submit_time)
        elif sort == "priority":
            query = query.order_by(desc(Job.priority).nulls_last(), Job.submit_time)
        elif sort == "submit_time":
            query = query.order_by(desc(Job.submit_time))  # 最新提交在前；wait_desc 是最早提交（等得最久）在前

        # 分页
        total = query.count()
        jobs = query.offset((page - 1) * page_size).limit(page_size).all()

        ranks = _pending_rank_map(session)
        job_list = []
        for j in jobs:
            wait_duration = None
            if j.submit_time:
                elapsed = _seconds_since(j.submit_time) or 0
                hours = int(elapsed // 3600)
                minutes = int((elapsed % 3600) // 60)
                seconds = int(elapsed % 60)
                wait_duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

            job_list.append({
                "job_id": int(j.job_id) if j.job_id else None,
                "job_label": _job_label(j),
                "user": j.user,
                "submit_time": _to_iso(j.submit_time),
                "wait_duration": wait_duration,
                "partition": j.partition,
                "req_gpu": j.req_gpus or 0,
                "req_cpu": j.req_cpus or 0,
                "req_mem_gb": round((j.req_mem_mb or 0) / 1024, 1),
                "target_nodes": [j.node_list] if j.node_list else [],
                "priority": j.priority,
                "queue_pos": ranks.get(j.job_id),
                "reason": j.pending_reason,
                "reason_detail": "",
            })

        data = {
            "total": total,
            "page": page,
            "page_size": page_size,
            "list": job_list,
        }

        cache.set(cache_key, data, TTL_PENDING)
        return success_response(data)


@router.get("/jobs/pending/summary")
def jobs_pending_summary(
    partition: Optional[str] = None,
    user: Optional[str] = None,
    range: str = Query("15m", regex="^(5m|15m|1h|1d)$"),
    long_threshold_min: int = Query(30, ge=1),
):
    """排队聚合分析 - 5.2 接口"""
    cache_key = f"pending_summary:{partition}:{user}:{range}:{long_threshold_min}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        from ..storage.models import PendingJobSnapshot
        import json

        # 当前排队作业统计
        query = session.query(Job).filter(Job.state == "PENDING")
        if partition and partition != "all":
            query = query.filter(Job.partition == partition)
        if user and user != "all":
            query = query.filter(Job.user == user)

        jobs = query.all()

        # 计算平均等待时间
        wait_times = []
        long_pending = 0
        for j in jobs:
            if j.submit_time:
                wait_sec = _seconds_since(j.submit_time) or 0
                wait_times.append(wait_sec)
                if wait_sec > long_threshold_min * 60:
                    long_pending += 1

        avg_wait_min = int(sum(wait_times) / len(wait_times) / 60) if wait_times else 0

        # 获取等待趋势（从快照表）
        end_dt = datetime.now(timezone.utc)
        if range == "5m":
            hours = 2
        elif range == "15m":
            hours = 6
        elif range == "1h":
            hours = 24
        else:  # 1d
            hours = 24 * 7

        start_dt = end_dt - timedelta(hours=hours)

        snapshots = session.query(PendingJobSnapshot).filter(
            PendingJobSnapshot.snapshot_ts >= start_dt,
            PendingJobSnapshot.snapshot_ts <= end_dt
        ).order_by(PendingJobSnapshot.snapshot_ts).all()

        times = []
        avg_wait_trend = []
        p90_wait_trend = []

        for snap in snapshots:
            local_tz = timezone(timedelta(hours=8))
            local_time = snap.snapshot_ts.astimezone(local_tz)
            time_str = local_time.strftime("%H:%M") if range != "1d" else local_time.strftime("%m-%d")
            times.append(time_str)
            avg_wait_trend.append(round(snap.avg_wait_minutes, 1))
            p90_wait_trend.append(round(snap.p90_wait_minutes, 1))

        # 获取最新快照的详细信息
        latest_snapshot = snapshots[-1] if snapshots else None
        reason_dist = {}
        top_users = []
        top_nodes = []

        if latest_snapshot:
            try:
                reason_dist = json.loads(latest_snapshot.reason_distribution) if latest_snapshot.reason_distribution else {}
                top_users = json.loads(latest_snapshot.top_users) if latest_snapshot.top_users else []
                top_nodes = json.loads(latest_snapshot.top_nodes) if latest_snapshot.top_nodes else []
            except:
                pass

        # 排队原因分布以当前排队作业实时统计为准（快照最长滞后 5 分钟，且与当前列表不一致会让图表对不上）
        reason_dist = {}
        for j in jobs:
            key = j.pending_reason or "Unknown"
            reason_dist[key] = reason_dist.get(key, 0) + 1

        data = {
            "avg_wait_min": avg_wait_min,
            "pending_total": len(jobs),
            "gpu_demand": sum(j.req_gpus or 0 for j in jobs),
            "long_pending": long_pending,
            "wait_trend": {
                "times": times,
                "avg_wait": avg_wait_trend,
                "p90_wait": p90_wait_trend,
            },
            "reason_dist": reason_dist,
            "top_users": top_users,
            "top_nodes": top_nodes,
        }

        cache.set(cache_key, data, TTL_PENDING)
        return success_response(data)


# ==================== 七、下拉框数据源 ====================

@router.get("/meta/partitions")
def meta_partitions():
    """分区列表"""
    cache_key = "meta_partitions"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    # 分区取自最近一轮节点快照；分区内只要还有一个非 down/drain 节点就算可用
    with get_session() as session:
        latest = session.query(func.max(NodeSnapshot.collected_at)).scalar()
        rows = session.query(NodeSnapshot.partition, NodeSnapshot.state).filter(
            NodeSnapshot.collected_at == latest, NodeSnapshot.partition.isnot(None)
        ).all() if latest else []
    parts: Dict[str, Dict[str, Any]] = {}
    for part, state in rows:
        p = parts.setdefault(part, {"name": part, "nodes": 0, "available": False})
        p["nodes"] += 1
        st = (state or "").lower()
        if "down" not in st and "drain" not in st:
            p["available"] = True
    data = sorted(parts.values(), key=lambda x: x["name"])

    cache.set(cache_key, data, TTL_META)
    return success_response(data)


@router.get("/meta/nodes")
def meta_nodes(partition: Optional[str] = None):
    """节点列表"""
    cache_key = f"meta_nodes:{partition}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        latest = session.query(NodeSnapshot).order_by(desc(NodeSnapshot.collected_at)).first()
        if not latest:
            return success_response([])

        snapshots = session.query(NodeSnapshot).filter(
            NodeSnapshot.collected_at == latest.collected_at
        ).all()

        nodes_data = []
        for s in snapshots:
            if partition and partition != "all" and s.partition != partition:
                continue
            state_norm = (s.state or "").lower()
            if "down" in state_norm or "drain" in state_norm:
                state_norm = "down"
            elif state_norm == "idle":
                state_norm = "idle"
            elif state_norm == "allocated":
                state_norm = "allocated"
            else:
                state_norm = "mixed"

            nodes_data.append({
                "name": s.node_name,
                "partition": s.partition,
                "state": state_norm,
            })

        cache.set(cache_key, nodes_data, TTL_META)
        return success_response(nodes_data)


@router.get("/meta/users")
def meta_users():
    """用户列表（近 30 天有作业）"""
    cache_key = "meta_users"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        cutoff = datetime.now(timezone.utc) - timedelta(days=30)
        users = session.query(Job.user).filter(
            Job.user.isnot(None),
            Job.submit_time >= cutoff
        ).distinct().all()

        user_list = sorted([u.user for u in users])

        cache.set(cache_key, user_list, TTL_META)
        return success_response(user_list)


@router.get("/meta/date-range")
def meta_date_range():
    """可查询日期范围：返回真正有采集数据的本地日期（降序），供对比日期下拉框使用。"""
    cache_key = "meta_date_range"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        rows = session.execute(text("""
            SELECT DISTINCT (collected_at AT TIME ZONE 'UTC' AT TIME ZONE :tz)::date AS d
            FROM (
                SELECT collected_at FROM node_snapshots
                UNION ALL SELECT collected_at FROM gpu_snapshots
                UNION ALL SELECT collected_at FROM node_host_stats
            ) t ORDER BY d DESC
        """), {"tz": settings.timezone}).all()

    available = [r[0].strftime("%Y-%m-%d") for r in rows]
    today = datetime.now(ZoneInfo(settings.timezone)).strftime("%Y-%m-%d")
    data = {
        "today": today,
        "min_date": available[-1] if available else today,
        "max_date": available[0] if available else today,
        "available": available,
    }
    cache.set(cache_key, data, TTL_META)
    return success_response(data)


# ==================== 六、个人统计页（简化实现）====================

@router.get("/user/{name}/summary")
def user_summary(
    name: str,
    range: str = Query("30d", regex="^(1d|7d|30d|90d|all)$"),
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """用户统计卡片 - 6.1 接口"""
    cache_key = f"user_summary:{name}:{range}:{start}:{end}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        from ..storage.models import JobHistory

        # 计算时间范围
        local_tz = timezone(timedelta(hours=8))
        now = datetime.now(local_tz)

        if range == "1d":
            start_dt = now - timedelta(days=1)
        elif range == "7d":
            start_dt = now - timedelta(days=7)
        elif range == "30d":
            start_dt = now - timedelta(days=30)
        elif range == "90d":
            start_dt = now - timedelta(days=90)
        elif range == "all":
            start_dt = datetime(2000, 1, 1, tzinfo=local_tz)
        else:
            start_dt = now - timedelta(days=30)

        end_dt = now

        if start:
            try:
                start_dt = datetime.fromisoformat(start).replace(tzinfo=local_tz)
            except:
                pass
        if end:
            try:
                end_dt = datetime.fromisoformat(end).replace(tzinfo=local_tz)
            except:
                pass

        # 查询历史作业（从 Job 表，JobHistory 表为空时的 fallback）
        def _is_finished(state: str) -> bool:
            s = (state or "").upper()
            return s in ("COMPLETED", "FAILED", "TIMEOUT") or s.startswith("CANCELLED")

        # 先转为 naive UTC 与数据库字段对齐
        start_naive = _to_naive(start_dt)
        end_naive = _to_naive(end_dt)

        jobs_q = session.query(Job).filter(
            Job.user == name,
            Job.submit_time >= start_naive,
            Job.submit_time <= end_naive,
        )
        jobs = [j for j in jobs_q.all() if _is_finished(j.state)]

        # 统计
        job_total = len(jobs)
        job_success = sum(1 for j in jobs if (j.state or "").upper() == "COMPLETED")
        job_failed = sum(1 for j in jobs if (j.state or "").upper() in ("FAILED", "TIMEOUT") or (j.state or "").upper().startswith("CANCELLED"))

        success_rate = round(100.0 * job_success / job_total, 1) if job_total > 0 else 0

        # 计算 GPU 时
        gpu_hours = 0
        for j in jobs:
            if j.alloc_gpus and j.elapsed_seconds:
                gpu_hours += j.alloc_gpus * j.elapsed_seconds / 3600

        # 计算平均等待时间
        wait_times = []
        for j in jobs:
            if j.submit_time and j.start_time:
                wait_sec = (j.start_time - j.submit_time).total_seconds()
                wait_times.append(wait_sec)

        avg_wait_min = int(sum(wait_times) / len(wait_times) / 60) if wait_times else 0

        # 当前运行和排队
        running = session.query(Job).filter(
            Job.user == name,
            Job.state == "RUNNING"
        ).count()

        pending = session.query(Job).filter(
            Job.user == name,
            Job.state == "PENDING"
        ).count()

        # 平均 GPU 利用率：按 GPU 时（卡数 × 时长）加权；优先用效率分析结果，没有则用实测汇总；都没有返回 None
        from ..storage.models import JobEfficiencyAnalysis
        job_ids = [j.job_id for j in jobs]
        avg_gpu_util = None
        if job_ids:
            analyses = {
                a.job_id: a.gpu_util_avg
                for a in session.query(JobEfficiencyAnalysis).filter(
                    JobEfficiencyAnalysis.job_id.in_(job_ids),
                    JobEfficiencyAnalysis.gpu_util_avg.isnot(None),
                ).all()
            }
            gstats = _gpu_job_stats(session, job_ids)
            weighted = []
            for j in jobs:
                util = analyses.get(j.job_id)
                if util is None:
                    util = gstats.get(j.job_id, {}).get("util")
                if util is None:
                    continue
                weighted.append((util, max(1, (j.alloc_gpus or 1) * (j.elapsed_seconds or 1))))
            if weighted:
                avg_gpu_util = round(sum(u * w for u, w in weighted) / sum(w for _, w in weighted), 1)

        data = {
            "user": name,
            "range": range,
            "gpu_hours": round(gpu_hours, 1),
            "job_total": job_total,
            "job_success": job_success,
            "job_failed": job_failed,
            "success_rate": success_rate,
            "avg_wait_min": avg_wait_min,
            "running": running,
            "pending": pending,
            "avg_gpu_util": avg_gpu_util,
        }

        cache.set(cache_key, data, 60)
        return success_response(data)


@router.get("/user/{name}/jobs")
def user_jobs(
    name: str,
    state: str = Query("finished", regex="^(running|pending|finished|all)$"),
    partition: Optional[str] = None,
    status: Optional[str] = None,
    start: Optional[str] = None,
    end: Optional[str] = None,
    range: Optional[str] = Query(None, pattern="^(1d|7d|30d|90d|all)$"),
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=5000),
):
    """用户作业列表 - 6.2 接口（range 与 start/end 二选一；历史作业按提交时间过滤）"""
    cache_key = f"user_jobs:{name}:{state}:{partition}:{status}:{start}:{end}:{range}:{page}:{page_size}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    with get_session() as session:
        from ..storage.models import JobHistory

        if state == "running":
            # 查询当前运行作业
            query = session.query(Job).filter(
                Job.user == name,
                Job.state == "RUNNING"
            )
            if partition and partition != "all":
                query = query.filter(Job.partition == partition)

            total = query.count()
            jobs = query.order_by(Job.start_time.desc()).offset((page - 1) * page_size).limit(page_size).all()

            gstats = _gpu_job_stats(session, [j.job_id for j in jobs])
            job_list = []
            for j in jobs:
                duration = None
                if j.start_time:
                    elapsed = _seconds_since(j.start_time) or 0
                    hours = int(elapsed // 3600)
                    minutes = int((elapsed % 3600) // 60)
                    seconds = int(elapsed % 60)
                    duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

                job_list.append({
                    "job_id": int(j.job_id) if j.job_id else None,
                    "job_label": _job_label(j),
                    "job_name": j.job_name,
                    "node": j.node_list,
                    "gpu_num": j.alloc_gpus or 0,
                    "start_time": _to_iso(j.start_time),
                    "end_time": None,
                    "partition": j.partition,
                    "cpu_num": j.alloc_cpus or 0,
                    "mem_gb": round((j.alloc_mem_mb or j.req_mem_mb or 0) / 1024),
                    "state": "running",
                    "exit_status": "RUNNING",
                    "duration": duration,
                    "wait_duration": None,
                    "gpu_util_avg": gstats.get(j.job_id, {}).get("util"),
                    "mem_peak_gb": gstats.get(j.job_id, {}).get("mem_peak_gb"),
                    "has_analysis": False,
                })

        elif state == "pending":
            # 查询排队作业
            query = session.query(Job).filter(
                Job.user == name,
                Job.state == "PENDING"
            )
            if partition and partition != "all":
                query = query.filter(Job.partition == partition)

            total = query.count()
            jobs = query.order_by(Job.submit_time.desc()).offset((page - 1) * page_size).limit(page_size).all()
            ranks = _pending_rank_map(session)

            job_list = []
            for j in jobs:
                wait_duration = None
                if j.submit_time:
                    elapsed = _seconds_since(j.submit_time) or 0
                    hours = int(elapsed // 3600)
                    minutes = int((elapsed % 3600) // 60)
                    seconds = int(elapsed % 60)
                    wait_duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

                job_list.append({
                    "job_id": int(j.job_id) if j.job_id else None,
                    "job_label": _job_label(j),
                    "job_name": j.job_name,
                    "node": j.node_list or "任意节点",
                    "gpu_num": j.req_gpus or 0,
                    "start_time": None,
                    "end_time": None,
                    "partition": j.partition,
                    "req_cpu": j.req_cpus or 0,
                    "req_mem_gb": round((j.req_mem_mb or 0) / 1024),
                    "reason": j.pending_reason,
                    "priority": j.priority,
                    "queue_pos": ranks.get(j.job_id),
                    "state": "pending",
                    "exit_status": "PENDING",
                    "duration": None,
                    "wait_duration": wait_duration,
                    "gpu_util_avg": None,
                    "mem_peak_gb": None,
                    "has_analysis": False,
                })

        else:  # finished or all
            # 查询历史作业（JobHistory 表为空时从 Job 表读取）
            def _is_fin(s):
                s = (s or "").upper()
                return s in ("COMPLETED", "FAILED", "TIMEOUT") or s.startswith("CANCELLED")

            query = session.query(Job).filter(Job.user == name)

            if state == "finished":
                # 过滤所有终态，包括 "CANCELLED by XXXX"
                query = query.filter(
                    Job.state.in_(["COMPLETED", "FAILED", "TIMEOUT"]) |
                    Job.state.ilike("CANCELLED%")
                )
            if partition and partition != "all":
                query = query.filter(Job.partition == partition)
            if status:
                # status 传具体终态（COMPLETED / FAILED / TIMEOUT / CANCELLED）
                if status.upper() == "CANCELLED":
                    query = query.filter(Job.state.ilike("CANCELLED%"))
                else:
                    query = query.filter(Job.state == status)

            # 时间范围过滤（与数据库 naive datetime 对齐）
            range_start = _range_start_naive(range)
            if range_start and not start:
                query = query.filter(Job.submit_time >= range_start)
            if start:
                try:
                    start_dt = _to_naive(datetime.fromisoformat(start).replace(tzinfo=timezone.utc))
                    query = query.filter(Job.submit_time >= start_dt)
                except Exception:
                    pass
            if end:
                try:
                    end_dt = _to_naive(datetime.fromisoformat(end).replace(tzinfo=timezone.utc))
                    query = query.filter(Job.submit_time <= end_dt)
                except Exception:
                    pass

            total = query.count()
            jobs = query.order_by(Job.end_time.desc()).offset((page - 1) * page_size).limit(page_size).all()

            # 获取效率分析数据
            from ..storage.models import JobEfficiencyAnalysis
            job_ids = [j.job_id for j in jobs]
            analyses = {}
            if job_ids:
                analysis_list = session.query(JobEfficiencyAnalysis).filter(
                    JobEfficiencyAnalysis.job_id.in_(job_ids)
                ).all()
                analyses = {a.job_id: a for a in analysis_list}
            gstats = _gpu_job_stats(session, job_ids)

            job_list = []
            for j in jobs:
                duration = None
                wait_duration = None

                # elapsed_seconds 有时为 0，尝试从 start/end 计算
                elapsed = j.elapsed_seconds or 0
                if elapsed == 0 and j.start_time and j.end_time:
                    elapsed = int((j.end_time - j.start_time).total_seconds())

                if elapsed:
                    hours = int(elapsed // 3600)
                    minutes = int((elapsed % 3600) // 60)
                    seconds = int(elapsed % 60)
                    duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

                if j.submit_time and j.start_time:
                    wait_sec = (j.start_time - j.submit_time).total_seconds()
                    hours = int(wait_sec // 3600)
                    minutes = int((wait_sec % 3600) // 60)
                    seconds = int(wait_sec % 60)
                    wait_duration = f"{hours:02d}:{minutes:02d}:{seconds:02d}"

                analysis = analyses.get(j.job_id)

                # 将 "CANCELLED by XXXX" 归一为 "CANCELLED"
                exit_status = j.state or ""
                if exit_status.upper().startswith("CANCELLED"):
                    exit_status = "CANCELLED"

                job_list.append({
                    "job_id": int(j.job_id) if j.job_id else None,
                    "job_label": _job_label(j),
                    "job_name": j.job_name,
                    "node": j.node_list,
                    "gpu_num": j.alloc_gpus or 0,
                    "start_time": _to_iso(j.start_time),
                    "end_time": _to_iso(j.end_time),
                    "partition": j.partition,
                    "state": "finished",
                    "exit_status": exit_status,
                    "duration": duration,
                    "wait_duration": wait_duration,
                    # 优先用效率分析结果，分析任务尚未跑到时直接用实测汇总
                    "gpu_util_avg": round(analysis.gpu_util_avg, 1) if analysis and analysis.gpu_util_avg is not None
                                    else gstats.get(j.job_id, {}).get("util"),
                    "mem_peak_gb": gstats.get(j.job_id, {}).get("mem_peak_gb"),  # 单卡显存峰值
                    "has_analysis": analysis is not None,
                })

        data = {
            "total": total,
            "page": page,
            "page_size": page_size,
            "list": job_list,
        }

        cache.set(cache_key, data, 30)
        return success_response(data)


@router.get("/user/{name}/trend")
def user_trend(
    name: str,
    range: str = Query("7d", pattern="^(1d|7d|30d|90d)$"),
    granularity: str = Query("1d", pattern="^(1h|1d)$"),
):
    """用户 GPU 耗时趋势：按作业**开始时间**归属到时间桶，一次返回三个指标，前端切换指标无需再请求。

    gpuh = 卡数 × 时长（卡时，运行中作业算到现在）；cnt = 作业数；util = 按 GPU 时加权的平均利用率（无实测为 null）。
    """
    days = _USER_RANGE_DAYS[range]
    gran = "1h" if granularity == "1h" and days <= 7 else "1d"  # 小时粒度最多 7 天（168 点），更长范围自动用天
    cache_key = f"user_trend:{name}:{range}:{gran}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    tz = ZoneInfo(settings.timezone)
    now_local = datetime.now(tz)
    unit = "hour" if gran == "1h" else "day"
    step = timedelta(hours=1) if gran == "1h" else timedelta(days=1)
    first = (now_local - timedelta(days=days - 1)).replace(minute=0, second=0, microsecond=0)
    if gran == "1d":
        first = first.replace(hour=0)
    else:
        first = (now_local - timedelta(days=days)).replace(minute=0, second=0, microsecond=0)
    start_utc = first.astimezone(timezone.utc).replace(tzinfo=None)

    with get_session() as session:
        rows = session.execute(text(f"""
            SELECT date_trunc('{unit}', (j.start_time AT TIME ZONE 'UTC') AT TIME ZONE :tz) AS b,
                   SUM(COALESCE(j.alloc_gpus, 0) * j.secs) / 3600.0 AS gpuh,
                   COUNT(*) AS cnt,
                   SUM(CASE WHEN g.u IS NOT NULL THEN g.u * COALESCE(j.alloc_gpus, 0) * j.secs END)
                     / NULLIF(SUM(CASE WHEN g.u IS NOT NULL THEN COALESCE(j.alloc_gpus, 0) * j.secs END), 0) AS util
            FROM (
                SELECT job_id, alloc_gpus, start_time,
                       CASE WHEN state = 'RUNNING' OR end_time IS NULL
                            THEN EXTRACT(EPOCH FROM ((NOW() AT TIME ZONE 'UTC') - start_time))
                            ELSE EXTRACT(EPOCH FROM (end_time - start_time)) END AS secs
                FROM jobs
                WHERE "user" = :u AND start_time IS NOT NULL AND start_time >= :s
            ) j
            LEFT JOIN (SELECT job_id, AVG(util_pct) AS u FROM gpu_snapshots WHERE job_id IS NOT NULL GROUP BY job_id) g
              ON g.job_id = j.job_id
            GROUP BY b
        """), {"u": name, "s": start_utc, "tz": settings.timezone}).all()

    by_bucket = {r.b: r for r in rows}
    times, gpuh, cnt, util = [], [], [], []
    t = first.replace(tzinfo=None)
    end_naive = now_local.replace(tzinfo=None)
    while t <= end_naive:
        r = by_bucket.get(t)
        times.append(t.strftime("%m-%d %H:00") if gran == "1h" else t.strftime("%m-%d"))
        gpuh.append(round(float(r.gpuh), 1) if r and r.gpuh is not None else 0)
        cnt.append(int(r.cnt) if r else 0)
        util.append(round(float(r.util), 1) if r and r.util is not None else None)
        t += step

    data = {"range": range, "granularity": gran, "times": times, "gpuh": gpuh, "cnt": cnt, "util": util}
    cache.set(cache_key, data, 60)
    return success_response(data)


def _csv_safe(v) -> str:
    """防 CSV 公式注入：以 = + - @ 开头的单元格前加单引号（作业名由用户随意起，导出后会在 Excel 里打开）。"""
    s = "" if v is None else str(v)
    return "'" + s if s[:1] in ("=", "+", "-", "@", "\t", "\r") else s


@router.get("/user/{name}/export")
def user_export(
    name: str,
    format: str = Query("csv", pattern="^(csv|json)$"),
    state: str = Query("finished", pattern="^(running|pending|finished|all)$"),
    status: Optional[str] = None,
    range: Optional[str] = Query(None, pattern="^(1d|7d|30d|90d|all)$"),
    partition: Optional[str] = None,
):
    """导出用户作业列表（与页面同一口径，最多 5000 条）。"""
    res = user_jobs(name=name, state=state, partition=partition, status=status,
                    start=None, end=None, range=range, page=1, page_size=5000)
    rows = res["data"]["list"]
    fname = f"jobs_{name}_{state}_{datetime.now(ZoneInfo(settings.timezone)).strftime('%Y%m%d_%H%M%S')}.{format}"
    headers = {"Content-Disposition": f'attachment; filename="{fname}"'}

    if format == "json":
        return Response(json.dumps(rows, ensure_ascii=False, indent=1), media_type="application/json", headers=headers)

    cols = [("job_label", "作业ID"), ("job_name", "作业名"), ("partition", "分区"), ("node", "节点"),
            ("gpu_num", "GPU卡数"), ("start_time", "开始时间"), ("end_time", "结束时间"), ("duration", "耗时"),
            ("wait_duration", "等待"), ("exit_status", "状态"), ("gpu_util_avg", "GPU平均利用率%"),
            ("mem_peak_gb", "单卡显存峰值GB")]
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow([c[1] for c in cols])
    for r in rows:
        w.writerow([_csv_safe(r.get(k)) for k, _ in cols])
    # utf-8-sig：带 BOM，Excel 直接打开中文不乱码
    return Response(buf.getvalue().encode("utf-8-sig"), media_type="text/csv; charset=utf-8", headers=headers)


@router.get("/job/{id}/analysis")
def job_analysis(id: str, points: int = Query(60, ge=10, le=500)):
    """单作业效率分析 - 基于 sacct 数据计算 jobeff.sh 风格指标"""
    cache_key = f"job_analysis_v3:{id}"
    cached = cache.get(cache_key)
    if cached is not None:
        return success_response(cached)

    try:
        job_id = int(id)
    except ValueError:
        return error_response(40001, "无效的作业 ID")

    with get_session() as session:
        job = session.query(Job).filter(Job.job_id == job_id).order_by(
            desc(Job.submit_time)
        ).first()

        if not job:
            return error_response(40401, f"作业 {job_id} 不存在")

        # ---- 尝试从 JobHistory 获取 CPU/内存数据（已结束的作业） ----
        total_cpu_seconds = None
        max_rss_mb = None
        job_history = session.query(JobHistory).filter(JobHistory.job_id == job_id).order_by(
            desc(JobHistory.submit_time)
        ).first()
        if job_history:
            total_cpu_seconds = job_history.total_cpu_seconds
            max_rss_mb = job_history.max_rss_mb

        # ---- 时间效率 ----
        elapsed_sec = job.elapsed_seconds or 0
        # elapsed_seconds 有时为 0，从 start/end 补算
        if elapsed_sec == 0 and job.start_time and job.end_time:
            elapsed_sec = int((job.end_time - job.start_time).total_seconds())

        timelimit_sec = (job.timelimit_minutes or 0) * 60

        def hms(sec):
            if not sec:
                return "n/a"
            sec = int(sec)
            d, r = divmod(sec, 86400)
            h, r2 = divmod(r, 3600)
            m, s = divmod(r2, 60)
            prefix = f"{d}-" if d else ""
            return f"{prefix}{h:02d}:{m:02d}:{s:02d}"

        def pct(a, b):
            if not a or not b:
                return None
            return round(100.0 * a / b, 2)

        time_eff = pct(elapsed_sec, timelimit_sec)

        # ---- CPU 效率 (从 JobHistory 获取 total_cpu_seconds) ----
        alloc_cpus = job.alloc_cpus or 0
        core_walltime_sec = alloc_cpus * elapsed_sec if alloc_cpus else None

        # ---- 内存效率 (从 JobHistory 获取 max_rss_mb) ----
        req_mem_mb = job.req_mem_mb  # 申请内存 (MB)
        alloc_mem_mb = job.alloc_mem_mb  # 分配内存 (MB)，有时为空

        def mb_to_gb(mb):
            return round(mb / 1024, 2) if mb else None

        # ---- 从 tres 解析额外信息 ----
        alloc_tres = job.alloc_tres_raw or ""
        req_tres = job.req_tres_raw or ""

        # ---- 状态判断 ----
        raw_state = job.state or ""
        exit_code = job.exit_code
        state_display = raw_state
        if raw_state.upper().startswith("CANCELLED"):
            state_display = "CANCELLED"

        # ---- 诊断提示 ----
        tips = []
        finished = state_display not in ("RUNNING", "PENDING")  # 没结束的作业，时长/状态类提示没有意义
        if not finished:
            time_eff = None  # 还没结束，"实际用时/申请时长"不是效率
        if finished and time_eff is not None and time_eff < 25:
            tips.append(
                f"只用了 {time_eff:.1f}% 的申请时长，下次把 -t 调小，排队会快很多"
            )
        if finished and time_eff is not None and time_eff < 10:
            tips.append("时间申请量严重过多，建议先 dry-run 估算实际用时")

        if req_mem_mb and req_mem_mb > 500 * 1024:  # > 500GB
            tips.append(
                f"申请内存 {mb_to_gb(req_mem_mb)} GB，请确认是否真正需要这么多"
            )

        if alloc_cpus and job.alloc_gpus and alloc_cpus > job.alloc_gpus * 16:
            tips.append(
                f"CPU 核数 ({alloc_cpus}) 远超 GPU 数 ({job.alloc_gpus}) × 16，"
                "考虑减少 --cpus-per-task"
            )

        if finished and state_display != "COMPLETED":
            tips.append(
                f"作业未正常结束（{state_display}），效率数据仅供参考，建议先排查失败原因"
            )

        if exit_code == 9 or (isinstance(exit_code, str) and "9" in str(exit_code)):
            tips.append("退出码含 9，可能是 OOM（内存不足被 kill），尝试加大 --mem")

        # ---- GPU 分析：gpu_snapshots 按作业号（agent 经 slurmstepd 解析）汇总 ----
        gpu = None
        gpu_note = None
        if job.alloc_gpus and job.start_time:
            end_dt = job.end_time or datetime.utcnow()
            rows = session.execute(text("""
                SELECT collected_at, AVG(util_pct) AS util, MAX(mem_used_mb) AS mem
                FROM gpu_snapshots
                WHERE job_id = :j AND util_pct IS NOT NULL AND collected_at BETWEEN :s AND :e
                GROUP BY collected_at ORDER BY collected_at
            """), {"j": job_id, "s": job.start_time, "e": end_dt}).all()
            if rows:
                utils = [float(r.util) for r in rows]
                mems = [r.mem for r in rows if r.mem is not None]
                srt = sorted(utils)
                avg_u = sum(utils) / len(utils)
                idle_ratio = 100.0 * sum(1 for u in utils if u < 10) / len(utils)
                score = max(0, min(100, round(avg_u * 0.7 + (100 - idle_ratio) * 0.3)))

                # 降采样到 points 个点：把相邻样本取平均
                step = max(1, -(-len(rows) // points))
                tz = ZoneInfo(settings.timezone)
                trend_times, trend_util, trend_mem = [], [], []
                for i in range(0, len(rows), step):
                    chunk = rows[i:i + step]
                    t = chunk[0].collected_at.replace(tzinfo=timezone.utc).astimezone(tz)
                    trend_times.append(t.strftime("%m-%d %H:%M"))
                    trend_util.append(round(sum(float(c.util) for c in chunk) / len(chunk), 1))
                    cm = [c.mem for c in chunk if c.mem is not None]
                    trend_mem.append(round(max(cm) / 1024, 1) if cm else None)

                if idle_ratio > 30:
                    diag = "存在明显空转"
                    tips.append(f"{idle_ratio:.0f}% 的采样时刻 GPU 利用率低于 10%（空转），检查数据加载/CPU 预处理是否拖慢 GPU，或作业结束后卡仍被占用")
                elif avg_u < 40:
                    diag = "利用率偏低"
                    tips.append(f"GPU 平均利用率仅 {avg_u:.0f}%，考虑增大 batch size / 减少申请的卡数")
                else:
                    diag = "资源利用良好"
                gpu = {
                    "samples": len(rows),
                    "avg": round(avg_u, 1),
                    "p50": round(srt[len(srt) // 2], 1),
                    "p90": round(srt[min(len(srt) - 1, int(len(srt) * 0.9))], 1),
                    "idle_ratio": round(idle_ratio, 1),
                    "mem_peak_gb": round(max(mems) / 1024, 1) if mems else None,
                    "score": score,
                    "diagnosis": diag,
                    "trend": {"times": trend_times, "util": trend_util, "mem": trend_mem},
                }
            else:
                gpu_note = ("该作业运行期间没有采集到 GPU 数据：GPU 采集只覆盖已在「集群配置」中配置了 IP 的节点，"
                            "且从配置之后才开始记录。")

        # ---- 等待时长 ----
        wait_sec = None
        if job.submit_time and job.start_time:
            wait_sec = int((job.start_time - job.submit_time).total_seconds())

        data = {
            "job_id": job_id,
            "job_name": job.job_name,
            "partition": job.partition,
            "user": job.user,
            "state": state_display,
            "exit_code": str(exit_code) if exit_code is not None else None,
            # 资源概览
            "nodes": job.nodes_alloc or 1,
            "cores": alloc_cpus,
            "gpus": job.alloc_gpus or 0,
            "alloc_tres": alloc_tres,
            "req_tres": req_tres,
            # 时间
            "elapsed": hms(elapsed_sec),
            "elapsed_sec": elapsed_sec,
            "timelimit": hms(timelimit_sec),
            "timelimit_sec": timelimit_sec,
            "time_eff_pct": time_eff,
            "wait_sec": wait_sec,
            "wait": hms(wait_sec),
            # CPU（从 JobHistory 获取）
            "cpu_used": hms(total_cpu_seconds) if total_cpu_seconds else None,
            "cpu_eff_pct": pct(total_cpu_seconds, core_walltime_sec) if total_cpu_seconds and core_walltime_sec else None,
            "core_walltime": hms(core_walltime_sec),
            # 内存（从 JobHistory 获取）
            "mem_used_gb": mb_to_gb(max_rss_mb) if max_rss_mb else None,
            "mem_req_gb": mb_to_gb(req_mem_mb),
            "mem_alloc_gb": mb_to_gb(alloc_mem_mb),
            "mem_eff_pct": pct(max_rss_mb, alloc_mem_mb or req_mem_mb) if max_rss_mb and (alloc_mem_mb or req_mem_mb) else None,
            # 诊断
            "tips": tips,
            "gpu_note": gpu_note,
            # 向后兼容旧前端字段
            "efficiency_score": gpu["score"] if gpu else max(0, min(100, int(time_eff or 0))),
            "diagnosis": gpu["diagnosis"] if gpu else (tips[0] if tips else ("作业正常完成，资源使用合理" if state_display == "COMPLETED" else state_display)),
            "suggestions": tips,
            "gpu": gpu,
            "gpu_util_avg": gpu["avg"] if gpu else None,
            "idle_ratio": gpu["idle_ratio"] if gpu else None,
        }

        cache.set(cache_key, data, 60 if job.state == "RUNNING" else 300)
        return success_response(data)
