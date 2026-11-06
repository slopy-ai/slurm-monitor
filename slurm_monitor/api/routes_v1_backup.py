"""FastAPI 路由

提供首页大屏、节点详情、个人统计所需的查询接口。
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Query
from sqlalchemy import func, desc, text

from ..storage.db import get_session
from ..storage.models import Job, NodeSnapshot, UserStat, NodeAggStat, GpuSnapshot
from ..storage.cache import cache, TTL_OVERVIEW, TTL_QUEUE, TTL_NODE_DETAIL, TTL_TIMESERIES, TTL_USER_STATS

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/v1", tags=["slurm"])

# 合法粒度 → PostgreSQL interval 字符串
_GRANULARITY_INTERVAL = {
    "1m":  "1 minute",
    "5m":  "5 minutes",
    "15m": "15 minutes",
    "1h":  "1 hour",
    "1d":  "1 day",
}

# 各粒度对应的合理查询窗口上限（天）
_GRANULARITY_MAX_DAYS = {
    "1m":  1,
    "5m":  3,
    "15m": 7,
    "1h":  30,
    "1d":  365,
}


def _to_iso(dt: Optional[datetime]) -> Optional[str]:
    return dt.isoformat() if dt else None


# ---- 首页大屏：集群总览 ----

@router.get("/overview")
def overview():
    """集群总览：GPU 总量、已分配、空闲、排队任务数、Top 用户。"""
    cached = cache.get("overview")
    if cached is not None:
        return cached

    with get_session() as session:
        latest = (
            session.query(NodeSnapshot)
            .order_by(desc(NodeSnapshot.collected_at))
            .first()
        )
        snapshots = []
        if latest:
            recent = (
                session.query(NodeSnapshot)
                .filter(NodeSnapshot.collected_at == latest.collected_at)
                .all()
            )
            for s in recent:
                snapshots.append({
                    "node": s.node_name,
                    "state": s.state,
                    "total_gpus": s.gres_total,
                    "alloc_gpus": s.gres_alloc,
                    "idle_gpus": (s.gres_total or 0) - (s.gres_alloc or 0) if s.gres_total else None,
                    "total_cpus": s.total_cpus,
                    "alloc_cpus": s.alloc_cpus,
                    "cpu_load": s.cpu_load,
                    "total_memory_mb": s.total_memory_mb,
                    "alloc_memory_mb": s.alloc_memory_mb,
                    "reason": s.reason,
                    "collected_at": _to_iso(s.collected_at),
                })

        pending = session.query(Job).filter(Job.state == "PENDING").all()
        running = session.query(Job).filter(Job.state == "RUNNING").all()

        total_gpu_alloc = sum((j.alloc_gpus or 0) for j in running)
        pending_gpu_demand = sum((j.req_gpus or 0) for j in pending)

        top_users = (
            session.query(
                Job.user,
                func.sum(Job.alloc_gpus).label("gpus"),
                func.count(Job.job_id).label("jobs"),
            )
            .filter(Job.state == "RUNNING", Job.user.isnot(None))
            .group_by(Job.user)
            .order_by(desc("gpus"))
            .limit(10)
            .all()
        )

        result = {
            "nodes": snapshots,
            "summary": {
                "total_gpus": sum(s.get("total_gpus") or 0 for s in snapshots),
                "alloc_gpus": sum(s.get("alloc_gpus") or 0 for s in snapshots),
                "running_jobs": len(running),
                "pending_jobs": len(pending),
                "running_gpus": total_gpu_alloc,
                "pending_gpu_demand": pending_gpu_demand,
            },
            "top_users": [
                {"user": u.user, "gpus": int(u.gpus or 0), "jobs": int(u.jobs or 0)}
                for u in top_users
            ],
        }
        cache.set("overview", result, TTL_OVERVIEW)
        return result


# ---- 节点时序：GPU 分配率/利用率折线图 ----

@router.get("/nodes/{node_name}/timeseries")
def node_timeseries(
    node_name: str,
    granularity: str = Query("5m", pattern="^(1m|5m|15m|1h|1d)$"),
    days: int = Query(1, ge=1, le=365),
):
    """节点 GPU 分配率时序数据，用于折线图。

    - granularity: 分桶粒度（1m/5m/15m/1h/1d）
    - days: 查询最近 N 天

    1d 粒度从预聚合表 node_agg_stats 读取；
    其余粒度从 NodeSnapshot 原始表用 date_bin 动态聚合（仅保留 30 天原始数据）。

    响应格式：
      {
        "node": "h100-3",
        "granularity": "5m",
        "series": [
          {"ts": "2026-11-03T00:00:00+00:00", "alloc_pct": 75.0,
           "gpu_util_pct": null, "mem_used_mb": null},
          ...
        ]
      }
    """
    cache_key = f"timeseries:{node_name}:{granularity}:{days}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    interval_str = _GRANULARITY_INTERVAL.get(granularity, "5 minutes")
    tz = timezone.utc
    end_dt = datetime.now(tz)
    start_dt = end_dt - timedelta(days=days)

    with get_session() as session:
        if granularity == "1d":
            # 从预聚合表读取
            rows = session.execute(
                text("""
                    SELECT
                        bucket_ts AS ts,
                        avg_alloc_pct AS alloc_pct,
                        avg_gpu_util_pct AS gpu_util_pct,
                        avg_mem_used_mb AS mem_used_mb
                    FROM node_agg_stats
                    WHERE node_name = :node
                      AND granularity = '1d'
                      AND bucket_ts >= :start AND bucket_ts < :end
                    ORDER BY bucket_ts
                """),
                {"node": node_name, "start": start_dt, "end": end_dt},
            ).fetchall()
        else:
            # 从 NodeSnapshot 原始表动态 date_bin 聚合
            rows = session.execute(
                text(f"""
                    SELECT
                        date_bin(
                            interval '{interval_str}',
                            collected_at,
                            TIMESTAMP '2000-01-01'
                        ) AS ts,
                        AVG(
                            CASE WHEN gres_total > 0
                            THEN gres_alloc::float / gres_total * 100
                            END
                        ) AS alloc_pct,
                        NULL::float AS gpu_util_pct,
                        NULL::float AS mem_used_mb
                    FROM node_snapshots
                    WHERE node_name = :node
                      AND collected_at >= :start AND collected_at < :end
                    GROUP BY 1
                    ORDER BY 1
                """),
                {"node": node_name, "start": start_dt, "end": end_dt},
            ).fetchall()

        series = [
            {
                "ts": _to_iso(r.ts),
                "alloc_pct": round(float(r.alloc_pct), 1) if r.alloc_pct is not None else None,
                "gpu_util_pct": round(float(r.gpu_util_pct), 1) if r.gpu_util_pct is not None else None,
                "mem_used_mb": round(float(r.mem_used_mb), 0) if r.mem_used_mb is not None else None,
            }
            for r in rows
        ]

    result = {"node": node_name, "granularity": granularity, "days": days, "series": series}
    cache.set(cache_key, result, TTL_TIMESERIES)
    return result


# ---- 两天对比时序 ----

@router.get("/nodes/{node_name}/compare")
def node_compare(
    node_name: str,
    day_a: str = Query(..., description="YYYY-MM-DD"),
    day_b: str = Query(..., description="YYYY-MM-DD"),
    granularity: str = Query("5m", pattern="^(1m|5m|15m|1h|1d)$"),
):
    """返回两天的 GPU 分配率时序，供大屏双色对比折线图使用。

    响应格式：
      {
        "node": "h100-3",
        "granularity": "5m",
        "day_a": "2026-11-01",
        "day_b": "2026-11-02",
        "series_a": [...],  // 每项 {"minute_of_day": 300, "alloc_pct": 80.0}
        "series_b": [...]
      }
    minute_of_day 为该桶在一天内的偏移分钟数，便于前端对齐两天的 x 轴。
    """
    interval_str = _GRANULARITY_INTERVAL.get(granularity, "5 minutes")
    tz = timezone.utc

    def _fetch_day(day_str: str) -> list[dict]:
        try:
            day_start = datetime.fromisoformat(day_str).replace(tzinfo=tz)
        except ValueError:
            return []
        day_end = day_start + timedelta(days=1)

        with get_session() as session:
            if granularity == "1d":
                rows = session.execute(
                    text("""
                        SELECT
                            0 AS minute_of_day,
                            avg_alloc_pct AS alloc_pct,
                            avg_gpu_util_pct AS gpu_util_pct
                        FROM node_agg_stats
                        WHERE node_name = :node
                          AND granularity = '1d'
                          AND bucket_ts >= :start AND bucket_ts < :end
                        LIMIT 1
                    """),
                    {"node": node_name, "start": day_start, "end": day_end},
                ).fetchall()
            else:
                rows = session.execute(
                    text(f"""
                        SELECT
                            EXTRACT(EPOCH FROM (
                                date_bin(
                                    interval '{interval_str}',
                                    collected_at,
                                    :start
                                ) - :start
                            ))::int / 60 AS minute_of_day,
                            AVG(
                                CASE WHEN gres_total > 0
                                THEN gres_alloc::float / gres_total * 100
                                END
                            ) AS alloc_pct,
                            NULL::float AS gpu_util_pct
                        FROM node_snapshots
                        WHERE node_name = :node
                          AND collected_at >= :start AND collected_at < :end
                        GROUP BY 1
                        ORDER BY 1
                    """),
                    {"node": node_name, "start": day_start, "end": day_end},
                ).fetchall()

        return [
            {
                "minute_of_day": int(r.minute_of_day),
                "alloc_pct": round(float(r.alloc_pct), 1) if r.alloc_pct is not None else None,
                "gpu_util_pct": round(float(r.gpu_util_pct), 1) if r.gpu_util_pct is not None else None,
            }
            for r in rows
        ]

    return {
        "node": node_name,
        "granularity": granularity,
        "day_a": day_a,
        "day_b": day_b,
        "series_a": _fetch_day(day_a),
        "series_b": _fetch_day(day_b),
    }


# ---- 排队任务 ----

@router.get("/queue")
def queue():
    """排队任务列表。"""
    cached = cache.get("queue")
    if cached is not None:
        return cached

    with get_session() as session:
        jobs = (
            session.query(Job)
            .filter(Job.state == "PENDING")
            .order_by(Job.submit_time)
            .all()
        )
        result = {
            "jobs": [
                {
                    "job_id": j.job_id,
                    "user": j.user,
                    "partition": j.partition,
                    "req_gpus": j.req_gpus,
                    "req_cpus": j.req_cpus,
                    "req_mem_mb": j.req_mem_mb,
                    "submit_time": _to_iso(j.submit_time),
                    "timelimit_minutes": j.timelimit_minutes,
                }
                for j in jobs
            ]
        }
        cache.set("queue", result, TTL_QUEUE)
        return result

@router.get("/nodes/{node_name}")
def node_detail(node_name: str):
    """节点详情：最新快照 + 该节点上的运行作业 + 最新 GPU 实测数据（若有）。"""
    cache_key = f"node_detail:{node_name}"
    cached = cache.get(cache_key)
    if cached is not None:
        return cached

    with get_session() as session:
        snap = (
            session.query(NodeSnapshot)
            .filter(NodeSnapshot.node_name == node_name)
            .order_by(desc(NodeSnapshot.collected_at))
            .first()
        )
        if not snap:
            return {"error": "node not found", "node": node_name}

        running = (
            session.query(Job)
            .filter(Job.state == "RUNNING", Job.node_list.like(f"%{node_name}%"))
            .all()
        )

        # Phase 2: nvidia-smi 最新快照（若无则为空列表）
        latest_gpu_snap = (
            session.query(GpuSnapshot)
            .filter(GpuSnapshot.node_name == node_name)
            .order_by(desc(GpuSnapshot.collected_at))
            .first()
        )
        gpu_snapshots = []
        if latest_gpu_snap:
            gpu_snapshots = [
                {
                    "gpu_index": g.gpu_index,
                    "gpu_uuid": g.gpu_uuid,
                    "gpu_name": g.gpu_name,
                    "util_pct": g.util_pct,
                    "mem_used_mb": g.mem_used_mb,
                    "mem_total_mb": g.mem_total_mb,
                    "power_w": g.power_w,
                    "temp_c": g.temp_c,
                    "job_id": g.job_id,
                    "collected_at": _to_iso(g.collected_at),
                }
                for g in session.query(GpuSnapshot)
                .filter(
                    GpuSnapshot.node_name == node_name,
                    GpuSnapshot.collected_at == latest_gpu_snap.collected_at,
                )
                .order_by(GpuSnapshot.gpu_index)
                .all()
            ]

        result = {
            "node": {
                "name": snap.node_name,
                "state": snap.state,
                "gres_config": snap.gres_config,
                "gres_total": snap.gres_total,
                "gres_alloc": snap.gres_alloc,
                "total_cpus": snap.total_cpus,
                "alloc_cpus": snap.alloc_cpus,
                "cpu_load": snap.cpu_load,
                "total_memory_mb": snap.total_memory_mb,
                "alloc_memory_mb": snap.alloc_memory_mb,
                "reason": snap.reason,
                "collected_at": _to_iso(snap.collected_at),
            },
            "running_jobs": [
                {
                    "job_id": j.job_id,
                    "user": j.user,
                    "alloc_gpus": j.alloc_gpus,
                    "alloc_cpus": j.alloc_cpus,
                    "elapsed_seconds": j.elapsed_seconds,
                    "submit_time": _to_iso(j.submit_time),
                    "start_time": _to_iso(j.start_time),
                }
                for j in running
            ],
            "gpu_snapshots": gpu_snapshots,
        }
        cache.set(cache_key, result, TTL_NODE_DETAIL)
        return result


# ---- 个人使用记录 ----

def _time_range(range_name: str, start: Optional[str], end: Optional[str]):
    """解析时间范围参数，返回 (start_dt, end_dt)。"""
    tz = timezone(timedelta(hours=8))
    now = datetime.now(tz)
    if range_name == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0), now
    if range_name == "week":
        weekday = now.weekday()
        return (now - timedelta(days=weekday)).replace(hour=0, minute=0, second=0, microsecond=0), now
    if range_name == "month":
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0), now
    if range_name == "custom":
        tz8 = timezone(timedelta(hours=8))
        s = datetime.fromisoformat(start).replace(tzinfo=tz8) if start else now - timedelta(days=30)
        e = datetime.fromisoformat(end).replace(tzinfo=tz8, hour=23, minute=59, second=59) if end else now
        return s, e
    # all
    return datetime(2000, 1, 1, tzinfo=tz), now


@router.get("/users/{user}/stats")
def user_stats(
    user: str,
    range: str = Query("today", pattern="^(today|week|month|all|custom)$"),
    start: Optional[str] = None,
    end: Optional[str] = None,
):
    """个人使用统计：任务数、时长、成功率、GPU 卡时。"""
    start_dt, end_dt = _time_range(range, start, end)

    with get_session() as session:
        jobs = (
            session.query(Job)
            .filter(Job.user == user, Job.submit_time >= start_dt, Job.submit_time <= end_dt)
            .all()
        )

        by_state: dict[str, int] = {}
        total_elapsed = 0
        total_gpu_seconds = 0
        for j in jobs:
            state = j.state or "UNKNOWN"
            by_state[state] = by_state.get(state, 0) + 1
            if j.elapsed_seconds:
                total_elapsed += j.elapsed_seconds
            if j.elapsed_seconds and j.alloc_gpus:
                total_gpu_seconds += j.alloc_gpus * j.elapsed_seconds

        ended = sum(by_state.get(s, 0) for s in ("COMPLETED", "FAILED", "TIMEOUT", "CANCELLED"))
        completed = by_state.get("COMPLETED", 0)
        success_rate = (completed / ended) if ended else None

        return {
            "user": user,
            "range": range,
            "start": _to_iso(start_dt),
            "end": _to_iso(end_dt),
            "total_jobs": len(jobs),
            "by_state": by_state,
            "total_elapsed_seconds": total_elapsed,
            "total_gpu_hours": round(total_gpu_seconds / 3600, 2),
            "success_rate": round(success_rate, 4) if success_rate is not None else None,
        }


@router.get("/users/{user}/jobs")
def user_jobs(
    user: str,
    range: str = Query("today", pattern="^(today|week|month|all|custom)$"),
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = Query(100, le=500),
):
    """个人作业明细列表。"""
    start_dt, end_dt = _time_range(range, start, end)

    with get_session() as session:
        jobs = (
            session.query(Job)
            .filter(Job.user == user, Job.submit_time >= start_dt, Job.submit_time <= end_dt)
            .order_by(desc(Job.submit_time))
            .limit(limit)
            .all()
        )
        return {
            "jobs": [
                {
                    "job_id": j.job_id,
                    "job_name": j.job_name,
                    "state": j.state,
                    "partition": j.partition,
                    "alloc_gpus": j.alloc_gpus,
                    "alloc_cpus": j.alloc_cpus,
                    "alloc_mem_mb": j.alloc_mem_mb,
                    "submit_time": _to_iso(j.submit_time),
                    "start_time": _to_iso(j.start_time),
                    "end_time": _to_iso(j.end_time),
                    "elapsed_seconds": j.elapsed_seconds,
                    "timelimit_minutes": j.timelimit_minutes,
                    "node_list": j.node_list,
                }
                for j in jobs
            ]
        }
