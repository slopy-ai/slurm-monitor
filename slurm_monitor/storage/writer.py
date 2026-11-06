"""存储层：作业写入与查询"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .db import get_session
from .models import Job, NodeSnapshot, NodeAggStat, GpuSnapshot
from ..parser.tres import parse_tres

logger = logging.getLogger(__name__)


def _parse_rest_exit_code(exit_code_data) -> Optional[int]:
    """解析 slurmrestd 的 exit_code 字段。

    slurmrestd 可能返回：
    - 整数：直接返回
    - 字典 {"status": 0, "return_code": 0}：取 return_code
    - 字符串 "0:0"：解析第一个数字
    - None/空：返回 None
    """
    if exit_code_data is None:
        return None

    # 如果是整数，直接返回
    if isinstance(exit_code_data, int):
        return exit_code_data

    # 如果是字典（新版slurmrestd格式）
    if isinstance(exit_code_data, dict):
        return exit_code_data.get("return_code") or exit_code_data.get("status")

    # 如果是字符串 "0:0" 格式
    if isinstance(exit_code_data, str):
        parts = exit_code_data.split(":", 1)
        try:
            return int(parts[0])
        except (ValueError, IndexError):
            return None

    return None


def upsert_jobs_from_sacct(records: list) -> int:
    """将 sacct 解析结果写入数据库，按 (cluster, job_id, submit_time) upsert。

    只处理主作业记录（非 step），step 由调用方另行处理。
    返回写入条数。
    """
    written = 0
    with get_session() as session:
        for rec in records:
            if rec.is_step:
                continue
            tres = parse_tres(rec.alloc_tres or "")
            req_tres = parse_tres(rec.req_tres or "")

            values = {
                "cluster": "",
                # 数组子任务各有唯一的真实作业号（JobIDRaw），用它做 job_id，
                # 否则同一数组的子任务会因唯一键 (cluster, job_id, submit_time) 互相覆盖
                "job_id": (rec.job_id_raw or rec.job_id) if rec.array_task_id and rec.array_task_id.isdigit() else rec.job_id,
                "job_id_raw": rec.job_id_raw,
                "job_name": rec.job_name,
                "array_task_id": int(rec.array_task_id) if rec.array_task_id and rec.array_task_id.isdigit() else None,
                "array_job_id": rec.job_id if rec.array_task_id and rec.array_task_id.isdigit() else None,
                "user": rec.user,
                "uid": rec.uid,
                "account": rec.account,
                "group": rec.group,
                "partition": rec.partition,
                "qos": rec.qos,
                "state": rec.state,
                "exit_code": rec.exit_code,
                "submit_time": rec.submit_time,
                "start_time": rec.start_time,
                "end_time": rec.end_time,
                "eligible_time": rec.eligible_time,
                "elapsed_seconds": rec.elapsed_seconds,
                "suspended_seconds": rec.suspended_seconds,
                "timelimit_minutes": rec.timelimit_minutes,
                "node_list": rec.node_list,
                "nodes_alloc": rec.nodes_alloc,
                "alloc_cpus": tres.get("cpu"),
                "alloc_gpus": tres.get("gpu") or 0,
                "alloc_mem_mb": tres.get("mem_mb"),
                "req_cpus": req_tres.get("cpu"),
                "req_gpus": req_tres.get("gpu") or 0,
                "req_mem_mb": req_tres.get("mem_mb"),
                "alloc_tres_raw": rec.alloc_tres,
                "req_tres_raw": rec.req_tres,
                "total_cpu_seconds": rec.total_cpu_seconds,
                "max_rss_mb": rec.max_rss_mb,
                "user_source": "sacct" if rec.user else "unknown",
                "updated_at": datetime.now(timezone.utc),
            }

            stmt = (
                pg_insert(Job)
                .values(**values)
                .on_conflict_do_update(
                    constraint="uq_job_identity",
                    set_={
                        k: v for k, v in values.items()
                        if k not in ("cluster", "job_id", "submit_time")
                    },
                )
            )
            session.execute(stmt)
            written += 1

    logger.debug("upserted %d job records from sacct", written)
    return written


def patch_job_user(job_id: int, user: str, uid: Optional[int] = None) -> bool:
    """为 user 为空的作业补全用户信息（REST API 缺失时调用）。"""
    with get_session() as session:
        job = session.query(Job).filter(Job.job_id == job_id, Job.user.is_(None)).first()
        if job is None:
            return False
        job.user = user
        if uid is not None:
            job.uid = uid
        job.user_source = "sacct"
        job.updated_at = datetime.now(timezone.utc)
    return True


def save_node_snapshot(nodes: list[dict]) -> None:
    """保存节点状态快照，nodes 为 slurmrestd /nodes 返回的原始列表。"""
    if not nodes:
        return
    now = datetime.now(timezone.utc)
    with get_session() as session:
        for node in nodes:
            name = node.get("name", "")
            if not name:
                continue

            gres_raw = ",".join(
                node.get("gres", []) if isinstance(node.get("gres"), list)
                else [str(node.get("gres", ""))]
            )
            gres_total = _parse_gres_count(gres_raw)
            gres_alloc = _parse_gres_count(
                ",".join(
                    node.get("gres_used", []) if isinstance(node.get("gres_used"), list)
                    else [str(node.get("gres_used", ""))]
                )
            )

            tres_alloc = parse_tres(node.get("tres_alloc_str", "") or "")

            # tres_alloc_str は空のことがあるため、直接フィールドでフォールバック
            alloc_cpus = tres_alloc.get("cpu")
            if alloc_cpus is None:
                alloc_cpus = node.get("alloc_cpus") or node.get("allocated_cpus")

            alloc_memory_mb = tres_alloc.get("mem_mb")
            if alloc_memory_mb is None:
                raw_mem = node.get("alloc_memory") or node.get("allocated_memory")
                if raw_mem is not None:
                    # slurmrestd が MB 単位の整数を返す場合はそのまま使う
                    alloc_memory_mb = int(raw_mem)

            snap = NodeSnapshot(
                node_name=name,
                state=node.get("state", ""),
                partition=",".join(node.get("partitions", [])) if isinstance(node.get("partitions"), list) else node.get("partition", ""),
                avail=node.get("state_flags", ""),
                total_cpus=node.get("cpus", None),
                alloc_cpus=alloc_cpus,
                idle_cpus=node.get("cpus_load", None),
                total_memory_mb=node.get("real_memory", None),
                alloc_memory_mb=alloc_memory_mb,
                gres_config=gres_raw,
                gres_total=gres_total,
                gres_alloc=gres_alloc,
                cpu_load=node.get("cpu_load", None),
                reason=node.get("reason", ""),
                collected_at=now,
            )
            session.add(snap)


def save_gpu_snapshots(node_name: str, gpu_rows: list[dict]) -> None:
    """将 GPU 采集结果写入 GpuSnapshot 表。

    gpu_rows 兼容两种来源：
    - nvidia-smi CSV 解析（parse_nvidia_smi）
    - agent.py 注入采集（agent_rows_to_db，额外提供 power_limit_w 和 proc_json）
    """
    if not gpu_rows:
        return
    now = datetime.now(timezone.utc)
    with get_session() as session:
        for row in gpu_rows:
            snap = GpuSnapshot(
                node_name=node_name,
                gpu_index=row.get("index", 0),
                gpu_uuid=row.get("uuid"),
                gpu_name=row.get("name"),
                job_id=row.get("job_id"),
                util_pct=row.get("util_pct"),
                mem_used_mb=row.get("mem_used_mb"),
                mem_total_mb=row.get("mem_total_mb"),
                power_w=row.get("power_w"),
                power_limit_w=row.get("power_limit_w"),
                temp_c=row.get("temp_c"),
                fan_pct=row.get("fan_pct"),
                proc_json=row.get("proc_json"),
                collected_at=now,
            )
            session.add(snap)


def save_node_host_stats(node_name: str, host: dict) -> None:
    """写入一条主机指标；agent 一项都没读到（空 dict）时不写。"""
    if not host:
        return
    from .models import NodeHostStat
    with get_session() as session:
        session.add(NodeHostStat(
            node_name=node_name,
            cpu_count=host.get("cpu_count"),
            cpu_util_pct=host.get("cpu_util_pct"),
            load1=host.get("load1"), load5=host.get("load5"), load15=host.get("load15"),
            mem_total_mb=host.get("mem_total_mb"),
            mem_used_mb=host.get("mem_used_mb"),
            mem_avail_mb=host.get("mem_avail_mb"),
            mem_cache_mb=host.get("mem_cache_mb"),
            swap_total_mb=host.get("swap_total_mb"),
            swap_free_mb=host.get("swap_free_mb"),
            cpu_cores=host.get("cpu_cores"),
            cpu_model=host.get("cpu_model"),
            sockets=host.get("sockets"),
            cpu_temp_c=host.get("cpu_temp_c"),
            uptime_s=host.get("uptime_s"),
            collected_at=datetime.now(timezone.utc),
        ))


def load_enabled_node_ip_map() -> list[dict]:
    """读取 node_ip_map 表中 enabled=1 的节点连接配置。

    返回 [{node_name, ip, port, ssh_user, ssh_password}, ...]
    """
    from .models import NodeIpMap
    with get_session() as session:
        rows = (
            session.query(NodeIpMap)
            .filter(NodeIpMap.enabled == 1)
            .order_by(NodeIpMap.node_name)
            .all()
        )
        return [
            {
                "node_name": r.node_name,
                "ip": r.ip,
                "port": r.port,
                "ssh_user": r.ssh_user,
                "ssh_password": r.ssh_password,
            }
            for r in rows
        ]


def aggregate_node_daily(node_name: str, day_ts: datetime) -> None:
    """将 NodeSnapshot 和 GpuSnapshot 聚合为天级统计，写入 NodeAggStat(1d)。

    day_ts 为 UTC 当天零点。由采集器每小时调用一次（幂等）。
    """
    from datetime import timedelta
    from sqlalchemy.dialects.postgresql import insert as pg_insert
    from .models import NodeAggStat

    day_start = day_ts.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=timezone.utc)
    day_end = day_start + timedelta(days=1)

    with get_session() as session:
        snap_sql = text("""
            SELECT
                AVG(CASE WHEN gres_total > 0 THEN gres_alloc::float / gres_total * 100 END) AS avg_alloc_pct,
                MAX(CASE WHEN gres_total > 0 THEN gres_alloc::float / gres_total * 100 END) AS max_alloc_pct,
                MAX(gres_total) AS gres_total,
                COUNT(*) AS samples
            FROM node_snapshots
            WHERE node_name = :node
              AND collected_at >= :start AND collected_at < :end
        """)
        snap = session.execute(snap_sql, {"node": node_name, "start": day_start, "end": day_end}).fetchone()

        gpu_sql = text("""
            SELECT
                AVG(util_pct)::float  AS avg_util,
                MAX(util_pct)         AS max_util,
                AVG(mem_used_mb)::float AS avg_mem,
                MAX(mem_used_mb)      AS max_mem,
                AVG(power_w)::float   AS avg_power,
                AVG(temp_c)::float    AS avg_temp,
                COUNT(*)              AS samples
            FROM gpu_snapshots
            WHERE node_name = :node
              AND collected_at >= :start AND collected_at < :end
        """)
        gpu = session.execute(gpu_sql, {"node": node_name, "start": day_start, "end": day_end}).fetchone()

        vals = {
            "node_name": node_name,
            "bucket_ts": day_start,
            "granularity": "1d",
            "avg_alloc_pct": snap.avg_alloc_pct if snap else None,
            "max_alloc_pct": snap.max_alloc_pct if snap else None,
            "gres_total": snap.gres_total if snap else None,
            "sample_count": snap.samples if snap else 0,
            "avg_gpu_util_pct": gpu.avg_util if gpu and gpu.samples else None,
            "max_gpu_util_pct": gpu.max_util if gpu and gpu.samples else None,
            "avg_mem_used_mb": gpu.avg_mem if gpu and gpu.samples else None,
            "max_mem_used_mb": gpu.max_mem if gpu and gpu.samples else None,
            "avg_power_w": gpu.avg_power if gpu and gpu.samples else None,
            "avg_temp_c": gpu.avg_temp if gpu and gpu.samples else None,
        }

        stmt = (
            pg_insert(NodeAggStat)
            .values(**vals)
            .on_conflict_do_update(
                constraint="uq_node_agg",
                set_={k: v for k, v in vals.items() if k not in ("node_name", "bucket_ts", "granularity")},
            )
        )
        session.execute(stmt)

def purge_old_snapshots(node_raw_days: int = 30, gpu_raw_days: int = 30) -> None:
    """清理过期原始数据（默认保留 30 天，与文档一致，节点页多日对比依赖它），防止快照表无限膨胀。"""
    from datetime import timedelta
    cutoff_node = datetime.now(timezone.utc) - timedelta(days=node_raw_days)
    cutoff_gpu = datetime.now(timezone.utc) - timedelta(days=gpu_raw_days)
    with get_session() as session:
        n = session.execute(
            text("DELETE FROM node_snapshots WHERE collected_at < :cutoff"),
            {"cutoff": cutoff_node},
        ).rowcount
        g = session.execute(
            text("DELETE FROM gpu_snapshots WHERE collected_at < :cutoff"),
            {"cutoff": cutoff_gpu},
        ).rowcount
        h = session.execute(
            text("DELETE FROM node_host_stats WHERE collected_at < :cutoff"),
            {"cutoff": cutoff_gpu},
        ).rowcount
        logger.info("purged %d node snapshots, %d gpu snapshots, %d host stats", n, g, h)


def _parse_gres_count(gres_str: str) -> int:
    """从 GRES 字符串中提取 GPU 总数量。"""
    total = 0
    for part in gres_str.split(","):
        part = part.strip()
        if not part.startswith("gpu:"):
            continue
        part = part.split("(")[0]
        segments = part.split(":")
        if segments and segments[-1].isdigit():
            total += int(segments[-1])
    return total


def _epoch_to_dt(v) -> Optional[datetime]:
    """slurmrestd 的时间是 Unix 秒，0/None 表示未设置。"""
    try:
        v = int(v)
    except (TypeError, ValueError):
        return None
    return datetime.fromtimestamp(v, tz=timezone.utc) if v > 0 else None


def upsert_jobs_from_rest(rest_jobs: list[dict], prune_pending: bool = False) -> int:
    """将 slurmrestd 实时作业（运行中/排队）upsert 到 jobs 表。

    sacct 默认不返回尚未启动的作业，所以排队作业只能来自 REST；运行中作业用 REST 补齐
    sacct 同步前的空窗（最长 5 分钟），并带上 sacct 没有的优先级和排队原因。
    键与 sacct 路径一致 (cluster, job_id, submit_time)，作业结束后由 sacct 同步写入最终状态。
    数组子任务以各自真实作业号为 job_id，父作业号存 array_job_id。

    prune_pending=True 时删除已不在 REST 排队列表里的 PENDING 行（作业已启动或被取消，
    仅在 REST 本轮请求成功时才能打开，否则故障会把排队数据清空）。返回写入条数。
    """
    from .models import Job, JobHistory
    written = 0
    pending_ids: set[int] = set()
    with get_session() as session:
        # slurmrestd 的 user_name 经常为空但 user_id 可靠：用库里已知的 uid→用户名补全
        uid_user: dict[int, str] = {}
        for model in (JobHistory, Job):
            for uid, name in session.query(model.uid, model.user).filter(
                model.uid.isnot(None), model.user.isnot(None)
            ).distinct().all():
                uid_user[uid] = name

        for j in rest_jobs:
            state = j.get("job_state")
            if isinstance(state, list):  # 部分 API 版本返回列表
                state = state[0] if state else None
            if state not in ("PENDING", "RUNNING") or j.get("job_id") is None:
                continue
            submit = _epoch_to_dt(j.get("submit_time"))
            if submit is None:
                continue

            raw_id = int(j["job_id"])
            array_parent = j.get("array_job_id") or 0
            task = j.get("array_task_id")
            alloc = parse_tres(j.get("tres_alloc_str") or "")
            req = parse_tres(j.get("tres_req_str") or "")
            reason = j.get("state_reason")

            values = {
                "cluster": "",
                    "job_id": raw_id,  # 数组子任务也用自己的真实作业号，保证唯一
                "job_id_raw": raw_id,
                "job_name": j.get("name") or None,
                "array_task_id": int(task) if isinstance(task, int) else None,
                "array_job_id": int(array_parent) if isinstance(task, int) and array_parent else None,
                "user": j.get("user_name") or uid_user.get(j.get("user_id")),
                "uid": j.get("user_id"),
                "account": j.get("account") or None,
                "partition": j.get("partition") or None,
                "qos": j.get("qos") or None,
                "state": state,
                "exit_code": _parse_rest_exit_code(j.get("exit_code")),
                "submit_time": submit,
                "start_time": _epoch_to_dt(j.get("start_time")) if state == "RUNNING" else None,
                "eligible_time": _epoch_to_dt(j.get("eligible_time")),
                "timelimit_minutes": j.get("time_limit") if isinstance(j.get("time_limit"), int) else None,
                # 排队作业尚未分配节点时 REST 返回 "None assigned"，按空处理
                "node_list": (j.get("nodes") or None) if j.get("nodes") != "None assigned" else None,
                "nodes_alloc": j.get("node_count"),
                "alloc_cpus": alloc.get("cpu"),
                "alloc_gpus": alloc.get("gpu") or 0,
                "alloc_mem_mb": alloc.get("mem_mb"),
                "req_cpus": req.get("cpu") or j.get("cpus"),
                "req_gpus": req.get("gpu") or 0,
                "req_mem_mb": req.get("mem_mb"),
                "alloc_tres_raw": j.get("tres_alloc_str") or None,
                "req_tres_raw": j.get("tres_req_str") or None,
                "priority": j.get("priority"),
                # 运行中作业的 state_reason 为 "None"，统一存 NULL
                "pending_reason": reason if state == "PENDING" and reason not in (None, "", "None") else None,
                "user_source": "rest" if j.get("user_name") else ("uid_map" if uid_user.get(j.get("user_id")) else "unknown"),
                "updated_at": datetime.now(timezone.utc),
            }
            if state == "PENDING":
                pending_ids.add(values["job_id"])

            # 冲突时不要用 REST 的空值覆盖 sacct 已补全的字段（如用户名）；
            # pending_reason / node_list 例外：状态变化后必须能被清空
            update = {
                k: v for k, v in values.items()
                if k not in ("cluster", "job_id", "submit_time") and (v is not None or k in ("pending_reason", "node_list"))
            }
            session.execute(
                pg_insert(Job).values(**values).on_conflict_do_update(constraint="uq_job_identity", set_=update)
            )
            written += 1

        if prune_pending:
            q = session.query(Job).filter(Job.state == "PENDING")
            if pending_ids:
                q = q.filter(~Job.job_id.in_(pending_ids))
            q.delete(synchronize_session=False)

    logger.debug("upserted %d realtime job records from REST", written)
    return written
