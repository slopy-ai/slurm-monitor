#!/usr/bin/env python3
"""初始化新增的数据表

创建以下表（幂等，已存在则跳过）：
- cluster_agg_stats: 集群级统计
- job_history: 作业历史记录
- job_efficiency_analysis: 作业效率分析
- pending_job_snapshots: 排队作业快照
- node_ip_map: 计算节点 SSH 连接配置（GPU 采集）
- node_host_stats: 节点主机指标（CPU/内存/负载）
- node_health_status: 节点健康状态（算力节点 + 登录机）

collector / web / aggregator 三个容器启动时都会执行本脚本，
必须能承受并发建表：用 PostgreSQL advisory lock 串行化，
并对 "并发下被别的进程抢先建好" 的重复对象错误做容错。

用法:
    python scripts/init_new_tables.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError, ProgrammingError

from slurm_monitor.storage.db import engine
from slurm_monitor.storage.models import (
    Base,
    ClusterAggStat,
    JobHistory,
    JobEfficiencyAnalysis,
    PendingJobSnapshot,
    NodeIpMap,
    NodeHostStat,
)
from slurm_monitor.storage.node_health import NodeHealthStatus

# 建表迁移专用的 advisory lock key（任意固定值，避免与业务锁冲突）
_INIT_LOCK_KEY = 0x57494E4954  # "WINIT"


def main():
    print("正在创建新表...")

    tables = [
        ClusterAggStat.__table__,
        JobHistory.__table__,
        JobEfficiencyAnalysis.__table__,
        PendingJobSnapshot.__table__,
        NodeIpMap.__table__,
        NodeHostStat.__table__,
        NodeHealthStatus.__table__,
    ]

    # 用 advisory lock 串行化：三个容器同时启动时只有一个真正建表，其余等其完成
    with engine.connect() as lock_conn:
        lock_conn.execute(text("SELECT pg_advisory_lock(:k)"), {"k": _INIT_LOCK_KEY})
        try:
            _create_tables(tables)
            # 迁移：为已存在的表补充新字段
            _migrate_columns()
            # 迁移：为已存在的表补建索引
            _migrate_indexes()
        finally:
            lock_conn.execute(text("SELECT pg_advisory_unlock(:k)"), {"k": _INIT_LOCK_KEY})
            lock_conn.commit()

    print("✅ 完成")


def _create_tables(tables) -> None:
    """逐表创建，容错并发场景下的重复对象错误。"""
    with engine.connect() as conn:
        for table in tables:
            if engine.dialect.has_table(conn, table.name):
                print(f"  表已存在: {table.name}")
                continue
            print(f"  创建表: {table.name}")
            try:
                table.create(engine, checkfirst=True)
            except (ProgrammingError, DBAPIError) as exc:
                # 已持有 advisory lock 时不应发生；保留兜底以免某处绕过锁导致启动失败
                if "already exists" in str(exc):
                    print(f"  表已被其他进程创建: {table.name}")
                else:
                    raise


# 已存在的表补充新字段（幂等）：(表, 列, 类型)
_ADD_COLUMNS = [
    ("gpu_snapshots", "power_limit_w", "FLOAT"),
    ("gpu_snapshots", "proc_json", "TEXT"),
    ("jobs", "priority", "BIGINT"),
    ("jobs", "pending_reason", "VARCHAR(64)"),
    ("jobs", "array_job_id", "BIGINT"),
    ("job_history", "array_job_id", "BIGINT"),
    # 主机指标：rgtop 的 CPU 行 / 每核条带 / 内存细分需要
    ("node_host_stats", "mem_avail_mb", "BIGINT"),
    ("node_host_stats", "mem_cache_mb", "BIGINT"),
    ("node_host_stats", "swap_total_mb", "BIGINT"),
    ("node_host_stats", "swap_free_mb", "BIGINT"),
    ("node_host_stats", "cpu_cores", "TEXT"),
    ("node_host_stats", "cpu_model", "VARCHAR(128)"),
    ("node_host_stats", "sockets", "INTEGER"),
    ("node_host_stats", "cpu_temp_c", "FLOAT"),
    ("node_host_stats", "uptime_s", "BIGINT"),
]


def _migrate_columns():
    """为已存在的表补充新增字段（幂等）。新建的表由 create 处理，这里只处理旧表。"""
    from sqlalchemy import text
    with engine.begin() as conn:
        for table, column, ddl in _ADD_COLUMNS:
            exists = conn.execute(text(
                "SELECT 1 FROM information_schema.columns WHERE table_name=:t AND column_name=:c"
            ), {"t": table, "c": column}).first()
            if exists:
                continue
            conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}"))
            print(f"  {table} 新增字段: {column}")


# 索引（幂等）：create_all 只对新库生效，旧库需要这里补建。
# (索引名, 表, 定义)
_INDEXES = [
    # 「每节点最新一帧 / 最近一条」的查询走 Index Only Scan，代价只与节点数相关
    ("ix_gpu_snapshots_node_ts", "gpu_snapshots", "(node_name, collected_at DESC)"),
    ("ix_node_host_stats_node_ts", "node_host_stats", "(node_name, collected_at DESC)"),
    # 作业效率分析：按作业号取 GPU 利用率（只索引有作业号的卡记录）
    ("idx_gpu_snapshots_job_time", "gpu_snapshots", "(job_id, collected_at) WHERE job_id IS NOT NULL"),
    # 用户统计 / Top 榜：按用户 + 状态 + 提交时间范围过滤
    ("idx_job_history_user_state_submit", "job_history", '("user", state, submit_time)'),
    # 节点列表 / 概览的排队作业统计
    ("idx_jobs_partition_state", "jobs", "(partition, state)"),
]


def _migrate_indexes():
    """为已存在的表补建索引（幂等）。"""
    from sqlalchemy import text
    with engine.begin() as conn:
        for name, table, definition in _INDEXES:
            exists = conn.execute(text(
                "SELECT 1 FROM pg_indexes WHERE tablename=:t AND indexname=:n"
            ), {"t": table, "n": name}).first()
            if exists:
                continue
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS {name} ON {table} {definition}"))
            print(f"  {table} 新增索引: {name}")


if __name__ == "__main__":
    main()
