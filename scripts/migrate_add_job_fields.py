#!/usr/bin/env python3
"""数据库迁移：为 jobs 表新增 total_cpu_seconds / max_rss_mb 两列，
并创建 user_mappings 表（用户名映射）。

幂等：已存在的列/表不会重复创建。

用法:
    docker exec slurm-monitor-web python3 /app/scripts/migrate_add_job_fields.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from sqlalchemy import text, inspect as sa_inspect
from slurm_monitor.storage.db import engine
from slurm_monitor.storage.models import UserMapping


def column_exists(conn, table: str, column: str) -> bool:
    insp = sa_inspect(engine)
    cols = [c["name"] for c in insp.get_columns(table)]
    return column in cols


def table_exists(conn, table: str) -> bool:
    return sa_inspect(engine).has_table(table)


def main():
    with engine.connect() as conn:
        # ---- jobs 表：新增两列 ----
        for col, dtype in [
            ("total_cpu_seconds", "DOUBLE PRECISION"),
            ("max_rss_mb", "BIGINT"),
        ]:
            if column_exists(conn, "jobs", col):
                print(f"  列已存在，跳过: jobs.{col}")
            else:
                conn.execute(text(f"ALTER TABLE jobs ADD COLUMN {col} {dtype}"))
                conn.commit()
                print(f"  ✅ 已添加列: jobs.{col}")

        # ---- user_mappings 表 ----
        if table_exists(conn, "user_mappings"):
            print("  表已存在，跳过: user_mappings")
        else:
            UserMapping.__table__.create(engine)
            print("  ✅ 已创建表: user_mappings")

    print("迁移完成")


if __name__ == "__main__":
    main()
