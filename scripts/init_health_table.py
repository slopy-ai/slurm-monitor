#!/usr/bin/env python3
"""初始化 node_health_status 表

用于记录算力节点和登录机的连接健康状态。
"""

import sys
import os

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import text
from slurm_monitor.storage.db import engine
from slurm_monitor.storage.node_health import NodeHealthStatus


def main():
    print("创建 node_health_status 表...")

    # 创建表
    NodeHealthStatus.__table__.create(engine, checkfirst=True)

    # 创建索引（如果不存在）
    with engine.connect() as conn:
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS ix_node_health_status_node_name
            ON node_health_status (node_name)
        """))
        conn.commit()

    print("✓ node_health_status 表创建成功")


if __name__ == "__main__":
    main()
