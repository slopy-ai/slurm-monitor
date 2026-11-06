#!/usr/bin/env python3
"""修正 job_history 表中 timelimit_minutes 字段的错误数据

历史问题：_parse_timelimit 函数返回的是秒数但字段名是 timelimit_minutes，
导致存储时未转换，所有 timelimit_minutes 都是实际分钟数的 60 倍。

修复策略：
1. 将所有 timelimit_minutes > 1440（大于24小时=1天）的记录除以60
2. 这是一个安全的启发式规则，因为很少有作业时限超过24小时
"""

import sys
from sqlalchemy import text

# 添加父目录到路径以导入项目模块
sys.path.insert(0, '/app')

from slurm_monitor.storage.db import get_session

def main():
    with get_session() as session:
        # 查看需要修复的记录数
        result = session.execute(
            text("SELECT COUNT(*) FROM job_history WHERE timelimit_minutes > 1440")
        )
        count_before = result.scalar()
        print(f"需要修正的记录数: {count_before}")

        if count_before == 0:
            print("没有需要修正的数据")
            return

        # 执行修正
        result = session.execute(
            text("""
                UPDATE job_history
                SET timelimit_minutes = timelimit_minutes / 60
                WHERE timelimit_minutes > 1440
            """)
        )
        session.commit()

        print(f"已修正 {result.rowcount} 条记录")

        # 验证修正结果（查看几个样例）
        result = session.execute(
            text("""
                SELECT job_id, job_name, elapsed_seconds, timelimit_minutes,
                       CASE WHEN timelimit_minutes > 0
                            THEN ROUND(100.0 * elapsed_seconds / (timelimit_minutes * 60), 2)
                       END as time_eff_pct
                FROM job_history
                WHERE job_id IN (40, 41, 42)
                ORDER BY job_id
                LIMIT 10
            """)
        )

        print("\n修正后的样例数据（作业40-42）:")
        print("job_id | job_name | elapsed_sec | timelimit_min | time_eff%")
        print("-" * 70)
        for row in result:
            print(f"{row[0]:6} | {(row[1] or 'N/A')[:20]:20} | {row[2]:11} | {row[3]:13} | {row[4] or 'N/A'}")

if __name__ == "__main__":
    main()
