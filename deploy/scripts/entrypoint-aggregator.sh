#!/bin/bash
# 聚合器容器启动脚本

set -e

echo "[aggregator] waiting for postgres..."
until pg_isready -h "${POSTGRES_HOST:-postgres}" -p "${POSTGRES_PORT:-5432}" -U "${POSTGRES_USER:-slurm}" > /dev/null 2>&1; do
    sleep 2
done

echo "[aggregator] postgres is ready"

# 初始化新表
echo "[aggregator] initializing new tables..."
python3 /app/scripts/init_new_tables.py

# 首次同步作业历史（最近 7 天）
echo "[aggregator] syncing job history (last 7 days)..."
python3 -m slurm_monitor.sync.job_history_sync --hours 168 || echo "⚠️ job history sync failed"

# 启动聚合器调度器
echo "[aggregator] starting aggregator scheduler..."
exec python3 -m slurm_monitor.aggregator.scheduler
