#!/bin/bash
set -e

# 等待 PostgreSQL 就绪（healthcheck 已保证，但双重保险）
until pg_isready -h "${POSTGRES_HOST:-postgres}" -U "${POSTGRES_USER:-slurm_monitor}" -d "${POSTGRES_DB:-slurm_monitor}" -q; do
  echo "[entrypoint] waiting for postgres..."
  sleep 2
done

# 检查 jobs 表是否存在；不存在则执行 init_db 建表
TABLE_EXISTS=$(PGPASSWORD="${POSTGRES_PASSWORD:-changeme}" psql \
  -h "${POSTGRES_HOST:-postgres}" \
  -U "${POSTGRES_USER:-slurm_monitor}" \
  -d "${POSTGRES_DB:-slurm_monitor}" \
  -tAc "SELECT to_regclass('public.jobs');" 2>/dev/null || echo "")

if [ "$TABLE_EXISTS" = "" ] || [ "$TABLE_EXISTS" = "NULL" ]; then
  echo "[entrypoint] tables not found, running init_db..."
  python scripts/init_db.py
  echo "[entrypoint] init_db done"
else
  echo "[entrypoint] tables already exist, skipping init"
fi

# 确保新增的表已创建（幂等）
echo "[entrypoint] checking new tables (node_ip_map, cluster_agg_stats, etc.)..."
python scripts/init_new_tables.py

echo "[entrypoint] starting: $*"
exec "$@"
