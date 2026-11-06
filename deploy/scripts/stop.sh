#!/bin/bash
# 停止 slurm-monitor 服务
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "[stop] stopping services..."
docker compose -f "${SCRIPT_DIR}/../docker-compose.yml" down
echo "[stop] done. pgdata volume is preserved."
