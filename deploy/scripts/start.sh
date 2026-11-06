#!/bin/bash
# 启动 slurm-monitor 服务
# 用法：./start.sh [--build]
#   --build  强制重新构建镜像
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${SCRIPT_DIR}/.."
ROOT_DIR="${DEPLOY_DIR}/.."

# 确保 pgdata 目录存在（PG 容器以 uid 999 写入）
mkdir -p "${DEPLOY_DIR}/pgdata"

# 检查 .env 是否存在
if [ ! -f "${ROOT_DIR}/.env" ]; then
  echo "ERROR: .env not found. Run: cp .env.example .env && edit it"
  exit 1
fi

# 必填项检查
REQUIRED_VARS=("SLURM_USER_TOKEN" "POSTGRES_PASSWORD" "SSH_PASSWORD")
for var in "${REQUIRED_VARS[@]}"; do
  value=$(grep "^${var}=" "${ROOT_DIR}/.env" | cut -d= -f2-)
  if [ -z "$value" ] || [ "$value" = "your_jwt_token_here" ] || [ "$value" = "your_password_here" ]; then
    echo "ERROR: ${var} is not set in .env"
    exit 1
  fi
done

# 构建或启动
if [[ "$1" == "--build" ]]; then
  echo "[start] building images..."
  docker compose -f "${DEPLOY_DIR}/docker-compose.yml" build
fi

echo "[start] starting services..."
docker compose -f "${DEPLOY_DIR}/docker-compose.yml" up -d

echo "[start] waiting for services to be healthy..."
sleep 3
docker compose -f "${DEPLOY_DIR}/docker-compose.yml" ps
echo ""
echo "[start] done. Web UI: http://localhost:${WEB_PORT:-8000}"
