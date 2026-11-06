#!/bin/bash
# 获取 Slurm JWT token 并更新 .env
# 用法：./refresh_token.sh
# 每次 token 过期（默认 24h）后重新执行

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="${SCRIPT_DIR}/../../.env"
SSH_HOST="${SSH_HOST:-192.168.112.182}"
SSH_USER="${SSH_USER:-djs}"

if [ ! -f "$ENV_FILE" ]; then
  echo "ERROR: .env not found at $ENV_FILE"
  exit 1
fi

echo "[token] fetching JWT from ${SSH_USER}@${SSH_HOST}..."

# 获取 token（lifespan=86400 为 24 小时）
RAW_TOKEN=$(ssh -o StrictHostKeyChecking=no "${SSH_USER}@${SSH_HOST}" "scontrol token lifespan=86400" 2>/dev/null)

# 提取 token 值（输出格式：SLURM_JWT=eyJ...）
TOKEN=$(echo "$RAW_TOKEN" | grep -oP '(?<=SLURM_JWT=)\S+')

if [ -z "$TOKEN" ]; then
  echo "ERROR: failed to get token. Output was: $RAW_TOKEN"
  exit 1
fi

# 更新 .env 中的 SLURM_USER_TOKEN
if grep -q "^SLURM_USER_TOKEN=" "$ENV_FILE"; then
  sed -i "s|^SLURM_USER_TOKEN=.*|SLURM_USER_TOKEN=${TOKEN}|" "$ENV_FILE"
else
  echo "SLURM_USER_TOKEN=${TOKEN}" >> "$ENV_FILE"
fi

echo "[token] SLURM_USER_TOKEN updated (expires in 24h)"
