#!/bin/bash
# Slurm Monitor 服务控制脚本
# 用法：./control.sh {build|start|stop|restart|status} [service]
#   service: collector|web|aggregator|postgres|redis|all (默认 all)

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEPLOY_DIR="${SCRIPT_DIR}/.."
ROOT_DIR="${DEPLOY_DIR}/.."
COMPOSE_FILE="${DEPLOY_DIR}/docker-compose.yml"
NETWORK="slurm-monitor-net"

# 数据目录：从 .env 读取，未设置时用 deploy/ 下的默认路径
# 可在 .env 或启动前设置 PGDATA_DIR / REDISDATA_DIR 来指向其他挂载点
PGDATA_DIR="${PGDATA_DIR:-${DEPLOY_DIR}/pgdata}"
REDISDATA_DIR="${REDISDATA_DIR:-${DEPLOY_DIR}/redisdata}"

# 颜色输出
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[control]${NC} $*"; }
log_warn() { echo -e "${YELLOW}[control]${NC} $*"; }
log_error() { echo -e "${RED}[control]${NC} $*"; }

# 检测 compose 命令：优先 `docker compose`（v2 插件），否则 `docker-compose`（v1）。
# 可用环境变量覆盖：COMPOSE_CMD="docker-compose" ./control.sh build
# 结果放在数组 COMPOSE 里。
detect_compose() {
  if [ -n "${COMPOSE_CMD:-}" ]; then
    read -r -a COMPOSE <<< "$COMPOSE_CMD"
  elif docker compose version >/dev/null 2>&1; then
    COMPOSE=(docker compose)
  elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE=(docker-compose)
  else
    log_error "Neither 'docker compose' nor 'docker-compose' found. Install the Docker Compose plugin or docker-compose."
    exit 1
  fi
}

# 镜像名在 docker-compose.yml 里固定为 slurm-monitor-<服务>:latest，不依赖 compose 版本
# （不固定的话 v1 会生成 deploy_<服务>，v2 是 deploy-<服务>）。启动前确认镜像存在。
require_image() {
  local image="$1" svc
  svc="${image#slurm-monitor-}"; svc="${svc%:latest}"
  if ! docker image inspect "$image" >/dev/null 2>&1; then
    log_error "Image $image not found. Run: $0 build $svc"
    exit 1
  fi
}

# 检查环境
check_env() {
  if [ ! -f "${ROOT_DIR}/.env" ]; then
    log_error ".env not found. Run: cp .env.example .env && edit it"
    exit 1
  fi

  REQUIRED_VARS=("SLURM_USER_TOKEN" "POSTGRES_PASSWORD")
  for var in "${REQUIRED_VARS[@]}"; do
    value=$(grep "^${var}=" "${ROOT_DIR}/.env" 2>/dev/null | cut -d= -f2- || echo "")
    if [ -z "$value" ] || [[ "$value" =~ your_.*_here ]]; then
      log_error "${var} is not set in .env"
      exit 1
    fi
  done
}

# 服务名映射
service_to_container() {
  case "$1" in
    collector) echo "slurm-monitor-collector" ;;
    web) echo "slurm-monitor-web" ;;
    aggregator) echo "slurm-monitor-aggregator" ;;
    postgres|pg) echo "slurm-monitor-pg" ;;
    redis) echo "slurm-monitor-redis" ;;
    all) echo "slurm-monitor-collector slurm-monitor-web slurm-monitor-aggregator slurm-monitor-pg slurm-monitor-redis" ;;
    *) log_error "Unknown service: $1"; exit 1 ;;
  esac
}

service_to_image() {
  case "$1" in
    collector) echo "slurm-monitor-collector:latest" ;;
    web) echo "slurm-monitor-web:latest" ;;
    aggregator) echo "slurm-monitor-aggregator:latest" ;;
    postgres|pg) echo "postgres:16-alpine" ;;
    redis) echo "redis:7-alpine" ;;
    all) echo "all" ;;
    *) log_error "Unknown service: $1"; exit 1 ;;
  esac
}

# 构建镜像
do_build() {
  local service="${1:-all}"
  check_env
  detect_compose
  mkdir -p "${PGDATA_DIR}"
  mkdir -p "${REDISDATA_DIR}"

  if [ "$service" = "all" ]; then
    log_info "Building all images (${COMPOSE[*]})..."
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" build collector web aggregator
  elif [ "$service" = "postgres" ] || [ "$service" = "pg" ] || [ "$service" = "redis" ]; then
    log_warn "$service uses official image, no build needed"
  else
    log_info "Building $service (${COMPOSE[*]})..."
    "${COMPOSE[@]}" -f "$COMPOSE_FILE" build "$service"
  fi
  log_info "Build complete"
}

# 启动服务（用 docker run 避免 compose up 的 ContainerConfig 问题）
do_start() {
  local service="${1:-all}"
  check_env
  detect_compose
  set -o pipefail  # 确保管道中任何命令失败都能被捕获

  # 确保网络存在
  docker network inspect "$NETWORK" >/dev/null 2>&1 || docker network create "$NETWORK"

  local containers=$(service_to_container "$service")

  # 清理旧的停止容器（包括 hash 前缀的 compose 容器），避免网络别名和数据目录冲突
  # 抽取为函数避免 pg / redis 各维护一份相同逻辑
  _cleanup_stopped_containers() {
    local name_pattern="$1"   # 如 slurm-monitor-pg
    local human_name="$2"     # 如 postgres
    local old_containers
    old_containers=$(docker ps -a --format '{{.Names}}' | grep -E "^(.*_)?${name_pattern}$" || true)
    if [ -n "$old_containers" ]; then
      for old_container in $old_containers; do
        if docker ps --format '{{.Names}}' | grep -q "^${old_container}$"; then
          log_warn "${human_name} container '$old_container' already running, skipping creation"
          return 1  # 1 = 已在运行，调用方 continue 2 跳出外层循环
        else
          log_info "Removing stopped ${human_name} container '$old_container'..."
          docker rm "$old_container"
        fi
      done
    fi
    return 0
  }

  for container in $containers; do
    if [ "$container" = "slurm-monitor-pg" ]; then
      _cleanup_stopped_containers "slurm-monitor-pg" "postgres" || continue
    fi
    if [ "$container" = "slurm-monitor-redis" ]; then
      _cleanup_stopped_containers "slurm-monitor-redis" "redis" || continue
    fi

    # 检查标准名称容器
    if docker ps -a --format '{{.Names}}' | grep -q "^${container}$"; then
      if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
        log_warn "$container is already running"
        continue
      fi

      # 已停止：若镜像已重建（容器指向的镜像 ID ≠ 当前 latest），必须重建容器，
      # 否则 docker start 会继续用旧镜像跑（issues.md #001）。
      # 走到这里的一定是应用容器（pg/redis 已在上面 _cleanup_stopped_containers 处理）。
      local want_image="${container}:latest"
      local want_id have_id
      if docker image inspect "$want_image" >/dev/null 2>&1; then
        # 镜像取 .Id，容器取 .Image（容器没有 .Id 以外的 .Image 键差异见 docker inspect 文档）
        want_id="$(docker inspect --format '{{.Id}}' "$want_image")"
        have_id="$(docker inspect --format '{{.Image}}' "$container")"
        if [ "$want_id" != "$have_id" ]; then
          log_info "$container uses an outdated image, recreating..."
          docker rm "$container" >/dev/null
        else
          log_info "Starting existing container $container..."
          docker start "$container"
          continue
        fi
      else
        log_info "Starting existing container $container..."
        docker start "$container"
        continue
      fi
    fi

    log_info "Creating and starting $container..."
    case "$container" in
      slurm-monitor-pg)
        # 应用通过主机名 postgres / redis 连接（compose 会自动加这个别名，docker run 不会）
        # PG_IMAGE 默认 postgres:16-alpine，体积小且稳定；不同机器可通过环境变量覆盖
        local pg_image="${PG_IMAGE:-postgres:16-alpine}"
        docker run -d --name "$container" \
          --network "$NETWORK" --network-alias postgres \
          --restart unless-stopped \
          -v "${PGDATA_DIR}:/var/lib/postgresql/data" \
          -e POSTGRES_USER=slurm_monitor \
          -e POSTGRES_PASSWORD=$(grep '^POSTGRES_PASSWORD=' "${ROOT_DIR}/.env" | cut -d= -f2-) \
          -e POSTGRES_DB=slurm_monitor \
          "$pg_image"
        sleep 3  # 等待 PG 启动
        ;;
      slurm-monitor-redis)
        local redis_image="${REDIS_IMAGE:-redis:7-alpine}"
        docker run -d --name "$container" \
          --network "$NETWORK" --network-alias redis \
          --restart unless-stopped \
          -v "${REDISDATA_DIR}:/data" \
          "$redis_image"
        ;;
      slurm-monitor-collector)
        require_image slurm-monitor-collector:latest
        docker run -d --name "$container" \
          --network "$NETWORK" \
          --restart unless-stopped \
          --env-file "${ROOT_DIR}/.env" \
          -e POSTGRES_HOST=postgres \
          -e REDIS_HOST=redis \
          slurm-monitor-collector:latest python -m slurm_monitor.collector
        ;;
      slurm-monitor-web)
        require_image slurm-monitor-web:latest
        docker run -d --name "$container" \
          --network "$NETWORK" \
          --restart unless-stopped \
          --env-file "${ROOT_DIR}/.env" \
          -e POSTGRES_HOST=postgres \
          -e REDIS_HOST=redis \
          -e WEB_HOST=0.0.0.0 \
          -e WEB_PORT=8000 \
          -p 8000:8000 \
          slurm-monitor-web:latest python -m slurm_monitor.web
        ;;
      slurm-monitor-aggregator)
        require_image slurm-monitor-aggregator:latest
        docker run -d --name "$container" \
          --network "$NETWORK" \
          --restart unless-stopped \
          --env-file "${ROOT_DIR}/.env" \
          -e POSTGRES_HOST=postgres \
          -e REDIS_HOST=redis \
          --entrypoint /entrypoint-aggregator.sh \
          slurm-monitor-aggregator:latest
        ;;
    esac
  done

  sleep 2
  log_info "Services started. Web UI: http://localhost:8000"
}

# 停止服务
do_stop() {
  local service="${1:-all}"
  local containers=$(service_to_container "$service")

  for container in $containers; do
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
      log_info "Stopping $container..."
      docker stop "$container"
    else
      log_warn "$container is not running"
    fi
  done
  log_info "Stop complete"
}

# 重启服务
do_restart() {
  local service="${1:-all}"
  log_info "Restarting $service..."

  if [ "$service" = "all" ]; then
    do_stop all
    sleep 2
    do_start all
  else
    local container=$(service_to_container "$service")
    for c in $container; do
      if docker ps -a --format '{{.Names}}' | grep -q "^${c}$"; then
        log_info "Removing old container $c..."
        docker rm -f "$c"
      fi
    done
    do_start "$service"
  fi
}

# 查看状态
do_status() {
  log_info "Service status:"
  echo ""
  docker ps -a --filter "name=slurm-monitor" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
  echo ""
  log_info "Recent logs (last 5 lines per service):"
  for container in slurm-monitor-collector slurm-monitor-web slurm-monitor-aggregator; do
    if docker ps --format '{{.Names}}' | grep -q "^${container}$"; then
      echo ""
      echo "=== $container ==="
      docker logs "$container" --tail 5 2>&1 | sed 's/^/  /'
    fi
  done
}

# 主逻辑
CMD="${1:-}"
SERVICE="${2:-all}"

case "$CMD" in
  build)
    do_build "$SERVICE"
    ;;
  start)
    do_start "$SERVICE"
    ;;
  stop)
    do_stop "$SERVICE"
    ;;
  restart)
    do_restart "$SERVICE"
    ;;
  status)
    do_status
    ;;
  "")
    log_error "Usage: $0 {build|start|stop|restart|status} [service]"
    echo ""
    echo "Services: collector, web, aggregator, postgres, redis, all (default)"
    echo ""
    echo "Examples:"
    echo "  $0 build              # 构建所有镜像"
    echo "  $0 build web          # 只构建 web"
    echo "  $0 start              # 启动所有服务"
    echo "  $0 restart web        # 重启 web"
    echo "  $0 stop               # 停止所有服务"
    echo "  $0 status             # 查看状态"
    exit 1
    ;;
  *)
    log_error "Unknown command: $CMD"
    exit 1
    ;;
esac
