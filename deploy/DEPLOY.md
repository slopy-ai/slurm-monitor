# Slurm Monitor 部署指南

## 快速部署（已有环境）

### 前置条件

- Docker 已安装
- 已有 PostgreSQL 16 和 Redis 7 镜像
- 可访问 Slurm 集群（slurmrestd + SSH）

### 部署步骤

```bash
# 1. 克隆代码
git clone <repo-url> slurm-monitor
cd slurm-monitor

# 2. 配置环境变量
cp .env.example .env
vim .env  # 填写必需配置（见下方）

# 3. 构建镜像
bash deploy/scripts/control.sh build

# 4. 启动所有服务
bash deploy/scripts/control.sh start

# 5. 验证服务状态
bash deploy/scripts/control.sh status

# 6. 访问 Web 界面
# http://<your-ip>:8000
```

### 必需的环境变量

编辑 `.env` 文件，至少配置以下项：

```bash
# Slurm 集群连接
SLURM_REST_URL=http://<slurmrestd-host>:6820   # slurmrestd API 地址
SLURM_USER_NAME=<slurm-user>                   # Slurm 用户名
SLURM_USER_TOKEN=your_jwt_token_here            # slurmrestd 认证 token

# SSH 登录节点（用于 sacct/squeue/sinfo）
SSH_HOST=<login-node-host>                      # SSH 登录节点 IP
SSH_PORT=22
SSH_USER=<ssh-user>                             # SSH 用户名
SSH_PASSWORD=your_ssh_password                  # SSH 密码

# 数据库密码
POSTGRES_PASSWORD=your_secure_password           # PostgreSQL 密码

# 采集配置
COLLECTOR_INTERVAL_REALTIME=30                  # 实时采集间隔（秒）
TIMEZONE=Asia/Shanghai                           # 集群本地时区

# GPU 采集（算力节点 SSH，密码认证）
NODE_SSH_USER=<node-ssh-user>                    # 算力节点 SSH 用户（预填默认值）
NODE_SSH_PASSWORD=your_node_password             # 算力节点 SSH 密码
```

> 认证方式：所有 SSH 连接（登录节点、算力节点）均使用密码认证，密钥文件在 `control.sh` 里已移除，无需配置 `~/.ssh`。

### 管理员账号配置

Web 界面「集群配置」页的管理员登录凭据通过以下 `.env` 变量配置：

```bash
ADMIN_USER=admin       # 管理员用户名
ADMIN_PASS=admin123    # 管理员密码（强烈建议修改默认值）
```

> 默认值 `admin/admin123` 在新机器部署前务必修改，否则任何能访问 8000 端口的人都可以管理节点配置。

**注意**：整数字段不允许行内注释，注释必须单独占一行。

---

## 服务管理命令

使用统一的控制脚本 `deploy/scripts/control.sh`：

```bash
# 构建服务
bash deploy/scripts/control.sh build              # 构建所有镜像
bash deploy/scripts/control.sh build web          # 只构建 web 服务
bash deploy/scripts/control.sh build collector    # 只构建 collector 服务

# 启动服务
bash deploy/scripts/control.sh start              # 启动所有服务
bash deploy/scripts/control.sh start web          # 只启动 web 服务

# 停止服务
bash deploy/scripts/control.sh stop               # 停止所有服务
bash deploy/scripts/control.sh stop aggregator    # 只停止 aggregator

# 重启服务
bash deploy/scripts/control.sh restart            # 重启所有服务
bash deploy/scripts/control.sh restart web        # 重启 web 服务

# 查看状态
bash deploy/scripts/control.sh status             # 显示所有容器状态和日志
```

### 支持的服务名

| 服务名 | 说明 | 容器名 |
|--------|------|--------|
| `collector` | 数据采集服务 | slurm-monitor-collector |
| `web` | Web API + 前端 | slurm-monitor-web |
| `aggregator` | 数据聚合 + 分析 | slurm-monitor-aggregator |
| `postgres` / `pg` | PostgreSQL 数据库 | slurm-monitor-pg |
| `redis` | Redis 缓存 | slurm-monitor-redis |
| `all` | 所有服务（默认） | - |

---

## 全新机器部署流程

### 前置条件

目标机器需要：
- Docker 已安装并运行
- 有 sudo/root 权限
- 能访问 Slurm 集群（网络可达）

### 步骤 1：安装 Docker（如未安装）

```bash
# Ubuntu/Debian
sudo apt update
sudo apt install -y docker.io
# Docker Compose：v2 插件（推荐）或 v1 独立程序，二选一
sudo apt install -y docker-compose-plugin  # v2: 提供 `docker compose` 命令
# 或：sudo apt install -y docker-compose     # v1: 提供 `docker-compose` 命令
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker $USER  # 将当前用户加入 docker 组
# 注销并重新登录使 docker 组生效

# CentOS/RHEL
sudo yum install -y docker
sudo yum install -y docker-compose-plugin  # 或 docker-compose（v1）
sudo systemctl enable docker
sudo systemctl start docker
sudo usermod -aG docker $USER
```

> control.sh 自动检测用 `docker compose`（v2）还是 `docker-compose`（v1），无需手动指定。
> 也可用 `COMPOSE_CMD` 环境变量强制覆盖。

### 步骤 2：拉取基础镜像

```bash
# 拉取 PostgreSQL 和 Redis 镜像
docker pull postgres:16-alpine
docker pull redis:7-alpine
```

### 步骤 3：部署项目

```bash
# 克隆代码
cd ~
git clone <repo-url> slurm-monitor
cd slurm-monitor

# 配置环境变量
cp .env.example .env
vim .env  # 根据实际情况填写配置

# 构建并启动
bash deploy/scripts/control.sh build
bash deploy/scripts/control.sh start

# 验证服务
bash deploy/scripts/control.sh status
```

### 步骤 4：初始化数据库表

首次部署需要创建数据库表：

```bash
docker exec slurm-monitor-web python3 /app/scripts/init_new_tables.py
```

### 步骤 5：配置节点 IP 映射（可选）

如果需要采集 GPU 数据，在 Web 界面配置节点 IP 映射：

1. 访问 `http://<ip>:8000`
2. 进入「集群配置 → 节点映射」
3. 添加算力节点的 IP、SSH 端口、用户名等信息

配置保存后，collector 会在下个采集周期自动读取，无需重启。

---

## 从旧机器迁移到新机器

### 方案 A：推送镜像（不需要代码仓库）

适用于已有构建好的镜像、新机器不便联网构建的场景。

#### 旧机器：导出镜像

```bash
# 导出三个应用镜像（postgres/redis 会自动拉取，不需要导出）
docker save slurm-monitor-web slurm-monitor-collector slurm-monitor-aggregator \
  | gzip > slurm-monitor-images.tar.gz

# 传输到新机器
scp slurm-monitor-images.tar.gz user@new-machine:~/
# 同时传 deploy 目录和 .env
rsync -av --exclude pgdata --exclude redisdata \
  deploy/ user@new-machine:~/slurm-monitor/deploy/
scp .env user@new-machine:~/slurm-monitor/.env
```

#### 新机器：加载并启动

```bash
cd ~/slurm-monitor

# 加载镜像
docker load < ~/slurm-monitor-images.tar.gz

# 启动（postgres/redis 镜像会自动拉取，需要联网）
bash deploy/scripts/control.sh start

# 初始化数据库
docker exec slurm-monitor-web python3 /app/scripts/init_new_tables.py
```

> 节点 IP 映射存在数据库里，新机器是空库，需要重新在 Web UI 「集群配置 → 节点映射」里配置。

### 方案 B：仅迁移代码（数据重新采集）

适用于数据不重要或愿意重新采集的场景。

```bash
# 在新机器上
git clone <repo-url> slurm-monitor
cd slurm-monitor
cp /path/to/old/.env .env  # 复制旧环境配置

# 构建并启动
bash deploy/scripts/control.sh build
bash deploy/scripts/control.sh start

# 初始化数据库
docker exec slurm-monitor-web python3 /app/scripts/init_new_tables.py
```

### 方案 B：迁移代码 + 数据库

适用于需要保留历史数据的场景。

#### 旧机器操作

```bash
# 1. 停止服务
cd ~/slurm-monitor
bash deploy/scripts/control.sh stop

# 2. 导出数据库
docker exec slurm-monitor-pg pg_dump -U slurm_monitor slurm_monitor > /tmp/slurm_monitor_backup.sql

# 3. 打包代码和配置
cd ~
tar czf slurm-monitor-backup.tar.gz slurm-monitor/.env /tmp/slurm_monitor_backup.sql

# 4. 传输到新机器
scp slurm-monitor-backup.tar.gz user@new-machine:/tmp/
```

#### 新机器操作

```bash
# 1. 解压备份
cd /tmp
tar xzf slurm-monitor-backup.tar.gz

# 2. 克隆代码
cd ~
git clone <repo-url> slurm-monitor
cd slurm-monitor

# 3. 恢复配置
cp /tmp/slurm-monitor/.env .env

# 4. 构建镜像
bash deploy/scripts/control.sh build

# 5. 仅启动数据库和 Redis
bash deploy/scripts/control.sh start postgres
bash deploy/scripts/control.sh start redis
sleep 5  # 等待 PG 启动完成

# 6. 导入数据
docker cp /tmp/slurm_monitor_backup.sql slurm-monitor-pg:/tmp/
docker exec slurm-monitor-pg psql -U slurm_monitor -d slurm_monitor -f /tmp/slurm_monitor_backup.sql

# 7. 启动其他服务
bash deploy/scripts/control.sh start collector
bash deploy/scripts/control.sh start web
bash deploy/scripts/control.sh start aggregator

# 8. 验证
bash deploy/scripts/control.sh status
curl http://localhost:8000/api/cluster/summary
```

### 方案 C：迁移完整容器数据卷

适用于需要完整保留所有数据和状态的场景。

#### 旧机器操作

```bash
# 1. 停止服务
cd ~/slurm-monitor
bash deploy/scripts/control.sh stop

# 2. 打包数据卷
cd ~/slurm-monitor/deploy
sudo tar czf /tmp/pgdata-backup.tar.gz pgdata/

# 3. 打包配置
cd ~/slurm-monitor
tar czf /tmp/slurm-config.tar.gz .env

# 4. 传输到新机器
scp /tmp/pgdata-backup.tar.gz /tmp/slurm-config.tar.gz user@new-machine:/tmp/
```

#### 新机器操作

```bash
# 1. 克隆代码
cd ~
git clone <repo-url> slurm-monitor
cd slurm-monitor

# 2. 恢复配置
cp /tmp/slurm-config.tar.gz .
tar xzf slurm-config.tar.gz

# 3. 恢复数据卷
cd deploy
sudo tar xzf /tmp/pgdata-backup.tar.gz
sudo chown -R 999:999 pgdata  # PostgreSQL 容器用户 UID

# 4. 构建并启动
cd ~/slurm-monitor
bash deploy/scripts/control.sh build
bash deploy/scripts/control.sh start

# 5. 验证
bash deploy/scripts/control.sh status
```

---

## 常见问题

### Q: 如何查看服务日志？

```bash
docker logs slurm-monitor-web --tail 50 -f
docker logs slurm-monitor-collector --tail 50 -f
docker logs slurm-monitor-aggregator --tail 50 -f
```

### Q: 如何热更新单个文件？

```bash
# 修改代码后
docker cp slurm_monitor/api/routes_new.py slurm-monitor-web:/app/slurm_monitor/api/routes_new.py
docker restart slurm-monitor-web
```

### Q: 如何验证 API 是否正常？

```bash
# 健康检查
curl http://localhost:8000/health

# 集群总览
curl http://localhost:8000/api/cluster/summary

# API 文档
open http://localhost:8000/docs
```

### Q: 构建后容器没有使用新镜像？

这是 Docker 镜像缓存问题（[docs/issues.md](../docs/issues.md) #001），解决方法：

```bash
# 方法 1：重新构建并强制替换容器
bash deploy/scripts/control.sh build web
docker rm -f slurm-monitor-web
bash deploy/scripts/control.sh start web

# 方法 2：清理旧镜像后重建
docker images | grep slurm-monitor-web
docker rmi <old-image-id>
bash deploy/scripts/control.sh build web
bash deploy/scripts/control.sh restart web
```

### Q: 如何清理历史数据？

```bash
# 进入容器执行清理脚本
docker exec -it slurm-monitor-pg psql -U slurm_monitor

# 在 psql 中执行
DELETE FROM node_snapshots WHERE snapshot_ts < NOW() - INTERVAL '30 days';
DELETE FROM gpu_snapshots WHERE snapshot_ts < NOW() - INTERVAL '30 days';
DELETE FROM pending_job_snapshots WHERE snapshot_ts < NOW() - INTERVAL '30 days';
VACUUM FULL;
```

### Q: 如何修改 Web 端口？

```bash
# 编辑 .env
WEB_PORT=8080

# 重启 web 服务（会自动使用新端口）
bash deploy/scripts/control.sh restart web
```

### Q: GPU 采集失败怎么办？

检查以下几点：

1. 节点 IP 映射是否正确（Web 界面 → 集群配置 → 节点映射）
2. SSH 认证是否成功（密码或密钥）
3. 查看 collector 日志：`docker logs slurm-monitor-collector --tail 100`
4. 手动测试 SSH 连接：`ssh <user>@<node-ip> nvidia-smi`

---

## 监控与维护

### 服务健康检查

```bash
# 每日检查脚本
#!/bin/bash
cd ~/slurm-monitor
bash deploy/scripts/control.sh status | grep -E "(Up|Exited)"
curl -f http://localhost:8000/health || echo "Web service down!"
```

### 日志轮转

容器日志会自动管理，但可手动清理：

```bash
# 清理容器日志
docker logs slurm-monitor-web 2>&1 | tail -1000 > /tmp/web.log
truncate -s 0 $(docker inspect --format='{{.LogPath}}' slurm-monitor-web)
```

### 数据库备份

建议定期备份数据库：

```bash
# 每周备份脚本
#!/bin/bash
BACKUP_DIR="/backup/slurm-monitor"
DATE=$(date +%Y%m%d)
mkdir -p $BACKUP_DIR

docker exec slurm-monitor-pg pg_dump -U slurm_monitor slurm_monitor | \
  gzip > $BACKUP_DIR/slurm_monitor_${DATE}.sql.gz

# 保留最近 30 天备份
find $BACKUP_DIR -name "*.sql.gz" -mtime +30 -delete
```

---

## 性能优化建议

### 数据库索引

已在 `storage/models.py` 中定义，首次部署会自动创建。如需手动检查：

```bash
docker exec -it slurm-monitor-pg psql -U slurm_monitor -c "\di"
```

### Redis 缓存配置

默认 TTL：
- 集群总览：15 秒
- 节点列表：30 秒
- 单节点详情：30 秒
- 趋势数据：60 秒

可在代码中调整 `slurm_monitor/api/routes_new.py` 的缓存时间。

### 采集间隔调整

根据集群规模调整采集频率：

```bash
# .env 配置
COLLECTOR_INTERVAL_REALTIME=30        # 小集群：30s，大集群：60s
AGGREGATOR_INTERVAL=300               # 5 分钟聚合一次
```

---

## 安全建议

1. **修改默认密码**：生产环境务必修改 `.env` 中的所有密码
2. **限制网络访问**：使用防火墙限制 8000 端口访问来源
3. **SSH 密钥认证**：优先使用密钥而非密码认证
4. **定期更新**：关注 Docker 镜像安全更新
5. **日志审计**：定期检查异常访问日志

---

## 附录：端口清单

| 服务 | 容器内端口 | 宿主机映射 | 说明 |
|------|-----------|-----------|------|
| Web API | 8000 | 8000 | HTTP API + 前端 |
| PostgreSQL | 5432 | - | 仅容器网络内可访问 |
| Redis | 6379 | - | 仅容器网络内可访问 |

如需从宿主机直接访问数据库，可在 `docker run` 时添加 `-p 5432:5432`。
