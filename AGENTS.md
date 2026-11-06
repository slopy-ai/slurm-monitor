# Slurm 集群监控 — 开发指南

## 项目概述

基于 slurmrestd API + sacct SSH 的 GPU 集群监控系统。数据采集后落入 PostgreSQL，前端通过 FastAPI 提供的 REST API 读取并展示。

## 架构

```
Slurm 集群
  ├── slurmrestd  ──────→ Collector (每 30s)
  └── sacct SSH   ──────↗
                         ↓
                    PostgreSQL
                         ↓
                    Web API (FastAPI)
                    Redis 缓存 (10-300s TTL，单飞防击穿 + 后台预热)
                         ↓
                    前端 SPA (index.html)
```

**容器列表**

| 容器名 | 职责 |
|--------|------|
| `slurm-monitor-pg` | PostgreSQL 16 数据库 |
| `slurm-monitor-redis` | Redis 7 缓存 |
| `slurm-monitor-collector` | 实时数据采集（slurmrestd + sacct） |
| `slurm-monitor-web` | FastAPI Web 服务（含静态前端） |
| `slurm-monitor-aggregator` | 集群聚合 + 排队快照 + 作业效率分析 |

## 快速启动

```bash
# 构建所有镜像（使用缓存；COPY 层按文件内容失效，改代码会自动重建该层）
docker-compose -f deploy/docker-compose.yml build

# 启动（如 compose up 报 ContainerConfig 错误，改用 docker run，见 docs/issues.md #003）
docker restart slurm-monitor-web slurm-monitor-aggregator
```

## 常用命令

```bash
# 查看所有容器状态
docker ps | grep slurm

# 查看某服务日志
docker logs slurm-monitor-web --tail 50
docker logs slurm-monitor-aggregator --tail 50

# 热更新单个文件（开发调试用，重启后仍有效）
docker cp slurm_monitor/api/routes_new.py slurm-monitor-web:/app/slurm_monitor/api/routes_new.py
docker cp slurm_monitor/app.py slurm-monitor-web:/app/slurm_monitor/app.py
docker restart slurm-monitor-web

# 初始化新表（首次或新增表时；同时幂等补建索引）
docker exec slurm-monitor-web python3 /app/scripts/init_new_tables.py

# 单独创建/补齐性能索引（CONCURRENTLY，可重复执行）
docker exec -i slurm-monitor-pg psql -U slurm_monitor -d slurm_monitor \
  < scripts/add_performance_indexes.sql

# 验证 API 路由数量
curl -s http://192.168.3.102:8000/openapi.json | python3 -c \
  "import sys,json; d=json.load(sys.stdin); print(f'{len(d[\"paths\"])} 个路由')"
```

## API 说明

**新版 API（前端对接）** — `/api/*`

| 接口 | 说明 |
|------|------|
| `GET /api/cluster/summary` | 集群总览（GPU/CPU/内存/作业） |
| `GET /api/cluster/trend` | 集群趋势曲线（从 cluster_agg_stats） |
| `GET /api/nodes` | 节点列表（含迷你趋势） |
| `GET /api/node/{name}` | 节点详情 |
| `GET /api/node/{name}/gpus` | GPU 单卡状态 |
| `GET /api/node/{name}/jobs` | 节点作业列表 |
| `GET /api/jobs/pending` | 排队作业列表（支持筛选分页） |
| `GET /api/jobs/pending/summary` | 排队聚合（等待趋势/原因分布） |
| `GET /api/meta/nodes` | 节点元数据（下拉框） |
| `GET /api/meta/users` | 用户元数据（下拉框） |
| `GET /api/meta/partitions` | 分区元数据（下拉框） |
| `GET /api/meta/date-range` | 可查询日期范围 |
| `GET /api/user/{name}/summary` | 用户统计（从 job_history） |
| `GET /api/user/{name}/jobs` | 用户作业列表 |
| `GET /api/job/{id}/analysis` | 单作业效率分析 |
| `GET /api/node/{name}/trend` | 节点历史曲线，按本地自然日切片，支持 `date_a`/`date_b` 两天叠加、`gpu` 单卡过滤 |
| `GET /api/node/{name}/gpus` | GPU 单卡实时状态（利用率/显存/温度/功耗/进程/所属作业，来自 `gpu_snapshots`） |
| `GET /api/gpu-live[/{node}]` | 最近一帧 GPU + 主机指标（CPU/内存/负载），每个进程带 `job_id`，供 rgtop 等命令行工具读取（无需 SSH 到算力节点） |
| `GET /api/user/{name}/trend` | 用户 GPU 耗时趋势（卡时/作业数/加权利用率，按作业开始时间归属） |
| `GET /api/user/{name}/export` | 导出作业列表（`format=csv\|json`，与页面同口径） |
| `/api/admin/nodes`（GET/POST/PUT/DELETE、`/test`） | 算力节点 IP 映射管理（需管理员 token，页面：集群配置 → 节点映射） |

**旧版 API（向后兼容）** — `/api/v1/*`

仍可用，不影响现有前端。

## 数据库表

| 表名 | 数据来源 | 保留策略 |
|------|---------|---------|
| `node_snapshots` | Collector 每 30s | 30 天原始数据 |
| `node_agg_stats` | 从 snapshots 聚合 | 长期，按粒度 |
| `jobs` | slurmrestd 当前态 | 仅当前运行/排队 |
| `cluster_agg_stats` | Aggregator 每 5m | 按粒度（7d/30d/90d/365d） |
| `job_history` | sacct 同步 | 长期 |
| `job_efficiency_analysis` | Aggregator 分析 | 长期 |
| `pending_job_snapshots` | Aggregator 每 5m | 30 天 |
| `gpu_snapshots` | Collector 每 30s SSH 到算力节点注入 agent 脚本采集（含每卡进程 `proc_json`） | 30 天 |
| `node_ip_map` | Web「集群配置 → 节点映射」维护：节点名 → IP/端口/SSH 账号 | 长期 |
| `node_host_stats` | Collector 每 30s 随 GPU 一起采集：CPU 利用率/负载/内存 | 30 天 |

## 已知问题

见 [docs/issues.md](./docs/issues.md)，重点关注：

- **#001** — Docker 构建缓存导致新文件未生效（最常见）
- **#002** — `.env` 行内注释导致 pydantic 整数解析失败
- **#003** — `docker-compose up` 的 ContainerConfig 兼容性问题
- **#005** — 新路由未注册（app.py 未更新到容器）

## 开发规范

### 新增 Python 模块

1. 在 `slurm_monitor/` 下创建模块目录，添加 `__init__.py`
2. 更新 `app.py` 或相关入口文件导入新模块
3. **重新构建**：`docker-compose build <service>`（无需 `--no-cache`；构建后确认容器用的是新镜像，见 docs/issues.md #001）
4. 验证导入：`docker exec slurm-monitor-web python -c "import slurm_monitor.新模块"`

### 新增数据库表

1. 在 `slurm_monitor/storage/models.py` 中定义 SQLAlchemy 模型
2. 在 `scripts/init_new_tables.py` 中添加表创建逻辑
3. 执行：`docker exec slurm-monitor-web python3 /app/scripts/init_new_tables.py`

### `.env` 文件规范

- 整数字段**不允许**行内注释
- 注释必须单独占一行，以 `#` 开头

```env
# 正确：注释单独一行
# 实时采集间隔（秒）
COLLECTOR_INTERVAL_REALTIME=30

# 错误：行内注释会导致解析失败
COLLECTOR_INTERVAL_REALTIME=30  # 实时采集间隔（秒）
```

### 前端开发

- 静态文件目录：`slurm_monitor/web/static/`
- 主入口：`index.html`（包含 CSS + JS，单文件 SPA）
- 修改后直接 `docker cp` 到容器，无需重建镜像：

```bash
docker cp slurm_monitor/web/static/index.html slurm-monitor-web:/app/slurm_monitor/web/static/index.html
```

## GPU 采集

- 算力节点的 **IP 在 Web 页面维护**（存 `node_ip_map` 表），collector 每个周期重新读取，改完无需重启。
- 默认 SSH 账号/密码在 `.env`：`NODE_SSH_USER` / `NODE_SSH_PASSWORD`；节点未单独配置时使用。
- 采集实现在 `slurm_monitor/collector/gpu_collector.py`（SSH 注入 Python agent，通过 NVML 读取）；`ssh_client.nvidia_smi_node` 保留为可替换的备用实现。
- sacct 时间是集群本地时间，解析统一走 `parser/sacct.py:parse_local_dt`（按 `TIMEZONE` 转 UTC）。
- **GPU 进程 → Slurm 作业**：集群没启用 cgroup 任务插件，进程 cgroup 里没有作业号；agent 沿进程祖先链找 `slurmstepd: [作业号.步骤]` 得到作业号，写入 `gpu_snapshots.job_id`。作业的 GPU 利用率、效率分析、个人统计都基于它。
- 排队作业来自 slurmrestd（sacct 默认不返回未启动的作业）：collector 每个周期把 REST 实时作业 upsert 进 `jobs`，带优先级和排队原因。
- 数组子任务：`jobs.job_id`/`job_history.job_id` 是子任务自己的真实作业号，父作业号存 `array_job_id`，页面显示为 `141_54`（`job_label`）。
- 节点连续采集失败会指数退避（30s→15min），避免对认证失败的账号高频重试导致被锁。

## 集群连接配置

`.env` 中的关键配置：

```env
SLURM_HOST=<集群登录节点 IP>
SLURM_REST_URL=http://<slurmrestd 地址>:<端口>
SLURM_SSH_USER=<SSH 用户名>
```

SSH 密钥通过 `-v ~/.ssh:/root/.ssh:ro` 挂载到容器。

## 监控面板访问

- **Web 界面**：`http://192.168.3.102:8000/`
- **API 文档**：`http://192.168.3.102:8000/docs`
- **健康检查**：`http://192.168.3.102:8000/health`
