# Slurm 集群监控

基于 **slurmrestd + sacct(SSH) + NVML** 的 GPU 集群监控系统。采集 Slurm 作业、节点资源与每张 GPU 的实时状态，落入 PostgreSQL，通过 FastAPI 提供 REST API 与单页 Web 界面。

- 实时采集：每 30 秒拉取 Slurm 作业 / 节点状态，并 SSH 到算力节点读取 GPU 与主机指标
- 长周期统计：作业历史、用户卡时、排队趋势、作业效率分析
- 纯内网部署：一套 Docker Compose 拉起数据库、缓存、采集、聚合与 Web 服务

## 功能特性

| 模块 | 说明 |
|------|------|
| 集群总览 | GPU/CPU/内存实时使用率、运行与排队作业、资源趋势、用户排行榜 |
| 节点详情 | 单节点历史曲线（支持两日叠加对比）、每张 GPU 的利用率/显存/温度/功耗、卡上进程与所属作业 |
| 排队作业 | 全集群排队列表、等待时长趋势、等待原因分布、优先级与排队位次 |
| 个人统计 | 用户 GPU 卡时、作业成功率、等待时长、逐日趋势、历史/运行/排队作业列表、CSV/JSON 导出 |
| 作业效率 | 单作业时间效率、GPU 利用率曲线、P90、空转占比、GPU 效率评分与改进建议 |
| 用户管理 | Slurm 账号 → 姓名映射、项目组归属、多用户合并展示 |
| 节点管理 | Web 页面维护算力节点 IP/SSH 账号，按节点启用或停用采集 |
| rgtop 集成 | 提供 HTTP 数据源，供 [rgtop](docs/RGTOP_HTTP_BACKEND.md) 等命令行工具直接读取，无需 SSH 到算力节点 |

## 架构

```
Slurm 集群
  ├── slurmrestd ───────→ Collector（每 30s）
  ├── sacct (SSH) ──────↗
  └── 算力节点 (SSH+NVML)↗
                          ↓
                     PostgreSQL
                          ↓
                  Web API（FastAPI）
                  Redis 缓存（单飞防击穿 + 后台预热）
                          ↓
                    前端 SPA（静态页面）
```

| 容器 | 职责 |
|------|------|
| `slurm-monitor-pg` | PostgreSQL 16，持久化所有监控数据 |
| `slurm-monitor-redis` | Redis 7，接口缓存（10–300s TTL） |
| `slurm-monitor-collector` | 实时数据采集：Slurm REST、sacct、GPU/主机指标 |
| `slurm-monitor-web` | FastAPI 服务 + 静态前端 |
| `slurm-monitor-aggregator` | 集群聚合、排队快照、作业效率分析 |

## 技术栈

- **后端**：Python 3.13、FastAPI、SQLAlchemy 2、Pydantic Settings
- **数据源**：Slurm REST API（slurmrestd）、`sacct`/`squeue`（SSH）、NVML（SSH 注入 agent）
- **存储**：PostgreSQL 16、Redis 7
- **前端**：原生 HTML/CSS/JavaScript 单页应用（无构建步骤）
- **部署**：Docker + Docker Compose

## 快速开始

### 前置条件

- Docker 与 Docker Compose（支持 `docker compose` 或 `docker-compose`）
- 可访问的 `slurmrestd` 地址与认证 token
- 可 SSH 登录的集群登录节点（用于 `sacct` 等命令）
- 算力节点 SSH 账号（用于采集 GPU/主机指标，需可读取 NVML）

### 部署

```bash
# 1. 克隆代码
git clone <repo-url> slurm-monitor
cd slurm-monitor

# 2. 准备配置
cp .env.example .env
vim .env   # 至少填写下文「必需配置」

# 3. 构建并启动
bash deploy/scripts/control.sh build
bash deploy/scripts/control.sh start

# 4. 查看状态
bash deploy/scripts/control.sh status
```

启动后访问：

- Web 界面：`http://<服务器IP>:8000/`
- API 文档：`http://<服务器IP>:8000/docs`
- 健康检查：`http://<服务器IP>:8000/health`

> 首次启动时 entrypoint 会自动建表；如需从 `sacct` 回填历史数据，可执行
> `docker exec slurm-monitor-web python3 /app/scripts/init_db.py --backfill`。

### 必需配置

完整配置见 [.env.example](.env.example)，以下为最关键的项：

```env
# Slurm REST API
SLURM_REST_URL=http://<slurmrestd-host>:6820
SLURM_USER_NAME=<slurm-user>
SLURM_USER_TOKEN=<jwt-token>

# 集群登录节点（sacct/squeue/sinfo）
SSH_HOST=<login-node>
SSH_PORT=22
SSH_USER=<ssh-user>
SSH_PASSWORD=<ssh-password>

# 算力节点默认 SSH 账号（节点 IP 在 Web「集群配置」页维护）
NODE_SSH_USER=<node-ssh-user>
NODE_SSH_PASSWORD=<node-ssh-password>

# 数据库
POSTGRES_PASSWORD=<strong-password>

# Web 管理员（用于「集群配置」页，务必修改默认值）
ADMIN_USER=admin
ADMIN_PASS=<strong-password>

# 集群本地时区
TIMEZONE=Asia/Shanghai
```

> 注意：`.env` 中的整数字段**不要写行内注释**（如 `INTERVAL=30  # 注释`），否则会解析失败；注释请单独占一行。

### 服务管理

```bash
bash deploy/scripts/control.sh build [service]    # 构建镜像
bash deploy/scripts/control.sh start [service]    # 启动服务
bash deploy/scripts/control.sh stop  [service]    # 停止服务
bash deploy/scripts/control.sh restart [service]  # 重启服务
bash deploy/scripts/control.sh status             # 查看状态
```

`service` 可选：`collector`、`web`、`aggregator`、`postgres`、`redis`，默认 `all`。

完整部署、迁移与故障排查见 [deploy/DEPLOY.md](deploy/DEPLOY.md)。

## API 概览

所有接口以 `/api` 为前缀，完整定义见运行后的 `/docs`。

| 接口 | 说明 |
|------|------|
| `GET /api/cluster/summary` | 集群总览（GPU/CPU/内存/作业） |
| `GET /api/cluster/trend` | 集群资源趋势 |
| `GET /api/cluster/top-users` | 用户排行榜（卡时/作业数/利用率） |
| `GET /api/cluster/top-pending` | 排队最久的作业 |
| `GET /api/cluster/health` | 采集与数据新鲜度检查 |
| `GET /api/nodes` | 节点列表（含迷你趋势） |
| `GET /api/node/{name}` | 节点详情 |
| `GET /api/node/{name}/trend` | 节点历史曲线（支持两日对比、单卡过滤） |
| `GET /api/node/{name}/gpus` | GPU 单卡实时状态与进程 |
| `GET /api/node/{name}/jobs` | 节点作业列表 |
| `GET /api/jobs/pending` | 排队作业列表（筛选、分页） |
| `GET /api/jobs/pending/summary` | 排队趋势与等待原因分布 |
| `GET /api/meta/nodes` | 节点元数据 |
| `GET /api/meta/users` | 用户元数据 |
| `GET /api/meta/partitions` | 分区元数据 |
| `GET /api/meta/date-range` | 可查询日期范围 |
| `GET /api/user/{name}/summary` | 用户统计 |
| `GET /api/user/{name}/jobs` | 用户作业列表 |
| `GET /api/user/{name}/trend` | 用户 GPU 卡时趋势 |
| `GET /api/user/{name}/export` | 导出作业（`format=csv\|json`） |
| `GET /api/job/{id}/analysis` | 单作业效率分析 |
| `GET /api/gpu-live[/{node}]` | 最近一帧 GPU + 主机指标 |
| `GET /api/rgtop/frame[/{node}]` | rgtop HTTP 数据源 |
| `/api/user-mapping`、`/api/project-groups` | 名字映射与项目组 |
| `/api/admin/*` | 管理员登录、项目组与节点映射管理 |

旧版接口 `/api/v1/*` 仍保留以兼容历史调用。

## 数据表

| 表 | 内容 | 保留策略 |
|----|------|---------|
| `jobs` | 当前运行/排队作业 | 仅当前态 |
| `job_steps` | 作业步骤 | 随作业 |
| `job_history` | 历史作业（来自 sacct） | 长期 |
| `node_snapshots` | 节点资源快照（30s） | 30 天 |
| `node_agg_stats` | 节点聚合统计 | 长期 |
| `node_host_stats` | 主机 CPU/负载/内存 | 30 天 |
| `gpu_snapshots` | 每卡 GPU 快照与进程 | 30 天 |
| `cluster_agg_stats` | 集群聚合统计 | 按粒度长期 |
| `pending_job_snapshots` | 排队作业快照（5m） | 30 天 |
| `job_efficiency_analysis` | 作业效率分析结果 | 长期 |
| `user_stats` | 用户统计 | 长期 |
| `user_mappings` | Slurm 账号 → 姓名/项目组 | 长期 |
| `project_groups` | 项目组 | 长期 |
| `node_ip_map` | 节点名 → IP/SSH 配置 | 长期 |

## 项目结构

```
slurm-monitor/
├── deploy/                  # Docker、Compose 与控制脚本
│   ├── docker-compose.yml
│   ├── Dockerfile
│   ├── DEPLOY.md            # 部署与迁移文档
│   └── scripts/             # control.sh、entrypoint 等
├── docs/                    # 使用/管理/rgtop 文档
├── scripts/                 # 初始化、迁移、数据修复工具
├── slurm_monitor/
│   ├── aggregator/          # 集群聚合、排队快照、效率分析
│   ├── api/                 # FastAPI 路由与缓存
│   ├── collector/           # slurmrestd / sacct / GPU 采集
│   ├── parser/              # sacct、TRES 解析
│   ├── storage/             # SQLAlchemy 模型、写入、缓存
│   ├── sync/                # 作业历史同步
│   ├── web/static/          # 前端 SPA
│   ├── app.py               # FastAPI 入口
│   └── config.py            # 环境变量配置
├── tests/                   # 单元测试
├── .env.example
└── requirements.txt
```

## 开发

```bash
# 安装依赖
pip install -r requirements.txt

# 运行单元测试（解析器等纯逻辑测试无需数据库）
pytest

# 本地启动 Web（需可访问 PostgreSQL/Redis）
python -m slurm_monitor.web

# 启动采集器
python -m slurm_monitor.collector
```

数据库初始化：

```bash
python scripts/init_db.py              # 仅建表
python scripts/init_db.py --backfill   # 建表并从 sacct 回填历史
python scripts/init_new_tables.py      # 幂等补齐新增表与索引
```

开发文档与环境变量规范见 [AGENTS.md](AGENTS.md)。

## 文档

- [用户使用手册](docs/user-guide.md)：各页面操作、名字映射、个人统计与作业分析
- [管理员配置手册](docs/admin-guide.md)：项目组与节点映射维护
- [部署指南](deploy/DEPLOY.md)：新机器部署、数据迁移、故障排查
- [rgtop HTTP 后端](docs/RGTOP_HTTP_BACKEND.md)：接口规范与客户端接入

## 安全提示

- 不要提交 `.env`，仓库已默认忽略；请只提交 `.env.example`
- 首次部署务必修改 `ADMIN_PASS`，否则任何能访问 8000 端口的人都能修改节点配置
- 采集账号只需节点读取权限，无需给使用者开放节点 SSH
- 建议仅在内网或受信网络暴露 Web 端口，或在前置反向代理上增加认证与 HTTPS

## License

未指定。
