# rgtop HTTP Backend Integration Guide

本文档说明如何让 rgtop 通过 HTTP API 采集 GPU/CPU 数据，以及服务端需要实现的接口规范。

---

## 概述

rgtop 原本只支持通过 SSH 连接到远程节点采集数据。HTTP 后端模式让 rgtop 可以从 Web API 获取数据，适用于以下场景：

- **统一采集**：集群已有中心化的监控系统（如 slurm-monitor），通过 agent 采集并入库
- **权限管理**：避免给每个用户开 SSH 权限，通过 HTTP API 统一鉴权
- **防火墙友好**：只需开放 HTTP 端口（如 8000），不需要 SSH 端口
- **降低负载**：多个 rgtop 客户端共享同一份采集数据（服务端缓存），而非每个客户端都 SSH 采集

---

## 服务端接口规范

### 端点

```
GET /api/rgtop/frame/{node}?procs={0|1}
```

**路径参数：**
- `node`：节点名称（如 `h100-3`）

**查询参数：**
- `procs`：是否返回每卡进程列表
  - `1`：返回完整进程信息（USER 列、进程面板需要）
  - `0`：省略进程列表（节省 ~56% 带宽）

### 响应格式

#### 成功响应（HTTP 200）

```json
{
  "code": 0,
  "data": {
    "node": "h100-3",
    "error": null,
    "collected_at": "2026-11-05 12:15:56",
    "collected_at_epoch": 1793852156,
    "age_seconds": 19.9,
    "gpus": [
      {
        "idx": 0,
        "uuid": "GPU-1dc8cdee-578c-578c-f417-18f12b5d41c4",
        "name": "NVIDIA H100 80GB HBM3",
        "util_pct": 0,
        "mem_used_mb": 16055,
        "mem_total_mb": 81559,
        "temp_c": 34,
        "power_w": 265.64,
        "power_limit_w": 700.0,
        "job_id": 1502,
        "procs": [  // 仅当 procs=1 时存在
          {
            "pid": 1234567,
            "user": "alice",
            "cmd": "python train.py --model bert-large",
            "mem_mb": 15234.5,
            "rss_kb": 15600640,
            "etime_s": 3600,
            "cpu_pct": 5.2
          }
        ]
      }
    ],
    "host": {
      "cpu_count": 192,
      "cpu_util_pct": 7.6,
      "load1": 14.5,
      "load5": 12.3,
      "load15": 10.1,
      "mem_total_mb": 2063876,
      "mem_used_mb": 143745,
      "mem_avail_mb": 1920131,
      "mem_cache_mb": 157909,
      "swap_total_mb": 16383,
      "swap_free_mb": 15473,
      "cpu_cores": "20000225222820420000...",  // base36，每位 0..z 表示 0..100%
      "cpu_model": "INTEL(R) XEON(R) PLATINUM 8558",
      "sockets": 2,
      "cpu_temp_c": 47.0,
      "uptime_s": 5079395,
      "collected_at": "2026-11-05 12:15:56",
      "collected_at_epoch": 1793852156,
      "age_seconds": 19.9
    }
  }
}
```

#### 错误响应

**节点不存在（HTTP 404）：**

```json
{
  "detail": "no GPU data for node h100-99"
}
```

**节点有配置但无数据（HTTP 200，但带 error 字段）：**

```json
{
  "code": 0,
  "data": {
    "node": "h100-3",
    "error": "no GPU data collected for this node",
    "collected_at": null,
    "collected_at_epoch": null,
    "age_seconds": null,
    "gpus": [],
    "host": null
  }
}
```

### 字段说明

#### GPU 字段

| 字段 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `idx` | int | ✓ | GPU 索引（0-based） |
| `uuid` | string | ✓ | GPU UUID |
| `name` | string | ✓ | GPU 型号 |
| `util_pct` | int/null | ✓ | GPU 利用率 (0-100)，null 表示采集失败 |
| `mem_used_mb` | int | ✓ | 显存已用（MiB） |
| `mem_total_mb` | int | ✓ | 显存总量（MiB） |
| `temp_c` | int/null | ✓ | 温度（℃） |
| `power_w` | float/null | ✓ | 功耗（W） |
| `power_limit_w` | float/null | ✓ | 功耗上限（W） |
| `job_id` | int/null | ✓ | Slurm 作业 ID（无作业时为 null） |
| `procs` | array | 仅 procs=1 | 进程列表（见下表） |

#### 进程字段（procs 数组元素）

| 字段 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `pid` | int | ✓ | 进程 ID |
| `user` | string | ✓ | 用户名 |
| `cmd` | string | ✓ | 命令行（建议截断至 300 字符） |
| `mem_mb` | float | ✓ | 显存占用（MiB） |
| `rss_kb` | int | ✓ | 物理内存（KiB） |
| `etime_s` | int | ✓ | 进程运行时间（秒） |
| `cpu_pct` | float | ✓ | CPU 使用率（瞬时值，0-100 * 核数） |

#### 主机字段（host 对象）

| 字段 | 类型 | 必需 | 说明 |
|------|------|------|------|
| `cpu_count` | int | ✓ | 逻辑核心数 |
| `cpu_util_pct` | float | ✓ | 整机 CPU 使用率 (0-100) |
| `load1/load5/load15` | float | ✓ | 1/5/15 分钟平均负载 |
| `mem_total_mb` | int | ✓ | 总内存（MiB） |
| `mem_used_mb` | int | ✓ | 已用内存（MiB，不含 cache） |
| `mem_avail_mb` | int | ✓ | 可用内存（MiB，/proc/meminfo MemAvailable） |
| `mem_cache_mb` | int | ✓ | 缓存（MiB） |
| `swap_total_mb` | int | ✓ | 交换空间总量（MiB） |
| `swap_free_mb` | int | ✓ | 交换空间可用（MiB） |
| `cpu_cores` | string | ✓ | 每核 CPU 使用率（base36 编码，见下） |
| `cpu_model` | string | ✓ | CPU 型号 |
| `sockets` | int | ✓ | 物理 CPU 数 |
| `cpu_temp_c` | float/null | - | CPU 温度（℃，可选） |
| `uptime_s` | int | ✓ | 系统运行时间（秒） |
| `collected_at_epoch` | int | ✓ | 采集时间戳（Unix 秒） |

**cpu_cores 编码规则：**

每位字符表示一个逻辑核心的瞬时使用率，用 base36（0-9a-z）编码：

- `0` → 0%
- `9` → 9/35 * 100% ≈ 25.7%
- `z` (35) → 100%

示例：`"20000225..."` 表示 192 核，第 1 核 5.7%，第 6-7 核 5.7%/14.3%，其余接近 0%。

---

## 采集端实现要求

### 1. 采集频率

建议 **30 秒**采集一次（与 slurm-monitor 一致），rgtop 默认 2 秒刷新一次画面，会多次读取同一帧数据。

### 2. 数据时效性

- 接口必须返回 `collected_at_epoch`（Unix 秒），rgtop 用它在客户端计算数据年龄
- rgtop 会在 UI 上显示数据年龄（如 `[21.4s ago]`），提示用户数据新鲜度

### 3. 进程采样

- **CPU 使用率**：必须是**瞬时值**（0.2s 窗口内的 /proc/stat 差分），而非进程生命周期平均值
- **命令行截断**：建议 300 字符，避免响应体积过大

### 4. 缓存策略

建议服务端缓存响应 10 秒（TTL），多个 rgtop 客户端共享缓存：

```python
cache_key = f"rgtop_frame:{node}:{procs}"
cache.set(cache_key, data, ttl=10)
```

### 5. 错误处理

- **节点从未采集过**：HTTP 404
- **节点配置存在但当前无数据**：HTTP 200 + `"error": "no GPU data collected"`
- **采集失败但有历史数据**：返回历史数据 + 更大的 `age_seconds`

---

## rgtop 客户端配置

### 基本用法

在 `~/.config/rgtop/hosts` 中，以 `http://` 或 `https://` 开头的行会被识别为 HTTP 模式：

```
# SSH 模式（原有方式）
h100-1
h100-2

# HTTP 模式（新增）
http://192.168.3.102:8000/api/rgtop/frame/h100-3
http://192.168.3.102:8000/api/rgtop/frame/h200-1
```

**三种写法：**

```
# 1) 只写服务地址（推荐，节点多时不用逐个写）：
#    启动时 rgtop 请求一次 GET /api/rgtop/frame?procs=0 拿到节点列表，
#    为每个节点各生成一个条目
http://192.168.3.102:8000
http://192.168.3.102:8000/api/rgtop/frame

# 2) 直接指向单节点：节点名取自 /frame/ 之后的那一段
http://192.168.3.102:8000/api/rgtop/frame/h100-3
```

第 1 种要求服务端的批量接口 `GET /api/rgtop/frame` 返回 `data.nodes[].node`。
节点名只允许 `[A-Za-z0-9._-]`，其余字符的节点会被跳过（不会拼进 URL 路径）。
批量地址拿不到节点列表时，rgtop 直接报错退出（`cannot list nodes from ...`），
不会带着一个悄悄变短的列表启动。批量地址与单节点 URL 同时出现时按完整 URL 去重。

> 注意：**不要用 `#节点名`**。`~/.config/rgtop/hosts` 里 `#` 是注释起始符，
> `http://host:8000/#h100-3` 会被截断成 `http://host:8000/`。节点名必须放在路径里。

### rgtop 端实现要点（提交 MR 时的改动清单）

| 文件 | 改动 | 说明 |
|------|------|------|
| `src/http.rs` | **新增**（~260 行，含测试） | URL 解析、阻塞式 HTTP/1.1 GET、JSON → `Sample` 转换 |
| `src/main.rs` | 注册 `mod http;`；usage 文案加 HTTP 示例 | 无逻辑改动 |
| `src/collect.rs` | `Worker` 增加 `url: Option<String>` 字段；`run()` 分派到 `http_loop()`；`request()`/`keepalive()` 对 HTTP 短路 | SSH 的 `session_loop`、`ssh_command`、`mux_socket`、登录流程**一行未动** |
| `src/draw.rs` | 卡片页脚显示 `Ns old` | 用服务端 `collected_at_epoch`，区分“采集时间”与“收到时间” |

设计要点：

- **不引入异步运行时**：直接 `TcpStream` + `ConnectTimeout`/`ReadTimeout`，
  请求头带 `Connection: close`，读到 EOF 即正文（避开 chunked 解析）。
  依赖不变，仍然只有 `serde_json`（HTTP 后端本身不需要新增 crate）。
- **`request()` 语义保持一致**：SSH 是往管道写一行，HTTP 是置一个 `retry_now`
  标志并唤醒 worker 线程，两者对 UI 都非阻塞。
- **不加编译期 feature**：单个二进制同时支持两种后端，运行时按 host 前缀分派。
- **UI 显示名与 URL 分离**：`Host.name` 只存节点名，完整 URL 留在 `Worker.url`，
  否则 24 字符的标题列会显示整条 URL。


### 混合模式

SSH 和 HTTP 可以混用：

```
# 本地节点用 SSH
localhost

# 远程集群用 HTTP
http://cluster1.example.com:8000/api/rgtop/frame/gpu01
http://cluster1.example.com:8000/api/rgtop/frame/gpu02

# 另一台独立机器用 SSH
standalone-gpu.example.com
```

### 命令行示例

```bash
# 一次性查看（不进入 TUI）
rgtop --once http://192.168.3.102:8000/api/rgtop/frame/h100-3

# 进入 TUI，实时刷新
rgtop http://192.168.3.102:8000/api/rgtop/frame/h100-3 http://192.168.3.102:8000/api/rgtop/frame/h200-1

# 自定义刷新间隔（5 秒）
rgtop -n 5 http://192.168.3.102:8000/api/rgtop/frame/h100-3
```

---

## 与 SSH 模式的差异

| 特性 | SSH 模式 | HTTP 模式 |
|------|----------|-----------|
| **连接建立** | 长连接（ControlMaster），首次需要密码 | 无状态 HTTP 请求，无需认证（或 API key） |
| **采集触发** | rgtop 控制采样时机（写 stdin） | 服务端定时采集，rgtop 被动读取 |
| **数据时效** | 实时（<1s 延迟） | 取决于采集间隔（通常 30s） |
| **带宽** | 每次传输完整帧 | 可缓存共享，多客户端节省带宽 |
| **权限** | 需要节点 SSH 权限 | 只需 HTTP 访问权限 |
| **故障恢复** | 自动重连 + backoff | HTTP 超时 + 重试 |

---

## 实现检查清单

### 服务端

- [ ] 实现 `GET /api/rgtop/frame/{node}?procs={0|1}` 接口
- [ ] 返回所有必需字段（见「字段说明」）
- [ ] `collected_at_epoch` 为 Unix 秒（UTC）
- [ ] `cpu_cores` 为 base36 字符串，长度等于 `cpu_count`
- [ ] 进程 `cpu_pct` 为瞬时值（0.2s 差分），非生命周期平均
- [ ] 节点不存在时返回 HTTP 404
- [ ] 节点存在但无数据时返回 HTTP 200 + `error` 字段
- [ ] 响应缓存 10 秒（可选但推荐）

### 采集端（agent）

- [ ] 30 秒采集一次（或可配置）
- [ ] 每卡进程包含 `pid/user/cmd/mem_mb/rss_kb/etime_s/cpu_pct`
- [ ] CPU 使用率通过 /proc/stat 差分计算（0.2s 窗口）
- [ ] 命令行截断至 300 字符
- [ ] 主机指标包含 `cpu_cores`（base36 编码）
- [ ] 写入数据库时带时间戳（`collected_at`）

### rgtop 客户端

- [ ] 识别 `http://` 或 `https://` 开头的 host
- [ ] HTTP 模式下跳过 SSH 连接逻辑
- [ ] 根据 UI 状态设置 `procs` 参数（USER 列关闭时传 0）
- [ ] 解析响应并转换为 `Sample` 结构
- [ ] 显示数据年龄（`collected_at_epoch` → 当前时间）
- [ ] 错误处理：超时、404、JSON 解析失败

---

## slurm-monitor 实现参考

slurm-monitor 已实现完整的 rgtop HTTP 后端：

**采集端：** `slurm_monitor/collector/gpu_collector.py`
- 注入脚本采集 GPU + 主机指标
- 30 秒一次，写入 `gpu_snapshots` 和 `node_host_stats` 表

**存储：** `slurm_monitor/storage/models.py`
- `gpu_snapshots.proc_json`：每卡进程列表（JSON）
- `node_host_stats.cpu_cores`：每核 CPU（base36）
- 索引 `(node_name, collected_at DESC)` 用于批量最新帧查询

**接口：** `slurm_monitor/api/routes_new.py`
- `GET /api/rgtop/frame?node=h100-3&procs=1`：单节点
- `GET /api/rgtop/frame?procs=0`：所有节点（批量）
- DISTINCT ON 查询 + 10 秒缓存

**验证：**

```bash
# 测试单节点接口
curl "http://192.168.3.102:8000/api/rgtop/frame/h100-3?procs=1" | jq .

# 测试不带进程列表（节省带宽）
curl "http://192.168.3.102:8000/api/rgtop/frame/h100-3?procs=0" | jq .

# 测试不存在的节点
curl "http://192.168.3.102:8000/api/rgtop/frame/nonexistent" -w '\nHTTP %{http_code}\n'
```

---

## 故障排查

### 问题：rgtop 显示 "connect failed"

**原因：** HTTP 服务不可达或端口错误

**排查：**

```bash
# 测试连通性
curl -v "http://192.168.3.102:8000/api/rgtop/frame/h100-3?procs=1"

# 检查防火墙
sudo iptables -L -n | grep 8000
```

### 问题：rgtop 显示 "HTTP 404"

**原因：** 节点名不存在或从未采集过

**排查：**

```bash
# 检查数据库
docker exec slurm-monitor-pg psql -U slurm_monitor -c \
  "SELECT DISTINCT node_name FROM gpu_snapshots ORDER BY 1;"
```

### 问题：rgtop 显示 "no GPU data collected"

**原因：** 节点在配置中但采集失败（SSH 失败、agent 未安装等）

**排查：**

```bash
# 检查采集日志
docker logs slurm-monitor-collector --tail 100 | grep h100-3

# 手动测试 SSH
ssh h100-3 "python3 -c 'import pynvml; pynvml.nvmlInit()'"
```

### 问题：数据年龄过大（> 60s）

**原因：** 采集 agent 挂了或间隔过长

**排查：**

```bash
# 查看最近一次采集时间
docker exec slurm-monitor-pg psql -U slurm_monitor -c \
  "SELECT node_name, collected_at, NOW() - collected_at AS age \
   FROM gpu_snapshots \
   WHERE node_name = 'h100-3' \
   ORDER BY collected_at DESC LIMIT 1;"
```

---

## 后续优化建议

1. **HTTPS 支持**：当前 rgtop 实现只支持 HTTP，生产环境建议加 TLS
2. **API 认证**：HTTP 模式无需 SSH 密钥，但应考虑 API key 或 JWT
3. **批量接口**：`GET /api/rgtop/frame?procs=0` 一次返回所有节点，rgtop 内部并发请求改为单次请求
4. **WebSocket**：对于需要实时性的场景，可以用 WebSocket 推送增量更新

---

## 总结

通过 HTTP 后端，rgtop 可以：

- **复用中心化采集**：多个用户共享同一份数据，降低节点负载
- **简化权限管理**：无需给每个用户配 SSH 密钥
- **提升稳定性**：服务端采集失败不影响其他节点，而 SSH 模式下一个节点卡住会阻塞整个 UI

服务端只需实现一个简单的 RESTful 接口，rgtop 客户端无需修改 UI 层代码，所有改动在采集层内部完成。
