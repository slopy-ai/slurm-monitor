"""SQLAlchemy 数据模型

按预研文档的口径约束设计：
- 历史记录不只以 Job ID 唯一标识，需考虑集群、提交时间、数组子任务和执行记录
- step 用于补充消耗，不重复累计作业和 step 的同类指标
- 缺失指标保留为 NULL，不填 0
"""

from datetime import datetime

from sqlalchemy import (
    Column,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    BigInteger,
    desc,
    text,
)
from sqlalchemy.orm import DeclarativeBase, relationship


class Base(DeclarativeBase):
    pass


class Job(Base):
    """Slurm 作业记录。一条记录对应一个作业（含重排队后的新执行）。"""

    __tablename__ = "jobs"
    __table_args__ = (
        UniqueConstraint("cluster", "job_id", "submit_time", name="uq_job_identity"),
        # 节点列表页的排队作业按节点/分区统计
        Index("idx_jobs_partition_state", "partition", "state"),
    )

    id = Column(Integer, primary_key=True)
    cluster = Column(String(64), nullable=False, default="")
    job_id = Column(BigInteger, nullable=False, index=True)
    job_id_raw = Column(BigInteger, index=True)
    job_name = Column(String(256))
    array_task_id = Column(Integer)  # 数组子任务 ID，非数组为 NULL
    array_job_id = Column(BigInteger)  # 数组父作业号（job_id 是子任务自己的真实作业号），非数组为 NULL

    user = Column(String(64), index=True)
    uid = Column(Integer, index=True)
    account = Column(String(64))
    group = Column(String(64))
    partition = Column(String(64), index=True)
    qos = Column(String(64))

    state = Column(String(32), index=True)
    exit_code = Column(Integer)

    submit_time = Column(DateTime, index=True)
    start_time = Column(DateTime, index=True)
    end_time = Column(DateTime)
    eligible_time = Column(DateTime)
    elapsed_seconds = Column(Integer)
    suspended_seconds = Column(Integer, default=0)
    timelimit_minutes = Column(Integer)

    node_list = Column(Text)
    nodes_alloc = Column(Integer, default=0)

    # TRES 解析后的字段
    alloc_cpus = Column(Integer)
    alloc_gpus = Column(Integer, default=0)
    alloc_mem_mb = Column(BigInteger)
    req_cpus = Column(Integer)
    req_gpus = Column(Integer, default=0)
    req_mem_mb = Column(BigInteger)

    # 调度信息（来自 slurmrestd 实时作业，sacct 里没有）
    priority = Column(BigInteger)                   # 调度优先级（原始值）
    pending_reason = Column(String(64))             # 排队原因（Resources/Priority/Dependency...），非排队为 NULL

    # 用户信息来源标记：rest / sacct / unknown
    user_source = Column(String(16), default="unknown")

    # 原始 TRES 字符串，便于回溯
    alloc_tres_raw = Column(Text)
    req_tres_raw = Column(Text)

    # 消耗统计（需集群配置 jobacct_gather；默认 none 时为 NULL）
    total_cpu_seconds = Column(Float)     # TotalCPU → 秒
    max_rss_mb = Column(BigInteger)       # MaxRSS  → MB

    # 采集元数据
    collected_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    steps = relationship("JobStep", back_populates="job", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Job {self.job_id} user={self.user} state={self.state}>"


class JobStep(Base):
    """作业 step，用于补充消耗统计，不重复累计。"""

    __tablename__ = "job_steps"

    id = Column(Integer, primary_key=True)
    job_id_col = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    step_id = Column(String(32), nullable=False)  # batch, extern, 0, 1, ...
    step_name = Column(String(256))

    state = Column(String(32))
    start_time = Column(DateTime)
    end_time = Column(DateTime)
    elapsed_seconds = Column(Integer)

    alloc_cpus = Column(Integer)
    alloc_gpus = Column(Integer, default=0)
    alloc_mem_mb = Column(BigInteger)

    # 实测消耗（若集群配置了 jobacct_gather）
    total_cpu_seconds = Column(Float)
    max_rss_mb = Column(BigInteger)
    tres_usage_in_tot = Column(Text)

    job = relationship("Job", back_populates="steps")


class NodeSnapshot(Base):
    """节点状态快照，定时采集保留时序。"""

    __tablename__ = "node_snapshots"

    id = Column(BigInteger, primary_key=True)
    node_name = Column(String(64), nullable=False, index=True)
    state = Column(String(32))
    partition = Column(String(64))
    avail = Column(String(16))  # up/down

    total_cpus = Column(Integer)
    alloc_cpus = Column(Integer)
    idle_cpus = Column(Integer)
    total_memory_mb = Column(BigInteger)
    alloc_memory_mb = Column(BigInteger)

    gres_config = Column(Text)       # 原始 GRES 配置
    gres_total = Column(Integer)      # 解析后的 GPU 总数
    gres_alloc = Column(Integer)      # 已分配 GPU 数

    cpu_load = Column(Float)
    reason = Column(String(256))

    collected_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class NodeAggStat(Base):
    """节点 GPU 利用率聚合表（多粒度）。

    granularity 取值：1m / 5m / 15m / 1h / 1d。
    由 date_bin 在查询时动态聚合，本表仅持久化常用粒度以加速重复查询；
    1m / 5m / 15m 默认不落表，直接对 NodeSnapshot 做 date_bin 查询。
    1d 粒度由采集器每小时聚合一次，长期保留。
    """

    __tablename__ = "node_agg_stats"
    __table_args__ = (
        UniqueConstraint("node_name", "bucket_ts", "granularity", name="uq_node_agg"),
    )

    id = Column(BigInteger, primary_key=True)
    node_name = Column(String(64), nullable=False, index=True)
    bucket_ts = Column(DateTime, nullable=False, index=True)  # 桶起始时间 UTC
    granularity = Column(String(8), nullable=False)  # 1m / 5m / 15m / 1h / 1d

    # 从 NodeSnapshot 聚合（Slurm 分配数据）
    avg_alloc_pct = Column(Float)        # 平均分配率 %
    max_alloc_pct = Column(Float)
    gres_total = Column(Integer)
    sample_count = Column(Integer, default=0)

    # Phase 2: nvidia-smi 实测数据（暂无时为 NULL）
    avg_gpu_util_pct = Column(Float)
    max_gpu_util_pct = Column(Float)
    avg_mem_used_mb = Column(Float)
    max_mem_used_mb = Column(Float)
    avg_power_w = Column(Float)
    avg_temp_c = Column(Float)

    created_at = Column(DateTime, default=datetime.utcnow)


class NodeIpMap(Base):
    """计算节点名 → SSH 连接信息映射表。

    通过 Web UI 管理，collector 据此 SSH 到算力节点采集 GPU 数据。
    ssh_password 为 NULL 时走挂载的 SSH 密钥，非 NULL 时走密码认证。
    """

    __tablename__ = "node_ip_map"

    id = Column(Integer, primary_key=True, autoincrement=True)
    node_name = Column(String(64), nullable=False, unique=True)   # Slurm 节点名，如 h100-3
    ip = Column(String(45), nullable=False)                        # SSH 目标 IP
    port = Column(Integer, nullable=False, default=22)
    ssh_user = Column(String(64), nullable=False, default="djs")
    ssh_password = Column(Text)                                    # NULL=用挂载 key；非空=密码登录
    enabled = Column(Integer, nullable=False, default=1)           # 1=启用 0=禁用
    remark = Column(String(256))
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<NodeIpMap {self.node_name} → {self.ip}>"


class GpuSnapshot(Base):
    """单卡 GPU 快照（agent.py 采集，SSH 轮询写入）。

    每张卡每 30s 一条，保留 30 天原始数据，
    可直接用 date_bin 按 1m/5m/15m 粒度聚合查询。
    proc_json 存储该卡上的进程列表（来自 NVML），格式：
    [{"pid": 12345, "user": "djs", "mem_mb": 8192}, ...]
    """

    __tablename__ = "gpu_snapshots"
    # 「每节点最新一帧」（/api/gpu-live、/api/rgtop/frame）按 (node_name, collected_at DESC)
    # 直接走 Index Only Scan，代价只与节点数相关，不随总行数增长。
    __table_args__ = (
        Index("ix_gpu_snapshots_node_ts", "node_name", desc("collected_at")),
        # 作业效率分析：按作业号查该作业的 GPU 利用率均值
        Index(
            "idx_gpu_snapshots_job_time", "job_id", "collected_at",
            postgresql_where=text("job_id IS NOT NULL"),
        ),
    )

    id = Column(BigInteger, primary_key=True)
    node_name = Column(String(64), nullable=False, index=True)
    gpu_index = Column(Integer, nullable=False)
    gpu_uuid = Column(String(64), index=True)
    gpu_name = Column(String(128))
    job_id = Column(BigInteger, index=True)

    util_pct = Column(Integer)
    mem_used_mb = Column(Integer)
    mem_total_mb = Column(Integer)
    power_w = Column(Float)
    power_limit_w = Column(Float)        # agent.py 额外提供功率上限
    temp_c = Column(Integer)
    fan_pct = Column(Integer)

    proc_json = Column(Text)             # JSON: [{pid, user, mem_mb}, ...]

    collected_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class NodeHostStat(Base):
    """算力节点主机指标（agent 采集，与 gpu_snapshots 同周期）：CPU/内存/负载。

    用于节点页趋势（节点负载、内存使用）和 rgtop 的 CPU 行。
    """

    __tablename__ = "node_host_stats"
    # 与 gpu_snapshots 同理：每节点取最近一条主机指标（_fresh_host_stat）。
    __table_args__ = (
        Index("ix_node_host_stats_node_ts", "node_name", desc("collected_at")),
    )

    id = Column(BigInteger, primary_key=True)
    node_name = Column(String(64), nullable=False, index=True)
    cpu_count = Column(Integer)
    cpu_util_pct = Column(Float)
    load1 = Column(Float)
    load5 = Column(Float)
    load15 = Column(Float)
    mem_total_mb = Column(BigInteger)
    mem_used_mb = Column(BigInteger)
    mem_avail_mb = Column(BigInteger)     # MemAvailable
    mem_cache_mb = Column(BigInteger)     # Buffers + Cached + SReclaimable - Shmem
    swap_total_mb = Column(BigInteger)
    swap_free_mb = Column(BigInteger)
    cpu_cores = Column(Text)              # 每核占用，base36 编码（0..35），与 rgtop @@C 一致
    cpu_model = Column(String(128))
    sockets = Column(Integer)
    cpu_temp_c = Column(Float)            # 封装温度（coretemp/k10temp）
    uptime_s = Column(BigInteger)
    collected_at = Column(DateTime, nullable=False, default=datetime.utcnow, index=True)


class UserStat(Base):
    """用户资源使用汇总，按天聚合。"""

    __tablename__ = "user_stats"
    __table_args__ = (UniqueConstraint("user", "stat_date", name="uq_user_day"),)

    id = Column(Integer, primary_key=True)
    user = Column(String(64), nullable=False, index=True)
    stat_date = Column(DateTime, nullable=False, index=True)  # 仅日期部分

    submitted_count = Column(Integer, default=0)
    completed_count = Column(Integer, default=0)
    failed_count = Column(Integer, default=0)
    cancelled_count = Column(Integer, default=0)
    timeout_count = Column(Integer, default=0)
    running_count = Column(Integer, default=0)

    total_elapsed_seconds = Column(Integer, default=0)
    total_gpu_seconds = Column(BigInteger, default=0)  # GPU 卡时基础单位
    total_cpu_seconds = Column(BigInteger, default=0)

    success_rate = Column(Float)  # completed / ended

    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class ClusterAggStat(Base):
    """集群级资源聚合统计（多粒度）。

    granularity 取值：5m / 15m / 1h / 1d。
    由采集器定时从 NodeSnapshot 和 Job 聚合生成。
    用于集群趋势曲线：GPU 使用数、GPU 利用率、运行作业数、排队作业数。
    """

    __tablename__ = "cluster_agg_stats"
    __table_args__ = (
        UniqueConstraint("bucket_ts", "granularity", name="uq_cluster_agg"),
    )

    id = Column(BigInteger, primary_key=True)
    bucket_ts = Column(DateTime, nullable=False, index=True)  # 桶起始时间 UTC
    granularity = Column(String(8), nullable=False)  # 5m / 15m / 1h / 1d

    # GPU 资源
    gpu_total = Column(Integer, default=0)
    gpu_used = Column(Integer, default=0)          # 平均已分配 GPU 数
    gpu_util_avg = Column(Float)                   # 平均 GPU 利用率 %（来自 gpu_snapshots）

    # CPU/内存资源
    cpu_total = Column(Integer, default=0)
    cpu_used = Column(Integer, default=0)
    mem_total_gb = Column(Float)
    mem_used_gb = Column(Float)

    # 作业统计
    job_running = Column(Integer, default=0)       # 平均运行作业数
    job_pending = Column(Integer, default=0)       # 平均排队作业数

    # 节点统计
    nodes_total = Column(Integer, default=0)
    nodes_idle = Column(Integer, default=0)
    nodes_mixed = Column(Integer, default=0)
    nodes_allocated = Column(Integer, default=0)
    nodes_down = Column(Integer, default=0)

    sample_count = Column(Integer, default=0)      # 聚合样本数

    created_at = Column(DateTime, default=datetime.utcnow)


class JobHistory(Base):
    """作业历史记录（已结束作业的持久化存储）。

    从 sacct 定期同步，保留已完成/失败/超时/取消的作业。
    用于用户统计、历史查询、效率分析。
    """

    __tablename__ = "job_history"
    __table_args__ = (
        UniqueConstraint("cluster", "job_id", "submit_time", name="uq_job_history"),
        # 用户统计 / Top 榜：按用户 + 状态 + 提交时间范围过滤
        Index("idx_job_history_user_state_submit", "user", "state", "submit_time"),
    )

    id = Column(BigInteger, primary_key=True)
    cluster = Column(String(64), nullable=False, default="", index=True)
    job_id = Column(BigInteger, nullable=False, index=True)
    job_id_raw = Column(BigInteger, index=True)
    job_name = Column(String(256))
    array_task_id = Column(Integer)
    array_job_id = Column(BigInteger)

    user = Column(String(64), nullable=False, index=True)
    uid = Column(Integer)
    account = Column(String(64))
    partition = Column(String(64), index=True)
    qos = Column(String(64))

    state = Column(String(32), index=True)  # COMPLETED/FAILED/TIMEOUT/CANCELLED
    exit_code = Column(String(16))

    submit_time = Column(DateTime, nullable=False, index=True)
    start_time = Column(DateTime, index=True)
    end_time = Column(DateTime, index=True)
    eligible_time = Column(DateTime)

    elapsed_seconds = Column(Integer)
    suspended_seconds = Column(Integer, default=0)
    timelimit_minutes = Column(Integer)

    node_list = Column(Text)
    nodes_alloc = Column(Integer, default=0)

    # TRES 字段
    alloc_cpus = Column(Integer)
    alloc_gpus = Column(Integer, default=0, index=True)
    alloc_mem_mb = Column(BigInteger)
    req_cpus = Column(Integer)
    req_gpus = Column(Integer, default=0)
    req_mem_mb = Column(BigInteger)

    # sacct 效率数据
    total_cpu_seconds = Column(Float)       # TotalCPU
    max_rss_mb = Column(BigInteger)         # MaxRSS (峰值内存)

    # 原始 TRES
    alloc_tres_raw = Column(Text)
    req_tres_raw = Column(Text)

    # 采集元数据
    synced_at = Column(DateTime, default=datetime.utcnow)  # 同步时间


class JobEfficiencyAnalysis(Base):
    """作业效率分析结果（基于 jobeff.sh 脚本分析）。

    存储已结束作业的效率分析结果，包括：
    - CPU 效率、内存效率、时间效率
    - GPU 利用率趋势（若有采集）
    - 诊断建议

    分析结果压缩存储，用户点击作业时展开显示。
    """

    __tablename__ = "job_efficiency_analysis"

    id = Column(BigInteger, primary_key=True)
    job_id = Column(BigInteger, nullable=False, index=True, unique=True)
    cluster = Column(String(64), nullable=False, default="")

    # 效率指标
    time_eff_pct = Column(Float)            # 实际用时 / 申请时长 %
    cpu_eff_pct = Column(Float)             # TotalCPU / core-walltime %
    mem_eff_pct = Column(Float)             # MaxRSS / ReqMem %

    # GPU 指标（若有）
    gpu_util_avg = Column(Float)            # 平均 GPU 利用率 %
    gpu_util_p50 = Column(Float)            # 中位数
    gpu_util_p90 = Column(Float)            # P90
    gpu_mem_peak_mb = Column(Integer)       # GPU 显存峰值
    gpu_idle_ratio = Column(Float)          # GPU 空转占比 % (util < 10%)

    # 综合评分
    efficiency_score = Column(Integer)      # 0-100 综合评分

    # 诊断建议（JSON 数组，压缩存储）
    diagnosis = Column(Text)                # 诊断文本
    suggestions = Column(Text)              # 建议列表（JSON）

    # 趋势数据（可选，压缩存储）
    gpu_util_trend = Column(Text)           # GPU 利用率时序（JSON 数组，降采样到 60 点）
    gpu_mem_trend = Column(Text)            # GPU 显存时序（JSON 数组）

    analyzed_at = Column(DateTime, default=datetime.utcnow)


class UserMapping(Base):
    """用户名显示映射表。

    将 Slurm 账号（username）映射到显示名（display_name），
    支持多个账号合并为同一显示名（例如 djs/dejs → Jason）。
    - display_name 全局唯一
    - canonical_user 为该显示名的主账号（合并后对外展示的账号）
    - 并发写入通过 version 乐观锁保护
    """

    __tablename__ = "user_mappings"
    __table_args__ = (
        UniqueConstraint("username", name="uq_user_mapping_username"),
        UniqueConstraint("display_name", name="uq_user_mapping_display"),
    )

    id = Column(Integer, primary_key=True)
    username = Column(String(64), nullable=False, index=True)   # Slurm 账号
    display_name = Column(String(128))                          # 对外显示名（可为空表示未设置）
    groups = Column(Text)                                       # 项目组 ID 列表（逗号分隔，如 "1,3"）
    canonical_user = Column(String(64))                        # 合并主账号（可为 NULL 表示本账号就是主账号）
    note = Column(String(256))                                  # 备注（可选）
    version = Column(Integer, nullable=False, default=1)        # 乐观锁版本号
    updated_by = Column(String(64))                             # 最后修改人
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    created_at = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<UserMapping {self.username!r} → {self.display_name!r}>"


class ProjectGroup(Base):
    """项目组配置表。

    id 自增，一旦创建永不复用（不允许删除后重用）。
    只允许管理员修改 name；id 永久冻结。
    """

    __tablename__ = "project_groups"

    id = Column(Integer, primary_key=True, autoincrement=True)  # 永久自增，不复用
    name = Column(String(128), nullable=False, unique=True)     # 项目组名称
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def __repr__(self):
        return f"<ProjectGroup {self.id}: {self.name!r}>"


class PendingJobSnapshot(Base):
    """排队作业快照（用于等待趋势分析）。

    每 5 分钟采集一次当前排队作业的统计信息。
    用于生成排队等待趋势、P90 等待时长、原因分布等。
    """

    __tablename__ = "pending_job_snapshots"

    id = Column(BigInteger, primary_key=True)
    snapshot_ts = Column(DateTime, nullable=False, index=True)

    # 排队作业统计
    pending_total = Column(Integer, default=0)
    pending_gpu_demand = Column(Integer, default=0)     # 排队作业申请的总 GPU 数

    avg_wait_minutes = Column(Float)                    # 平均等待时长（分钟）
    p50_wait_minutes = Column(Float)                    # 中位数等待
    p90_wait_minutes = Column(Float)                    # P90 等待
    max_wait_minutes = Column(Float)                    # 最长等待

    long_pending_count = Column(Integer, default=0)     # 等待超过 30 分钟的作业数

    # 等待原因分布（JSON）
    reason_distribution = Column(Text)                  # {"Resources": 15, "Priority": 5, ...}

    # Top 用户/节点（JSON）
    top_users = Column(Text)                            # [{"user": "zhang", "pending": 5}, ...]
    top_nodes = Column(Text)                            # [{"node": "node01", "pending": 8}, ...]

    created_at = Column(DateTime, default=datetime.utcnow)
