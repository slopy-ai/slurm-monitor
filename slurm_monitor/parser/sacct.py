"""sacct 输出解析器

sacct --parsable2 输出使用 '|' 作为分隔符，末尾无额外 '|'（区别于 --parsable 的末尾 '|'）。
每行第一列为 JobID，格式有：
  - "35"          → 作业主记录
  - "35.batch"    → batch step
  - "35.extern"   → extern step
  - "35.0"        → 用户步骤
  - "35_2"        → 数组子任务
  - "35_2.batch"  → 数组子任务的 batch step
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional
from zoneinfo import ZoneInfo

from ..config import settings

_NONE_VALUES = {"", "None", "N/A", "Unknown"}
_PARSABLE2_SEP = "|"

# 作业 ID 解析：job_id、array_task_id、step_id
_JOB_ID_RE = re.compile(
    r"^(?P<job>\d+)"
    r"(?:_(?P<array_task>\d+|\[[\d%-]+\]))?"
    r"(?:\.(?P<step>.+))?$"
)


@dataclass
class SacctRecord:
    """一条 sacct 行记录（作业或 step）。"""

    raw_job_id: str          # 原始 JobID 字段，如 "35_2.batch"
    job_id: int              # 基础作业 ID
    array_task_id: Optional[str] = None  # 数组子任务 ID，非数组为 None
    step_id: Optional[str] = None        # step 标识，非 step 为 None

    # 作业基本信息
    job_id_raw: Optional[int] = None
    job_name: Optional[str] = None
    user: Optional[str] = None
    uid: Optional[int] = None
    account: Optional[str] = None
    group: Optional[str] = None
    partition: Optional[str] = None
    qos: Optional[str] = None

    state: Optional[str] = None
    exit_code: Optional[str] = None

    submit_time: Optional[datetime] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    eligible_time: Optional[datetime] = None
    elapsed_seconds: Optional[int] = None
    suspended_seconds: Optional[int] = None
    timelimit_minutes: Optional[int] = None

    node_list: Optional[str] = None
    nodes_alloc: Optional[int] = None

    alloc_tres: Optional[str] = None
    req_tres: Optional[str] = None

    # 消耗统计（需集群配置 jobacct_gather）
    cpu_time_seconds: Optional[int] = None
    total_cpu_seconds: Optional[float] = None
    max_rss_mb: Optional[int] = None
    tres_usage_in_tot: Optional[str] = None

    @property
    def is_step(self) -> bool:
        return self.step_id is not None

    @property
    def is_array_task(self) -> bool:
        return self.array_task_id is not None

    @property
    def is_main_job(self) -> bool:
        return not self.is_step


def _none(v: str) -> Optional[str]:
    return None if v in _NONE_VALUES else v


def _int(v: str) -> Optional[int]:
    try:
        return int(v)
    except (ValueError, TypeError):
        return None


def _float(v: str) -> Optional[float]:
    try:
        return float(v)
    except (ValueError, TypeError):
        return None


def _parse_exit_code(s: str) -> Optional[int]:
    """解析 ExitCode 字段，格式为 "退出码:信号"（如 "0:0"、"1:0"），返回退出码部分。"""
    if not s or s in _NONE_VALUES:
        return None
    # ExitCode 格式：exit_code:signal，取第一个数字
    parts = s.split(":", 1)
    try:
        return int(parts[0])
    except (ValueError, IndexError):
        return None


def _parse_elapsed(s: str) -> Optional[int]:
    """将 D-HH:MM:SS 或 HH:MM:SS 格式转换为秒数。"""
    if s in _NONE_VALUES:
        return None
    # 替换 D-HH:MM:SS 为统一格式
    s = s.strip()
    days = 0
    if "-" in s:
        parts = s.split("-", 1)
        days = int(parts[0])
        s = parts[1]
    hms = s.split(":")
    if len(hms) == 3:
        h, m, sec = int(hms[0]), int(hms[1]), int(hms[2])
        return days * 86400 + h * 3600 + m * 60 + sec
    return None


def parse_local_dt(s: str) -> Optional[datetime]:
    """将 sacct 的时间字符串解析为 UTC datetime。

    sacct 输出的是 slurmctld 所在时区的本地时间且不带时区后缀，
    按配置的 TIMEZONE（默认 Asia/Shanghai）解释后转为 UTC，与 REST 的 epoch 时间口径一致。
    """
    if not s or s in _NONE_VALUES:
        return None
    s = s.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            local = datetime.strptime(s, fmt).replace(tzinfo=ZoneInfo(settings.timezone))
            return local.astimezone(timezone.utc)
        except ValueError:
            continue
    return None


_parse_dt = parse_local_dt  # 兼容旧名


def _parse_timelimit(s: str) -> Optional[int]:
    """将时限字符串转换为分钟数。

    sacct 的 Timelimit 格式与 Elapsed 相同（D-HH:MM:SS 或 HH:MM:SS），
    _parse_elapsed 返回秒数，这里需除以 60 得到分钟数。
    """
    if s in _NONE_VALUES or s.lower() == "partition_limit":
        return None
    seconds = _parse_elapsed(s)
    return seconds // 60 if seconds is not None else None


def _parse_job_id_field(raw: str) -> tuple[int, Optional[str], Optional[str]]:
    """解析 JobID 字段，返回 (job_id, array_task_id, step_id)。"""
    m = _JOB_ID_RE.match(raw.strip())
    if not m:
        raise ValueError(f"Cannot parse JobID: {raw!r}")
    job_id = int(m.group("job"))
    array_task = m.group("array_task")
    step = m.group("step")
    return job_id, array_task, step


# 默认字段顺序，对应 ssh_client.sacct_jobs 中的 --format 顺序
# 末尾追加 TotalCPU/MaxRSS，用于 jobacct_gather 启用后的消耗统计
# sacct --format 的字段列表，ssh_client.sacct_jobs 直接复用这份，保证列顺序与解析一致
DEFAULT_FIELDS = [
    "JobID", "JobIDRaw", "User", "UID", "Account", "Group", "State", "QOS",
    "Partition", "NodeList", "Submit", "Eligible", "Start", "End", "Elapsed",
    "AllocTRES", "ReqTRES", "ExitCode", "Timelimit",
    "TotalCPU", "MaxRSS",
    "JobName",  # 必须放最后：作业名是用户自定义文本，可能含 "|"，解析时由最后一列吸收多余的分隔符
]


def parse_sacct_output(output: str, fields: list[str] = DEFAULT_FIELDS) -> list[SacctRecord]:
    """将 sacct --parsable2 输出解析为 SacctRecord 列表。

    Args:
        output: sacct --parsable2 -n 的标准输出字符串
        fields: 与 --format 参数对应的字段名列表，顺序必须一致

    Returns:
        解析后的记录列表（包含 step 记录）
    """
    records: list[SacctRecord] = []
    for line in output.splitlines():
        line = line.rstrip("\n").rstrip("\r")
        if not line:
            continue
        parts = line.split(_PARSABLE2_SEP, len(fields) - 1)  # 最后一列（JobName）可含分隔符
        # 列数不足时用空字符串补齐，避免越界
        while len(parts) < len(fields):
            parts.append("")
        col = dict(zip(fields, parts))

        raw_id = col.get("JobID", "")
        try:
            job_id, array_task, step = _parse_job_id_field(raw_id)
        except ValueError:
            continue

        rec = SacctRecord(raw_job_id=raw_id, job_id=job_id, array_task_id=array_task, step_id=step)

        rec.job_id_raw = _int(col.get("JobIDRaw", ""))
        rec.job_name = _none(col.get("JobName", ""))
        rec.user = _none(col.get("User", ""))
        rec.uid = _int(col.get("UID", ""))
        rec.account = _none(col.get("Account", ""))
        rec.group = _none(col.get("Group", ""))
        rec.state = _none(col.get("State", ""))
        rec.partition = _none(col.get("Partition", ""))
        rec.node_list = _none(col.get("NodeList", ""))
        rec.exit_code = _parse_exit_code(col.get("ExitCode", ""))
        rec.alloc_tres = _none(col.get("AllocTRES", ""))
        rec.req_tres = _none(col.get("ReqTRES", ""))
        rec.qos = _none(col.get("QOS", ""))

        rec.submit_time = _parse_dt(col.get("Submit", ""))
        rec.start_time = _parse_dt(col.get("Start", ""))
        rec.end_time = _parse_dt(col.get("End", ""))
        rec.eligible_time = _parse_dt(col.get("Eligible", ""))
        rec.elapsed_seconds = _parse_elapsed(col.get("Elapsed", ""))
        rec.suspended_seconds = _parse_elapsed(col.get("Suspended", ""))
        rec.timelimit_minutes = _parse_timelimit(col.get("Timelimit", ""))

        rec.nodes_alloc = _int(col.get("AllocNodes", ""))
        rec.cpu_time_seconds = _parse_elapsed(col.get("CPUTime", ""))
        rec.total_cpu_seconds = _float(col.get("TotalCPU", ""))
        rec.tres_usage_in_tot = _none(col.get("TRESUsageInTot", ""))

        # MaxRSS：格式如 "512K"、"2G"，转换为 MiB
        max_rss = col.get("MaxRSS", "")
        if max_rss and max_rss not in _NONE_VALUES:
            from .tres import _parse_mem_mb
            rec.max_rss_mb = _parse_mem_mb(max_rss)

        records.append(rec)

    return records
