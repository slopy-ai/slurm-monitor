#!/usr/bin/env python3
"""作业历史同步器

从 sacct 同步已结束的作业到 job_history 表。
定期运行，获取最近结束的作业并持久化。

用法:
    python -m slurm_monitor.sync.job_history_sync
"""

import logging
import shlex
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from typing import List, Dict, Any, Optional

from sqlalchemy.dialects.postgresql import insert

from ..storage.db import get_session
from ..storage.models import JobHistory
from ..config import settings
from ..parser.sacct import parse_local_dt
from ..collector.ssh_client import ssh_client

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


class JobHistorySyncer:
    """作业历史同步器"""

    def __init__(self, cluster: str = ""):
        self.cluster = cluster

    def sync_recent_jobs(self, hours: int = 24):
        """同步最近 N 小时内结束的作业

        Args:
            hours: 回溯小时数
        """
        logger.info(f"开始同步最近 {hours} 小时的作业历史")

        # 计算起始时间：sacct 按集群本地时间解释 --starttime，所以这里也用本地时区
        start_time = datetime.now(ZoneInfo(settings.timezone)) - timedelta(hours=hours)
        start_time_str = start_time.strftime("%Y-%m-%dT%H:%M:%S")

        # 调用 sacct 获取作业数据
        jobs = self._fetch_jobs_from_sacct(start_time_str)

        if not jobs:
            logger.info("没有找到新的已结束作业")
            return

        logger.info(f"获取到 {len(jobs)} 个作业记录")

        # 写入数据库
        with get_session() as session:
            stmt = insert(JobHistory).values(jobs)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_job_history",
                set_={
                    "job_name": stmt.excluded.job_name,
                    "state": stmt.excluded.state,
                    "exit_code": stmt.excluded.exit_code,
                    "start_time": stmt.excluded.start_time,
                    "end_time": stmt.excluded.end_time,
                    "elapsed_seconds": stmt.excluded.elapsed_seconds,
                    "node_list": stmt.excluded.node_list,
                    "alloc_cpus": stmt.excluded.alloc_cpus,
                    "array_job_id": stmt.excluded.array_job_id,
                    "alloc_gpus": stmt.excluded.alloc_gpus,
                    "alloc_mem_mb": stmt.excluded.alloc_mem_mb,
                    "total_cpu_seconds": stmt.excluded.total_cpu_seconds,
                    "max_rss_mb": stmt.excluded.max_rss_mb,
                    "synced_at": datetime.now(timezone.utc),
                }
            )
            session.execute(stmt)
            session.commit()

        logger.info(f"已同步 {len(jobs)} 个作业到历史表")

    def _fetch_jobs_from_sacct(self, start_time: str) -> List[Dict[str, Any]]:
        """从 sacct 获取作业数据

        Args:
            start_time: 起始时间 (ISO 格式)

        Returns:
            作业数据列表
        """
        try:
            # 构造 sacct 命令（不用 --allocations，需要读取 .batch 步骤的 CPU/内存数据）
            cmd = [
                "sacct",
                "--allusers",  # 否则非特权账号只能看到自己的作业
                "--starttime", start_time,
                "--endtime", "now",  # 使用 --state 时必须带结束时间，否则 sacct 返回空
                "--state", "COMPLETED,FAILED,TIMEOUT,CANCELLED",
                "--format", "JobID,JobName,User,Account,Partition,QOS,State,ExitCode,"
                            "Submit,Start,End,Elapsed,Timelimit,NodeList,NNodes,"
                            "AllocCPUS,AllocTRES,ReqCPUS,ReqTRES,ReqMem,"
                            "TotalCPU,MaxRSS,JobIDRaw",
                "--parsable2",
                "--noheader",
            ]

            # 容器内没有 Slurm 客户端，通过 SSH 在登录节点执行（与 collector 共用连接配置）
            output = ssh_client.run(" ".join(shlex.quote(c) for c in cmd), timeout=120)

            # 解析输出，合并 batch 步骤的资源数据到主作业
            jobs_dict = {}  # key: job_id, value: job_data
            for line in output.strip().split("\n"):
                if not line:
                    continue

                try:
                    job_data = self._parse_sacct_line(line)
                    if not job_data:
                        continue

                    job_id = job_data["job_id"]
                    is_batch_step = job_data.pop("_is_batch_step", False)

                    if is_batch_step:
                        # .batch 步骤：只提取资源数据，合并到主作业
                        if job_id in jobs_dict:
                            if job_data.get("total_cpu_seconds") is not None:
                                jobs_dict[job_id]["total_cpu_seconds"] = job_data["total_cpu_seconds"]
                            if job_data.get("max_rss_mb") is not None:
                                jobs_dict[job_id]["max_rss_mb"] = job_data["max_rss_mb"]
                    else:
                        # 主作业：直接记录（可能后续被 batch 步骤的资源数据覆盖）
                        jobs_dict[job_id] = job_data

                except Exception as e:
                    logger.warning(f"解析作业数据失败: {line[:100]}... 错误: {e}")

            return list(jobs_dict.values())

        except Exception as e:
            logger.error(f"获取 sacct 数据失败: {e}")
            return []

    def _parse_sacct_line(self, line: str) -> Optional[Dict[str, Any]]:
        """解析 sacct 输出的一行

        Args:
            line: sacct 输出行

        Returns:
            作业数据字典，解析失败返回 None
        """
        fields = line.split("|")
        # 作业名（第 2 列）是用户自定义文本，可能含 "|"：多出来的分隔符数量 = 被拆开的段数，合并回去
        extra = len(fields) - 23
        if extra > 0:
            fields[1:2 + extra] = ["|".join(fields[1:2 + extra])]
        if len(fields) < 23:
            return None

        # 识别 step（batch/extern 等）
        job_id_str = fields[0]
        is_batch_step = False

        if "." in job_id_str:
            # 有 step 标识，检查是否是 .batch
            base_id, step_name = job_id_str.rsplit(".", 1)
            if step_name == "batch":
                is_batch_step = True
                job_id_str = base_id  # 提取基础作业 ID
            else:
                # 其他 step（extern/0 等）跳过
                return None

        # 数组子任务：JobID 形如 "60_0"，JobIDRaw 是该子任务唯一的真实作业号。
        # 用 JobIDRaw 作为 job_id，避免同一数组的子任务因唯一键 (cluster, job_id, submit_time) 互相覆盖。
        array_task_id = None
        if "_" in job_id_str:
            suffix = job_id_str.split("_", 1)[1]
            if not suffix.isdigit():  # 形如 60_[1-10] 的未展开数组，不是具体作业
                return None
            array_task_id = int(suffix)

        try:
            # JobIDRaw 可能也带 .batch 后缀，需要去除
            job_id_raw = fields[22]
            if "." in job_id_raw:
                job_id_raw = job_id_raw.split(".", 1)[0]
            job_id = int(job_id_raw)
        except ValueError:
            return None

        # 解析时间
        submit_time = self._parse_time(fields[8])
        start_time = self._parse_time(fields[9])
        end_time = self._parse_time(fields[10])

        # 解析时长
        elapsed_seconds = self._parse_duration(fields[11])
        timelimit_minutes = self._parse_duration(fields[12], to_minutes=True)

        # 解析 TRES
        alloc_tres = fields[16]
        alloc_gpus = self._parse_tres_gpu(alloc_tres)

        req_tres = fields[18]
        req_gpus = self._parse_tres_gpu(req_tres)

        # 解析内存
        req_mem_mb = self._parse_memory(fields[19], int(fields[14] or 1), int(fields[15] or 1))

        # 解析 TotalCPU
        total_cpu_seconds = self._parse_duration(fields[20])

        # 解析 MaxRSS
        max_rss_mb = self._parse_memory_rss(fields[21])

        return {
            "_is_batch_step": is_batch_step,  # 内部标记，用于数据合并逻辑
            "cluster": self.cluster,
            "job_id": job_id,
            "job_id_raw": job_id,
            "array_task_id": array_task_id,
            "array_job_id": int(job_id_str.split("_", 1)[0]) if array_task_id is not None else None,
            "job_name": fields[1] or None,
            "user": fields[2] or None,
            "account": fields[3] or None,
            "partition": fields[4] or None,
            "qos": fields[5] or None,
            "state": fields[6].split()[0] if fields[6] else None,
            "exit_code": self._parse_exit_code(fields[7]),
            "submit_time": submit_time,
            "start_time": start_time,
            "end_time": end_time,
            "elapsed_seconds": elapsed_seconds,
            "timelimit_minutes": timelimit_minutes,
            "node_list": fields[13] or None,
            "nodes_alloc": int(fields[14]) if fields[14] and fields[14].isdigit() else 0,
            "alloc_cpus": int(fields[15]) if fields[15] and fields[15].isdigit() else 0,
            "alloc_gpus": alloc_gpus,
            "req_cpus": int(fields[17]) if fields[17] and fields[17].isdigit() else 0,
            "req_gpus": req_gpus,
            "req_mem_mb": req_mem_mb,
            "total_cpu_seconds": total_cpu_seconds,
            "max_rss_mb": max_rss_mb,
            "alloc_tres_raw": alloc_tres,
            "req_tres_raw": req_tres,
        }

    def _parse_time(self, time_str: str) -> Optional[datetime]:
        """解析 sacct 时间（本地时间 → UTC，见 parse_local_dt）"""
        return parse_local_dt(time_str)

    def _parse_exit_code(self, exit_code_str: str) -> Optional[int]:
        """解析 ExitCode 字段，格式为 "退出码:信号"（如 "0:0"、"1:0"），返回退出码部分。"""
        if not exit_code_str or exit_code_str in ("", "None", "N/A"):
            return None
        # ExitCode 格式：exit_code:signal，取第一个数字
        parts = exit_code_str.split(":", 1)
        try:
            return int(parts[0])
        except (ValueError, IndexError):
            return None

    def _parse_duration(self, duration_str: str, to_minutes: bool = False) -> Optional[float]:
        """解析时长字符串

        Args:
            duration_str: 时长字符串 (DD-HH:MM:SS 或 HH:MM:SS 或 MM:SS 或 MM:SS.ms)
            to_minutes: 是否转换为分钟

        Returns:
            秒数或分钟数（float 以支持 TotalCPU 的小数秒）
        """
        if not duration_str or duration_str in ("UNLIMITED", "Partition_Limit"):
            return None

        try:
            days = 0
            if "-" in duration_str:
                day_part, time_part = duration_str.split("-", 1)
                days = int(day_part)
                duration_str = time_part

            parts = duration_str.split(":")
            if len(parts) == 3:
                hours = int(parts[0])
                minutes = int(parts[1])
                seconds = float(parts[2])  # 支持小数秒
            elif len(parts) == 2:
                hours = 0
                minutes = int(parts[0])
                seconds = float(parts[1])  # 支持小数秒
            else:
                return None

            total_seconds = days * 86400 + hours * 3600 + minutes * 60 + seconds

            if to_minutes:
                return int(total_seconds / 60)
            else:
                return total_seconds

        except:
            return None

    def _parse_tres_gpu(self, tres_str: str) -> int:
        """从 TRES 字符串解析 GPU 数量

        Args:
            tres_str: TRES 字符串，例如 "cpu=16,mem=64G,gres/gpu=2"

        Returns:
            GPU 数量
        """
        if not tres_str:
            return 0

        for part in tres_str.split(","):
            if "gres/gpu=" in part:
                try:
                    return int(part.split("=")[1])
                except:
                    return 0

        return 0

    def _parse_memory(self, mem_str: str, nnodes: int, ncpus: int) -> Optional[int]:
        """解析内存字符串

        Args:
            mem_str: 内存字符串，例如 "64G" 或 "4Gc" 或 "120Gn"
            nnodes: 节点数
            ncpus: CPU 数

        Returns:
            内存 MB 数
        """
        if not mem_str or mem_str == "0":
            return None

        mem_str = mem_str.strip()

        # 解析单位和值
        multiplier = {"K": 1, "M": 1, "G": 1024, "T": 1024 * 1024}

        if mem_str[-1] == "c":  # per-cpu
            mem_str = mem_str[:-1]
            per_cpu = True
        elif mem_str[-1] == "n":  # per-node
            mem_str = mem_str[:-1]
            per_node = True
        else:
            per_cpu = False
            per_node = False

        if mem_str[-1] in multiplier:
            unit = mem_str[-1]
            value = float(mem_str[:-1])
            mb = value * multiplier[unit]
        else:
            mb = float(mem_str)  # 默认 MB

        if per_cpu:
            mb *= ncpus
        elif per_node:
            mb *= nnodes

        return int(mb)

    def _parse_memory_rss(self, rss_str: str) -> Optional[int]:
        """解析 MaxRSS 字符串

        Args:
            rss_str: MaxRSS 字符串，例如 "12345K" 或 "123M"

        Returns:
            内存 MB 数（四舍五入）
        """
        if not rss_str or rss_str == "0":
            return None

        multiplier = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024}

        if rss_str[-1] in multiplier:
            unit = rss_str[-1]
            value = float(rss_str[:-1])
            return round(value * multiplier[unit])  # 四舍五入而不是向下取整
        else:
            return round(float(rss_str) / 1024)  # 默认 KB


def main():
    """主入口"""
    import argparse

    parser = argparse.ArgumentParser(description="同步作业历史")
    parser.add_argument("--hours", type=int, default=24, help="回溯小时数")
    parser.add_argument("--cluster", type=str, default="", help="集群名称")

    args = parser.parse_args()

    syncer = JobHistorySyncer(cluster=args.cluster)
    syncer.sync_recent_jobs(hours=args.hours)


if __name__ == "__main__":
    main()
