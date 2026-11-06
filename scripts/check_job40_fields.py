#!/usr/bin/env python3
"""检查作业40的所有字段，对比数据库与Web API显示"""

import sys
import json
from datetime import datetime

sys.path.insert(0, '/app')

from slurm_monitor.storage.db import get_session
from slurm_monitor.storage.models import JobHistory
from sqlalchemy import select

def format_datetime(dt):
    """格式化时间为本地时间显示"""
    if dt is None:
        return "None"
    # UTC时间，显示时转为本地（假设+8）
    from datetime import timezone, timedelta
    local = dt.astimezone(timezone(timedelta(hours=8)))
    return local.strftime("%Y-%m-%d %H:%M:%S")

def format_duration(seconds):
    """格式化秒数为 HH:MM:SS"""
    if seconds is None:
        return "None"
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def main():
    with get_session() as session:
        stmt = select(JobHistory).where(JobHistory.job_id == 40)
        job = session.execute(stmt).scalar_one_or_none()

        if not job:
            print("❌ 作业40未找到，可能尚未同步到数据库")
            return

        print("=" * 80)
        print("作业40 数据库字段详情")
        print("=" * 80)

        # 基本信息
        print(f"\n【基本信息】")
        print(f"  job_id          : {job.job_id}")
        print(f"  job_id_raw      : {job.job_id_raw}")
        print(f"  job_name        : {job.job_name}")
        print(f"  array_task_id   : {job.array_task_id}")
        print(f"  array_job_id    : {job.array_job_id}")
        print(f"  user            : {job.user}")
        print(f"  account         : {job.account}")
        print(f"  partition       : {job.partition}")
        print(f"  qos             : {job.qos}")
        print(f"  state           : {job.state}")
        print(f"  exit_code       : {job.exit_code}")

        # 时间信息
        print(f"\n【时间信息】")
        print(f"  submit_time     : {format_datetime(job.submit_time)}")
        print(f"  start_time      : {format_datetime(job.start_time)}")
        print(f"  end_time        : {format_datetime(job.end_time)}")
        print(f"  elapsed_seconds : {job.elapsed_seconds} 秒 = {format_duration(job.elapsed_seconds)}")
        print(f"  timelimit_minutes: {job.timelimit_minutes} 分钟 = {format_duration(job.timelimit_minutes * 60 if job.timelimit_minutes else 0)}")

        # 资源信息
        print(f"\n【资源信息】")
        print(f"  node_list       : {job.node_list}")
        print(f"  nodes_alloc     : {job.nodes_alloc}")
        print(f"  alloc_cpus      : {job.alloc_cpus}")
        print(f"  alloc_gpus      : {job.alloc_gpus}")
        print(f"  req_cpus        : {job.req_cpus}")
        print(f"  req_gpus        : {job.req_gpus}")
        print(f"  req_mem_mb      : {job.req_mem_mb} MB = {job.req_mem_mb / 1024 if job.req_mem_mb else 0:.2f} GB")

        # 消耗统计
        print(f"\n【消耗统计】")
        print(f"  total_cpu_seconds: {job.total_cpu_seconds}")
        print(f"  max_rss_mb      : {job.max_rss_mb}")

        # TRES原始字符串
        print(f"\n【TRES原始】")
        print(f"  alloc_tres_raw  : {job.alloc_tres_raw}")
        print(f"  req_tres_raw    : {job.req_tres_raw}")

        # 计算效率指标
        print(f"\n【效率计算】")
        if job.elapsed_seconds and job.timelimit_minutes:
            timelimit_sec = job.timelimit_minutes * 60
            time_eff = round(100.0 * job.elapsed_seconds / timelimit_sec, 2)
            print(f"  时间效率        : {time_eff}% ({job.elapsed_seconds}秒 / {timelimit_sec}秒)")
        else:
            print(f"  时间效率        : N/A")

        if job.total_cpu_seconds is not None and job.alloc_cpus and job.elapsed_seconds:
            core_walltime = job.alloc_cpus * job.elapsed_seconds
            cpu_eff = round(100.0 * job.total_cpu_seconds / core_walltime, 2)
            print(f"  CPU效率         : {cpu_eff}% ({format_duration(int(job.total_cpu_seconds))} / {format_duration(core_walltime)} core-walltime)")
        else:
            print(f"  CPU效率         : N/A (TotalCPU={job.total_cpu_seconds}, AllocCPUs={job.alloc_cpus})")

        if job.max_rss_mb and job.req_mem_mb:
            mem_eff = round(100.0 * job.max_rss_mb / job.req_mem_mb, 2)
            print(f"  内存效率        : {mem_eff}% ({job.max_rss_mb}MB / {job.req_mem_mb}MB)")
        else:
            print(f"  内存效率        : N/A (MaxRSS={job.max_rss_mb}, ReqMem={job.req_mem_mb})")

        print(f"\n{'=' * 80}")
        print("请对比Slurm终端 jobeff.sh 的输出，检查每个字段是否一致")

if __name__ == "__main__":
    main()
