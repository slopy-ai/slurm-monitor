#!/usr/bin/env python3
"""检查作业40在两个表中的数据"""

import sys
sys.path.insert(0, '/app')

from slurm_monitor.storage.db import get_session
from slurm_monitor.storage.models import Job, JobHistory
from sqlalchemy import select, desc

with get_session() as session:
    # 检查Job表
    job = session.query(Job).filter(Job.job_id == 40).order_by(desc(Job.submit_time)).first()
    print("=" * 80)
    print("Job表（实时作业）:")
    if job:
        print(f"  job_id: {job.job_id}")
        print(f"  state: {job.state}")
        print(f"  exit_code: {job.exit_code} (类型: {type(job.exit_code).__name__})")
        print(f"  timelimit_minutes: {job.timelimit_minutes}")
        print(f"  elapsed_seconds: {job.elapsed_seconds}")
    else:
        print("  ❌ 作业40不存在")

    # 检查JobHistory表
    job_hist = session.query(JobHistory).filter(JobHistory.job_id == 40).first()
    print("\n" + "=" * 80)
    print("JobHistory表（历史作业）:")
    if job_hist:
        print(f"  job_id: {job_hist.job_id}")
        print(f"  state: {job_hist.state}")
        print(f"  exit_code: {job_hist.exit_code} (类型: {type(job_hist.exit_code).__name__})")
        print(f"  timelimit_minutes: {job_hist.timelimit_minutes}")
        print(f"  elapsed_seconds: {job_hist.elapsed_seconds}")
    else:
        print("  ❌ 作业40不存在")
