#!/usr/bin/env python3
"""直接查询数据库中作业40的exit_code"""

import sys
sys.path.insert(0, '/app')

from slurm_monitor.storage.db import get_session
from slurm_monitor.storage.models import JobHistory
from sqlalchemy import select

with get_session() as session:
    stmt = select(JobHistory).where(JobHistory.job_id == 40)
    job = session.execute(stmt).scalar_one_or_none()

    if job:
        print(f"作业40在job_history表中的数据:")
        print(f"  job_id: {job.job_id}")
        print(f"  state: {job.state}")
        print(f"  exit_code: {job.exit_code} (类型: {type(job.exit_code)})")
        print(f"  timelimit_minutes: {job.timelimit_minutes}")
        print(f"  elapsed_seconds: {job.elapsed_seconds}")
    else:
        print("❌ 作业40在job_history表中不存在")
