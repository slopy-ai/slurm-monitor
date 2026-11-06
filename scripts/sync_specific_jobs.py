#!/usr/bin/env python3
"""手动同步指定作业的完整数据（从sacct重新获取并更新数据库）"""

import sys
sys.path.insert(0, '/app')

from slurm_monitor.sync.job_history_sync import JobHistorySyncer
from slurm_monitor.storage.db import get_session

def sync_specific_jobs(job_ids: list[int]):
    """同步指定作业ID的数据"""
    syncer = JobHistorySyncer(cluster="")

    for job_id in job_ids:
        print(f"同步作业 {job_id}...")
        # 直接用sacct获取特定作业
        try:
            from slurm_monitor.collector.ssh_client import ssh_client
            cmd = f"sacct -j {job_id} --format=JobID,JobName,User,Account,Partition,QOS,State,ExitCode,Submit,Start,End,Elapsed,Timelimit,NodeList,NNodes,AllocCPUS,AllocTRES,ReqCPUS,ReqTRES,ReqMem,TotalCPU,MaxRSS,JobIDRaw --parsable2 --noheader --allocations"
            output = ssh_client.run(cmd, timeout=30)

            if not output.strip():
                print(f"  ⚠️  作业 {job_id} 未找到")
                continue

            # 解析并更新
            jobs = []
            for line in output.strip().split("\n"):
                if line and not line.startswith("JobID"):
                    job_data = syncer._parse_sacct_line(line)
                    if job_data:
                        jobs.append(job_data)

            if jobs:
                from sqlalchemy.dialects.postgresql import insert
                from slurm_monitor.storage.models import JobHistory
                from datetime import datetime, timezone

                with get_session() as session:
                    for job in jobs:
                        job['synced_at'] = datetime.now(timezone.utc)

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
                            "timelimit_minutes": stmt.excluded.timelimit_minutes,
                            "node_list": stmt.excluded.node_list,
                            "alloc_cpus": stmt.excluded.alloc_cpus,
                            "alloc_gpus": stmt.excluded.alloc_gpus,
                            "total_cpu_seconds": stmt.excluded.total_cpu_seconds,
                            "max_rss_mb": stmt.excluded.max_rss_mb,
                            "synced_at": datetime.now(timezone.utc),
                        }
                    )
                    session.execute(stmt)
                    session.commit()

                print(f"  ✅ 作业 {job_id} 同步成功 ({len(jobs)} 条记录)")
            else:
                print(f"  ⚠️  作业 {job_id} 解析失败")

        except Exception as e:
            print(f"  ❌ 作业 {job_id} 同步失败: {e}")

if __name__ == "__main__":
    # 同步作业29和40
    sync_specific_jobs([29, 40])
