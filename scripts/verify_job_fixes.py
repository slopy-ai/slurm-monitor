#!/usr/bin/env python3
"""验证作业字段修复 - 对比作业29和40的Slurm原始数据与Web API数据"""

import sys
import json
import requests
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo

def format_seconds(sec):
    """格式化秒数为 HH:MM:SS 或 D-HH:MM:SS"""
    if sec is None:
        return "N/A"
    days = sec // 86400
    hours = (sec % 86400) // 3600
    minutes = (sec % 3600) // 60
    seconds = sec % 60
    if days > 0:
        return f"{days}-{hours:02d}:{minutes:02d}:{seconds:02d}"
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

def verify_job(job_id, expected_data):
    """验证单个作业的字段

    expected_data: 从Slurm终端获取的期望值字典
    """
    print(f"\n{'='*80}")
    print(f"验证作业 {job_id}")
    print(f"{'='*80}")

    # 获取Web API数据
    try:
        resp = requests.get(f"http://192.168.3.102:8000/api/job/{job_id}/analysis", timeout=5)
        if resp.status_code != 200:
            print(f"❌ API请求失败: {resp.status_code}")
            return False
        api_data = resp.json().get("data", {})
    except Exception as e:
        print(f"❌ API请求异常: {e}")
        return False

    # 逐字段对比
    all_match = True

    checks = [
        ("job_id", "JobID", expected_data.get("job_id"), api_data.get("job_id")),
        ("job_name", "JobName", expected_data.get("job_name"), api_data.get("job_name")),
        ("user", "User", expected_data.get("user"), api_data.get("user")),
        ("partition", "Partition", expected_data.get("partition"), api_data.get("partition")),
        ("state", "State", expected_data.get("state"), api_data.get("state")),
        ("exit_code", "ExitCode", str(expected_data.get("exit_code")) if expected_data.get("exit_code") is not None else None, api_data.get("exit_code")),
        ("nodes", "Nodes", expected_data.get("nodes"), api_data.get("nodes")),
        ("cores", "Cores", expected_data.get("cores"), api_data.get("cores")),
        ("gpus", "GPUs", expected_data.get("gpus"), api_data.get("gpus")),
        ("elapsed_sec", "Elapsed(sec)", expected_data.get("elapsed_sec"), api_data.get("elapsed_sec")),
        ("timelimit_sec", "Timelimit(sec)", expected_data.get("timelimit_sec"), api_data.get("timelimit_sec")),
        ("time_eff_pct", "TimeEff%", expected_data.get("time_eff_pct"), api_data.get("time_eff_pct")),
    ]

    print(f"\n{'字段':<20} {'期望值':<25} {'实际值':<25} {'状态':<10}")
    print("-" * 80)

    for field, display_name, expected, actual in checks:
        # 浮点数比较允许小误差
        if isinstance(expected, float) and isinstance(actual, (int, float)):
            match = abs(expected - actual) < 0.01
        else:
            match = expected == actual

        status = "✅" if match else "❌"
        if not match:
            all_match = False

        print(f"{display_name:<20} {str(expected):<25} {str(actual):<25} {status:<10}")

    # 显示格式化的时间字段
    print(f"\n【时间格式化显示】")
    print(f"  Elapsed     : {api_data.get('elapsed')} (期望: {expected_data.get('elapsed')})")
    print(f"  Timelimit   : {api_data.get('timelimit')} (期望: {expected_data.get('timelimit')})")

    # 显示TRES
    print(f"\n【TRES字段】")
    print(f"  AllocTRES   : {api_data.get('alloc_tres')}")
    print(f"  期望        : {expected_data.get('alloc_tres')}")

    if all_match:
        print(f"\n✅ 作业{job_id}所有字段验证通过")
    else:
        print(f"\n❌ 作业{job_id}存在字段不匹配")

    return all_match

def main():
    print("=" * 80)
    print("作业字段修复验证")
    print("=" * 80)

    # 作业40 - 成功作业 (COMPLETED)
    job40_expected = {
        "job_id": 40,
        "job_name": "gpuinfo",
        "user": "djs",
        "partition": "h200",
        "state": "COMPLETED",
        "exit_code": 0,  # 从 "0:0" 解析出来
        "nodes": 1,
        "cores": 2,
        "gpus": 1,
        "elapsed": "00:00:18",
        "elapsed_sec": 18,
        "timelimit": "00:10:00",
        "timelimit_sec": 600,
        "time_eff_pct": 3.0,
        "alloc_tres": "billing=12,cpu=2,gres/gpu=1,node=1",
    }

    # 作业29 - 失败作业 (FAILED)
    # 注意：需要从Slurm终端获取实际值，这里是示例
    job29_expected = {
        "job_id": 29,
        "job_name": "bash",
        "user": "djs",
        "partition": "h200",
        "state": "FAILED",
        "exit_code": None,  # 需要从sacct确认实际退出码
        "nodes": 1,
        "cores": 2,
        "gpus": 1,
        "elapsed": "00:00:10",
        "elapsed_sec": 10,
        "timelimit": "7-00:00:00",
        "timelimit_sec": 604800,  # 7天
        "time_eff_pct": 0.0,
        "alloc_tres": "billing=12,cpu=2,gres/gpu=1,node=1",
    }

    result40 = verify_job(40, job40_expected)
    result29 = verify_job(29, job29_expected)

    print(f"\n{'='*80}")
    print("验证总结")
    print(f"{'='*80}")
    print(f"作业40 (COMPLETED): {'✅ 通过' if result40 else '❌ 失败'}")
    print(f"作业29 (FAILED):    {'✅ 通过' if result29 else '❌ 失败'}")

    if result40 and result29:
        print(f"\n🎉 所有修复验证成功！")
        return 0
    else:
        print(f"\n⚠️  部分字段仍需调整")
        return 1

if __name__ == "__main__":
    sys.exit(main())
