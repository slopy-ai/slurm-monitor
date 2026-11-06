"""测试 sacct 输出解析器"""

from slurm_monitor.parser.sacct import parse_sacct_output, DEFAULT_FIELDS


SAMPLE_OUTPUT = """\
29|29|djs|1001|djs|djs|FAILED|h100|h100-3|2026-10-16T16:40:34|2026-10-16T16:40:34|2026-10-16T16:40:34|00:00:00|billing=12,cpu=2,gres/gpu=1,node=1|billing=499,cpu=1,gres/gpu=1,mem=2000000M,node=1|1:0|1-00:00:00
31|31|||djs|djs|FAILED|h100|h100-3|2026-10-16T17:01:24|2026-10-16T17:01:24|2026-10-16T17:01:25|00:00:01|billing=18,cpu=8,gres/gpu=1,node=1|billing=34,cpu=8,gres/gpu=1,mem=64G,node=1|0:0|08:00:00
31.batch||||||FAILED|||||||||cpu=8,gres/gpu=1,mem=0,node=1||
34|34|djs|1001|||COMPLETED|h100|h100-3|2026-10-16T18:00:00|2026-10-16T18:00:10|2026-10-16T18:02:10|00:02:00|billing=28,cpu=8,gres/gpu=2,node=1|billing=58,cpu=8,gres/gpu=2,mem=120G,node=1|0:0|08:00:00
"""


def test_parse_main_jobs():
    records = parse_sacct_output(SAMPLE_OUTPUT, DEFAULT_FIELDS)
    main = [r for r in records if r.is_main_job]
    assert len(main) == 3


def test_parse_step():
    records = parse_sacct_output(SAMPLE_OUTPUT, DEFAULT_FIELDS)
    steps = [r for r in records if r.is_step]
    assert len(steps) == 1
    assert steps[0].step_id == "batch"
    assert steps[0].job_id == 31


def test_user_field():
    records = parse_sacct_output(SAMPLE_OUTPUT, DEFAULT_FIELDS)
    job29 = next(r for r in records if r.job_id == 29 and r.is_main_job)
    assert job29.user == "djs"
    assert job29.uid == 1001


def test_tres_preserved():
    records = parse_sacct_output(SAMPLE_OUTPUT, DEFAULT_FIELDS)
    job34 = next(r for r in records if r.job_id == 34 and r.is_main_job)
    assert job34.alloc_tres == "billing=28,cpu=8,gres/gpu=2,node=1"


def test_elapsed():
    records = parse_sacct_output(SAMPLE_OUTPUT, DEFAULT_FIELDS)
    job34 = next(r for r in records if r.job_id == 34 and r.is_main_job)
    assert job34.elapsed_seconds == 120


def test_null_user_becomes_none():
    records = parse_sacct_output(SAMPLE_OUTPUT, DEFAULT_FIELDS)
    job31 = next(r for r in records if r.job_id == 31 and r.is_main_job)
    # User 字段为空，应解析为 None
    assert job31.user is None


def test_empty_output():
    records = parse_sacct_output("", DEFAULT_FIELDS)
    assert records == []
