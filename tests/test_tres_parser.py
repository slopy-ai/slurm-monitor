"""测试 TRES 解析器"""

import pytest
from slurm_monitor.parser.tres import parse_tres, gpu_seconds, _parse_mem_mb


class TestParseTres:
    def test_full_tres(self):
        result = parse_tres("billing=18,cpu=8,gres/gpu=1,mem=64G,node=1")
        assert result["cpu"] == 8
        assert result["gpu"] == 1
        assert result["mem_mb"] == 65536  # 64 * 1024
        assert result["node"] == 1
        assert result["billing"] == 18

    def test_multi_gpu(self):
        result = parse_tres("billing=28,cpu=8,gres/gpu=2,node=1")
        assert result["gpu"] == 2

    def test_no_gpu(self):
        result = parse_tres("billing=2,cpu=2,node=1")
        assert result.get("gpu") is None

    def test_empty(self):
        assert parse_tres("") == {}
        assert parse_tres(None) == {}

    def test_large_mem(self):
        result = parse_tres("billing=1,cpu=2,gres/gpu=1,mem=2000000M,node=1")
        assert result["mem_mb"] == 2000000

    def test_mem_no_unit(self):
        # 无单位视为 MiB
        result = parse_tres("mem=512")
        assert result["mem_mb"] == 512


class TestParseMem:
    def test_gib(self):
        assert _parse_mem_mb("64G") == 65536

    def test_mib(self):
        assert _parse_mem_mb("512M") == 512

    def test_lowercase(self):
        assert _parse_mem_mb("32g") == 32768

    def test_tib(self):
        assert _parse_mem_mb("2T") == 2 * 1024 * 1024

    def test_invalid(self):
        assert _parse_mem_mb("abc") is None


class TestGpuSeconds:
    def test_one_gpu_one_hour(self):
        tres = "billing=10,cpu=4,gres/gpu=1,node=1"
        assert gpu_seconds(tres, 3600) == 3600

    def test_four_gpus(self):
        tres = "billing=28,cpu=8,gres/gpu=4,node=1"
        assert gpu_seconds(tres, 3600) == 4 * 3600

    def test_no_gpu(self):
        assert gpu_seconds("cpu=4,node=1", 3600) == 0
