"""TRES 字符串解析器

AllocTRES/ReqTRES 格式示例：
  "billing=18,cpu=8,gres/gpu=1,node=1"
  "billing=34,cpu=8,gres/gpu=1,mem=64G,node=1"
  "billing=28,cpu=8,gres/gpu=2,node=1"
"""

import re
from typing import Optional


def parse_tres(tres_str: str) -> dict:
    """将 TRES 字符串解析为字典。

    Args:
        tres_str: 形如 "billing=18,cpu=8,gres/gpu=1,mem=64G,node=1" 的 TRES 字符串

    Returns:
        解析后的字典，例如：
        {"billing": 18, "cpu": 8, "gpu": 1, "mem_mb": 65536, "node": 1}
    """
    if not tres_str or not tres_str.strip():
        return {}

    result = {}
    for part in tres_str.split(","):
        part = part.strip()
        if "=" not in part:
            continue
        key, _, value = part.partition("=")
        key = key.strip()
        value = value.strip()

        if key == "gres/gpu":
            result["gpu"] = _parse_int(value)
        elif key == "cpu":
            result["cpu"] = _parse_int(value)
        elif key == "mem":
            result["mem_mb"] = _parse_mem_mb(value)
        elif key == "node":
            result["node"] = _parse_int(value)
        elif key == "billing":
            result["billing"] = _parse_int(value)
        # 其余字段（如 gres/gpumem）按原始 key 保留，供扩展使用
        else:
            result[key] = value

    return result


def _parse_int(value: str) -> Optional[int]:
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def _parse_mem_mb(value: str) -> Optional[int]:
    """将内存字符串转换为 MiB。

    支持格式：512M、512m、64G、64g、2T、2000000M（无单位时视为 MiB）
    """
    if not value:
        return None
    value = value.strip()
    match = re.fullmatch(r"(\d+(?:\.\d+)?)([KkMmGgTt]?)", value)
    if not match:
        return None
    amount = float(match.group(1))
    unit = match.group(2).upper()
    multiplier = {"K": 1 / 1024, "M": 1, "G": 1024, "T": 1024 * 1024, "": 1}
    return int(amount * multiplier.get(unit, 1))


def gpu_seconds(alloc_tres: str, elapsed_seconds: int) -> int:
    """根据分配 TRES 和运行秒数计算 GPU 秒数（卡时的基础单位）。

    Args:
        alloc_tres: AllocTRES 字符串
        elapsed_seconds: 作业实际运行秒数

    Returns:
        GPU 秒数；若无 GPU 分配则返回 0
    """
    parsed = parse_tres(alloc_tres)
    gpu_count = parsed.get("gpu") or 0
    return gpu_count * elapsed_seconds
