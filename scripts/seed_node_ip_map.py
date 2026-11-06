#!/usr/bin/env python3
"""向 node_ip_map 表同步节点配置（幂等 upsert）。

从环境变量 NODE_IP_MAP 读取配置，格式：
  NODE_IP_MAP=node_name:ip:port:user:password[;node_name:ip:port:user:password...]

示例（.env）：
  NODE_IP_MAP=node1:10.0.0.11:22:user:your_password;node2:10.0.0.12:22:user:your_password

- port 省略或为空时默认 22
- 不在 NODE_IP_MAP 中的现有节点保持不变（不删除）
- 已存在的节点会更新 ip/port/user/password/enabled
"""

import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))


def parse_node_ip_map(raw: str) -> list[dict]:
    """解析 NODE_IP_MAP 环境变量，返回节点配置列表。"""
    nodes = []
    for entry in raw.strip().split(";"):
        entry = entry.strip()
        if not entry:
            continue
        parts = entry.split(":")
        if len(parts) < 2:
            print(f"  ⚠ 跳过格式错误的节点配置: {entry!r}")
            continue
        node_name = parts[0].strip()
        ip = parts[1].strip()
        port = int(parts[2].strip()) if len(parts) > 2 and parts[2].strip().isdigit() else 22
        ssh_user = parts[3].strip() if len(parts) > 3 and parts[3].strip() else "djs"
        ssh_password = parts[4].strip() if len(parts) > 4 else None
        nodes.append({
            "node_name": node_name,
            "ip": ip,
            "port": port,
            "ssh_user": ssh_user,
            "ssh_password": ssh_password or None,
        })
    return nodes


def main():
    raw = os.environ.get("NODE_IP_MAP", "").strip()
    if not raw:
        print("  NODE_IP_MAP 未配置，跳过节点 seed")
        return

    nodes = parse_node_ip_map(raw)
    if not nodes:
        print("  NODE_IP_MAP 解析结果为空，跳过")
        return

    from slurm_monitor.storage.db import get_session
    from slurm_monitor.storage.models import NodeIpMap

    with get_session() as s:
        for cfg in nodes:
            existing = s.query(NodeIpMap).filter_by(node_name=cfg["node_name"]).first()
            if existing:
                for k, v in cfg.items():
                    setattr(existing, k, v)
                existing.enabled = 1
                print(f"  更新节点: {cfg['node_name']} → {cfg['ip']}:{cfg['port']}")
            else:
                s.add(NodeIpMap(enabled=1, **cfg))
                print(f"  新增节点: {cfg['node_name']} → {cfg['ip']}:{cfg['port']}")

    print(f"  节点 seed 完成（{len(nodes)} 个节点）")


if __name__ == "__main__":
    main()
