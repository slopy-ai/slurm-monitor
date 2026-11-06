#!/usr/bin/env python3
"""管理员脚本：批量设置用户名映射 / 合并账号。

功能：
  1. 批量导入映射（CSV：username,display_name[,note]）
  2. 合并多个账号为同一显示名（dejs/djs → Jason，对外只显示 Jason）
  3. 查看当前所有映射
  4. 删除单条映射

用法:
    # 查看所有映射
    python scripts/user_mapping_cli.py list

    # 设置单条映射
    python scripts/user_mapping_cli.py set djs "张三"

    # 合并账号：dejs 和 djs 都显示为 "张三"，djs 为 canonical 主账号
    python scripts/user_mapping_cli.py merge --display "张三" --canonical djs dejs djs

    # 从 CSV 批量导入（表头：username,display_name,note）
    python scripts/user_mapping_cli.py import mappings.csv

    # 删除单条
    python scripts/user_mapping_cli.py delete dejs

环境要求：在 web 容器内执行，或本地能连数据库。
    docker exec slurm-monitor-web python3 /app/scripts/user_mapping_cli.py list
"""

import sys
from pathlib import Path
from datetime import datetime, timezone

sys.path.insert(0, str(Path(__file__).parent.parent))

import argparse
from slurm_monitor.storage.db import get_session
from slurm_monitor.storage.models import UserMapping
from sqlalchemy.exc import IntegrityError


def cmd_list(args):
    with get_session() as s:
        rows = s.query(UserMapping).order_by(UserMapping.display_name, UserMapping.username).all()
        if not rows:
            print("（暂无映射）")
            return
        print(f"{'账号':<16}{'显示名':<20}{'主账号':<12}{'备注':<16}{'版本':<6}{'更新时间'}")
        print("-" * 90)
        for r in rows:
            print(f"{r.username:<16}{r.display_name:<20}{r.canonical_user or '-':<12}"
                  f"{r.note or '':<16}{r.version:<6}"
                  f"{r.updated_at.strftime('%Y-%m-%d %H:%M') if r.updated_at else '-'}")


def cmd_set(args):
    """设置单条映射（存在则更新 display_name）。"""
    with get_session() as s:
        m = s.query(UserMapping).filter(UserMapping.username == args.username).first()
        if m:
            m.display_name = args.display_name
            m.note = args.note
            m.version += 1
            m.updated_at = datetime.now(timezone.utc)
            print(f"已更新：{args.username} → {args.display_name}")
        else:
            m = UserMapping(
                username=args.username,
                display_name=args.display_name,
                note=args.note,
                version=1,
                updated_at=datetime.now(timezone.utc),
                created_at=datetime.now(timezone.utc),
            )
            s.add(m)
            print(f"已创建：{args.username} → {args.display_name}")
        try:
            s.commit()
        except IntegrityError as e:
            s.rollback()
            print(f"❌ 失败：显示名 '{args.display_name}' 可能已被占用 ({e})")


def cmd_merge(args):
    """合并多个账号为同一显示名。

    --canonical 指定主账号；其余账号的 canonical_user 指向主账号。
    对外显示统一为 display_name。
    """
    usernames = args.usernames
    if args.canonical and args.canonical not in usernames:
        usernames.append(args.canonical)

    if len(usernames) < 1:
        print("❌ 至少需要一个账号")
        return

    canonical = args.canonical or usernames[0]
    display = args.display or canonical

    with get_session() as s:
        now = datetime.now(timezone.utc)
        for u in usernames:
            m = s.query(UserMapping).filter(UserMapping.username == u).first()
            is_canonical = (u == canonical)
            if m:
                m.display_name = display
                m.canonical_user = None if is_canonical else canonical
                m.version += 1
                m.updated_at = now
                action = "更新"
            else:
                m = UserMapping(
                    username=u,
                    display_name=display,
                    canonical_user=None if is_canonical else canonical,
                    version=1,
                    updated_at=now,
                    created_at=now,
                )
                s.add(m)
                action = "创建"
            print(f"  {action}：{u} → {display}"
                  f"{' (主账号)' if is_canonical else f' → canonical={canonical}'}")
        try:
            s.commit()
            print(f"✅ 合并完成：{', '.join(usernames)} 统一显示为 '{display}'，主账号 {canonical}")
        except IntegrityError as e:
            s.rollback()
            print(f"❌ 合并失败（显示名冲突或账号重复）：{e}")


def cmd_import(args):
    """从 CSV 批量导入。格式：username,display_name,note（note 可选）。"""
    import csv
    count = 0
    now = datetime.now(timezone.utc)
    with get_session() as s:
        with open(args.file, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                username = (row.get("username") or "").strip()
                display = (row.get("display_name") or "").strip()
                note = (row.get("note") or "").strip() or None
                if not username or not display:
                    continue
                m = s.query(UserMapping).filter(UserMapping.username == username).first()
                if m:
                    m.display_name = display
                    m.note = note
                    m.version += 1
                    m.updated_at = now
                else:
                    s.add(UserMapping(
                        username=username, display_name=display, note=note,
                        version=1, updated_at=now, created_at=now,
                    ))
                count += 1
        try:
            s.commit()
            print(f"✅ 导入完成：{count} 条映射")
        except IntegrityError as e:
            s.rollback()
            print(f"❌ 导入失败（可能存在显示名冲突）：{e}")


def cmd_delete(args):
    with get_session() as s:
        m = s.query(UserMapping).filter(UserMapping.username == args.username).first()
        if not m:
            print(f"❌ 账号 {args.username} 无映射记录")
            return
        s.delete(m)
        s.commit()
        print(f"✅ 已删除：{args.username}")


def main():
    parser = argparse.ArgumentParser(description="用户名映射管理工具")
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("list", help="查看所有映射").set_defaults(func=cmd_list)

    p_set = sub.add_parser("set", help="设置单条映射")
    p_set.add_argument("username")
    p_set.add_argument("display_name")
    p_set.add_argument("--note", default=None)
    p_set.set_defaults(func=cmd_set)

    p_merge = sub.add_parser("merge", help="合并多个账号为同一显示名")
    p_merge.add_argument("usernames", nargs="+", help="要合并的账号列表")
    p_merge.add_argument("--display", default=None, help="统一显示名（默认用 canonical）")
    p_merge.add_argument("--canonical", default=None, help="主账号（默认第一个）")
    p_merge.set_defaults(func=cmd_merge)

    p_imp = sub.add_parser("import", help="从 CSV 批量导入")
    p_imp.add_argument("file", help="CSV 文件路径")
    p_imp.set_defaults(func=cmd_import)

    p_del = sub.add_parser("delete", help="删除单条映射")
    p_del.add_argument("username")
    p_del.set_defaults(func=cmd_delete)

    args = parser.parse_args()
    if not args.cmd:
        parser.print_help()
        return
    args.func(args)


if __name__ == "__main__":
    main()
