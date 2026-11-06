"""用户名映射 API

功能：
- GET  /api/user-mapping        列出所有映射
- POST /api/user-mapping        新建或更新映射（乐观锁）
- PUT  /api/user-mapping/{username}  更新单条（乐观锁）
- DELETE /api/user-mapping/{username} 删除映射（管理员）

并发保护：乐观锁，客户端提交时须携带当前 version；
版本不匹配则返回 409，前端刷新后重试。

display_name 全局唯一约束由数据库唯一索引保证。
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Query, Header, HTTPException
from pydantic import BaseModel, field_validator
from sqlalchemy.exc import IntegrityError

from ..storage.db import get_session
from ..storage.models import UserMapping

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/user-mapping", tags=["user-mapping"])


# ---- Pydantic schemas ----

class MappingCreate(BaseModel):
    username: str
    display_name: Optional[str] = None
    groups: Optional[str] = None   # 逗号分隔的项目组 ID，如 "1,3"
    note: Optional[str] = None

    @field_validator("username")
    @classmethod
    def no_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("不能为空")
        return v


class MappingUpdate(BaseModel):
    display_name: Optional[str] = None
    groups: Optional[str] = None   # 逗号分隔的项目组 ID
    note: Optional[str] = None
    version: int  # 乐观锁：必须与当前数据库版本一致


def _row_to_dict(m: UserMapping) -> dict:
    return {
        "username": m.username,
        "display_name": m.display_name,
        "canonical_user": m.canonical_user,
        "note": m.note,
        "version": m.version,
        "updated_by": m.updated_by,
        "updated_at": m.updated_at.strftime("%Y-%m-%d %H:%M:%S") if m.updated_at else None,
    }


def _ok(data) -> dict:
    return {"code": 0, "data": data}


def _err(code: int, msg: str) -> dict:
    return {"code": code, "message": msg}


# ---- 路由 ----

@router.get("")
def list_mappings():
    """获取所有用户映射"""
    with get_session() as s:
        rows = s.query(UserMapping).order_by(UserMapping.username).all()
        return _ok([_row_to_dict(r) for r in rows])


@router.post("")
def create_mapping(body: MappingCreate, x_user: Optional[str] = Header(None, alias="X-Username")):
    """新建映射。display_name 须全局唯一。"""
    with get_session() as s:
        existing = s.query(UserMapping).filter(UserMapping.username == body.username).first()
        if existing:
            return _err(40901, f"用户 {body.username} 已有映射，请使用 PUT 更新")

        m = UserMapping(
            username=body.username,
            display_name=body.display_name,
            note=body.note,
            version=1,
            updated_by=x_user,
            updated_at=datetime.now(timezone.utc),
            created_at=datetime.now(timezone.utc),
        )
        s.add(m)
        try:
            s.commit()
            s.refresh(m)
            return _ok(_row_to_dict(m))
        except IntegrityError as e:
            s.rollback()
            if "uq_user_mapping_display" in str(e.orig):
                return _err(40902, f"显示名 '{body.display_name}' 已被其他账号使用，请换一个")
            return _err(50001, f"写入失败: {e}")


@router.put("/{username}")
def update_mapping(
    username: str,
    body: MappingUpdate,
    x_user: Optional[str] = Header(None, alias="X-Username"),
):
    """更新映射（乐观锁）。version 必须与当前值一致，否则返回 409。"""
    with get_session() as s:
        m = s.query(UserMapping).filter(UserMapping.username == username).first()
        if not m:
            return _err(40401, f"用户 {username} 没有映射记录，请先 POST 创建")

        if m.version != body.version:
            return _err(40901, f"数据已被其他人修改（当前版本 {m.version}，你的版本 {body.version}），请刷新后重试")

        m.display_name = body.display_name
        m.note = body.note
        m.version = m.version + 1
        m.updated_by = x_user
        m.updated_at = datetime.now(timezone.utc)

        try:
            s.commit()
            s.refresh(m)
            return _ok(_row_to_dict(m))
        except IntegrityError as e:
            s.rollback()
            if "uq_user_mapping_display" in str(e.orig):
                return _err(40902, f"显示名 '{body.display_name}' 已被其他账号使用，请换一个")
            return _err(50001, f"更新失败: {e}")


@router.delete("/{username}")
def delete_mapping(username: str, x_user: Optional[str] = Header(None, alias="X-Username")):
    """删除映射"""
    with get_session() as s:
        m = s.query(UserMapping).filter(UserMapping.username == username).first()
        if not m:
            return _err(40401, f"用户 {username} 没有映射记录")
        s.delete(m)
        s.commit()
        return _ok({"deleted": username})


@router.get("/resolve/{username}")
def resolve_display(username: str):
    """将 Slurm 账号解析为显示名；无映射时原样返回"""
    with get_session() as s:
        m = s.query(UserMapping).filter(UserMapping.username == username).first()
        if m:
            canonical = m.canonical_user or username
            # 找 canonical 对应的显示名
            if m.canonical_user:
                cm = s.query(UserMapping).filter(UserMapping.username == m.canonical_user).first()
                display = cm.display_name if cm else m.display_name
            else:
                display = m.display_name
            return _ok({"username": username, "display_name": display, "canonical": canonical})
        return _ok({"username": username, "display_name": username, "canonical": username})
