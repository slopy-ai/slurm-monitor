"""项目组 API + 管理员认证（带限速）

功能：
- GET  /api/project-groups            获取项目组列表
- POST /api/admin/login               管理员登录（内部限速：每 IP 每天 3 次失败）
- POST /api/admin/project-groups      创建项目组
- PUT  /api/admin/project-groups/{id} 修改项目组名称（id 不可改）

认证：简单 token 机制，登录成功返回 session token（进程内 dict，重启失效）。
限速：同一 IP 每天失败 3 次后锁定，重启解除（无需持久化，内部简单管控）。
"""

from __future__ import annotations

import logging
import secrets
from collections import defaultdict
from datetime import datetime, timezone, date
from typing import Optional

from fastapi import APIRouter, Request, Header
from pydantic import BaseModel, field_validator
from sqlalchemy.exc import IntegrityError

from ..storage.db import get_session
from ..storage.models import ProjectGroup
from ..config import settings

logger = logging.getLogger(__name__)

router = APIRouter(tags=["project-groups"])

# ---- 进程内 session store（重启失效）----
_sessions: dict[str, datetime] = {}   # token -> created_at
SESSION_TTL_HOURS = 8

# ---- 暴力破解限速（进程内）----
# { ip: { date_str: fail_count } }
_fail_log: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
MAX_DAILY_FAILURES = 3


def _ok(data) -> dict:
    return {"code": 0, "data": data}


def _err(code: int, msg: str) -> dict:
    return {"code": code, "message": msg}


def _client_ip(request: Request) -> str:
    fwd = request.headers.get("x-forwarded-for")
    if fwd:
        return fwd.split(",")[0].strip()
    return request.client.host if request.client else "unknown"


def _check_session(token: Optional[str]) -> bool:
    if not token:
        return False
    created = _sessions.get(token)
    if not created:
        return False
    age_h = (datetime.now(timezone.utc) - created).total_seconds() / 3600
    if age_h > SESSION_TTL_HOURS:
        _sessions.pop(token, None)
        return False
    return True


# ---- 公开接口 ----

@router.get("/api/project-groups")
def list_groups():
    """获取项目组列表（公开）"""
    with get_session() as s:
        rows = s.query(ProjectGroup).order_by(ProjectGroup.id).all()
        return _ok([{"id": r.id, "name": r.name, "created_at": r.created_at.strftime("%Y-%m-%d") if r.created_at else None} for r in rows])


# ---- 管理员认证 ----

class LoginBody(BaseModel):
    username: str
    password: str


@router.post("/api/admin/login")
def admin_login(body: LoginBody, request: Request):
    """管理员登录。同一 IP 每天最多失败 3 次。"""
    ip = _client_ip(request)
    today = date.today().isoformat()
    fail_count = _fail_log[ip][today]

    if fail_count >= MAX_DAILY_FAILURES:
        return _err(42901, f"该 IP 今日失败次数过多，请明天再试或联系管理员")

    if body.username == settings.admin_user and body.password == settings.admin_pass:
        token = secrets.token_urlsafe(32)
        _sessions[token] = datetime.now(timezone.utc)
        logger.info("admin login ok ip=%s", ip)
        return _ok({"token": token, "ttl_hours": SESSION_TTL_HOURS})

    _fail_log[ip][today] += 1
    remaining = MAX_DAILY_FAILURES - _fail_log[ip][today]
    logger.warning("admin login failed ip=%s attempts=%d", ip, _fail_log[ip][today])
    return _err(40101, f"用户名或密码错误，今日还剩 {remaining} 次机会")


@router.get("/api/admin/check")
def admin_check(x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token")):
    """检查 token 是否有效"""
    if _check_session(x_admin_token):
        return _ok({"valid": True})
    return _err(40101, "未登录或 token 已过期")


# ---- 管理员操作：项目组 ----

class GroupCreate(BaseModel):
    name: str

    @field_validator("name")
    @classmethod
    def no_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("名称不能为空")
        return v


class GroupUpdate(BaseModel):
    name: str

    @field_validator("name")
    @classmethod
    def no_empty(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("名称不能为空")
        return v


@router.post("/api/admin/project-groups")
def create_group(
    body: GroupCreate,
    x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token"),
):
    """创建项目组（管理员）。id 自增，成功后永久冻结。"""
    if not _check_session(x_admin_token):
        return _err(40101, "未登录或 token 已过期，请重新登录")

    with get_session() as s:
        g = ProjectGroup(name=body.name, created_at=datetime.now(timezone.utc))
        s.add(g)
        try:
            s.commit()
            s.refresh(g)
            return _ok({"id": g.id, "name": g.name})
        except IntegrityError:
            s.rollback()
            return _err(40902, f"项目组名称 '{body.name}' 已存在")


@router.put("/api/admin/project-groups/{group_id}")
def update_group(
    group_id: int,
    body: GroupUpdate,
    x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token"),
):
    """修改项目组名称（id 永久冻结，只能改名称）。"""
    if not _check_session(x_admin_token):
        return _err(40101, "未登录或 token 已过期，请重新登录")

    with get_session() as s:
        g = s.query(ProjectGroup).filter(ProjectGroup.id == group_id).first()
        if not g:
            return _err(40401, f"项目组 ID {group_id} 不存在")
        g.name = body.name
        g.updated_at = datetime.now(timezone.utc)
        try:
            s.commit()
            s.refresh(g)
            return _ok({"id": g.id, "name": g.name})
        except IntegrityError:
            s.rollback()
            return _err(40902, f"项目组名称 '{body.name}' 已被占用")
