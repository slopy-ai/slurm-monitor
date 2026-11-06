"""算力节点映射 API（管理员）

节点名 → IP/端口/SSH 账号。collector 每个采集周期都会从 node_ip_map 读取，
所以这里改完无需重启容器，下一轮（≤30s）自动生效。

- GET    /api/admin/nodes          节点列表 + Slurm 已知节点名 + 默认账号信息
- POST   /api/admin/nodes          新增节点
- PUT    /api/admin/nodes/{id}     修改节点（ssh_password 不传=保持不变，传空串=清除并回退默认）
- DELETE /api/admin/nodes/{id}     删除节点
- POST   /api/admin/nodes/test     测试连接并采集一帧 GPU 数据

密码只写不读：任何响应都不会包含 ssh_password，只返回 has_password。
"""

from __future__ import annotations

import ipaddress
import logging
import re
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Header
from pydantic import BaseModel, field_validator
from sqlalchemy.exc import IntegrityError

from ..collector.gpu_collector import fetch_gpu_frame
from ..config import settings
from ..storage.db import get_session
from ..storage.models import NodeIpMap, NodeSnapshot
from .routes_project_groups import _check_session, _err, _ok

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/admin/nodes", tags=["nodes"])

_HOSTNAME_RE = re.compile(r"^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$")
_NODE_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
_UNAUTH = (40101, "未登录或 token 已过期，请重新登录")


def _valid_host(v: str) -> bool:
    try:
        ipaddress.ip_address(v)
        return True
    except ValueError:
        return bool(_HOSTNAME_RE.match(v))


class _NodeFields(BaseModel):
    ip: str
    port: int = 22
    ssh_user: Optional[str] = None
    ssh_password: Optional[str] = None
    enabled: bool = True
    remark: Optional[str] = None

    @field_validator("ip")
    @classmethod
    def check_ip(cls, v: str) -> str:
        v = v.strip()
        if not _valid_host(v):
            raise ValueError("IP/主机名格式不正确")
        return v

    @field_validator("port")
    @classmethod
    def check_port(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError("端口范围 1-65535")
        return v


class NodeCreate(_NodeFields):
    node_name: str

    @field_validator("node_name")
    @classmethod
    def check_name(cls, v: str) -> str:
        v = v.strip()
        if not _NODE_NAME_RE.match(v):
            raise ValueError("节点名只能包含字母、数字、. _ -")
        return v


class NodeTest(BaseModel):
    ip: str
    port: int = 22
    ssh_user: Optional[str] = None
    ssh_password: Optional[str] = None
    node_id: Optional[int] = None  # 编辑时未重新输入密码，则用该节点已保存的密码

    @field_validator("ip")
    @classmethod
    def check_ip(cls, v: str) -> str:
        v = v.strip()
        if not _valid_host(v):
            raise ValueError("IP/主机名格式不正确")
        return v


def _dump(n: NodeIpMap) -> dict:
    return {
        "id": n.id,
        "node_name": n.node_name,
        "ip": n.ip,
        "port": n.port,
        "ssh_user": n.ssh_user,
        "has_password": bool(n.ssh_password),
        "enabled": bool(n.enabled),
        "remark": n.remark,
        "updated_at": n.updated_at.strftime("%Y-%m-%d %H:%M:%S") if n.updated_at else None,
    }


@router.get("")
def list_nodes(x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token")):
    if not _check_session(x_admin_token):
        return _err(*_UNAUTH)
    with get_session() as s:
        rows = s.query(NodeIpMap).order_by(NodeIpMap.node_name).all()
        mapped = {r.node_name for r in rows}
        slurm_nodes = [
            r[0] for r in s.query(NodeSnapshot.node_name).distinct().order_by(NodeSnapshot.node_name).all()
        ]
        return _ok({
            "nodes": [_dump(r) for r in rows],
            "unmapped_slurm_nodes": [n for n in slurm_nodes if n not in mapped],
            "defaults": {
                "ssh_user": settings.node_ssh_user,
                "has_password": bool(settings.node_ssh_password),
            },
        })


@router.post("")
def create_node(body: NodeCreate, x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token")):
    if not _check_session(x_admin_token):
        return _err(*_UNAUTH)
    with get_session() as s:
        n = NodeIpMap(
            node_name=body.node_name,
            ip=body.ip,
            port=body.port,
            ssh_user=(body.ssh_user or "").strip() or settings.node_ssh_user,
            ssh_password=body.ssh_password or None,
            enabled=1 if body.enabled else 0,
            remark=(body.remark or "").strip() or None,
        )
        s.add(n)
        try:
            s.commit()
        except IntegrityError:
            s.rollback()
            return _err(40902, f"节点 '{body.node_name}' 已存在")
        s.refresh(n)
        logger.info("node_ip_map created: %s -> %s:%s", n.node_name, n.ip, n.port)
        return _ok(_dump(n))


@router.put("/{node_id}")
def update_node(node_id: int, body: _NodeFields, x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token")):
    if not _check_session(x_admin_token):
        return _err(*_UNAUTH)
    with get_session() as s:
        n = s.query(NodeIpMap).filter(NodeIpMap.id == node_id).first()
        if not n:
            return _err(40401, f"节点 ID {node_id} 不存在")
        n.ip = body.ip
        n.port = body.port
        n.ssh_user = (body.ssh_user or "").strip() or settings.node_ssh_user
        if "ssh_password" in body.model_fields_set:
            n.ssh_password = body.ssh_password or None  # 空串=清除，回退到 .env 默认密码
        n.enabled = 1 if body.enabled else 0
        n.remark = (body.remark or "").strip() or None
        n.updated_at = datetime.now(timezone.utc)
        s.commit()
        s.refresh(n)
        logger.info("node_ip_map updated: %s -> %s:%s", n.node_name, n.ip, n.port)
        return _ok(_dump(n))


@router.delete("/{node_id}")
def delete_node(node_id: int, x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token")):
    if not _check_session(x_admin_token):
        return _err(*_UNAUTH)
    with get_session() as s:
        n = s.query(NodeIpMap).filter(NodeIpMap.id == node_id).first()
        if not n:
            return _err(40401, f"节点 ID {node_id} 不存在")
        name = n.node_name
        s.delete(n)
        s.commit()
        logger.info("node_ip_map deleted: %s", name)
        return _ok({"id": node_id, "node_name": name})


@router.post("/test")
def test_node(body: NodeTest, x_admin_token: Optional[str] = Header(None, alias="X-Admin-Token")):
    """用表单里的参数连一次节点并采集一帧，返回 GPU 数量或失败原因（不写库）。"""
    if not _check_session(x_admin_token):
        return _err(*_UNAUTH)

    password = body.ssh_password or None
    if password is None and body.node_id is not None:
        with get_session() as s:
            saved = s.query(NodeIpMap.ssh_password).filter(NodeIpMap.id == body.node_id).scalar()
        password = saved or None
    user = (body.ssh_user or "").strip() or settings.node_ssh_user
    password = password or settings.node_ssh_password

    try:
        frame = fetch_gpu_frame(body.ip, body.port, user, password)
        gpus = frame["gpus"]
    except Exception as exc:
        return _err(50201, f"连接或采集失败: {exc}")
    return _ok({
        "gpu_count": len(gpus),
        "gpu_models": sorted({g.get("name") for g in gpus if g.get("name")}),
    })
