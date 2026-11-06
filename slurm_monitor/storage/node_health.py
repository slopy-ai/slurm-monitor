"""节点健康状态管理

用于记录算力节点（GPU采集）和登录机（sacct连接）的连接健康状态。
"""

from datetime import datetime, timezone
from typing import Optional, Literal
import logging

from sqlalchemy import Column, String, Integer, Text, DateTime, BigInteger, literal_column
from sqlalchemy.dialects.postgresql import insert

from .models import Base
from .db import get_session

logger = logging.getLogger(__name__)


class NodeHealthStatus(Base):
    """节点健康状态表"""

    __tablename__ = "node_health_status"

    id = Column(BigInteger, primary_key=True)
    node_name = Column(String(64), nullable=False, unique=True, index=True)
    node_type = Column(String(16), nullable=False)  # 'compute' / 'login'

    # 健康状态
    status = Column(String(16), nullable=False, default='unknown')  # 'ok' / 'degraded' / 'error' / 'unknown'

    # 失败统计
    consecutive_failures = Column(Integer, default=0)
    total_failures = Column(Integer, default=0)
    last_success_at = Column(DateTime)
    last_failure_at = Column(DateTime)
    last_error_msg = Column(Text)

    # 下次重试时间（算力节点有指数退避）
    next_retry_at = Column(DateTime)

    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


NodeType = Literal['compute', 'login']
HealthStatus = Literal['ok', 'degraded', 'error', 'unknown']


def update_node_health(
    node_name: str,
    node_type: NodeType,
    status: HealthStatus,
    consecutive_failures: int = 0,
    last_success_at: Optional[datetime] = None,
    last_failure_at: Optional[datetime] = None,
    last_error_msg: Optional[str] = None,
    next_retry_at: Optional[datetime] = None,
) -> None:
    """更新节点健康状态（upsert）

    Args:
        node_name: 节点名（算力节点为节点名，登录机统一用 'slurm-login'）
        node_type: 'compute' 或 'login'
        status: 'ok' / 'degraded' / 'error' / 'unknown'
        consecutive_failures: 连续失败次数
        last_success_at: 最后成功时间
        last_failure_at: 最后失败时间
        last_error_msg: 错误信息（截断到500字符）
        next_retry_at: 下次重试时间（仅算力节点有退避）
    """
    with get_session() as session:
        now = datetime.now(timezone.utc)

        # 构造更新数据
        data = {
            "node_name": node_name,
            "node_type": node_type,
            "status": status,
            "consecutive_failures": consecutive_failures,
            "updated_at": now,
        }

        if last_success_at is not None:
            data["last_success_at"] = last_success_at
        if last_failure_at is not None:
            data["last_failure_at"] = last_failure_at
        if last_error_msg is not None:
            data["last_error_msg"] = last_error_msg[:500]
        if next_retry_at is not None:
            data["next_retry_at"] = next_retry_at

        # upsert
        stmt = insert(NodeHealthStatus).values(**data)

        # 冲突时更新（除了 node_name 和 node_type）
        update_dict = {
            "status": stmt.excluded.status,
            "consecutive_failures": stmt.excluded.consecutive_failures,
            "updated_at": stmt.excluded.updated_at,
        }

        if last_success_at is not None:
            update_dict["last_success_at"] = stmt.excluded.last_success_at
        if last_failure_at is not None:
            update_dict["last_failure_at"] = stmt.excluded.last_failure_at
            # 失败时累加 total_failures（使用 PostgreSQL 原子操作避免竞态）
            update_dict["total_failures"] = literal_column("node_health_status.total_failures + 1")
        if last_error_msg is not None:
            update_dict["last_error_msg"] = stmt.excluded.last_error_msg
        if next_retry_at is not None:
            update_dict["next_retry_at"] = stmt.excluded.next_retry_at

        stmt = stmt.on_conflict_do_update(
            index_elements=["node_name"],
            set_=update_dict
        )

        session.execute(stmt)
        session.commit()


def get_cluster_health() -> dict:
    """获取集群整体健康状态

    Returns:
        {
            "login_node": {...} or None,
            "compute_nodes": [...]
        }
    """
    with get_session() as session:
        rows = session.query(NodeHealthStatus).all()

        login_node = None
        compute_nodes = []

        for r in rows:
            node_data = {
                "name": r.node_name,
                "status": r.status,
                "consecutive_failures": r.consecutive_failures,
                "total_failures": r.total_failures,
                "last_success_at": r.last_success_at,
                "last_failure_at": r.last_failure_at,
                "last_error_msg": r.last_error_msg,
                "next_retry_at": r.next_retry_at,
            }

            if r.node_type == 'login':
                login_node = node_data
            elif r.node_type == 'compute':
                compute_nodes.append(node_data)

        return {
            "login_node": login_node,
            "compute_nodes": compute_nodes,
        }
