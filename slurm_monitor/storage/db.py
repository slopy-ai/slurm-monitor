"""数据库连接与会话管理"""

from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session

from ..config import settings
from .models import Base

engine = create_engine(
    settings.postgres_dsn,
    pool_pre_ping=True,
    pool_size=20,        # 扩大连接池以支持高并发（原 5）
    max_overflow=30,     # 峰值最多 50 个连接（原 10）
    pool_recycle=3600,   # 1 小时回收连接，避免 PostgreSQL idle timeout
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)


def init_db() -> None:
    """建表（幂等）。"""
    Base.metadata.create_all(bind=engine)


@contextmanager
def get_session() -> Session:
    session = SessionLocal()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()
