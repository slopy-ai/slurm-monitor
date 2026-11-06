"""Redis 缓存客户端

用于缓存频繁查询的 API 数据，减少 PostgreSQL 压力。
TTL 策略：
  - overview: 30s（实时性要求较高）
  - queue: 15s（排队情况需要较实时）
  - node_detail: 30s
  - timeseries: 60s（时序数据变化较慢）
  - user_stats: 60s
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from typing import Any, Callable, Optional

import redis

from ..config import settings

logger = logging.getLogger(__name__)

# 默认 TTL（秒）
TTL_OVERVIEW = 30
TTL_QUEUE = 15
TTL_NODE_DETAIL = 30
TTL_TIMESERIES = 60
TTL_USER_STATS = 60

# 陈旧值保留倍数：fresh TTL 到期后旧值再保留 STALE_FACTOR 倍时间，用于回源失败/等待时兜底
STALE_FACTOR = 6
# 回源锁的持有时间（秒），避免持锁进程崩溃导致死锁
LOCK_TTL = 10
# 未抢到锁时等待新值的总时长与轮询间隔（秒）
WAIT_TIMEOUT = 2.0
WAIT_INTERVAL = 0.05


class CacheClient:
    """Redis 缓存客户端，连接失败时降级为直接查询 DB（不抛异常）。"""

    def __init__(self):
        self._client: Optional[redis.Redis] = None

    @property
    def client(self) -> Optional[redis.Redis]:
        if self._client is None:
            try:
                self._client = redis.Redis(
                    host=settings.redis_host,
                    port=settings.redis_port,
                    password=settings.redis_password or None,
                    db=settings.redis_db,
                    socket_connect_timeout=3,
                    socket_timeout=3,
                    decode_responses=True,
                )
                self._client.ping()
                logger.info("redis connected at %s:%d", settings.redis_host, settings.redis_port)
            except Exception as exc:
                logger.warning("redis unavailable, cache disabled: %s", exc)
                self._client = None
        return self._client

    def get(self, key: str) -> Optional[Any]:
        """获取缓存，返回反序列化的对象；缓存未命中或失败返回 None。"""
        try:
            c = self.client
            if c is None:
                return None
            raw = c.get(key)
            if raw is None:
                return None
            return json.loads(raw)
        except Exception as exc:
            logger.debug("cache get failed key=%s: %s", key, exc)
            self._client = None  # 重置连接，下次重试
            return None

    def set(self, key: str, value: Any, ttl: int = 30) -> None:
        """写入缓存，失败时静默降级。"""
        try:
            c = self.client
            if c is None:
                return
            c.setex(key, ttl, json.dumps(value, default=str))
        except Exception as exc:
            logger.debug("cache set failed key=%s: %s", key, exc)
            self._client = None

    def delete(self, key: str) -> None:
        try:
            c = self.client
            if c:
                c.delete(key)
                c.delete(self._stale_key(key))
        except Exception:
            self._client = None

    def invalidate_pattern(self, pattern: str) -> None:
        """删除匹配 pattern 的所有 key（小心使用，适合低频批量失效）。"""
        try:
            c = self.client
            if c is None:
                return
            keys = list(c.scan_iter(pattern, count=500))
            if keys:
                c.delete(*keys, *[self._stale_key(k) for k in keys])
        except Exception as exc:
            logger.debug("cache invalidate failed pattern=%s: %s", pattern, exc)
            self._client = None

    # ---------- 单飞 / 陈旧兜底 ----------

    @staticmethod
    def _stale_key(key: str) -> str:
        return f"{key}:stale"

    @staticmethod
    def _lock_key(key: str) -> str:
        return f"{key}:lock"

    def get_or_compute(self, key: str, ttl: int, compute: Callable[[], Any]) -> Any:
        """取缓存；未命中时只让一个调用方回源，其余等新值或读陈旧值。

        多个用户以不同刷新间隔打开页面时，重叠的请求会复用同一份结果，
        不会因为 TTL 恰好同时到期而一起打到数据库。
        """
        cached = self.get(key)
        if cached is not None:
            return cached

        client = self.client
        if client is None:
            return compute()  # 缓存不可用，直接回源

        token = self._acquire_lock(key, client)
        if token is None:
            # 别人正在回源：先等一小会儿拿新值
            deadline = time.monotonic() + WAIT_TIMEOUT
            while time.monotonic() < deadline:
                time.sleep(WAIT_INTERVAL)
                fresh = self.get(key)
                if fresh is not None:
                    return fresh
            # 等不到就用陈旧值兜底，仍然没有再自己回源
            stale = self.get(self._stale_key(key))
            if stale is not None:
                logger.debug("cache stale served key=%s", key)
                return stale
            return compute()

        try:
            value = compute()
            self.set(key, value, ttl)
            self._set_stale(key, value, ttl * STALE_FACTOR)
            return value
        finally:
            self._release_lock(key, token, client)

    def _acquire_lock(self, key: str, client: redis.Redis) -> Optional[str]:
        token = uuid.uuid4().hex
        try:
            ok = client.set(self._lock_key(key), token, nx=True, ex=LOCK_TTL)
            return token if ok else None
        except Exception as exc:
            logger.debug("cache lock failed key=%s: %s", key, exc)
            return None

    def _release_lock(self, key: str, token: str, client: redis.Redis) -> None:
        # 只删自己持有的锁，避免误删他人刚拿到的锁
        try:
            client.eval(
                "if redis.call('get', KEYS[1]) == ARGV[1] then "
                "return redis.call('del', KEYS[1]) else return 0 end",
                1, self._lock_key(key), token,
            )
        except Exception:
            pass

    def _set_stale(self, key: str, value: Any, ttl: int) -> None:
        try:
            c = self.client
            if c is not None:
                c.setex(self._stale_key(key), ttl, json.dumps(value, default=str))
        except Exception:
            pass


# 全局单例
cache = CacheClient()
