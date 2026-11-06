"""缓存预热模块

后台线程定期预热高频接口缓存，避免缓存击穿和缓存雪崩。
多用户不同刷新时间时，可以复用同一份缓存。
"""

import logging
import threading
import time
from typing import Callable, List, Tuple

logger = logging.getLogger(__name__)


class CacheWarmer:
    """缓存预热器"""

    def __init__(self, interval: int = 30):
        """
        Args:
            interval: 预热间隔（秒），默认 30 秒
        """
        self.interval = interval
        self.running = False
        self.thread = None
        self._warmup_tasks: List[Tuple[str, Callable]] = []

    def register_task(self, name: str, func: Callable) -> None:
        """注册一个预热任务

        Args:
            name: 任务名称（用于日志）
            func: 预热函数（无参数，捕获自己的异常）
        """
        self._warmup_tasks.append((name, func))

    def _warmup_loop(self) -> None:
        """预热循环"""
        logger.info(f"缓存预热线程启动，间隔 {self.interval} 秒")
        while self.running:
            try:
                for task_name, task_func in self._warmup_tasks:
                    try:
                        task_func()
                        logger.debug(f"缓存预热成功: {task_name}")
                    except Exception as e:
                        logger.error(f"缓存预热失败 {task_name}: {e}", exc_info=True)
            except Exception as e:
                logger.error(f"缓存预热循环异常: {e}", exc_info=True)

            # 等待下一个周期
            time.sleep(self.interval)

        logger.info("缓存预热线程停止")

    def start(self) -> None:
        """启动预热线程"""
        if self.running:
            logger.warning("缓存预热线程已在运行")
            return

        self.running = True
        self.thread = threading.Thread(target=self._warmup_loop, daemon=True, name="CacheWarmer")
        self.thread.start()
        logger.info("缓存预热线程已启动")

    def stop(self) -> None:
        """停止预热线程"""
        if not self.running:
            return

        self.running = False
        if self.thread:
            self.thread.join(timeout=5)
        logger.info("缓存预热线程已停止")


# 全局单例
_warmer = CacheWarmer(interval=30)


def get_warmer() -> CacheWarmer:
    """获取全局缓存预热器"""
    return _warmer


def start_cache_warmer() -> None:
    """启动缓存预热（在 app.py 启动事件中调用）"""
    _warmer.start()


def stop_cache_warmer() -> None:
    """停止缓存预热（在 app.py 关闭事件中调用）"""
    _warmer.stop()
