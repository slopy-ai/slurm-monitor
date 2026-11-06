#!/usr/bin/env python3
"""聚合器定时任务

定时运行各种聚合任务：
- 集群级数据聚合（每 5 分钟）
- 作业历史同步 sacct → job_history（每 5 分钟，回溯 3 小时）
- 排队作业快照（每 5 分钟）
- 作业效率分析（每小时）
- 数据清理（每天）

用法:
    python -m slurm_monitor.aggregator.scheduler
"""

import logging
import time
from datetime import datetime
from threading import Thread

from ..aggregator import ClusterAggregator, JobEfficiencyAnalyzer, PendingJobSnapshotCollector
from ..config import settings
from ..sync.job_history_sync import JobHistorySyncer

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


class AggregatorScheduler:
    """聚合器调度器"""

    def __init__(self):
        self.cluster_agg = ClusterAggregator()
        self.job_analyzer = JobEfficiencyAnalyzer()
        self.pending_collector = PendingJobSnapshotCollector()
        self.history_syncer = JobHistorySyncer()

        self.last_cluster_agg = 0
        self.last_pending_snapshot = 0
        self.last_history_sync = 0
        self.last_job_analysis = 0
        self.last_cleanup = 0

    def run_forever(self):
        """持续运行调度循环"""
        logger.info("聚合器调度器已启动")

        while True:
            try:
                now = time.time()

                # 每 5 分钟：集群聚合
                if now - self.last_cluster_agg >= 300:
                    self._run_task("集群聚合", self.cluster_agg.aggregate_all_granularities)
                    self.last_cluster_agg = now

                # 每 5 分钟：作业历史增量同步（先于效率分析，保证分析用到最新结束的作业）
                if now - self.last_history_sync >= 300:
                    self._run_task("作业历史同步", lambda: self.history_syncer.sync_recent_jobs(hours=3))
                    self.last_history_sync = now

                # 每 5 分钟：排队作业快照
                if now - self.last_pending_snapshot >= 300:
                    self._run_task("排队作业快照", self.pending_collector.collect_snapshot)
                    self.last_pending_snapshot = now

                # 每小时：作业效率分析
                if now - self.last_job_analysis >= 3600:
                    self._run_task("作业效率分析", lambda: self.job_analyzer.analyze_recent_jobs(hours=2))
                    self.last_job_analysis = now

                # 每天：数据清理
                if now - self.last_cleanup >= 86400:
                    self._run_cleanup()
                    self.last_cleanup = now

                # 休眠 30 秒
                time.sleep(30)

            except KeyboardInterrupt:
                logger.info("收到停止信号")
                break
            except Exception as e:
                logger.error(f"调度循环异常: {e}", exc_info=True)
                time.sleep(60)

    def _run_task(self, name: str, func):
        """运行任务"""
        try:
            logger.info(f"开始任务: {name}")
            func()
            logger.info(f"完成任务: {name}")
        except Exception as e:
            logger.error(f"任务失败 {name}: {e}", exc_info=True)

    def _run_cleanup(self):
        """运行清理任务"""
        try:
            logger.info("开始数据清理")

            # 清理排队快照（保留 30 天）
            self.pending_collector.cleanup_old_snapshots(days=30)

            # 清理集群聚合数据（各粒度按配置保留）
            for granularity in ["5m", "15m", "1h", "1d"]:
                self.cluster_agg.cleanup_old_data(granularity)

            logger.info("完成数据清理")
        except Exception as e:
            logger.error(f"数据清理失败: {e}", exc_info=True)


def main():
    """主入口"""
    scheduler = AggregatorScheduler()
    scheduler.run_forever()


if __name__ == "__main__":
    main()
