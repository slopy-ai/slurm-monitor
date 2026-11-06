"""初始化数据库与首次回填

用法：
    python scripts/init_db.py            # 仅建表
    python scripts/init_db.py --backfill # 建表并从 sacct 回填历史（首次部署）
"""

import argparse
import logging
import sys
from datetime import datetime, timezone

from slurm_monitor.config import settings
from slurm_monitor.storage.db import init_db, get_session
from slurm_monitor.storage.models import Job
from slurm_monitor.collector.collector import SlurmCollector

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="初始化 Slurm 监控数据库")
    parser.add_argument("--backfill", action="store_true", help="建表后从 sacct 回填历史作业")
    args = parser.parse_args()

    logger.info("initializing database at %s", settings.postgres_host)
    init_db()
    logger.info("database tables ready")

    if args.backfill:
        logger.info("starting history backfill via sacct")
        collector = SlurmCollector()
        # 首次回溯 30 天
        collector.sync_history(lookback_seconds=0)
        logger.info("backfill complete")

    logger.info("done")


if __name__ == "__main__":
    sys.exit(main())
