"""数据采集器启动入口"""

import logging
import sys

from slurm_monitor.collector.collector import SlurmCollector
from slurm_monitor.config import settings

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

if __name__ == "__main__":
    collector = SlurmCollector()
    collector.run()
