"""Web 服务启动入口"""

import uvicorn
from slurm_monitor.config import settings

if __name__ == "__main__":
    uvicorn.run(
        "slurm_monitor.app:app",
        host=settings.web_host,
        port=settings.web_port,
        reload=settings.web_debug,
    )
