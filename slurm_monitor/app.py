"""FastAPI 应用入口"""

import logging
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from .api.routes import router as router_v1
from .api.routes_new import router as router_new
from .api.routes_user_mapping import router as router_user_mapping
from .api.routes_project_groups import router as router_project_groups
from .api.routes_nodes import router as router_nodes
from .config import settings

logging.basicConfig(
    level=getattr(logging, settings.log_level.upper(), logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)

app = FastAPI(
    title="Slurm 集群监控",
    description="基于 slurmrestd API 和 sacct 的 GPU 集群监控系统",
    version="0.2.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

# 注册旧版 API（保持兼容）
app.include_router(router_v1)

# 注册新版 API（符合前端接口文档规范）
app.include_router(router_new)

# 注册用户名映射 API
app.include_router(router_user_mapping)

# 注册项目组配置 API
app.include_router(router_project_groups)

# 注册算力节点映射 API
app.include_router(router_nodes)

# 挂载静态文件
_static = Path(__file__).parent / "web" / "static"
if _static.exists():
    app.mount("/static", StaticFiles(directory=str(_static)), name="static")


@app.on_event("startup")
def startup_event():
    """应用启动事件"""
    logger.info("启动缓存预热服务...")
    from .api.cache_warmer import get_warmer, start_cache_warmer
    from .api import routes_new

    warmer = get_warmer()

    # 注册高频接口预热任务（无参数，调用内部会处理缓存逻辑）
    def warmup_cluster_summary():
        try:
            routes_new.cluster_summary()
        except Exception as e:
            logger.debug(f"预热 cluster_summary 失败: {e}")

    def warmup_nodes_list():
        try:
            # 预热默认参数的节点列表（最常用）
            routes_new.nodes_list(partition=None, state=None, sort="gpu_util", trend_range="1h")
        except Exception as e:
            logger.debug(f"预热 nodes_list 失败: {e}")

    def warmup_top_users():
        try:
            routes_new.cluster_top_users(range="7d", metric="gpu_hours", limit=10)
        except Exception as e:
            logger.debug(f"预热 cluster_top_users 失败: {e}")

    warmer.register_task("cluster_summary", warmup_cluster_summary)
    warmer.register_task("nodes_list", warmup_nodes_list)
    warmer.register_task("cluster_top_users", warmup_top_users)

    start_cache_warmer()
    logger.info("缓存预热服务已启动")


@app.on_event("shutdown")
def shutdown_event():
    """应用关闭事件"""
    logger.info("停止缓存预热服务...")
    from .api.cache_warmer import stop_cache_warmer
    stop_cache_warmer()
    logger.info("缓存预热服务已停止")


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(str(_static / "index.html"))
