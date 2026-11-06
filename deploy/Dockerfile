FROM python:3.13-slim

WORKDIR /app

# 安装系统依赖：psycopg2 需要 libpq，entrypoint 需要 psql/pg_isready，collector 需要 openssh-client
RUN apt-get update && apt-get install -y --no-install-recommends \
    libpq-dev \
    postgresql-client \
    openssh-client \
    && rm -rf /var/lib/apt/lists/*

# 先复制依赖文件，利用 Docker layer 缓存
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制应用代码
COPY slurm_monitor/ ./slurm_monitor/
COPY scripts/ ./scripts/

# 确保 /app 在 Python 路径中，slurm_monitor 包可以被导入
ENV PYTHONPATH=/app

# 入口脚本负责建表检测和启动
COPY deploy/scripts/entrypoint.sh /entrypoint.sh
COPY deploy/scripts/entrypoint-aggregator.sh /entrypoint-aggregator.sh
RUN chmod +x /entrypoint.sh /entrypoint-aggregator.sh

ENTRYPOINT ["/entrypoint.sh"]
