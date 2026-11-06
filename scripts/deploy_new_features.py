#!/usr/bin/env python3
"""添加用户排行榜和节点健康状态功能的部署脚本

执行步骤：
1. 创建 node_health_status 表
2. 重新构建并重启相关容器
3. 验证功能
"""

import subprocess
import sys
import time

def run_cmd(cmd, description):
    """执行命令并打印输出"""
    print(f"\n{'='*60}")
    print(f"执行: {description}")
    print(f"命令: {cmd}")
    print(f"{'='*60}\n")
    result = subprocess.run(cmd, shell=True, capture_output=False, text=True)
    if result.returncode != 0:
        print(f"❌ 失败: {description}")
        return False
    print(f"✓ 成功: {description}")
    return True

def main():
    print("""
╔════════════════════════════════════════════════════════════╗
║  Slurm Monitor - 用户排行榜 & 节点健康状态 部署脚本        ║
╚════════════════════════════════════════════════════════════╝
    """)

    # 步骤1: 创建健康状态表
    if not run_cmd(
        "docker exec slurm-monitor-web python3 /app/scripts/init_health_table.py",
        "创建 node_health_status 表"
    ):
        print("\n⚠️ 如果表已存在，可以忽略此错误")

    # 步骤2: 重新构建镜像（包含新代码）
    if not run_cmd(
        "cd /home/jason/code/slurm-monitor && docker-compose -f deploy/docker-compose.yml build slurm-monitor-web slurm-monitor-collector",
        "重新构建 Web 和 Collector 镜像"
    ):
        sys.exit(1)

    # 步骤3: 重启容器
    if not run_cmd(
        "docker restart slurm-monitor-web slurm-monitor-collector",
        "重启 Web 和 Collector 容器"
    ):
        sys.exit(1)

    # 等待容器启动
    print("\n等待容器启动...")
    time.sleep(5)

    # 步骤4: 验证 API
    print("\n" + "="*60)
    print("验证新增 API 接口")
    print("="*60 + "\n")

    run_cmd(
        "curl -s http://192.168.3.102:8000/api/cluster/health | python3 -c 'import sys,json; d=json.load(sys.stdin); print(f\"✓ 健康状态 API: {d[\\\"code\\\"]==0}\")'",
        "测试健康状态 API"
    )

    run_cmd(
        "curl -s http://192.168.3.102:8000/api/cluster/top-users | python3 -c 'import sys,json; d=json.load(sys.stdin); print(f\"✓ 用户排行榜 API: {d[\\\"code\\\"]==0}，返回 {len(d[\\\"data\\\"][\\\"top_users\\\"])} 个用户\")'",
        "测试用户排行榜 API"
    )

    print("""
╔════════════════════════════════════════════════════════════╗
║  部署完成！                                                 ║
╚════════════════════════════════════════════════════════════╝

✅ 新功能已上线：

1. 节点健康状态监控
   - 总览页左上角显示登录机连接状态
   - 节点列表中每个节点名前显示状态灯（绿/黄/红）
   - 鼠标悬浮可查看错误详情

2. 用户排行榜
   - 位置：总览页 → 集群资源趋势 和 节点列表 之间
   - 支持切换时间范围：1天/7天/30天/全部
   - 支持切换排序指标：GPU卡时/作业数/GPU利用率
   - 显示前10名用户及其统计数据
   - 点击用户名跳转到个人统计页

访问地址: http://192.168.3.102:8000/

提示：
- 健康状态数据在采集器运行后开始累积
- 用户排行榜基于已结束的作业统计
- 如果数据较少，可能显示"暂无数据"
    """)

if __name__ == "__main__":
    main()
