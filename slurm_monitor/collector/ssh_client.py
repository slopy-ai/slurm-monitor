"""SSH 命令执行客户端

封装通过 SSH 在 Slurm 登录节点执行 sacct/squeue/sinfo 的逻辑。
作为 REST API 的备用数据源：API 无用户信息时用 sacct 补全，修复后可直接去掉。
"""

import logging
import shlex
import threading
from datetime import datetime, timezone
from typing import Optional

import paramiko

from ..config import settings
from ..parser.sacct import DEFAULT_FIELDS as DEFAULT_SACCT_FIELDS

logger = logging.getLogger(__name__)

# 登录机连接健康状态（模块级，collector 重启后归零）
# 只追踪 SSH 连接层面的失败；命令非零退出码不影响此计数
_login_consecutive_failures = 0
_login_status = 'unknown'  # 'ok' / 'error' / 'unknown'
_login_health_lock = threading.Lock()  # 保护全局状态的线程锁


def _update_login_health(status: str, consecutive_failures: int,
                         last_success_at=None, last_failure_at=None,
                         last_error_msg=None) -> None:
    """写登录机健康状态到 DB；失败时静默（避免健康状态写失败级联报错）。"""
    try:
        from ..storage.node_health import update_node_health
        update_node_health(
            node_name='slurm-login',
            node_type='login',
            status=status,
            consecutive_failures=consecutive_failures,
            last_success_at=last_success_at,
            last_failure_at=last_failure_at,
            last_error_msg=last_error_msg,
        )
    except Exception as exc:
        logger.debug("failed to update login node health: %s", exc)


class SSHClient:
    """Slurm 登录节点 SSH 客户端，复用长连接以降低频繁调用的开销。"""

    def __init__(self):
        self._client: Optional[paramiko.SSHClient] = None

    def _connect(self) -> paramiko.SSHClient:
        """建立 SSH 连接；连接失败时抛异常，由调用方（client property）处理健康状态。"""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        connect_kwargs = {
            "hostname": settings.ssh_host,
            "port": settings.ssh_port,
            "username": settings.ssh_user,
            "timeout": settings.ssh_connect_timeout,
        }
        if settings.ssh_key_path:
            connect_kwargs["key_filename"] = settings.ssh_key_path
        elif settings.ssh_password:
            connect_kwargs["password"] = settings.ssh_password
        client.connect(**connect_kwargs)
        return client

    @property
    def client(self) -> paramiko.SSHClient:
        """返回已连接的 SSH 客户端；连接失败时更新健康状态并抛异常。

        健康状态只在此处更新，避免 _connect 和 run 双重计数。
        """
        global _login_consecutive_failures, _login_status
        if self._client is None or self._client.get_transport() is None or not self._client.get_transport().is_active():
            try:
                self._client = self._connect()
                # 连接成功：只在状态转变（非 ok → ok）或首次时写 DB
                with _login_health_lock:
                    if _login_status != 'ok':
                        _update_login_health(
                            status='ok',
                            consecutive_failures=0,
                            last_success_at=datetime.now(timezone.utc),
                        )
                    _login_consecutive_failures = 0
                    _login_status = 'ok'
            except Exception as exc:
                with _login_health_lock:
                    _login_consecutive_failures += 1
                    _login_status = 'error'
                    _update_login_health(
                        status='error',
                        consecutive_failures=_login_consecutive_failures,
                        last_failure_at=datetime.now(timezone.utc),
                        last_error_msg=str(exc),
                    )
                raise
        return self._client

    def run(self, command: str, timeout: int = 60) -> str:
        """执行命令并返回 stdout，失败抛异常。

        注意：命令非零退出码（如 sacct 权限问题）属于命令层面错误，
        不代表 SSH 连通性有问题，不更新登录机健康状态。
        """
        logger.debug("SSH exec: %s", command)
        stdin, stdout, stderr = self.client.exec_command(command, timeout=timeout)
        exit_code = stdout.channel.recv_exit_status()
        out = stdout.read().decode("utf-8", errors="replace")
        err = stderr.read().decode("utf-8", errors="replace")
        if exit_code != 0:
            raise RuntimeError(f"SSH command failed (exit {exit_code}): {err.strip() or out.strip()}")
        return out

    def close(self):
        if self._client is not None:
            self._client.close()
            self._client = None


# 全局单例，供采集器复用
ssh_client = SSHClient()


def _quote(s: str) -> str:
    return shlex.quote(s)


def sacct_jobs(
    start: str,
    end: str = "now",
    fields: str = ",".join(DEFAULT_SACCT_FIELDS),
    all_users: bool = True,
) -> str:
    """查询历史作业。start/end 形如 '2026-11-01' 或 'now'。

    all_users=True 等价于 sacct -a，返回所有用户作业。
    返回 -n 的纯数据（无表头），调用方按列顺序解析。
    """
    cmd = "sacct"
    if all_users:
        cmd += " -a"
    cmd += f" -S {_quote(start)} -E {_quote(end)}"
    cmd += f" --format={fields}"
    cmd += " -n --parsable2"
    return ssh_client.run(cmd)


def squeue_jobs() -> str:
    """获取实时排队与运行作业。返回 parsable2 格式。"""
    fmt = "%i|%P|%j|%u|%t|%M|%D|%C|%b|%R|%S|%l|%p|%q|%H"
    cmd = f"squeue -h --format={_quote(fmt)} -a"
    return ssh_client.run(cmd)


def sinfo_nodes() -> str:
    """获取节点状态。返回 parsable2 格式。"""
    fmt = "%n|%a|%t|%C|%G|%m|%f|%O|%e|%d|%r|%g"
    cmd = f"sinfo -h --format={_quote(fmt)}"
    return ssh_client.run(cmd)


def sacct_job_user(job_ids: list) -> str:
    """按 Job ID 批量查询用户信息，用于补全 REST API user 为空的记录。"""
    if not job_ids:
        return ""
    ids = ",".join(str(int(j)) for j in job_ids)
    cmd = f"sacct -j {_quote(ids)} --format=JobID,User,UID,Account,Group -n --parsable2"
    return ssh_client.run(cmd)


def nvidia_smi_node(node_host: str) -> str:
    """SSH 到计算节点执行 nvidia-smi，返回每卡一行的 CSV 输出。

    输出格式（逗号分隔）：
      index, uuid, name, utilization.gpu, memory.used, memory.total,
      power.draw, temperature.gpu, fan.speed
    """
    query_fields = (
        "index,uuid,name,"
        "utilization.gpu,"
        "memory.used,memory.total,"
        "power.draw,temperature.gpu,fan.speed"
    )
    # 先 SSH 到登录节点，再从那里 SSH 到计算节点执行命令
    # 计算节点账号与登录节点相同，通过 SSH ProxyJump 路由
    inner_cmd = f"nvidia-smi --query-gpu={query_fields} --format=csv,noheader,nounits"
    # 通过登录节点跳板连接到计算节点
    outer_cmd = f"ssh -o StrictHostKeyChecking=no {_quote(node_host)} {_quote(inner_cmd)}"
    return ssh_client.run(outer_cmd)
