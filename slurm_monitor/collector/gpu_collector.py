"""GPU 采集器 —— rgtop agent.py 注入方式

通过 paramiko 直连算力节点，注入 rgtop agent.py 的精简版并采集一帧 GPU 数据。
采集内容：GPU util/mem/temp/power/power_limit、每卡进程
（pid/user/mem_mb/job_id/cmd/rss_kb/cpu_pct/etime_s）、主机指标。
节点连接信息从 node_ip_map 表读取，支持密码和 SSH 密钥两种认证。

与 rgtop 原版 src/agent.py 的差异：本脚本是「一次调用出一帧 JSON」的批处理形态
（rgtop 是长连接逐帧），所以进程 %CPU 用采集窗口内的 /proc/<pid>/stat 差分算出，
语义与 rgtop 的单帧 %CPU 一致，而不是 ps 那种进程生命周期均值。
"""

from __future__ import annotations

import json
import logging
import textwrap
from typing import Optional

import paramiko

logger = logging.getLogger(__name__)

# rgtop agent.py 的内容，注入到目标节点执行
# 只取其中读取一帧 GPU 数据的部分：请求 "s"（system frame）和 "p"（进程帧）
# 为减少传输体积，使用内联的轻量采集脚本（兼容 Python 3.4+，stdlib only）
_AGENT_SCRIPT = textwrap.dedent(r"""
import ctypes, os, pwd, sys, json, time, re

NA = 2**64 - 1
TCK = os.sysconf("SC_CLK_TCK")
PAGE_KB = os.sysconf("SC_PAGE_SIZE") // 1024
LEVELS = "0123456789abcdefghijklmnopqrstuvwxyz"  # 每核占用 0..35，与 rgtop 的 @@C 编码一致
c_uint, c_ull, byref = ctypes.c_uint, ctypes.c_ulonglong, ctypes.byref

class Util(ctypes.Structure):
    _fields_ = [("gpu", c_uint), ("memory", c_uint)]

class Mem2(ctypes.Structure):
    _fields_ = [("version", c_uint), ("total", c_ull), ("reserved", c_ull), ("free", c_ull), ("used", c_ull)]

class Mem1(ctypes.Structure):
    _fields_ = [("total", c_ull), ("free", c_ull), ("used", c_ull)]

class Proc1(ctypes.Structure):
    _fields_ = [("pid", c_uint), ("mem", c_ull)]

# 沿进程祖先链找 "slurmstepd: [作业号.步骤]"，得到该进程所属的 Slurm 作业号（cgroup 里没有作业信息）
def job_of(pid):
    q = pid
    for _ in range(16):
        try:
            c = open("/proc/%d/cmdline" % q, "rb").read().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            return None
        m = re.match(r"slurmstepd: \[(\d+)\.", c)
        if m:
            return int(m.group(1))
        try:
            q = int(open("/proc/%d/stat" % q).read().rsplit(")", 1)[1].split()[1])
        except (OSError, IndexError, ValueError):
            return None
        if q <= 1:
            return None
    return None

names = {}
def user(uid):
    u = names.get(uid)
    if u is None:
        try: u = pwd.getpwuid(uid).pw_name
        except KeyError: u = str(uid)
        names[uid] = u
    return u

# 进程的 (cpu ticks, 启动 ticks, rss kB)；进程已退出返回 None
def proc_stat(pid):
    try:
        f = open("/proc/%d/stat" % pid).read().rsplit(")", 1)[1].split()  # comm 里可能有空格/括号
        return int(f[11]) + int(f[12]), int(f[19]), int(f[21]) * PAGE_KB
    except (IOError, OSError, IndexError, ValueError):
        return None

# 进程命令行，截断 300 字符（与 rgtop 一致）；已退出/不可见返回 ""
def proc_cmd(pid):
    try:
        return (open("/proc/%d/cmdline" % pid, "rb").read().replace(b"\0", b" ")
                .decode("utf-8", "replace").replace("\n", " ").replace("\r", " ").strip()[:300])
    except (IOError, OSError):
        return ""

try:
    uptime = float(open("/proc/uptime").read().split()[0])
except (IOError, OSError, IndexError, ValueError):
    uptime = 0.0

meta = {}
def proc_meta(pid):
    # 归属/命令行/常驻内存/已运行秒数。同一 pid 出现在多张卡上时只读一次 /proc
    m = meta.get(pid)
    if m is not None:
        return m
    m = {"user": None, "cmd": "", "rss_kb": None, "etime_s": None}
    try:
        uid = int(open("/proc/%d/status" % pid).read().split("Uid:")[1].split()[0])
        m["user"] = user(uid)
    except (IOError, OSError, IndexError, ValueError):
        pass  # 进程已退出或不可见，不要回退成当前用户
    m["cmd"] = proc_cmd(pid)
    st = proc_stat(pid)
    if st is not None:
        m["rss_kb"] = st[2]
        if uptime:
            m["etime_s"] = max(0, int(uptime - st[1] / TCK))
    meta[pid] = m
    return m


nv, devs = None, []
try:
    nv = ctypes.CDLL("libnvidia-ml.so.1")
    if nv.nvmlInit_v2(): raise OSError("nvmlInit failed")
    n = c_uint()
    nv.nvmlDeviceGetCount_v2(byref(n))
    buf = ctypes.create_string_buffer(96)
    for i in range(n.value):
        h = ctypes.c_void_p()
        if nv.nvmlDeviceGetHandleByIndex_v2(i, byref(h)): continue
        nm = buf.value.decode(errors="replace") if nv.nvmlDeviceGetName(h, buf, 96) == 0 else "[N/A]"
        uu = buf.value.decode(errors="replace") if nv.nvmlDeviceGetUUID(h, buf, 96) == 0 else "GPU-%d"%i
        devs.append((i, h, uu, nm))
except Exception: nv = None

if nv:
    mem2 = hasattr(nv, "nvmlDeviceGetMemoryInfo_v2")
    M = Mem2() if mem2 else Mem1()
    if mem2: M.version = ctypes.sizeof(Mem2) | (2<<24)
    get_mem = nv.nvmlDeviceGetMemoryInfo_v2 if mem2 else nv.nvmlDeviceGetMemoryInfo
    get_limit = getattr(nv, "nvmlDeviceGetEnforcedPowerLimit", None) or nv.nvmlDeviceGetPowerManagementLimit
    get_procs = nv.nvmlDeviceGetComputeRunningProcesses
    for fn in ("nvmlDeviceGetComputeRunningProcesses_v3", "nvmlDeviceGetComputeRunningProcesses_v2"):
        if hasattr(nv, fn): get_procs = getattr(nv, fn); break
U, T, P, L = Util(), c_uint(), c_uint(), c_uint()

# 采集窗口内的进程 %CPU：先记一次 ticks，host() 的 0.2s 采样后再记一次差分。
# 与 rgtop 逐帧的语义一致，也不是 ps 那种进程生命周期均值。
def ticks_now(pids):
    out, now = {}, time.monotonic()
    for pid in pids:
        st = proc_stat(pid)
        if st is not None:
            out[pid] = (st[0], now)
    return out

result = []
gpu_pids = set()
for i, h, uuid, nm in devs:
    util = U.gpu if nv.nvmlDeviceGetUtilizationRates(h, byref(U)) == 0 else None
    if get_mem(h, byref(M)) == 0:
        used_mb = (M.used + 524288) // 1048576
        total_mb = (M.total + 524288) // 1048576
    else:
        used_mb = total_mb = None
    temp = T.value if nv.nvmlDeviceGetTemperature(h, 0, byref(T)) == 0 else None
    power = round(P.value/1000.0, 2) if nv.nvmlDeviceGetPowerUsage(h, byref(P)) == 0 else None
    limit = round(L.value/1000.0, 2) if get_limit(h, byref(L)) == 0 else None

    # 进程列表
    procs = []
    n2 = 64
    while True:
        arr, c2 = (Proc1*n2)(), c_uint(n2)
        rc = get_procs(h, byref(c2), arr)
        if rc == 7 and n2 < 4096: n2 *= 4; continue
        if rc == 0:
            for k in range(c2.value):
                pid = arr[k].pid
                if pid >= 2**31:  # NVML 对不可见命名空间里的进程返回无效 pid
                    continue
                m = proc_meta(pid)
                procs.append({
                    "pid": pid,
                    "user": m["user"] or "?",
                    "mem_mb": (arr[k].mem+524288)//1048576,
                    "job_id": job_of(pid),
                    "cmd": m["cmd"],
                    "rss_kb": m["rss_kb"],
                    "etime_s": m["etime_s"],
                })
                gpu_pids.add(pid)
        break

    result.append({"idx": i, "uuid": uuid, "name": nm,
        "util_pct": util, "mem_used_mb": used_mb, "mem_total_mb": total_mb,
        "temp_c": temp, "power_w": power, "power_limit_w": limit,
        "procs": procs})

def host():
    h = {}
    try:
        mem = {}
        for line in open("/proc/meminfo"):
            k, v = line.split(":", 1)
            mem[k] = int(v.split()[0])  # kB
        h["mem_total_mb"] = mem["MemTotal"] // 1024
        h["mem_used_mb"] = (mem["MemTotal"] - mem["MemAvailable"]) // 1024
        h["mem_avail_mb"] = mem["MemAvailable"] // 1024
        h["mem_cache_mb"] = max(0, mem.get("Buffers", 0) + mem.get("Cached", 0)
                                + mem.get("SReclaimable", 0) - mem.get("Shmem", 0)) // 1024
        h["swap_total_mb"] = mem.get("SwapTotal", 0) // 1024
        h["swap_free_mb"] = mem.get("SwapFree", 0) // 1024
    except Exception: pass
    try:
        l = open("/proc/loadavg").read().split()
        h["load1"], h["load5"], h["load15"] = float(l[0]), float(l[1]), float(l[2])
    except Exception: pass
    try:
        # 解析 /proc/stat 的 cpu 行：第 1 行是总量，其余是每核；返回 [(total, idle+iowait)]
        def stat():
            out = []
            for l in open("/proc/stat"):
                if not l.startswith("cpu"): break
                v = list(map(int, l.split()[1:9]))
                out.append((sum(v), v[3] + (v[4] if len(v) > 4 else 0)))
            return out
        def busy(x, y):
            dt, di = y[0] - x[0], y[1] - x[1]
            return 0.0 if dt <= 0 else max(0.0, min(100.0, 100.0 * (dt - di) / dt))
        # 同一次 0.2s 窗口既算主机 CPU，也算 GPU 进程 %CPU
        a = stat(); p1 = ticks_now(gpu_pids); time.sleep(0.2); b = stat(); p2 = ticks_now(gpu_pids)
        h["cpu_util_pct"] = round(busy(a[0], b[0]), 1)
        h["cpu_count"] = len(b) - 1 or os.cpu_count()
        # 每核占用压成 base36（0..35，同 rgtop 的 @@C 编码），192 核也只有 192 字节
        h["cpu_cores"] = "".join(LEVELS[min(35, max(0, int(round(busy(x, y) * 35.0 / 100.0))))]
                                 for x, y in zip(a[1:], b[1:]))
        for pid, (tk, at) in p2.items():
            prev = p1.get(pid)
            if prev is None or at <= prev[1]:
                continue
            cpu_pct[pid] = round(100.0 * (tk - prev[0]) / TCK / (at - prev[1]), 1)
    except Exception: pass
    try:
        info = open("/proc/cpuinfo").read().splitlines()
        model = next((l.split(":", 1)[1].strip() for l in info if l.startswith("model name")), "")
        if model: h["cpu_model"] = model[:128]
        h["sockets"] = len(set(l for l in info if l.startswith("physical id"))) or 1
    except Exception: pass
    try:
        temps = []
        for d in os.listdir("/sys/class/hwmon"):
            base = "/sys/class/hwmon/" + d
            try:
                if open(base + "/name").read().strip() not in ("coretemp", "k10temp", "zenpower"):
                    continue
                t = int(open(base + "/temp1_input").read().strip())
                temps.append(t / 1000.0)
            except (IOError, OSError, ValueError):
                continue
        if temps: h["cpu_temp_c"] = round(max(temps), 0)
    except (IOError, OSError):
        pass
    if uptime:
        h["uptime_s"] = int(uptime)
    return h

cpu_pct = {}
host_info = host()
for g in result:
    for p in g["procs"]:
        p["cpu_pct"] = cpu_pct.get(p["pid"])

print(json.dumps({"gpus": result, "host": host_info}))
""").strip()


def _make_ssh_client(ip: str, port: int, ssh_user: str,
                     ssh_password: Optional[str] = None) -> paramiko.SSHClient:
    """创建并返回已连接的 paramiko SSH 客户端。

    ssh_password 非空时走密码认证（优先），为 None 时走挂载的 SSH 密钥。
    """
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    kwargs: dict = {
        "hostname": ip,
        "port": port,
        "username": ssh_user,
        "timeout": 15,
    }
    if ssh_password:
        kwargs["password"] = ssh_password
        kwargs["look_for_keys"] = False   # 密码登录时不尝试 key，避免 paramiko 警告
        kwargs["allow_agent"] = False
    else:
        kwargs["look_for_keys"] = True    # 走挂载的 SSH key（容器内 /root/.ssh/）
    client.connect(**kwargs)
    return client


def fetch_gpu_frame(
    ip: str,
    port: int = 22,
    ssh_user: str = "djs",
    ssh_password: Optional[str] = None,
) -> dict:
    """SSH 到算力节点注入 agent 脚本，采集一帧数据；失败抛异常（带可读原因）。

    返回 {"gpus": [...], "host": {...}}：
    gpus 每个元素对应一张 GPU 卡：{idx, uuid, name, util_pct, mem_used_mb, mem_total_mb,
        temp_c, power_w, power_limit_w, procs: [{pid, user, mem_mb}]}
    host 为主机指标：{cpu_util_pct, cpu_count, load1/5/15, mem_total_mb, mem_used_mb}（读不到的项缺省）
    """
    client = _make_ssh_client(ip, port, ssh_user, ssh_password)
    try:
        _, stdout, stderr = client.exec_command(f"python3 -c {_shell_quote(_AGENT_SCRIPT)}", timeout=30)
        out = stdout.read().decode("utf-8", errors="replace").strip()
        err = stderr.read().decode("utf-8", errors="replace").strip()
        exit_code = stdout.channel.recv_exit_status()
        if exit_code != 0:
            raise RuntimeError(f"agent 退出码 {exit_code}: {err[:200] or out[:200]}")
        try:
            data = json.loads(out)
            return data if isinstance(data, dict) else {"gpus": data, "host": {}}
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"agent 输出无法解析: {exc}; 原始输出: {out[:200]}") from exc
    finally:
        client.close()


def collect_gpu_node_agent(
    node_name: str,
    ip: str,
    port: int = 22,
    ssh_user: str = "djs",
    ssh_password: Optional[str] = None,
) -> list[dict]:
    """兼容旧调用：失败只记日志并返回空列表。"""
    try:
        return fetch_gpu_frame(ip, port, ssh_user, ssh_password)["gpus"]
    except Exception as exc:
        logger.warning("GPU agent failed for %s (%s): %s", node_name, ip, exc)
        return []


def _shell_quote(s: str) -> str:
    """用单引号包裹脚本，转义内部单引号。"""
    return "'" + s.replace("'", "'\\''") + "'"


def agent_rows_to_db(node_name: str, gpus: list[dict]) -> list[dict]:
    """将 agent 采集结果转换为 save_gpu_snapshots 期望的格式。"""
    rows = []
    for g in gpus:
        # 一张卡通常只属于一个作业；有多个时取显存占用最大的进程所属作业
        owned = [p for p in g.get("procs", []) if p.get("job_id")]
        job_id = max(owned, key=lambda p: p.get("mem_mb") or 0)["job_id"] if owned else None
        rows.append({
            "index":         g.get("idx"),
            "uuid":          g.get("uuid"),
            "name":          g.get("name"),
            "util_pct":      g.get("util_pct"),
            "mem_used_mb":   g.get("mem_used_mb"),
            "mem_total_mb":  g.get("mem_total_mb"),
            "power_w":       g.get("power_w"),
            "power_limit_w": g.get("power_limit_w"),
            "temp_c":        g.get("temp_c"),
            "fan_pct":       None,
            "proc_json":     json.dumps(g.get("procs", []), ensure_ascii=False),
            "node_name":     node_name,
            "job_id":        job_id,
        })
    return rows
