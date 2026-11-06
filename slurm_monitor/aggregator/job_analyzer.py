"""作业效率分析器

基于 jobeff.sh 脚本逻辑，分析已结束作业的效率。
从 JobHistory 读取作业数据，生成效率分析报告。
"""

import json
import logging
from datetime import datetime, timezone
from typing import Optional, List, Dict, Any

from sqlalchemy.dialects.postgresql import insert

from ..storage.db import get_session
from ..storage.models import JobHistory, JobEfficiencyAnalysis, GpuSnapshot

logger = logging.getLogger(__name__)


class JobEfficiencyAnalyzer:
    """作业效率分析器"""

    def analyze_job(self, job_id: int, cluster: str = "") -> Optional[Dict[str, Any]]:
        """分析单个作业的效率

        Args:
            job_id: 作业 ID
            cluster: 集群名称

        Returns:
            效率分析结果字典，若作业不存在则返回 None
        """
        with get_session() as session:
            job = session.query(JobHistory).filter(
                JobHistory.job_id == job_id,
                JobHistory.cluster == cluster
            ).first()

            if not job:
                logger.warning(f"作业 {job_id} 不存在")
                return None

            # 1. 计算时间效率
            time_eff_pct = None
            if job.elapsed_seconds and job.timelimit_minutes:
                timelimit_seconds = job.timelimit_minutes * 60
                time_eff_pct = round(100.0 * job.elapsed_seconds / timelimit_seconds, 2)

            # 2. 计算 CPU 效率
            cpu_eff_pct = None
            if job.total_cpu_seconds and job.alloc_cpus and job.elapsed_seconds:
                core_wall_seconds = job.alloc_cpus * job.elapsed_seconds
                cpu_eff_pct = round(100.0 * job.total_cpu_seconds / core_wall_seconds, 2)

            # 3. 计算内存效率
            mem_eff_pct = None
            req_mem_bytes = self._calculate_req_mem_bytes(
                job.req_mem_mb, job.nodes_alloc, job.alloc_cpus
            )
            if job.max_rss_mb and req_mem_bytes:
                mem_eff_pct = round(100.0 * job.max_rss_mb * 1024 * 1024 / req_mem_bytes, 2)

            # 4. 获取 GPU 指标（如果有）
            gpu_metrics = self._get_gpu_metrics(job_id, job.start_time, job.end_time, session)

            # 5. 生成诊断建议
            diagnosis, suggestions = self._generate_diagnosis(
                job, time_eff_pct, cpu_eff_pct, mem_eff_pct, gpu_metrics
            )

            # 6. 计算综合评分
            efficiency_score = self._calculate_score(
                time_eff_pct, cpu_eff_pct, mem_eff_pct, gpu_metrics
            )

            result = {
                "job_id": job_id,
                "cluster": cluster,
                "time_eff_pct": time_eff_pct,
                "cpu_eff_pct": cpu_eff_pct,
                "mem_eff_pct": mem_eff_pct,
                "gpu_util_avg": gpu_metrics.get("avg"),
                "gpu_util_p50": gpu_metrics.get("p50"),
                "gpu_util_p90": gpu_metrics.get("p90"),
                "gpu_mem_peak_mb": gpu_metrics.get("mem_peak"),
                "gpu_idle_ratio": gpu_metrics.get("idle_ratio"),
                "efficiency_score": efficiency_score,
                "diagnosis": diagnosis,
                "suggestions": json.dumps(suggestions, ensure_ascii=False),
                "gpu_util_trend": json.dumps(gpu_metrics.get("trend", []), ensure_ascii=False) if gpu_metrics.get("trend") else None,
                "gpu_mem_trend": json.dumps(gpu_metrics.get("mem_trend", []), ensure_ascii=False) if gpu_metrics.get("mem_trend") else None,
            }

            # 写入数据库
            stmt = insert(JobEfficiencyAnalysis).values(result)
            stmt = stmt.on_conflict_do_update(
                index_elements=["job_id"],
                set_={
                    "time_eff_pct": stmt.excluded.time_eff_pct,
                    "cpu_eff_pct": stmt.excluded.cpu_eff_pct,
                    "mem_eff_pct": stmt.excluded.mem_eff_pct,
                    "gpu_util_avg": stmt.excluded.gpu_util_avg,
                    "gpu_util_p50": stmt.excluded.gpu_util_p50,
                    "gpu_util_p90": stmt.excluded.gpu_util_p90,
                    "gpu_mem_peak_mb": stmt.excluded.gpu_mem_peak_mb,
                    "gpu_idle_ratio": stmt.excluded.gpu_idle_ratio,
                    "efficiency_score": stmt.excluded.efficiency_score,
                    "diagnosis": stmt.excluded.diagnosis,
                    "suggestions": stmt.excluded.suggestions,
                    "gpu_util_trend": stmt.excluded.gpu_util_trend,
                    "gpu_mem_trend": stmt.excluded.gpu_mem_trend,
                    "analyzed_at": datetime.now(timezone.utc),
                }
            )
            session.execute(stmt)
            session.commit()

            logger.info(f"已分析作业 {job_id}, 评分: {efficiency_score}")
            return result

    def _calculate_req_mem_bytes(self, req_mem_mb: Optional[int], nodes: int, cpus: int) -> Optional[int]:
        """计算申请内存字节数（简化版）"""
        if not req_mem_mb:
            return None
        # 简化处理：假设 req_mem_mb 是总内存（实际可能是 per-cpu 或 per-node）
        return req_mem_mb * 1024 * 1024

    def _get_gpu_metrics(self, job_id: int, start_time, end_time, session) -> Dict[str, Any]:
        """获取作业的 GPU 指标"""
        if not start_time or not end_time:
            return {}

        # 查询该作业的 GPU 快照
        snapshots = session.query(GpuSnapshot).filter(
            GpuSnapshot.job_id == job_id,
            GpuSnapshot.collected_at >= start_time,
            GpuSnapshot.collected_at <= end_time,
            GpuSnapshot.util_pct.isnot(None)
        ).order_by(GpuSnapshot.collected_at).all()

        if not snapshots:
            return {}

        utils = [s.util_pct for s in snapshots if s.util_pct is not None]
        mems = [s.mem_used_mb for s in snapshots if s.mem_used_mb is not None]

        if not utils:
            return {}

        # 统计指标
        utils_sorted = sorted(utils)
        n = len(utils_sorted)

        avg_util = sum(utils) / len(utils)
        p50_util = utils_sorted[n // 2] if n > 0 else 0
        p90_util = utils_sorted[int(n * 0.9)] if n > 0 else 0
        mem_peak = max(mems) if mems else None

        # 计算空转占比（利用率 < 10%）
        idle_count = sum(1 for u in utils if u < 10)
        idle_ratio = round(100.0 * idle_count / len(utils), 2)

        # 降采样趋势数据到 60 点
        trend = self._downsample(utils, 60)
        mem_trend = self._downsample(mems, 60) if mems else []

        return {
            "avg": round(avg_util, 1),
            "p50": round(p50_util, 1),
            "p90": round(p90_util, 1),
            "mem_peak": mem_peak,
            "idle_ratio": idle_ratio,
            "trend": trend,
            "mem_trend": mem_trend,
        }

    def _downsample(self, data: List[float], target_points: int) -> List[float]:
        """降采样数据到目标点数"""
        if len(data) <= target_points:
            return [round(v, 1) for v in data]

        step = len(data) / target_points
        result = []
        for i in range(target_points):
            idx = int(i * step)
            result.append(round(data[idx], 1))
        return result

    def _generate_diagnosis(
        self,
        job,
        time_eff: Optional[float],
        cpu_eff: Optional[float],
        mem_eff: Optional[float],
        gpu_metrics: Dict[str, Any]
    ) -> tuple[str, List[str]]:
        """生成诊断和建议"""
        diagnosis_parts = []
        suggestions = []

        # 作业状态
        if job.state != "COMPLETED":
            diagnosis_parts.append(f"作业未正常结束（{job.state}）")
            suggestions.append(f"检查作业失败原因，退出码: {job.exit_code}")
            return " · ".join(diagnosis_parts), suggestions

        # 时间效率
        if time_eff and time_eff < 25:
            diagnosis_parts.append(f"时间利用率偏低 ({time_eff:.1f}%)")
            suggestions.append(f"申请时长过长，建议下次将 -t 参数减小到实际运行时长的 1.2 倍")

        # CPU 效率
        if cpu_eff and cpu_eff < 50:
            diagnosis_parts.append(f"CPU 效率偏低 ({cpu_eff:.1f}%)")
            suggestions.append(f"考虑减小 --cpus-per-task，当前申请 {job.alloc_cpus} 核可能过多")
        elif cpu_eff and cpu_eff > 90:
            diagnosis_parts.append(f"CPU 高效利用 ({cpu_eff:.1f}%)")

        # 内存效率
        if mem_eff and mem_eff < 25:
            diagnosis_parts.append(f"内存效率偏低 ({mem_eff:.1f}%)")
            suggestions.append(f"考虑减小 --mem，当前申请 {job.req_mem_mb / 1024:.1f}GB 可能过多")
        elif mem_eff and mem_eff > 95:
            suggestions.append(f"内存使用接近上限，建议增加 10-20% 余量避免 OOM")

        # GPU 效率
        gpu_avg = gpu_metrics.get("avg")
        idle_ratio = gpu_metrics.get("idle_ratio")

        if gpu_avg is not None:
            if gpu_avg < 40:
                diagnosis_parts.append(f"GPU 利用率偏低 ({gpu_avg:.1f}%)")
                suggestions.append("检查代码是否充分利用 GPU，或考虑使用更小的 GPU 型号")
            elif gpu_avg > 80:
                diagnosis_parts.append(f"GPU 高效利用 ({gpu_avg:.1f}%)")

            if idle_ratio and idle_ratio > 30:
                diagnosis_parts.append(f"GPU 空转占比高 ({idle_ratio:.1f}%)")
                suggestions.append("检查训练/推理代码，可能存在数据加载瓶颈或长时间初始化")

        # 综合评价
        if not diagnosis_parts:
            diagnosis_parts.append("资源利用良好")

        return " · ".join(diagnosis_parts), suggestions

    def _calculate_score(
        self,
        time_eff: Optional[float],
        cpu_eff: Optional[float],
        mem_eff: Optional[float],
        gpu_metrics: Dict[str, Any]
    ) -> int:
        """计算综合效率评分 0-100

        权重：
        - GPU 利用率: 40%
        - CPU 效率: 30%
        - 内存效率: 20%
        - 时间效率: 10%
        """
        score = 0
        weights = 0

        # GPU 评分（最重要）
        gpu_avg = gpu_metrics.get("avg")
        if gpu_avg is not None:
            gpu_score = min(100, gpu_avg * 1.2)  # 80% 利用率得满分
            score += gpu_score * 0.4
            weights += 0.4

        # CPU 评分
        if cpu_eff is not None:
            cpu_score = min(100, cpu_eff * 1.1)  # 90% 效率得满分
            score += cpu_score * 0.3
            weights += 0.3

        # 内存评分
        if mem_eff is not None:
            # 50-90% 内存利用率最佳，过低或过高都扣分
            if 50 <= mem_eff <= 90:
                mem_score = 100
            elif mem_eff < 50:
                mem_score = mem_eff * 2
            else:  # > 90
                mem_score = 100 - (mem_eff - 90) * 2
            score += mem_score * 0.2
            weights += 0.2

        # 时间评分
        if time_eff is not None:
            # 70-100% 时间利用率最佳
            if time_eff >= 70:
                time_score = 100
            else:
                time_score = time_eff * 1.4
            score += time_score * 0.1
            weights += 0.1

        # 归一化
        if weights > 0:
            final_score = int(score / weights)
        else:
            final_score = 0

        return max(0, min(100, final_score))

    def analyze_recent_jobs(self, hours: int = 24, limit: int = 100):
        """分析最近 N 小时内结束的作业

        Args:
            hours: 回溯小时数
            limit: 最多分析作业数
        """
        from datetime import timedelta

        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

        with get_session() as session:
            jobs = session.query(JobHistory).filter(
                JobHistory.end_time >= cutoff,
                JobHistory.state.in_(["COMPLETED", "FAILED", "TIMEOUT", "CANCELLED"])
            ).order_by(JobHistory.end_time.desc()).limit(limit).all()

            logger.info(f"找到 {len(jobs)} 个待分析作业")

            for job in jobs:
                try:
                    self.analyze_job(job.job_id, job.cluster)
                except Exception as e:
                    logger.error(f"分析作业 {job.job_id} 失败: {e}")
