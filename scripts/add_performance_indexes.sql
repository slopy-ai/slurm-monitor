-- 性能优化索引创建脚本（幂等，可重复执行）
--
-- 说明：`gpu_snapshots`、`node_host_stats` 的 (node_name, collected_at DESC) 索引
-- 已由 SQLAlchemy 模型定义并在 scripts/init_new_tables.py 中补建，本脚本不再重复创建。
-- 这里只包含本次性能优化新增、且值较高的索引。
--
-- 执行方式：
--   docker exec -i slurm-monitor-pg psql -U slurm_monitor -d slurm_monitor \
--     < scripts/add_performance_indexes.sql
--
-- 注意：CREATE INDEX CONCURRENTLY 不能在事务块中运行，psql 逐条执行即可。

\echo '开始创建性能优化索引...'

-- 1. GPU 快照：作业号 + 时间（作业效率分析按 job_id 求利用率均值）
\echo '创建 idx_gpu_snapshots_job_time...'
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_gpu_snapshots_job_time
ON gpu_snapshots(job_id, collected_at)
WHERE job_id IS NOT NULL;

-- 2. 作业历史：用户 + 状态 + 提交时间（用户统计、Top 榜按时间范围过滤）
\echo '创建 idx_job_history_user_state_submit...'
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_job_history_user_state_submit
ON job_history("user", state, submit_time);

-- 3. 作业：分区 + 状态（节点列表的排队作业统计）
\echo '创建 idx_jobs_partition_state...'
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_jobs_partition_state
ON jobs(partition, state);

\echo '索引创建完成！'

\echo '当前相关索引：'
SELECT tablename, indexname
FROM pg_indexes
WHERE tablename IN ('gpu_snapshots', 'job_history', 'jobs', 'node_host_stats')
  AND (indexname LIKE 'idx_%' OR indexname LIKE 'ix_%')
ORDER BY tablename, indexname;
