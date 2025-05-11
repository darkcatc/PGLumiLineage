-- 插入 TPC-DS 数据源配置
INSERT INTO lumi_config.data_sources (
    source_name,
    source_type,
    description,
    is_active,
    db_host,
    db_port,
    db_name,
    db_user,
    db_password,
    log_retrieval_method,
    log_path_pattern
) VALUES (
    'tpcds',
    'postgresql',
    'TPC-DS 测试数据库，用于生成 SQL 日志',
    TRUE,
    '127.0.0.1',
    5432,
    'tpcds',
    'postgres',
    'postgres',
    'local_path',
    '/mnt/e/Projects/PGLumiLineage/tmp/tpcds-log/postgresql-*.csv'
);

-- 插入同步计划
INSERT INTO lumi_config.source_sync_schedules (
    source_id,
    is_schedule_active,
    sync_frequency_type,
    sync_interval_seconds,
    priority
) VALUES (
    (SELECT source_id FROM lumi_config.data_sources WHERE source_name = 'tpcds'),
    TRUE,
    'interval',
    60, -- 每分钟同步一次
    10  -- 优先级较高
);

-- 查询插入的记录
SELECT ds.source_id, ds.source_name, ds.log_retrieval_method, ds.log_path_pattern, 
       ss.schedule_id, ss.sync_frequency_type, ss.sync_interval_seconds
FROM lumi_config.data_sources ds
JOIN lumi_config.source_sync_schedules ss ON ds.source_id = ss.source_id
WHERE ds.source_name = 'tpcds';
