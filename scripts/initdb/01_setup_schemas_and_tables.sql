-- ============================================================================
-- PGLumiLineage 数据库表结构初始化脚本
-- 
-- 此脚本用于在iwdb数据库中创建PGLumiLineage项目所需的schema和表结构。
-- 脚本设计为幂等的，可以多次执行而不会产生错误。
-- 此脚本应以lumiadmin用户身份执行。
--
-- 作者: Vance Chen
-- ============================================================================

-- 连接到iwdb数据库
\connect iwdb

-- ----------------------------------------------------------------------------
-- 1. 创建 lumi_logs schema
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS lumi_logs AUTHORIZATION lumiadmin;

COMMENT ON SCHEMA lumi_logs IS 'Schema to store raw, parsed log entries captured for PGLumiLineage analysis (within iwdb).';

-- ----------------------------------------------------------------------------
-- 2. 创建 lumi_analytics schema
-- ----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS lumi_analytics AUTHORIZATION lumiadmin;

COMMENT ON SCHEMA lumi_analytics IS 'Schema to store normalized SQL patterns, their aggregated statistics, and LLM analysis results for PGLumiLineage (within iwdb).';

-- ----------------------------------------------------------------------------
-- 3.Table: lumi_logs.captured_logs
--# Stores individual log entries after parsing from PostgreSQL logs.
--# This table is partitioned by log_time (monthly).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lumi_logs.captured_logs (
    log_id BIGSERIAL,
    captured_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    log_time TIMESTAMPTZ NOT NULL,
    source_database_name TEXT NOT NULL, -- Name of the source database being monitored (e.g., 'tpcds')
    username TEXT,
    database_name_logged TEXT,
    client_addr INET,
    application_name TEXT,
    session_id TEXT,
    query_id BIGINT,
    duration_ms INTEGER NOT NULL,
    raw_sql_text TEXT NOT NULL,
    normalized_sql_hash TEXT,
    is_processed_for_analysis BOOLEAN DEFAULT FALSE NOT NULL,
    log_source_identifier TEXT,

    CONSTRAINT pk_captured_logs PRIMARY KEY (log_id, log_time)
) PARTITION BY RANGE (log_time);

-- 添加表和列的注释
COMMENT ON TABLE lumi_logs.captured_logs IS 'Stores individual parsed SQL log entries. Partitioned monthly by log_time.';
COMMENT ON COLUMN lumi_logs.captured_logs.log_id IS 'Unique identifier for this captured log entry.';
COMMENT ON COLUMN lumi_logs.captured_logs.captured_at IS 'Timestamp when this specific record was ingested into the PGLumiLineage system.';
COMMENT ON COLUMN lumi_logs.captured_logs.log_time IS 'Actual timestamp of the log event from the source PostgreSQL log. Partition key.';
COMMENT ON COLUMN lumi_logs.captured_logs.source_database_name IS 'The name of the source database being monitored (e.g., tpcds, erp_prod). Configurable.';
COMMENT ON COLUMN lumi_logs.captured_logs.username IS 'Username associated with the log entry.';
COMMENT ON COLUMN lumi_logs.captured_logs.database_name_logged IS 'Database name as reported in the log line.';
COMMENT ON COLUMN lumi_logs.captured_logs.client_addr IS 'Client IP address and port, if available.';
COMMENT ON COLUMN lumi_logs.captured_logs.application_name IS 'Application name reported by the client.';
COMMENT ON COLUMN lumi_logs.captured_logs.session_id IS 'PostgreSQL backend session ID.';
COMMENT ON COLUMN lumi_logs.captured_logs.query_id IS 'Internal query identifier (e.g., from pg_stat_statements or auto_explain if logged).';
COMMENT ON COLUMN lumi_logs.captured_logs.duration_ms IS 'Query execution time in milliseconds.';
COMMENT ON COLUMN lumi_logs.captured_logs.raw_sql_text IS 'The verbatim SQL query text.';
COMMENT ON COLUMN lumi_logs.captured_logs.normalized_sql_hash IS 'Hash of the normalized version of raw_sql_text. Links to the analytical patterns table.';
COMMENT ON COLUMN lumi_logs.captured_logs.is_processed_for_analysis IS 'True if this log entry has been fed into the SQL normalization and pattern aggregation process.';
COMMENT ON COLUMN lumi_logs.captured_logs.log_source_identifier IS 'Optional identifier for the log origin, if collecting from multiple distinct PG instances/log files.';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_captured_logs_log_time ON lumi_logs.captured_logs USING BRIN (log_time);
CREATE INDEX IF NOT EXISTS idx_captured_logs_is_processed ON lumi_logs.captured_logs (is_processed_for_analysis) WHERE is_processed_for_analysis = FALSE;
CREATE INDEX IF NOT EXISTS idx_captured_logs_normalized_sql_hash ON lumi_logs.captured_logs (normalized_sql_hash) WHERE normalized_sql_hash IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_captured_logs_source_database_name ON lumi_logs.captured_logs (source_database_name);

-- ----------------------------------------------------------------------------
-- 4. Table: lumi_analytics.sql_patterns
-- # Stores unique, normalized SQL patterns and their analysis metadata.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS lumi_analytics.sql_patterns (
    sql_hash TEXT PRIMARY KEY,
    normalized_sql_text TEXT NOT NULL,
    sample_raw_sql_text TEXT NOT NULL,
    source_database_name TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL,
    last_seen_at TIMESTAMPTZ NOT NULL,
    execution_count BIGINT DEFAULT 1 NOT NULL,
    total_duration_ms BIGINT DEFAULT 0 NOT NULL,
    avg_duration_ms DOUBLE PRECISION DEFAULT 0.0 NOT NULL,
    max_duration_ms INTEGER DEFAULT 0 NOT NULL,
    min_duration_ms INTEGER DEFAULT 0 NOT NULL,
    llm_analysis_status TEXT DEFAULT 'PENDING' NOT NULL,
    llm_extracted_relations_json JSONB,
    last_llm_analysis_at TIMESTAMPTZ,
    tags TEXT[]
);

-- 添加表和列的注释
COMMENT ON TABLE lumi_analytics.sql_patterns IS 'Stores unique, normalized SQL patterns, their aggregated statistics, and LLM analysis results.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.sql_hash IS 'SHA256 (or similar) hash of normalized_sql_text, primary key.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.normalized_sql_text IS 'The SQL query template after removing literals and standardizing structure.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.sample_raw_sql_text IS 'One example of an original SQL query that matches this normalized pattern.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.source_database_name IS 'The name of the source database (e.g., tpcds) where this pattern was first or is predominantly observed.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.first_seen_at IS 'Timestamp of the first log entry that matched this pattern.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.last_seen_at IS 'Timestamp of the most recent log entry that matched this pattern.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.execution_count IS 'Total number of times queries matching this pattern have been observed.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.total_duration_ms IS 'Cumulative execution time (in ms) for all queries matching this pattern.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.avg_duration_ms IS 'Average execution time (in ms) for queries matching this pattern.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.max_duration_ms IS 'Maximum execution time (in ms) observed for any query matching this pattern.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.min_duration_ms IS 'Minimum execution time (in ms) observed for any query matching this pattern.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.llm_analysis_status IS 'Current status of the LLM analysis for this pattern (e.g., PENDING, COMPLETED_SUCCESS).';
COMMENT ON COLUMN lumi_analytics.sql_patterns.llm_extracted_relations_json IS 'Structured JSON output from the LLM detailing entities and relationships.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.last_llm_analysis_at IS 'Timestamp of the last time LLM analysis was performed or attempted on this pattern.';
COMMENT ON COLUMN lumi_analytics.sql_patterns.tags IS 'Array of tags for categorization, e.g., ["critical", "daily_etl", "user_facing_report"].';

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_sql_patterns_llm_analysis_status ON lumi_analytics.sql_patterns (llm_analysis_status) WHERE llm_analysis_status IN ('PENDING', 'NEEDS_REANALYSIS');
CREATE INDEX IF NOT EXISTS idx_sql_patterns_last_seen_at ON lumi_analytics.sql_patterns (last_seen_at DESC);
CREATE INDEX IF NOT EXISTS idx_sql_patterns_source_database_name ON lumi_analytics.sql_patterns (source_database_name);
CREATE INDEX IF NOT EXISTS idx_sql_patterns_tags_gin ON lumi_analytics.sql_patterns USING GIN (tags);

-- ----------------------------------------------------------------------------
-- 5. 创建 lumi_logs.captured_logs 的月度分区
-- ----------------------------------------------------------------------------
-- 注意：实际生产环境中，应该有一个定时任务来提前创建未来的分区
-- 动态创建当前月和未来三个月的分区表
DO $$
DECLARE
    current_month DATE;
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
    i INTEGER;
BEGIN
    -- 获取当前月份的第一天
    current_month := date_trunc('month', CURRENT_DATE);
    
    -- 创建当前月和未来三个月的分区，共四个月
    FOR i IN 0..3 LOOP
        -- 计算分区的开始和结束日期
        start_date := current_month + (i || ' month')::INTERVAL;
        end_date := start_date + '1 month'::INTERVAL;
        
        -- 构造分区名称，格式为 captured_logs_y<year>m<month>
        partition_name := 'captured_logs_y' || 
                         to_char(start_date, 'YYYY') || 
                         'm' || 
                         to_char(start_date, 'MM');
        
        -- 创建分区表
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS lumi_logs.%I PARTITION OF lumi_logs.captured_logs FOR VALUES FROM (%L) TO (%L)',
            partition_name,
            start_date,
            end_date
        );
        
        RAISE NOTICE '已创建分区表 lumi_logs.%: % 至 %', 
                     partition_name, 
                     to_char(start_date, 'YYYY-MM-DD'), 
                     to_char(end_date, 'YYYY-MM-DD');
    END LOOP;
END
$$;

-- ----------------------------------------------------------------------------
-- 6. 添加外键约束
-- ----------------------------------------------------------------------------
-- 注意：此操作是幂等的，如果约束已存在，会抛出错误但不会中断脚本执行
DO $$
BEGIN
    -- 检查外键约束是否已存在
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.table_constraints 
        WHERE constraint_name = 'fk_captured_logs_to_sql_patterns' 
        AND table_schema = 'lumi_logs'
        AND table_name = 'captured_logs'
    ) THEN
        -- 添加外键约束
        ALTER TABLE lumi_logs.captured_logs
        ADD CONSTRAINT fk_captured_logs_to_sql_patterns
        FOREIGN KEY (normalized_sql_hash)
        REFERENCES lumi_analytics.sql_patterns(sql_hash)
        ON DELETE SET NULL
        ON UPDATE CASCADE;
        
        RAISE NOTICE '已添加外键约束 fk_captured_logs_to_sql_patterns';
    ELSE
        RAISE NOTICE '外键约束 fk_captured_logs_to_sql_patterns 已存在，跳过创建';
    END IF;
END
$$;

-- ----------------------------------------------------------------------------
-- 设置lumiadmin权限
-- ----------------------------------------------------------------------------

-- 将 lumi_logs schema 的所有者设置为 lumiadmin
ALTER SCHEMA lumi_logs OWNER TO lumiadmin;

-- 将 lumi_logs schema 中的所有表的所有者设置为 lumiadmin
DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN 
        SELECT tablename FROM pg_tables WHERE schemaname = 'lumi_logs'
    LOOP
        EXECUTE format('ALTER TABLE lumi_logs.%I OWNER TO lumiadmin', obj.tablename);
    END LOOP;
    FOR obj IN 
        SELECT tablename FROM pg_tables WHERE schemaname = 'lumi_analytics'
    LOOP
        EXECUTE format('ALTER TABLE lumi_analytics.%I OWNER TO lumiadmin', obj.tablename);
    END LOOP;
END $$;

-- 将 lumi_logs schema 中的所有序列的所有者设置为 lumiadmin
DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN 
        SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'lumi_logs'
    LOOP
        EXECUTE format('ALTER SEQUENCE lumi_logs.%I OWNER TO lumiadmin', obj.sequence_name);
    END LOOP;
    FOR obj IN 
        SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'lumi_analytics'
    LOOP
        EXECUTE format('ALTER SEQUENCE lumi_analytics.%I OWNER TO lumiadmin', obj.sequence_name);
    END LOOP;
END $$;
-- ----------------------------------------------------------------------------
-- 7. 为 lumiuser 授予必要权限
-- ----------------------------------------------------------------------------
-- 授予schema使用权限
GRANT USAGE ON SCHEMA lumi_logs TO lumiuser;
GRANT USAGE ON SCHEMA lumi_analytics TO lumiuser;

-- 授予表操作权限
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE lumi_logs.captured_logs TO lumiuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE lumi_analytics.sql_patterns TO lumiuser;

--##############################################################################
--# Table: lumi_analytics.sql_normalization_errors
--# 存储SQL规范化过程中失败的记录及原因
--##############################################################################
CREATE TABLE IF NOT EXISTS lumi_analytics.sql_normalization_errors (
    error_id BIGSERIAL PRIMARY KEY,
    source_type TEXT NOT NULL CHECK (source_type IN ('LOG', 'VIEW', 'FUNCTION')), -- 来源类型
    source_id INTEGER NOT NULL, -- 日志ID、对象ID或函数ID
    raw_sql_text TEXT NOT NULL, -- 原始SQL文本
    error_reason TEXT NOT NULL, -- 失败原因
    error_details TEXT, -- 详细错误信息
    source_database_name TEXT, -- 源数据库名称
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    processed_flag BOOLEAN DEFAULT FALSE NOT NULL -- 是否已处理标志
);
ALTER TABLE lumi_analytics.sql_normalization_errors OWNER TO lumiadmin;

-- 添加索引以加快查询
CREATE INDEX IF NOT EXISTS idx_sql_normalization_errors_source_type ON lumi_analytics.sql_normalization_errors(source_type);
CREATE INDEX IF NOT EXISTS idx_sql_normalization_errors_source_id ON lumi_analytics.sql_normalization_errors(source_id);
CREATE INDEX IF NOT EXISTS idx_sql_normalization_errors_processed_flag ON lumi_analytics.sql_normalization_errors(processed_flag);
CREATE INDEX IF NOT EXISTS idx_sql_normalization_errors_created_at ON lumi_analytics.sql_normalization_errors(created_at);

-- 表注释
COMMENT ON TABLE lumi_analytics.sql_normalization_errors IS '存储SQL规范化过程中失败的记录及原因，用于后续分析和改进SQL规范化逻辑';

-- 列注释
COMMENT ON COLUMN lumi_analytics.sql_normalization_errors.error_id IS '错误记录唯一标识符，自增主键';
COMMENT ON COLUMN lumi_analytics.sql_normalization_errors.source_type IS 'SQL来源类型，如LOG（日志）、VIEW（视图）、FUNCTION（函数）';
COMMENT ON COLUMN lumi_analytics.sql_normalization_errors.source_id IS '来源ID，根据source_type不同对应不同表的ID';
COMMENT ON COLUMN lumi_analytics.sql_normalization_errors.raw_sql_text IS '原始SQL文本';
COMMENT ON COLUMN lumi_analytics.sql_normalization_errors.error_reason IS '失败原因，如解析错误、非数据流转SQL等';
COMMENT ON COLUMN lumi_analytics.sql_normalization_errors.error_details IS '详细错误信息，如异常堆栈等';
COMMENT ON COLUMN lumi_analytics.sql_normalization_errors.source_database_name IS '源数据库名称';
COMMENT ON COLUMN lumi_analytics.sql_normalization_errors.created_at IS '记录创建时间';
COMMENT ON COLUMN lumi_analytics.sql_normalization_errors.processed_flag IS '是否已处理标志，用于标记是否已经分析和处理过该错误';

-- 授予 lumiuser 对该表的权限
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE lumi_analytics.sql_normalization_errors TO lumiuser;
GRANT USAGE, SELECT ON SEQUENCE lumi_analytics.sql_normalization_errors_error_id_seq TO lumiuser;

-- 授予序列使用权限
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA lumi_logs TO lumiuser;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA lumi_analytics TO lumiuser;

-- ----------------------------------------------------------------------------
-- 8. 设置默认权限
-- ----------------------------------------------------------------------------
-- 为lumi_logs schema设置默认权限
ALTER DEFAULT PRIVILEGES IN SCHEMA lumi_logs FOR ROLE lumiadmin
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO lumiuser;

ALTER DEFAULT PRIVILEGES IN SCHEMA lumi_logs FOR ROLE lumiadmin
GRANT USAGE, SELECT ON SEQUENCES TO lumiuser;

-- 为lumi_analytics schema设置默认权限
ALTER DEFAULT PRIVILEGES IN SCHEMA lumi_analytics FOR ROLE lumiadmin
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO lumiuser;

ALTER DEFAULT PRIVILEGES IN SCHEMA lumi_analytics FOR ROLE lumiadmin
GRANT USAGE, SELECT ON SEQUENCES TO lumiuser;

-- ----------------------------------------------------------------------------
-- 完成初始化
-- ----------------------------------------------------------------------------
\echo '数据库表结构初始化完成'
