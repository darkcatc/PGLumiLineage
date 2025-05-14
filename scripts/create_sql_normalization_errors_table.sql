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
