--##############################################################################
--# Schema for PGLumiLineage Metadata Store
--##############################################################################
--
--# 该模式用于存储被监控数据源的技术元数据信息
--# 包括表、视图、函数等对象的元数据
--# 这些元数据将用于构建数据血缘关系

-- 安装必要的扩展
CREATE EXTENSION IF NOT EXISTS pg_trgm;

CREATE SCHEMA IF NOT EXISTS lumi_metadata_store;

COMMENT ON SCHEMA lumi_metadata_store IS '存储被监控数据源的技术元数据信息，用于构建数据血缘关系。';

--##############################################################################
--# Table: lumi_metadata_store.objects_metadata
--# 存储数据库中的对象，如表、视图、物化视图等。
--##############################################################################
CREATE TABLE IF NOT EXISTS lumi_metadata_store.objects_metadata (
    object_id BIGSERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL, -- 来自哪个数据源，对应 lumi_config.data_sources 表的 source_id
    schema_name TEXT NOT NULL,
    object_name TEXT NOT NULL,
    object_type TEXT NOT NULL CHECK (object_type IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW', 'INDEX', 'SEQUENCE', 'FOREIGN TABLE')), -- 对象类型
    owner TEXT, -- 对象所有者
    description TEXT, -- 对象描述 (从 COMMENT ON ... 获取)
    definition TEXT, -- 对于视图、物化视图，这里存放其定义SQL；对于表，可为空或存放额外信息
    row_count BIGINT, -- 表的行数估算 (从 pg_class.reltuples 获取)
    last_ddl_time TIMESTAMPTZ, -- 最后 DDL 时间 (可能较难获取，某些数据库支持，PG 中可从事件触发器或审计日志间接获取)
    last_analyzed TIMESTAMPTZ, -- PG 中的 pg_stat_all_tables.last_analyze / last_autoanalyze
    properties JSONB, -- 存储其他特定于对象类型的属性
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL, -- 应用层更新

    CONSTRAINT uq_object_identity UNIQUE (source_id, schema_name, object_name, object_type)
);

-- 添加索引以加快查询
CREATE INDEX IF NOT EXISTS idx_objects_metadata_source_id ON lumi_metadata_store.objects_metadata(source_id);
CREATE INDEX IF NOT EXISTS idx_objects_metadata_schema_name ON lumi_metadata_store.objects_metadata(schema_name);
CREATE INDEX IF NOT EXISTS idx_objects_metadata_object_type ON lumi_metadata_store.objects_metadata(object_type);
CREATE INDEX IF NOT EXISTS idx_objects_metadata_object_name_pattern ON lumi_metadata_store.objects_metadata USING gin(object_name gin_trgm_ops);

-- 表注释
COMMENT ON TABLE lumi_metadata_store.objects_metadata IS '存储被监控数据源中的数据库对象元数据，如表、视图、物化视图等。';

-- 列注释
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.object_id IS '对象唯一标识符，自增主键';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.source_id IS '数据源ID，关联到lumi_config.data_sources表';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.schema_name IS '对象所属的schema名称';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.object_name IS '对象名称';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.object_type IS '对象类型，如TABLE、VIEW、MATERIALIZED VIEW等';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.owner IS '对象的所有者（数据库用户）';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.description IS '对象的描述，从数据库COMMENT中提取';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.definition IS '对象的定义SQL，主要用于视图和物化视图';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.row_count IS '表的估计行数，从统计信息中获取';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.last_ddl_time IS '最后一次DDL操作的时间';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.last_analyzed IS '最后一次分析统计信息的时间';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.properties IS '其他对象属性，以JSONB格式存储';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.created_at IS '记录创建时间';
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.updated_at IS '记录更新时间';

--##############################################################################
--# Table: lumi_metadata_store.columns_metadata
--# 存储表和视图的列信息
--##############################################################################
CREATE TABLE IF NOT EXISTS lumi_metadata_store.columns_metadata (
    column_id BIGSERIAL PRIMARY KEY,
    object_id BIGINT NOT NULL, -- 关联到 objects_metadata 表的 object_id
    column_name TEXT NOT NULL,
    ordinal_position INTEGER NOT NULL, -- 列的顺序
    data_type TEXT NOT NULL, -- 例如 INTEGER, VARCHAR(255), TIMESTAMPTZ
    max_length INTEGER, -- 对于字符类型
    numeric_precision INTEGER, -- 数值类型的精度
    numeric_scale INTEGER, -- 数值类型的小数位数
    is_nullable BOOLEAN NOT NULL, -- 是否允许为空
    default_value TEXT, -- 默认值
    is_primary_key BOOLEAN DEFAULT FALSE, -- 是否为主键列
    is_unique BOOLEAN DEFAULT FALSE, -- 是否有唯一约束(简化，实际唯一约束可能跨多列)
    foreign_key_to_table_schema TEXT, -- 被引用的外键表schema
    foreign_key_to_table_name TEXT,   -- 被引用的外键表名
    foreign_key_to_column_name TEXT,  -- 被引用的外键列名
    description TEXT, -- 列描述 (从 COMMENT ON ... 获取)
    properties JSONB, -- 其他列属性
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL, -- 应用层更新

    CONSTRAINT uq_column_identity UNIQUE (object_id, column_name)
);

-- 添加索引以加快查询
CREATE INDEX IF NOT EXISTS idx_columns_metadata_object_id ON lumi_metadata_store.columns_metadata(object_id);
CREATE INDEX IF NOT EXISTS idx_columns_metadata_column_name ON lumi_metadata_store.columns_metadata(column_name);
CREATE INDEX IF NOT EXISTS idx_columns_metadata_data_type ON lumi_metadata_store.columns_metadata(data_type);
CREATE INDEX IF NOT EXISTS idx_columns_metadata_is_primary_key ON lumi_metadata_store.columns_metadata(is_primary_key) WHERE is_primary_key = TRUE;
CREATE INDEX IF NOT EXISTS idx_columns_metadata_is_unique ON lumi_metadata_store.columns_metadata(is_unique) WHERE is_unique = TRUE;
CREATE INDEX IF NOT EXISTS idx_columns_metadata_foreign_key ON lumi_metadata_store.columns_metadata(foreign_key_to_table_schema, foreign_key_to_table_name) 
    WHERE foreign_key_to_table_schema IS NOT NULL AND foreign_key_to_table_name IS NOT NULL;

-- 表注释
COMMENT ON TABLE lumi_metadata_store.columns_metadata IS '存储表和视图的列元数据信息，用于数据血缘分析。';

-- 列注释
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.column_id IS '列唯一标识符，自增主键';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.object_id IS '关联到objects_metadata表的外键';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.column_name IS '列名称';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.ordinal_position IS '列在表中的顺序位置';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.data_type IS '列的数据类型';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.max_length IS '字符类型的最大长度';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.numeric_precision IS '数值类型的精度';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.numeric_scale IS '数值类型的小数位数';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.is_nullable IS '是否允许为空';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.default_value IS '列的默认值';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.is_primary_key IS '是否为主键列';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.is_unique IS '是否有唯一约束';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.foreign_key_to_table_schema IS '外键引用的表所在的schema';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.foreign_key_to_table_name IS '外键引用的表名';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.foreign_key_to_column_name IS '外键引用的列名';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.description IS '列的描述，从数据库COMMENT中提取';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.properties IS '其他列属性，以JSONB格式存储';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.created_at IS '记录创建时间';
COMMENT ON COLUMN lumi_metadata_store.columns_metadata.updated_at IS '记录更新时间';

--##############################################################################
--# Table: lumi_metadata_store.functions_metadata
--# 存储用户自定义函数和过程的元数据
--##############################################################################
CREATE TABLE IF NOT EXISTS lumi_metadata_store.functions_metadata (
    function_id BIGSERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL, -- 来自哪个数据源，对应 lumi_config.data_sources 表的 source_id
    schema_name TEXT NOT NULL,
    function_name TEXT NOT NULL,
    function_type TEXT NOT NULL CHECK (function_type IN ('FUNCTION', 'PROCEDURE', 'AGGREGATE', 'WINDOW')), -- 函数类型
    return_type TEXT, -- 返回类型
    parameters JSONB, -- 参数列表，包含名称、类型、默认值等
    definition TEXT, -- 函数定义代码
    language TEXT, -- 实现语言，如 SQL, PLPGSQL, PYTHON 等
    owner TEXT, -- 函数所有者
    description TEXT, -- 函数描述 (从 COMMENT ON ... 获取)
    properties JSONB, -- 其他函数属性
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL, -- 应用层更新

    CONSTRAINT uq_function_identity UNIQUE (source_id, schema_name, function_name, function_type)
);

-- 添加索引以加快查询
CREATE INDEX IF NOT EXISTS idx_functions_metadata_source_id ON lumi_metadata_store.functions_metadata(source_id);
CREATE INDEX IF NOT EXISTS idx_functions_metadata_schema_name ON lumi_metadata_store.functions_metadata(schema_name);
CREATE INDEX IF NOT EXISTS idx_functions_metadata_function_type ON lumi_metadata_store.functions_metadata(function_type);
CREATE INDEX IF NOT EXISTS idx_functions_metadata_function_name_pattern ON lumi_metadata_store.functions_metadata USING gin(function_name gin_trgm_ops);
CREATE INDEX IF NOT EXISTS idx_functions_metadata_language ON lumi_metadata_store.functions_metadata(language);

-- 表注释
COMMENT ON TABLE lumi_metadata_store.functions_metadata IS '存储用户自定义函数和存储过程的元数据信息，用于数据血缘分析。';

-- 列注释
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.function_id IS '函数唯一标识符，自增主键';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.source_id IS '数据源ID，关联到lumi_config.data_sources表';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.schema_name IS '函数所属的schema名称';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.function_name IS '函数名称';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.function_type IS '函数类型，如FUNCTION、PROCEDURE、AGGREGATE等';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.return_type IS '函数返回值类型';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.parameters IS '函数参数列表，以JSONB格式存储';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.definition IS '函数的定义SQL代码';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.language IS '函数实现语言，如SQL、PLPGSQL、Python等';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.owner IS '函数的所有者（数据库用户）';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.description IS '函数的描述，从数据库COMMENT中提取';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.properties IS '其他函数属性，以JSONB格式存储';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.created_at IS '记录创建时间';
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.updated_at IS '记录更新时间';

--注意: 表和字段的血缘分析和关系将使用 Apache AGE 图数据库来保存
--因此，我们不在这里创建 table_relationships 和 sql_queries 表

--##############################################################################
--# Table: lumi_metadata_store.metadata_sync_status
--# 跟踪元数据提取的同步状态
--##############################################################################
CREATE TABLE IF NOT EXISTS lumi_metadata_store.metadata_sync_status (
    sync_id BIGSERIAL PRIMARY KEY,
    source_id INTEGER NOT NULL, -- 数据源ID，对应 lumi_config.data_sources 表的 source_id
    object_type TEXT NOT NULL, -- 同步的对象类型（TABLES, COLUMNS, FUNCTIONS, RELATIONSHIPS, QUERIES）
    sync_start_time TIMESTAMPTZ NOT NULL, -- 同步开始时间
    sync_end_time TIMESTAMPTZ, -- 同步结束时间
    sync_status TEXT NOT NULL CHECK (sync_status IN ('RUNNING', 'COMPLETED', 'FAILED', 'PARTIAL')), -- 同步状态
    items_processed INTEGER DEFAULT 0, -- 处理的项目数
    items_succeeded INTEGER DEFAULT 0, -- 成功处理的项目数
    items_failed INTEGER DEFAULT 0, -- 处理失败的项目数
    error_details TEXT, -- 错误详情（如果有）
    sync_details JSONB, -- 同步详情，如范围、过滤条件等
    created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP NOT NULL,
    
    CONSTRAINT uq_current_sync UNIQUE (source_id, object_type, sync_start_time)
);

-- 添加索引以加快查询
CREATE INDEX IF NOT EXISTS idx_metadata_sync_status_source_id ON lumi_metadata_store.metadata_sync_status(source_id);
CREATE INDEX IF NOT EXISTS idx_metadata_sync_status_object_type ON lumi_metadata_store.metadata_sync_status(object_type);
CREATE INDEX IF NOT EXISTS idx_metadata_sync_status_sync_status ON lumi_metadata_store.metadata_sync_status(sync_status);
CREATE INDEX IF NOT EXISTS idx_metadata_sync_status_sync_time ON lumi_metadata_store.metadata_sync_status(sync_start_time, sync_end_time);

-- 表注释
COMMENT ON TABLE lumi_metadata_store.metadata_sync_status IS '跟踪元数据提取的同步状态和进度';

-- 列注释
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.sync_id IS '同步任务唯一标识符，自增主键';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.source_id IS '数据源ID，关联到lumi_config.data_sources表';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.object_type IS '同步的对象类型，如TABLES、COLUMNS、FUNCTIONS等';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.sync_start_time IS '同步开始的时间';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.sync_end_time IS '同步结束的时间';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.sync_status IS '同步状态，如RUNNING、COMPLETED、FAILED等';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.items_processed IS '处理的项目总数';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.items_succeeded IS '成功处理的项目数';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.items_failed IS '处理失败的项目数';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.error_details IS '错误详情，如果同步过程中出现错误';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.sync_details IS '同步详情，以JSONB格式存储额外信息';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.created_at IS '记录创建时间';
COMMENT ON COLUMN lumi_metadata_store.metadata_sync_status.updated_at IS '记录更新时间';

-- 注意: 不使用触发器自动更新updated_at字段
-- 应用层需要负责在更新数据时同时更新updated_at字段
-- 例如: UPDATE table SET column1 = value1, updated_at = CURRENT_TIMESTAMP WHERE ...

-- ----------------------------------------------------------------------------
-- 设置权限
-- ----------------------------------------------------------------------------

-- 将 lumi_metadata_store schema 的所有者设置为 lumiadmin
ALTER SCHEMA lumi_metadata_store OWNER TO lumiadmin;

-- 将 lumi_metadata_store schema 中的所有表的所有者设置为 lumiadmin
DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN 
        SELECT tablename FROM pg_tables WHERE schemaname = 'lumi_metadata_store'
    LOOP
        EXECUTE format('ALTER TABLE lumi_metadata_store.%I OWNER TO lumiadmin', obj.tablename);
    END LOOP;
END $$;

-- 将 lumi_metadata_store schema 中的所有序列的所有者设置为 lumiadmin
DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN 
        SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'lumi_metadata_store'
    LOOP
        EXECUTE format('ALTER SEQUENCE lumi_metadata_store.%I OWNER TO lumiadmin', obj.sequence_name);
    END LOOP;
END $$;

-- 将 lumi_metadata_store schema 中的所有索引的所有者设置为 lumiadmin
DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN 
        SELECT indexname FROM pg_indexes WHERE schemaname = 'lumi_metadata_store'
    LOOP
        EXECUTE format('ALTER INDEX lumi_metadata_store.%I OWNER TO lumiadmin', obj.indexname);
    END LOOP;
END $$;

-- 授予 lumiuser 对 lumi_metadata_store schema 的使用权限
GRANT USAGE ON SCHEMA lumi_metadata_store TO lumiuser;

-- 授予 lumiuser 对 lumi_metadata_store schema 中所有表的权限
DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN 
        SELECT tablename FROM pg_tables WHERE schemaname = 'lumi_metadata_store'
    LOOP
        EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE lumi_metadata_store.%I TO lumiuser', obj.tablename);
    END LOOP;
END $$;

-- 授予 lumiuser 对 lumi_metadata_store schema 中所有序列的权限
DO $$
DECLARE
    obj RECORD;
BEGIN
    FOR obj IN 
        SELECT sequence_name FROM information_schema.sequences WHERE sequence_schema = 'lumi_metadata_store'
    LOOP
        EXECUTE format('GRANT USAGE, SELECT, UPDATE ON SEQUENCE lumi_metadata_store.%I TO lumiuser', obj.sequence_name);
    END LOOP;
END $$;

-- 为 lumi_metadata_store schema 设置默认权限
ALTER DEFAULT PRIVILEGES IN SCHEMA lumi_metadata_store FOR ROLE lumiadmin
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO lumiuser;

ALTER DEFAULT PRIVILEGES IN SCHEMA lumi_metadata_store FOR ROLE lumiadmin
GRANT USAGE, SELECT ON SEQUENCES TO lumiuser;
