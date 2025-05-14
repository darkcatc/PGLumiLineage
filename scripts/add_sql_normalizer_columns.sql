-- 为SQL规范化器添加必要的列
-- 作者: Vance Chen

-- 为 objects_metadata 表添加 normalized_sql_hash 列
ALTER TABLE lumi_metadata_store.objects_metadata 
ADD COLUMN IF NOT EXISTS normalized_sql_hash TEXT;

-- 添加列注释
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.normalized_sql_hash 
IS 'SQL规范化后的哈希值，用于标识相似的SQL模式，引用lumi_analytics.sql_patterns表的sql_hash列';

-- 为 functions_metadata 表添加 normalized_sql_hash 列
ALTER TABLE lumi_metadata_store.functions_metadata 
ADD COLUMN IF NOT EXISTS normalized_sql_hash TEXT;

-- 添加列注释
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.normalized_sql_hash 
IS 'SQL规范化后的哈希值，用于标识相似的SQL模式，引用lumi_analytics.sql_patterns表的sql_hash列';

-- 为 functions_metadata 表添加 parameter_types 列
ALTER TABLE lumi_metadata_store.functions_metadata 
ADD COLUMN IF NOT EXISTS parameter_types TEXT[];

-- 添加列注释
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.parameter_types 
IS '函数参数类型列表';
