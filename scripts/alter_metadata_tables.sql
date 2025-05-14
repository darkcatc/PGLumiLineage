-- 为元数据表添加 database_name 列
-- 作者: Vance Chen

-- 为 objects_metadata 表添加 database_name 列
ALTER TABLE lumi_metadata_store.objects_metadata 
ADD COLUMN IF NOT EXISTS database_name TEXT;

-- 设置默认值（临时）
UPDATE lumi_metadata_store.objects_metadata 
SET database_name = 'default_db' 
WHERE database_name IS NULL;

-- 设置为非空约束
ALTER TABLE lumi_metadata_store.objects_metadata 
ALTER COLUMN database_name SET NOT NULL;

-- 添加列注释
COMMENT ON COLUMN lumi_metadata_store.objects_metadata.database_name 
IS '数据库名称，用于区分不同数据库中的同名对象';

-- 删除旧的唯一约束
ALTER TABLE lumi_metadata_store.objects_metadata 
DROP CONSTRAINT IF EXISTS uq_object_identity;

-- 添加新的唯一约束，包含 database_name 列
ALTER TABLE lumi_metadata_store.objects_metadata 
ADD CONSTRAINT uq_object_identity 
UNIQUE (source_id, database_name, schema_name, object_name, object_type);

-- 为 functions_metadata 表添加 database_name 列
ALTER TABLE lumi_metadata_store.functions_metadata 
ADD COLUMN IF NOT EXISTS database_name TEXT;

-- 设置默认值（临时）
UPDATE lumi_metadata_store.functions_metadata 
SET database_name = 'default_db' 
WHERE database_name IS NULL;

-- 设置为非空约束
ALTER TABLE lumi_metadata_store.functions_metadata 
ALTER COLUMN database_name SET NOT NULL;

-- 添加列注释
COMMENT ON COLUMN lumi_metadata_store.functions_metadata.database_name 
IS '数据库名称，用于区分不同数据库中的同名函数';

-- 删除旧的唯一约束
ALTER TABLE lumi_metadata_store.functions_metadata 
DROP CONSTRAINT IF EXISTS uq_function_identity;

-- 添加新的唯一约束，包含 database_name 列
ALTER TABLE lumi_metadata_store.functions_metadata 
ADD CONSTRAINT uq_function_identity 
UNIQUE (source_id, database_name, schema_name, function_name, function_type);
