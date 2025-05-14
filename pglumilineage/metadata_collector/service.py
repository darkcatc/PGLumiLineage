#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
元数据收集服务模块

该模块负责连接到 lumi_config.data_sources 中配置的 PostgreSQL 数据源，
并提取其技术元数据，存入 iwdb.lumi_metadata_store 下的表中。

此模块还负责：
1. 从 lumi_config.source_sync_schedules 表获取元数据同步调度规则
2. 根据调度规则执行元数据收集
3. 更新同步状态
4. 提供批量处理和缓存机制以优化性能

作者: Vance Chen
"""

import asyncio
import json
import logging
import functools
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional, Tuple, Set, Union, cast

import asyncpg
from pydantic import BaseModel

from pglumilineage.common import config, db_utils, models

# 配置日志记录器
logger = logging.getLogger(__name__)

# 缓存设置
SCHEDULES_CACHE_TTL = 300  # 调度规则缓存有效期（秒）
_schedules_cache: Dict[str, Any] = {
    "data": None,
    "timestamp": None
}

# 批处理设置
BATCH_SIZE = 100  # 批量处理的大小


# 元数据模型定义
class ObjectMetadata(BaseModel):
    """表示数据库对象（表、视图等）的元数据"""
    object_id: Optional[int] = None  # 数据库自动生成
    source_id: int
    database_name: str = 'default_db'  # 数据库名称，默认为'default_db'
    schema_name: str
    object_name: str
    object_type: str
    owner: Optional[str] = None
    description: Optional[str] = None
    definition: Optional[str] = None
    row_count: Optional[int] = None
    last_ddl_time: Optional[datetime] = None
    last_analyzed: Optional[datetime] = None
    properties: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class ColumnMetadata(BaseModel):
    """表示数据库列的元数据"""
    column_id: Optional[int] = None  # 数据库自动生成
    object_id: int
    column_name: str
    ordinal_position: int
    data_type: str
    max_length: Optional[int] = None
    numeric_precision: Optional[int] = None
    numeric_scale: Optional[int] = None
    is_nullable: bool
    default_value: Optional[str] = None
    is_primary_key: Optional[bool] = False
    is_unique: Optional[bool] = False
    foreign_key_to_table_schema: Optional[str] = None
    foreign_key_to_table_name: Optional[str] = None
    foreign_key_to_column_name: Optional[str] = None
    description: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class FunctionMetadata(BaseModel):
    """表示数据库函数的元数据"""
    function_id: Optional[int] = None  # 数据库自动生成
    source_id: int
    database_name: str = 'default_db'  # 数据库名称，默认为'default_db'
    schema_name: str
    function_name: str
    function_type: str
    return_type: Optional[str] = None
    parameters: Optional[List[Dict[str, Any]]] = None  # 修改为列表类型，每个元素是一个参数字典
    definition: Optional[str] = None
    language: Optional[str] = None
    owner: Optional[str] = None
    description: Optional[str] = None
    properties: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class MetadataSyncStatus(BaseModel):
    """表示元数据同步状态"""
    sync_id: Optional[int] = None  # 数据库自动生成
    source_id: int
    object_type: str
    sync_start_time: datetime
    sync_end_time: Optional[datetime] = None
    sync_status: str
    items_processed: int = 0
    items_succeeded: int = 0
    items_failed: int = 0
    error_details: Optional[str] = None
    sync_details: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


async def get_source_db_connection(source_config: models.DataSourceConfig) -> asyncpg.Connection:
    """
    建立与源数据库的连接
    
    Args:
        source_config: 数据源配置
        
    Returns:
        asyncpg.Connection: 数据库连接对象
    """
    logger.info(f"正在连接到数据源: {source_config.source_name} ({source_config.host}:{source_config.port}/{source_config.database})")
    
    try:
        conn = await asyncpg.connect(
            user=source_config.username,
            password=source_config.password.get_secret_value(),
            host=source_config.host,
            port=source_config.port,
            database=source_config.database
        )
        logger.info(f"成功连接到数据源: {source_config.source_name}")
        return conn
    except Exception as e:
        logger.error(f"连接数据源 {source_config.source_name} 失败: {str(e)}")
        raise


async def fetch_objects_metadata(conn: asyncpg.Connection, source_config: models.DataSourceConfig) -> List[ObjectMetadata]:
    """
    获取数据库对象（表、视图等）的元数据
    
    Args:
        conn: 源数据库连接
        source_config: 数据源配置
        
    Returns:
        List[ObjectMetadata]: 对象元数据列表
    """
    logger.info(f"正在从数据源 {source_config.source_name} 获取对象元数据")
    
    # 使用数据源配置中的数据库名称
    db_name = source_config.database
    logger.info(f"使用数据源配置中的数据库名称: {db_name}")
    
    # 如果数据源配置中没有数据库名称，则使用默认值
    if not db_name:
        logger.warning(f"数据源配置中没有数据库名称，使用默认值 'default_db'")
        db_name = 'default_db'
        db_name = 'default_db'
    
    # 查询获取表和视图的元数据
    query = """
    WITH tables_info AS (
        -- 从 information_schema.tables 获取表信息
        SELECT 
            t.table_schema AS schema_name,
            t.table_name AS object_name,
            'TABLE' AS object_type,
            -- 从 pg_tables 获取表所有者
            (SELECT tableowner FROM pg_catalog.pg_tables 
             WHERE schemaname = t.table_schema AND tablename = t.table_name) AS owner,
            -- 获取表的OID以用于查询描述和行数
            (SELECT c.oid FROM pg_catalog.pg_class c 
             JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid 
             WHERE n.nspname = t.table_schema AND c.relname = t.table_name) AS oid,
            NULL AS view_definition
        FROM information_schema.tables t
        WHERE t.table_type = 'BASE TABLE'
          AND t.table_schema NOT IN ('pg_catalog', 'information_schema')
          AND t.table_schema NOT LIKE 'pg_toast%'
          AND t.table_schema NOT LIKE 'pg_temp%'
    ),
    views_info AS (
        -- 从 information_schema.views 获取视图信息
        SELECT 
            v.table_schema AS schema_name,
            v.table_name AS object_name,
            'VIEW' AS object_type,
            -- 从 pg_views 获取视图所有者
            (SELECT viewowner FROM pg_catalog.pg_views 
             WHERE schemaname = v.table_schema AND viewname = v.table_name) AS owner,
            -- 获取视图的OID以用于查询描述
            (SELECT c.oid FROM pg_catalog.pg_class c 
             JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid 
             WHERE n.nspname = v.table_schema AND c.relname = v.table_name) AS oid,
            v.view_definition
        FROM information_schema.views v
        WHERE v.table_schema NOT IN ('pg_catalog', 'information_schema')
          AND v.table_schema NOT LIKE 'pg_toast%'
          AND v.table_schema NOT LIKE 'pg_temp%'
    ),
    combined_objects AS (
        -- 合并表和视图信息
        SELECT * FROM tables_info
        UNION ALL
        SELECT * FROM views_info
    )
    SELECT 
        co.schema_name,
        co.object_name,
        co.object_type,
        co.owner,
        -- 从 pg_description 获取对象描述
        pg_catalog.obj_description(co.oid, 'pg_class') AS description,
        co.view_definition,
        -- 仅对表获取行数估计
        CASE WHEN co.object_type = 'TABLE' THEN 
            (SELECT c.reltuples::bigint FROM pg_catalog.pg_class c WHERE c.oid = co.oid)
        ELSE 0 END AS row_count,
        NULL AS last_ddl_time,
        -- 获取最后分析时间
        CASE WHEN co.object_type = 'TABLE' THEN 
            GREATEST(
                pg_catalog.pg_stat_get_last_analyze_time(co.oid),
                pg_catalog.pg_stat_get_last_autoanalyze_time(co.oid)
            )
        ELSE NULL END AS last_analyzed,
        -- 构建对象属性
        CASE 
            WHEN co.object_type = 'TABLE' THEN (
                SELECT jsonb_build_object(
                    'has_primary_key', (SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid = co.oid AND contype = 'p')),
                    'has_foreign_keys', (SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_constraint WHERE conrelid = co.oid AND contype = 'f')),
                    'has_indexes', (SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_index WHERE indrelid = co.oid)),
                    'is_partitioned', (SELECT c.relispartition FROM pg_catalog.pg_class c WHERE c.oid = co.oid),
                    'table_size', pg_catalog.pg_total_relation_size(co.oid),
                    'table_size_pretty', pg_catalog.pg_size_pretty(pg_catalog.pg_total_relation_size(co.oid))
                )
            )
            WHEN co.object_type = 'VIEW' THEN (
                SELECT jsonb_build_object(
                    'is_updatable', (SELECT is_updatable FROM information_schema.views WHERE table_schema = co.schema_name AND table_name = co.object_name),
                    'is_insertable_into', (SELECT is_insertable_into FROM information_schema.views WHERE table_schema = co.schema_name AND table_name = co.object_name),
                    'is_trigger_updatable', (SELECT is_trigger_updatable FROM information_schema.views WHERE table_schema = co.schema_name AND table_name = co.object_name),
                    'is_trigger_deletable', (SELECT is_trigger_deletable FROM information_schema.views WHERE table_schema = co.schema_name AND table_name = co.object_name),
                    'is_trigger_insertable_into', (SELECT is_trigger_insertable_into FROM information_schema.views WHERE table_schema = co.schema_name AND table_name = co.object_name)
                )
            )
            ELSE '{}'::jsonb
        END AS properties
    FROM combined_objects co
    ORDER BY co.schema_name, co.object_name
    """
    
    try:
        rows = await conn.fetch(query)
        
        result = []
        for row in rows:
            obj_metadata = ObjectMetadata(
                source_id=source_config.source_id,
                database_name=db_name,  # 添加数据库名称
                schema_name=row['schema_name'],
                object_name=row['object_name'],
                object_type=row['object_type'],
                owner=row['owner'],
                description=row['description'],
                definition=row['view_definition'],
                row_count=row['row_count'],
                last_ddl_time=row['last_ddl_time'],
                last_analyzed=row['last_analyzed'],
                properties={}
            )
            result.append(obj_metadata)
        
        logger.info(f"从数据源 {source_config.source_name} 获取到 {len(result)} 个对象元数据")
        return result
    except Exception as e:
        logger.error(f"获取对象元数据失败: {str(e)}")
        raise


async def fetch_columns_metadata(conn: asyncpg.Connection, object_id: int, schema_name: str, object_name: str, source_config: models.DataSourceConfig) -> List[ColumnMetadata]:
    """
    获取指定对象的列元数据
    
    Args:
        conn: 源数据库连接
        object_id: 对象ID
        schema_name: 模式名称
        object_name: 对象名称
        source_config: 数据源配置
        
    Returns:
        List[ColumnMetadata]: 列元数据列表
    """
    logger.info(f"正在获取 {schema_name}.{object_name} 的列元数据")
    
    # 查询获取列的元数据
    query = """
    WITH column_info AS (
        -- 从 information_schema.columns 获取基本列信息
        SELECT
            c.table_schema,
            c.table_name,
            c.column_name,
            c.ordinal_position,
            c.data_type,
            c.character_maximum_length AS max_length,
            c.numeric_precision,
            c.numeric_scale,
            CASE WHEN c.is_nullable = 'YES' THEN TRUE ELSE FALSE END AS is_nullable,
            c.column_default AS default_value,
            -- 从 pg_catalog 获取更多信息
            (SELECT a.attnum FROM pg_catalog.pg_attribute a
             JOIN pg_catalog.pg_class cl ON a.attrelid = cl.oid
             JOIN pg_catalog.pg_namespace n ON cl.relnamespace = n.oid
             WHERE n.nspname = c.table_schema AND cl.relname = c.table_name AND a.attname = c.column_name) AS attnum,
            (SELECT cl.oid FROM pg_catalog.pg_class cl
             JOIN pg_catalog.pg_namespace n ON cl.relnamespace = n.oid
             WHERE n.nspname = c.table_schema AND cl.relname = c.table_name) AS reloid
        FROM information_schema.columns c
        WHERE c.table_schema = $1 AND c.table_name = $2
    ),
    primary_keys AS (
        -- 获取主键信息
        SELECT
            kcu.table_schema,
            kcu.table_name,
            kcu.column_name,
            TRUE AS is_primary_key
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.table_constraints tc ON
            kcu.constraint_name = tc.constraint_name AND
            kcu.table_schema = tc.table_schema AND
            kcu.table_name = tc.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND kcu.table_schema = $1 AND kcu.table_name = $2
    ),
    unique_constraints AS (
        -- 获取唯一约束信息
        SELECT
            kcu.table_schema,
            kcu.table_name,
            kcu.column_name,
            TRUE AS is_unique
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.table_constraints tc ON
            kcu.constraint_name = tc.constraint_name AND
            kcu.table_schema = tc.table_schema AND
            kcu.table_name = tc.table_name
        WHERE tc.constraint_type = 'UNIQUE'
          AND kcu.table_schema = $1 AND kcu.table_name = $2
    ),
    foreign_keys AS (
        -- 获取外键信息
        SELECT
            kcu.table_schema,
            kcu.table_name,
            kcu.column_name,
            ccu.table_schema AS foreign_key_to_table_schema,
            ccu.table_name AS foreign_key_to_table_name,
            ccu.column_name AS foreign_key_to_column_name
        FROM information_schema.key_column_usage kcu
        JOIN information_schema.referential_constraints rc ON
            kcu.constraint_name = rc.constraint_name AND
            kcu.table_schema = rc.constraint_schema
        JOIN information_schema.constraint_column_usage ccu ON
            rc.unique_constraint_name = ccu.constraint_name AND
            rc.unique_constraint_schema = ccu.constraint_schema
        WHERE kcu.table_schema = $1 AND kcu.table_name = $2
    )
    SELECT
        ci.column_name,
        ci.ordinal_position,
        ci.data_type,
        ci.max_length,
        ci.numeric_precision,
        ci.numeric_scale,
        ci.is_nullable,
        ci.default_value,
        COALESCE(pk.is_primary_key, FALSE) AS is_primary_key,
        COALESCE(uc.is_unique, FALSE) AS is_unique,
        fk.foreign_key_to_table_schema,
        fk.foreign_key_to_table_name,
        fk.foreign_key_to_column_name,
        pg_catalog.col_description(ci.reloid, ci.attnum) AS description,
        -- 构建列的其他属性
        jsonb_build_object(
            'is_identity', (SELECT EXISTS(
                SELECT 1 FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = ci.reloid AND a.attname = ci.column_name AND a.attidentity != ''
            )),
            'is_generated', (SELECT EXISTS(
                SELECT 1 FROM pg_catalog.pg_attribute a
                WHERE a.attrelid = ci.reloid AND a.attname = ci.column_name AND a.attgenerated != ''
            )),
            'statistics_target', (SELECT attstattarget FROM pg_catalog.pg_attribute a
                                 WHERE a.attrelid = ci.reloid AND a.attname = ci.column_name),
            'has_default', (ci.default_value IS NOT NULL)
        ) AS properties
    FROM column_info ci
    LEFT JOIN primary_keys pk ON
        ci.table_schema = pk.table_schema AND
        ci.table_name = pk.table_name AND
        ci.column_name = pk.column_name
    LEFT JOIN unique_constraints uc ON
        ci.table_schema = uc.table_schema AND
        ci.table_name = uc.table_name AND
        ci.column_name = uc.column_name
    LEFT JOIN foreign_keys fk ON
        ci.table_schema = fk.table_schema AND
        ci.table_name = fk.table_name AND
        ci.column_name = fk.column_name
    ORDER BY ci.ordinal_position
    """
    
    try:
        rows = await conn.fetch(query, schema_name, object_name)
        
        result = []
        for row in rows:
            col_metadata = ColumnMetadata(
                object_id=object_id,
                column_name=row['column_name'],
                ordinal_position=row['ordinal_position'],
                data_type=row['data_type'],
                max_length=row['max_length'],
                numeric_precision=row['numeric_precision'],
                numeric_scale=row['numeric_scale'],
                is_nullable=row['is_nullable'],
                default_value=row['default_value'],
                is_primary_key=row['is_primary_key'],
                is_unique=row['is_unique'],
                foreign_key_to_table_schema=row['foreign_key_to_table_schema'],
                foreign_key_to_table_name=row['foreign_key_to_table_name'],
                foreign_key_to_column_name=row['foreign_key_to_column_name'],
                description=row['description'],
                properties={}
            )
            result.append(col_metadata)
        
        logger.info(f"获取到 {schema_name}.{object_name} 的 {len(result)} 个列元数据")
        return result
    except Exception as e:
        logger.error(f"获取 {schema_name}.{object_name} 的列元数据失败: {str(e)}")
        raise


async def fetch_functions_metadata(conn: asyncpg.Connection, source_config: models.DataSourceConfig) -> List[FunctionMetadata]:
    """
    获取数据库函数的元数据
    
    Args:
        conn: 源数据库连接
        source_config: 数据源配置
        
    Returns:
        List[FunctionMetadata]: 函数元数据列表
    """
    logger.info(f"正在从数据源 {source_config.source_name} 获取函数元数据")
    
    # 使用数据源配置中的数据库名称
    db_name = source_config.database
    logger.info(f"使用数据源配置中的数据库名称: {db_name}")
    
    # 如果数据源配置中没有数据库名称，则使用默认值
    if not db_name:
        logger.warning(f"数据源配置中没有数据库名称，使用默认值 'default_db'")
        db_name = 'default_db'
        db_name = 'default_db'
    
    # 查询获取函数的元数据
    query = """
    SELECT
        n.nspname AS schema_name,
        p.proname AS function_name,
        CASE p.prokind
            WHEN 'f' THEN 'FUNCTION'
            WHEN 'p' THEN 'PROCEDURE'
            WHEN 'a' THEN 'AGGREGATE'
            WHEN 'w' THEN 'WINDOW'
            ELSE p.prokind::text
        END AS function_type,
        pg_get_function_result(p.oid) AS return_type,
        pg_get_function_arguments(p.oid) AS arguments,
        -- 使用 COALESCE 处理可能的空值情况
        COALESCE(
            CASE WHEN p.prokind = 'a' THEN NULL ELSE pg_get_functiondef(p.oid) END,
            'NOT AVAILABLE'::text
        ) AS definition,
        l.lanname AS language,
        pg_get_userbyid(p.proowner) AS owner,
        obj_description(p.oid, 'pg_proc') AS description
    FROM pg_proc p
    JOIN pg_namespace n ON p.pronamespace = n.oid
    JOIN pg_language l ON p.prolang = l.oid
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND n.nspname NOT LIKE 'pg_temp%'
      AND n.nspname NOT LIKE 'pg_toast%'
    ORDER BY n.nspname, p.proname
    """
    
    try:
        rows = await conn.fetch(query)
        
        result = []
        for row in rows:
            # 将参数转换为JSON格式
            args_str = row['arguments']
            args_list = []
            if args_str:
                args_parts = args_str.split(',')
                for i, arg in enumerate(args_parts):
                    arg = arg.strip()
                    if arg:
                        parts = arg.split(' ')
                        if len(parts) >= 2:
                            arg_name = parts[0]
                            arg_type = ' '.join(parts[1:])
                            args_list.append({
                                "name": arg_name,
                                "type": arg_type,
                                "position": i + 1
                            })
            
            func_metadata = FunctionMetadata(
                source_id=source_config.source_id,
                database_name=db_name,  # 添加数据库名称
                schema_name=row['schema_name'],
                function_name=row['function_name'],
                function_type=row['function_type'],
                return_type=row['return_type'],
                parameters=args_list,
                definition=row['definition'],  # 修正列名，之前错误地使用了 'view_definition'
                language=row['language'],
                owner=row['owner'],
                description=row['description'],
                properties={}
            )
            result.append(func_metadata)
        
        logger.info(f"从数据源 {source_config.source_name} 获取到 {len(result)} 个函数元数据")
        return result
    except Exception as e:
        logger.error(f"获取函数元数据失败: {str(e)}")
        raise


async def save_objects_metadata(metadata_list: List[ObjectMetadata]) -> List[int]:
    """
    保存对象元数据到元数据存储
    
    Args:
        metadata_list: 对象元数据列表
        
    Returns:
        List[int]: 保存的对象ID列表
    """
    if not metadata_list:
        logger.warning("没有对象元数据需要保存")
        return []
    
    logger.info(f"正在保存 {len(metadata_list)} 个对象元数据")
    
    # 创建独立的数据库连接
    try:
        # 使用RAW_LOGS_DSN创建连接
        dsn = str(config.settings.RAW_LOGS_DSN)
        conn = await asyncpg.connect(dsn=dsn)
        
        try:
            object_ids = []
            
            # 使用事务来确保原子性
            async with conn.transaction():
                for metadata in metadata_list:
                    # 将 properties 转换为 JSON 格式
                    properties_json = json.dumps(metadata.properties) if metadata.properties else None
                    
                    # 使用 UPSERT 操作保存元数据
                    query = """
                    INSERT INTO lumi_metadata_store.objects_metadata (
                        source_id, database_name, schema_name, object_name, object_type,
                        owner, description, definition, row_count,
                        last_ddl_time, last_analyzed, properties,
                        created_at, updated_at
                    ) VALUES (
                        $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                        CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                    )
                    ON CONFLICT (source_id, database_name, schema_name, object_name, object_type)
                    DO UPDATE SET
                        object_type = $5,
                        owner = $6,
                        description = $7,
                        definition = $8,
                        row_count = $9,
                        last_ddl_time = $10,
                        last_analyzed = $11,
                        properties = $12,
                        updated_at = CURRENT_TIMESTAMP
                    RETURNING object_id
                    """
                    
                    result = await conn.fetchval(
                        query,
                        metadata.source_id,
                        metadata.database_name,  # 添加数据库名称参数
                        metadata.schema_name,
                        metadata.object_name,
                        metadata.object_type,
                        metadata.owner,
                        metadata.description,
                        metadata.definition,
                        metadata.row_count,
                        metadata.last_ddl_time,
                        metadata.last_analyzed,
                        properties_json
                    )
                    
                    object_ids.append(result)
            
            logger.info(f"成功保存 {len(object_ids)} 个对象元数据")
            return object_ids
        finally:
            # 关闭连接
            await conn.close()
    except Exception as e:
        logger.error(f"保存对象元数据失败: {str(e)}")
        raise



async def save_columns_metadata(metadata_list: List[ColumnMetadata]) -> List[int]:
    """
    保存列元数据到元数据存储
    
    Args:
        metadata_list: 列元数据列表
        
    Returns:
        List[int]: 保存的列ID列表
    """
    if not metadata_list:
        logger.warning("没有列元数据需要保存")
        return []
    
    logger.info(f"正在保存 {len(metadata_list)} 个列元数据")
    
    # 创建独立的数据库连接
    try:
        # 使用RAW_LOGS_DSN创建连接
        dsn = str(config.settings.RAW_LOGS_DSN)
        conn = await asyncpg.connect(dsn=dsn)
        
        try:
            column_ids = []
            
            for metadata in metadata_list:
                # 使用 UPSERT 操作保存元数据
                query = """
                INSERT INTO lumi_metadata_store.columns_metadata (
                    object_id, column_name, ordinal_position, data_type, max_length,
                    numeric_precision, numeric_scale, is_nullable, default_value,
                    is_primary_key, is_unique, foreign_key_to_table_schema,
                    foreign_key_to_table_name, foreign_key_to_column_name,
                    description, properties, created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                ON CONFLICT (object_id, column_name)
                DO UPDATE SET
                    ordinal_position = $3,
                    data_type = $4,
                    max_length = $5,
                    numeric_precision = $6,
                    numeric_scale = $7,
                    is_nullable = $8,
                    default_value = $9,
                    is_primary_key = $10,
                    is_unique = $11,
                    foreign_key_to_table_schema = $12,
                    foreign_key_to_table_name = $13,
                    foreign_key_to_column_name = $14,
                    description = $15,
                    properties = $16,
                    updated_at = CURRENT_TIMESTAMP
                RETURNING column_id
                """
                
                # 将 properties 转换为 JSON 格式
                properties_json = json.dumps(metadata.properties) if metadata.properties else None
                
                result = await conn.fetchval(
                    query,
                    metadata.object_id,
                    metadata.column_name,
                    metadata.ordinal_position,
                    metadata.data_type,
                    metadata.max_length,
                    metadata.numeric_precision,
                    metadata.numeric_scale,
                    metadata.is_nullable,
                    metadata.default_value,
                    metadata.is_primary_key,
                    metadata.is_unique,
                    metadata.foreign_key_to_table_schema,
                    metadata.foreign_key_to_table_name,
                    metadata.foreign_key_to_column_name,
                    metadata.description,
                    properties_json
                )
                
                column_ids.append(result)
            
            logger.info(f"成功保存 {len(column_ids)} 个列元数据")
            return column_ids
        finally:
            # 关闭连接
            await conn.close()
    except Exception as e:
        logger.error(f"保存列元数据失败: {str(e)}")
        raise



async def save_metadata_to_store(conn: asyncpg.Connection, query: str, params_list: List[Tuple]) -> List[int]:
    """
    通用函数，将元数据保存到元数据存储数据库
    
    Args:
        conn: 元数据存储数据库连接
        query: SQL查询语句
        params_list: 参数列表，每个元素是一组参数
        
    Returns:
        List[int]: 保存的记录ID列表
    """
    if not params_list:
        return []
    
    result_ids = []
    for params in params_list:
        result = await conn.fetchval(query, *params)
        result_ids.append(result)
    
    return result_ids


async def save_functions_metadata(metadata_list: List[FunctionMetadata]) -> List[int]:
    """
    保存函数元数据到元数据存储
    
    Args:
        metadata_list: 函数元数据列表
        
    Returns:
        List[int]: 保存的函数ID列表
    """
    if not metadata_list:
        logger.warning("没有函数元数据需要保存")
        return []
    
    logger.info(f"正在保存 {len(metadata_list)} 个函数元数据")
    
    # 使用元数据存储数据库连接池
    try:
        # 获取元数据存储数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 准备参数列表
            params_list = []
            
            for metadata in metadata_list:
                # 将 properties 转换为 JSON 格式
                properties_json = json.dumps(metadata.properties) if metadata.properties else None
                
                # 将参数转换为 JSON 格式
                parameters_json = json.dumps(metadata.parameters) if metadata.parameters else None
                
                params = (
                    metadata.source_id,
                    metadata.database_name,
                    metadata.schema_name,
                    metadata.function_name,
                    metadata.function_type,
                    parameters_json,
                    metadata.return_type,
                    metadata.language,
                    metadata.owner,
                    metadata.description,
                    metadata.definition,
                    properties_json
                )
                
                params_list.append(params)
            
            # 使用 UPSERT 操作保存元数据
            query = """
            INSERT INTO lumi_metadata_store.functions_metadata (
                source_id, database_name, schema_name, function_name, function_type,
                parameters, return_type, language, owner,
                description, definition, properties, created_at, updated_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12,
                CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
            )
            ON CONFLICT (source_id, database_name, schema_name, function_name, function_type)
            DO UPDATE SET
                parameters = $6,
                return_type = $7,
                language = $8,
                owner = $9,
                description = $10,
                definition = $11,
                properties = $12,
                updated_at = CURRENT_TIMESTAMP
            RETURNING function_id
            """
            
            # 保存元数据
            function_ids = await save_metadata_to_store(conn, query, params_list)
            
            logger.info(f"成功保存 {len(function_ids)} 个函数元数据")
            return function_ids
    except Exception as e:
        logger.error(f"保存函数元数据失败: {str(e)}")
        raise



async def update_sync_status(sync_status: MetadataSyncStatus) -> int:
    """
    更新元数据同步状态
    
    Args:
        sync_status: 同步状态对象
        
    Returns:
        int: 同步状态ID
    """
    logger.info(f"正在更新数据源 {sync_status.source_id} 的 {sync_status.object_type} 同步状态")
    
    # 创建独立的数据库连接
    try:
        # 使用RAW_LOGS_DSN创建连接
        dsn = str(config.settings.RAW_LOGS_DSN)
        conn = await asyncpg.connect(dsn=dsn)
        
        try:
            if sync_status.sync_id:
                # 更新现有同步状态
                query = """
                UPDATE lumi_metadata_store.metadata_sync_status SET
                    sync_end_time = $1,
                    sync_status = $2,
                    items_processed = $3,
                    items_succeeded = $4,
                    items_failed = $5,
                    error_details = $6,
                    sync_details = $7,
                    updated_at = CURRENT_TIMESTAMP
                WHERE sync_id = $8
                RETURNING sync_id
                """
                
                result = await conn.fetchval(
                    query,
                    sync_status.sync_end_time,
                    sync_status.sync_status,
                    sync_status.items_processed,
                    sync_status.items_succeeded,
                    sync_status.items_failed,
                    sync_status.error_details,
                    sync_status.sync_details,
                    sync_status.sync_id
                )
            else:
                # 创建新的同步状态
                query = """
                INSERT INTO lumi_metadata_store.metadata_sync_status (
                    source_id, object_type, sync_start_time, sync_end_time,
                    sync_status, items_processed, items_succeeded, items_failed,
                    error_details, sync_details, created_at, updated_at
                ) VALUES (
                    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10,
                    CURRENT_TIMESTAMP, CURRENT_TIMESTAMP
                )
                RETURNING sync_id
                """
                
                result = await conn.fetchval(
                    query,
                    sync_status.source_id,
                    sync_status.object_type,
                    sync_status.sync_start_time,
                    sync_status.sync_end_time,
                    sync_status.sync_status,
                    sync_status.items_processed,
                    sync_status.items_succeeded,
                    sync_status.items_failed,
                    sync_status.error_details,
                    sync_status.sync_details
                )
            
            logger.info(f"成功更新同步状态，ID: {result}")
            return result
        finally:
            # 关闭连接
            await conn.close()
    except Exception as e:
        logger.error(f"更新同步状态失败: {str(e)}")
        raise


@functools.lru_cache(maxsize=32)
async def get_metadata_sync_schedules_from_db() -> List[Dict[str, Any]]:
    """
    从数据库获取元数据同步调度规则
    
    Returns:
        List[Dict[str, Any]]: 调度规则列表
    """
    pool = await db_utils.get_db_pool()
    async with pool.acquire() as conn:
        query = """
        SELECT 
            s.schedule_id,
            s.source_id,
            s.is_schedule_active,
            s.sync_frequency_type,
            s.sync_interval_seconds,
            s.cron_expression,
            s.last_sync_attempt_at,
            s.last_sync_success_at,
            s.last_sync_status,
            s.last_sync_message,
            s.created_at,
            s.updated_at,
            d.source_name,
            d.db_host,
            d.db_port,
            d.db_name,
            d.db_user,
            d.db_password
        FROM lumi_config.source_sync_schedules s
        JOIN lumi_config.data_sources d ON s.source_id = d.source_id
        WHERE s.is_schedule_active = TRUE
        """
        
        rows = await conn.fetch(query)
        schedules = []
        
        for row in rows:
            # 将密码字符串转换为 SecretStr
            password_secret = models.SecretStr(row['db_password']) if row['db_password'] else models.SecretStr('')
            
            schedule = {
                'schedule_id': row['schedule_id'],
                'source_id': row['source_id'],
                'is_schedule_active': row['is_schedule_active'],
                'sync_frequency_type': row['sync_frequency_type'],
                'sync_interval_seconds': row['sync_interval_seconds'],
                'cron_expression': row['cron_expression'],
                'last_sync_attempt_at': row['last_sync_attempt_at'],
                'last_sync_success_at': row['last_sync_success_at'],
                'source_config': models.DataSourceConfig(
                    source_id=row['source_id'],
                    source_name=row['source_name'],
                    host=row['db_host'],
                    port=row['db_port'],
                    username=row['db_user'],
                    password=password_secret,
                    database=row['db_name']
                )
            }
            schedules.append(schedule)
        
        return schedules


async def get_metadata_sync_schedules() -> List[Dict[str, Any]]:
    """
    从 lumi_config.source_sync_schedules 表获取元数据同步调度规则
    使用缓存机制减少数据库查询

    Returns:
        List[Dict[str, Any]]: 调度规则列表
    """
    global _schedules_cache

    # 检查缓存是否有效
    now = datetime.now()
    if (_schedules_cache["data"] is not None and
        _schedules_cache["timestamp"] is not None and
        (now - _schedules_cache["timestamp"]).total_seconds() < SCHEDULES_CACHE_TTL):
        logger.debug("使用缓存的元数据同步调度规则")
        return _schedules_cache["data"]
    
    logger.info("获取元数据同步调度规则")
    
    # 从数据库获取调度规则
    schedules = await get_metadata_sync_schedules_from_db()
    
    # 更新缓存
    _schedules_cache["data"] = schedules
    _schedules_cache["timestamp"] = now
    
    return schedules


async def update_schedule_sync_status(schedule_id: int, success: bool, message: str = None) -> None:
    """
    更新调度规则的同步状态
    
    Args:
        schedule_id: 调度规则ID
        success: 同步是否成功
        message: 同步消息
    """
    logger.info(f"更新调度规则 {schedule_id} 的同步状态: {'SUCCESS' if success else 'FAILED'}")
    
    now = datetime.now(timezone.utc)
    status = "SUCCESS" if success else "FAILED"
    
    # 使用全局连接池而不是创建新连接
    pool = await db_utils.get_db_pool()
    async with pool.acquire() as conn:
        try:
            query = """
            UPDATE lumi_config.source_sync_schedules
            SET last_sync_attempt_at = $1, 
                last_sync_status = $2, 
                last_sync_message = $3,
                last_sync_success_at = CASE WHEN $4 THEN $1 ELSE last_sync_success_at END,
                updated_at = CURRENT_TIMESTAMP
            WHERE schedule_id = $5
            """
            
            await conn.execute(query, now, status, message, success, schedule_id)
            
            # 清除缓存，确保下次获取最新数据
            if hasattr(get_metadata_sync_schedules_from_db, 'cache_clear'):
                get_metadata_sync_schedules_from_db.cache_clear()
            
            # 同时清除内存缓存
            global _schedules_cache
            _schedules_cache["data"] = None
            _schedules_cache["timestamp"] = None
            
        except Exception as e:
            logger.error(f"更新调度规则同步状态时出错: {str(e)}")
            raise


async def calculate_next_run_time(sync_frequency_type: str, sync_interval_seconds: int, 
                               cron_expression: str, from_time: datetime) -> datetime:
    """
    根据调度类型计算下次运行时间
    
    Args:
        sync_frequency_type: 同步频率类型 ('interval', 'cron', 'manual')
        sync_interval_seconds: 同步间隔秒数
        cron_expression: cron 表达式
        from_time: 起始时间
        
    Returns:
        datetime: 下次运行时间
    """
    if sync_frequency_type == 'interval' and sync_interval_seconds:
        return from_time + timedelta(seconds=sync_interval_seconds)
    elif sync_frequency_type == 'cron' and cron_expression:
        # 注意: 实际实现中应使用 croniter 等库来处理 cron 表达式
        # 这里简化处理，返回一天后的时间
        logger.warning(f"Cron 表达式处理未实现，使用默认间隔 (1 天): {cron_expression}")
        return from_time + timedelta(days=1)
    elif sync_frequency_type == 'manual':
        # 手动模式下，返回远期时间
        return from_time + timedelta(days=365)
    else:
        # 默认情况，返回一天后的时间
        logger.warning(f"未知的同步频率类型: {sync_frequency_type}，使用默认间隔 (1 天)")
        return from_time + timedelta(days=1)


async def process_metadata_collection(interval_seconds: int = 86400, run_once: bool = False) -> None:
    """
    处理元数据收集
    
    Args:
        interval_seconds: 检查间隔时间（秒），默认为86400秒（1天）
        run_once: 是否只运行一次
    """
    logger.info(f"启动元数据收集服务，检查间隔: {interval_seconds}秒，{'单次运行' if run_once else '持续运行'}")
    
    try:
        while True:
            now = datetime.now(timezone.utc)
            logger.info(f"检查元数据同步调度规则，当前时间: {now}")
            
            # 获取调度规则
            schedules = await get_metadata_sync_schedules()
            logger.info(f"找到 {len(schedules)} 个启用的元数据同步调度规则")
            
            # 并行处理每个调度规则
            tasks = []
            for schedule in schedules:
                schedule_id = schedule['schedule_id']
                source_config = schedule['source_config']
                last_sync_success_at = schedule.get('last_sync_success_at')
                sync_frequency_type = schedule.get('sync_frequency_type', 'interval')
                sync_interval_seconds = schedule.get('sync_interval_seconds', 86400)
                cron_expression = schedule.get('cron_expression', '')
                
                # 如果有上次成功同步时间，计算下次应该同步的时间
                should_sync = True
                if last_sync_success_at:
                    next_run_time = await calculate_next_run_time(
                        sync_frequency_type, sync_interval_seconds, cron_expression, last_sync_success_at
                    )
                    should_sync = now >= next_run_time
                
                # 如果应该同步，创建异步任务执行元数据收集
                if should_sync:
                    task = asyncio.create_task(
                        process_single_source(schedule_id, source_config)
                    )
                    tasks.append(task)
                else:
                    next_run_time = await calculate_next_run_time(
                        sync_frequency_type, sync_interval_seconds, cron_expression, last_sync_success_at
                    )
                    logger.info(f"数据源 {source_config.source_name} 的元数据收集还不需要运行，下次运行时间: {next_run_time}")
            
            # 等待所有任务完成
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            
            # 如果只运行一次，则退出循环
            if run_once:
                logger.info("单次运行模式，元数据收集完成")
                break
            
            # 等待下一次检查
            logger.info(f"等待 {interval_seconds} 秒后进行下一次检查")
            await asyncio.sleep(interval_seconds)
    
    except asyncio.CancelledError:
        logger.info("元数据收集服务任务被取消")
        raise
    except Exception as e:
        logger.error(f"元数据收集服务出错: {str(e)}")
        raise


async def process_single_source(schedule_id: int, source_config: models.DataSourceConfig) -> None:
    """
    处理单个数据源的元数据收集
    
    Args:
        schedule_id: 调度规则ID
        source_config: 数据源配置
    """
    logger.info(f"开始执行数据源 {source_config.source_name} 的元数据收集")
    
    try:
        # 调用元数据收集服务
        success, error_message = await collect_metadata_for_source(source_config)
        
        # 更新调度规则的同步状态
        if success:
            logger.info(f"数据源 {source_config.source_name} 的元数据收集成功")
            await update_schedule_sync_status(schedule_id, True, "元数据收集成功")
        else:
            logger.error(f"数据源 {source_config.source_name} 的元数据收集失败: {error_message}")
            await update_schedule_sync_status(schedule_id, False, f"元数据收集失败: {error_message}")
    
    except Exception as e:
        error_msg = f"处理数据源 {source_config.source_name} 的元数据收集时出错: {str(e)}"
        logger.error(error_msg)
        await update_schedule_sync_status(schedule_id, False, error_msg)


async def collect_metadata_for_source(source_config: models.DataSourceConfig) -> Tuple[bool, str]:
    """
    为指定数据源收集元数据
    
    Args:
        source_config: 数据源配置
        
    Returns:
        Tuple[bool, str]: (成功标志, 错误信息)
    """
    logger.info(f"开始为数据源 {source_config.source_name} 收集元数据")
    
    # 初始化同步状态
    now = datetime.now()
    
    # 对象元数据同步状态
    objects_sync_status = MetadataSyncStatus(
        source_id=source_config.source_id,
        object_type="OBJECTS",
        sync_start_time=now,
        sync_status="RUNNING"
    )
    objects_sync_id = await update_sync_status(objects_sync_status)
    objects_sync_status.sync_id = objects_sync_id
    
    # 函数元数据同步状态
    functions_sync_status = MetadataSyncStatus(
        source_id=source_config.source_id,
        object_type="FUNCTIONS",
        sync_start_time=now,
        sync_status="RUNNING"
    )
    functions_sync_id = await update_sync_status(functions_sync_status)
    functions_sync_status.sync_id = functions_sync_id
    
    try:
        # 连接到源数据库
        conn = await get_source_db_connection(source_config)
        
        try:
            # 获取对象元数据
            objects_metadata = await fetch_objects_metadata(conn, source_config)
            objects_sync_status.items_processed = len(objects_metadata)
            
            # 批量保存对象元数据
            object_ids = await save_objects_metadata(objects_metadata)
            objects_sync_status.items_succeeded = len(object_ids)
            
            # 获取并保存列元数据
            columns_count = 0
            columns_success = 0
            
            for i, obj_metadata in enumerate(objects_metadata):
                if obj_metadata.object_type in ('TABLE', 'VIEW', 'MATERIALIZED VIEW'):
                    try:
                        columns_metadata = await fetch_columns_metadata(
                            conn, 
                            object_ids[i], 
                            obj_metadata.schema_name, 
                            obj_metadata.object_name,
                            source_config
                        )
                        
                        columns_count += len(columns_metadata)
                        # 批量保存列元数据
                        column_ids = await save_columns_metadata(columns_metadata)
                        columns_success += len(column_ids)
                    except Exception as e:
                        logger.error(f"处理 {obj_metadata.schema_name}.{obj_metadata.object_name} 的列元数据时出错: {str(e)}")
                        objects_sync_status.items_failed += 1
            
            # 更新对象同步状态
            objects_sync_status.sync_end_time = datetime.now()
            objects_sync_status.sync_status = "COMPLETED"
            objects_sync_status.items_failed = objects_sync_status.items_processed - objects_sync_status.items_succeeded
            await update_sync_status(objects_sync_status)
            
            logger.info(f"对象元数据同步完成: 处理 {objects_sync_status.items_processed} 个对象，成功 {objects_sync_status.items_succeeded} 个")
            logger.info(f"列元数据同步完成: 处理 {columns_count} 个列，成功 {columns_success} 个")
            
            # 获取函数元数据
            functions_metadata = await fetch_functions_metadata(conn, source_config)
            functions_sync_status.items_processed = len(functions_metadata)
            
            # 保存函数元数据
            function_ids = await save_functions_metadata(functions_metadata)
            functions_sync_status.items_succeeded = len(function_ids)
            
            # 更新函数同步状态
            functions_sync_status.sync_end_time = datetime.now()
            functions_sync_status.sync_status = "COMPLETED"
            functions_sync_status.items_failed = functions_sync_status.items_processed - functions_sync_status.items_succeeded
            await update_sync_status(functions_sync_status)
            
            logger.info(f"函数元数据同步完成: 处理 {functions_sync_status.items_processed} 个函数，成功 {functions_sync_status.items_succeeded} 个")
            
            return True, ""
        
        except Exception as e:
            error_msg = f"处理数据源 {source_config.source_name} 的元数据时出错: {str(e)}"
            logger.error(error_msg)
            
            # 更新同步状态为失败
            now = datetime.now()
            
            objects_sync_status.sync_end_time = now
            objects_sync_status.sync_status = "FAILED"
            objects_sync_status.error_details = error_msg
            await update_sync_status(objects_sync_status)
            
            functions_sync_status.sync_end_time = now
            functions_sync_status.sync_status = "FAILED"
            functions_sync_status.error_details = error_msg
            await update_sync_status(functions_sync_status)
            
            return False, error_msg
        finally:
            # 关闭数据库连接
            await conn.close()
    
    except Exception as e:
        error_msg = f"连接数据源 {source_config.source_name} 时出错: {str(e)}"
        logger.error(error_msg)
        
        # 更新同步状态为失败
        now = datetime.now()
        
        objects_sync_status.sync_end_time = now
        objects_sync_status.sync_status = "FAILED"
        objects_sync_status.error_details = error_msg
        await update_sync_status(objects_sync_status)
        
        functions_sync_status.sync_end_time = now
        functions_sync_status.sync_status = "FAILED"
        functions_sync_status.error_details = error_msg
        await update_sync_status(functions_sync_status)
        
        return False, error_msg