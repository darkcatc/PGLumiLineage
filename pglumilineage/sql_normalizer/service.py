#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL 范式化模块

该模块负责将 SQL 语句转换为标准化格式，并生成唯一哈希值。
这些哈希值将用于识别相似的 SQL 模式，并建立 SQL 语句之间的关联。

作者: Vance Chen
"""

import asyncio
import hashlib
import logging
import re
import functools
from typing import Optional, Dict, List, Tuple, Any, Union
from datetime import datetime, timezone

import sqlglot
from sqlglot import parse_one, transpile
from sqlglot.errors import ParseError
from pydantic import BaseModel

from pglumilineage.common import config, db_utils, models
from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common.models import RawSQLLog

# 从元数据收集器导入模型定义
from pglumilineage.metadata_collector.service import ObjectMetadata, FunctionMetadata

# 设置日志记录器
logger = logging.getLogger(__name__)

# 缓存设置
# 缓存已处理的SQL哈希值和规范化结果，避免重复处理
SQL_HASH_CACHE_SIZE = 1000  # 缓存大小
NORMALIZE_CACHE_SIZE = 1000  # 规范化缓存大小

# 初始化缓存
_normalize_cache = {}
_normalize_cache_timestamp = datetime.now(timezone.utc)
NORMALIZE_CACHE_TTL = 3600  # 缓存有效期（秒）


def is_data_flow_sql(sql: str) -> bool:
    """
    判断SQL语句是否与数据流转相关
    
    只有与数据流转相关的SQL语句才需要进行血缘分析，例如：
    - INSERT 语句
    - CREATE TABLE AS/SELECT 语句
    - SELECT INTO 语句
    - UPDATE 语句
    - UPSERT 语句 (INSERT ON CONFLICT)
    - MERGE 语句
    - COPY 语句
    - WITH ... INSERT/UPDATE/DELETE
    - 表分区操作
    - 物化视图刷新
    
    Args:
        sql: SQL语句
        
    Returns:
        bool: 如果是数据流转相关的SQL语句则返回True，否则返回False
    """
    # 转换为小写以便于匹配
    sql_lower = sql.lower()
    
    # 快速过滤明显不是数据流转的SQL语句
    non_data_flow_prefixes = [
        'select ', 'select\n', 'select\t',  # 简单查询
        'show ', 'show\n', 'show\t',        # 显示信息
        'explain ', 'explain\n', 'explain\t',  # 执行计划
        'comment on ', 'comment\n', 'comment\t',  # 注释
        'grant ', 'grant\n', 'grant\t',      # 授权
        'revoke ', 'revoke\n', 'revoke\t',    # 撤销授权
        'create index', 'create unique index',  # 创建索引
        'alter index', 'drop index',         # 索引操作
        'analyze ', 'analyse ', 'vacuum ',    # 维护操作
        'begin', 'commit', 'rollback', 'savepoint',  # 事务操作
        'set ', 'reset ',                     # 会话设置
        'create schema', 'alter schema', 'drop schema',  # 模式操作
        'create user', 'alter user', 'drop user',  # 用户操作
        'create role', 'alter role', 'drop role',  # 角色操作
        'create extension', 'drop extension',  # 扩展操作
        'create trigger', 'drop trigger',      # 触发器操作
        'create sequence', 'alter sequence', 'drop sequence',  # 序列操作
    ]
    
    # 如果以这些前缀开头，并且不包含数据流转关键字，则跳过
    for prefix in non_data_flow_prefixes:
        if sql_lower.startswith(prefix):
            # 对于 SELECT 语句，需要进一步检查是否包含 INTO
            if prefix.startswith('select'):
                if ' into ' not in sql_lower and '\ninto ' not in sql_lower and '\tinto ' not in sql_lower:
                    logger.debug(f"跳过简单查询: {sql[:50]}...")
                    return False
            else:
                logger.debug(f"跳过非数据流转 SQL: {sql[:50]}...")
                return False
    
    # 对于 CREATE TABLE 语句，需要区分是否是 CREATE TABLE AS SELECT
    if sql_lower.startswith('create table'):
        if ' as ' not in sql_lower and '\nas ' not in sql_lower and '\tas ' not in sql_lower:
            logger.debug(f"跳过简单表创建: {sql[:50]}...")
            return False
    
    # 检查是否包含数据流转关键字和模式
    data_flow_patterns = [
        # INSERT 语句
        r'\binsert\s+into\b',
        # CREATE TABLE AS/SELECT
        r'\bcreate\s+table\b.*\bas\s+select\b',
        # SELECT INTO
        r'\bselect\b.*\binto\b',
        # UPDATE
        r'\bupdate\b.*\bset\b',
        # UPSERT (INSERT ON CONFLICT)
        r'\binsert\b.*\bon\s+conflict\b',
        # MERGE
        r'\bmerge\b.*\binto\b',
        # COPY
        r'\bcopy\b.*\bfrom\b',
        r'\bcopy\b.*\bto\b',
        # WITH + 数据修改
        r'\bwith\b.*\binsert\b',
        r'\bwith\b.*\bupdate\b',
        r'\bwith\b.*\bdelete\b',
        # DELETE RETURNING
        r'\bdelete\b.*\breturning\b',
        # 表分区操作
        r'\balter\s+table\b.*\battach\s+partition\b',
        r'\balter\s+table\b.*\bdetach\s+partition\b',
        # 物化视图刷新
        r'\brefresh\s+materialized\s+view\b'
    ]
    
    import re
    for pattern in data_flow_patterns:
        if re.search(pattern, sql_lower):
            logger.debug(f"检测到数据流转 SQL: {sql[:50]}...")
            return True
    
    logger.debug(f"未检测到数据流转模式: {sql[:50]}...")
    return False


@functools.lru_cache(maxsize=NORMALIZE_CACHE_SIZE)
def normalize_sql(raw_sql: str, dialect: str = 'postgres') -> Optional[str]:
    """
    将原始 SQL 语句转换为标准化格式
    
    该函数使用 sqlglot 库将 SQL 语句转换为标准化格式，包括：
    - 将字面量（数字、字符串、日期等）替换为占位符
    - 移除或标准化注释
    - 标准化空白字符
    - 标准化 SQL 语法结构
    
    只处理与数据流转相关的SQL语句，例如INSERT、CREATE TABLE AS、UPDATE等。
    简单的SELECT查询、GRANT权限、CREATE INDEX等不会被处理。
    
    Args:
        raw_sql: 原始 SQL 语句
        dialect: SQL 方言，默认为 postgres
        
    Returns:
        Optional[str]: 标准化后的 SQL 语句，如果解析失败或不是数据流转SQL则返回 None
    """
    if not raw_sql or not raw_sql.strip():
        logger.warning("收到空的 SQL 语句")
        return None
    
    # 去除 SQL 两端的空白字符
    raw_sql = raw_sql.strip()
    
    # 检查是否为数据流转相关的 SQL
    if not is_data_flow_sql(raw_sql):
        logger.debug(f"跳过非数据流转 SQL: {raw_sql[:100]}...")
        return None
    
    # 手动实现字面量标准化的函数
    def standardize_literals(sql):
        """将 SQL 中的字面量替换为占位符"""
        import re
        
        # 替换数字字面量（整数和浮点数）
        sql = re.sub(r'\b\d+\.\d+\b', '?', sql)
        sql = re.sub(r'\b\d+\b', '?', sql)
        
        # 替换字符串字面量（单引号和双引号）
        # 先处理单引号字符串，注意跳过转义字符
        sql = re.sub(r"'([^'\\]|\\.)*'", "'?'", sql)
        # 处理双引号字符串
        sql = re.sub(r'"([^"\\]|\\.)*"', '"?"', sql)
        
        # 替换日期/时间字面量（简单模式）
        date_patterns = [
            r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
            r'\d{4}/\d{2}/\d{2}',  # YYYY/MM/DD
            r'\d{2}-\d{2}-\d{4}',  # DD-MM-YYYY
            r'\d{2}/\d{2}/\d{4}',  # DD/MM/YYYY
            r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}',  # YYYY-MM-DD HH:MM:SS
            r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}',  # YYYY-MM-DDThh:mm:ss
        ]
        
        for pattern in date_patterns:
            sql = re.sub(pattern, '?', sql)
        
        return sql
    
    try:
        # 使用 sqlglot 解析 SQL
        try:
            # 尝试使用 sqlglot 解析
            parsed_sql = parse_one(raw_sql, read=dialect)
            
            if parsed_sql is None:
                # 如果解析失败，使用简单的文本处理
                logger.warning(f"SQL 无法解析为 AST，将使用简单文本处理: {raw_sql[:100]}...")
                normalized_sql = standardize_literals(raw_sql.lower())
                normalized_sql = " ".join(normalized_sql.split())  # 标准化空白字符
                return normalized_sql
            
            # 生成标准化的 SQL 字符串
            normalized_sql = parsed_sql.sql(pretty=False, comments=False)
            
            # 手动处理字面量标准化
            normalized_sql = standardize_literals(normalized_sql)
            
            logger.debug(f"SQL 范式化成功: {normalized_sql[:100]}...")
            return normalized_sql
            
        except ParseError as e:
            # 如果 sqlglot 解析失败，使用简单的文本处理
            logger.warning(f"SQL 解析失败，将使用简单文本处理: {str(e)}, SQL: {raw_sql[:100]}...")
            normalized_sql = standardize_literals(raw_sql.lower())
            normalized_sql = " ".join(normalized_sql.split())  # 标准化空白字符
            return normalized_sql
    
    except Exception as e:
        logger.error(f"SQL 范式化过程中出现未知错误: {str(e)}, SQL: {raw_sql[:100]}...")
        # 对于完全无法处理的情况，返回简化版本的原始 SQL
        try:
            simplified = " ".join(raw_sql.lower().split())
            return simplified
        except:
            return None


@functools.lru_cache(maxsize=SQL_HASH_CACHE_SIZE)
def generate_sql_hash(normalized_sql: str) -> str:
    """
    为标准化后的 SQL 语句生成唯一哈希值
    
    该函数使用 SHA-256 算法为标准化后的 SQL 语句生成哈希值，
    这个哈希值将作为 SQL 模式的唯一标识符，用于识别相似的 SQL 语句。
    使用 LRU 缓存提高性能，避免重复计算相同 SQL 的哈希值。
    
    Args:
        normalized_sql: 标准化后的 SQL 语句
        
    Returns:
        str: 十六进制表示的 SQL 哈希值，如果输入为空则返回空字符串
    """
    if not normalized_sql:
        logger.warning("收到空的标准化 SQL 语句")
        return ""
    
    try:
        # 将 SQL 字符串转换为 UTF-8 编码的字节串
        sql_bytes = normalized_sql.encode('utf-8')
        
        # 使用 SHA-256 算法生成哈希
        hash_obj = hashlib.sha256(sql_bytes)
        
        # 获取十六进制表示的哈希字符串
        sql_hash = hash_obj.hexdigest()
        
        logger.debug(f"生成 SQL 哈希: {sql_hash}")
        return sql_hash
    except Exception as e:
        logger.error(f"生成 SQL 哈希时出错: {str(e)}")
        return ""


async def upsert_sql_pattern_from_log(sql_hash: str, normalized_sql: str, sample_raw_sql: str, source_database_name: str, log_time: datetime, duration_ms: int) -> Optional[int]:
    """
    将 SQL 模式从日志中插入或更新到 lumi_analytics.sql_patterns 表
    
    此函数使用 SQL 哈希值作为唯一标识符，将 SQL 模式信息插入到 lumi_analytics.sql_patterns 表中。
    如果表中已存在相同哈希值的记录，则更新该记录的统计信息。
    
    Args:
        sql_hash: SQL 哈希值，作为唯一标识符
        normalized_sql: 标准化后的 SQL 语句
        sample_raw_sql: 原始 SQL 语句样本
        source_database_name: 源数据库名称
        log_time: 日志记录时间
        duration_ms: SQL 执行时间（毫秒）
        
    Returns:
        Optional[int]: SQL 模式 ID，如果操作失败则返回 None
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 构建 UPSERT SQL 语句
        query = """
        INSERT INTO lumi_analytics.sql_patterns (
            sql_hash,
            normalized_sql_text,
            sample_raw_sql_text,
            source_database_name,
            first_seen_at,
            last_seen_at,
            execution_count,
            total_duration_ms,
            avg_duration_ms,
            max_duration_ms,
            min_duration_ms,
            llm_analysis_status
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        )
        ON CONFLICT (sql_hash) DO UPDATE SET
            last_seen_at = EXCLUDED.last_seen_at,
            execution_count = lumi_analytics.sql_patterns.execution_count + 1,
            total_duration_ms = lumi_analytics.sql_patterns.total_duration_ms + EXCLUDED.total_duration_ms,
            avg_duration_ms = (lumi_analytics.sql_patterns.total_duration_ms + EXCLUDED.total_duration_ms) / (lumi_analytics.sql_patterns.execution_count + 1),
            max_duration_ms = GREATEST(lumi_analytics.sql_patterns.max_duration_ms, EXCLUDED.max_duration_ms),
            min_duration_ms = LEAST(lumi_analytics.sql_patterns.min_duration_ms, EXCLUDED.min_duration_ms)
        RETURNING sql_hash
        """
        
        async with pool.acquire() as conn:
            # 执行 UPSERT 操作
            returned_hash = await conn.fetchval(
                query,
                sql_hash,                # $1: sql_hash
                normalized_sql,          # $2: normalized_sql_text
                sample_raw_sql,          # $3: sample_raw_sql_text
                source_database_name,    # $4: source_database_name
                log_time,                # $5: first_seen_at
                log_time,                # $6: last_seen_at
                1,                       # $7: execution_count (初始为1)
                duration_ms,             # $8: total_duration_ms
                duration_ms,             # $9: avg_duration_ms (初始等于 duration_ms)
                duration_ms,             # $10: max_duration_ms (初始等于 duration_ms)
                duration_ms,             # $11: min_duration_ms (初始等于 duration_ms)
                'PENDING'                # $12: llm_analysis_status
            )
            
            logger.info(f"成功更新来自日志的 SQL 模式，哈希值: {sql_hash[:8]}...")
            return returned_hash  # 返回 SQL 哈希值
            
    except Exception as e:
        logger.error(f"更新来自日志的 SQL 模式失败: {str(e)}, SQL 哈希值: {sql_hash[:8]}...")
        return None


async def upsert_sql_pattern_from_definition(sql_hash: str, normalized_sql: str, sample_raw_sql: str, source_database_name: str, definition_type: str, definition_metadata_updated_at: datetime) -> Optional[int]:
    """
    将 SQL 模式从视图或函数定义中插入或更新到 lumi_analytics.sql_patterns 表
    
    此函数处理来自元数据的 SQL 模式，如视图定义或函数定义。
    它使用特殊的冲突处理策略，确保不会错误地覆盖来自日志的统计数据。
    
    Args:
        sql_hash: SQL 哈希值，作为唯一标识符
        normalized_sql: 标准化后的 SQL 语句
        sample_raw_sql: 原始 SQL 定义
        source_database_name: 源数据库名称
        definition_type: 定义类型，'VIEW' 或 'FUNCTION'
        definition_metadata_updated_at: 元数据更新时间
        
    Returns:
        Optional[int]: SQL 模式 ID，如果操作失败则返回 None
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 构建 UPSERT SQL 语句
        # 注意冲突处理策略：
        # 1. 如果记录不存在，插入新记录，执行次数设为 0，表示这是一个定义
        # 2. 如果记录已存在，且执行次数为 0，表示这是一个定义，更新定义样本和最后见到时间
        # 3. 如果记录已存在，且执行次数大于 0，表示这是一个日志记录，只更新最后见到时间，不覆盖统计数据
        # 4. 如果 LLM 分析状态不是 'SUCCESS'，则重置为 'PENDING'，以便重新分析
        query = """
        INSERT INTO lumi_analytics.sql_patterns (
            sql_hash,
            normalized_sql_text,
            sample_raw_sql_text,
            source_database_name,
            first_seen_at,
            last_seen_at,
            execution_count,
            total_duration_ms,
            avg_duration_ms,
            max_duration_ms,
            min_duration_ms,
            llm_analysis_status
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
        )
        ON CONFLICT (sql_hash) DO UPDATE SET
            last_seen_at = GREATEST(lumi_analytics.sql_patterns.last_seen_at, EXCLUDED.last_seen_at),
            sample_raw_sql_text = CASE 
                WHEN lumi_analytics.sql_patterns.execution_count = 0 THEN EXCLUDED.sample_raw_sql_text 
                ELSE lumi_analytics.sql_patterns.sample_raw_sql_text 
            END,
            llm_analysis_status = CASE 
                WHEN lumi_analytics.sql_patterns.llm_analysis_status = 'SUCCESS' THEN lumi_analytics.sql_patterns.llm_analysis_status 
                ELSE 'PENDING' 
            END
        RETURNING sql_hash
        """
        
        async with pool.acquire() as conn:
            # 执行 UPSERT 操作
            returned_hash = await conn.fetchval(
                query,
                sql_hash,                        # $1: sql_hash
                normalized_sql,                  # $2: normalized_sql_text
                sample_raw_sql,                  # $3: sample_raw_sql_text
                source_database_name,            # $4: source_database_name
                definition_metadata_updated_at,  # $5: first_seen_at
                definition_metadata_updated_at,  # $6: last_seen_at
                0,                               # $7: execution_count (定义设为 0)
                0,                               # $8: total_duration_ms
                0,                               # $9: avg_duration_ms
                0,                               # $10: max_duration_ms
                0,                               # $11: min_duration_ms
                'PENDING'                        # $12: llm_analysis_status
            )
            
            logger.info(f"成功更新来自{definition_type}定义的 SQL 模式，哈希值: {sql_hash[:8]}...")
            return returned_hash  # 返回 SQL 哈希值
            
    except Exception as e:
        logger.error(f"更新来自{definition_type}定义的 SQL 模式失败: {str(e)}, SQL 哈希值: {sql_hash[:8]}...")
        return None


async def store_sql_pattern(
    normalized_sql: str, 
    sql_hash: str, 
    source_type: str,
    execution_time_ms: Optional[float] = None,
    source_id: Optional[int] = None,
    object_id: Optional[int] = None,
    function_id: Optional[int] = None
) -> Optional[int]:
    """
    将 SQL 模式存储到数据库
    
    Args:
        normalized_sql: 标准化后的 SQL 语句
        sql_hash: SQL 哈希值
        source_type: 来源类型，如 'LOG', 'VIEW', 'FUNCTION'
        execution_time_ms: 执行时间（毫秒），仅适用于日志
        source_id: 数据源 ID
        object_id: 对象 ID（用于视图）
        function_id: 函数 ID（用于函数）
        
    Returns:
        Optional[int]: SQL 模式 ID，如果存储失败则返回 None
    """
    # 这里将实现 SQL 模式的存储逻辑
    # 暂时返回 None，后续实现
    return None


async def update_metadata_sql_hash(
    object_id: Optional[int] = None,
    function_id: Optional[int] = None,
    sql_hash: Optional[str] = None
) -> bool:
    """
    更新元数据表中的 SQL 哈希字段
    
    Args:
        object_id: 对象 ID（用于视图）
        function_id: 函数 ID（用于函数）
        sql_hash: SQL 哈希值
        
    Returns:
        bool: 更新是否成功
    """
    # 这里将实现元数据表的更新逻辑
    # 暂时返回 False，后续实现
    return False


async def fetch_unprocessed_view_definitions() -> List[ObjectMetadata]:
    """
    从 lumi_metadata_store.objects_metadata 表中获取未处理的视图定义
    
    获取 object_type 为 'VIEW' 或 'MATERIALIZED VIEW'，且 definition 非空，
    并且 normalized_sql_hash 字段为空或需要更新的记录。
    
    Returns:
        List[models.ObjectMetadata]: 对象元数据列表
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 查询未处理的视图定义
        query = """
        SELECT 
            object_id, 
            database_name, 
            schema_name, 
            object_name, 
            object_type, 
            definition, 
            created_at, 
            updated_at, 
            normalized_sql_hash
        FROM 
            lumi_metadata_store.objects_metadata
        WHERE 
            object_type IN ('VIEW', 'MATERIALIZED VIEW')
            AND definition IS NOT NULL
            AND definition != ''
            AND (normalized_sql_hash IS NULL OR normalized_sql_hash = '')
        ORDER BY 
            object_id
        """
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
            
            # 将查询结果转换为 ObjectMetadata 对象列表
            objects = []
            for row in rows:
                obj = ObjectMetadata(
                    object_id=row['object_id'],
                    source_id=1,  # 假设源ID为1，实际应从配置获取
                    database_name=row['database_name'],
                    schema_name=row['schema_name'],
                    object_name=row['object_name'],
                    object_type=row['object_type'],
                    definition=row['definition'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at'],
                    normalized_sql_hash=row['normalized_sql_hash']
                )
                objects.append(obj)
            
            logger.info(f"从 lumi_metadata_store.objects_metadata 表中获取到 {len(objects)} 条未处理的视图定义")
            return objects
            
    except Exception as e:
        logger.error(f"获取未处理的视图定义失败: {str(e)}")
        return []


async def fetch_unprocessed_function_definitions() -> List[FunctionMetadata]:
    """
    从 lumi_metadata_store.functions_metadata 表中获取未处理的函数定义
    
    获取 definition 非空，并且 normalized_sql_hash 字段为空或需要更新的记录。
    
    Returns:
        List[models.FunctionMetadata]: 函数元数据列表
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 查询未处理的函数定义
        query = """
        SELECT 
            function_id, 
            database_name, 
            schema_name, 
            function_name, 
            definition, 
            return_type, 
            language, 
            parameter_types, 
            created_at, 
            updated_at, 
            normalized_sql_hash
        FROM 
            lumi_metadata_store.functions_metadata
        WHERE 
            definition IS NOT NULL
            AND definition != ''
            AND (normalized_sql_hash IS NULL OR normalized_sql_hash = '')
        ORDER BY 
            function_id
        """
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
            
            # 将查询结果转换为 FunctionMetadata 对象列表
            functions = []
            for row in rows:
                func = FunctionMetadata(
                    function_id=row['function_id'],
                    source_id=1,  # 假设源ID为1，实际应从配置获取
                    database_name=row['database_name'],
                    schema_name=row['schema_name'],
                    function_name=row['function_name'],
                    function_type='FUNCTION',  # 默认函数类型
                    definition=row['definition'],
                    return_type=row['return_type'],
                    language=row['language'],
                    parameter_types=row['parameter_types'],
                    created_at=row['created_at'],
                    updated_at=row['updated_at'],
                    normalized_sql_hash=row['normalized_sql_hash']
                )
                functions.append(func)
            
            logger.info(f"从 lumi_metadata_store.functions_metadata 表中获取到 {len(functions)} 条未处理的函数定义")
            return functions
            
    except Exception as e:
        logger.error(f"获取未处理的函数定义失败: {str(e)}")
        return []


async def mark_object_definition_as_processed(object_id: int, sql_hash: str) -> bool:
    """
    将对象定义标记为已处理
    
    更新 lumi_metadata_store.objects_metadata 表中的 normalized_sql_hash 和 updated_at 字段。
    
    Args:
        object_id: 对象 ID
        sql_hash: SQL 哈希值
        
    Returns:
        bool: 更新是否成功
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 构建更新语句
        query = """
        UPDATE lumi_metadata_store.objects_metadata 
        SET 
            normalized_sql_hash = $2, 
            updated_at = CURRENT_TIMESTAMP 
        WHERE 
            object_id = $1
        """
        
        async with pool.acquire() as conn:
            # 执行更新
            await conn.execute(query, object_id, sql_hash)
            
            logger.debug(f"成功将对象 {object_id} 的定义标记为已处理，哈希值: {sql_hash[:8]}...")
            return True
            
    except Exception as e:
        logger.error(f"将对象 {object_id} 的定义标记为已处理失败: {str(e)}")
        return False


async def mark_function_definition_as_processed(function_id: int, sql_hash: str) -> bool:
    """
    将函数定义标记为已处理
    
    更新 lumi_metadata_store.functions_metadata 表中的 normalized_sql_hash 和 updated_at 字段。
    
    Args:
        function_id: 函数 ID
        sql_hash: SQL 哈希值
        
    Returns:
        bool: 更新是否成功
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 构建更新语句
        query = """
        UPDATE lumi_metadata_store.functions_metadata 
        SET 
            normalized_sql_hash = $2, 
            updated_at = CURRENT_TIMESTAMP 
        WHERE 
            function_id = $1
        """
        
        async with pool.acquire() as conn:
            # 执行更新
            await conn.execute(query, function_id, sql_hash)
            
            logger.debug(f"成功将函数 {function_id} 的定义标记为已处理，哈希值: {sql_hash[:8]}...")
            return True
            
    except Exception as e:
        logger.error(f"将函数 {function_id} 的定义标记为已处理失败: {str(e)}")
        return False


async def update_metadata_sql_hash(object_id: Optional[int] = None, function_id: Optional[int] = None, sql_hash: str = '') -> bool:
    """
    更新元数据表中的 SQL 哈希字段
    
    Args:
        object_id: 对象 ID（用于视图）
        function_id: 函数 ID（用于函数）
        sql_hash: SQL 哈希值
        
    Returns:
        bool: 更新是否成功
    """
    if object_id:
        return await mark_object_definition_as_processed(object_id, sql_hash)
    elif function_id:
        return await mark_function_definition_as_processed(function_id, sql_hash)
    else:
        logger.error("更新元数据 SQL 哈希失败: 未提供 object_id 或 function_id")
        return False


async def fetch_unprocessed_logs(limit: int) -> List[RawSQLLog]:
    """
    从 lumi_logs.captured_logs 表中获取未处理的日志记录
    
    Args:
        limit: 返回的最大记录数
        
    Returns:
        List[RawSQLLog]: 原始 SQL 日志对象列表
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 查询未处理的日志记录
        query = """
        SELECT 
            log_id, 
            source_database_name, 
            raw_sql_text, 
            log_time, 
            duration_ms,
            is_processed_for_analysis,
            normalized_sql_hash
        FROM 
            lumi_logs.captured_logs
        WHERE 
            is_processed_for_analysis = FALSE
        ORDER BY 
            log_id
        LIMIT $1
        """
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, limit)
            
            # 将查询结果转换为 RawSQLLog 对象列表
            logs = []
            for row in rows:
                log = RawSQLLog(
                    log_id=row['log_id'],
                    source_database_name=row['source_database_name'],
                    raw_sql_text=row['raw_sql_text'],
                    log_time=row['log_time'],
                    duration_ms=row['duration_ms'],  # 使用正确的列名 duration_ms
                    is_processed_for_analysis=row['is_processed_for_analysis'],
                    normalized_sql_hash=row['normalized_sql_hash']
                )
                logs.append(log)
            
            logger.info(f"从 lumi_logs.captured_logs 表中获取到 {len(logs)} 条未处理的日志记录")
            return logs
            
    except Exception as e:
        logger.error(f"获取未处理的日志记录失败: {str(e)}")
        return []


async def mark_logs_as_processed(log_ids_with_hashes: List[Tuple[int, str]]) -> int:
    """
    将日志记录标记为已处理
    
    Args:
        log_ids_with_hashes: 日志 ID 和对应的 SQL 哈希值列表
        
    Returns:
        int: 成功更新的记录数
    """
    if not log_ids_with_hashes:
        logger.warning("没有日志记录需要标记为已处理")
        return 0
    
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 分别处理有哈希值和无哈希值的情况
        logs_with_hash = [(log_id, sql_hash) for log_id, sql_hash in log_ids_with_hashes if sql_hash]
        logs_without_hash = [log_id for log_id, sql_hash in log_ids_with_hashes if not sql_hash]
        
        updated_count = 0
        
        async with pool.acquire() as conn:
            # 处理有哈希值的记录（成功泛化的）
            if logs_with_hash:
                query_with_hash = """
                UPDATE lumi_logs.captured_logs 
                SET 
                    is_processed_for_analysis = TRUE, 
                    normalized_sql_hash = $2 
                WHERE 
                    log_id = $1
                """
                
                # 批量更新有哈希值的记录
                await conn.executemany(query_with_hash, logs_with_hash)
                updated_count += len(logs_with_hash)
                logger.info(f"成功将 {len(logs_with_hash)} 条成功泛化的日志记录标记为已处理并更新哈希值")
            
            # 处理无哈希值的记录（非数据流SQL或解析失败的）
            if logs_without_hash:
                query_without_hash = """
                UPDATE lumi_logs.captured_logs 
                SET 
                    is_processed_for_analysis = TRUE
                WHERE 
                    log_id = ANY($1)
                """
                
                # 批量更新无哈希值的记录
                await conn.execute(query_without_hash, logs_without_hash)
                updated_count += len(logs_without_hash)
                logger.info(f"成功将 {len(logs_without_hash)} 条非数据流SQL或解析失败的日志记录标记为已处理")
            
            # 返回总的更新行数
            logger.info(f"总共成功将 {updated_count} 条日志记录标记为已处理")
            return updated_count
            
    except Exception as e:
        logger.error(f"标记日志记录为已处理失败: {str(e)}")
        return 0


async def record_sql_normalization_error(source_type: str, source_id: int, raw_sql_text: str, error_reason: str, error_details: str = None, source_database_name: str = None) -> bool:
    """
    记录SQL规范化失败的信息
    
    Args:
        source_type: 来源类型，如 'LOG'、'VIEW'、'FUNCTION'
        source_id: 来源ID，如日志ID、对象ID或函数ID
        raw_sql_text: 原始SQL文本
        error_reason: 失败原因
        error_details: 详细错误信息（可选）
        source_database_name: 源数据库名称（可选）
        
    Returns:
        bool: 记录是否成功
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 构建插入语句
        query = """
        INSERT INTO lumi_analytics.sql_normalization_errors (
            source_type, 
            source_id, 
            raw_sql_text, 
            error_reason, 
            error_details, 
            source_database_name
        ) VALUES ($1, $2, $3, $4, $5, $6)
        RETURNING error_id
        """
        
        async with pool.acquire() as conn:
            # 执行插入
            error_id = await conn.fetchval(
                query, 
                source_type, 
                source_id, 
                raw_sql_text, 
                error_reason, 
                error_details, 
                source_database_name
            )
            
            logger.info(f"成功记录SQL规范化失败信息，ID: {error_id}, 来源: {source_type} {source_id}, 原因: {error_reason}")
            return True
            
    except Exception as e:
        logger.error(f"记录SQL规范化失败信息时出错: {str(e)}")
        return False

async def update_log_sql_hash(log_id: int, sql_hash: str) -> bool:
    """
    更新日志表中的 SQL 哈希字段
    
    Args:
        log_id: 日志 ID
        sql_hash: SQL 哈希值
        
    Returns:
        bool: 更新是否成功
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 构建更新语句
        query = """
        UPDATE lumi_logs.captured_logs 
        SET 
            normalized_sql_hash = $2, 
            is_processed_for_analysis = TRUE 
        WHERE 
            log_id = $1
        """
        
        async with pool.acquire() as conn:
            # 执行更新
            await conn.execute(query, log_id, sql_hash)
            
            logger.debug(f"成功更新日志 {log_id} 的 SQL 哈希字段: {sql_hash[:8]}...")
            return True
            
    except Exception as e:
        logger.error(f"更新日志 {log_id} 的 SQL 哈希字段失败: {str(e)}")
        return False


async def process_metadata_definitions() -> Tuple[int, int, int, int]:
    """
    处理元数据定义（视图和函数）
    
    该函数从 lumi_metadata_store.objects_metadata 和 lumi_metadata_store.functions_metadata 表中
    获取未处理的视图和函数定义，对每个定义进行 SQL 范式化和哈希生成，
    并将结果存储到 lumi_analytics.sql_patterns 表中。
    处理完成后，将定义标记为已处理。
    
    Returns:
        Tuple[int, int, int, int]: 
            - 处理的视图定义数量
            - 处理的函数定义数量
            - 成功范式化的定义数量
            - 成功更新的定义数量
    """
    logger.info("开始处理元数据定义（视图和函数）")
    
    # 获取未处理的视图定义
    views = await fetch_unprocessed_view_definitions()
    
    # 获取未处理的函数定义
    functions = await fetch_unprocessed_function_definitions()
    
    if not views and not functions:
        logger.info("没有找到未处理的元数据定义")
        return 0, 0, 0, 0
    
    # 统计信息
    view_count = len(views)
    function_count = len(functions)
    normalized_count = 0
    successful_updates = 0
    
    # 处理视图定义
    for view in views:
        try:
            # 范式化 SQL
            try:
                normalized_sql = normalize_sql(view.definition)
                
                if not normalized_sql:
                    error_reason = "非数据流转SQL或解析失败"
                    logger.warning(f"SQL 范式化失败，视图 ID: {view.object_id}, 名称: {view.schema_name}.{view.object_name}, 原因: {error_reason}")
                    # 记录失败信息
                    await record_sql_normalization_error(
                        source_type="VIEW",
                        source_id=view.object_id,
                        raw_sql_text=view.definition,
                        error_reason=error_reason,
                        source_database_name=view.database_name
                    )
                    continue
            except Exception as e:
                error_reason = f"SQL范式化异常: {str(e)}"
                logger.warning(f"SQL 范式化过程中出现异常，视图 ID: {view.object_id}, 名称: {view.schema_name}.{view.object_name}, 异常: {str(e)}")
                # 记录失败信息
                await record_sql_normalization_error(
                    source_type="VIEW",
                    source_id=view.object_id,
                    raw_sql_text=view.definition,
                    error_reason=error_reason,
                    error_details=str(e),
                    source_database_name=view.database_name
                )
                continue
            
            normalized_count += 1
            
            # 生成 SQL 哈希
            sql_hash = generate_sql_hash(normalized_sql)
            
            if not sql_hash:
                logger.warning(f"SQL 哈希生成失败，视图 ID: {view.object_id}, 名称: {view.schema_name}.{view.object_name}")
                continue
            
            # 将 SQL 模式信息写入/更新到 lumi_analytics.sql_patterns 表
            pattern_id = await upsert_sql_pattern_from_definition(
                sql_hash=sql_hash,
                normalized_sql=normalized_sql,
                sample_raw_sql=view.definition,
                source_database_name=view.database_name,
                definition_type=view.object_type,
                definition_metadata_updated_at=view.updated_at or datetime.now()
            )
            
            if pattern_id:
                # 标记视图定义为已处理
                success = await mark_object_definition_as_processed(view.object_id, sql_hash)
                if success:
                    successful_updates += 1
            
        except Exception as e:
            logger.error(f"处理视图定义 {view.object_id} 时出错: {str(e)}")
    
    # 处理函数定义
    for func in functions:
        try:
            # 范式化 SQL
            try:
                normalized_sql = normalize_sql(func.definition)
                
                if not normalized_sql:
                    error_reason = "非数据流转SQL或解析失败"
                    logger.warning(f"SQL 范式化失败，函数 ID: {func.function_id}, 名称: {func.schema_name}.{func.function_name}, 原因: {error_reason}")
                    # 记录失败信息
                    await record_sql_normalization_error(
                        source_type="FUNCTION",
                        source_id=func.function_id,
                        raw_sql_text=func.definition,
                        error_reason=error_reason,
                        source_database_name=func.database_name
                    )
                    continue
            except Exception as e:
                error_reason = f"SQL范式化异常: {str(e)}"
                logger.warning(f"SQL 范式化过程中出现异常，函数 ID: {func.function_id}, 名称: {func.schema_name}.{func.function_name}, 异常: {str(e)}")
                # 记录失败信息
                await record_sql_normalization_error(
                    source_type="FUNCTION",
                    source_id=func.function_id,
                    raw_sql_text=func.definition,
                    error_reason=error_reason,
                    error_details=str(e),
                    source_database_name=func.database_name
                )
                continue
            
            normalized_count += 1
            
            # 生成 SQL 哈希
            sql_hash = generate_sql_hash(normalized_sql)
            
            if not sql_hash:
                logger.warning(f"SQL 哈希生成失败，函数 ID: {func.function_id}, 名称: {func.schema_name}.{func.function_name}")
                continue
            
            # 将 SQL 模式信息写入/更新到 lumi_analytics.sql_patterns 表
            pattern_id = await upsert_sql_pattern_from_definition(
                sql_hash=sql_hash,
                normalized_sql=normalized_sql,
                sample_raw_sql=func.definition,
                source_database_name=func.database_name,
                definition_type="FUNCTION",
                definition_metadata_updated_at=func.updated_at or datetime.now()
            )
            
            if pattern_id:
                # 标记函数定义为已处理
                success = await mark_function_definition_as_processed(func.function_id, sql_hash)
                if success:
                    successful_updates += 1
            
        except Exception as e:
            logger.error(f"处理函数定义 {func.function_id} 时出错: {str(e)}")
    
    logger.info(f"处理完成，总共 {view_count} 条视图定义和 {function_count} 条函数定义，成功范式化 {normalized_count} 条，成功更新 {successful_updates} 条")
    
    return view_count, function_count, normalized_count, successful_updates


async def _process_single_log_entry(log: RawSQLLog) -> Optional[Tuple[int, str]]:
    """
    处理单个日志条目：范式化SQL、生成哈希并更新数据库。

    Args:
        log: 原始SQL日志对象。

    Returns:
        Optional[Tuple[int, str]]: 如果成功，则返回 (log_id, sql_hash)，否则返回 None。
    """
    try:
        # 范式化 SQL
        try:
            normalized_sql = normalize_sql(log.raw_sql_text)
            
            if not normalized_sql:
                error_reason = "非数据流转SQL或解析失败"
                logger.warning(f"SQL 范式化失败，日志 ID: {log.log_id}, 原因: {error_reason}, SQL: {log.raw_sql_text[:200]}...")
                # 记录失败信息
                await record_sql_normalization_error(
                    source_type="LOG",
                    source_id=log.log_id,
                    raw_sql_text=log.raw_sql_text,
                    error_reason=error_reason,
                    source_database_name=log.source_database_name
                )
                # 返回日志ID和空哈希值，表示已处理但未成功泛化
                return log.log_id, ""
        except Exception as e:
            error_reason = f"SQL范式化异常: {str(e)}"
            logger.warning(f"SQL 范式化过程中出现异常，日志 ID: {log.log_id}, 异常: {str(e)}")
            # 记录失败信息
            await record_sql_normalization_error(
                source_type="LOG",
                source_id=log.log_id,
                raw_sql_text=log.raw_sql_text,
                error_reason=error_reason,
                error_details=str(e),
                source_database_name=log.source_database_name
            )
            # 返回日志ID和空哈希值，表示已处理但未成功泛化
            return log.log_id, ""

        # 生成 SQL 哈希
        sql_hash = generate_sql_hash(normalized_sql)
        
        if not sql_hash:
            logger.warning(f"SQL 哈希生成失败，日志 ID: {log.log_id}, Normalized SQL: {normalized_sql[:200]}...")
            # 返回日志ID和空哈希值，表示已处理但未成功泛化
            return log.log_id, ""

        # 将 SQL 模式信息写入/更新到 lumi_analytics.sql_patterns 表
        pattern_id = await upsert_sql_pattern_from_log(
            sql_hash=sql_hash,
            normalized_sql=normalized_sql,
            sample_raw_sql=log.raw_sql_text,
            source_database_name=log.source_database_name,
            log_time=log.log_time,
            duration_ms=log.duration_ms or 0
        )

        if pattern_id:
            logger.debug(f"成功处理日志 ID: {log.log_id}, SQL 哈希: {sql_hash}")
            return log.log_id, sql_hash
        else:
            logger.error(f"将 SQL 模式写入数据库失败，日志 ID: {log.log_id}")
            # 返回日志ID和空哈希值，表示已处理但未成功泛化
            return log.log_id, ""
            
    except Exception as e:
        logger.error(f"处理日志 ID: {log.log_id} (SQL: {log.raw_sql_text[:200]}...) 时发生意外错误: {str(e)}", exc_info=True)
        # 返回日志ID和空哈希值，表示已处理但未成功泛化
        return log.log_id, ""


async def process_captured_logs(batch_size: int = 100, max_concurrency: int = 10) -> Tuple[int, int, int]:
    """
    处理未分析的 SQL 日志
    
    该函数从 lumi_logs.captured_logs 表中获取未处理的日志记录，
    对每条记录进行 SQL 范式化和哈希生成，并将结果存储到 lumi_analytics.sql_patterns 表中。
    处理完成后，将日志记录标记为已处理。
    使用并发处理提高性能。
    
    Args:
        batch_size: 每批处理的日志数量
        max_concurrency: 并发处理的最大任务数
        
    Returns:
        Tuple[int, int, int]: 
            - 获取并尝试处理的日志数量
            - 成功范式化并存储模式的日志数量
            - 成功标记为已处理的日志数量
    """
    start_time = datetime.now(timezone.utc)
    logger.info(f"开始处理未分析的 SQL 日志，批大小: {batch_size}, 最大并发数: {max_concurrency}")
    
    try:
        # 获取未处理的日志记录
        logs_to_process = await fetch_unprocessed_logs(batch_size)
    except Exception as e:
        logger.error(f"获取未处理的日志时出错: {str(e)}", exc_info=True)
        return 0, 0, 0

    if not logs_to_process:
        logger.info("没有找到未处理的 SQL 日志")
        return 0, 0, 0

    total_fetched_count = len(logs_to_process)
    processed_successfully_count = 0
    marked_as_processed_count = 0
    
    log_ids_and_hashes_to_mark = []

    # 创建一个Semaphore对象来限制并发协程的数量
    semaphore = asyncio.Semaphore(max_concurrency)
    tasks = []

    # 辅助函数，用于包装 _process_single_log_entry 调用以使用 semaphore
    async def _process_with_semaphore(log_item):
        async with semaphore:
            return await _process_single_log_entry(log_item)

    # 为每个日志条目创建一个处理任务
    for log_item in logs_to_process:
        tasks.append(_process_with_semaphore(log_item))

    # 并发运行所有任务并收集结果
    # return_exceptions=True 确保一个任务的失败不会取消其他任务
    results = await asyncio.gather(*tasks, return_exceptions=True)

    for result in results:
        if isinstance(result, Exception):
            # 错误已在 _process_single_log_entry 中通过 exc_info=True 记录
            logger.error(f"并发处理日志时捕获到未处理异常: {result}")
        elif result is not None:
            log_id, sql_hash = result
            log_ids_and_hashes_to_mark.append((log_id, sql_hash))
            # 只有当哈希值非空时，才计算为成功泛化
            if sql_hash:
                processed_successfully_count += 1

    # 批量标记成功处理的日志
    if log_ids_and_hashes_to_mark:
        try:
            marked_as_processed_count = await mark_logs_as_processed(log_ids_and_hashes_to_mark)
        except Exception as e:
            logger.error(f"批量标记日志为已处理时出错: {str(e)}", exc_info=True)
    
    end_time = datetime.now(timezone.utc)
    duration_seconds = (end_time - start_time).total_seconds()
    logger.info(
        f"SQL 日志处理完成。耗时: {duration_seconds:.2f} 秒. "
        f"获取日志: {total_fetched_count}, 成功处理并存储模式: {processed_successfully_count}, "
        f"成功标记为已处理: {marked_as_processed_count}"
    )
    
    return total_fetched_count, processed_successfully_count, marked_as_processed_count


async def process_sql(
    raw_sql: str, 
    source_type: str,
    log_id: Optional[int] = None,
    object_id: Optional[int] = None,
    function_id: Optional[int] = None,
    execution_time_ms: Optional[float] = None,
    source_id: Optional[int] = None,
    dialect: str = 'postgres'
) -> Tuple[bool, Optional[str], Optional[str]]:
    """
    处理 SQL 语句的主函数
    
    Args:
        raw_sql: 原始 SQL 语句
        source_type: 来源类型，如 'LOG', 'VIEW', 'FUNCTION'
        log_id: 日志 ID（用于日志）
        object_id: 对象 ID（用于视图）
        function_id: 函数 ID（用于函数）
        execution_time_ms: 执行时间（毫秒），仅适用于日志
        source_id: 数据源 ID
        dialect: SQL 方言，默认为 postgres
        
    Returns:
        Tuple[bool, Optional[str], Optional[str]]: 
            - 处理是否成功
            - 标准化后的 SQL 语句
            - SQL 哈希值
    """
    # 范式化 SQL
    normalized_sql = normalize_sql(raw_sql, dialect)
    if not normalized_sql:
        return False, None, None
    
    # 生成哈希
    sql_hash = generate_sql_hash(normalized_sql)
    if not sql_hash:
        return False, normalized_sql, None
    
    # 存储 SQL 模式
    pattern_id = await store_sql_pattern(
        normalized_sql=normalized_sql,
        sql_hash=sql_hash,
        source_type=source_type,
        execution_time_ms=execution_time_ms,
        source_id=source_id,
        object_id=object_id,
        function_id=function_id
    )
    
    # 更新相关表的哈希字段
    success = False
    if source_type == 'LOG' and log_id:
        success = await update_log_sql_hash(log_id, sql_hash)
    elif (source_type == 'VIEW' and object_id) or (source_type == 'FUNCTION' and function_id):
        success = await update_metadata_sql_hash(object_id, function_id, sql_hash)
    
    return success, normalized_sql, sql_hash


if __name__ == "__main__":
    # 简单的测试代码
    setup_logging()
    
    test_sql = """
    SELECT 
        a.id, 
        b.name, 
        c.value
    FROM 
        table_a a
    JOIN 
        table_b b ON a.id = b.id
    LEFT JOIN 
        table_c c ON b.id = c.id
    WHERE 
        a.id > 100
        AND b.name LIKE 'test%'
    ORDER BY 
        a.id DESC
    LIMIT 10;
    """
    
    normalized = normalize_sql(test_sql)
    if normalized:
        sql_hash = generate_sql_hash(normalized)
        print(f"原始 SQL: {test_sql}")
        print(f"标准化 SQL: {normalized}")
        print(f"SQL 哈希: {sql_hash}")
