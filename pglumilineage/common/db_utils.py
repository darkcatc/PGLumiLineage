#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库工具模块

提供数据库连接池和常用数据库操作函数。

作者: Vance Chen
"""

import asyncio
import logging
from typing import Dict, Any, List, Optional, Union

import asyncpg

from pglumilineage.common.config import settings
from pglumilineage.common.models import RawSQLLog, AnalyticalSQLPattern

# 全局连接池
db_pool = None

# 获取日志记录器
logger = logging.getLogger(__name__)


async def init_db_pool() -> None:
    """
    初始化数据库连接池
    
    使用配置中的DSN创建asyncpg连接池
    """
    global db_pool
    
    if db_pool is not None:
        logger.info("数据库连接池已存在，跳过初始化")
        return
    
    try:
        # 使用RAW_LOGS_DSN创建连接池
        # 注意：settings.RAW_LOGS_DSN是PostgresDsn类型，需要转换为字符串
        dsn = str(settings.RAW_LOGS_DSN)
        logger.info(f"正在初始化数据库连接池，DSN: {dsn.replace(settings.POSTGRES_PASSWORD.get_secret_value(), '****')}")
        
        db_pool = await asyncpg.create_pool(
            dsn=dsn,
            min_size=5,
            max_size=20,
            command_timeout=60,
            timeout=10
        )
        
        logger.info("数据库连接池初始化成功")
    except Exception as e:
        logger.error(f"初始化数据库连接池失败: {str(e)}")
        raise


async def get_db_pool() -> asyncpg.Pool:
    """
    获取数据库连接池
    
    如果连接池不存在，则初始化它
    
    Returns:
        asyncpg.Pool: 数据库连接池
    """
    global db_pool
    
    if db_pool is None:
        await init_db_pool()
    
    return db_pool


async def close_db_pool() -> None:
    """
    关闭数据库连接池
    """
    global db_pool
    
    if db_pool is None:
        logger.warning("数据库连接池不存在，无需关闭")
        return
    
    try:
        logger.info("正在关闭数据库连接池")
        await db_pool.close()
        db_pool = None
        logger.info("数据库连接池已关闭")
    except Exception as e:
        logger.error(f"关闭数据库连接池失败: {str(e)}")
        raise


async def execute_ddl(ddl_statement: str) -> None:
    """
    执行DDL语句
    
    Args:
        ddl_statement: DDL语句
    """
    global db_pool
    
    if db_pool is None:
        raise RuntimeError("数据库连接池未初始化，请先调用init_db_pool()")
    
    try:
        logger.debug(f"执行DDL语句: {ddl_statement[:100]}...")
        async with db_pool.acquire() as conn:
            await conn.execute(ddl_statement)
        logger.debug("DDL语句执行成功")
    except Exception as e:
        logger.error(f"执行DDL语句失败: {str(e)}")
        raise


async def insert_raw_log(log_entry: RawSQLLog) -> int:
    """
    插入原始SQL日志
    
    Args:
        log_entry: 原始SQL日志对象
        
    Returns:
        int: 插入记录的ID
    """
    global db_pool
    
    if db_pool is None:
        raise RuntimeError("数据库连接池未初始化，请先调用init_db_pool()")
    
    try:
        # 将Pydantic模型转换为字典
        data = log_entry.model_dump(exclude={"log_id"})
        
        # 构建SQL语句
        fields = ", ".join(data.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(data)))
        values = list(data.values())
        
        # 执行插入
        async with db_pool.acquire() as conn:
            result = await conn.fetchval(
                f"""
                INSERT INTO lumi_logs.captured_logs 
                ({fields}) 
                VALUES ({placeholders})
                RETURNING log_id
                """,
                *values
            )
            
        logger.debug(f"插入原始SQL日志成功，ID: {result}")
        return result
    except Exception as e:
        logger.error(f"插入原始SQL日志失败: {str(e)}")
        raise


async def insert_sql_pattern(pattern: AnalyticalSQLPattern) -> str:
    """
    插入分析SQL模式
    
    Args:
        pattern: 分析SQL模式对象
        
    Returns:
        str: 插入记录的哈希值
    """
    global db_pool
    
    if db_pool is None:
        raise RuntimeError("数据库连接池未初始化，请先调用init_db_pool()")
    
    try:
        # 将Pydantic模型转换为字典
        data = pattern.model_dump()
        
        # 构建SQL语句
        fields = ", ".join(data.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(data)))
        values = list(data.values())
        
        # 执行插入
        async with db_pool.acquire() as conn:
            result = await conn.fetchval(
                f"""
                INSERT INTO lumi_analytics.sql_patterns 
                ({fields}) 
                VALUES ({placeholders})
                ON CONFLICT (sql_hash) DO UPDATE 
                SET 
                    last_seen_at = EXCLUDED.last_seen_at,
                    execution_count = lumi_analytics.sql_patterns.execution_count + 1,
                    total_duration_ms = lumi_analytics.sql_patterns.total_duration_ms + EXCLUDED.total_duration_ms,
                    avg_duration_ms = (lumi_analytics.sql_patterns.total_duration_ms + EXCLUDED.total_duration_ms) / 
                                      (lumi_analytics.sql_patterns.execution_count + 1),
                    max_duration_ms = GREATEST(lumi_analytics.sql_patterns.max_duration_ms, EXCLUDED.max_duration_ms),
                    min_duration_ms = LEAST(lumi_analytics.sql_patterns.min_duration_ms, EXCLUDED.min_duration_ms)
                RETURNING sql_hash
                """,
                *values
            )
            
        logger.debug(f"插入分析SQL模式成功，哈希值: {result}")
        return result
    except Exception as e:
        logger.error(f"插入分析SQL模式失败: {str(e)}")
        raise


async def insert_data(table_name: str, data: Dict[str, Any]) -> Any:
    """
    通用数据插入函数
    
    Args:
        table_name: 表名（包含schema）
        data: 要插入的数据字典
        
    Returns:
        Any: 插入记录的主键值
    """
    global db_pool
    
    if db_pool is None:
        raise RuntimeError("数据库连接池未初始化，请先调用init_db_pool()")
    
    try:
        # 构建SQL语句
        fields = ", ".join(data.keys())
        placeholders = ", ".join(f"${i+1}" for i in range(len(data)))
        values = list(data.values())
        
        # 获取表的主键列
        async with db_pool.acquire() as conn:
            primary_key = await conn.fetchval(
                """
                SELECT a.attname
                FROM pg_index i
                JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                WHERE i.indrelid = $1::regclass AND i.indisprimary
                LIMIT 1
                """,
                table_name
            )
            
            # 执行插入
            result = await conn.fetchval(
                f"""
                INSERT INTO {table_name} 
                ({fields}) 
                VALUES ({placeholders})
                RETURNING {primary_key}
                """,
                *values
            )
            
        logger.debug(f"插入数据到 {table_name} 成功，主键: {result}")
        return result
    except Exception as e:
        logger.error(f"插入数据到 {table_name} 失败: {str(e)}")
        raise


async def execute_query(query: str, *args) -> List[Dict[str, Any]]:
    """
    执行查询并返回结果
    
    Args:
        query: SQL查询语句
        *args: 查询参数
        
    Returns:
        List[Dict[str, Any]]: 查询结果列表
    """
    global db_pool
    
    if db_pool is None:
        raise RuntimeError("数据库连接池未初始化，请先调用init_db_pool()")
    
    try:
        async with db_pool.acquire() as conn:
            result = await conn.fetch(query, *args)
            
        # 将结果转换为字典列表
        return [dict(row) for row in result]
    except Exception as e:
        logger.error(f"执行查询失败: {str(e)}")
        raise
