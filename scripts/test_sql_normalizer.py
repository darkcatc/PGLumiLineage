#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试 SQL 规范化器

此脚本用于测试 SQL 规范化器的功能，包括：
1. 测试数据库连接
2. 测试表访问权限
3. 测试 SQL 规范化和哈希生成
4. 测试日志和元数据处理

作者: Vance Chen
"""

import os
import sys
import asyncio
import argparse
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from pglumilineage.common import db_utils, models
from pglumilineage.common.logging_config import setup_logging
from pglumilineage.sql_normalizer import service as sql_normalizer_service

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)


async def test_database_connection() -> None:
    """测试数据库连接"""
    logger.info("测试数据库连接...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        logger.info("数据库连接池初始化成功")
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 测试连接
            version = await conn.fetchval("SELECT version()")
            logger.info(f"数据库连接成功，版本: {version}")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
        logger.info("数据库连接池已关闭")
    
    except Exception as e:
        logger.error(f"测试数据库连接时出错: {str(e)}")
        raise


async def test_table_access() -> None:
    """测试表访问权限"""
    logger.info("测试表访问权限...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 需要检查的表
        tables = {
            "lumi_logs.captured_logs": False,
            "lumi_metadata_store.objects_metadata": False,
            "lumi_metadata_store.functions_metadata": False,
            "lumi_analytics.sql_patterns": False
        }
        
        async with pool.acquire() as conn:
            # 检查表是否存在
            for table_name in tables.keys():
                schema, table = table_name.split(".")
                exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = $1 AND table_name = $2
                    )
                """, schema, table)
                tables[table_name] = exists
                logger.info(f"表 {table_name} {'存在' if exists else '不存在'}")
            
            # 检查表权限
            for table_name, exists in tables.items():
                if exists:
                    schema, table = table_name.split(".")
                    try:
                        # 尝试查询表中的记录数
                        count = await conn.fetchval(f"""
                            SELECT COUNT(*) FROM {schema}.{table}
                        """)
                        logger.info(f"表 {table_name} 中有 {count} 条记录，访问权限正常")
                    except Exception as e:
                        logger.error(f"访问表 {table_name} 时出错: {str(e)}")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
    
    except Exception as e:
        logger.error(f"测试表访问权限时出错: {str(e)}")
        raise


async def test_sql_normalization() -> None:
    """测试 SQL 规范化和哈希生成"""
    logger.info("测试 SQL 规范化和哈希生成...")
    
    # 测试用例
    test_cases = [
        {
            "description": "简单的 SELECT 语句",
            "sql": "SELECT * FROM users WHERE id = 123",
            "expected_normalized": None  # 简单的 SELECT 不是数据流 SQL，应该返回 None
        },
        {
            "description": "INSERT 语句",
            "sql": "INSERT INTO users (id, name, age) VALUES (123, 'John', 30)",
            "expected_contains": "INSERT INTO users (id, name, age) VALUES (?, ?, ?)"
        },
        {
            "description": "CREATE TABLE AS 语句",
            "sql": "CREATE TABLE new_users AS SELECT * FROM users WHERE age > 18",
            "expected_contains": "CREATE TABLE new_users AS SELECT * FROM users WHERE age > ?"
        },
        {
            "description": "UPDATE 语句",
            "sql": "UPDATE users SET name = 'John', age = 30 WHERE id = 123",
            "expected_contains": "UPDATE users SET name = ?, age = ? WHERE id = ?"
        },
        {
            "description": "带有注释的 SQL",
            "sql": "-- 这是一个注释\nINSERT INTO users (id, name) VALUES (123, 'John') -- 插入用户",
            "expected_contains": "INSERT INTO users (id, name) VALUES (?, ?)"
        }
    ]
    
    for case in test_cases:
        sql = case["sql"]
        description = case["description"]
        
        # 规范化 SQL
        normalized_sql = sql_normalizer_service.normalize_sql(sql)
        
        if "expected_normalized" in case:
            if normalized_sql == case["expected_normalized"]:
                logger.info(f"测试用例 '{description}' 规范化成功: {normalized_sql}")
            else:
                logger.error(f"测试用例 '{description}' 规范化失败: 期望 {case['expected_normalized']}，实际 {normalized_sql}")
        
        elif "expected_contains" in case and normalized_sql is not None:
            if case["expected_contains"] in normalized_sql:
                logger.info(f"测试用例 '{description}' 规范化成功: {normalized_sql}")
            else:
                logger.error(f"测试用例 '{description}' 规范化失败: 期望包含 {case['expected_contains']}，实际 {normalized_sql}")
        
        # 如果规范化成功，生成哈希
        if normalized_sql is not None:
            sql_hash = sql_normalizer_service.generate_sql_hash(normalized_sql)
            logger.info(f"SQL 哈希: {sql_hash}")


async def test_process_logs() -> None:
    """测试处理日志记录"""
    logger.info("测试处理日志记录...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 获取未处理的日志记录
        logs = await sql_normalizer_service.fetch_unprocessed_logs(limit=5)
        logger.info(f"获取到 {len(logs)} 条未处理的日志记录")
        
        # 处理日志记录
        if logs:
            for log in logs:
                logger.info(f"处理日志记录: ID={log.log_id}, SQL={log.sql_text[:50]}...")
                
                success, normalized_sql, sql_hash = await sql_normalizer_service.process_sql(
                    raw_sql=log.sql_text,
                    source_type="LOG",
                    log_id=log.log_id,
                    execution_time_ms=log.duration_ms
                )
                
                if success:
                    logger.info(f"日志记录处理成功: 哈希={sql_hash}")
                else:
                    logger.info(f"日志记录处理跳过: 不是数据流 SQL 或规范化失败")
        
        # 批量处理日志记录
        total_logs, processed_logs, marked_logs = await sql_normalizer_service.process_captured_logs(
            batch_size=10,
            max_concurrency=5
        )
        logger.info(f"批量处理日志记录: 总计={total_logs}, 处理={processed_logs}, 标记={marked_logs}")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
    
    except Exception as e:
        logger.error(f"测试处理日志记录时出错: {str(e)}")
        raise


async def test_process_metadata() -> None:
    """测试处理元数据定义"""
    logger.info("测试处理元数据定义...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 获取未处理的视图定义
        views = await sql_normalizer_service.fetch_unprocessed_view_definitions()
        logger.info(f"获取到 {len(views)} 个未处理的视图定义")
        
        # 获取未处理的函数定义
        functions = await sql_normalizer_service.fetch_unprocessed_function_definitions()
        logger.info(f"获取到 {len(functions)} 个未处理的函数定义")
        
        # 处理元数据定义
        view_count, func_count, normalized_count, updated_count = await sql_normalizer_service.process_metadata_definitions()
        logger.info(f"处理元数据定义: 视图={view_count}, 函数={func_count}, 规范化={normalized_count}, 更新={updated_count}")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
    
    except Exception as e:
        logger.error(f"测试处理元数据定义时出错: {str(e)}")
        raise


async def test_sql_patterns_table() -> None:
    """测试 SQL 模式表"""
    logger.info("测试 SQL 模式表...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 检查 SQL 模式表中的记录数
            count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_analytics.sql_patterns
            """)
            logger.info(f"SQL 模式表中有 {count} 条记录")
            
            # 获取最近的 5 条记录
            rows = await conn.fetch("""
                SELECT pattern_id, sql_hash, normalized_sql, source_type, execution_count, avg_execution_time_ms
                FROM lumi_analytics.sql_patterns
                ORDER BY last_seen_at DESC
                LIMIT 5
            """)
            
            logger.info(f"最近的 {len(rows)} 条 SQL 模式记录:")
            for row in rows:
                logger.info(f"  ID={row['pattern_id']}, 哈希={row['sql_hash'][:8]}..., 类型={row['source_type']}, 执行次数={row['execution_count']}, 平均执行时间={row['avg_execution_time_ms']:.2f}ms")
                logger.info(f"  SQL: {row['normalized_sql'][:100]}...")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
    
    except Exception as e:
        logger.error(f"测试 SQL 模式表时出错: {str(e)}")
        raise


async def main() -> None:
    """主函数"""
    parser = argparse.ArgumentParser(description="测试 SQL 规范化器")
    parser.add_argument("--db", action="store_true", help="测试数据库连接")
    parser.add_argument("--tables", action="store_true", help="测试表访问权限")
    parser.add_argument("--normalize", action="store_true", help="测试 SQL 规范化和哈希生成")
    parser.add_argument("--logs", action="store_true", help="测试处理日志记录")
    parser.add_argument("--metadata", action="store_true", help="测试处理元数据定义")
    parser.add_argument("--patterns", action="store_true", help="测试 SQL 模式表")
    parser.add_argument("--all", action="store_true", help="测试所有功能")
    
    args = parser.parse_args()
    
    # 如果没有指定任何参数，则显示帮助信息
    if not any([args.db, args.tables, args.normalize, args.logs, args.metadata, args.patterns, args.all]):
        parser.print_help()
        return
    
    try:
        # 测试数据库连接
        if args.db or args.all:
            await test_database_connection()
            print()
        
        # 测试表访问权限
        if args.tables or args.all:
            await test_table_access()
            print()
        
        # 测试 SQL 规范化和哈希生成
        if args.normalize or args.all:
            await test_sql_normalization()
            print()
        
        # 测试处理日志记录
        if args.logs or args.all:
            await test_process_logs()
            print()
        
        # 测试处理元数据定义
        if args.metadata or args.all:
            await test_process_metadata()
            print()
        
        # 测试 SQL 模式表
        if args.patterns or args.all:
            await test_sql_patterns_table()
            print()
        
        logger.info("测试完成")
    
    except Exception as e:
        logger.error(f"测试过程中出错: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"\n程序出错: {str(e)}")
        import traceback
        traceback.print_exc()
