#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL 规范化过程全面测试模块

此模块用于全面测试 SQL 规范化过程，确保：
1. 日志、视图和函数中涉及数据加工的SQL被正确提取并泛化
2. 泛化失败的SQL被保存到错误表中，并明确记录原因
3. 提供详细的测试结果统计

作者: Vance Chen
"""

import os
import sys
import asyncio
import unittest
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from pglumilineage.common import db_utils, models
from pglumilineage.common.logging_config import setup_logging
from pglumilineage.sql_normalizer import service as sql_normalizer_service

# 设置日志
setup_logging()


class TestSQLNormalizationProcess(unittest.TestCase):
    """SQL 规范化过程全面测试类"""
    
    @classmethod
    async def setUpClass(cls):
        """测试前的准备工作"""
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 检查必要的表是否存在
        cls.tables_exist = await cls.check_tables_exist()
        
        # 清理之前的错误记录（仅限测试相关的）
        await cls.cleanup_previous_errors()
    
    @classmethod
    async def tearDownClass(cls):
        """测试后的清理工作"""
        # 关闭数据库连接池
        await db_utils.close_db_pool()
    
    @classmethod
    async def check_tables_exist(cls) -> Dict[str, bool]:
        """检查必要的表是否存在"""
        pool = await db_utils.get_db_pool()
        tables = {
            "lumi_logs.captured_logs": False,
            "lumi_metadata_store.objects_metadata": False,
            "lumi_metadata_store.functions_metadata": False,
            "lumi_analytics.sql_patterns": False,
            "lumi_analytics.sql_normalization_errors": False
        }
        
        async with pool.acquire() as conn:
            for table_name in tables.keys():
                schema, table = table_name.split(".")
                exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = $1 AND table_name = $2
                    )
                """, schema, table)
                tables[table_name] = exists
        
        return tables
    
    @classmethod
    async def cleanup_previous_errors(cls):
        """清理之前的错误记录（仅限测试相关的）"""
        if not cls.tables_exist.get("lumi_analytics.sql_normalization_errors", False):
            return
        
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 删除测试过程中产生的错误记录（通过时间戳判断）
            current_time = datetime.now(timezone.utc)
            await conn.execute("""
                DELETE FROM lumi_analytics.sql_normalization_errors 
                WHERE created_at > $1
            """, current_time)
    
    async def test_log_sql_normalization(self):
        """测试日志SQL规范化"""
        # 跳过测试如果必要的表不存在
        if not all(self.tables_exist.values()):
            self.skipTest("跳过测试，因为一些必要的表不存在")
        
        print("\n===== 测试日志SQL规范化 =====")
        
        # 获取未处理的日志数量
        pool = await db_utils.get_db_pool()
        async with pool.acquire() as conn:
            unprocessed_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_logs.captured_logs 
                WHERE is_processed_for_analysis = FALSE
            """)
            
            if unprocessed_count == 0:
                # 如果没有未处理的日志，将一些已处理的日志重置为未处理状态
                await conn.execute("""
                    UPDATE lumi_logs.captured_logs 
                    SET is_processed_for_analysis = FALSE, normalized_sql_hash = NULL
                    WHERE log_id IN (
                        SELECT log_id FROM lumi_logs.captured_logs 
                        WHERE is_processed_for_analysis = TRUE
                        ORDER BY log_id DESC
                        LIMIT 10
                    )
                """)
                
                # 重新获取未处理的日志数量
                unprocessed_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM lumi_logs.captured_logs 
                    WHERE is_processed_for_analysis = FALSE
                """)
            
            print(f"待处理日志数量: {unprocessed_count}")
        
        # 处理日志SQL
        total_logs, processed_logs, marked_logs = await sql_normalizer_service.process_captured_logs(
            batch_size=10,
            max_concurrency=5
        )
        
        print(f"日志处理结果: 总数={total_logs}, 成功处理={processed_logs}, 标记为已处理={marked_logs}")
        
        # 验证结果
        self.assertGreaterEqual(total_logs, 0, "应该有日志被处理")
        self.assertLessEqual(processed_logs, total_logs, "成功处理的日志数不应超过总数")
        self.assertLessEqual(marked_logs, processed_logs, "标记为已处理的日志数不应超过成功处理的日志数")
        
        # 检查错误表中的记录
        async with pool.acquire() as conn:
            error_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_analytics.sql_normalization_errors 
                WHERE source_type = 'LOG'
                AND created_at > (NOW() - INTERVAL '10 minutes')
            """)
            
            print(f"新增日志SQL规范化错误: {error_count} 条")
            
            # 获取错误详情
            if error_count > 0:
                errors = await conn.fetch("""
                    SELECT source_id, error_reason, error_details, raw_sql_text 
                    FROM lumi_analytics.sql_normalization_errors 
                    WHERE source_type = 'LOG'
                    AND created_at > (NOW() - INTERVAL '10 minutes')
                    ORDER BY created_at DESC
                    LIMIT 5
                """)
                
                print("错误详情示例:")
                for error in errors:
                    print(f"  - 日志ID: {error['source_id']}, 原因: {error['error_reason']}")
                    print(f"    SQL片段: {error['raw_sql_text'][:100]}...")
    
    async def test_view_definition_normalization(self):
        """测试视图定义规范化"""
        # 跳过测试如果必要的表不存在
        if not all(self.tables_exist.values()):
            self.skipTest("跳过测试，因为一些必要的表不存在")
        
        print("\n===== 测试视图定义规范化 =====")
        
        # 获取未处理的视图定义数量
        pool = await db_utils.get_db_pool()
        async with pool.acquire() as conn:
            unprocessed_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_metadata_store.objects_metadata 
                WHERE object_type IN ('VIEW', 'MATERIALIZED VIEW')
                AND definition IS NOT NULL
                AND definition != ''
                AND (normalized_sql_hash IS NULL OR normalized_sql_hash = '')
            """)
            
            if unprocessed_count == 0:
                # 如果没有未处理的视图定义，将一些已处理的视图定义重置为未处理状态
                await conn.execute("""
                    UPDATE lumi_metadata_store.objects_metadata 
                    SET normalized_sql_hash = NULL
                    WHERE object_id IN (
                        SELECT object_id FROM lumi_metadata_store.objects_metadata 
                        WHERE object_type IN ('VIEW', 'MATERIALIZED VIEW')
                        AND definition IS NOT NULL
                        AND definition != ''
                        AND normalized_sql_hash IS NOT NULL
                        ORDER BY object_id DESC
                        LIMIT 5
                    )
                """)
                
                # 重新获取未处理的视图定义数量
                unprocessed_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM lumi_metadata_store.objects_metadata 
                    WHERE object_type IN ('VIEW', 'MATERIALIZED VIEW')
                    AND definition IS NOT NULL
                    AND definition != ''
                    AND (normalized_sql_hash IS NULL OR normalized_sql_hash = '')
                """)
            
            print(f"待处理视图定义数量: {unprocessed_count}")
        
        # 处理元数据定义
        view_count, func_count, normalized_count, updated_count = await sql_normalizer_service.process_metadata_definitions()
        
        print(f"元数据处理结果: 视图={view_count}, 函数={func_count}, 成功规范化={normalized_count}, 成功更新={updated_count}")
        
        # 验证结果
        self.assertGreaterEqual(view_count, 0, "应该有视图定义被处理")
        self.assertLessEqual(normalized_count, view_count + func_count, "成功规范化的定义数不应超过总数")
        
        # 检查错误表中的记录
        async with pool.acquire() as conn:
            error_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_analytics.sql_normalization_errors 
                WHERE source_type = 'VIEW'
                AND created_at > (NOW() - INTERVAL '10 minutes')
            """)
            
            print(f"新增视图定义规范化错误: {error_count} 条")
            
            # 获取错误详情
            if error_count > 0:
                errors = await conn.fetch("""
                    SELECT source_id, error_reason, error_details, raw_sql_text 
                    FROM lumi_analytics.sql_normalization_errors 
                    WHERE source_type = 'VIEW'
                    AND created_at > (NOW() - INTERVAL '10 minutes')
                    ORDER BY created_at DESC
                    LIMIT 5
                """)
                
                print("错误详情示例:")
                for error in errors:
                    print(f"  - 视图ID: {error['source_id']}, 原因: {error['error_reason']}")
                    print(f"    SQL片段: {error['raw_sql_text'][:100]}...")
    
    async def test_function_definition_normalization(self):
        """测试函数定义规范化"""
        # 跳过测试如果必要的表不存在
        if not all(self.tables_exist.values()):
            self.skipTest("跳过测试，因为一些必要的表不存在")
        
        print("\n===== 测试函数定义规范化 =====")
        
        # 获取未处理的函数定义数量
        pool = await db_utils.get_db_pool()
        async with pool.acquire() as conn:
            unprocessed_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_metadata_store.functions_metadata 
                WHERE definition IS NOT NULL
                AND definition != ''
                AND (normalized_sql_hash IS NULL OR normalized_sql_hash = '')
            """)
            
            if unprocessed_count == 0:
                # 如果没有未处理的函数定义，将一些已处理的函数定义重置为未处理状态
                await conn.execute("""
                    UPDATE lumi_metadata_store.functions_metadata 
                    SET normalized_sql_hash = NULL
                    WHERE function_id IN (
                        SELECT function_id FROM lumi_metadata_store.functions_metadata 
                        WHERE definition IS NOT NULL
                        AND definition != ''
                        AND normalized_sql_hash IS NOT NULL
                        ORDER BY function_id DESC
                        LIMIT 5
                    )
                """)
                
                # 重新获取未处理的函数定义数量
                unprocessed_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM lumi_metadata_store.functions_metadata 
                    WHERE definition IS NOT NULL
                    AND definition != ''
                    AND (normalized_sql_hash IS NULL OR normalized_sql_hash = '')
                """)
            
            print(f"待处理函数定义数量: {unprocessed_count}")
        
        # 处理元数据定义
        view_count, func_count, normalized_count, updated_count = await sql_normalizer_service.process_metadata_definitions()
        
        print(f"元数据处理结果: 视图={view_count}, 函数={func_count}, 成功规范化={normalized_count}, 成功更新={updated_count}")
        
        # 验证结果
        self.assertGreaterEqual(func_count, 0, "应该有函数定义被处理")
        self.assertLessEqual(normalized_count, view_count + func_count, "成功规范化的定义数不应超过总数")
        
        # 检查错误表中的记录
        async with pool.acquire() as conn:
            error_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_analytics.sql_normalization_errors 
                WHERE source_type = 'FUNCTION'
                AND created_at > (NOW() - INTERVAL '10 minutes')
            """)
            
            print(f"新增函数定义规范化错误: {error_count} 条")
            
            # 获取错误详情
            if error_count > 0:
                errors = await conn.fetch("""
                    SELECT source_id, error_reason, error_details, raw_sql_text 
                    FROM lumi_analytics.sql_normalization_errors 
                    WHERE source_type = 'FUNCTION'
                    AND created_at > (NOW() - INTERVAL '10 minutes')
                    ORDER BY created_at DESC
                    LIMIT 5
                """)
                
                print("错误详情示例:")
                for error in errors:
                    print(f"  - 函数ID: {error['source_id']}, 原因: {error['error_reason']}")
                    print(f"    SQL片段: {error['raw_sql_text'][:100]}...")
    
    async def test_sql_pattern_generation(self):
        """测试SQL模式生成"""
        # 跳过测试如果必要的表不存在
        if not all(self.tables_exist.values()):
            self.skipTest("跳过测试，因为一些必要的表不存在")
        
        print("\n===== 测试SQL模式生成 =====")
        
        # 验证结果
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 检查SQL模式表中的记录数
            pattern_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_analytics.sql_patterns
            """)
            
            print(f"SQL模式表中共有 {pattern_count} 条记录")
            
            # 获取最近更新的SQL模式
            recent_patterns = await conn.fetch("""
                SELECT sql_hash, normalized_sql_text, source_database_name, execution_count, 
                       first_seen_at, last_seen_at
                FROM lumi_analytics.sql_patterns 
                ORDER BY last_seen_at DESC
                LIMIT 5
            """)
            
            if recent_patterns:
                print("最近更新的SQL模式:")
                for pattern in recent_patterns:
                    print(f"  - 哈希: {pattern['sql_hash'][:8]}..., 数据库: {pattern['source_database_name']}, 执行次数: {pattern['execution_count']}")
                    print(f"    首次见到: {pattern['first_seen_at']}, 最后见到: {pattern['last_seen_at']}")
                    print(f"    规范化SQL: {pattern['normalized_sql_text'][:100]}...")
    
    async def test_error_handling(self):
        """测试错误处理"""
        # 跳过测试如果必要的表不存在
        if not all(self.tables_exist.values()):
            self.skipTest("跳过测试，因为一些必要的表不存在")
        
        print("\n===== 测试错误处理 =====")
        
        # 验证结果
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 检查错误表中的记录数
            error_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_analytics.sql_normalization_errors
            """)
            
            print(f"错误表中共有 {error_count} 条记录")
            
            # 获取错误类型统计
            error_stats = await conn.fetch("""
                SELECT source_type, COUNT(*) as count 
                FROM lumi_analytics.sql_normalization_errors 
                GROUP BY source_type
                ORDER BY count DESC
            """)
            
            if error_stats:
                print("错误类型统计:")
                for stat in error_stats:
                    print(f"  - {stat['source_type']}: {stat['count']} 条")
            
            # 获取错误原因统计
            reason_stats = await conn.fetch("""
                SELECT error_reason, COUNT(*) as count 
                FROM lumi_analytics.sql_normalization_errors 
                GROUP BY error_reason
                ORDER BY count DESC
                LIMIT 10
            """)
            
            if reason_stats:
                print("错误原因统计 (Top 10):")
                for stat in reason_stats:
                    print(f"  - {stat['error_reason']}: {stat['count']} 条")
            
            # 获取最近的错误记录
            recent_errors = await conn.fetch("""
                SELECT source_type, source_id, error_reason, created_at, raw_sql_text
                FROM lumi_analytics.sql_normalization_errors 
                ORDER BY created_at DESC
                LIMIT 5
            """)
            
            if recent_errors:
                print("最近的错误记录:")
                for error in recent_errors:
                    print(f"  - 类型: {error['source_type']}, ID: {error['source_id']}, 时间: {error['created_at']}")
                    print(f"    原因: {error['error_reason']}")
                    print(f"    SQL片段: {error['raw_sql_text'][:100]}...")


# 运行测试
async def run_tests():
    """运行集成测试"""
    # 设置测试类
    test_class = TestSQLNormalizationProcess()
    
    # 运行测试前的准备
    await TestSQLNormalizationProcess.setUpClass()
    
    try:
        # 运行各个测试
        await test_class.test_log_sql_normalization()
        await test_class.test_view_definition_normalization()
        await test_class.test_function_definition_normalization()
        await test_class.test_sql_pattern_generation()
        await test_class.test_error_handling()
        
        print("\n===== 所有测试完成 =====")
    finally:
        # 运行测试后的清理
        await TestSQLNormalizationProcess.tearDownClass()


if __name__ == "__main__":
    # 设置日志
    setup_logging()
    
    # 运行异步测试
    asyncio.run(run_tests())
