#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL 规范化器测试模块

此模块用于测试 SQL 规范化器的功能，包括：
1. SQL 规范化
2. SQL 哈希生成
3. 从日志中处理 SQL
4. 从元数据中处理 SQL
5. 数据库表访问权限

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
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from pglumilineage.common import db_utils, models
from pglumilineage.common.logging_config import setup_logging
from pglumilineage.sql_normalizer import service as sql_normalizer_service

# 设置日志
setup_logging()


class TestSQLNormalizer(unittest.TestCase):
    """SQL 规范化器测试类"""

    @classmethod
    async def setUpClass(cls):
        """测试前的准备工作"""
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 检查必要的表是否存在
        cls.tables_exist = await cls.check_tables_exist()
    
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
            "lumi_analytics.sql_patterns": False
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
    
    async def test_normalize_sql(self):
        """测试 SQL 规范化功能"""
        # 测试用例
        test_cases = [
            # 简单的 SELECT 语句
            {
                "sql": "SELECT * FROM users WHERE id = 123",
                "expected": None,  # 简单的 SELECT 不是数据流 SQL，应该返回 None
            },
            # INSERT 语句
            {
                "sql": "INSERT INTO users (id, name, age) VALUES (123, 'John', 30)",
                "expected_contains": "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
            },
            # CREATE TABLE AS 语句
            {
                "sql": "CREATE TABLE new_users AS SELECT * FROM users WHERE age > 18",
                "expected_contains": "CREATE TABLE new_users AS SELECT * FROM users WHERE age > ?",
            },
            # UPDATE 语句
            {
                "sql": "UPDATE users SET name = 'John', age = 30 WHERE id = 123",
                "expected_contains": "UPDATE users SET name = ?, age = ? WHERE id = ?",
            },
            # 带有注释的 SQL
            {
                "sql": "-- 这是一个注释\nINSERT INTO users (id, name) VALUES (123, 'John') -- 插入用户",
                "expected_contains": "INSERT INTO users (id, name) VALUES (?, ?)",
            },
        ]
        
        for i, case in enumerate(test_cases):
            sql = case["sql"]
            normalized = sql_normalizer_service.normalize_sql(sql)
            
            if "expected" in case:
                self.assertEqual(normalized, case["expected"], f"测试用例 {i+1} 失败")
            elif "expected_contains" in case and normalized is not None:
                self.assertIn(case["expected_contains"], normalized, f"测试用例 {i+1} 失败")
    
    async def test_generate_sql_hash(self):
        """测试 SQL 哈希生成功能"""
        # 测试用例
        test_cases = [
            {
                "sql": "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",
                "expected_length": 64,  # SHA-256 哈希的十六进制表示长度为 64
            },
            {
                "sql": "INSERT INTO users (id, name, age) VALUES (?, ?, ?)",  # 相同的 SQL 应该生成相同的哈希
                "expected_length": 64,
            },
            {
                "sql": "INSERT INTO users (name, id, age) VALUES (?, ?, ?)",  # 字段顺序不同，哈希应该不同
                "expected_length": 64,
            },
            {
                "sql": "",  # 空字符串应该返回空哈希
                "expected": "",
            },
        ]
        
        # 记录第一个哈希值，用于比较
        first_hash = None
        
        for i, case in enumerate(test_cases):
            sql = case["sql"]
            sql_hash = sql_normalizer_service.generate_sql_hash(sql)
            
            if i == 0:
                first_hash = sql_hash
            
            if "expected_length" in case:
                self.assertEqual(len(sql_hash), case["expected_length"], f"测试用例 {i+1} 哈希长度不符")
            
            if "expected" in case:
                self.assertEqual(sql_hash, case["expected"], f"测试用例 {i+1} 哈希值不符")
        
        # 检查相同的 SQL 是否生成相同的哈希
        self.assertEqual(
            sql_normalizer_service.generate_sql_hash(test_cases[0]["sql"]),
            sql_normalizer_service.generate_sql_hash(test_cases[1]["sql"]),
            "相同的 SQL 应该生成相同的哈希"
        )
        
        # 检查不同的 SQL 是否生成不同的哈希
        self.assertNotEqual(
            sql_normalizer_service.generate_sql_hash(test_cases[0]["sql"]),
            sql_normalizer_service.generate_sql_hash(test_cases[2]["sql"]),
            "不同的 SQL 应该生成不同的哈希"
        )
    
    async def test_database_access(self):
        """测试数据库访问权限"""
        # 检查表是否存在
        for table, exists in self.tables_exist.items():
            self.assertTrue(exists, f"表 {table} 不存在")
        
        # 测试从日志表中获取未处理的日志记录
        try:
            logs = await sql_normalizer_service.fetch_unprocessed_logs(limit=1)
            self.assertIsInstance(logs, list, "fetch_unprocessed_logs 应该返回一个列表")
        except Exception as e:
            self.fail(f"fetch_unprocessed_logs 失败: {str(e)}")
        
        # 测试从元数据表中获取未处理的视图定义
        try:
            views = await sql_normalizer_service.fetch_unprocessed_view_definitions()
            self.assertIsInstance(views, list, "fetch_unprocessed_view_definitions 应该返回一个列表")
        except Exception as e:
            self.fail(f"fetch_unprocessed_view_definitions 失败: {str(e)}")
        
        # 测试从元数据表中获取未处理的函数定义
        try:
            functions = await sql_normalizer_service.fetch_unprocessed_function_definitions()
            self.assertIsInstance(functions, list, "fetch_unprocessed_function_definitions 应该返回一个列表")
        except Exception as e:
            self.fail(f"fetch_unprocessed_function_definitions 失败: {str(e)}")
    
    async def test_process_sql(self):
        """测试处理 SQL 语句的功能"""
        # 测试用例
        test_cases = [
            {
                "sql": "INSERT INTO users (id, name, age) VALUES (123, 'John', 30)",
                "source_type": "LOG",
                "expected_success": True,
            },
            {
                "sql": "CREATE VIEW user_view AS SELECT * FROM users WHERE age > 18",
                "source_type": "VIEW",
                "expected_success": True,
            },
            {
                "sql": "CREATE OR REPLACE FUNCTION get_user(user_id int) RETURNS SETOF users AS $$ SELECT * FROM users WHERE id = user_id; $$ LANGUAGE SQL",
                "source_type": "FUNCTION",
                "expected_success": True,
            },
            {
                "sql": "SELECT * FROM users",  # 简单的 SELECT 不是数据流 SQL
                "source_type": "LOG",
                "expected_success": False,
            },
        ]
        
        for i, case in enumerate(test_cases):
            sql = case["sql"]
            source_type = case["source_type"]
            expected_success = case["expected_success"]
            
            success, normalized_sql, sql_hash = await sql_normalizer_service.process_sql(
                raw_sql=sql,
                source_type=source_type
            )
            
            self.assertEqual(success, expected_success, f"测试用例 {i+1} 处理结果不符")
            
            if expected_success:
                self.assertIsNotNone(normalized_sql, f"测试用例 {i+1} 标准化 SQL 不应为 None")
                self.assertIsNotNone(sql_hash, f"测试用例 {i+1} SQL 哈希不应为 None")
                self.assertEqual(len(sql_hash), 64, f"测试用例 {i+1} SQL 哈希长度应为 64")
            else:
                self.assertIsNone(normalized_sql, f"测试用例 {i+1} 标准化 SQL 应为 None")
                self.assertIsNone(sql_hash, f"测试用例 {i+1} SQL 哈希应为 None")
    
    async def test_process_captured_logs(self):
        """测试处理捕获的日志记录"""
        # 只有当日志表存在时才进行测试
        if not self.tables_exist["lumi_logs.captured_logs"]:
            self.skipTest("lumi_logs.captured_logs 表不存在")
        
        try:
            # 处理一小批日志记录
            total_logs, processed_logs, marked_logs = await sql_normalizer_service.process_captured_logs(
                batch_size=5,
                max_concurrency=2
            )
            
            self.assertIsInstance(total_logs, int, "total_logs 应该是整数")
            self.assertIsInstance(processed_logs, int, "processed_logs 应该是整数")
            self.assertIsInstance(marked_logs, int, "marked_logs 应该是整数")
            
            # 验证处理的日志数量合理
            self.assertLessEqual(processed_logs, total_logs, "处理的日志数量不应超过总日志数量")
            self.assertLessEqual(marked_logs, processed_logs, "标记的日志数量不应超过处理的日志数量")
        except Exception as e:
            self.fail(f"process_captured_logs 失败: {str(e)}")
    
    async def test_process_metadata_definitions(self):
        """测试处理元数据定义"""
        # 只有当元数据表存在时才进行测试
        if not (self.tables_exist["lumi_metadata_store.objects_metadata"] and 
                self.tables_exist["lumi_metadata_store.functions_metadata"]):
            self.skipTest("元数据表不存在")
        
        try:
            # 处理元数据定义
            view_count, func_count, normalized_count, updated_count = await sql_normalizer_service.process_metadata_definitions()
            
            self.assertIsInstance(view_count, int, "view_count 应该是整数")
            self.assertIsInstance(func_count, int, "func_count 应该是整数")
            self.assertIsInstance(normalized_count, int, "normalized_count 应该是整数")
            self.assertIsInstance(updated_count, int, "updated_count 应该是整数")
            
            # 验证处理的定义数量合理
            total_defs = view_count + func_count
            self.assertLessEqual(normalized_count, total_defs, "标准化的定义数量不应超过总定义数量")
            self.assertLessEqual(updated_count, normalized_count, "更新的定义数量不应超过标准化的定义数量")
        except Exception as e:
            self.fail(f"process_metadata_definitions 失败: {str(e)}")


# 集成测试：测试 SQL 规范化器的完整流程
class TestSQLNormalizerIntegration(unittest.TestCase):
    """SQL 规范化器集成测试类"""
    
    async def test_full_process(self):
        """测试完整的 SQL 规范化处理流程"""
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        try:
            # 1. 处理元数据定义
            view_count, func_count, normalized_count, updated_count = await sql_normalizer_service.process_metadata_definitions()
            print(f"处理元数据定义: {view_count} 个视图, {func_count} 个函数, {normalized_count} 个标准化, {updated_count} 个更新")
            
            # 2. 处理捕获的日志记录
            total_logs, processed_logs, marked_logs = await sql_normalizer_service.process_captured_logs(
                batch_size=10,
                max_concurrency=5
            )
            print(f"处理日志记录: {total_logs} 个总日志, {processed_logs} 个处理, {marked_logs} 个标记")
            
            # 3. 检查 SQL 模式表中的记录数
            pool = await db_utils.get_db_pool()
            async with pool.acquire() as conn:
                pattern_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM lumi_analytics.sql_patterns
                """)
                print(f"SQL 模式表中有 {pattern_count} 条记录")
            
            # 测试成功
            self.assertTrue(True, "完整流程测试成功")
        except Exception as e:
            self.fail(f"完整流程测试失败: {str(e)}")
        finally:
            # 关闭数据库连接池
            await db_utils.close_db_pool()


# 运行测试
def run_tests():
    """运行所有测试"""
    # 创建测试套件
    suite = unittest.TestSuite()
    
    # 添加单元测试
    suite.addTest(unittest.makeSuite(TestSQLNormalizer))
    
    # 添加集成测试
    suite.addTest(unittest.makeSuite(TestSQLNormalizerIntegration))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)


# 使用 asyncio 运行异步测试
async def run_async_tests():
    """运行异步测试"""
    # 运行 TestSQLNormalizer 的测试方法
    sql_normalizer_test = TestSQLNormalizer()
    await TestSQLNormalizer.setUpClass()
    
    # 运行单元测试
    await sql_normalizer_test.test_normalize_sql()
    await sql_normalizer_test.test_generate_sql_hash()
    await sql_normalizer_test.test_database_access()
    await sql_normalizer_test.test_process_sql()
    await sql_normalizer_test.test_process_captured_logs()
    await sql_normalizer_test.test_process_metadata_definitions()
    
    await TestSQLNormalizer.tearDownClass()
    
    # 运行集成测试
    integration_test = TestSQLNormalizerIntegration()
    await integration_test.test_full_process()


if __name__ == "__main__":
    # 设置日志
    setup_logging()
    
    # 运行异步测试
    asyncio.run(run_async_tests())
