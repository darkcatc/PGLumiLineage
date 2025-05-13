#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL 规范化模块单元测试

该模块测试 SQL 规范化、哈希生成和日志处理功能。

作者: Vance Chen
"""

import asyncio
import datetime
import unittest
from unittest import mock
from typing import List, Optional, Tuple

import pytest
from sqlglot.errors import ParseError

from pglumilineage.common.models import RawSQLLog
from pglumilineage.sql_normalizer.service import (
    generate_sql_hash,
    _process_single_log_entry,
    process_captured_logs
)

# 导入测试专用的 normalize_sql 函数
from tests.unit.sql_normalizer.test_normalize_sql import normalize_sql_for_test as normalize_sql


class TestSQLNormalizer(unittest.TestCase):
    """测试 SQL 规范化功能"""

    def test_normalize_sql_select(self):
        """测试 SELECT 语句的规范化"""
        # 测试基本的 SELECT 语句
        raw_sql = "SELECT id, name FROM users WHERE id = 123"
        expected = "SELECT id, name FROM users WHERE id = ?"
        result = normalize_sql(raw_sql)
        self.assertEqual(result, expected)

    def test_normalize_sql_insert(self):
        """测试 INSERT 语句的规范化"""
        # 测试基本的 INSERT 语句
        raw_sql = "INSERT INTO users (id, name) VALUES (123, 'John')"
        expected = "INSERT INTO users (id, name) VALUES (?, ?)"
        result = normalize_sql(raw_sql)
        self.assertEqual(result, expected)

    def test_normalize_sql_update(self):
        """测试 UPDATE 语句的规范化"""
        # 测试基本的 UPDATE 语句
        raw_sql = "UPDATE users SET name = 'John' WHERE id = 123"
        expected = "UPDATE users SET name = ? WHERE id = ?"
        result = normalize_sql(raw_sql)
        self.assertEqual(result, expected)

    def test_normalize_sql_delete(self):
        """测试 DELETE 语句的规范化"""
        # 测试基本的 DELETE 语句
        raw_sql = "DELETE FROM users WHERE id = 123"
        expected = "DELETE FROM users WHERE id = ?"
        result = normalize_sql(raw_sql)
        self.assertEqual(result, expected)

    def test_normalize_sql_with_comments(self):
        """测试带有注释的 SQL 语句的规范化"""
        # 测试带有行注释和块注释的 SQL
        raw_sql = """
        -- 这是一个查询用户的 SQL
        SELECT id, name 
        FROM users 
        /* 这是一个
        多行注释 */
        WHERE id = 123
        """
        expected = "SELECT id, name FROM users WHERE id = ?"
        result = normalize_sql(raw_sql)
        self.assertEqual(result, expected)

    def test_normalize_sql_invalid(self):
        """测试无效 SQL 语句的规范化"""
        # 测试语法错误的 SQL
        raw_sql = "SELECT * FROM"
        result = normalize_sql(raw_sql)
        self.assertIsNone(result)

    def test_generate_sql_hash(self):
        """测试 SQL 哈希生成"""
        # 测试相同的 SQL 生成相同的哈希
        sql1 = "SELECT id FROM users WHERE id = ?"
        sql2 = "SELECT id FROM users WHERE id = ?"
        hash1 = generate_sql_hash(sql1)
        hash2 = generate_sql_hash(sql2)
        self.assertEqual(hash1, hash2)

        # 测试不同的 SQL 生成不同的哈希
        sql3 = "SELECT name FROM users WHERE id = ?"
        hash3 = generate_sql_hash(sql3)
        self.assertNotEqual(hash1, hash3)

    def test_generate_sql_hash_cache(self):
        """测试 SQL 哈希缓存功能"""
        # 测试相同的 SQL 只计算一次哈希
        sql = "SELECT id FROM users WHERE id = ?"
        
        # 第一次调用应该计算哈希
        hash1 = generate_sql_hash(sql)
        
        # 第二次调用应该从缓存中获取
        with mock.patch('hashlib.sha256') as mock_sha256:
            hash2 = generate_sql_hash(sql)
            # 验证 sha256 没有被调用（因为使用了缓存）
            mock_sha256.assert_not_called()
        
        self.assertEqual(hash1, hash2)


class TestSQLProcessing(unittest.IsolatedAsyncioTestCase):
    """测试 SQL 处理功能"""

    async def test_process_single_log_entry(self):
        """测试处理单个日志条目"""
        # 创建模拟的日志对象
        log = RawSQLLog(
            log_id=1,
            raw_sql_text="SELECT * FROM users WHERE id = 123",
            source_database_name="test_db",
            log_time=datetime.datetime.now(datetime.timezone.utc),
            duration_ms=100
        )

        # 模拟 normalize_sql 和 generate_sql_hash 函数
        with mock.patch('pglumilineage.sql_normalizer.service.normalize_sql', 
                       return_value="SELECT * FROM users WHERE id = ?") as mock_normalize, \
             mock.patch('pglumilineage.sql_normalizer.service.generate_sql_hash',
                       return_value="test_hash") as mock_hash, \
             mock.patch('pglumilineage.sql_normalizer.service.upsert_sql_pattern_from_log',
                       return_value=1) as mock_upsert:
            
            # 调用被测试的函数
            result = await _process_single_log_entry(log)
            
            # 验证结果
            self.assertEqual(result, (1, "test_hash"))
            
            # 验证模拟函数被正确调用
            mock_normalize.assert_called_once_with(log.raw_sql_text)
            mock_hash.assert_called_once_with("SELECT * FROM users WHERE id = ?")
            mock_upsert.assert_called_once()

    async def test_process_captured_logs(self):
        """测试处理捕获的日志"""
        # 创建模拟的日志列表
        logs = [
            RawSQLLog(
                log_id=1,
                raw_sql_text="SELECT * FROM users WHERE id = 123",
                source_database_name="test_db",
                log_time=datetime.datetime.now(datetime.timezone.utc),
                duration_ms=100
            ),
            RawSQLLog(
                log_id=2,
                raw_sql_text="INSERT INTO users (id, name) VALUES (123, 'John')",
                source_database_name="test_db",
                log_time=datetime.datetime.now(datetime.timezone.utc),
                duration_ms=50
            )
        ]

        # 模拟 fetch_unprocessed_logs 和 mark_logs_as_processed 函数
        with mock.patch('pglumilineage.sql_normalizer.service.fetch_unprocessed_logs',
                       return_value=logs) as mock_fetch, \
             mock.patch('pglumilineage.sql_normalizer.service._process_single_log_entry',
                       side_effect=[
                           (1, "hash1"),
                           (2, "hash2")
                       ]) as mock_process, \
             mock.patch('pglumilineage.sql_normalizer.service.mark_logs_as_processed',
                       return_value=2) as mock_mark:
            
            # 调用被测试的函数
            result = await process_captured_logs(batch_size=10, max_concurrency=5)
            
            # 验证结果
            self.assertEqual(result, (2, 2, 2))
            
            # 验证模拟函数被正确调用
            mock_fetch.assert_called_once_with(10)
            self.assertEqual(mock_process.call_count, 2)
            mock_mark.assert_called_once_with([(1, "hash1"), (2, "hash2")])


if __name__ == "__main__":
    unittest.main()
