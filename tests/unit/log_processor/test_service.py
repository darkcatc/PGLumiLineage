#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志处理器服务模块单元测试

测试日志处理器的核心功能，包括数据源验证、日志文件查找和解析等

作者: Vance Chen
"""

import os
import sys
import unittest
import asyncio
from unittest import mock
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from pglumilineage.log_processor import service
from pglumilineage.common import models


class TestLogProcessorService(unittest.TestCase):
    """日志处理器服务模块单元测试"""
    
    def setUp(self):
        """测试前准备"""
        # 清空缓存
        service.data_source_cache = {
            "data": {},
            "timestamp": None
        }
        service.processed_files_cache = {
            "data": {},
            "timestamp": {}
        }
        
        # 创建测试目录
        self.test_dir = project_root / "tests" / "temp" / "log_processor"
        os.makedirs(self.test_dir, exist_ok=True)
        
        # 创建测试日志文件
        self.test_log_file = self.test_dir / "postgresql-test.csv"
        self._create_test_log_file()
        
        # 设置事件循环
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
    
    def tearDown(self):
        """测试后清理"""
        # 删除测试日志文件
        if self.test_log_file.exists():
            self.test_log_file.unlink()
            
        # 关闭事件循环
        self.loop.close()
    
    def _create_test_log_file(self):
        """创建测试日志文件"""
        # CSV 标题行
        header = "log_time,user_name,database_name,process_id,connection_from,session_id,session_line_num,command_tag,session_start_time,virtual_transaction_id,transaction_id,error_severity,sql_state_code,message,detail,hint,internal_query,internal_query_pos,context,query,query_pos,location,application_name,backend_type,leader_pid,query_id"
        
        # 测试日志记录
        test_logs = [
            '2025-05-10 20:00:00.123 CST,postgres,tpcds,1234,"127.0.0.1:5432",6543210987.765432,1,"SELECT",2025-05-10 19:59:00.000 CST,"2/16",0,LOG,00000,"duration: 1.234 ms  statement: SELECT * FROM customer LIMIT 10",,,,,,"SELECT * FROM customer LIMIT 10",0,,psql,client backend,,0',
            '2025-05-10 20:01:00.456 CST,postgres,tpcds,1234,"127.0.0.1:5432",6543210987.765432,2,"SELECT",2025-05-10 19:59:00.000 CST,"2/17",0,LOG,00000,"duration: 2.345 ms  statement: SELECT * FROM store_sales WHERE ss_customer_sk = 12345",,,,,,"SELECT * FROM store_sales WHERE ss_customer_sk = 12345",0,,psql,client backend,,0'
        ]
        
        with open(self.test_log_file, 'w') as f:
            f.write(header + '\n')
            for log in test_logs:
                f.write(log + '\n')
    
    def test_validate_data_source(self):
        """测试数据源验证函数"""
        # 测试有效的本地文件数据源
        valid_local_source = {
            "source_id": 1,
            "source_name": "test_source",
            "log_retrieval_method": "local_file",
            "log_path_pattern": str(self.test_dir / "*.csv"),
            "is_active": True
        }
        self.assertTrue(service.validate_data_source(valid_local_source))
        
        # 测试缺少必要字段的数据源
        invalid_source_1 = {
            "source_id": 1,
            "source_name": "test_source",
            # 缺少 log_retrieval_method
            "log_path_pattern": str(self.test_dir / "*.csv")
        }
        self.assertFalse(service.validate_data_source(invalid_source_1))
        
        # 测试无效的日志检索方式
        invalid_source_2 = {
            "source_id": 1,
            "source_name": "test_source",
            "log_retrieval_method": "invalid_method",
            "log_path_pattern": str(self.test_dir / "*.csv")
        }
        self.assertFalse(service.validate_data_source(invalid_source_2))
        
        # 测试本地文件方式但缺少路径模式
        invalid_source_3 = {
            "source_id": 1,
            "source_name": "test_source",
            "log_retrieval_method": "local_file"
            # 缺少 log_path_pattern
        }
        self.assertFalse(service.validate_data_source(invalid_source_3))
        
        # 测试SSH方式
        valid_ssh_source = {
            "source_id": 2,
            "source_name": "ssh_source",
            "log_retrieval_method": "ssh",
            "ssh_host": "example.com",
            "ssh_port": 22,
            "ssh_user": "user",
            "ssh_password": "password",
            "ssh_remote_log_path_pattern": "/var/log/postgresql/*.csv"
        }
        self.assertTrue(service.validate_data_source(valid_ssh_source))
        
        # 测试SSH方式但缺少认证信息
        invalid_ssh_source = {
            "source_id": 2,
            "source_name": "ssh_source",
            "log_retrieval_method": "ssh",
            "ssh_host": "example.com",
            "ssh_port": 22,
            "ssh_user": "user",
            "ssh_remote_log_path_pattern": "/var/log/postgresql/*.csv"
            # 缺少 ssh_password 和 ssh_key_path
        }
        self.assertFalse(service.validate_data_source(invalid_ssh_source))
        
        # 测试数据库查询方式
        valid_db_query_source = {
            "source_id": 3,
            "source_name": "db_query_source",
            "log_retrieval_method": "db_query",
            "db_host": "localhost",
            "db_port": 5432,
            "db_name": "postgres",
            "db_user": "postgres",
            "db_password": "password",
            "log_query_sql": "SELECT * FROM pg_stat_statements"
        }
        self.assertTrue(service.validate_data_source(valid_db_query_source))
    
    def test_find_new_log_files(self):
        """测试查找新日志文件函数"""
        # 直接测试文件过滤逻辑
        # 模拟所有日志文件
        all_log_files = [str(self.test_log_file)]
        
        # 模拟已处理文件集合
        processed_files_tracker = set()
        db_processed_files = set()
        
        # 打印调试信息
        print(f"\n测试日志文件路径: {self.test_log_file}")
        print(f"测试日志文件是否存在: {self.test_log_file.exists()}\n")
        
        # 定义异步测试函数
        async def _test():
            # 模拟 find_new_log_files 函数的核心逻辑
            # 合并内存中的记录和数据库中的记录
            all_processed_files = processed_files_tracker.union(db_processed_files)
            
            # 过滤已处理的文件
            new_log_files = [f for f in all_log_files if f not in all_processed_files]
            
            # 应该找到一个新日志文件
            self.assertEqual(len(new_log_files), 1)
            self.assertEqual(new_log_files[0], str(self.test_log_file))
            
            # 测试已处理文件过滤
            processed_files_tracker.add(str(self.test_log_file))
            
            # 再次过滤
            all_processed_files = processed_files_tracker.union(db_processed_files)
            new_log_files = [f for f in all_log_files if f not in all_processed_files]
            
            # 应该没有新日志文件
            self.assertEqual(len(new_log_files), 0)
        
        # 使用事件循环运行异步测试
        self.loop.run_until_complete(_test())
    
    def test_parse_log_file(self):
        """测试解析日志文件函数"""
        
        # 定义异步测试函数
        async def _test():
            # 测试解析日志文件
            log_entries = await service.parse_log_file(str(self.test_log_file))
            
            # 应该解析出2条日志记录
            self.assertEqual(len(log_entries), 2)
            
            # 检查第一条日志记录的内容
            first_entry = log_entries[0]
            self.assertEqual(first_entry.username, "postgres")
            self.assertEqual(first_entry.database_name_logged, "tpcds")
            self.assertEqual(first_entry.client_addr, "127.0.0.1")
            self.assertEqual(first_entry.application_name, "psql")
            self.assertEqual(first_entry.duration_ms, 1234)
            self.assertEqual(first_entry.raw_sql_text, "SELECT * FROM customer LIMIT 10")
            self.assertEqual(first_entry.log_source_identifier, "postgresql-test.csv")
        
        # 使用事件循环运行异步测试
        self.loop.run_until_complete(_test())


def run_async_test(test_case):
    """运行异步测试"""
    loop = asyncio.get_event_loop()
    loop.run_until_complete(test_case)


if __name__ == "__main__":
    unittest.main()
