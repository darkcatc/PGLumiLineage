#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志处理器集成测试

测试日志处理器的完整流程，包括数据源配置、日志文件查找、解析和存储

作者: Vance Chen
"""

import os
import sys
import asyncio
import unittest
from unittest import mock
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from pglumilineage.common import db_utils
from pglumilineage.common.config import get_settings_instance
from pglumilineage.log_processor import service


class TestLogProcessorIntegration(unittest.TestCase):
    """日志处理器集成测试"""
    
    @classmethod
    def setUpClass(cls):
        """测试前准备"""
        # 设置事件循环
        cls.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(cls.loop)
        
        # 创建测试目录
        cls.test_dir = project_root / "tests" / "temp" / "log_processor"
        os.makedirs(cls.test_dir, exist_ok=True)
        
        # 创建测试日志文件
        cls.test_log_file = cls.test_dir / "postgresql-test.csv"
        cls._create_test_log_file(cls)
        
        # 初始化数据库连接池
        cls.loop.run_until_complete(db_utils.init_db_pool())
        
        # 在数据库中创建测试数据源
        cls.loop.run_until_complete(cls._create_test_data_source(cls))
    
    @classmethod
    def tearDownClass(cls):
        """测试后清理"""
        # 删除测试日志文件
        if cls.test_log_file.exists():
            cls.test_log_file.unlink()
        
        # 删除测试数据源
        cls.loop.run_until_complete(cls._delete_test_data_source(cls))
        
        # 关闭数据库连接池
        cls.loop.run_until_complete(db_utils.close_db_pool())
        
        # 关闭事件循环
        cls.loop.close()
    
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
    
    async def _create_test_data_source(self):
        """在数据库中创建测试数据源"""
        try:
            # 获取数据库连接池
            pool = await db_utils.get_db_pool()
            
            # 创建 lumi_config schema 和 data_sources 表
            create_schema_query = "CREATE SCHEMA IF NOT EXISTS lumi_config"
            create_table_query = """
            CREATE TABLE IF NOT EXISTS lumi_config.data_sources (
                source_id SERIAL PRIMARY KEY,
                source_name TEXT NOT NULL UNIQUE,
                source_type TEXT,
                log_retrieval_method TEXT NOT NULL,
                log_path_pattern TEXT,
                db_host TEXT,
                db_port INTEGER,
                db_name TEXT,
                db_user TEXT,
                db_password TEXT,
                ssh_host TEXT,
                ssh_port INTEGER,
                ssh_user TEXT,
                ssh_password TEXT,
                ssh_key_path TEXT,
                ssh_remote_log_path_pattern TEXT,
                kafka_bootstrap_servers TEXT,
                kafka_topic TEXT,
                kafka_group_id TEXT,
                log_query_sql TEXT,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
            """
            
            # 插入测试数据源
            insert_query = """
            INSERT INTO lumi_config.data_sources (
                source_name, source_type, log_retrieval_method, log_path_pattern, is_active
            ) VALUES (
                'test_integration_source', 'postgres', 'local_file', $1, TRUE
            ) ON CONFLICT (source_name) DO UPDATE
            SET log_path_pattern = $1, updated_at = CURRENT_TIMESTAMP
            """
            
            async with pool.acquire() as conn:
                await conn.execute(create_schema_query)
                await conn.execute(create_table_query)
                await conn.execute(insert_query, str(self.test_log_file))
            
            # 清空缓存，确保下次获取数据源时会从数据库中重新加载
            service.data_source_cache = {
                "data": {},
                "timestamp": None
            }
            
            print(f"\n已创建测试数据源: test_integration_source, 日志文件路径: {self.test_log_file}")
        
        except Exception as e:
            print(f"创建测试数据源时出错: {str(e)}")
            raise
    
    async def _delete_test_data_source(self):
        """删除测试数据源"""
        try:
            # 获取数据库连接池
            pool = await db_utils.get_db_pool()
            
            # 删除测试数据源
            delete_query = """
            DELETE FROM lumi_config.data_sources
            WHERE source_name = 'test_integration_source'
            """
            
            async with pool.acquire() as conn:
                await conn.execute(delete_query)
            
            print("\n已删除测试数据源: test_integration_source")
        
        except Exception as e:
            print(f"删除测试数据源时出错: {str(e)}")
    
    def test_get_data_sources(self):
        """测试获取数据源配置"""
        async def _test():
            # 获取数据源配置
            data_sources = await service.get_data_sources()
            
            # 应该至少有一个数据源
            self.assertGreaterEqual(len(data_sources), 1)
            
            # 检查测试数据源是否存在
            test_source = None
            for source in data_sources:
                if source.get('source_name') == 'test_integration_source':
                    test_source = source
                    break
            
            self.assertIsNotNone(test_source, "未找到测试数据源")
            self.assertEqual(test_source.get('log_retrieval_method'), 'local_file')
            self.assertEqual(test_source.get('log_path_pattern'), str(self.test_log_file))
            self.assertTrue(test_source.get('is_active', False))
        
        # 使用事件循环运行异步测试
        self.loop.run_until_complete(_test())
    
    def test_find_and_parse_log_files(self):
        """测试查找和解析日志文件"""
        async def _test():
            # 获取测试数据源名称
            source_name = 'test_integration_source'
            
            # 查找新日志文件
            processed_files_tracker = set()
            new_log_files = await service.find_new_log_files(source_name, processed_files_tracker)
            
            # 应该找到一个新日志文件
            self.assertEqual(len(new_log_files), 1)
            self.assertEqual(new_log_files[0], str(self.test_log_file))
            
            # 解析日志文件
            log_entries = await service.parse_log_file(new_log_files[0])
            
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
        
        # 使用事件循环运行异步测试
        self.loop.run_until_complete(_test())


if __name__ == "__main__":
    unittest.main()
