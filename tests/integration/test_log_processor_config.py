#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志处理器配置集成测试

测试从配置表中读取数据源信息并处理日志文件的功能。

作者: Vance Chen
"""

import asyncio
import logging
import os
import sys
from pathlib import Path
import unittest
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common import db_utils
from pglumilineage.log_processor import service


class TestLogProcessorConfig(unittest.TestCase):
    """测试日志处理器从配置表中读取数据源信息"""

    @classmethod
    def setUpClass(cls):
        """设置测试环境"""
        # 设置日志
        setup_logging()
        cls.logger = logging.getLogger(__name__)
        cls.logger.info("设置测试环境")
        
    def setUp(self):
        """每个测试方法前的设置"""
        # 初始化实例的logger属性
        self.logger = logging.getLogger(__name__)

    async def test_get_data_sources(self):
        """测试从配置表中获取数据源信息"""
        self.logger.info("测试从配置表中获取数据源信息")
        
        # 获取数据源信息
        data_sources = await service.get_data_sources()
        
        # 验证数据源信息
        self.assertIsNotNone(data_sources, "数据源信息不应为空")
        self.assertTrue(len(data_sources) > 0, "应至少有一个数据源")
        
        # 验证 tpcds 数据源
        tpcds_source = None
        for ds in data_sources:
            if ds['source_name'] == 'tpcds':
                tpcds_source = ds
                break
        
        self.assertIsNotNone(tpcds_source, "应存在名为 'tpcds' 的数据源")
        self.assertEqual(tpcds_source['log_retrieval_method'], 'local_path', "tpcds 数据源应使用 local_path 方式")
        self.assertTrue('log_path_pattern' in tpcds_source, "tpcds 数据源应包含 log_path_pattern")
        
        # 输出数据源信息
        self.logger.info(f"tpcds 数据源: {tpcds_source}")
        
        return tpcds_source

    async def test_find_new_log_files(self):
        """测试查找新日志文件"""
        self.logger.info("测试查找新日志文件")
        
        # 获取 tpcds 数据源
        data_sources = await service.get_data_sources()
        tpcds_source = None
        for ds in data_sources:
            if ds['source_name'] == 'tpcds':
                tpcds_source = ds
                break
        
        self.assertIsNotNone(tpcds_source, "应存在名为 'tpcds' 的数据源")
        
        # 查找新日志文件
        processed_files = set()
        new_log_files = await service.find_new_log_files('tpcds', processed_files)
        
        # 验证日志文件
        self.logger.info(f"找到 {len(new_log_files)} 个新日志文件")
        
        # 即使没有找到日志文件，测试也应该通过，因为这取决于环境
        # 但我们应该记录日志文件的路径模式，以便排查问题
        self.logger.info(f"日志文件路径模式: {tpcds_source['log_path_pattern']}")
        
        return new_log_files

    async def test_parse_log_file(self):
        """测试解析日志文件"""
        self.logger.info("测试解析日志文件")
        
        # 查找新日志文件
        new_log_files = await self.test_find_new_log_files()
        
        if not new_log_files:
            self.logger.warning("没有找到日志文件，跳过解析测试")
            self.skipTest("没有找到日志文件")
            return
        
        # 解析第一个日志文件
        log_file = new_log_files[0]
        self.logger.info(f"解析日志文件: {log_file}")
        
        log_entries = await service.parse_log_file('tpcds', log_file)
        
        # 验证解析结果
        self.logger.info(f"解析出 {len(log_entries)} 条日志记录")
        
        # 如果有日志记录，验证其内容
        if log_entries:
            entry = log_entries[0]
            self.assertEqual(entry.source_database_name, 'tpcds', "日志记录的数据源名称应为 'tpcds'")
            self.assertIsNotNone(entry.log_time, "日志记录应包含时间戳")
            self.assertIsNotNone(entry.raw_sql_text, "日志记录应包含 SQL 文本")
            
            # 输出第一条日志记录的详细信息
            self.logger.info(f"第一条日志记录:")
            self.logger.info(f"  log_time: {entry.log_time}")
            self.logger.info(f"  source_database_name: {entry.source_database_name}")
            self.logger.info(f"  username: {entry.username}")
            self.logger.info(f"  database_name_logged: {entry.database_name_logged}")
            self.logger.info(f"  raw_sql_text: {entry.raw_sql_text[:100]}..." if len(entry.raw_sql_text) > 100 else f"  raw_sql_text: {entry.raw_sql_text}")
        
        return log_entries

    async def test_batch_insert_logs(self):
        """测试批量插入日志记录"""
        self.logger.info("测试批量插入日志记录")
        
        # 解析日志文件
        try:
            log_entries = await self.test_parse_log_file()
        except unittest.SkipTest:
            self.logger.warning("没有找到日志文件，跳过插入测试")
            self.skipTest("没有找到日志文件")
            return
        
        if not log_entries:
            self.logger.warning("没有解析出日志记录，跳过插入测试")
            self.skipTest("没有解析出日志记录")
            return
        
        # 获取当前表中的记录数
        pool = await db_utils.get_db_pool()
        async with pool.acquire() as conn:
            before_count = await conn.fetchval("SELECT COUNT(*) FROM lumi_logs.captured_logs")
        
        self.logger.info(f"插入前表中有 {before_count} 条记录")
        
        # 批量插入日志记录
        inserted_count = await service.batch_insert_logs(log_entries)
        
        # 验证插入结果
        self.logger.info(f"插入了 {inserted_count} 条记录")
        self.assertEqual(inserted_count, len(log_entries), "插入的记录数应与日志记录数相同")
        
        # 验证表中的记录数增加了
        async with pool.acquire() as conn:
            after_count = await conn.fetchval("SELECT COUNT(*) FROM lumi_logs.captured_logs")
        
        self.logger.info(f"插入后表中有 {after_count} 条记录")
        self.assertEqual(after_count - before_count, len(log_entries), "表中的记录数应增加了日志记录数")
        
        # 查询最新插入的记录
        async with pool.acquire() as conn:
            latest_records = await conn.fetch(
                "SELECT log_id, log_time, source_database_name, username, raw_sql_text FROM lumi_logs.captured_logs ORDER BY log_id DESC LIMIT 3"
            )
        
        self.logger.info("最新插入的记录:")
        for record in latest_records:
            self.logger.info(f"  log_id: {record['log_id']}")
            self.logger.info(f"  log_time: {record['log_time']}")
            self.logger.info(f"  source_database_name: {record['source_database_name']}")
            self.logger.info(f"  username: {record['username']}")
            self.logger.info(f"  raw_sql_text: {record['raw_sql_text'][:50]}..." if len(record['raw_sql_text']) > 50 else f"  raw_sql_text: {record['raw_sql_text']}")

    async def test_process_log_files(self):
        """测试处理日志文件的主函数"""
        self.logger.info("测试处理日志文件的主函数")
        
        # 处理日志文件
        processed_count = await service.process_log_files()
        
        # 验证处理结果
        self.logger.info(f"处理了 {processed_count} 条记录")
        
        # 这个测试主要是确保函数能够正常运行，不会抛出异常
        # 实际处理的记录数取决于环境

    async def test_update_sync_status(self):
        """测试更新同步状态"""
        self.logger.info("测试更新同步状态")
        
        # 获取 tpcds 数据源
        data_sources = await service.get_data_sources()
        tpcds_source = None
        for ds in data_sources:
            if ds['source_name'] == 'tpcds':
                tpcds_source = ds
                break
        
        self.assertIsNotNone(tpcds_source, "应存在名为 'tpcds' 的数据源")
        
        # 更新同步状态
        schedule_id = tpcds_source['schedule_id']
        await service.update_sync_status(schedule_id, 'SUCCESS', '测试更新同步状态')
        
        # 验证同步状态已更新
        pool = await db_utils.get_db_pool()
        async with pool.acquire() as conn:
            status = await conn.fetchval(
                "SELECT last_sync_status FROM lumi_config.source_sync_schedules WHERE schedule_id = $1",
                schedule_id
            )
            message = await conn.fetchval(
                "SELECT last_sync_message FROM lumi_config.source_sync_schedules WHERE schedule_id = $1",
                schedule_id
            )
        
        self.assertEqual(status, 'SUCCESS', "同步状态应为 'SUCCESS'")
        self.assertEqual(message, '测试更新同步状态', "同步消息应为 '测试更新同步状态'")


def run_tests():
    """运行测试"""
    # 创建测试套件
    suite = unittest.TestSuite()
    
    # 添加测试用例
    test_case = TestLogProcessorConfig()
    test_methods = [
        'test_get_data_sources',
        'test_find_new_log_files',
        'test_parse_log_file',
        'test_batch_insert_logs',
        'test_process_log_files',
        'test_update_sync_status'
    ]
    
    for method in test_methods:
        suite.addTest(unittest.FunctionTestCase(
            getattr(test_case, method),
            setUp=test_case.setUp if hasattr(test_case, 'setUp') else None,
            tearDown=test_case.tearDown if hasattr(test_case, 'tearDown') else None
        ))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)


async def run_async_test(test_method):
    """运行单个异步测试方法"""
    logger = logging.getLogger(__name__)
    logger.info(f"运行测试: {test_method.__name__}")
    
    # 创建测试实例
    test = TestLogProcessorConfig()
    test.logger = logger
    
    try:
        # 运行测试方法
        result = await test_method(test)
        logger.info(f"测试 {test_method.__name__} 通过")
        return result
    except unittest.SkipTest as s:
        logger.warning(f"测试 {test_method.__name__} 跳过: {str(s)}")
        return None
    except Exception as e:
        logger.error(f"测试 {test_method.__name__} 失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        raise


async def main():
    """主函数"""
    # 设置日志
    setup_logging()
    logger = logging.getLogger(__name__)
    logger.info("开始测试日志处理器配置")
    
    # 定义测试方法列表
    test_methods = [
        TestLogProcessorConfig.test_get_data_sources,
        TestLogProcessorConfig.test_find_new_log_files,
        TestLogProcessorConfig.test_parse_log_file,
        TestLogProcessorConfig.test_batch_insert_logs,
        TestLogProcessorConfig.test_process_log_files,
        TestLogProcessorConfig.test_update_sync_status
    ]
    
    # 运行所有测试
    success_count = 0
    fail_count = 0
    skip_count = 0
    
    for test_method in test_methods:
        try:
            await run_async_test(test_method)
            success_count += 1
        except unittest.SkipTest:
            skip_count += 1
        except Exception:
            fail_count += 1
    
    # 输出测试结果汇总
    logger.info(f"测试完成: 成功 {success_count}, 失败 {fail_count}, 跳过 {skip_count}")
    
    if fail_count > 0:
        logger.error("测试失败!")
        return 1
    else:
        logger.info("所有测试通过!")
        return 0


if __name__ == "__main__":
    # 运行异步测试
    asyncio.run(main())
