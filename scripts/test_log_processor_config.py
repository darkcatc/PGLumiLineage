#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试日志处理器配置和功能

此脚本用于测试日志处理器的配置和功能，包括：
1. 测试数据库连接
2. 测试数据源配置获取
3. 测试调度规则获取
4. 测试日志文件查找和解析

作者: Vance Chen
"""

import os
import sys
import asyncio
import argparse
import logging
from pathlib import Path
from datetime import datetime

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common import db_utils, config
from pglumilineage.log_processor import service as log_processor_service
from pglumilineage.scheduler import log_processor_main

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)


async def test_database_connection():
    """测试数据库连接"""
    logger.info("测试数据库连接...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        logger.info("数据库连接池初始化成功")
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 测试连接
        async with pool.acquire() as conn:
            version = await conn.fetchval("SELECT version()")
            logger.info(f"数据库连接成功，版本: {version}")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
        logger.info("数据库连接池已关闭")
        
        return True
    except Exception as e:
        logger.error(f"数据库连接测试失败: {str(e)}")
        return False


async def test_data_source_config():
    """测试数据源配置获取"""
    logger.info("测试数据源配置获取...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 获取数据源配置
        data_sources = await log_processor_service.get_data_sources()
        
        if not data_sources:
            logger.warning("未找到数据源配置")
            return False
        
        # 打印数据源配置
        logger.info(f"找到 {len(data_sources)} 个数据源配置:")
        for i, source in enumerate(data_sources, 1):
            source_name = source.get("source_name", "未知")
            source_type = source.get("source_type", "未知")
            log_method = source.get("log_retrieval_method", "未知")
            is_active = source.get("is_active", False)
            
            logger.info(f"  {i}. {source_name} ({source_type}) - 方式: {log_method}, 状态: {'活跃' if is_active else '不活跃'}")
            
            # 验证数据源配置
            is_valid = log_processor_service.validate_data_source(source)
            logger.info(f"     配置验证: {'有效' if is_valid else '无效'}")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
        
        return True
    except Exception as e:
        logger.error(f"数据源配置测试失败: {str(e)}")
        return False


async def test_sync_schedules():
    """测试调度规则获取"""
    logger.info("测试调度规则获取...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 获取调度规则
        schedules = await log_processor_main.get_sync_schedules()
        
        if not schedules:
            logger.warning("未找到调度规则")
            return False
        
        # 打印调度规则
        logger.info(f"找到 {len(schedules)} 个调度规则:")
        for i, schedule in enumerate(schedules, 1):
            source_name = schedule.get("source_name", "未知")
            interval = schedule.get("interval_seconds", 0)
            is_active = schedule.get("is_active", False)
            last_run = schedule.get("last_run")
            next_run = schedule.get("next_run")
            
            logger.info(f"  {i}. {source_name} - 间隔: {interval}秒, 状态: {'活跃' if is_active else '不活跃'}")
            logger.info(f"     上次运行: {last_run or '从未'}, 下次运行: {next_run or '未调度'}")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
        
        return True
    except Exception as e:
        logger.error(f"调度规则测试失败: {str(e)}")
        return False


async def test_log_file_finding(source_name=None):
    """测试日志文件查找"""
    logger.info(f"测试日志文件查找{f' (数据源: {source_name})' if source_name else ''}...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 获取数据源配置
        data_sources = await log_processor_service.get_data_sources()
        
        if not data_sources:
            logger.warning("未找到数据源配置")
            return False
        
        # 如果指定了数据源名称，则只测试该数据源
        if source_name:
            source_found = False
            for source in data_sources:
                if source.get("source_name") == source_name:
                    source_found = True
                    await _test_single_source(source)
                    break
            
            if not source_found:
                logger.warning(f"未找到名为 {source_name} 的数据源")
                return False
        else:
            # 测试所有数据源
            for source in data_sources:
                await _test_single_source(source)
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
        
        return True
    except Exception as e:
        logger.error(f"日志文件查找测试失败: {str(e)}")
        return False


async def _test_single_source(source):
    """测试单个数据源的日志文件查找和解析"""
    source_name = source.get("source_name", "未知")
    log_method = source.get("log_retrieval_method", "未知")
    
    logger.info(f"测试数据源: {source_name} (方式: {log_method})")
    
    # 验证数据源配置
    is_valid = log_processor_service.validate_data_source(source)
    if not is_valid:
        logger.warning(f"数据源 {source_name} 配置无效，跳过测试")
        return
    
    # 查找新日志文件
    processed_files_tracker = set()
    new_log_files = await log_processor_service.find_new_log_files(source_name, processed_files_tracker)
    
    if not new_log_files:
        logger.warning(f"数据源 {source_name} 未找到新日志文件")
        return
    
    logger.info(f"找到 {len(new_log_files)} 个新日志文件:")
    for i, file_path in enumerate(new_log_files, 1):
        logger.info(f"  {i}. {file_path}")
        
        # 尝试解析第一个日志文件
        if i == 1:
            try:
                logger.info(f"尝试解析日志文件: {file_path}")
                log_entries = await log_processor_service.parse_log_file(file_path)
                
                if not log_entries:
                    logger.warning(f"日志文件 {file_path} 中未找到日志条目")
                else:
                    logger.info(f"成功解析 {len(log_entries)} 条日志条目")
                    
                    # 打印前3条日志条目的摘要
                    for j, entry in enumerate(log_entries[:3], 1):
                        logger.info(f"  日志条目 {j}:")
                        logger.info(f"    时间: {entry.log_time}")
                        logger.info(f"    用户: {entry.username}")
                        logger.info(f"    数据库: {entry.database_name_logged}")
                        logger.info(f"    应用: {entry.application_name}")
                        logger.info(f"    SQL: {entry.raw_sql_text[:100]}...")
            except Exception as e:
                logger.error(f"解析日志文件 {file_path} 时出错: {str(e)}")


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description="测试日志处理器配置和功能")
    parser.add_argument("--db", action="store_true", help="测试数据库连接")
    parser.add_argument("--sources", action="store_true", help="测试数据源配置获取")
    parser.add_argument("--schedules", action="store_true", help="测试调度规则获取")
    parser.add_argument("--logs", action="store_true", help="测试日志文件查找和解析")
    parser.add_argument("--source", type=str, help="指定要测试的数据源名称")
    parser.add_argument("--all", action="store_true", help="运行所有测试")
    
    args = parser.parse_args()
    
    # 如果没有指定任何测试，则运行所有测试
    if not (args.db or args.sources or args.schedules or args.logs or args.all):
        args.all = True
    
    # 运行测试
    if args.db or args.all:
        await test_database_connection()
        print()
    
    if args.sources or args.all:
        await test_data_source_config()
        print()
    
    if args.schedules or args.all:
        await test_sync_schedules()
        print()
    
    if args.logs or args.all:
        await test_log_file_finding(args.source)
        print()
    
    logger.info("测试完成")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"\n程序出错: {str(e)}")
        import traceback
        traceback.print_exc()
