#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
全量日志同步脚本

此脚本用于同步所有日志文件，包括之前已经处理过的，以便进行全面测试。
"""

import asyncio
import logging
import os
import sys
import glob
from typing import List, Dict, Any, Set, Optional

# 添加项目根目录到Python路径
# 获取当前脚本的绝对路径
script_path = os.path.abspath(__file__)
# 获取项目根目录（假设脚本在 scripts 目录下）
project_root = os.path.dirname(os.path.dirname(script_path))
# 将项目根目录添加到Python路径
sys.path.insert(0, project_root)

from pglumilineage.common import logging_config, db_utils, models
from pglumilineage.log_processor import service as log_processor_service
from pglumilineage.sql_normalizer import service as sql_normalizer_service

# 设置日志
logging_config.setup_logging()
logger = logging.getLogger(__name__)


async def clear_processed_files_records():
    """
    清除已处理文件的记录，以便重新处理所有文件
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 清空已处理文件记录表
        query = """
        TRUNCATE TABLE lumi_logs.processed_log_files
        """
        
        async with pool.acquire() as conn:
            await conn.execute(query)
            
        logger.info("已清空已处理文件记录表")
        return True
            
    except Exception as e:
        logger.error(f"清空已处理文件记录表失败: {str(e)}")
        return False


async def sync_all_logs(source_name: str = "tpcds"):
    """
    同步指定数据源的所有日志文件
    
    Args:
        source_name: 数据源名称，默认为 tpcds
    """
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        logger.info("数据库连接池已初始化")
        
        # 清除已处理文件的记录
        success = await clear_processed_files_records()
        if not success:
            logger.error("清除已处理文件记录失败，无法继续")
            return
        
        # 获取数据源配置
        data_sources = await log_processor_service.get_data_sources()
        if not data_sources:
            logger.error("未找到任何数据源配置")
            return
        
        # 查找指定的数据源
        target_source = None
        for ds in data_sources:
            if ds['source_name'] == source_name:
                target_source = ds
                break
        
        if not target_source:
            logger.error(f"未找到名为 {source_name} 的数据源")
            return
        
        # 获取日志文件路径模式
        log_path_pattern = target_source.get('log_path_pattern')
        if not log_path_pattern:
            logger.error(f"数据源 {source_name} 未指定日志文件路径模式")
            return
        
        logger.info(f"使用日志路径模式: {log_path_pattern}")
        
        # 查找所有日志文件
        all_log_files = []
        
        # 检查log_path_pattern是否是目录
        if os.path.isdir(log_path_pattern):
            # 如果是目录，则查找目录下的所有CSV文件
            csv_pattern = os.path.join(log_path_pattern, "*.csv")
            log_files = glob.glob(csv_pattern)
            logger.info(f"检测到目录路径，自动查找目录下的CSV文件: {csv_pattern}")
        else:
            # 如果不是目录，则使用原有的glob模式
            log_files = glob.glob(log_path_pattern)
        
        all_log_files.extend(log_files)
        logger.info(f"找到 {len(all_log_files)} 个日志文件")
        
        # 处理所有日志文件
        processed_count = 0
        inserted_count = 0
        
        for log_file in all_log_files:
            # 获取数据源配置的目标数据库名称
            target_db_name = target_source.get('db_name')
            if target_db_name:
                logger.info(f"将只处理目标数据库 {target_db_name} 的日志")
            
            # 解析日志文件，并传入目标数据库名称进行过滤
            log_entries = await log_processor_service.parse_log_file(source_name, log_file, target_db_name)
            
            if not log_entries:
                logger.info(f"日志文件 {log_file} 中没有找到有效的SQL日志条目")
                await log_processor_service.save_processed_file(source_name, log_file)
                continue
            
            # 初始化插入计数
            file_inserted_count = 0
            
            # 分批处理日志条目
            for i in range(0, len(log_entries), log_processor_service.BATCH_SIZE):
                batch = log_entries[i:i + log_processor_service.BATCH_SIZE]
                batch_inserted_count = await log_processor_service.batch_insert_logs(batch)
                file_inserted_count += batch_inserted_count
                inserted_count += batch_inserted_count
            
            processed_count += len(log_entries)
            
            # 标记文件为已处理
            await log_processor_service.save_processed_file(source_name, log_file)
            logger.info(f"已成功处理日志文件 {log_file}，插入 {file_inserted_count} 条记录")
            
            # 更新同步状态
            await log_processor_service.update_sync_status(source_name, len(log_entries), file_inserted_count)
        
        logger.info(f"所有日志文件处理完成，共处理 {processed_count} 条记录，成功插入 {inserted_count} 条记录")
        
        # 运行SQL规范化处理
        logger.info("开始运行SQL规范化处理...")
        # 处理未处理的日志记录
        batch_size = 100
        max_loops = 10  # 防止无限循环
        loop_count = 0
        total_processed = 0
        total_success = 0
        
        while loop_count < max_loops:
            loop_count += 1
            
            # 获取未处理的日志记录
            fetched_count, success_count = await sql_normalizer_service.process_captured_logs(
                batch_size=batch_size,
                max_concurrency=10
            )
            
            total_processed += fetched_count
            total_success += success_count
            
            logger.info(f"第 {loop_count} 批处理: 获取 {fetched_count}, 成功 {success_count}")
            
            # 如果没有更多记录需要处理，则退出循环
            if fetched_count == 0:
                break
        
        logger.info(f"SQL规范化处理完成，共处理 {total_processed} 条记录，成功 {total_success} 条")
        
        logger.info("全量日志同步和SQL规范化处理完成")
        
    except Exception as e:
        logger.error(f"同步日志时出错: {str(e)}")
    finally:
        # 关闭数据库连接池
        await db_utils.close_db_pool()
        logger.info("数据库连接池已关闭")


async def main():
    """
    主函数
    """
    await sync_all_logs()


if __name__ == "__main__":
    asyncio.run(main())
