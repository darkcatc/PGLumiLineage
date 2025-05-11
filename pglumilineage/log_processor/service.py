#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PostgreSQL 日志处理服务

此模块负责：
1. 读取 PostgreSQL 的 CSV 格式日志文件
2. 解析 CSV 日志行，提取关键字段
3. 将解析后的数据异步批量写入数据库

作者: Vance Chen
"""

import asyncio
import csv
import os
import glob
import logging
from typing import List, Set, Dict, Optional, Tuple, Any
from datetime import datetime
import re

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common import config, db_utils, models

# 设置日志
logger = logging.getLogger(__name__)

# 批处理大小
BATCH_SIZE = 1000

# 数据源配置缓存
data_source_cache: Dict[str, Dict[str, Any]] = {}


async def get_data_sources() -> List[Dict[str, Any]]:
    """
    从配置表中获取数据源信息
    
    Returns:
        List[Dict[str, Any]]: 数据源信息列表
    """
    logger.info("从配置表中获取数据源信息")
    
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 查询活跃的数据源
        query = """
        SELECT ds.source_id, ds.source_name, ds.source_type, ds.description,
               ds.db_host, ds.db_port, ds.db_name, ds.db_user, ds.db_password,
               ds.log_retrieval_method, ds.log_path_pattern,
               ds.ssh_host, ds.ssh_port, ds.ssh_user, ds.ssh_password, ds.ssh_key_path, ds.ssh_remote_log_path_pattern,
               ds.kafka_bootstrap_servers, ds.kafka_topic, ds.kafka_consumer_group,
               ss.schedule_id, ss.sync_frequency_type, ss.sync_interval_seconds, ss.priority
        FROM lumi_config.data_sources ds
        JOIN lumi_config.source_sync_schedules ss ON ds.source_id = ss.source_id
        WHERE ds.is_active = TRUE AND ss.is_schedule_active = TRUE
        ORDER BY ss.priority DESC
        """
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(query)
        
        data_sources = [dict(row) for row in rows]
        logger.info(f"找到 {len(data_sources)} 个活跃的数据源")
        
        # 更新缓存
        for ds in data_sources:
            data_source_cache[ds['source_name']] = ds
        
        return data_sources
    
    except Exception as e:
        logger.error(f"获取数据源信息时出错: {str(e)}")
        return []


async def find_new_log_files(source_name: str, processed_log_files_tracker: Set[str]) -> List[str]:
    """
    查找需要处理的新日志文件
    
    Args:
        source_name: 数据源名称
        processed_log_files_tracker: 已处理的日志文件集合
        
    Returns:
        List[str]: 新日志文件路径列表
    """
    # 从缓存中获取数据源信息
    data_source = data_source_cache.get(source_name)
    
    if not data_source:
        logger.error(f"未找到数据源: {source_name}")
        return []
    
    # 获取日志文件路径模式
    log_retrieval_method = data_source['log_retrieval_method']
    
    if log_retrieval_method == 'local_path':
        log_files_pattern = data_source['log_path_pattern']
        logger.info(f"查找本地日志文件: {log_files_pattern}")
        
        # 使用glob查找匹配的文件
        all_log_files = glob.glob(log_files_pattern)
        
        # 过滤出未处理的文件
        new_log_files = [f for f in all_log_files if f not in processed_log_files_tracker]
        
        if new_log_files:
            logger.info(f"找到 {len(new_log_files)} 个新日志文件")
        else:
            logger.info("没有找到新日志文件")
        
        return new_log_files
    
    elif log_retrieval_method == 'ssh':
        # SSH 方式获取日志文件（未实现）
        logger.warning(f"SSH 方式获取日志文件尚未实现")
        return []
    
    elif log_retrieval_method == 'kafka_topic':
        # Kafka 方式获取日志（未实现）
        logger.warning(f"Kafka 方式获取日志尚未实现")
        return []
    
    else:
        logger.error(f"不支持的日志获取方式: {log_retrieval_method}")
        return []


async def parse_log_file(source_name: str, log_file_path: str) -> List[models.RawSQLLog]:
    """
    解析日志文件，提取SQL日志条目
    
    Args:
        source_name: 数据源名称
        log_file_path: 日志文件路径
        
    Returns:
        List[models.RawSQLLog]: 解析后的SQL日志条目列表
    """
    logger.info(f"开始解析日志文件: {log_file_path}")
    log_entries = []
    
    try:
        with open(log_file_path, 'r', newline='') as csvfile:
            # PostgreSQL CSV日志格式通常包含标题行
            csv_reader = csv.DictReader(csvfile)
            
            for row in csv_reader:
                # 只处理包含SQL语句的行
                if 'query' in row and row['query'] and not row['query'].strip().startswith('--'):
                    try:
                        # 创建RawSQLLog对象
                        log_entry = models.RawSQLLog(
                            log_time=datetime.fromisoformat(row.get('log_time', '')) if row.get('log_time') else datetime.now(),
                            source_database_name=source_name,
                            username=row.get('user_name', ''),
                            database_name_logged=row.get('database_name', ''),
                            client_addr=row.get('remote_host', ''),
                            application_name=row.get('application_name', ''),
                            session_id=row.get('session_id', ''),
                            query_id=int(row.get('query_id', 0)) if row.get('query_id', '').isdigit() else None,
                            duration_ms=int(float(row.get('duration_ms', 0)) * 1000) if row.get('duration_ms') else 0,
                            raw_sql_text=row.get('query', ''),
                            log_source_identifier=os.path.basename(log_file_path)
                        )
                        log_entries.append(log_entry)
                    except Exception as e:
                        logger.error(f"解析日志行时出错: {str(e)}, 行数据: {row}")
                        continue
    
    except Exception as e:
        logger.error(f"解析日志文件 {log_file_path} 时出错: {str(e)}")
    
    logger.info(f"从日志文件 {log_file_path} 中解析出 {len(log_entries)} 条SQL日志")
    return log_entries


async def batch_insert_logs(log_entries: List[models.RawSQLLog]) -> int:
    """
    批量插入日志条目到数据库
    
    Args:
        log_entries: 要插入的日志条目列表
        
    Returns:
        int: 成功插入的记录数
    """
    if not log_entries:
        return 0
    
    logger.info(f"准备批量插入 {len(log_entries)} 条日志记录")
    
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 准备插入语句
        insert_query = """
        INSERT INTO lumi_logs.captured_logs (
            log_time, source_database_name, username, database_name_logged,
            client_addr, application_name, session_id, query_id,
            duration_ms, raw_sql_text, log_source_identifier
        ) VALUES (
            $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11
        )
        """
        
        # 准备批量插入的参数
        values = [
            (
                entry.log_time,
                entry.source_database_name,
                entry.username,
                entry.database_name_logged,
                entry.client_addr,
                entry.application_name,
                entry.session_id,
                entry.query_id,
                entry.duration_ms,
                entry.raw_sql_text,
                entry.log_source_identifier
            )
            for entry in log_entries
        ]
        
        # 执行批量插入
        async with pool.acquire() as conn:
            # 使用executemany进行批量插入
            await conn.executemany(insert_query, values)
        
        logger.info(f"成功插入 {len(log_entries)} 条日志记录")
        return len(log_entries)
    
    except Exception as e:
        logger.error(f"批量插入日志记录时出错: {str(e)}")
        return 0


async def process_log_files() -> int:
    """
    处理日志文件的主函数
    
    Returns:
        int: 处理的记录总数
    """
    logger.info("开始处理PostgreSQL日志文件")
    
    # 跟踪已处理的文件
    processed_log_files: Dict[str, Set[str]] = {}  # 数据源名称 -> 已处理文件集合
    total_processed_records = 0
    
    try:
        # 获取数据源信息
        data_sources = await get_data_sources()
        
        if not data_sources:
            logger.warning("没有找到活跃的数据源，无法处理日志文件")
            return 0
        
        for data_source in data_sources:
            source_name = data_source['source_name']
            logger.info(f"处理数据源: {source_name}")
            
            # 初始化已处理文件集合
            if source_name not in processed_log_files:
                processed_log_files[source_name] = set()
            
            # 查找新的日志文件
            new_log_files = await find_new_log_files(source_name, processed_log_files[source_name])
            
            for log_file in new_log_files:
                # 解析日志文件
                log_entries = await parse_log_file(source_name, log_file)
                
                if not log_entries:
                    logger.info(f"日志文件 {log_file} 中没有找到有效的SQL日志条目")
                    processed_log_files[source_name].add(log_file)
                    continue
                
                # 分批处理日志条目
                for i in range(0, len(log_entries), BATCH_SIZE):
                    batch = log_entries[i:i + BATCH_SIZE]
                    inserted_count = await batch_insert_logs(batch)
                    total_processed_records += inserted_count
                
                # 标记文件为已处理
                processed_log_files[source_name].add(log_file)
                logger.info(f"完成处理日志文件: {log_file}")
                
                # 更新同步状态
                await update_sync_status(data_source['schedule_id'], 'SUCCESS', f"处理了 {len(log_entries)} 条记录")
    
    except Exception as e:
        logger.error(f"处理日志文件时出错: {str(e)}")
    
    logger.info(f"日志处理完成，共处理 {total_processed_records} 条记录")
    return total_processed_records


async def update_sync_status(schedule_id: int, status: str, message: str) -> None:
    """
    更新同步状态
    
    Args:
        schedule_id: 同步计划ID
        status: 状态 ('SUCCESS', 'FAILED', 'IN_PROGRESS')
        message: 状态消息
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 更新同步状态
        query = """
        UPDATE lumi_config.source_sync_schedules
        SET last_sync_attempt_at = CURRENT_TIMESTAMP,
            last_sync_success_at = CASE WHEN $2 = 'SUCCESS' THEN CURRENT_TIMESTAMP ELSE last_sync_success_at END,
            last_sync_status = $2,
            last_sync_message = $3,
            updated_at = CURRENT_TIMESTAMP
        WHERE schedule_id = $1
        """
        
        async with pool.acquire() as conn:
            await conn.execute(query, schedule_id, status, message)
        
        logger.debug(f"已更新同步计划 {schedule_id} 的状态: {status}")
    
    except Exception as e:
        logger.error(f"更新同步状态时出错: {str(e)}")
        # 这里不抛出异常，因为这是一个次要操作，不应该影响主要流程


async def run_log_processor(interval_seconds: int = 60) -> None:
    """
    定期运行日志处理器
    
    Args:
        interval_seconds: 处理间隔（秒）
    """
    logger.info(f"启动日志处理器，处理间隔: {interval_seconds}秒")
    
    while True:
        try:
            # 处理日志文件
            await process_log_files()
        except Exception as e:
            logger.error(f"日志处理器运行出错: {str(e)}")
        
        logger.info(f"等待 {interval_seconds} 秒后继续处理...")
        await asyncio.sleep(interval_seconds)


async def main():
    """
    主函数
    """
    # 设置日志
    setup_logging()
    
    # 运行日志处理器
    await run_log_processor()


if __name__ == "__main__":
    # 运行主函数
    asyncio.run(main())
