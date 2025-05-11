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
import time
from typing import List, Set, Dict, Optional, Tuple, Any
from datetime import datetime
import re

from pglumilineage.common import logging_config, config, db_utils, models

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
    
    # 定义PostgreSQL CSV日志的列顺序
    # 参考: https://www.postgresql.org/docs/current/runtime-config-logging.html#RUNTIME-CONFIG-LOGGING-CSVLOG
    # CSV columns expected: 
    # log_time, user_name, database_name, process_id, connection_from, session_id, session_line_num, 
    # command_tag, session_start_time, virtual_transaction_id, transaction_id, error_severity, 
    # sql_state_code, message, detail, hint, internal_query, internal_query_pos, context, query, 
    # query_pos, location, application_name, backend_type, leader_pid, query_id
    fieldnames = [
        'log_time', 'user_name', 'database_name', 'process_id', 'connection_from', 
        'session_id', 'session_line_num', 'command_tag', 'session_start_time', 
        'virtual_transaction_id', 'transaction_id', 'error_severity', 'sql_state_code', 
        'message', 'detail', 'hint', 'internal_query', 'internal_query_pos', 
        'context', 'query', 'query_pos', 'location', 'application_name', 'backend_type',
        'leader_pid', 'query_id'
    ]
    
    try:
        with open(log_file_path, 'r', newline='') as csvfile:
            # PostgreSQL CSV日志没有标题行，需要手动指定字段名
            csv_reader = csv.DictReader(csvfile, fieldnames=fieldnames)
            
            for row in csv_reader:
                # 提取SQL语句 - 可能在query字段或message字段(以statement:开头)
                sql_text = None
                
                if row.get('query') and row.get('query').strip():
                    sql_text = row.get('query').strip()
                elif row.get('message', '').startswith('statement:'):
                    sql_text = row.get('message')[len('statement:'):].strip()
                
                # 只处理包含SQL语句的行
                if sql_text and not sql_text.startswith('--'):
                    try:
                        # 提取客户端地址 (从connection_from字段，格式可能是host:port)
                        client_addr = row.get('connection_from', '')
                        if ':' in client_addr:
                            client_addr = client_addr.split(':')[0]  # 只保留IP地址部分
                        
                        # 提取持续时间 (可能在message字段中，格式如"duration: X.XXX ms")
                        duration_ms = 0
                        message = row.get('message', '')
                        duration_match = re.search(r'duration:\s*([0-9.]+)\s*ms', message)
                        if duration_match:
                            try:
                                duration_ms = int(float(duration_match.group(1)) * 1000)
                            except (ValueError, IndexError):
                                logger.warning(f"无法解析持续时间: {message}")
                        
                        # 解析日志时间 (格式如"2023-01-01 12:34:56.789 UTC")
                        log_time = datetime.now()
                        if row.get('log_time'):
                            try:
                                # 处理带时区的时间戳
                                log_time_str = row.get('log_time')
                                if ' UTC' in log_time_str:
                                    # 如果有UTC标记，先移除它，然后添加Z表示UTC
                                    log_time_str = log_time_str.replace(' UTC', 'Z')
                                # 处理毫秒部分
                                if '.' in log_time_str:
                                    # 确保毫秒部分格式正确
                                    date_part, time_part = log_time_str.split(' ', 1)
                                    time_part, tz_part = time_part.split('Z', 1) if 'Z' in time_part else (time_part, '')
                                    time_parts = time_part.split('.')
                                    if len(time_parts) > 1:
                                        # 确保毫秒部分最多3位
                                        ms_part = time_parts[1][:3]
                                        time_part = f"{time_parts[0]}.{ms_part}"
                                    log_time_str = f"{date_part} {time_part}{'Z' if tz_part == '' and 'Z' in log_time_str else tz_part}"
                                
                                # 尝试解析时间
                                log_time = datetime.fromisoformat(log_time_str.replace('Z', '+00:00'))
                            except (ValueError, IndexError) as e:
                                logger.warning(f"无法解析日志时间: {row.get('log_time')}, 错误: {str(e)}")
                        
                        # 创建RawSQLLog对象
                        log_entry = models.RawSQLLog(
                            log_time=log_time,
                            source_database_name=source_name,
                            username=row.get('user_name', ''),
                            database_name_logged=row.get('database_name', ''),
                            client_addr=client_addr,
                            application_name=row.get('application_name', ''),
                            session_id=row.get('session_id', ''),
                            query_id=int(row.get('query_id', 0)) if row.get('query_id', '').isdigit() else None,
                            duration_ms=duration_ms,
                            raw_sql_text=sql_text,
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
        
        # 定义目标表的列顺序
        # lumi_logs.captured_logs 表的列顺序如下：
        # 1. log_id (serial, 自增主键，不需要插入)
        # 2. log_time
        # 3. source_database_name
        # 4. username
        # 5. database_name_logged
        # 6. client_addr
        # 7. application_name
        # 8. session_id
        # 9. query_id
        # 10. duration_ms
        # 11. raw_sql_text
        # 12. log_source_identifier
        # 13. created_at (默认为 now()，不需要插入)
        # 14. updated_at (默认为 now()，不需要插入)
        
        # 定义要插入的列
        columns = [
            'log_time', 'source_database_name', 'username', 'database_name_logged',
            'client_addr', 'application_name', 'session_id', 'query_id',
            'duration_ms', 'raw_sql_text', 'log_source_identifier'
        ]
        
        # 准备要插入的记录
        # 注意：记录顺序必须与 columns 定义的顺序一致
        records = [
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
        
        # 使用 copy_records_to_table 进行高性能批量插入
        async with pool.acquire() as conn:
            try:
                # 开始事务
                async with conn.transaction():
                    # 使用 COPY 协议批量插入数据
                    # 确保使用完全限定名称并设置 schema
                    await conn.execute('SET search_path TO lumi_logs, public')
                    result = await conn.copy_records_to_table(
                        'captured_logs',  # 不使用 schema 前缀，因为已经设置了搜索路径
                        records=records,
                        columns=columns
                    )
                    
                    # copy_records_to_table 不返回影响行数，所以我们使用记录数量
                    inserted_count = len(records)
                    logger.info(f"成功插入 {inserted_count} 条日志记录")
                    return inserted_count
            except Exception as e:
                # 如果 COPY 失败，尝试使用 executemany 方法
                logger.warning(f"COPY 协议插入失败，尝试使用 executemany: {str(e)}")
                
                # 准备 INSERT 语句
                insert_query = """
                INSERT INTO lumi_logs.captured_logs (
                    log_time, source_database_name, username, database_name_logged,
                    client_addr, application_name, session_id, query_id,
                    duration_ms, raw_sql_text, log_source_identifier
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
                """
                
                # 开始新事务
                async with conn.transaction():
                    # 执行批量插入
                    await conn.executemany(insert_query, records)
                    
                    inserted_count = len(records)
                    logger.info(f"成功插入 {inserted_count} 条日志记录 (使用 executemany)")
                    return inserted_count
    
    except Exception as e:
        logger.error(f"批量插入日志记录时出错: {str(e)}")
        return 0


async def process_log_files(interval_seconds: int = 60, run_once: bool = False) -> int:
    """
    处理日志文件的主函数
    
    Args:
        interval_seconds: 处理间隔时间（秒）
        run_once: 是否只运行一次，如果为 False 则作为服务持续运行
    
    Returns:
        int: 处理的记录总数
    """
    # 初始化日志和数据库连接池
    logging_config.setup_logging()
    logger.info("开始处理PostgreSQL日志文件")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 跟踪已处理的文件
        processed_log_files: Dict[str, Set[str]] = {}  # 数据源名称 -> 已处理文件集合
        total_processed_records = 0
        
        # 定义处理一次日志的函数
        async def process_logs_once() -> int:
            processed_count = 0
            
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
                            processed_count += inserted_count
                        
                        # 标记文件为已处理
                        processed_log_files[source_name].add(log_file)
                        logger.info(f"完成处理日志文件: {log_file}")
                        
                        # 更新同步状态
                        await update_sync_status(source_name, len(log_entries), inserted_count)
                
                return processed_count
            except Exception as e:
                logger.error(f"处理日志文件时出错: {str(e)}")
                return 0
        
        # 主处理循环
        if run_once:
            # 只运行一次
            total_processed_records = await process_logs_once()
        else:
            # 作为服务持续运行
            try:
                while True:
                    start_time = time.time()
                    
                    # 处理一次日志
                    processed_count = await process_logs_once()
                    total_processed_records += processed_count
                    
                    # 计算实际需要等待的时间
                    elapsed_time = time.time() - start_time
                    wait_time = max(0, interval_seconds - elapsed_time)
                    
                    if processed_count > 0:
                        logger.info(f"本次处理了 {processed_count} 条记录，总计: {total_processed_records}")
                    
                    if wait_time > 0:
                        logger.info(f"等待 {wait_time:.1f} 秒后继续处理...")
                        await asyncio.sleep(wait_time)
            except asyncio.CancelledError:
                logger.info("服务被取消，正在优雅退出...")
            except KeyboardInterrupt:
                logger.info("收到键盘中断，正在优雅退出...")
        
        logger.info(f"日志处理完成，共处理 {total_processed_records} 条记录")
        return total_processed_records
    
    except Exception as e:
        logger.error(f"日志处理服务出错: {str(e)}")
        return 0
    finally:
        # 关闭数据库连接池
        try:
            await db_utils.close_db_pool()
            logger.info("数据库连接池已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接池时出错: {str(e)}")


async def update_sync_status(source_name: str, processed_count: int, inserted_count: int) -> None:
    """
    更新数据源的同步状态
    
    Args:
        source_name: 数据源名称
        processed_count: 处理的记录数
        inserted_count: 插入的记录数
    """
    try:
        # 获取数据源信息
        data_source = data_source_cache.get(source_name)
        
        if not data_source:
            logger.error(f"未找到数据源: {source_name}")
            return
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 更新同步状态
        update_query = """
        UPDATE lumi_config.source_sync_schedules
        SET 
            last_sync_attempt_at = NOW(),
            last_sync_success_at = CASE WHEN $3 > 0 THEN NOW() ELSE last_sync_success_at END,
            last_sync_status = CASE WHEN $3 > 0 THEN 'SUCCESS' ELSE 'NO_RECORDS' END,
            last_sync_message = $4,
            updated_at = NOW()
            -- 注意：表中没有 processed_count 列
        WHERE source_id = $1 AND is_schedule_active = TRUE
        """
        
        # 构造同步状态消息
        sync_message = f"处理了 {processed_count} 条记录，成功插入 {inserted_count} 条"
        
        async with pool.acquire() as conn:
            # 执行更新
            # 确保参数类型正确并与 SQL 中的参数匹配
            result = await conn.execute(
                update_query, 
                int(data_source['source_id']),  # 确保 source_id 是整数
                int(processed_count),           # $2
                int(inserted_count),            # $3
                str(sync_message)               # $4
            )
            
            if result == 'UPDATE 0':
                # 如果没有更新任何行，可能是因为没有活跃的计划
                logger.warning(f"没有找到数据源 {source_name} 的活跃同步计划")
            else:
                logger.info(f"已更新数据源 {source_name} 的同步状态: {sync_message}")
    
    except Exception as e:
        logger.error(f"更新同步状态时出错: {str(e)}")


# 如果直接运行该模块，则启动日志处理服务
if __name__ == "__main__":
    import argparse
    
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="PostgreSQL 日志处理服务")
    parser.add_argument(
        "--interval", 
        type=int, 
        default=60, 
        help="处理间隔时间（秒），默认为 60 秒"
    )
    parser.add_argument(
        "--run-once", 
        action="store_true", 
        help="只运行一次然后退出，而不是作为服务持续运行"
    )
    
    args = parser.parse_args()
    
    # 运行日志处理服务
    try:
        asyncio.run(process_log_files(interval_seconds=args.interval, run_once=args.run_once))
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"\n程序出错: {str(e)}")
        import traceback
        traceback.print_exc()
