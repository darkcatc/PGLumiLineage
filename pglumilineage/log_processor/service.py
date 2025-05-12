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


def validate_data_source(data_source: Dict[str, Any]) -> bool:
    """
    验证数据源配置是否有效
    
    Args:
        data_source: 数据源配置字典
        
    Returns:
        bool: 配置是否有效
    """
    # 检查必要字段
    required_fields = ['source_id', 'source_name', 'log_retrieval_method']
    for field in required_fields:
        if field not in data_source or not data_source[field]:
            logger.error(f"数据源配置缺失必要字段: {field}")
            return False
    
    # 根据日志检索方法检查相关字段
    log_method = data_source['log_retrieval_method']
    
    if log_method == 'local_file' or log_method == 'local_path':
        if 'log_path_pattern' not in data_source or not data_source['log_path_pattern']:
            logger.error(f"本地文件方式需要指定 log_path_pattern")
            return False
    elif log_method == 'ssh':
        ssh_fields = ['ssh_host', 'ssh_port', 'ssh_user', 'ssh_remote_log_path_pattern']
        for field in ssh_fields:
            if field not in data_source or not data_source[field]:
                logger.error(f"SSH方式需要指定 {field}")
                return False
        # 检查认证方式，密码或密钥至少需要一种
        if ('ssh_password' not in data_source or not data_source['ssh_password']) and \
           ('ssh_key_path' not in data_source or not data_source['ssh_key_path']):
            logger.error("SSH方式需要指定 ssh_password 或 ssh_key_path")
            return False
    elif log_method == 'kafka' or log_method == 'kafka_topic':
        kafka_fields = ['kafka_bootstrap_servers', 'kafka_topic', 'kafka_consumer_group']
        for field in kafka_fields:
            if field not in data_source or not data_source[field]:
                logger.error(f"Kafka方式需要指定 {field}")
                return False
    else:
        logger.error(f"不支持的日志检索方式: {log_method}")
        return False
    
    return True


async def get_processed_files_from_db(source_name: str) -> Set[str]:
    """
    从数据库中获取已处理的文件记录
    
    Args:
        source_name: 数据源名称
        
    Returns:
        Set[str]: 已处理的文件路径集合
    """
    processed_files = set()
    
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 检查表是否存在
        check_table_query = """
        SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE table_schema = 'lumi_logs' AND table_name = 'processed_log_files'
        )
        """
        
        async with pool.acquire() as conn:
            exists = await conn.fetchval(check_table_query)
            
            if exists:
                # 查询已处理的文件
                query = """
                SELECT file_path FROM lumi_logs.processed_log_files
                WHERE source_name = $1
                """
                
                rows = await conn.fetch(query, source_name)
                processed_files = {row['file_path'] for row in rows}
                
                logger.debug(f"从数据库中获取到 {len(processed_files)} 个已处理的文件记录")
    
    except Exception as e:
        logger.error(f"从数据库中获取已处理文件记录时出错: {str(e)}")
    
    return processed_files


async def save_processed_file(source_name: str, file_path: str) -> None:
    """
    将已处理的文件记录保存到数据库
    
    Args:
        source_name: 数据源名称
        file_path: 文件路径
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 检查是否存在已处理文件记录表，如果不存在则创建
        create_table_query = """
        CREATE TABLE IF NOT EXISTS lumi_logs.processed_log_files (
            id SERIAL PRIMARY KEY,
            source_name TEXT NOT NULL,
            file_path TEXT NOT NULL,
            processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT uq_source_file UNIQUE (source_name, file_path)
        )
        """
        
        async with pool.acquire() as conn:
            await conn.execute(create_table_query)
            
            # 插入或更新已处理文件记录
            insert_query = """
            INSERT INTO lumi_logs.processed_log_files (source_name, file_path)
            VALUES ($1, $2)
            ON CONFLICT (source_name, file_path) DO UPDATE
            SET processed_at = CURRENT_TIMESTAMP
            """
            
            await conn.execute(insert_query, source_name, file_path)
        
        logger.debug(f"已保存已处理文件记录: {source_name} - {file_path}")
    except Exception as e:
        logger.error(f"保存已处理文件记录时出错: {str(e)}")



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
        
        # 清空缓存，确保每次都获取最新的数据源信息
        data_source_cache.clear()
        
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
        
        # 验证数据源配置
        valid_data_sources = []
        for ds in data_sources:
            if validate_data_source(ds):
                # 更新缓存
                data_source_cache[ds['source_name']] = ds
                valid_data_sources.append(ds)
            else:
                logger.warning(f"数据源 {ds.get('source_name', 'unknown')} 配置无效，已跳过")
        
        return valid_data_sources
    
    except Exception as e:
        logger.error(f"获取数据源信息时出错: {str(e)}")
        return []


async def find_new_log_files(source_name: str, processed_log_files_tracker: Set[str]) -> List[str]:
    """
    查找需要处理的新日志文件
    
    Args:
        source_name: 数据源名称
        processed_log_files_tracker: 内存中跟踪的已处理的日志文件集合
        
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
    
    # 获取所有日志文件
    all_log_files = []
    
    if log_retrieval_method == 'local_file' or log_retrieval_method == 'local_path':
        log_files_pattern = data_source['log_path_pattern']
        logger.info(f"查找本地日志文件: {log_files_pattern}")
    
    if log_retrieval_method == 'local_file' or log_retrieval_method == 'local_path':
        # 使用glob查找匹配的文件
        log_files = glob.glob(log_files_pattern)
        all_log_files.extend(log_files)
        logger.info(f"找到 {len(log_files)} 个本地日志文件")
    elif log_retrieval_method == 'ssh':
        # SSH 方式，待实现
        logger.warning("SSH 方式尚未实现")
    elif log_retrieval_method == 'kafka':
        # Kafka 方式，待实现
        logger.warning("Kafka 方式尚未实现")
    else:
        logger.error(f"不支持的日志检索方式: {log_retrieval_method}")
    
    # 从数据库中获取已处理的文件记录
    db_processed_files = await get_processed_files_from_db(source_name)
    
    # 合并内存中的记录和数据库中的记录
    all_processed_files = processed_log_files_tracker.union(db_processed_files)
    
    # 过滤已处理的文件
    new_log_files = [f for f in all_log_files if f not in all_processed_files]
    logger.info(f"找到 {len(new_log_files)} 个新日志文件需要处理（共 {len(all_log_files)} 个文件，{len(all_processed_files)} 个已处理）")
    
    return new_log_files


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
    logger.info("开始处理PostgreSQL日志文件")
    
    try:
        # 使用已初始化的数据库连接池
        # 注意：连接池应该在上层调用者中初始化
        
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
                        
                        # 初始化插入计数
                        total_inserted_count = 0
                        
                        # 分批处理日志条目
                        for i in range(0, len(log_entries), BATCH_SIZE):
                            batch = log_entries[i:i + BATCH_SIZE]
                            batch_inserted_count = await batch_insert_logs(batch)
                            total_inserted_count += batch_inserted_count
                            processed_count += batch_inserted_count
                        
                        # 标记文件为已处理
                        processed_log_files[source_name].add(log_file)
                        logger.info(f"完成处理日志文件: {log_file}，解析 {len(log_entries)} 条记录，成功插入 {total_inserted_count} 条记录")
                        
                        # 更新同步状态
                        await update_sync_status(source_name, len(log_entries), total_inserted_count)
                        
                        # 持久化已处理文件记录
                        await save_processed_file(source_name, log_file)
                
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
