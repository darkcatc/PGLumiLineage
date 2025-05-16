#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PGLumiLineage 日志处理调度器

此模块负责：
1. 根据配置的调度规则调用日志收集和解析服务
2. 从 lumi_config.source_sync_schedules 表获取日志处理调度规则
3. 处理信号和优雅关闭
4. 提供命令行接口

注意：此模块专注于日志收集和解析的调度，元数据收集由 metadata_scheduler.py 负责

作者: Vance Chen
"""

import asyncio
import argparse
import logging
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Set, Tuple

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common import config, db_utils
from pglumilineage.log_processor import service as log_processor_service

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)

# 全局任务列表
tasks: List[asyncio.Task] = []

# 调度规则缓存
schedule_cache: Dict[str, Dict[str, Any]] = {
    "data": {},
    "timestamp": None
}

# 调度规则缓存过期时间（秒）
SCHEDULE_CACHE_TTL = 300  # 5分钟


async def get_sync_schedules() -> List[Dict[str, Any]]:
    """
    从内部数据库获取日志同步调度规则
    使用缓存机制减少数据库查询
    
    Returns:
        List[Dict[str, Any]]: 调度规则列表
    """
    global schedule_cache
    
    # 检查缓存是否有效
    now = datetime.now()
    if (schedule_cache["timestamp"] is not None and 
        (now - schedule_cache["timestamp"]).total_seconds() < SCHEDULE_CACHE_TTL and
        schedule_cache["data"]):
        logger.debug("使用缓存的调度规则配置")
        return list(schedule_cache["data"].values())
    
    logger.info("从数据库获取调度规则配置")
    
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 首先检查表是否存在
            check_table_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'lumi_config' 
                AND table_name = 'source_sync_schedules'
            )
            """
            
            table_exists = await conn.fetchval(check_table_query)
            
            if not table_exists:
                logger.warning("表 lumi_config.source_sync_schedules 不存在，将使用默认调度规则")
                # 返回默认调度规则
                default_schedule = {
                    "schedule_id": 1,
                    "source_name": "default",
                    "interval_seconds": 86400,  # 1天
                    "is_active": True,
                    "last_run": None,
                    "next_run": None
                }
                
                # 更新缓存
                schedule_cache["data"] = {"default": default_schedule}
                schedule_cache["timestamp"] = now
                
                return [default_schedule]
            
            # 查询活跃的调度规则
            query = """
            SELECT 
                s.schedule_id, 
                d.source_name, 
                s.sync_interval_seconds as interval_seconds, 
                s.is_schedule_active as is_active, 
                s.last_sync_success_at as last_run, 
                NULL as next_run,
                s.created_at,
                s.updated_at
            FROM lumi_config.source_sync_schedules s
            JOIN lumi_config.data_sources d ON s.source_id = d.source_id
            WHERE s.is_schedule_active = TRUE
            ORDER BY d.source_name
            """
            
            rows = await conn.fetch(query)
            
            if not rows:
                logger.warning("未找到活跃的调度规则，将使用默认调度规则")
                # 返回默认调度规则
                default_schedule = {
                    "schedule_id": 1,
                    "source_name": "default",
                    "interval_seconds": 86400,  # 1天
                    "is_active": True,
                    "last_run": None,
                    "next_run": None
                }
                
                # 更新缓存
                schedule_cache["data"] = {"default": default_schedule}
                schedule_cache["timestamp"] = now
                
                return [default_schedule]
            
            # 处理查询结果
            schedules = {}
            for row in rows:
                schedule = dict(row)
                source_name = schedule["source_name"]
                schedules[source_name] = schedule
            
            # 更新缓存
            schedule_cache["data"] = schedules
            schedule_cache["timestamp"] = now
            
            logger.info(f"成功加载 {len(schedules)} 个调度规则")
            return list(schedules.values())
    
    except Exception as e:
        logger.error(f"获取调度规则时出错: {str(e)}")
        
        # 如果出错，返回默认调度规则
        default_schedule = {
            "schedule_id": 1,
            "source_name": "default",
            "interval_seconds": 86400,  # 1天
            "is_active": True,
            "last_run": None,
            "next_run": None
        }
        
        return [default_schedule]


async def update_schedule_status(source_name: str, status: str, processed_count: int = 0) -> None:
    """
    更新调度规则状态
    
    Args:
        source_name: 数据源名称
        status: 状态（success, error）
        processed_count: 处理的记录数
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 首先检查表是否存在
            check_table_query = """
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'lumi_config' 
                AND table_name = 'source_sync_schedules'
            )
            """
            
            table_exists = await conn.fetchval(check_table_query)
            
            if not table_exists:
                logger.warning("表 lumi_config.source_sync_schedules 不存在，无法更新调度状态")
                return
            
            # 更新调度状态
            now = datetime.now()
            next_run = None
            
            # 获取当前调度规则
            get_schedule_query = """
            SELECT interval_seconds FROM lumi_config.source_sync_schedules
            WHERE source_name = $1 AND is_active = TRUE
            """
            
            interval_seconds = await conn.fetchval(get_schedule_query, source_name)
            
            if interval_seconds:
                next_run = now + timedelta(seconds=interval_seconds)
            
            # 更新调度规则
            update_query = """
            UPDATE lumi_config.source_sync_schedules
            SET 
                last_run = $1, 
                next_run = $2, 
                last_status = $3,
                last_processed_count = $4,
                updated_at = $1
            WHERE source_name = $5 AND is_active = TRUE
            """
            
            await conn.execute(update_query, now, next_run, status, processed_count, source_name)
            
            logger.info(f"已更新调度规则状态: {source_name}, 状态: {status}, 下次运行: {next_run}")
            
            # 清空缓存，确保下次获取调度规则时会从数据库中重新加载
            global schedule_cache
            schedule_cache["timestamp"] = None
    
    except Exception as e:
        logger.error(f"更新调度规则状态时出错: {str(e)}")


async def start_log_processor(source_name: str, interval_seconds: int = 86400, run_once: bool = True) -> asyncio.Task:
    """
    启动日志处理器服务
    
    Args:
        source_name: 数据源名称
        interval_seconds: 处理间隔时间（秒）
        run_once: 是否只运行一次
        
    Returns:
        asyncio.Task: 日志处理器任务
    """
    logger.info(f"启动日志处理器服务，数据源: {source_name}，间隔: {interval_seconds}秒，{'单次运行' if run_once else '持续运行'}")
    
    # 创建并启动日志处理器任务
    task = asyncio.create_task(
        log_processor_service.process_log_files(
            source_name=source_name,
            interval_seconds=interval_seconds,
            run_once=run_once
        ),
        name=f"log_processor_{source_name}"
    )
    
    return task


async def create_necessary_tables() -> None:
    """
    创建必要的数据库表
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 创建 lumi_config schema
            create_schema_query = "CREATE SCHEMA IF NOT EXISTS lumi_config"
            await conn.execute(create_schema_query)
            logger.info("Schema lumi_config 已创建")
            
            # 创建 data_sources 表
            create_data_sources_table_query = """
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
            await conn.execute(create_data_sources_table_query)
            logger.info("表 lumi_config.data_sources 已创建")
            
            # 创建 source_sync_schedules 表
            create_schedules_table_query = """
            CREATE TABLE IF NOT EXISTS lumi_config.source_sync_schedules (
                schedule_id SERIAL PRIMARY KEY,
                source_name TEXT NOT NULL UNIQUE,
                interval_seconds INTEGER NOT NULL DEFAULT 86400,
                is_active BOOLEAN DEFAULT TRUE,
                last_run TIMESTAMPTZ,
                next_run TIMESTAMPTZ,
                last_status TEXT,
                last_processed_count INTEGER DEFAULT 0,
                created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT fk_source_name FOREIGN KEY (source_name)
                    REFERENCES lumi_config.data_sources (source_name) ON DELETE CASCADE
            )
            """
            await conn.execute(create_schedules_table_query)
            logger.info("表 lumi_config.source_sync_schedules 已创建")
            
            # 创建 lumi_logs schema
            create_logs_schema_query = "CREATE SCHEMA IF NOT EXISTS lumi_logs"
            await conn.execute(create_logs_schema_query)
            logger.info("Schema lumi_logs 已创建")
            
            # 创建 processed_log_files 表
            create_processed_files_table_query = """
            CREATE TABLE IF NOT EXISTS lumi_logs.processed_log_files (
                id SERIAL PRIMARY KEY,
                source_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                processed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT uq_source_file UNIQUE (source_name, file_path)
            )
            """
            await conn.execute(create_processed_files_table_query)
            logger.info("表 lumi_logs.processed_log_files 已创建")
            
            # 创建 raw_sql_logs 表
            create_raw_logs_table_query = """
            CREATE TABLE IF NOT EXISTS lumi_logs.raw_sql_logs (
                id SERIAL PRIMARY KEY,
                log_time TIMESTAMPTZ NOT NULL,
                username TEXT,
                database_name_logged TEXT,
                process_id INTEGER,
                client_addr TEXT,
                client_port INTEGER,
                session_id TEXT,
                session_line_num INTEGER,
                command_tag TEXT,
                session_start_time TIMESTAMPTZ,
                virtual_transaction_id TEXT,
                transaction_id BIGINT,
                error_severity TEXT,
                sql_state_code TEXT,
                message TEXT,
                detail TEXT,
                hint TEXT,
                internal_query TEXT,
                internal_query_pos INTEGER,
                context TEXT,
                raw_sql_text TEXT,
                query_pos INTEGER,
                location TEXT,
                application_name TEXT,
                backend_type TEXT,
                leader_pid INTEGER,
                query_id BIGINT,
                duration_ms INTEGER,
                log_source_identifier TEXT,
                source_name TEXT NOT NULL,
                parsed_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
            )
            """
            await conn.execute(create_raw_logs_table_query)
            logger.info("表 lumi_logs.raw_sql_logs 已创建")
            
            # 创建索引
            create_indexes_query = """
            CREATE INDEX IF NOT EXISTS idx_raw_sql_logs_log_time ON lumi_logs.raw_sql_logs (log_time);
            CREATE INDEX IF NOT EXISTS idx_raw_sql_logs_username ON lumi_logs.raw_sql_logs (username);
            CREATE INDEX IF NOT EXISTS idx_raw_sql_logs_database ON lumi_logs.raw_sql_logs (database_name_logged);
            CREATE INDEX IF NOT EXISTS idx_raw_sql_logs_application ON lumi_logs.raw_sql_logs (application_name);
            CREATE INDEX IF NOT EXISTS idx_raw_sql_logs_source ON lumi_logs.raw_sql_logs (source_name);
            """
            await conn.execute(create_indexes_query)
            logger.info("索引已创建")
            
            logger.info("所有必要的数据库表已创建完成")
    
    except Exception as e:
        logger.error(f"创建数据库表时出错: {str(e)}")
        raise


async def shutdown(sig: signal.Signals) -> None:
    """
    优雅关闭所有服务
    
    Args:
        sig: 触发关闭的信号
    """
    logger.info(f"收到信号 {sig.name}，开始优雅关闭...")
    
    # 取消所有任务
    for task in tasks:
        if not task.done():
            logger.info(f"取消任务: {task.get_name()}")
            task.cancel()
    
    # 等待所有任务完成
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    
    # 关闭数据库连接池
    try:
        await db_utils.close_db_pool()
        logger.info("数据库连接池已关闭")
    except Exception as e:
        logger.error(f"关闭数据库连接池时出错: {str(e)}")
    
    logger.info("所有服务已关闭")


async def main() -> None:
    """
    主函数
    """
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="PGLumiLineage 日志处理调度器")
    parser.add_argument(
        "--source-name", 
        type=str, 
        help="指定要处理的数据源名称，如果不指定则处理所有活跃的数据源"
    )
    parser.add_argument(
        "--interval", 
        type=int, 
        default=86400, 
        help="日志处理器间隔时间（秒），默认为 86400 秒（1天）"
    )
    parser.add_argument(
        "--run-once", 
        action="store_true", 
        help="日志处理器只运行一次"
    )
    parser.add_argument(
        "--ignore-schedule", 
        action="store_true", 
        help="忽略数据库中的调度规则，使用命令行参数"
    )
    parser.add_argument(
        "--create-tables", 
        action="store_true", 
        help="创建必要的数据库表"
    )
    
    args = parser.parse_args()
    
    # 初始化数据库连接池
    await db_utils.init_db_pool()
    logger.info("数据库连接池已初始化")
    
    # 设置信号处理
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(
            sig,
            lambda s=sig: asyncio.create_task(shutdown(s))
        )
    
    # 如果需要创建表
    if args.create_tables:
        await create_necessary_tables()
    
    # 如果指定了数据源名称或忽略调度规则
    if args.source_name or args.ignore_schedule:
        source_name = args.source_name or "default"
        logger.info(f"使用命令行参数处理数据源: {source_name}")
        
        # 启动日志处理器
        log_processor_task = await start_log_processor(
            source_name=source_name,
            interval_seconds=args.interval,
            run_once=args.run_once
        )
        tasks.append(log_processor_task)
    else:
        # 从数据库获取调度规则
        schedules = await get_sync_schedules()
        
        if not schedules:
            logger.warning("没有找到活跃的调度规则，将使用默认调度规则")
            # 使用默认调度规则
            log_processor_task = await start_log_processor(
                source_name="default",
                interval_seconds=args.interval,
                run_once=args.run_once
            )
            tasks.append(log_processor_task)
        else:
            # 根据调度规则启动日志处理器
            for schedule in schedules:
                source_name = schedule["source_name"]
                interval_seconds = schedule.get("interval_seconds", args.interval)
                
                # 检查是否需要运行
                next_run = schedule.get("next_run")
                if next_run is None or next_run <= datetime.now():
                    logger.info(f"根据调度规则启动日志处理器: {source_name}")
                    
                    # 启动日志处理器
                    log_processor_task = await start_log_processor(
                        source_name=source_name,
                        interval_seconds=interval_seconds,
                        run_once=args.run_once
                    )
                    tasks.append(log_processor_task)
                else:
                    logger.info(f"跳过数据源 {source_name}，下次运行时间: {next_run}")
    
    # 等待所有任务完成
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("主任务被取消")
    
    logger.info("日志处理调度器已退出")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"\n程序出错: {str(e)}")
        import traceback
        traceback.print_exc()
