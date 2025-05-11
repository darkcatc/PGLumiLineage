#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PGLumiLineage 元数据收集调度器

此模块负责：
1. 根据配置的调度规则调用元数据收集服务
2. 从 lumi_config.source_sync_schedules 表获取调度规则
3. 处理信号和优雅关闭
4. 提供命令行接口

作者: Vance Chen
"""

import asyncio
import argparse
import logging
import signal
import sys
import time
import asyncpg
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional, Set, Tuple

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common import config, db_utils, models
from pglumilineage.metadata_collector import service as metadata_collector_service

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)

# 全局任务列表
tasks: List[asyncio.Task] = []


async def get_metadata_sync_schedules() -> List[Dict[str, Any]]:
    """
    从 lumi_config.source_sync_schedules 表获取元数据同步调度规则
    
    Returns:
        List[Dict[str, Any]]: 调度规则列表
    """
    logger.info("获取元数据同步调度规则")
    
    pool = await db_utils.get_db_pool()
    try:
        async with pool.acquire() as conn:
            query = """
            SELECT 
                s.schedule_id,
                s.source_id,
                s.is_schedule_active,
                s.sync_frequency_type,
                s.sync_interval_seconds,
                s.cron_expression,
                s.last_sync_attempt_at,
                s.last_sync_success_at,
                s.last_sync_status,
                s.last_sync_message,
                s.created_at,
                s.updated_at,
                d.source_name,
                d.db_host,
                d.db_port,
                d.db_name,
                d.db_user,
                d.db_password
            FROM lumi_config.source_sync_schedules s
            JOIN lumi_config.data_sources d ON s.source_id = d.source_id
            WHERE s.is_schedule_active = TRUE
            """
            
            rows = await conn.fetch(query)
            schedules = []
            
            for row in rows:
                # 将密码字符串转换为 SecretStr
                password_secret = models.SecretStr(row['db_password']) if row['db_password'] else models.SecretStr('')
                
                schedule = {
                    'schedule_id': row['schedule_id'],
                    'source_id': row['source_id'],
                    'is_schedule_active': row['is_schedule_active'],
                    'sync_frequency_type': row['sync_frequency_type'],
                    'sync_interval_seconds': row['sync_interval_seconds'],
                    'cron_expression': row['cron_expression'],
                    'last_sync_attempt_at': row['last_sync_attempt_at'],
                    'last_sync_success_at': row['last_sync_success_at'],
                    'source_config': models.DataSourceConfig(
                        source_id=row['source_id'],
                        source_name=row['source_name'],
                        host=row['db_host'],
                        port=row['db_port'],
                        username=row['db_user'],
                        password=password_secret,
                        database=row['db_name']
                    )
                }
                schedules.append(schedule)
            
            return schedules
    finally:
        await pool.close()


async def update_schedule_sync_status(schedule_id: int, success: bool, message: str = None) -> None:
    """
    更新调度规则的同步状态
    
    Args:
        schedule_id: 调度规则ID
        success: 同步是否成功
        message: 同步消息
    """
    logger.info(f"更新调度规则 {schedule_id} 的同步状态: {'SUCCESS' if success else 'FAILED'}")
    
    now = datetime.now(timezone.utc)
    status = "SUCCESS" if success else "FAILED"
    
    # 创建独立的数据库连接
    try:
        # 使用RAW_LOGS_DSN创建连接
        dsn = str(config.settings.RAW_LOGS_DSN)
        conn = await asyncpg.connect(dsn=dsn)
        
        try:
            query = """
            UPDATE lumi_config.source_sync_schedules
            SET last_sync_attempt_at = $1, 
                last_sync_status = $2, 
                last_sync_message = $3,
                last_sync_success_at = CASE WHEN $4 THEN $1 ELSE last_sync_success_at END,
                updated_at = CURRENT_TIMESTAMP
            WHERE schedule_id = $5
            """
            
            await conn.execute(query, now, status, message, success, schedule_id)
        finally:
            # 关闭连接
            await conn.close()
    except Exception as e:
        logger.error(f"更新调度规则同步状态时出错: {str(e)}")


async def calculate_next_run_time(sync_frequency_type: str, sync_interval_seconds: int, 
                               cron_expression: str, from_time: datetime) -> datetime:
    """
    根据调度类型计算下次运行时间
    
    Args:
        sync_frequency_type: 同步频率类型 ('interval', 'cron', 'manual')
        sync_interval_seconds: 同步间隔秒数
        cron_expression: cron 表达式
        from_time: 起始时间
        
    Returns:
        datetime: 下次运行时间
    """
    if sync_frequency_type == 'interval' and sync_interval_seconds:
        return from_time + timedelta(seconds=sync_interval_seconds)
    elif sync_frequency_type == 'cron' and cron_expression:
        # 注意: 实际实现中应使用 croniter 等库来处理 cron 表达式
        # 这里简化处理，返回一天后的时间
        logger.warning(f"Cron 表达式处理未实现，使用默认间隔 (1 天): {cron_expression}")
        return from_time + timedelta(days=1)
    elif sync_frequency_type == 'manual':
        # 手动模式下，返回远期时间
        return from_time + timedelta(days=365)
    else:
        # 默认情况，返回一天后的时间
        logger.warning(f"未知的同步频率类型: {sync_frequency_type}，使用默认间隔 (1 天)")
        return from_time + timedelta(days=1)


async def process_metadata_collection(interval_seconds: int = 300, run_once: bool = False) -> None:
    """
    处理元数据收集
    
    Args:
        interval_seconds: 检查间隔时间（秒）
        run_once: 是否只运行一次
    """
    logger.info(f"启动元数据收集调度器，检查间隔: {interval_seconds}秒，{'\u5355\u6b21\u8fd0\u884c' if run_once else '\u6301\u7eed\u8fd0\u884c'}")
    
    try:
        while True:
            now = datetime.now(timezone.utc)
            logger.info(f"检查元数据同步调度规则，当前时间: {now}")
            
            # 获取调度规则
            schedules = await get_metadata_sync_schedules()
            logger.info(f"找到 {len(schedules)} 个启用的元数据同步调度规则")
            
            # 处理每个调度规则
            for schedule in schedules:
                schedule_id = schedule['schedule_id']
                source_config = schedule['source_config']
                last_sync_success_at = schedule['last_sync_success_at']
                sync_frequency_type = schedule['sync_frequency_type']
                sync_interval_seconds = schedule['sync_interval_seconds']
                cron_expression = schedule['cron_expression']
                
                # 如果有上次成功同步时间，计算下次应该同步的时间
                should_sync = True
                if last_sync_success_at:
                    next_run_time = await calculate_next_run_time(
                        sync_frequency_type, sync_interval_seconds, cron_expression, last_sync_success_at
                    )
                    should_sync = now >= next_run_time
                
                # 如果应该同步，执行元数据收集
                if should_sync:
                    logger.info(f"开始执行数据源 {source_config.source_name} 的元数据收集")
                    
                    try:
                        # 调用元数据收集服务
                        success, error_message = await metadata_collector_service.collect_metadata_for_source(source_config)
                        
                        # 更新调度规则的同步状态
                        if success:
                            logger.info(f"数据源 {source_config.source_name} 的元数据收集成功")
                            await update_schedule_sync_status(schedule_id, True, "元数据收集成功")
                        else:
                            logger.error(f"数据源 {source_config.source_name} 的元数据收集失败: {error_message}")
                            await update_schedule_sync_status(schedule_id, False, f"元数据收集失败: {error_message}")
                    
                    except Exception as e:
                        error_msg = f"处理数据源 {source_config.source_name} 的元数据收集时出错: {str(e)}"
                        logger.error(error_msg)
                        await update_schedule_sync_status(schedule_id, False, error_msg)
                else:
                    next_run_time = await calculate_next_run_time(
                        sync_frequency_type, sync_interval_seconds, cron_expression, last_sync_success_at
                    )
                    logger.info(f"数据源 {source_config.source_name} 的元数据收集还不需要运行，下次运行时间: {next_run_time}")
            
            # 如果只运行一次，则退出循环
            if run_once:
                logger.info("单次运行模式，元数据收集完成")
                break
            
            # 等待下一次检查
            logger.info(f"等待 {interval_seconds} 秒后进行下一次检查")
            await asyncio.sleep(interval_seconds)
    
    except asyncio.CancelledError:
        logger.info("元数据收集调度器任务被取消")
        raise
    except Exception as e:
        logger.error(f"元数据收集调度器出错: {str(e)}")
        raise


async def start_metadata_scheduler(interval_seconds: int = 300, run_once: bool = False) -> asyncio.Task:
    """
    启动元数据收集调度器
    
    Args:
        interval_seconds: 检查间隔时间（秒）
        run_once: 是否只运行一次
        
    Returns:
        asyncio.Task: 元数据收集调度器任务
    """
    # 创建并启动元数据收集调度器任务
    task = asyncio.create_task(
        process_metadata_collection(
            interval_seconds=interval_seconds,
            run_once=run_once
        ),
        name="metadata_scheduler"
    )
    
    return task


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
    parser = argparse.ArgumentParser(description="PGLumiLineage 元数据收集调度器")
    parser.add_argument(
        "--interval", 
        type=int, 
        default=300, 
        help="检查间隔时间（秒），默认为 300 秒（5分钟）"
    )
    parser.add_argument(
        "--run-once", 
        action="store_true", 
        help="只运行一次"
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
    
    # 启动元数据收集调度器
    metadata_scheduler_task = await start_metadata_scheduler(
        interval_seconds=args.interval,
        run_once=args.run_once
    )
    tasks.append(metadata_scheduler_task)
    
    # 等待所有任务完成
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("主任务被取消")
    
    # 在run_once模式下不关闭连接池，连接池的关闭由shutdown函数统一管理
    if args.run_once:
        logger.info("单次运行模式完成")
    
    logger.info("元数据收集调度器已退出")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"\n程序出错: {str(e)}")
        import traceback
        traceback.print_exc()
