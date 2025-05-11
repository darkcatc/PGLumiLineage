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
from typing import Dict, Any, List, Optional, Set

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common import config, db_utils
from pglumilineage.log_processor import service as log_processor_service

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)

# 全局任务列表
tasks: List[asyncio.Task] = []


async def start_log_processor(interval_seconds: int = 60, run_once: bool = False) -> asyncio.Task:
    """
    启动日志处理器服务
    
    Args:
        interval_seconds: 处理间隔时间（秒）
        run_once: 是否只运行一次
        
    Returns:
        asyncio.Task: 日志处理器任务
    """
    logger.info(f"启动日志处理器服务，间隔: {interval_seconds}秒，{'单次运行' if run_once else '持续运行'}")
    
    # 创建并启动日志处理器任务
    task = asyncio.create_task(
        log_processor_service.process_log_files(
            interval_seconds=interval_seconds,
            run_once=run_once
        ),
        name="log_processor"
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
    parser = argparse.ArgumentParser(description="PGLumiLineage 服务编排器")
    parser.add_argument(
        "--log-processor-interval", 
        type=int, 
        default=60, 
        help="日志处理器间隔时间（秒），默认为 60 秒"
    )
    parser.add_argument(
        "--log-processor-run-once", 
        action="store_true", 
        help="日志处理器只运行一次"
    )
    parser.add_argument(
        "--log-processor-only", 
        action="store_true", 
        help="只启动日志处理器服务"
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
    
    # 启动日志处理器
    log_processor_task = await start_log_processor(
        interval_seconds=args.log_processor_interval,
        run_once=args.log_processor_run_once
    )
    tasks.append(log_processor_task)
    
    # 在这里可以添加其他服务的启动代码
    # 例如: SQL 血缘分析服务、Web API 服务等
    
    # 等待所有任务完成
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("主任务被取消")
    
    logger.info("服务编排器已退出")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"\n程序出错: {str(e)}")
        import traceback
        traceback.print_exc()
