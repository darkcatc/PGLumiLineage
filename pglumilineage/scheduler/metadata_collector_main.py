#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PGLumiLineage 元数据收集调度器

此模块负责：
1. 调度元数据收集服务的执行
2. 处理信号和优雅关闭
3. 提供命令行接口

作者: Vance Chen
"""

import asyncio
import argparse
import logging
import signal
import sys
from typing import List

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common import db_utils
from pglumilineage.metadata_collector import service as metadata_collector_service

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)

# 全局任务列表
tasks: List[asyncio.Task] = []





async def start_metadata_collector(interval_seconds: int = 86400, run_once: bool = False) -> asyncio.Task:
    """
    启动元数据收集服务
    
    Args:
        interval_seconds: 检查间隔时间（秒），默认为86400秒（1天）
        run_once: 是否只运行一次
        
    Returns:
        asyncio.Task: 元数据收集任务
    """
    logger.info(f"启动元数据收集服务，间隔: {interval_seconds}秒，{'单次运行' if run_once else '持续运行'}")
    
    # 创建并启动元数据收集任务
    task = asyncio.create_task(
        metadata_collector_service.process_metadata_collection(
            interval_seconds=interval_seconds,
            run_once=run_once
        ),
        name="metadata_collector"
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
        default=86400, 
        help="检查间隔时间（秒），默认为 86400 秒（1天）"
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
    
    # 启动元数据收集服务
    metadata_collector_task = await start_metadata_collector(
        interval_seconds=args.interval,
        run_once=args.run_once
    )
    tasks.append(metadata_collector_task)
    
    # 等待所有任务完成
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        logger.info("主任务被取消")
    
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
