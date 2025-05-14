#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PGLumiLineage SQL规范化处理调度器

此模块负责：
1. 调度SQL规范化处理服务的执行
2. 确保处理所有未处理的日志记录
3. 处理信号和优雅关闭
4. 提供命令行接口

作者: Vance Chen
"""

import asyncio
import argparse
import logging
import signal
import sys
from typing import List, Tuple

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common import db_utils
from pglumilineage.sql_normalizer import service as sql_normalizer_service

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)

# 全局任务列表
tasks: List[asyncio.Task] = []


async def mark_unprocessed_logs_as_processed(limit: int = 100) -> int:
    """
    将未处理的日志记录直接标记为已处理，避免无限循环处理相同的非数据流SQL
    
    Args:
        limit: 最大处理数量
        
    Returns:
        int: 成功标记的记录数
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 查询未处理的日志记录
        query = """
        UPDATE lumi_logs.captured_logs
        SET is_processed_for_analysis = TRUE
        WHERE is_processed_for_analysis = FALSE
        ORDER BY log_id
        LIMIT $1
        RETURNING log_id
        """
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(query, limit)
            
            # 获取更新的行数
            updated_count = len(rows)
            if updated_count > 0:
                log_ids = [row['log_id'] for row in rows]
                logger.info(f"已手动标记 {updated_count} 条日志为已处理: {log_ids}")
            return updated_count
            
    except Exception as e:
        logger.error(f"手动标记日志记录为已处理失败: {str(e)}")
        return 0


async def process_all_unprocessed_logs(batch_size: int = 100, max_concurrency: int = 10) -> None:
    """
    处理所有未处理的日志记录，直到全部处理完毕

    Args:
        batch_size: 每批处理的日志数量
        max_concurrency: 并发处理的最大任务数
    """
    total_processed = 0
    total_success = 0
    loop_count = 0
    max_loops = 10  # 最大循环次数，避免无限循环
    
    while True:
        loop_count += 1
        
        # 获取并处理一批未处理的日志
        fetched_count, success_count, marked_count = await sql_normalizer_service.process_captured_logs(
            batch_size=batch_size,
            max_concurrency=max_concurrency
        )
        
        # 更新统计信息
        total_processed += fetched_count
        total_success += success_count
        
        logger.info(f"本批次处理: 获取 {fetched_count}, 成功 {success_count}, 标记 {marked_count}")
        
        # 如果没有获取到任何日志，说明所有日志都已处理完毕
        if fetched_count == 0:
            logger.info(f"所有日志处理完毕! 总共处理: {total_processed}, 成功: {total_success}")
            break
            
        # 如果获取到日志但没有成功处理任何一条，并且已经循环多次，则手动标记这些日志为已处理
        if fetched_count > 0 and success_count == 0 and marked_count == 0 and loop_count >= 3:
            logger.warning(f"检测到可能的无限循环: 连续 {loop_count} 次获取日志但无法处理，将手动标记这些日志为已处理")
            marked_count = await mark_unprocessed_logs_as_processed(fetched_count)
            logger.info(f"手动标记 {marked_count} 条日志为已处理")
            
            # 如果已经达到最大循环次数，强制退出
            if loop_count >= max_loops:
                logger.warning(f"已达到最大循环次数 {max_loops}，强制退出循环")
                break
        
        # 短暂暂停，避免过度消耗数据库资源
        await asyncio.sleep(1)


async def start_sql_normalizer(
    interval_seconds: int = 3600, 
    run_once: bool = False,
    batch_size: int = 100,
    max_concurrency: int = 10
) -> asyncio.Task:
    """
    启动SQL规范化处理服务
    
    Args:
        interval_seconds: 检查间隔时间（秒），默认为3600秒（1小时）
        run_once: 是否只运行一次
        batch_size: 每批处理的日志数量
        max_concurrency: 并发处理的最大任务数
        
    Returns:
        asyncio.Task: SQL规范化处理任务
    """
    logger.info(
        f"启动SQL规范化处理服务，间隔: {interval_seconds}秒，"
        f"{'单次运行' if run_once else '持续运行'}，"
        f"批大小: {batch_size}，最大并发: {max_concurrency}"
    )
    
    async def _run_sql_normalizer():
        try:
            # 初始化数据库连接池
            await db_utils.init_db_pool()
            
            while True:
                start_time = asyncio.get_event_loop().time()
                
                try:
                    # 处理所有未处理的日志记录
                    await process_all_unprocessed_logs(batch_size, max_concurrency)
                    
                    # 处理元数据定义（视图和函数）
                    view_count, func_count, success_count, update_count = await sql_normalizer_service.process_metadata_definitions()
                    logger.info(
                        f"元数据定义处理完成: 视图 {view_count}, 函数 {func_count}, "
                        f"成功 {success_count}, 更新 {update_count}"
                    )
                    
                except Exception as e:
                    logger.error(f"SQL规范化处理过程中出错: {str(e)}", exc_info=True)
                
                # 如果是单次运行模式，则退出循环
                if run_once:
                    break
                
                # 计算下次运行时间
                elapsed = asyncio.get_event_loop().time() - start_time
                sleep_time = max(0, interval_seconds - elapsed)
                
                logger.info(f"SQL规范化处理完成，将在 {sleep_time:.2f} 秒后再次运行")
                await asyncio.sleep(sleep_time)
        
        except asyncio.CancelledError:
            logger.info("SQL规范化处理任务被取消")
            raise
        finally:
            # 确保关闭数据库连接池
            try:
                await db_utils.close_db_pool()
            except Exception as e:
                logger.error(f"关闭数据库连接池时出错: {str(e)}")
    
    # 创建并启动SQL规范化处理任务
    task = asyncio.create_task(_run_sql_normalizer(), name="sql_normalizer")
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
    parser = argparse.ArgumentParser(description="PGLumiLineage SQL规范化处理调度器")
    parser.add_argument(
        "--interval", 
        type=int, 
        default=3600, 
        help="检查间隔时间（秒），默认为 3600 秒（1小时）"
    )
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="只运行一次，然后退出"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=100,
        help="每批处理的日志数量，默认为 100"
    )
    parser.add_argument(
        "--max-concurrency",
        type=int,
        default=10,
        help="并发处理的最大任务数，默认为 10"
    )
    
    args = parser.parse_args()
    
    # 注册信号处理程序
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(sig, lambda s=sig: asyncio.create_task(shutdown(s)))
    
    # 启动SQL规范化处理服务
    task = await start_sql_normalizer(
        interval_seconds=args.interval,
        run_once=args.run_once,
        batch_size=args.batch_size,
        max_concurrency=args.max_concurrency
    )
    tasks.append(task)
    
    # 等待任务完成
    try:
        await asyncio.gather(*tasks)
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("收到键盘中断，退出程序")
    except Exception as e:
        logger.error(f"程序运行时出错: {str(e)}", exc_info=True)
        sys.exit(1)
