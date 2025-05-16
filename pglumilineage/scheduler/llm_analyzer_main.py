#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PGLumiLineage LLM分析器调度器

此模块负责：
1. 调度LLM分析服务的执行
2. 处理信号和优雅关闭
3. 提供命令行接口

作者: Vance Chen
"""

import asyncio
import argparse
import logging
import os
import signal
import sys
from typing import List, Optional
from datetime import datetime

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common import db_utils
from pglumilineage.llm_analyzer import service as llm_analyzer_service

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)

# 全局任务列表
tasks: List[asyncio.Task] = []

# 定义输出目录结构
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), "data")
LLM_DATA_DIR = os.path.join(DATA_DIR, "llm")
PROMPTS_DIR = os.path.join(LLM_DATA_DIR, "prompts")
RESPONSES_DIR = os.path.join(LLM_DATA_DIR, "responses")
METADATA_DIR = os.path.join(LLM_DATA_DIR, "metadata")
RELATIONS_DIR = os.path.join(LLM_DATA_DIR, "relations")
DEBUG_DIR = os.path.join(LLM_DATA_DIR, "debug")

# 确保目录存在
for directory in [PROMPTS_DIR, RESPONSES_DIR, METADATA_DIR, RELATIONS_DIR, DEBUG_DIR]:
    os.makedirs(directory, exist_ok=True)


async def start_llm_analyzer(batch_size: int = 10, interval_seconds: int = 300, run_once: bool = False) -> asyncio.Task:
    """
    启动LLM分析服务
    
    Args:
        batch_size: 每批处理的SQL模式数量
        interval_seconds: 检查间隔时间（秒），默认为300秒（5分钟）
        run_once: 是否只运行一次
        
    Returns:
        asyncio.Task: LLM分析任务
    """
    logger.info(f"启动LLM分析服务，批量大小: {batch_size}，间隔: {interval_seconds}秒，{'单次运行' if run_once else '持续运行'}")
    
    # 创建并启动LLM分析任务
    task = asyncio.create_task(
        llm_analyzer_service.analyze_sql_patterns_with_llm(
            batch_size=batch_size,
            poll_interval_seconds=interval_seconds,
            run_once=run_once
        )
    )
    
    # 添加到全局任务列表
    tasks.append(task)
    
    return task


def shutdown(sig: signal.Signals) -> None:
    """
    优雅关闭所有服务
    
    Args:
        sig: 触发关闭的信号
    """
    logger.info(f"收到信号 {sig.name}，开始优雅关闭...")
    
    # 取消所有任务
    for task in tasks:
        if not task.done():
            task.cancel()
    
    # 关闭数据库连接池
    asyncio.create_task(db_utils.close_db_pool())
    
    logger.info("所有服务已关闭")


async def main() -> None:
    """
    主函数
    """
    # 解析命令行参数
    parser = argparse.ArgumentParser(description="PGLumiLineage LLM分析器")
    parser.add_argument("--batch-size", type=int, default=10, help="每批处理的SQL模式数量")
    parser.add_argument("--interval", type=int, default=300, help="检查间隔时间（秒）")
    parser.add_argument("--run-once", action="store_true", help="只运行一次")
    parser.add_argument("--analyze-sql", type=str, help="分析指定SQL哈希的模式")
    args = parser.parse_args()
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 注册信号处理器
        for sig in [signal.SIGINT, signal.SIGTERM]:
            signal.signal(sig, lambda s, _: asyncio.create_task(asyncio.shield(asyncio.to_thread(shutdown, s))))
        
        # 如果指定了SQL哈希，则只分析该SQL模式
        if args.analyze_sql:
            logger.info(f"分析指定的SQL模式: {args.analyze_sql}")
            # 从数据库获取SQL模式
            query = """
            SELECT 
                sql_hash,
                normalized_sql_text,
                sample_raw_sql_text,
                source_database_name
            FROM 
                lumi_analytics.sql_patterns
            WHERE 
                sql_hash = $1
            LIMIT 1
            """
            
            pool = await db_utils.get_db_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow(query, args.analyze_sql)
                
                if not row:
                    logger.error(f"未找到SQL哈希为 {args.analyze_sql} 的模式")
                    return
                
                # 创建SQL模式对象
                from datetime import datetime
                current_time = datetime.now()
                
                from pglumilineage.common.models import AnalyticalSQLPattern
                sql_pattern = AnalyticalSQLPattern(
                    sql_hash=row['sql_hash'],
                    normalized_sql_text=row['normalized_sql_text'],
                    sample_raw_sql_text=row['sample_raw_sql_text'],
                    source_database_name=row['source_database_name'],
                    first_seen_at=current_time,
                    last_seen_at=current_time,
                    execution_count=1,
                    total_duration_ms=0,
                    avg_duration_ms=0.0,
                    max_duration_ms=0,
                    min_duration_ms=0
                )
                
                logger.info(f"获取到SQL模式: {sql_pattern.sql_hash[:8]}...")
                
                # 获取元数据上下文
                metadata_context = await llm_analyzer_service.fetch_metadata_context_for_sql(sql_pattern)
                
                # 保存元数据上下文到文件
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                metadata_file = os.path.join(METADATA_DIR, f"{sql_pattern.sql_hash[:8]}_{timestamp}.json")
                with open(metadata_file, "w", encoding="utf-8") as f:
                    import json
                    json.dump(metadata_context, f, indent=2, ensure_ascii=False)
                
                logger.info(f"元数据上下文已保存到文件: {metadata_file}")
                
                # 确定SQL模式类型
                sql_mode = "INSERT"  # 默认值
                if sql_pattern.normalized_sql_text.strip().upper().startswith("SELECT"):
                    sql_mode = "SELECT"
                elif sql_pattern.normalized_sql_text.strip().upper().startswith("UPDATE"):
                    sql_mode = "UPDATE"
                elif sql_pattern.normalized_sql_text.strip().upper().startswith("DELETE"):
                    sql_mode = "DELETE"
                elif sql_pattern.normalized_sql_text.strip().upper().startswith("INSERT"):
                    sql_mode = "INSERT"
                
                logger.info(f"SQL模式类型: {sql_mode}")
                
                # 构造LLM的prompt
                messages = llm_analyzer_service.construct_prompt_for_qwen(
                    sql_mode=sql_mode,
                    sample_sql=sql_pattern.sample_raw_sql_text,
                    metadata_context=metadata_context,
                    sql_hash=sql_pattern.sql_hash
                )
                
                # 保存prompt到文件
                prompt_file = os.path.join(PROMPTS_DIR, f"{sql_pattern.sql_hash[:8]}_{timestamp}.json")
                with open(prompt_file, "w", encoding="utf-8") as f:
                    json.dump(messages, f, indent=2, ensure_ascii=False)
                
                logger.info(f"LLM prompt已保存到文件: {prompt_file}")
                
                # 调用LLM API
                response_content = await llm_analyzer_service.call_qwen_api(messages)
                
                if not response_content:
                    logger.error("LLM API调用失败，未获取到响应内容")
                    return
                
                # 保存LLM响应内容到文件
                response_file = os.path.join(RESPONSES_DIR, f"{sql_pattern.sql_hash[:8]}_{timestamp}.txt")
                with open(response_file, "w", encoding="utf-8") as f:
                    f.write(response_content)
                
                logger.info(f"LLM响应内容已保存到文件: {response_file}")
                
                # 解析LLM响应内容，提取实体关系
                relations_json = llm_analyzer_service.parse_llm_response(response_content)
                
                if not relations_json:
                    logger.error("解析LLM响应内容失败，未获取到实体关系")
                    return
                
                # 保存实体关系到文件
                relations_file = os.path.join(RELATIONS_DIR, f"{sql_pattern.sql_hash[:8]}_{timestamp}.json")
                with open(relations_file, "w", encoding="utf-8") as f:
                    json.dump(relations_json, f, indent=2, ensure_ascii=False)
                
                logger.info(f"实体关系已保存到文件: {relations_file}")
                
                # 更新SQL模式的分析结果
                await llm_analyzer_service.update_sql_pattern_analysis_result(
                    sql_hash=sql_pattern.sql_hash,
                    status="COMPLETED",
                    relations_json=relations_json
                )
                
                logger.info(f"已更新SQL模式 {sql_pattern.sql_hash[:8]}... 的分析结果")
        else:
            # 启动LLM分析服务
            await start_llm_analyzer(
                batch_size=args.batch_size,
                interval_seconds=args.interval,
                run_once=args.run_once
            )
            
            # 等待所有任务完成
            await asyncio.gather(*tasks, return_exceptions=True)
    
    except asyncio.CancelledError:
        logger.info("任务被取消")
    except Exception as e:
        logger.error(f"运行LLM分析器时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # 关闭数据库连接池
        await db_utils.close_db_pool()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("收到键盘中断，退出程序")
    except Exception as e:
        logger.error(f"程序异常退出: {str(e)}")
        sys.exit(1)
