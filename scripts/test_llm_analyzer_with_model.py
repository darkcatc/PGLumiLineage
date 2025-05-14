#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LLM分析器测试脚本（指定模型名称）

该脚本用于测试LLM分析器模块，使用通义千问(Qwen)对SQL模式进行解析，
提取实体关系并输出JSON格式。可以指定模型名称。

作者: Vance Chen
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any

# 添加项目根目录到Python路径
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pglumilineage.common import config, db_utils
from pglumilineage.common.logging_config import setup_logging
from pglumilineage.llm_analyzer import service as llm_service

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)

async def fetch_test_sql_pattern(sql_hash: Optional[str] = None) -> Optional[Dict]:
    """
    从lumi_analytics.sql_patterns表中获取一个测试用的SQL模式
    
    Args:
        sql_hash: 指定的SQL哈希值，如果为None则获取一个随机的SQL模式
        
    Returns:
        Optional[Dict]: SQL模式对象
    """
    try:
        # 初始化数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 构造查询语句
            if sql_hash:
                query = """
                SELECT 
                    sql_hash,
                    normalized_sql_text,
                    sample_raw_sql_text,
                    source_database_name,
                    first_seen_at,
                    last_seen_at,
                    execution_count,
                    total_duration_ms,
                    avg_duration_ms,
                    max_duration_ms,
                    min_duration_ms,
                    llm_analysis_status,
                    llm_extracted_relations_json,
                    last_llm_analysis_at,
                    tags
                FROM 
                    lumi_analytics.sql_patterns
                WHERE 
                    sql_hash = $1
                """
                row = await conn.fetchrow(query, sql_hash)
            else:
                # 获取一个随机的SQL模式，优先选择INSERT/UPDATE/CREATE语句
                query = """
                SELECT 
                    sql_hash,
                    normalized_sql_text,
                    sample_raw_sql_text,
                    source_database_name,
                    first_seen_at,
                    last_seen_at,
                    execution_count,
                    total_duration_ms,
                    avg_duration_ms,
                    max_duration_ms,
                    min_duration_ms,
                    llm_analysis_status,
                    llm_extracted_relations_json,
                    last_llm_analysis_at,
                    tags
                FROM 
                    lumi_analytics.sql_patterns
                WHERE 
                    normalized_sql_text ILIKE 'INSERT%' OR
                    normalized_sql_text ILIKE 'UPDATE%' OR
                    normalized_sql_text ILIKE 'CREATE%'
                ORDER BY 
                    execution_count DESC
                LIMIT 1
                """
                row = await conn.fetchrow(query)
            
            if not row:
                logger.warning("未找到符合条件的SQL模式")
                return None
            
            # 将查询结果转换为字典
            pattern = dict(row)
            logger.info(f"获取到SQL模式: {pattern['sql_hash'][:8]}...")
            
            return pattern
            
    except Exception as e:
        logger.error(f"获取测试SQL模式失败: {str(e)}")
        return None

async def test_llm_analysis(sql_hash: Optional[str] = None, model_name: str = "qwen-max") -> None:
    """
    测试LLM分析功能
    
    Args:
        sql_hash: 指定的SQL哈希值，如果为None则获取一个随机的SQL模式
        model_name: 指定的模型名称，默认为qwen-max
    """
    try:
        # 1. 获取测试SQL模式
        sql_pattern = await fetch_test_sql_pattern(sql_hash)
        if not sql_pattern:
            logger.error("无法获取测试SQL模式，测试终止")
            return
        
        # 2. 初始化数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 3. 创建一个模拟的AnalyticalSQLPattern对象
        from pglumilineage.common.models import AnalyticalSQLPattern
        pattern_obj = AnalyticalSQLPattern(
            sql_hash=sql_pattern['sql_hash'],
            normalized_sql_text=sql_pattern['normalized_sql_text'],
            sample_raw_sql_text=sql_pattern['sample_raw_sql_text'],
            source_database_name=sql_pattern['source_database_name'],
            first_seen_at=sql_pattern['first_seen_at'],
            last_seen_at=sql_pattern['last_seen_at'],
            execution_count=sql_pattern['execution_count'],
            total_duration_ms=sql_pattern['total_duration_ms'],
            avg_duration_ms=sql_pattern['avg_duration_ms'],
            max_duration_ms=sql_pattern['max_duration_ms'],
            min_duration_ms=sql_pattern['min_duration_ms'],
            llm_analysis_status=sql_pattern['llm_analysis_status'],
            llm_extracted_relations_json=sql_pattern['llm_extracted_relations_json'],
            last_llm_analysis_at=sql_pattern['last_llm_analysis_at'],
            tags=sql_pattern['tags']
        )
        
        # 4. 获取SQL模式的元数据上下文
        metadata_context = await llm_service.fetch_metadata_context_for_sql(pattern_obj)
        
        # 5. 确定SQL模式类型
        sql_mode = "UNKNOWN"
        normalized_sql_lower = sql_pattern['normalized_sql_text'].lower()
        if normalized_sql_lower.startswith("insert"):
            sql_mode = "INSERT"
        elif normalized_sql_lower.startswith("update"):
            sql_mode = "UPDATE"
        elif normalized_sql_lower.startswith("select"):
            sql_mode = "SELECT"
        elif normalized_sql_lower.startswith("create"):
            sql_mode = "CREATE"
        elif normalized_sql_lower.startswith("delete"):
            sql_mode = "DELETE"
        elif normalized_sql_lower.startswith("merge"):
            sql_mode = "MERGE"
        
        logger.info(f"SQL模式类型: {sql_mode}, 哈希值: {sql_pattern['sql_hash'][:8]}...")
        
        # 6. 构造Qwen prompt
        messages = llm_service.construct_prompt_for_qwen(
            sql_mode=sql_mode,
            sample_sql=sql_pattern['sample_raw_sql_text'],
            metadata_context=metadata_context,
            sql_hash=sql_pattern['sql_hash']
        )
        
        # 打印构造的prompt，便于调试
        logger.info("构造的Qwen prompt:")
        for msg in messages:
            logger.info(f"Role: {msg['role']}")
            logger.info(f"Content: {msg['content'][:100]}...")
        
        # 7. 调用Qwen API，使用指定的模型名称
        logger.info(f"调用Qwen API分析SQL模式: {sql_pattern['sql_hash'][:8]}..., 使用模型: {model_name}")
        response_content = await llm_service.call_qwen_api(messages, model_name=model_name)
        
        if not response_content:
            logger.error("LLM API返回空响应")
            return
        
        # 8. 解析LLM响应
        logger.info(f"解析LLM响应...")
        relations_json = llm_service.parse_llm_response(response_content)
        
        if not relations_json:
            logger.error("解析LLM响应失败")
            return
            
        # 9. 打印解析结果
        logger.info(f"解析结果:")
        logger.info(json.dumps(relations_json, ensure_ascii=False, indent=2))
        
        # 10. 更新SQL模式的分析结果
        # 这里只是测试，所以不实际更新数据库
        logger.info(f"测试完成，不更新数据库")
        
    except Exception as e:
        logger.error(f"测试LLM分析失败: {str(e)}")
    finally:
        # 关闭数据库连接池
        try:
            await db_utils.close_db_pool()
            logger.info("数据库连接池已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接池失败: {str(e)}")

async def main():
    """
    主函数
    """
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='测试LLM分析器')
    parser.add_argument('--sql-hash', type=str, help='指定的SQL哈希值')
    parser.add_argument('--model', type=str, default='qwen-max', help='指定的模型名称，默认为qwen-max')
    args = parser.parse_args()
    
    # 测试LLM分析
    await test_llm_analysis(args.sql_hash, args.model)

if __name__ == "__main__":
    asyncio.run(main())
