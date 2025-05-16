#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试LLM分析SQL模式的脚本

此脚本用于测试使用LLM分析SQL模式并获取实体关系结果。

作者: Vance Chen
"""

import asyncio
import json
import logging
import sys
import os
from typing import Dict, Any, Optional

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common import db_utils, models
from pglumilineage.llm_analyzer import service as llm_analyzer_service

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)


async def test_llm_analysis_for_sql():
    """
    测试使用LLM分析SQL模式并获取实体关系结果
    """
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 从数据库中获取一个SQL模式
        query = """
        SELECT 
            sql_hash,
            normalized_sql_text,
            sample_raw_sql_text,
            source_database_name
        FROM 
            lumi_analytics.sql_patterns
        WHERE 
            sql_hash = '8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8'
        LIMIT 1
        """
        
        # 定义SQL模式变量
        sql_pattern = None
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 从连接池中获取连接
        async with pool.acquire() as conn:
            row = await conn.fetchrow(query)
            
            if not row:
                logger.error("未找到指定的SQL模式")
                return
            
            # 创建SQL模式对象
            from datetime import datetime
            current_time = datetime.now()
            
            sql_pattern = models.AnalyticalSQLPattern(
                sql_hash=row['sql_hash'],
                normalized_sql_text=row['normalized_sql_text'],
                sample_raw_sql_text=row['sample_raw_sql_text'],
                source_database_name=row['source_database_name'],
                first_seen_at=current_time,  # 必要字段
                last_seen_at=current_time,   # 必要字段
                execution_count=1,
                total_duration_ms=0,
                avg_duration_ms=0.0,
                max_duration_ms=0,
                min_duration_ms=0
            )
            
            logger.info(f"获取到SQL模式: {sql_pattern.sql_hash[:8]}...")
            logger.info(f"SQL模式类型: {sql_pattern.source_database_name}")
            logger.info(f"规范化SQL: {sql_pattern.normalized_sql_text[:100]}...")
            logger.info(f"原始SQL示例: {sql_pattern.sample_raw_sql_text[:100]}...")
        
        # 如果没有找到SQL模式，则退出
        if sql_pattern is None:
            return
        
        # 获取SQL模式的元数据上下文
        metadata_context = await llm_analyzer_service.fetch_metadata_context_for_sql(sql_pattern)
        
        # 保存元数据上下文到文件，方便查看
        with open("llm_metadata_context.json", "w", encoding="utf-8") as f:
            json.dump(metadata_context, f, indent=2, ensure_ascii=False)
        
        logger.info("元数据上下文已保存到文件: llm_metadata_context.json")
        
        # 确定SQL模式类型
        sql_mode = "INSERT"  # 这里可以根据SQL语句自动判断，但为了简化，我们直接指定
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
        
        # 保存prompt到文件，方便查看
        with open("llm_prompt.json", "w", encoding="utf-8") as f:
            json.dump(messages, f, indent=2, ensure_ascii=False)
        
        logger.info("LLM prompt已保存到文件: llm_prompt.json")
        
        # 调用LLM API
        response_content = await llm_analyzer_service.call_qwen_api(messages)
        
        if not response_content:
            logger.error("LLM API调用失败，未获取到响应内容")
            return
        
        # 保存LLM响应内容到文件，方便查看
        with open("llm_response.txt", "w", encoding="utf-8") as f:
            f.write(response_content)
        
        logger.info("LLM响应内容已保存到文件: llm_response.txt")
        
        # 解析LLM响应内容，提取实体关系
        try:
            # 尝试使用服务中的解析函数
            relations_json = llm_analyzer_service.parse_llm_response(response_content)
            
            if not relations_json:
                # 如果服务中的解析函数失败，尝试自定义解析逻辑
                logger.warning("使用服务中的解析函数失败，尝试自定义解析逻辑")
                
                # 尝试提取JSON内容
                import re
                json_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
                json_match = re.search(json_pattern, response_content)
                
                if json_match:
                    json_str = json_match.group(1)
                    logger.debug(f"从 markdown 代码块中提取到 JSON 字符串")
                else:
                    # 如果没有 markdown 格式，尝试直接解析整个响应
                    json_str = response_content
                    logger.debug(f"使用完整响应作为 JSON 字符串")
                
                # 清理 JSON 字符串，处理可能的格式问题
                
                # 特别处理单引号在JSON字符串中的问题
                # 先将单引号包围的字符串中的双引号替换为临时占位符
                json_str = re.sub(r"'([^']*?)'", lambda m: "'" + m.group(1).replace('"', '##TEMP_QUOTE##') + "'", json_str)
                # 将单引号替换为双引号
                json_str = json_str.replace("'", "\"")
                # 将临时占位符替换回双引号
                json_str = json_str.replace('##TEMP_QUOTE##', '\\"')
                
                # 删除可能的 JavaScript 注释
                json_str = re.sub(r'\s*//.*?[\r\n]', '\n', json_str)
                
                # 尝试修复常见的JSON格式问题
                json_str = re.sub(r',\s*([\]\}])', r'\1', json_str)  # 移除尾随逗号
                
                # 手动修复第22行的问题
                json_str = json_str.replace('"TO_CHAR(d.d_date, \'YYYY-MM\')"', '"TO_CHAR(d.d_date, \\"YYYY-MM\\")"')
                
                try:
                    relations_json = json.loads(json_str)
                    logger.info(f"使用自定义解析逻辑成功解析 JSON")
                except json.JSONDecodeError as e:
                    logger.error(f"自定义解析逻辑也失败: {str(e)}")
                    logger.debug(f"尝试解析的内容: {json_str[:500]}...")
                    return
            
        except Exception as e:
            logger.error(f"解析LLM响应内容失败: {str(e)}")
            import traceback
            logger.error(traceback.format_exc())
            return
        
        # 保存实体关系到文件，方便查看
        with open("llm_relations.json", "w", encoding="utf-8") as f:
            json.dump(relations_json, f, indent=2, ensure_ascii=False)
        
        logger.info("实体关系已保存到文件: llm_relations.json")
        
        # 打印实体关系
        logger.info("实体关系:")
        logger.info(json.dumps(relations_json, indent=2, ensure_ascii=False))
        
    except Exception as e:
        logger.error(f"测试LLM分析SQL模式失败: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # 关闭数据库连接池
        await db_utils.close_db_pool()


if __name__ == "__main__":
    asyncio.run(test_llm_analysis_for_sql())
