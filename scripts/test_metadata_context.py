#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试 fetch_metadata_context_for_sql 函数，验证其返回结果是否符合预期
"""

import asyncio
import json
import logging
import sys
import os

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pglumilineage.common import db_utils, logging_config
from pglumilineage.common import models
from pglumilineage.llm_analyzer import service as llm_analyzer_service

# 设置日志
logging_config.setup_logging()
logger = logging.getLogger(__name__)

async def test_fetch_metadata_context_for_sql():
    """
    测试 fetch_metadata_context_for_sql 函数
    
    从 lumi_analytics.sql_patterns 表中获取一个 SQL 模式，
    调用 fetch_metadata_context_for_sql 函数获取元数据上下文，
    验证返回结果是否符合预期
    """
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 从 lumi_analytics.sql_patterns 表中获取一个 SQL 模式
        async with db_utils.db_pool.acquire() as conn:
            # 使用指定的 SQL 模式
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
                last_llm_analysis_at,
                tags
            FROM 
                lumi_analytics.sql_patterns
            WHERE
                sql_hash = '839308704aafe4972f898acd1d5966ad4c52d84451591da5d0ad5fcb1954f43f'
            """
            
            row = await conn.fetchrow(query)
            
            if not row:
                logger.error("未找到任何 SQL 模式")
                return
            
            # 创建 SQL 模式对象
            sql_pattern = models.AnalyticalSQLPattern(
                sql_hash=row['sql_hash'],
                normalized_sql_text=row['normalized_sql_text'],
                sample_raw_sql_text=row['sample_raw_sql_text'],
                source_database_name=row['source_database_name'],
                first_seen_at=row['first_seen_at'],
                last_seen_at=row['last_seen_at'],
                execution_count=row['execution_count'],
                total_duration_ms=row['total_duration_ms'],
                avg_duration_ms=row['avg_duration_ms'],
                max_duration_ms=row['max_duration_ms'],
                min_duration_ms=row['min_duration_ms'],
                llm_analysis_status=row['llm_analysis_status'],
                last_llm_analysis_at=row['last_llm_analysis_at'],
                tags=row['tags']
            )
            
            logger.info(f"获取到 SQL 模式: {sql_pattern.sql_hash[:8]}...")
            logger.info(f"SQL 模式类型: {sql_pattern.source_database_name}")
            logger.info(f"规范化 SQL: {sql_pattern.normalized_sql_text[:100]}...")
            
            # 调用 fetch_metadata_context_for_sql 函数获取元数据上下文
            metadata_context = await llm_analyzer_service.fetch_metadata_context_for_sql(sql_pattern)
            
            # 输出元数据上下文结果
            logger.info(f"元数据上下文获取结果:")
            logger.info(f"源数据库名称: {metadata_context.get('source_database_name')}")
            logger.info(f"表/视图元数据数量: {len(metadata_context.get('tables_metadata', []))}")
            logger.info(f"视图定义数量: {len(metadata_context.get('view_definitions', []))}")
            
            # 使用更友好的格式输出表/视图元数据详情
            print("\n\n=== 元数据上下文详细信息 ===\n")
            
            for i, table in enumerate(metadata_context.get('tables_metadata', []), 1):
                print(f"\u8868名: {table['schema']}.{table['name']}")
                print(f"\u7c7b\u578b: {table['type']}")
                print(f"\u63cf\u8ff0: {table.get('description')}")
                print("\u5217\u4fe1\u606f:")
                
                # 输出列信息
                for column in table.get('columns', []):
                    nullable_str = "NULL" if column['nullable'] else "NOT NULL"
                    pk_str = ", PRIMARY KEY" if column['primary_key'] else ""
                    desc_str = column.get('description') or ""
                    
                    # 处理外键信息（如果有）
                    fk_str = ""
                    if 'foreign_key_to' in column:
                        fk = column['foreign_key_to']
                        fk_str = f", 关联到 {fk['schema']}.{fk['table']}.{fk['column']}"
                    
                    # 将列信息格式化为更友好的形式
                    print(f"  - {column['name']} ({column['type']}, {nullable_str}{pk_str}): {desc_str}{fk_str}")
                
                print("\n")  # 添加空行分隔不同的表
            
            # 输出视图定义详情
            for i, view in enumerate(metadata_context.get('view_definitions', [])):
                logger.info(f"视图定义 {i+1}: {view.get('schema')}.{view.get('name')} ({view.get('type')})")
                logger.info(f"  定义: {view.get('definition')[:100]}...")
            
            # 将完整结果保存到文件
            output_file = "metadata_context_result.json"
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(metadata_context, f, ensure_ascii=False, indent=2)
            logger.info(f"完整结果已保存到文件: {output_file}")
            
    except Exception as e:
        logger.error(f"测试过程中发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # 关闭数据库连接池
        await db_utils.close_db_pool()

if __name__ == "__main__":
    asyncio.run(test_fetch_metadata_context_for_sql())
