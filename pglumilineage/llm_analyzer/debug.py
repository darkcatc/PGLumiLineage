#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
LLM分析器调试模块

该模块提供了调试LLM分析器功能的工具，用于测试通义千问对SQL模式的解析，
提取实体关系并输出JSON格式。

作者: Vance Chen
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple

import asyncpg

# 导入项目模块
from pglumilineage.common import config, db_utils
from pglumilineage.common.logging_config import setup_logging, get_logger
from pglumilineage.common.models import AnalyticalSQLPattern
from pglumilineage.llm_analyzer import service as llm_service

# 设置日志
logger = get_logger(__name__)

async def get_metadata_for_tables(conn: asyncpg.Connection, source_id: int, tables: List[Dict]) -> Dict:
    """
    获取指定表的元数据信息
    
    Args:
        conn: 数据库连接
        source_id: 数据源ID
        tables: 表信息列表，每个元素包含schema和name
        
    Returns:
        Dict: 元数据信息
    """
    metadata_context = {
        "tables_metadata": [],
        "view_definitions": []
    }
    
    for table_info in tables:
        schema_name = table_info.get("schema", "public")
        table_name = table_info.get("name")
        
        if not table_name:
            continue
        
        # 查询对象元数据
        object_query = """
        SELECT 
            object_id, 
            object_type, 
            definition,
            row_count,
            description
        FROM 
            lumi_metadata_store.objects_metadata 
        WHERE 
            source_id = $1 AND 
            schema_name = $2 AND 
            object_name = $3
        """
        
        object_row = await conn.fetchrow(
            object_query, 
            source_id, 
            schema_name, 
            table_name
        )
        
        if not object_row:
            logger.warning(f"未找到对象 {schema_name}.{table_name} 的元数据")
            continue
        
        object_id = object_row['object_id']
        object_type = object_row['object_type']
        definition = object_row['definition']
        row_count = object_row['row_count']
        description = object_row['description']
        
        # 查询列元数据
        columns_query = """
        SELECT 
            column_name, 
            data_type, 
            is_nullable, 
            is_primary_key,
            description
        FROM 
            lumi_metadata_store.columns_metadata 
        WHERE 
            object_id = $1
        ORDER BY 
            ordinal_position
        """
        
        columns_rows = await conn.fetch(columns_query, object_id)
        
        # 构造列信息列表
        columns = []
        for col in columns_rows:
            columns.append({
                "name": col['column_name'],
                "type": col['data_type'],
                "nullable": col['is_nullable'],
                "primary_key": col['is_primary_key'],
                "description": col['description']
            })
        
        # 构造对象元数据
        object_metadata = {
            "schema": schema_name,
            "name": table_name,
            "type": object_type,
            "columns": columns,
            "row_count": row_count,
            "description": description
        }
        
        # 将对象元数据添加到相应的列表中
        metadata_context["tables_metadata"].append(object_metadata)
        
        # 如果是视图，将定义添加到视图定义列表中
        if object_type == 'VIEW' and definition:
            metadata_context["view_definitions"].append({
                "schema": schema_name,
                "name": table_name,
                "definition": definition
            })
    
    return metadata_context

async def extract_tables_from_sql(sql: str) -> List[Dict]:
    """
    从SQL中提取表引用
    
    Args:
        sql: SQL语句
        
    Returns:
        List[Dict]: 表引用列表
    """
    try:
        import sqlglot
        from sqlglot import exp
        
        # 解析SQL语句
        parsed_sql = sqlglot.parse_one(sql)
        
        # 提取所有表引用
        table_refs = parsed_sql.find_all(exp.Table)
        
        # 处理每个表引用
        tables_info = []
        for table_ref in table_refs:
            schema_name = table_ref.args.get('db') or 'public'  # 默认使用public schema
            table_name = table_ref.args.get('this')
            table_alias = table_ref.args.get('alias')
            
            if not table_name:
                continue
            
            # 将表信息添加到列表中
            tables_info.append({
                "schema": schema_name,
                "name": table_name,
                "alias": table_alias
            })
        
        # 去重，避免重复查询相同的表
        unique_tables = []
        for table in tables_info:
            if table not in unique_tables:
                unique_tables.append(table)
        
        logger.info(f"从SQL中提取到 {len(unique_tables)} 个表/视图引用")
        return unique_tables
        
    except sqlglot.errors.ParseError as e:
        logger.warning(f"SQL解析失败: {str(e)}")
        return []
    except Exception as e:
        logger.error(f"提取表引用失败: {str(e)}")
        return []

async def analyze_sql_with_llm(sql: str, source_database_name: str) -> Tuple[bool, Dict]:
    """
    使用LLM分析SQL语句
    
    Args:
        sql: SQL语句
        source_database_name: 源数据库名称
        
    Returns:
        Tuple[bool, Dict]: (成功标志, 分析结果)
    """
    try:
        # 初始化数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 确定SQL模式类型
        sql_mode = "UNKNOWN"
        sql_lower = sql.lower()
        if sql_lower.startswith("insert"):
            sql_mode = "INSERT"
        elif sql_lower.startswith("update"):
            sql_mode = "UPDATE"
        elif sql_lower.startswith("select"):
            sql_mode = "SELECT"
        elif sql_lower.startswith("create"):
            sql_mode = "CREATE"
        elif sql_lower.startswith("delete"):
            sql_mode = "DELETE"
        elif sql_lower.startswith("merge"):
            sql_mode = "MERGE"
        
        logger.info(f"SQL模式类型: {sql_mode}")
        
        # 提取表引用
        tables = await extract_tables_from_sql(sql)
        if not tables:
            logger.warning("未从SQL中提取到表引用")
            return False, {"error": "未从SQL中提取到表引用"}
        
        # 获取源数据库的source_id
        async with pool.acquire() as conn:
            source_id_query = """
            SELECT source_id 
            FROM lumi_config.data_sources 
            WHERE source_name = $1
            """
            source_id_row = await conn.fetchrow(source_id_query, source_database_name)
            
            if not source_id_row:
                logger.warning(f"未找到源数据库 {source_database_name} 的配置信息")
                return False, {"error": f"未找到源数据库 {source_database_name} 的配置信息"}
            
            source_id = source_id_row['source_id']
            
            # 获取表的元数据信息
            metadata_context = await get_metadata_for_tables(conn, source_id, tables)
            
            if not metadata_context["tables_metadata"]:
                logger.warning("未找到相关表的元数据信息")
                return False, {"error": "未找到相关表的元数据信息"}
            
            # 构造Qwen prompt
            # 生成一个简单的哈希值作为测试用途
            import hashlib
            test_sql_hash = hashlib.md5(sql.encode('utf-8')).hexdigest()
            
            messages = llm_service.construct_prompt_for_qwen(
                sql_mode=sql_mode,
                sample_sql=sql,
                metadata_context=metadata_context,
                sql_hash=test_sql_hash
            )
            
            # 调用Qwen API
            logger.info("调用Qwen API分析SQL...")
            response_content = await llm_service.call_qwen_api(messages)
            
            if not response_content:
                logger.error("LLM API返回空响应")
                return False, {"error": "LLM API返回空响应"}
            
            # 解析LLM响应
            logger.info("解析LLM响应...")
            relations_json = llm_service.parse_llm_response(response_content)
            
            if not relations_json:
                logger.error("无法解析LLM响应")
                return False, {"error": "无法解析LLM响应"}
            
            return True, relations_json
            
    except Exception as e:
        logger.error(f"分析SQL失败: {str(e)}")
        return False, {"error": str(e)}

async def main():
    """主函数"""
    # 设置日志
    setup_logging()
    
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='使用LLM分析SQL语句')
    parser.add_argument('--sql', type=str, help='要分析的SQL语句')
    parser.add_argument('--file', type=str, help='包含SQL语句的文件路径')
    parser.add_argument('--db', type=str, default='tpcds', help='源数据库名称')
    parser.add_argument('--output', type=str, help='输出结果的文件路径')
    args = parser.parse_args()
    
    # 获取SQL语句
    sql = None
    if args.sql:
        sql = args.sql
    elif args.file:
        try:
            with open(args.file, 'r', encoding='utf-8') as f:
                sql = f.read()
        except Exception as e:
            logger.error(f"读取SQL文件失败: {str(e)}")
            return
    else:
        logger.error("请提供SQL语句或SQL文件路径")
        return
    
    if not sql:
        logger.error("SQL语句为空")
        return
    
    # 分析SQL
    success, result = await analyze_sql_with_llm(sql, args.db)
    
    # 输出结果
    if success:
        if args.output:
            try:
                with open(args.output, 'w', encoding='utf-8') as f:
                    json.dump(result, f, ensure_ascii=False, indent=2)
                logger.info(f"分析结果已保存到 {args.output}")
            except Exception as e:
                logger.error(f"保存分析结果失败: {str(e)}")
                # 打印到控制台
                print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            # 打印到控制台
            print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        logger.error(f"分析失败: {result.get('error', '未知错误')}")

if __name__ == "__main__":
    asyncio.run(main())
