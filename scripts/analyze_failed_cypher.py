#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
分析失败的 AGE Cypher 语句脚本

此脚本用于分析执行失败的 Cypher 语句，并提供诊断信息。

作者: Vance Chen
"""

import os
import sys
import asyncio
import asyncpg
import argparse
import logging
import re
import json
from typing import List, Dict, Any, Tuple

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入转换函数
from pglumilineage.graph_builder.service import convert_cypher_for_age

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def load_cypher_statements(file_path: str) -> List[str]:
    """
    从文件加载 Cypher 语句

    Args:
        file_path: Cypher 语句文件路径

    Returns:
        List[str]: Cypher 语句列表
    """
    statements = []
    current_statement = ""
    statement_started = False
    statement_index = 0
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip()
            
            # 检查是否是新语句的开始标记
            if line.startswith('-- 语句 '):
                # 如果已经有收集的语句，保存它
                if statement_started and current_statement.strip():
                    statements.append((statement_index, current_statement.strip()))
                
                # 开始新的语句
                try:
                    statement_index = int(line.replace('-- 语句 ', '').strip())
                except ValueError:
                    statement_index += 1
                current_statement = ""
                statement_started = True
                continue
            
            # 如果已经开始收集语句，添加行到当前语句
            if statement_started:
                current_statement += line + "\n"
    
    # 添加最后一个语句
    if statement_started and current_statement.strip():
        statements.append((statement_index, current_statement.strip()))
    
    return statements


async def analyze_statement(conn: asyncpg.Connection, graph_name: str, stmt_index: int, stmt: str) -> Dict[str, Any]:
    """
    分析单个 Cypher 语句

    Args:
        conn: 数据库连接
        graph_name: 图名称
        stmt_index: 语句索引
        stmt: Cypher 语句

    Returns:
        Dict[str, Any]: 分析结果
    """
    result = {
        "index": stmt_index,
        "original_statement": stmt,
        "converted_statement": convert_cypher_for_age(stmt),
        "success": False,
        "error": None,
        "error_type": None,
        "diagnosis": None,
        "fix_suggestion": None
    }
    
    try:
        # 使用 convert_cypher_for_age 函数转换 Cypher 语句
        converted_stmt = convert_cypher_for_age(stmt)
        result["converted_statement"] = converted_stmt
        
        # 尝试执行转换后的语句
        query = f"SELECT * FROM cypher('{graph_name}', $$ {converted_stmt} $$) AS (result agtype);"
        await conn.execute(query)
        
        result["success"] = True
    except Exception as e:
        error_msg = str(e)
        result["error"] = error_msg
        
        # 分析错误类型
        if "syntax error at or near" in error_msg:
            result["error_type"] = "SYNTAX_ERROR"
            
            # 提取错误位置
            error_at = re.search(r'syntax error at or near "([^"]+)"', error_msg)
            if error_at:
                error_token = error_at.group(1)
                result["diagnosis"] = f"语法错误，位于或靠近 '{error_token}'"
                
                # 根据错误位置提供修复建议
                if error_token == "ON":
                    result["fix_suggestion"] = "AGE 1.5.0 不支持 ON CREATE SET/ON MATCH SET 语法，需要拆分为多个语句执行"
                elif error_token == "{":
                    result["fix_suggestion"] = "属性语法可能有问题，检查大括号内的格式"
                elif error_token == "end of input":
                    result["fix_suggestion"] = "语句可能不完整，或者缺少必要的子句"
            else:
                result["diagnosis"] = "未知语法错误"
                result["fix_suggestion"] = "检查语句的整体结构"
        elif "variable" in error_msg and "is for an edge" in error_msg:
            result["error_type"] = "EDGE_VARIABLE_ERROR"
            result["diagnosis"] = "AGE 1.5.0 不允许在某些上下文中使用边变量"
            result["fix_suggestion"] = "将语句拆分为多个部分，避免在 MERGE 中引用边变量"
        else:
            result["error_type"] = "UNKNOWN_ERROR"
            result["diagnosis"] = "未知错误"
            result["fix_suggestion"] = "检查语句的整体结构和语法"
    
    return result


async def analyze_statements(conn: asyncpg.Connection, graph_name: str, statements: List[Tuple[int, str]]) -> List[Dict[str, Any]]:
    """
    分析 Cypher 语句列表

    Args:
        conn: 数据库连接
        graph_name: 图名称
        statements: Cypher 语句列表，每项为 (索引, 语句)

    Returns:
        List[Dict[str, Any]]: 分析结果列表
    """
    results = []
    
    # 设置搜索路径
    await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
    logger.info("已设置搜索路径")
    
    # 确保图存在
    try:
        await conn.execute(f"SELECT * FROM create_graph('{graph_name}')")
        logger.info(f"成功创建图 {graph_name}")
    except Exception as e:
        if "already exists" in str(e):
            logger.info(f"图 {graph_name} 已存在，继续执行")
        else:
            logger.error(f"创建图失败: {e}")
            return []
    
    # 分析每条语句
    for idx, stmt in statements:
        logger.info(f"分析语句 {idx}...")
        result = await analyze_statement(conn, graph_name, idx, stmt)
        results.append(result)
        
        # 如果分析失败，记录详细信息
        if not result["success"]:
            logger.error(f"语句 {idx} 执行失败: {result['error']}")
            logger.error(f"诊断: {result['diagnosis']}")
            logger.error(f"修复建议: {result['fix_suggestion']}")
    
    return results


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='分析失败的 AGE Cypher 语句')
    parser.add_argument('--host', default='localhost', help='数据库主机')
    parser.add_argument('--port', type=int, default=5432, help='数据库端口')
    parser.add_argument('--user', default='lumiadmin', help='数据库用户名')
    parser.add_argument('--password', default='lumiadmin', help='数据库密码')
    parser.add_argument('--database', default='iwdb', help='数据库名称')
    parser.add_argument('--graph', default='pglumilineage_graph', help='图名称')
    parser.add_argument('--file', required=True, help='Cypher 语句文件路径')
    parser.add_argument('--output', default='failed_cypher_analysis.json', help='分析结果输出文件路径')
    
    args = parser.parse_args()
    
    # 连接数据库
    try:
        conn = await asyncpg.connect(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database
        )
        logger.info(f"成功连接到数据库 {args.database}")
    except Exception as e:
        logger.error(f"连接数据库失败: {e}")
        return
    
    try:
        # 加载 Cypher 语句
        statements = await load_cypher_statements(args.file)
        logger.info(f"从文件 {args.file} 加载了 {len(statements)} 条 Cypher 语句")
        
        # 分析 Cypher 语句
        results = await analyze_statements(conn, args.graph, statements)
        
        # 统计成功和失败的语句
        success_count = sum(1 for r in results if r["success"])
        failed_count = len(results) - success_count
        logger.info(f"总共分析了 {len(results)} 条语句，成功 {success_count} 条，失败 {failed_count} 条")
        
        # 保存分析结果
        with open(args.output, 'w', encoding='utf-8') as f:
            json.dump(results, f, ensure_ascii=False, indent=2)
        logger.info(f"分析结果已保存到 {args.output}")
        
        # 输出失败语句的统计信息
        if failed_count > 0:
            error_types = {}
            for r in results:
                if not r["success"]:
                    error_type = r["error_type"]
                    if error_type not in error_types:
                        error_types[error_type] = 0
                    error_types[error_type] += 1
            
            logger.info("失败语句的错误类型统计:")
            for error_type, count in error_types.items():
                logger.info(f"  {error_type}: {count} 条")
    finally:
        # 关闭数据库连接
        await conn.close()
        logger.info("数据库连接已关闭")


if __name__ == "__main__":
    asyncio.run(main())
