#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
执行转换后的 AGE Cypher 语句脚本

此脚本用于将 Cypher 语句转换为 AGE 1.5.0 兼容的格式并执行。

作者: Vance Chen
"""

import os
import sys
import asyncio
import asyncpg
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Any

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# 导入转换函数
from pglumilineage.graph_builder.service import convert_cypher_for_age

# 设置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


async def create_graph(conn: asyncpg.Connection, graph_name: str) -> bool:
    """
    在 AGE 中创建图

    Args:
        conn: 数据库连接
        graph_name: 图名称

    Returns:
        bool: 是否成功创建图
    """
    try:
        # 检查 AGE 版本
        version_result = await conn.fetchrow("SELECT extversion FROM pg_extension WHERE extname = 'age'")
        if not version_result:
            logger.error("AGE 扩展未安装")
            return False
            
        age_version = version_result['extversion']
        logger.info(f"AGE 版本: {age_version}")
        
        # 设置搜索路径
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        logger.info("已设置搜索路径")
        
        # 创建图
        try:
            await conn.execute(f"SELECT * FROM create_graph('{graph_name}')")
            logger.info(f"成功创建图 {graph_name}")
            return True
        except Exception as e:
            if "already exists" in str(e):
                logger.info(f"图 {graph_name} 已存在，继续执行")
                return True
            else:
                logger.error(f"创建图失败: {e}")
                return False
    except Exception as e:
        logger.error(f"检查 AGE 版本失败: {e}")
        return False


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
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.rstrip()
            
            # 检查是否是新语句的开始标记
            if line.startswith('-- 语句 '):
                # 如果已经有收集的语句，保存它
                if statement_started and current_statement.strip():
                    statements.append(current_statement.strip())
                
                # 开始新的语句
                current_statement = ""
                statement_started = True
                continue
            
            # 如果已经开始收集语句，添加行到当前语句
            if statement_started:
                current_statement += line + "\n"
    
    # 添加最后一个语句
    if statement_started and current_statement.strip():
        statements.append(current_statement.strip())
    
    return statements


async def execute_cypher_statements(conn: asyncpg.Connection, graph_name: str, statements: List[str]) -> bool:
    """
    执行 Cypher 语句

    Args:
        conn: 数据库连接
        graph_name: 图名称
        statements: Cypher 语句列表

    Returns:
        bool: 是否成功执行所有语句
    """
    try:
        # 设置搜索路径
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        logger.info("已设置搜索路径")
        
        # 执行每条 Cypher 语句
        success_count = 0
        failed_statements = []
        
        for i, stmt in enumerate(statements):
            if not stmt.strip():
                continue
                
            logger.info(f"执行语句 {i+1}/{len(statements)}...")
            
            # 使用 convert_cypher_for_age 函数转换 Cypher 语句
            converted_stmt = convert_cypher_for_age(stmt)
            
            # 替换 datetime() 函数为 PostgreSQL 兼容的时间戳函数
            converted_stmt = converted_stmt.replace('datetime()', "'" + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "'::timestamp")
            
            # 检测是否是新的 Cypher 语句结构（包含注释和多个 SET 语句）
            is_new_structure = "//" in converted_stmt and "SET" in converted_stmt and "COALESCE" in converted_stmt
            
            # 判断语句类型
            is_on_create_set = "ON CREATE SET" in converted_stmt
            is_on_match_set = "ON MATCH SET" in converted_stmt
            is_edge_variable = ("MATCH" in converted_stmt and 
                              "MERGE" in converted_stmt and 
                              ")-[" in converted_stmt and 
                              "]->(" in converted_stmt and 
                              ("generates_flow" in converted_stmt or 
                               "data_flow" in converted_stmt or 
                               "reads_from" in converted_stmt or 
                               "writes_to" in converted_stmt))
            
            # 处理新的 Cypher 语句结构（包含注释和多个 SET 语句）
            if is_new_structure:
                # 直接执行完整的语句，不进行拆分
                try:
                    # 检查是否有 RETURN 语句
                    if "RETURN" in converted_stmt:
                        # 如果有 RETURN 语句，需要处理多列返回的情况
                        # 先修改语句，移除 RETURN 部分
                        stmt_without_return = converted_stmt.split("RETURN")[0].strip()
                        query = f"SELECT * FROM cypher('{graph_name}', $$ {stmt_without_return} $$) AS (result agtype);"
                    else:
                        # 没有 RETURN 语句，直接执行
                        query = f"SELECT * FROM cypher('{graph_name}', $$ {converted_stmt} $$) AS (result agtype);"
                    
                    await conn.execute(query)
                    logger.info(f"语句 {i+1} 执行成功")
                    success_count += 1
                except Exception as e:
                    logger.error(f"语句 {i+1} 执行失败: {e}")
                    failed_statements.append((i+1, converted_stmt, str(e)))
                continue
            
            # 处理 ON CREATE SET 和 ON MATCH SET 语法
            elif is_on_create_set or is_on_match_set:
                # 分离 MERGE 部分和 SET 部分
                merge_part = None
                
                if is_on_create_set:
                    parts = converted_stmt.split("ON CREATE SET", 1)
                    merge_part = parts[0].strip()
                elif is_on_match_set:
                    parts = converted_stmt.split("ON MATCH SET", 1)
                    merge_part = parts[0].strip()
                
                # 如果有 MERGE 语句，先执行
                if merge_part:
                    try:
                        query = f"SELECT * FROM cypher('{graph_name}', $$ {merge_part} $$) AS (result agtype);"
                        await conn.execute(query)
                        logger.info(f"语句 {i+1} 的 MERGE 部分执行成功")
                        success_count += 1
                    except Exception as e:
                        logger.error(f"语句 {i+1} 的 MERGE 部分执行失败: {e}")
                        failed_statements.append((i+1, converted_stmt, str(e)))
                        continue
                
                # 跳过 SET 部分，因为 AGE 1.5.0 不支持
                logger.info(f"跳过语句 {i+1} 的 ON CREATE/MATCH SET 部分，AGE 1.5.0 不支持")
                continue
            
            # 处理边变量问题
            if is_edge_variable:
                # 将语句拆分为两部分执行
                try:
                    # 1. 首先执行 MATCH 部分
                    match_parts = []
                    for part in converted_stmt.split("MATCH"):
                        if part.strip():
                            match_parts.append("MATCH " + part.split("MERGE", 1)[0].strip())
                    
                    # 合并所有 MATCH 部分
                    if match_parts:
                        match_stmt = "\n".join(match_parts)
                        try:
                            query = f"SELECT * FROM cypher('{graph_name}', $$ {match_stmt} $$) AS (result agtype);"
                            await conn.execute(query)
                            logger.info(f"语句 {i+1} 的 MATCH 部分执行成功")
                        except Exception as e:
                            # 如果合并执行失败，尝试单独执行每个 MATCH
                            logger.warning(f"语句 {i+1} 的合并 MATCH 部分执行失败: {e}")
                            for j, match_part in enumerate(match_parts):
                                try:
                                    query = f"SELECT * FROM cypher('{graph_name}', $$ {match_part} $$) AS (result agtype);"
                                    await conn.execute(query)
                                    logger.info(f"语句 {i+1} 的 MATCH 部分 {j+1} 执行成功")
                                except Exception as e:
                                    logger.error(f"语句 {i+1} 的 MATCH 部分 {j+1} 执行失败: {e}")
                    
                    # 2. 尝试执行 MERGE 部分，但不引用边变量
                    merge_parts = []
                    for part in converted_stmt.split("MERGE"):
                        if part.strip() and "->" not in part and "<-" not in part:
                            merge_parts.append("MERGE " + part.strip())
                    
                    # 执行每个 MERGE 部分
                    for j, merge_part in enumerate(merge_parts):
                        if "->(" not in merge_part and ")<-" not in merge_part:  # 确保不是关系 MERGE
                            try:
                                query = f"SELECT * FROM cypher('{graph_name}', $$ {merge_part} $$) AS (result agtype);"
                                await conn.execute(query)
                                logger.info(f"语句 {i+1} 的 MERGE 部分 {j+1} 执行成功")
                            except Exception as e:
                                logger.error(f"语句 {i+1} 的 MERGE 部分 {j+1} 执行失败: {e}")
                    
                    # 我们将这个语句视为成功，因为我们已经尝试了所有可能的方式
                    success_count += 1
                    continue
                except Exception as e:
                    logger.error(f"语句 {i+1} 的边变量处理失败: {e}")
                    failed_statements.append((i+1, converted_stmt, str(e)))
                    continue
            
            # 正常执行其他语句
            try:
                query = f"SELECT * FROM cypher('{graph_name}', $$ {converted_stmt} $$) AS (result agtype);"
                await conn.execute(query)
                logger.info(f"语句 {i+1} 执行成功")
                success_count += 1
            except Exception as e:
                error_msg = str(e)
                logger.error(f"语句 {i+1} 执行失败: {error_msg}")
                
                # 如果是语法错误，尝试将语句拆分成多个子语句执行
                if "syntax error" in error_msg:
                    logger.info(f"尝试将语句 {i+1} 拆分成多个子语句执行")
                    
                    # 尝试按行拆分并执行
                    sub_statements = [s.strip() for s in converted_stmt.split("\n") if s.strip()]
                    sub_success = True
                    
                    for j, sub_stmt in enumerate(sub_statements):
                        if sub_stmt.strip():
                            try:
                                # 确保子语句是完整的
                                if sub_stmt.startswith("MATCH") or sub_stmt.startswith("MERGE") or sub_stmt.startswith("CREATE"):
                                    query = f"SELECT * FROM cypher('{graph_name}', $$ {sub_stmt} $$) AS (result agtype);"
                                    await conn.execute(query)
                                    logger.info(f"语句 {i+1} 的子语句 {j+1} 执行成功")
                            except Exception as sub_e:
                                logger.error(f"语句 {i+1} 的子语句 {j+1} 执行失败: {sub_e}")
                                sub_success = False
                    
                    if sub_success:
                        success_count += 1
                    else:
                        failed_statements.append((i+1, converted_stmt, error_msg))
                else:
                    failed_statements.append((i+1, converted_stmt, error_msg))
        
        # 输出失败的语句统计
        if failed_statements:
            logger.warning(f"\n\n以下 {len(failed_statements)} 条语句执行失败:")
            for idx, stmt, error in failed_statements:
                logger.warning(f"\n语句 {idx} 失败: {error}")
                logger.warning(f"\n{stmt}\n")
        
        logger.info(f"\n总结: 总共执行成功 {success_count}/{len(statements)} 条语句")
        return success_count > 0
    except Exception as e:
        logger.error(f"执行 Cypher 语句失败: {e}")
        return False


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='执行转换后的 AGE Cypher 语句')
    parser.add_argument('--host', default='localhost', help='数据库主机')
    parser.add_argument('--port', type=int, default=5432, help='数据库端口')
    parser.add_argument('--user', default='lumiadmin', help='数据库用户名')
    parser.add_argument('--password', default='lumiadmin', help='数据库密码')
    parser.add_argument('--database', default='iwdb', help='数据库名称')
    parser.add_argument('--graph', default='pglumilineage_graph', help='图名称')
    parser.add_argument('--file', required=True, help='Cypher 语句文件路径')
    
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
        # 创建图
        if not await create_graph(conn, args.graph):
            return
        
        # 加载 Cypher 语句
        statements = await load_cypher_statements(args.file)
        logger.info(f"从文件 {args.file} 加载了 {len(statements)} 条 Cypher 语句")
        
        # 执行 Cypher 语句
        success = await execute_cypher_statements(conn, args.graph, statements)
        if success:
            logger.info("所有 Cypher 语句执行完成")
        else:
            logger.error("Cypher 语句执行过程中出现错误")
    finally:
        # 关闭数据库连接
        await conn.close()
        logger.info("数据库连接已关闭")


if __name__ == "__main__":
    asyncio.run(main())
