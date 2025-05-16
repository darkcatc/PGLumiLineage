#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
执行AGE Cypher语句脚本

此脚本用于在Apache AGE图数据库中创建图并执行生成的Cypher语句。

作者: Vance Chen
"""

import os
import sys
import asyncio
import asyncpg
import argparse
from datetime import datetime
from typing import List, Dict, Any, Optional

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


async def create_graph(conn: asyncpg.Connection, graph_name: str) -> bool:
    """
    在AGE中创建图

    Args:
        conn: 数据库连接
        graph_name: 图名称

    Returns:
        bool: 是否成功创建图
    """
    try:
        # 检查AGE版本
        version_result = await conn.fetchrow("SELECT extversion FROM pg_extension WHERE extname = 'age'")
        if not version_result:
            print("AGE扩展未安装")
            return False
            
        age_version = version_result['extversion']
        print(f"AGE版本: {age_version}")
        
        # 设置搜索路径
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        print("已设置搜索路径")
        
        # 创建图
        try:
            await conn.execute(f"SELECT * FROM create_graph('{graph_name}')")
            print(f"成功创建图 {graph_name}")
            return True
        except Exception as e:
            if "already exists" in str(e):
                print(f"图 {graph_name} 已存在，继续执行")
                return True
            else:
                print(f"创建图失败: {e}")
                return False
    except Exception as e:
        print(f"检查AGE版本失败: {e}")
        return False


async def load_cypher_statements(file_path: str) -> List[str]:
    """
    从文件加载Cypher语句

    Args:
        file_path: Cypher语句文件路径

    Returns:
        List[str]: Cypher语句列表
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
    执行Cypher语句

    Args:
        conn: 数据库连接
        graph_name: 图名称
        statements: Cypher语句列表

    Returns:
        bool: 是否成功执行所有语句
    """
    try:
        # 检查AGE版本
        version_result = await conn.fetchrow("SELECT extversion FROM pg_extension WHERE extname = 'age'")
        if not version_result:
            print("AGE扩展未安装")
            return False
            
        age_version = version_result['extversion']
        print(f"AGE版本: {age_version}")
        
        # 设置搜索路径
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        print("已设置搜索路径")
        
        # 执行每条Cypher语句
        for i, stmt in enumerate(statements):
            if not stmt.strip():
                continue
                
            print(f"执行语句 {i+1}/{len(statements)}...")
            try:
                # 使用AGE的cypher函数执行Cypher语句
                query = f"SELECT * FROM cypher('{graph_name}', $$ {stmt} $$) as (n agtype)"
                await conn.execute(query)
                print(f"语句 {i+1} 执行成功")
            except Exception as e:
                print(f"语句 {i+1} 执行失败: {e}")
                print(f"失败的语句: {stmt}")
                # 继续执行下一条语句
                continue
        
        return True
    except Exception as e:
        print(f"执行Cypher语句失败: {e}")
        return False


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='执行AGE Cypher语句')
    parser.add_argument('--host', default='localhost', help='数据库主机')
    parser.add_argument('--port', type=int, default=5432, help='数据库端口')
    parser.add_argument('--user', default='postgres', help='数据库用户名')
    parser.add_argument('--password', default='postgres', help='数据库密码')
    parser.add_argument('--database', default='iwdb', help='数据库名称')
    parser.add_argument('--graph', default='pglumilineage_graph', help='图名称')
    parser.add_argument('--file', required=True, help='Cypher语句文件路径')
    
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
        print(f"成功连接到数据库 {args.database}")
    except Exception as e:
        print(f"连接数据库失败: {e}")
        return
    
    try:
        # 创建图
        if not await create_graph(conn, args.graph):
            return
        
        # 加载Cypher语句
        statements = await load_cypher_statements(args.file)
        print(f"从文件 {args.file} 加载了 {len(statements)} 条Cypher语句")
        
        # 执行Cypher语句
        success = await execute_cypher_statements(conn, args.graph, statements)
        if success:
            print("所有Cypher语句执行完成")
        else:
            print("Cypher语句执行过程中出现错误")
    finally:
        # 关闭数据库连接
        await conn.close()
        print("数据库连接已关闭")


if __name__ == "__main__":
    asyncio.run(main())
