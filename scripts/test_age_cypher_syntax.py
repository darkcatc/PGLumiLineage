#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试 AGE 1.5.0 Cypher 语法

此脚本用于测试 AGE 1.5.0 中的 Cypher 语法，特别是节点和关系的属性和标签语法。

作者: Vance Chen
"""

import os
import sys
import asyncio
import asyncpg
import argparse
from typing import List, Dict, Any

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
        print(f"创建图错误: {e}")
        return False


async def test_node_syntax(conn: asyncpg.Connection, graph_name: str) -> None:
    """
    测试节点语法

    Args:
        conn: 数据库连接
        graph_name: 图名称
    """
    print("\n=== 测试节点语法 ===")
    
    # 测试 1: 创建带标签的节点
    try:
        query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            CREATE (n {{name: 'test_node', label: 'test_node'}})
            RETURN n
        $$) as (n agtype);
        """
        await conn.execute(query)
        print("测试 1 成功: 创建带标签的节点")
    except Exception as e:
        print(f"测试 1 失败: {e}")
    
    # 测试 2: MATCH 带标签的节点
    try:
        query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            MATCH (n {{label: 'test_node'}})
            RETURN n
        $$) as (n agtype);
        """
        result = await conn.fetch(query)
        print(f"测试 2 成功: MATCH 带标签的节点，找到 {len(result)} 个节点")
    except Exception as e:
        print(f"测试 2 失败: {e}")


async def test_relationship_syntax(conn: asyncpg.Connection, graph_name: str) -> None:
    """
    测试关系语法

    Args:
        conn: 数据库连接
        graph_name: 图名称
    """
    print("\n=== 测试关系语法 ===")
    
    # 测试 1: 创建带标签的关系
    try:
        query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            CREATE (a {{name: 'node_a', label: 'node_a'}})-[r:relates_to {{label: 'relates_to'}}]->(b {{name: 'node_b', label: 'node_b'}})
            RETURN a, r, b
        $$) as (a agtype, r agtype, b agtype);
        """
        await conn.execute(query)
        print("测试 1 成功: 创建带标签的关系")
    except Exception as e:
        print(f"测试 1 失败: {e}")
    
    # 测试 2: MATCH 带标签的关系
    try:
        query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            MATCH (a)-[r {{label: 'relates_to'}}]->(b)
            RETURN a, r, b
        $$) as (a agtype, r agtype, b agtype);
        """
        result = await conn.fetch(query)
        print(f"测试 2 成功: MATCH 带标签的关系，找到 {len(result)} 个关系")
    except Exception as e:
        print(f"测试 2 失败: {e}")


async def test_merge_syntax(conn: asyncpg.Connection, graph_name: str) -> None:
    """
    测试 MERGE 语法

    Args:
        conn: 数据库连接
        graph_name: 图名称
    """
    print("\n=== 测试 MERGE 语法 ===")
    
    # 测试 1: MERGE 节点
    try:
        query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            MERGE (n {{name: 'merge_node', label: 'merge_node'}})
            RETURN n
        $$) as (n agtype);
        """
        await conn.execute(query)
        print("测试 1 成功: MERGE 节点")
    except Exception as e:
        print(f"测试 1 失败: {e}")
    
    # 测试 2: MERGE 关系
    try:
        query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            MATCH (a {{label: 'merge_node'}})
            MERGE (b {{name: 'merge_target', label: 'merge_target'}})
            MERGE (a)-[r:merge_rel {{label: 'merge_rel'}}]->(b)
            RETURN a, r, b
        $$) as (a agtype, r agtype, b agtype);
        """
        await conn.execute(query)
        print("测试 2 成功: MERGE 关系")
    except Exception as e:
        print(f"测试 2 失败: {e}")


async def test_where_syntax(conn: asyncpg.Connection, graph_name: str) -> None:
    """
    测试 WHERE 语法

    Args:
        conn: 数据库连接
        graph_name: 图名称
    """
    print("\n=== 测试 WHERE 语法 ===")
    
    # 测试 1: WHERE 标签条件
    try:
        query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            MATCH (n)
            WHERE n.label = 'test_node'
            RETURN n
        $$) as (n agtype);
        """
        result = await conn.fetch(query)
        print(f"测试 1 成功: WHERE 标签条件，找到 {len(result)} 个节点")
    except Exception as e:
        print(f"测试 1 失败: {e}")
    
    # 测试 2: WHERE OR 条件
    try:
        query = f"""
        SELECT * FROM cypher('{graph_name}', $$
            MATCH (n)
            WHERE n.label = 'test_node' OR n.label = 'merge_node'
            RETURN n
        $$) as (n agtype);
        """
        result = await conn.fetch(query)
        print(f"测试 2 成功: WHERE OR 条件，找到 {len(result)} 个节点")
    except Exception as e:
        print(f"测试 2 失败: {e}")


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='测试 AGE 1.5.0 Cypher 语法')
    parser.add_argument('--host', default='localhost', help='数据库主机')
    parser.add_argument('--port', type=int, default=5432, help='数据库端口')
    parser.add_argument('--user', default='lumiadmin', help='数据库用户名')
    parser.add_argument('--password', default='lumiadmin', help='数据库密码')
    parser.add_argument('--database', default='iwdb', help='数据库名称')
    parser.add_argument('--graph', default='test_syntax_graph', help='图名称')
    
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
        
        # 测试节点语法
        await test_node_syntax(conn, args.graph)
        
        # 测试关系语法
        await test_relationship_syntax(conn, args.graph)
        
        # 测试 MERGE 语法
        await test_merge_syntax(conn, args.graph)
        
        # 测试 WHERE 语法
        await test_where_syntax(conn, args.graph)
        
    finally:
        # 关闭数据库连接
        await conn.close()
        print("\n数据库连接已关闭")


if __name__ == "__main__":
    asyncio.run(main())
