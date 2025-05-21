#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
为 AGE 1.5.0 创建基本图结构

此脚本专门用于在 AGE 1.5.0 中创建基本的图结构，采用最简单的方法，
确保与 AGE 1.5.0 的语法兼容。

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


async def create_basic_structure(conn: asyncpg.Connection, graph_name: str) -> bool:
    """
    创建基本的图结构

    Args:
        conn: 数据库连接
        graph_name: 图名称

    Returns:
        bool: 是否成功创建基本结构
    """
    try:
        # 设置搜索路径
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        
        # 创建数据库节点
        try:
            db_query = f"""
            SELECT * FROM cypher('{graph_name}', $$
                CREATE (db {{name: 'tpcds', label: 'database'}})
                RETURN db
            $$) as (db agtype);
            """
            await conn.execute(db_query)
            print("创建数据库节点成功")
        except Exception as e:
            if "already exists" in str(e):
                print("数据库节点已存在，继续执行")
            else:
                print(f"创建数据库节点失败: {e}")
        
        # 创建模式节点
        try:
            # 先创建模式节点
            schema_create_query = f"""
            SELECT * FROM cypher('{graph_name}', $$
                CREATE (schema {{name: 'public', database_name: 'tpcds', label: 'schema'}})
                RETURN schema
            $$) as (schema agtype);
            """
            await conn.execute(schema_create_query)
            print("创建模式节点成功")
            
            # 然后创建数据库和模式之间的关系
            schema_relation_query = f"""
            SELECT * FROM cypher('{graph_name}', $$
                MATCH (db {{label: 'database', name: 'tpcds'}})
                MATCH (schema {{label: 'schema', name: 'public', database_name: 'tpcds'}})
                CREATE (db)-[:has_schema {{label: 'has_schema'}}]->(schema)
                RETURN 'has_schema' as relation
            $$) as (relation agtype);
            """
            await conn.execute(schema_relation_query)
            print("创建数据库和模式之间的关系成功")
        except Exception as e:
            if "already exists" in str(e):
                print("模式节点已存在，继续执行")
            else:
                print(f"创建模式节点失败: {e}")
        
        # 创建表节点
        tables = [
            'monthly_channel_returns_analysis_report',
            'store_sales',
            'catalog_sales',
            'web_sales',
            'store_returns',
            'catalog_returns',
            'web_returns',
            'date_dim',
            'reason'
        ]
        
        for table in tables:
            try:
                # 先创建表节点
                table_create_query = f"""
                SELECT * FROM cypher('{graph_name}', $$
                    CREATE (t {{name: '{table}', schema_name: 'public', database_name: 'tpcds', object_type: 'table', label: 'table'}})
                    RETURN t as table_node
                $$) as (table_node agtype);
                """
                await conn.execute(table_create_query)
                print(f"创建表节点 {table} 成功")
                
                # 然后创建模式和表之间的关系
                table_relation_query = f"""
                SELECT * FROM cypher('{graph_name}', $$
                    MATCH (schema {{label: 'schema', name: 'public', database_name: 'tpcds'}})
                    MATCH (t {{label: 'table', name: '{table}', schema_name: 'public', database_name: 'tpcds'}})
                    CREATE (schema)-[:has_object {{label: 'has_object'}}]->(t)
                    RETURN 'has_object' as relation
                $$) as (relation agtype);
                """
                await conn.execute(table_relation_query)
                print(f"创建模式和表 {table} 之间的关系成功")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"表节点 {table} 已存在，继续执行")
                else:
                    print(f"创建表节点 {table} 失败: {e}")
        
        # 创建SQL模式节点
        try:
            sql_pattern_query = f"""
            SELECT * FROM cypher('{graph_name}', $$
                CREATE (sp {{sql_hash: '8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8', label: 'sql_pattern'}})
                RETURN sp
            $$) as (sp agtype);
            """
            await conn.execute(sql_pattern_query)
            print("创建SQL模式节点成功")
        except Exception as e:
            if "already exists" in str(e):
                print("SQL模式节点已存在，继续执行")
            else:
                print(f"创建SQL模式节点失败: {e}")
        
        # 创建SQL模式与表的关系
        for table in tables:
            try:
                relation_type = 'READS_FROM'
                if table == 'monthly_channel_returns_analysis_report':
                    relation_type = 'WRITES_TO'
                    
                relation_query = f"""
                SELECT * FROM cypher('{graph_name}', $$
                    MATCH (sp {{label: 'sql_pattern', sql_hash: '8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8'}})
                    MATCH (t {{label: 'table', name: '{table}', schema_name: 'public', database_name: 'tpcds'}})
                    CREATE (sp)-[:{relation_type.lower()} {{label: '{relation_type.lower()}'}}]->(t)
                    RETURN '{relation_type.lower()}' as relation_type
                $$) as (relation_type agtype);
                """
                await conn.execute(relation_query)
                print(f"创建SQL模式与表 {table} 的 {relation_type} 关系成功")
            except Exception as e:
                if "already exists" in str(e):
                    print(f"SQL模式与表 {table} 的关系已存在，继续执行")
                else:
                    print(f"创建SQL模式与表 {table} 的关系失败: {e}")
        
        return True
    except Exception as e:
        print(f"创建基本结构失败: {e}")
        return False


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='为 AGE 1.5.0 创建基本图结构')
    parser.add_argument('--host', default='localhost', help='数据库主机')
    parser.add_argument('--port', type=int, default=5432, help='数据库端口')
    parser.add_argument('--user', default='lumiadmin', help='数据库用户名')
    parser.add_argument('--password', default='lumiadmin', help='数据库密码')
    parser.add_argument('--database', default='iwdb', help='数据库名称')
    parser.add_argument('--graph', default='pglumilineage_graph', help='图名称')
    
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
        
        # 创建基本结构
        if await create_basic_structure(conn, args.graph):
            print("成功创建基本图结构")
        else:
            print("创建基本图结构失败")
    finally:
        # 关闭数据库连接
        await conn.close()
        print("数据库连接已关闭")


if __name__ == "__main__":
    asyncio.run(main())
