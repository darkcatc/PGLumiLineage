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
import re
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
        
        # 导入转换函数
        sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
        from pglumilineage.age_graph_builder.service import convert_cypher_for_age
        
        # 执行每条Cypher语句
        success_count = 0
        for i, stmt in enumerate(statements):
            if not stmt.strip():
                continue
                
            print(f"执行语句 {i+1}/{len(statements)}...")
            try:
                # 自定义转换函数，专门处理AGE 1.5.0的语法特性
                def custom_convert_for_age(stmt):
                    # 在AGE 1.5.0中，标签必须作为属性内部的一部分
                    # 正确格式为 (n {name: 'value', label: 'Label'})
                    
                    # 检查并替换保留关键字变量名
                    reserved_keywords = ['table', 'group', 'order', 'limit', 'match', 'where', 'return']
                    for keyword in reserved_keywords:
                        # 替换形如 (table {properties}) 的模式
                        stmt = re.sub(r'\((' + keyword + r')\s+({[^}]+})\)', r'(t_\1 \2)', stmt, flags=re.IGNORECASE)
                        # 替换形如 MATCH (table) 的模式
                        stmt = re.sub(r'MATCH\s+\((' + keyword + r')\)', r'MATCH (t_\1)', stmt, flags=re.IGNORECASE)
                        # 替换形如 (table:Label) 的模式
                        stmt = re.sub(r'\((' + keyword + r'):(\w+)\)', r'(t_\1:\2)', stmt, flags=re.IGNORECASE)
                    
                    # 处理节点属性中的标签语法
                    # 例如：(node {prop: 'value'}, label: "Label")
                    # 转换为：(node {prop: 'value', label: 'Label'})
                    stmt = re.sub(r'({[^}]+}),\s*label:\s*"([^"]+)"', r'\1, label: "\2"', stmt)
                    
                    # 处理节点标签语法
                    # 例如：(db:Database {name: 'tpcds'})
                    # 转换为：(db {name: 'tpcds', label: 'Database'})
                    stmt = re.sub(r'(\w+):(\w+)\s+({[^}]+})', r'\1 \3, label: "\2"', stmt)
                    
                    # 处理没有属性的节点标签
                    # 例如：(db:Database)
                    # 转换为：(db {label: 'Database'})
                    stmt = re.sub(r'(\w+):(\w+)(?!\s+{)', r'\1 {label: "\2"}', stmt)
                    
                    # 处理WHERE条件中的标签语法
                    # 例如：WHERE (obj:Table OR obj:View)
                    # 转换为：WHERE (obj.label = 'Table' OR obj.label = 'View')
                    stmt = re.sub(r'WHERE\s+\((\w+):(\w+)\s+OR\s+\w+:(\w+)\)', r'WHERE (\1.label = "\2" OR \1.label = "\3")', stmt)
                    
                    # 处理单个标签的WHERE条件
                    # 例如：WHERE n:Label
                    # 转换为：WHERE n.label = 'Label'
                    stmt = re.sub(r'WHERE\s+(\w+):(\w+)', r'WHERE \1.label = "\2"', stmt)
                    
                    # 处理关系属性中的标签语法
                    # 例如：(a)-[r {prop: 'value'}, label: "TYPE"]->(b)
                    # 转换为：(a)-[r {prop: 'value', label: 'TYPE'}]->(b)
                    stmt = re.sub(r'(\[\w+\s+{[^}]+}),\s*label:\s*"([^"]+)"\]', r'\1, label: "\2"}]', stmt)
                    
                    # 处理关系类型语法
                    # 例如：-[:REL_TYPE]->
                    # 转换为：-[:REL_TYPE {label: 'REL_TYPE'}]->
                    # 注意：不使用固定的变量名r，避免变量名冲突
                    stmt = re.sub(r'-\[:(\w+)\]->', r'-[:\1 {label: "\1"}]->', stmt)
                    
                    # 处理带变量名的关系类型语法
                    # 例如：-[r:REL_TYPE]->
                    # 转换为：-[r:REL_TYPE {label: 'REL_TYPE'}]->
                    stmt = re.sub(r'-\[(\w+):(\w+)\]->', r'-[\1:\2 {label: "\2"}]->', stmt)
                    
                    # 处理没有指定标签的关系
                    # 例如：(a)-[r]->(b)
                    # 转换为：(a)-[r {label: 'RELATED_TO'}]->(b)
                    stmt = re.sub(r'(\([^)]+\))-\[([^:{\]]+)\]->', r'\1-[\2 {label: "RELATED_TO"}]->', stmt)
                    
                    # 修复MERGE语句中的节点语法
                    # 例如：MERGE (node {prop: 'value'}, label: "Label")
                    # 转换为：MERGE (node {prop: 'value', label: 'Label'})
                    stmt = re.sub(r'MERGE\s+\((\w+)\s+({[^}]+}),\s*label:\s*"([^"]+)"\)', r'MERGE (\1 \2, label: "\3")', stmt)
                    
                    # 修复MATCH语句中的节点语法
                    # 例如：MATCH (node {prop: 'value'}, label: "Label")
                    # 转换为：MATCH (node {prop: 'value', label: 'Label'})
                    stmt = re.sub(r'MATCH\s+\((\w+)\s+({[^}]+}),\s*label:\s*"([^"]+)"\)', r'MATCH (\1 \2, label: "\3")', stmt)
                    
                    # 替换ON CREATE SET和ON MATCH SET语法
                    # AGE 1.5.0可能不支持这些语法
                    if "ON CREATE SET" in stmt:
                        # 提取ON CREATE SET部分的属性设置
                        create_set_match = re.search(r'ON\s+CREATE\s+SET\s+([^O]+?)(?:ON\s+MATCH\s+SET|$)', stmt, re.DOTALL)
                        if create_set_match:
                            create_set = create_set_match.group(1).strip()
                            # 移除ON CREATE SET部分
                            stmt = re.sub(r'ON\s+CREATE\s+SET\s+([^O]+?)(?:ON\s+MATCH\s+SET|$)', '', stmt, flags=re.DOTALL)
                            # 添加普通的SET语句
                            if not stmt.strip().endswith(';'):
                                stmt = stmt.strip() + " SET " + create_set
                    
                    # 移除ON MATCH SET部分
                    stmt = re.sub(r'ON\s+MATCH\s+SET\s+.+', '', stmt, flags=re.DOTALL)
                    
                    # 替换datetime()函数
                    stmt = stmt.replace('datetime()', 'current_timestamp')
                    
                    # 修复WHERE条件中的标签比较
                    # 例如：WHERE (obj {label: "Table"} OR obj {label: "View"})
                    # 转换为：WHERE (obj.label = 'Table' OR obj.label = 'View')
                    stmt = re.sub(r'WHERE\s+\((\w+)\s+{label:\s*"(\w+)"}\s+OR\s+\1\s+{label:\s*"(\w+)"}\)', r'WHERE (\1.label = "\2" OR \1.label = "\3")', stmt)
                    
                    return stmt
                
                # 使用自定义转换函数
                converted_stmt = custom_convert_for_age(stmt)
                
                # 使用AGE的cypher函数执行Cypher语句
                # 注意：AGE 1.5.0需要正确的列名定义
                # 分析RETURN子句，确定正确的列名
                return_match = re.search(r'RETURN\s+(.+?)(?:;|$)', converted_stmt, re.IGNORECASE | re.DOTALL)
                
                if return_match:
                    return_clause = return_match.group(1).strip()
                    # 检查是否有多列返回（逗号分隔）
                    if ',' in return_clause:
                        columns = []
                        for col in return_clause.split(','):
                            col = col.strip()
                            as_match = re.search(r'\s+AS\s+(\w+)', col, re.IGNORECASE)
                            if as_match:
                                columns.append(f"{as_match.group(1)} agtype")
                            else:
                                columns.append(f"col{len(columns)+1} agtype")
                        as_clause = ', '.join(columns)
                    else:
                        as_match = re.search(r'\s+AS\s+(\w+)', return_clause, re.IGNORECASE)
                        if as_match:
                            as_clause = f"{as_match.group(1)} agtype"
                        else:
                            as_clause = "result agtype"
                    
                    query = f"SELECT * FROM cypher('{graph_name}', $$ {converted_stmt} $$) AS ({as_clause});"
                else:
                    query = f"SELECT * FROM cypher('{graph_name}', $$ {converted_stmt} $$) AS (result agtype);"
                
                await conn.execute(query)
                print(f"语句 {i+1} 执行成功")
                success_count += 1
            except Exception as e:
                print(f"语句 {i+1} 执行失败: {e}")
                print(f"失败的语句: {converted_stmt}")
                # 继续执行下一条语句
                continue
        
        print(f"总共执行成功 {success_count}/{len(statements)} 条语句")
        return success_count > 0
    except Exception as e:
        print(f"执行Cypher语句失败: {e}")
        return False


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='执行AGE Cypher语句')
    parser.add_argument('--host', default='localhost', help='数据库主机')
    parser.add_argument('--port', type=int, default=5432, help='数据库端口')
    parser.add_argument('--user', default='lumiadmin', help='数据库用户名')
    parser.add_argument('--password', default='lumiadmin', help='数据库密码')
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
