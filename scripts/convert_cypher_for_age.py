#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
转换Cypher语句以适应AGE 1.5.0版本

此脚本用于将标准Cypher语句转换为适合AGE 1.5.0版本的格式，
并提供清空图中所有节点和关系的功能。

作者: Vance Chen
修改: Cascade Assistant
"""

import os
import sys
import argparse
import re
import asyncio
import asyncpg
from typing import List, Dict, Any, Optional

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


async def clear_graph(host: str, port: int, user: str, password: str, database: str, graph_name: str) -> bool:
    """
    清空图中的所有节点和关系

    Args:
        host: 数据库主机
        port: 数据库端口
        user: 数据库用户名
        password: 数据库密码
        database: 数据库名称
        graph_name: 图名称

    Returns:
        bool: 是否成功清空图
    """
    try:
        # 连接数据库
        conn = await asyncpg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        print(f"成功连接到数据库 {database}")
        
        # 设置搜索路径
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        
        # 执行清空图的Cypher语句
        clear_query = f"SELECT * FROM cypher('{graph_name}', $$ MATCH (n) DETACH DELETE n $$) as (result agtype)"
        await conn.execute(clear_query)
        print(f"已清空图 {graph_name} 中的所有节点和关系")
        
        # 关闭数据库连接
        await conn.close()
        print("数据库连接已关闭")
        
        return True
    except Exception as e:
        print(f"清空图时出错: {e}")
        return False


def load_cypher_statements(file_path: str) -> List[str]:
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


def convert_cypher_for_age(statements: List[str]) -> List[str]:
    """
    转换Cypher语句以适应AGE 1.5.0版本

    Args:
        statements: 原始Cypher语句列表

    Returns:
        List[str]: 转换后的Cypher语句列表
    """
    result_statements = []
    
    for i, stmt in enumerate(statements):
        # 先处理节点标签
        # 例如：(db:Database {name: 'tpcds'}) -> (db {name: 'tpcds', label: 'Database'})
        processed_stmt = process_node_labels(stmt)
        
        # 检查是否包含关系 MERGE
        if "MERGE" in processed_stmt and "-[" in processed_stmt and "]->" in processed_stmt:
            # 如果是 MERGE (a)-[r:TYPE]->(b) 形式，则拆分为多个语句
            if re.search(r'MERGE\s+\([^)]+\)\s*-\[[^\]]+\]->\s*\([^)]+\)', processed_stmt):
                # 拆分关系 MERGE
                split_stmts = split_relationship_merge(processed_stmt)
                result_statements.extend(split_stmts)
            else:
                # 其他关系相关语句
                result_statements.append(processed_stmt)
        else:
            # 普通语句
            result_statements.append(processed_stmt)
    
    return result_statements


def process_node_labels(stmt: str) -> str:
    """
    处理节点标签

    Args:
        stmt: Cypher语句

    Returns:
        str: 处理后的Cypher语句
    """
    # 替换节点标签语法，带属性的情况
    # 例如：(db:Database {name: 'tpcds'}) -> (db {name: 'tpcds', label: 'Database'})
    stmt = re.sub(r'\((\w+):(\w+)\s+({[^}]*})\)', r'(\1 \3, label: "\2")', stmt)
    
    # 替换节点标签语法，不带属性的情况
    # 例如：(db:Database) -> (db {label: 'Database'})
    stmt = re.sub(r'\((\w+):(\w+)\)', r'(\1 {label: "\2"})', stmt)
    
    # 替换WHERE条件中的标签语法
    # 例如：WHERE (obj:Table OR obj:View) -> WHERE (obj.label = "Table" OR obj.label = "View")
    stmt = re.sub(r'WHERE\s+\((\w+):(\w+)\s+OR\s+\w+:(\w+)\)', r'WHERE (\1.label = "\2" OR \1.label = "\3")', stmt)
    
    # 替换单个标签的WHERE条件
    # 例如：WHERE n:Person -> WHERE n.label = "Person"
    stmt = re.sub(r'WHERE\s+(\w+):(\w+)', r'WHERE \1.label = "\2"', stmt)
    
    # 替换datetime()函数
    stmt = stmt.replace('datetime()', 'current_timestamp')
    
    # 处理ON CREATE SET和ON MATCH SET语法
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
    
    return stmt


def split_relationship_merge(stmt: str) -> List[str]:
    """
    拆分关系MERGE语句为多个语句

    Args:
        stmt: 包含关系MERGE的Cypher语句

    Returns:
        List[str]: 拆分后的多个Cypher语句
    """
    # 示例输入：MERGE (a:TypeA {prop: 'value'})-[r:REL_TYPE]->(b:TypeB)
    # 示例输出：
    # 1. MERGE (a {prop: 'value', label: 'TypeA'})
    # 2. MERGE (b {label: 'TypeB'})
    # 3. MATCH (a {label: 'TypeA', prop: 'value'})
    #    MATCH (b {label: 'TypeB'})
    #    MERGE (a)-[r:REL_TYPE]->(b)
    
    # 提取节点和关系信息
    match = re.search(r'MERGE\s+\(([^)]+)\)\s*-\[([^\]]+)\]->\s*\(([^)]+)\)', stmt)
    if not match:
        return [stmt]  # 如果没有匹配到预期的模式，则返回原始语句
    
    source_node_str = match.group(1)
    relationship_str = match.group(2)
    target_node_str = match.group(3)
    
    # 处理源节点
    source_var_match = re.search(r'^(\w+)', source_node_str)
    source_var = source_var_match.group(1) if source_var_match else "source"
    
    # 处理目标节点
    target_var_match = re.search(r'^(\w+)', target_node_str)
    target_var = target_var_match.group(1) if target_var_match else "target"
    
    # 处理关系
    rel_type_match = re.search(r':(\w+)', relationship_str)
    rel_type = rel_type_match.group(1) if rel_type_match else "RELATED_TO"
    
    rel_var_match = re.search(r'^(\w+)', relationship_str)
    rel_var = rel_var_match.group(1) if rel_var_match else "r"
    
    # 处理节点标签和属性
    processed_source_node = process_node_labels(f"({source_node_str})")
    processed_target_node = process_node_labels(f"({target_node_str})")
    
    # 提取处理后的节点内容（去掉括号）
    processed_source_content = processed_source_node[1:-1]
    processed_target_content = processed_target_node[1:-1]
    
    # 构建MERGE语句
    source_merge = f"MERGE ({processed_source_content})"
    target_merge = f"MERGE ({processed_target_content})"
    
    # 构建MATCH和MERGE关系语句
    # 从处理后的节点内容中提取标签
    source_label_match = re.search(r'label:\s*"(\w+)"', processed_source_content)
    source_label = source_label_match.group(1) if source_label_match else "Node"
    
    target_label_match = re.search(r'label:\s*"(\w+)"', processed_target_content)
    target_label = target_label_match.group(1) if target_label_match else "Node"
    
    relation_merge = f"MATCH ({source_var} {{label: \"{source_label}\"}})"
    relation_merge += f"\nMATCH ({target_var} {{label: \"{target_label}\"}})"
    relation_merge += f"\nMERGE ({source_var})-[:{rel_type}]->({target_var})"
    
    # 如果原始语句中有其他部分（如WHERE子句），则保留
    remaining = ""
    after_merge_match = re.search(r'\)\s*(.+)$', stmt)
    if after_merge_match and not after_merge_match.group(1).strip().startswith('-['):
        remaining = after_merge_match.group(1)
        relation_merge += " " + remaining
    
    return [source_merge, target_merge, relation_merge]


def save_converted_statements(statements: List[str], output_file: str) -> None:
    """
    保存转换后的Cypher语句到文件

    Args:
        statements: 转换后的Cypher语句列表
        output_file: 输出文件路径
    """
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# 转换后的Cypher语句 (适用于AGE 1.5.0)\n\n")
        
        for i, stmt in enumerate(statements):
            f.write(f"-- 语句 {i+1}\n\n")
            f.write(f"{stmt}\n\n")


async def main_async():
    """异步主函数"""
    parser = argparse.ArgumentParser(description='转换Cypher语句以适应AGE 1.5.0版本并清空图')
    parser.add_argument('--input', help='输入Cypher语句文件路径')
    parser.add_argument('--output', help='输出转换后的Cypher语句文件路径')
    parser.add_argument('--clear-graph', action='store_true', help='清空图中的所有节点和关系')
    parser.add_argument('--host', default='localhost', help='数据库主机')
    parser.add_argument('--port', type=int, default=5432, help='数据库端口')
    parser.add_argument('--user', default='postgres', help='数据库用户名')
    parser.add_argument('--password', default='postgres', help='数据库密码')
    parser.add_argument('--database', default='postgres', help='数据库名称')
    parser.add_argument('--graph', default='cypher_graph', help='图名称')
    
    args = parser.parse_args()
    
    # 如果需要清空图
    if args.clear_graph:
        success = await clear_graph(
            host=args.host,
            port=args.port,
            user=args.user,
            password=args.password,
            database=args.database,
            graph_name=args.graph
        )
        if not success:
            print("清空图失败")
            return
    
    # 如果需要转换Cypher语句
    if args.input and args.output:
        # 加载Cypher语句
        statements = load_cypher_statements(args.input)
        print(f"从文件 {args.input} 加载了 {len(statements)} 条Cypher语句")
        
        # 转换Cypher语句
        converted_statements = convert_cypher_for_age(statements)
        print(f"转换了 {len(converted_statements)} 条Cypher语句")
        
        # 保存转换后的Cypher语句
        save_converted_statements(converted_statements, args.output)
        print(f"转换后的Cypher语句已保存到 {args.output}")
    elif not args.clear_graph:
        print("需要指定 --input 和 --output 参数来转换Cypher语句，或者指定 --clear-graph 参数来清空图")


def main():
    """主函数"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
