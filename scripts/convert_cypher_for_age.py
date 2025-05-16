#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
转换Cypher语句以适应AGE 1.5.0版本

此脚本用于将标准Cypher语句转换为适合AGE 1.5.0版本的格式。

作者: Vance Chen
"""

import os
import sys
import argparse
import re
from typing import List, Dict, Any, Optional

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


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
    converted_statements = []
    
    for i, stmt in enumerate(statements):
        converted_stmt = stmt
        
        # 替换标签语法
        # 例如：MERGE (db:Database {name: 'tpcds'})
        # 转换为：MERGE (db {name: 'tpcds', label: 'Database'})
        converted_stmt = re.sub(r'(\w+):(\w+)\s+({[^}]+})', r'\1 \3, label: "\2"', converted_stmt)
        
        # 替换WHERE条件中的标签语法
        # 例如：WHERE (obj:Table OR obj:View)
        # 转换为：WHERE (obj.label = 'Table' OR obj.label = 'View')
        converted_stmt = re.sub(r'WHERE\s+\((\w+):(\w+)\s+OR\s+\w+:(\w+)\)', r'WHERE (\1.label = "\2" OR \1.label = "\3")', converted_stmt)
        
        # 替换关系类型语法
        # 例如：MERGE (a)-[:REL_TYPE]->(b)
        # 转换为：MERGE (a)-[r {label: 'REL_TYPE'}]->(b)
        converted_stmt = re.sub(r'-\[:(\w+)\]->', r'-[r {label: "\1"}]->', converted_stmt)
        
        # 替换ON CREATE SET和ON MATCH SET语法
        # AGE 1.5.0可能不支持这些语法
        if "ON CREATE SET" in converted_stmt:
            # 提取ON CREATE SET部分的属性设置
            create_set_match = re.search(r'ON\s+CREATE\s+SET\s+([^O]+?)(?:ON\s+MATCH\s+SET|$)', converted_stmt, re.DOTALL)
            if create_set_match:
                create_set = create_set_match.group(1).strip()
                # 移除ON CREATE SET部分
                converted_stmt = re.sub(r'ON\s+CREATE\s+SET\s+([^O]+?)(?:ON\s+MATCH\s+SET|$)', '', converted_stmt, flags=re.DOTALL)
                # 添加普通的SET语句
                if not converted_stmt.strip().endswith(';'):
                    converted_stmt = converted_stmt.strip() + " SET " + create_set
        
        # 移除ON MATCH SET部分
        converted_stmt = re.sub(r'ON\s+MATCH\s+SET\s+.+', '', converted_stmt, flags=re.DOTALL)
        
        # 替换datetime()函数
        converted_stmt = converted_stmt.replace('datetime()', 'current_timestamp')
        
        # 添加到转换后的语句列表
        converted_statements.append(converted_stmt)
        
    return converted_statements


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


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='转换Cypher语句以适应AGE 1.5.0版本')
    parser.add_argument('--input', required=True, help='输入Cypher语句文件路径')
    parser.add_argument('--output', required=True, help='输出转换后的Cypher语句文件路径')
    
    args = parser.parse_args()
    
    # 加载Cypher语句
    statements = load_cypher_statements(args.input)
    print(f"从文件 {args.input} 加载了 {len(statements)} 条Cypher语句")
    
    # 转换Cypher语句
    converted_statements = convert_cypher_for_age(statements)
    print(f"转换了 {len(converted_statements)} 条Cypher语句")
    
    # 保存转换后的Cypher语句
    save_converted_statements(converted_statements, args.output)
    print(f"转换后的Cypher语句已保存到 {args.output}")


if __name__ == "__main__":
    main()
