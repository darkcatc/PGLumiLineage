#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
从 JSON 文件生成 Cypher 语句脚本

此脚本用于从 JSON 文件生成 Cypher 语句，并将其保存到文件中。

作者: Vance Chen
"""

import os
import sys
import json
import argparse
from datetime import datetime
from typing import Dict, Any

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pglumilineage.graph_builder.service import transform_json_to_cypher
from pglumilineage.common.models import AnalyticalSQLPattern

def load_json_file(file_path: str) -> Dict[str, Any]:
    """
    加载 JSON 文件
    
    Args:
        file_path: JSON 文件路径
        
    Returns:
        Dict[str, Any]: JSON 内容
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        return json.load(f)

def main():
    parser = argparse.ArgumentParser(description='从 JSON 文件生成 Cypher 语句')
    parser.add_argument('--json-file', required=True, help='JSON 文件路径')
    parser.add_argument('--output-file', required=True, help='输出 Cypher 文件路径')
    
    args = parser.parse_args()
    
    # 加载 JSON 文件
    json_data = load_json_file(args.json_file)
    
    # 确保数据库名称正确设置
    database_name = json_data.get('source_database_name', 'tpcds')
    if not database_name or database_name == 'unknown_db':
        database_name = 'tpcds'  # 使用默认值
    
    # 将数据库名称更新到 JSON 中
    json_data['source_database_name'] = database_name
    
    # 创建 AnalyticalSQLPattern 对象
    pattern = AnalyticalSQLPattern(
        sql_hash=json_data.get('sql_pattern_hash', ''),
        normalized_sql_text=json_data.get('normalized_sql', '') or 'SELECT 1',  # 提供默认值避免验证错误
        sample_raw_sql_text=json_data.get('sample_sql', '') or 'SELECT 1',  # 提供默认值避免验证错误
        llm_extracted_relations_json=json_data,  # 直接传入 JSON 对象
        source_database_name=database_name,  # 设置正确的数据库名称
        first_seen_at=datetime.now(),  # 添加必要的字段
        last_seen_at=datetime.now()  # 添加必要的字段
    )
    
    # 生成 Cypher 语句
    cypher_statements = transform_json_to_cypher(pattern)
    
    # 将 Cypher 语句写入文件
    with open(args.output_file, 'w', encoding='utf-8') as f:
        for i, stmt in enumerate(cypher_statements):
            if isinstance(stmt, dict):
                # 处理带参数的 Cypher 语句
                f.write(f"-- 语句 {i+1}\n\n{stmt['query']}\n\n")
            else:
                # 处理普通 Cypher 语句
                f.write(f"-- 语句 {i+1}\n\n{stmt}\n\n")
    
    print(f"已生成 Cypher 语句并保存到 {args.output_file}")

if __name__ == '__main__':
    main()
