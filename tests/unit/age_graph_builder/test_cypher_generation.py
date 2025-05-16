#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AGE图谱构建器Cypher生成测试

此模块专门测试AGE图谱构建器的Cypher语句生成功能，并将生成的语句保存到文件中，
便于后续的人工审查和验证。

作者: Vance Chen
"""

import os
import json
import pytest
from typing import Dict, Any, List

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pglumilineage.age_graph_builder import service as age_builder_service


def test_cypher_generation_and_save(test_pattern):
    """测试Cypher语句生成并保存到文件"""
    # 生成Cypher语句
    cypher_statements = age_builder_service.transform_json_to_cypher(test_pattern)
    
    # 基本验证
    assert isinstance(cypher_statements, list), "返回值应该是列表"
    assert len(cypher_statements) > 0, "应该生成至少一条Cypher语句"
    
    # 创建输出目录
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        'data', 'cypher'
    )
    os.makedirs(output_dir, exist_ok=True)
    
    # 保存Cypher语句到文件
    output_file = os.path.join(output_dir, f"{test_pattern.sql_hash}_cypher.txt")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(f"# Cypher语句生成结果 - SQL哈希: {test_pattern.sql_hash}\n\n")
        f.write(f"## 源数据库: {test_pattern.source_database_name}\n\n")
        f.write(f"## 生成的Cypher语句 (共 {len(cypher_statements)} 条):\n\n")
        
        for i, stmt in enumerate(cypher_statements):
            if isinstance(stmt, dict):
                f.write(f"### 语句 {i+1} (带参数)\n\n")
                f.write(f"```cypher\n{stmt['query']}\n```\n\n")
                f.write(f"参数:\n```json\n{json.dumps(stmt['params'], ensure_ascii=False, indent=2)}\n```\n\n")
            else:
                f.write(f"### 语句 {i+1}\n\n")
                f.write(f"```cypher\n{stmt}\n```\n\n")
    
    # 验证文件是否创建
    assert os.path.exists(output_file), f"应该创建输出文件: {output_file}"
    
    print(f"\nCypher语句已保存到: {output_file}")
    return output_file


def test_cypher_node_creation(test_pattern):
    """测试节点创建的Cypher语句"""
    # 生成Cypher语句
    cypher_statements = age_builder_service.transform_json_to_cypher(test_pattern)
    
    # 验证数据库节点创建语句
    db_statements = [stmt for stmt in cypher_statements 
                    if isinstance(stmt, str) and "MERGE (db:Database" in stmt]
    assert len(db_statements) > 0, "应该包含创建数据库节点的Cypher语句"
    
    # 验证Schema节点创建语句
    schema_statements = [stmt for stmt in cypher_statements 
                         if isinstance(stmt, str) and "MERGE (schema:Schema" in stmt]
    assert len(schema_statements) > 0, "应该包含创建Schema节点的Cypher语句"
    
    # 验证表/视图节点创建语句
    table_statements = [stmt for stmt in cypher_statements 
                       if isinstance(stmt, str) and ("MERGE (table:" in stmt or "MERGE (view:" in stmt)]
    assert len(table_statements) > 0, "应该包含创建表/视图节点的Cypher语句"
    
    # 验证列节点创建语句
    column_statements = [stmt for stmt in cypher_statements 
                        if isinstance(stmt, str) and ("MERGE (tgt_col:Column" in stmt or "MERGE (src_col:Column" in stmt)]
    assert len(column_statements) > 0, "应该包含创建列节点的Cypher语句"
    
    # 验证SQL模式节点创建语句
    sql_pattern_statements = [stmt for stmt in cypher_statements 
                             if isinstance(stmt, str) and "MERGE (sp:SqlPattern" in stmt]
    assert len(sql_pattern_statements) > 0, "应该包含创建SQL模式节点的Cypher语句"


def test_cypher_relationship_creation(test_pattern):
    """测试关系创建的Cypher语句"""
    # 生成Cypher语句
    cypher_statements = age_builder_service.transform_json_to_cypher(test_pattern)
    
    # 验证HAS_SCHEMA关系创建语句
    has_schema_statements = [stmt for stmt in cypher_statements 
                            if isinstance(stmt, str) and "MERGE (db)-[:HAS_SCHEMA]->(schema)" in stmt]
    assert len(has_schema_statements) > 0, "应该包含创建HAS_SCHEMA关系的Cypher语句"
    
    # 验证HAS_OBJECT关系创建语句
    has_object_statements = [stmt for stmt in cypher_statements 
                            if isinstance(stmt, str) and "MERGE (schema)-[:HAS_OBJECT]->" in stmt]
    assert len(has_object_statements) > 0, "应该包含创建HAS_OBJECT关系的Cypher语句"
    
    # 验证HAS_COLUMN关系创建语句
    has_column_statements = [stmt for stmt in cypher_statements 
                            if isinstance(stmt, str) and ("MERGE (tgt_obj)-[:HAS_COLUMN]->(tgt_col)" in stmt or 
                                                      "MERGE (src_obj)-[:HAS_COLUMN]->(src_col)" in stmt)]
    assert len(has_column_statements) > 0, "应该包含创建HAS_COLUMN关系的Cypher语句"
    
    # 验证DATA_FLOW关系创建语句
    data_flow_statements = [stmt for stmt in cypher_statements 
                           if (isinstance(stmt, dict) and "DATA_FLOW" in stmt["query"]) or
                              (isinstance(stmt, str) and "DATA_FLOW" in stmt)]
    assert len(data_flow_statements) > 0, "应该包含创建DATA_FLOW关系的Cypher语句"
    
    # 验证READS_FROM/WRITES_TO关系创建语句
    reference_statements = [stmt for stmt in cypher_statements 
                           if isinstance(stmt, str) and 
                           ("MERGE (sp)-[:READS_FROM]->(obj)" in stmt or 
                            "MERGE (sp)-[:WRITES_TO]->(obj)" in stmt)]
    assert len(reference_statements) > 0, "应该包含创建READS_FROM/WRITES_TO关系的Cypher语句"


def test_special_cases(special_pattern):
    """测试特殊情况处理"""
    # 生成Cypher语句
    cypher_statements = age_builder_service.transform_json_to_cypher(special_pattern)
    
    # 验证字面量处理
    literal_handling = False
    for stmt in cypher_statements:
        if isinstance(stmt, dict) and "DATA_FLOW" in stmt["query"] and "special_column" in stmt["query"]:
            literal_handling = True
            break
        elif isinstance(stmt, str) and "DATA_FLOW" in stmt and "special_column" in stmt:
            literal_handling = True
            break
    
    assert literal_handling, "应该包含处理字面量的Cypher语句"
    
    # 保存特殊情况的Cypher语句
    output_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        'data', 'cypher'
    )
    os.makedirs(output_dir, exist_ok=True)
    
    output_file = os.path.join(output_dir, "special_cases_cypher.txt")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("# 特殊情况Cypher语句生成结果\n\n")
        f.write(f"## 生成的Cypher语句 (共 {len(cypher_statements)} 条):\n\n")
        
        for i, stmt in enumerate(cypher_statements):
            if isinstance(stmt, dict):
                f.write(f"### 语句 {i+1} (带参数)\n\n")
                f.write(f"```cypher\n{stmt['query']}\n```\n\n")
                f.write(f"参数:\n```json\n{json.dumps(stmt['params'], ensure_ascii=False, indent=2)}\n```\n\n")
            else:
                f.write(f"### 语句 {i+1}\n\n")
                f.write(f"```cypher\n{stmt}\n```\n\n")
    
    print(f"\n特殊情况Cypher语句已保存到: {output_file}")


def test_empty_relations(empty_pattern):
    """测试空关系处理"""
    # 生成Cypher语句
    cypher_statements = age_builder_service.transform_json_to_cypher(empty_pattern)
    
    # 验证结果
    assert isinstance(cypher_statements, list), "返回值应该是列表"
    assert len(cypher_statements) == 0, "空关系应该返回空列表"


if __name__ == "__main__":
    # 直接运行此文件时，执行测试并保存结果
    import sys
    sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))
    
    # 创建测试数据
    from conftest import test_pattern, special_pattern, empty_pattern
    
    # 执行测试
    output_file = test_cypher_generation_and_save(test_pattern())
    test_special_cases(special_pattern())
    
    print(f"\n测试完成，Cypher语句已保存到data/cypher目录")
