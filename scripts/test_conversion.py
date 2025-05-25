#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import os
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from pglumilineage.graph_builder.common_graph_utils import convert_cypher_for_age

def test_conversion():
    """测试Cypher语句转换"""
    
    # 测试用例：包含ON CREATE SET和ON MATCH SET的MERGE语句
    test_cypher = """
                MATCH (sp:sqlpattern {sql_hash: $sql_hash})
                MATCH (obj {fqn: $object_fqn})
                MERGE (sp)-[r:reads_from]->(obj)
                ON CREATE SET
                    r.created_at = datetime(),
                    r.last_seen_at = $last_seen_at
                ON MATCH SET
                    r.last_seen_at = $last_seen_at
                RETURN r
                """
    
    print("=== 原始Cypher语句 ===")
    print(test_cypher)
    
    print("\n=== 转换后的Cypher语句 ===")
    converted = convert_cypher_for_age(test_cypher)
    print(converted)
    
    # 验证转换结果
    print("\n=== 转换验证 ===")
    if "ON CREATE SET" not in converted and "ON MATCH SET" not in converted:
        print("✅ 成功移除ON CREATE SET和ON MATCH SET语法")
    else:
        print("❌ 未能移除ON CREATE SET/ON MATCH SET语法")
    
    if "SET" in converted:
        print("✅ 成功添加SET语句")
    else:
        print("❌ 未能添加SET语句")
    
    if "COALESCE" in converted:
        print("✅ 成功添加COALESCE处理created_at")
    else:
        print("❌ 未能添加COALESCE处理")

if __name__ == "__main__":
    test_conversion() 