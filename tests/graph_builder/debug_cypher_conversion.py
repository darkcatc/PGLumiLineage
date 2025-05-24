#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
调试Cypher转换函数

测试ON CREATE SET和ON MATCH SET语法的转换
"""

import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from pglumilineage.graph_builder.common_graph_utils import convert_cypher_for_age

def test_conversion():
    """测试Cypher转换"""
    
    # 测试1: 简单的数据源节点Cypher
    print("=== 测试1: 简单的数据源节点 ===")
    test_cypher1 = """
        MERGE (ds:DataSource {fqn: $fqn})
        ON CREATE SET 
            ds.name = $name,
            ds.source_id = $source_id,
            ds.created_at = datetime(),
            ds.updated_at = datetime()
        ON MATCH SET
            ds.name = $name,
            ds.updated_at = datetime()
        RETURN ds
        """
    
    print("原始:")
    print(test_cypher1)
    print("转换后:")
    converted1 = convert_cypher_for_age(test_cypher1)
    print(converted1)
    print("转换是否成功:", '✅' if 'ON CREATE SET' not in converted1 and 'ON MATCH SET' not in converted1 else '❌')
    print()
    
    # 测试2: 复杂的对象节点Cypher（包含WITH和多个MERGE）
    print("=== 测试2: 复杂的对象节点（包含WITH） ===")
    test_cypher2 = """
        MERGE (obj:TABLE {fqn: $fqn})
        ON CREATE SET
            obj.name = $name,
            obj.schema_fqn = $schema_fqn,
            obj.object_type = $object_type,
            obj.created_at = datetime(),
            obj.updated_at = datetime()
        ON MATCH SET
            obj.name = $name,
            obj.object_type = $object_type,
            obj.updated_at = datetime()
        WITH obj
        MATCH (schema:Schema {fqn: $schema_fqn})
        MERGE (schema)-[r:HAS_OBJECT]->(obj)
        ON CREATE SET
            r.created_at = datetime(),
            r.updated_at = datetime()
        ON MATCH SET
            r.updated_at = datetime()
        RETURN obj
        """
    
    print("原始:")
    print(test_cypher2)
    print("转换后:")
    converted2 = convert_cypher_for_age(test_cypher2)
    print(converted2)
    print("转换是否成功:", '✅' if 'ON CREATE SET' not in converted2 and 'ON MATCH SET' not in converted2 else '❌')

if __name__ == "__main__":
    test_conversion() 