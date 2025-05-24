#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
调试确切的Cypher语句错误
"""

import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

print("开始调试脚本...")

try:
    from pglumilineage.graph_builder.common_graph_utils import convert_cypher_for_age
    print("导入成功")
    
    # 实际错误中的确切Cypher语句
    error_cypher = """MERGE (obj:TABLE {fqn: 'tpcds.tpcds.public.catalog_returns'})
ON CREATE SET
obj.name = 'catalog_returns',
obj.schema_fqn = 'tpcds.tpcds.public',
obj.object_type = 'TABLE',
obj.owner = 'postgres',
obj.description = 'Fact table for catalog returns.',
obj.definition_sql = null,
obj.properties = null,
obj.row_count = 144067,
obj.size_bytes = null,
obj.last_analyzed = null,
obj.created_at = datetime(),
obj.updated_at = datetime()
ON MATCH SET
obj.name = 'catalog_returns',
obj.owner = 'postgres',
obj.description = 'Fact table for catalog returns.',
obj.definition_sql = null,
obj.properties = null,
obj.row_count = 144067,
obj.size_bytes = null,
obj.last_analyzed = null,
obj.updated_at = datetime()
WITH obj
MATCH (schema:Schema {fqn: 'tpcds.tpcds.public'})
MERGE (schema)-[r:HAS_OBJECT]->(obj)
ON CREATE SET
r.created_at = datetime(),
r.updated_at = datetime()
ON MATCH SET
r.updated_at = datetime()
RETURN obj"""

    print("实际错误的Cypher:")
    print(error_cypher)
    print("\n" + "="*50 + "\n")

    print("开始转换...")
    converted = convert_cypher_for_age(error_cypher)
    print("转换完成")
    
    print("转换后:")
    print(converted)
    print("\n" + "="*20 + "\n")

    if 'ON CREATE SET' in converted or 'ON MATCH SET' in converted:
        print("❌ 转换失败")
        # 找出哪些行没有转换
        lines = converted.split('\n')
        for i, line in enumerate(lines):
            if 'ON CREATE SET' in line or 'ON MATCH SET' in line:
                print(f"第{i+1}行未转换: {line}")
    else:
        print("✅ 转换成功")
        
except Exception as e:
    print(f"发生错误: {type(e).__name__}: {str(e)}")
    import traceback
    traceback.print_exc() 