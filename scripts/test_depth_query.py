#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试不同深度的血缘关系查询

作者: Vance Chen
"""

import asyncio
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pglumilineage.api.lineage.repository import LineageRepository
from pglumilineage.api.lineage.models import NodeType

async def test_depth_queries():
    """测试不同深度的查询"""
    
    repo = LineageRepository()
    table_name = "monthly_channel_returns_analysis_report"
    
    print("=" * 80)
    print("测试不同深度的血缘关系查询")
    print("=" * 80)
    
    for depth in [1, 2, 3]:
        print(f"\n{'='*20} 深度 {depth} {'='*20}")
        
        try:
            result = await repo.query_subgraph(
                NodeType.TABLE, 
                table_name, 
                depth
            )
            
            nodes_count = len(result.get('nodes', []))
            edges_count = len(result.get('relationships', []))
            
            print(f"节点数量: {nodes_count}")
            print(f"关系数量: {edges_count}")
            
            # 分析节点类型
            node_types = {}
            for node in result.get('nodes', []):
                node_label = node.get('label', 'unknown')
                node_types[node_label] = node_types.get(node_label, 0) + 1
            
            print("节点类型分布:")
            for node_type, count in node_types.items():
                print(f"  {node_type}: {count}")
            
            # 分析关系类型
            rel_types = {}
            for rel in result.get('relationships', []):
                rel_label = rel.get('label', 'unknown')
                rel_types[rel_label] = rel_types.get(rel_label, 0) + 1
            
            print("关系类型分布:")
            for rel_type, count in rel_types.items():
                print(f"  {rel_type}: {count}")
            
            if depth == 2:
                # 深度2时，检查是否有数据流关系
                data_flow_count = rel_types.get('data_flow', 0)
                print(f"\n🔍 数据流关系数量: {data_flow_count}")
                if data_flow_count == 0:
                    print("⚠️ 注意：深度2应该显示列级血缘关系，但没有找到data_flow关系")
            
        except Exception as e:
            print(f"❌ 查询失败: {e}")

if __name__ == "__main__":
    asyncio.run(test_depth_queries()) 