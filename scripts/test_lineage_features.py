#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
全面测试血缘关系功能

包括：
1. 后端查询逻辑测试
2. 不同深度的查询测试
3. 边属性检查
4. 节点类型验证

作者: Vance Chen
"""

import asyncio
import sys
import os
import json

# 添加项目根目录到路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from pglumilineage.api.lineage.repository import LineageRepository
from pglumilineage.api.lineage.models import NodeType

async def test_lineage_features():
    """全面测试血缘关系功能"""
    
    repo = LineageRepository()
    table_name = "monthly_channel_returns_analysis_report"
    
    print("=" * 80)
    print("PGLumiLineage 血缘关系功能全面测试")
    print("=" * 80)
    
    # 测试1：基础连接测试
    print("\n🔧 测试1: 基础连接测试")
    try:
        result = await repo.query_subgraph(NodeType.TABLE, table_name, 1)
        nodes_count = len(result.get('nodes', []))
        edges_count = len(result.get('relationships', []))
        print(f"✅ 连接成功 - 节点: {nodes_count}, 边: {edges_count}")
    except Exception as e:
        print(f"❌ 连接失败: {e}")
        return
    
    # 测试2：不同深度查询测试
    print("\n🔍 测试2: 不同深度查询测试")
    depth_results = {}
    
    for depth in [1, 2, 3]:
        try:
            result = await repo.query_subgraph(NodeType.TABLE, table_name, depth)
            
            nodes_count = len(result.get('nodes', []))
            edges_count = len(result.get('relationships', []))
            
            # 分析节点类型分布
            node_types = {}
            for node in result.get('nodes', []):
                node_label = node.get('label', 'unknown')
                node_types[node_label] = node_types.get(node_label, 0) + 1
            
            # 分析关系类型分布
            rel_types = {}
            for rel in result.get('relationships', []):
                rel_label = rel.get('label', 'unknown')
                rel_types[rel_label] = rel_types.get(rel_label, 0) + 1
            
            depth_results[depth] = {
                'nodes': nodes_count,
                'edges': edges_count,
                'node_types': node_types,
                'rel_types': rel_types
            }
            
            print(f"  深度 {depth}: 节点={nodes_count}, 边={edges_count}")
            print(f"    节点类型: {', '.join([f'{k}({v})' for k, v in node_types.items()])}")
            print(f"    关系类型: {', '.join([f'{k}({v})' for k, v in rel_types.items()])}")
            
        except Exception as e:
            print(f"  ❌ 深度 {depth} 查询失败: {e}")
    
    # 测试3：节点类型验证
    print("\n🏷️ 测试3: 节点类型验证")
    if 1 in depth_results:
        result = await repo.query_subgraph(NodeType.TABLE, table_name, 1)
        
        expected_types = {
            'table': '表（目标表）',
            'schema': '模式',
            'column': '列',
            'sqlpattern': 'SQL模式',
            'database': '数据库'
        }
        
        found_types = set()
        for node in result.get('nodes', []):
            node_label = node.get('label', '')
            found_types.add(node_label)
            
            # 检查节点属性
            properties = node.get('properties', {})
            if node_label == 'table':
                print(f"  ✅ 表节点: {properties.get('name', 'N/A')}")
                if 'fqn' in properties:
                    print(f"     FQN: {properties['fqn']}")
            elif node_label == 'schema':
                print(f"  ✅ 模式节点: {properties.get('name', 'N/A')}")
            elif node_label == 'sqlpattern':
                print(f"  ✅ SQL模式节点: ID={node.get('id')}")
                
        for expected_type, description in expected_types.items():
            if expected_type in found_types:
                print(f"  ✅ {description}")
            else:
                print(f"  ⚠️ 缺失 {description}")
    
    # 测试4：数据流关系验证
    print("\n🌊 测试4: 数据流关系验证")
    if 2 in depth_results:
        result = await repo.query_subgraph(NodeType.TABLE, table_name, 2)
        
        data_flow_count = 0
        for rel in result.get('relationships', []):
            if rel.get('label') == 'data_flow':
                data_flow_count += 1
                properties = rel.get('properties', {})
                print(f"  📊 数据流关系: {rel.get('start_id')} -> {rel.get('end_id')}")
                if properties:
                    print(f"     属性: {properties}")
        
        if data_flow_count > 0:
            print(f"  ✅ 找到 {data_flow_count} 条数据流关系")
        else:
            print("  ⚠️ 未找到数据流关系，可能数据不完整或查询有误")
    
    # 测试5：边属性详细检查
    print("\n🔗 测试5: 边属性详细检查")
    if 1 in depth_results:
        result = await repo.query_subgraph(NodeType.TABLE, table_name, 1)
        
        for rel in result.get('relationships', [])[:3]:  # 只检查前3个
            rel_type = rel.get('label', 'unknown')
            properties = rel.get('properties', {})
            
            print(f"  关系: {rel_type}")
            print(f"    ID: {rel.get('id')}")
            print(f"    源: {rel.get('start_id')} -> 目标: {rel.get('end_id')}")
            if properties:
                print(f"    属性: {json.dumps(properties, indent=6, ensure_ascii=False)}")
            else:
                print(f"    属性: 无")
    
    # 测试6：深度递增验证
    print("\n📊 测试6: 深度递增效果验证")
    if len(depth_results) >= 2:
        for depth in sorted(depth_results.keys())[1:]:
            prev_depth = depth - 1
            if prev_depth in depth_results:
                current = depth_results[depth]
                previous = depth_results[prev_depth]
                
                node_increase = current['nodes'] - previous['nodes']
                edge_increase = current['edges'] - previous['edges']
                
                print(f"  深度 {prev_depth} -> {depth}:")
                print(f"    节点增加: +{node_increase}")
                print(f"    边增加: +{edge_increase}")
                
                if node_increase > 0 or edge_increase > 0:
                    print(f"    ✅ 深度递增有效")
                else:
                    print(f"    ⚠️ 深度递增无效果")
    
    print("\n" + "=" * 80)
    print("测试完成！")
    print("=" * 80)

if __name__ == "__main__":
    asyncio.run(test_lineage_features()) 