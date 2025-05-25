#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查边关系的属性信息

作者: Vance Chen
"""

import asyncio
import asyncpg
import json
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def parse_age_data(raw_data):
    """解析AGE返回的数据"""
    if isinstance(raw_data, str):
        if "::vertex" in raw_data:
            raw_data = raw_data.replace("::vertex", "")
        elif "::edge" in raw_data:
            raw_data = raw_data.replace("::edge", "")
    return json.loads(raw_data)

async def check_edge_properties():
    """检查边关系的属性信息"""
    
    # 数据库配置
    db_config = {
        "user": "lumiadmin",
        "password": "lumiadmin", 
        "host": "localhost",
        "port": 5432,
        "database": "iwdb"
    }
    
    graph_name = "lumi_graph"
    
    try:
        conn = await asyncpg.connect(**db_config)
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        
        print("=" * 80)
        print("检查边关系的属性信息")
        print("=" * 80)
        
        # 查询所有边关系类型
        edge_types_query = f"""
        SELECT DISTINCT * FROM cypher('{graph_name}', $$ 
            MATCH ()-[r]->()
            RETURN DISTINCT labels(r)[0] as edge_type, count(*) as count
        $$) as (edge_type agtype, count agtype);
        """
        
        print("\n边关系类型统计:")
        edge_types_result = await conn.fetch(edge_types_query)
        for row in edge_types_result:
            edge_type = row['edge_type'].replace('"', '')
            count = json.loads(row['count'])
            print(f"  {edge_type}: {count} 条")
        
        # 检查数据流关系的详细属性
        print("\n" + "=" * 50)
        print("数据流关系详细信息")
        print("=" * 50)
        
        data_flow_query = f"""
        SELECT * FROM cypher('{graph_name}', $$ 
            MATCH (source)-[r:data_flow]->(target)
            RETURN source, r, target
            LIMIT 5
        $$) as (source agtype, r agtype, target agtype);
        """
        
        data_flow_result = await conn.fetch(data_flow_query)
        
        for i, row in enumerate(data_flow_result):
            print(f"\n第 {i+1} 条数据流关系:")
            
            source_data = parse_age_data(row['source'])
            rel_data = parse_age_data(row['r'])
            target_data = parse_age_data(row['target'])
            
            print(f"  源列: {source_data.get('properties', {}).get('name', 'N/A')}")
            print(f"  目标列: {target_data.get('properties', {}).get('name', 'N/A')}")
            print(f"  关系ID: {rel_data.get('id')}")
            print(f"  关系属性: {rel_data.get('properties', {})}")
        
        # 检查其他关系类型的属性
        print("\n" + "=" * 50)
        print("其他关系类型属性示例")
        print("=" * 50)
        
        for edge_type in ['writes_to', 'has_column', 'has_object']:
            other_query = f"""
            SELECT * FROM cypher('{graph_name}', $$ 
                MATCH ()-[r:{edge_type}]->()
                RETURN r
                LIMIT 2
            $$) as (r agtype);
            """
            
            try:
                other_result = await conn.fetch(other_query)
                if other_result:
                    print(f"\n{edge_type} 关系属性:")
                    for j, row in enumerate(other_result):
                        rel_data = parse_age_data(row['r'])
                        print(f"  示例 {j+1}: {rel_data.get('properties', {})}")
            except Exception as e:
                print(f"  查询 {edge_type} 失败: {e}")
        
        await conn.close()
        
    except Exception as e:
        logger.error(f"检查边属性时发生错误: {e}")

if __name__ == "__main__":
    asyncio.run(check_edge_properties()) 