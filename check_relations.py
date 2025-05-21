#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import asyncpg
import json

async def check_graph_relations():
    conn = await asyncpg.connect(
        user='lumiadmin',
        password='lumiadmin',
        host='127.0.0.1',
        port=5432,
        database='iwdb'
    )
    
    # 设置搜索路径
    await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
    
    graph_name = 'pglumilineage_graph'
    
    # 查询monthly_channel_returns_analysis_report表的关系
    query = f"""
    SELECT * FROM cypher('{graph_name}', $$ 
        MATCH (n)-[r]-(m)
        WHERE n.label = 'table' AND n.name = 'monthly_channel_returns_analysis_report' 
        RETURN r, m LIMIT 10
    $$) as (r agtype, m agtype);
    """
    
    try:
        result = await conn.fetch(query)
        print('monthly_channel_returns_analysis_report表的关系:')
        for row in result:
            relation = row['r']
            related_node = row['m']
            print("关系:", json.dumps(relation, indent=2))
            print("相关节点:", json.dumps(related_node, indent=2))
            print("---")
    except Exception as e:
        print(f"查询关系时发生错误: {e}")
    
    await conn.close()

asyncio.run(check_graph_relations())
