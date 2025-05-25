#!/usr/bin/env python
# -*- coding: utf-8 -*-

import asyncio
import asyncpg
import json

async def check_graph_nodes():
    conn = await asyncpg.connect(
        user='lumiadmin',
        password='lumiadmin',
        host='127.0.0.1',
        port=5432,
        database='iwdb'
    )
    
    # 设置搜索路径
    await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
    
    graph_name = 'lumi_graph'
    
    # 查询表节点
    query = f"""
    SELECT * FROM cypher('{graph_name}', $$ 
        MATCH (n) 
        WHERE n.label = 'table' 
        RETURN n LIMIT 5
    $$) as (n agtype);
    """
    
    try:
        result = await conn.fetch(query)
        print('表节点:')
        for row in result:
            node = row['n']
            print(json.dumps(node, indent=2))
    except Exception as e:
        print(f"查询表节点时发生错误: {e}")
    
    # 查询视图节点
    query = f"""
    SELECT * FROM cypher('{graph_name}', $$ 
        MATCH (n) 
        WHERE n.label = 'view' 
        RETURN n LIMIT 5
    $$) as (n agtype);
    """
    
    try:
        result = await conn.fetch(query)
        print('\n视图节点:')
        for row in result:
            node = row['n']
            print(json.dumps(node, indent=2))
    except Exception as e:
        print(f"查询视图节点时发生错误: {e}")
    
    # 查询monthly_channel_returns_analysis_report表
    query = f"""
    SELECT * FROM cypher('{graph_name}', $$ 
        MATCH (n) 
        WHERE n.label = 'table' AND n.name = 'monthly_channel_returns_analysis_report' 
        RETURN n
    $$) as (n agtype);
    """
    
    try:
        result = await conn.fetch(query)
        print('\nmonthly_channel_returns_analysis_report表:')
        for row in result:
            node = row['n']
            print(json.dumps(node, indent=2))
    except Exception as e:
        print(f"查询monthly_channel_returns_analysis_report表时发生错误: {e}")
    
    await conn.close()

asyncio.run(check_graph_nodes())
