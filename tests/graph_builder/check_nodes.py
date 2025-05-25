#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查AGE图中节点的实际属性
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from tests.graph_builder.test_settings import get_settings_instance

async def check_nodes():
    settings = get_settings_instance()
    conn = await asyncpg.connect(
        user=settings.INTERNAL_DB.USER,
        password=settings.INTERNAL_DB.PASSWORD.get_secret_value(),
        host=settings.INTERNAL_DB.HOST,
        port=settings.INTERNAL_DB.PORT,
        database=settings.INTERNAL_DB.DB_AGE,
    )
    try:
        await conn.execute('SET search_path = ag_catalog, "$user", public;')
        
        # 检查所有节点的属性
        result = await conn.fetch("""
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (n) 
                RETURN n.source_id as sid, n.database_name as db, n.schema_name as sch, n.object_type as ot, n.column_name as col
                LIMIT 10
            $$) AS (sid agtype, db agtype, sch agtype, ot agtype, col agtype);
        """)
        
        print('前10个节点的属性:')
        for i, row in enumerate(result, 1):
            print(f'{i}. source_id={row["sid"]}, db={row["db"]}, schema={row["sch"]}, type={row["ot"]}, col={row["col"]}')
            
        # 分别检查各种类型节点
        print('\n=== 分类统计 ===')
        
        # 有source_id的节点（数据源）
        ds_result = await conn.fetch("""
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (n) WHERE n.source_id IS NOT NULL
                RETURN n.fqn as fqn, n.source_id as sid
                LIMIT 5
            $$) AS (fqn agtype, sid agtype);
        """)
        print('数据源节点:')
        for row in ds_result:
            print(f'  FQN: {row["fqn"]}, source_id: {row["sid"]}')
            
        # 有column_name的节点（列）
        col_result = await conn.fetch("""
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (n) WHERE n.column_name IS NOT NULL
                RETURN count(n) as cnt
            $$) AS (cnt agtype);
        """)
        print(f'列节点数量: {col_result[0]["cnt"]}')
        
        # 有object_type的节点（表/视图）
        obj_result = await conn.fetch("""
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (n) WHERE n.object_type IS NOT NULL
                RETURN count(n) as cnt
            $$) AS (cnt agtype);
        """)
        print(f'表/视图节点数量: {obj_result[0]["cnt"]}')
        
        # 检查表节点的详细信息
        table_detail = await conn.fetch("""
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (n) WHERE n.object_type IS NOT NULL
                RETURN n.fqn as fqn, n.object_name as name, n.object_type as type
                LIMIT 3
            $$) AS (fqn agtype, name agtype, type agtype);
        """)
        
        print('表节点详情:')
        for row in table_detail:
            print(f'  FQN: {row["fqn"]}, Name: {row["name"]}, Type: {row["type"]}')
        
        # 检查数据库节点
        db_detail = await conn.fetch("""
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (n) WHERE n.database_name IS NOT NULL AND n.source_id IS NULL
                RETURN n.fqn as fqn, n.database_name as db
                LIMIT 3
            $$) AS (fqn agtype, db agtype);
        """)
        
        print(f'数据库节点数量: {len(db_detail)}')
        for row in db_detail:
            print(f'  FQN: {row["fqn"]}, DB: {row["db"]}')
        
        # 检查Schema节点
        schema_detail = await conn.fetch("""
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (n) WHERE n.schema_name IS NOT NULL AND n.database_name IS NULL
                RETURN n.fqn as fqn, n.schema_name as sch
                LIMIT 3
            $$) AS (fqn agtype, sch agtype);
        """)
        
        print(f'Schema节点数量: {len(schema_detail)}')
        for row in schema_detail:
            print(f'  FQN: {row["fqn"]}, Schema: {row["sch"]}')
        
        # 检查是否有列相关的关系
        column_rels = await conn.fetch("""
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH ()-[r]->() WHERE type(r) CONTAINS 'column'
                RETURN type(r) as rel_type, count(r) as cnt
            $$) AS (rel_type agtype, cnt agtype);
        """)
        
        print(f'包含column的关系: {len(column_rels)}')
        for row in column_rels:
            print(f'  关系类型: {row["rel_type"]}, 数量: {row["cnt"]}')
        
        # 再次确认列节点
        all_with_column = await conn.fetch("""
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (n) WHERE n.column_name IS NOT NULL OR n.data_type IS NOT NULL
                RETURN count(n) as cnt
            $$) AS (cnt agtype);
        """)
        print(f'包含列相关属性的节点: {all_with_column[0]["cnt"]}')
            
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(check_nodes()) 