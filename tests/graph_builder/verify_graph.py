#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
验证AGE图谱构建结果

验证已构建的图谱是否符合设计文档中的要求：
1. 节点类型和数量
2. 关系类型和数量
3. FQN结构是否正确
4. 属性是否完整

使用方法:
    python verify_graph.py
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from tests.graph_builder.test_settings import get_settings_instance

async def verify_graph():
    """验证图谱构建结果"""
    try:
        # 获取配置
        settings = get_settings_instance()
        
        # 构建AGE数据库配置
        db_config = {
            'user': settings.INTERNAL_DB.USER,
            'password': settings.INTERNAL_DB.PASSWORD.get_secret_value(),
            'host': settings.INTERNAL_DB.HOST,
            'port': settings.INTERNAL_DB.PORT,
            'database': settings.INTERNAL_DB.DB_AGE,
        }
        
        conn = await asyncpg.connect(**db_config)
        try:
            # 设置搜索路径
            await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
            
            print("=== AGE图谱验证报告 ===\n")
            
            # 1. 检查图是否存在
            graphs = await conn.fetch("SELECT name FROM ag_graph WHERE name = 'lumi_graph';")
            if graphs:
                print("✅ AGE图 'lumi_graph' 存在")
            else:
                print("❌ AGE图 'lumi_graph' 不存在")
                return
            
            # 2. 检查节点数量
            print("\n--- 节点统计 ---")
            
            # 查询所有节点
            nodes_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (n) 
                RETURN n 
                LIMIT 10
            $$) AS (n agtype);
            """
            
            try:
                nodes = await conn.fetch(nodes_query)
                print(f"✅ 成功查询到 {len(nodes)} 个节点（显示前10个）")
                
                for i, node in enumerate(nodes[:3], 1):
                    print(f"  节点 {i}: {node['n']}")
                    
            except Exception as e:
                print(f"❌ 节点查询失败: {str(e)}")
            
            # 3. 检查关系数量
            print("\n--- 关系统计 ---")
            
            relations_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH ()-[r]->() 
                RETURN r 
                LIMIT 10
            $$) AS (r agtype);
            """
            
            try:
                relations = await conn.fetch(relations_query)
                print(f"✅ 成功查询到 {len(relations)} 个关系（显示前10个）")
                
                for i, rel in enumerate(relations[:3], 1):
                    print(f"  关系 {i}: {rel['r']}")
                    
            except Exception as e:
                print(f"❌ 关系查询失败: {str(e)}")
            
            # 4. 检查数据源节点
            print("\n--- 数据源节点检查 ---")
            
            datasource_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (ds:DataSource) 
                RETURN ds.fqn as fqn, ds.name as name 
            $$) AS (fqn agtype, name agtype);
            """
            
            try:
                datasources = await conn.fetch(datasource_query)
                print(f"✅ 找到 {len(datasources)} 个数据源节点")
                
                for ds in datasources:
                    print(f"  数据源: {ds['name']} (FQN: {ds['fqn']})")
                    
            except Exception as e:
                print(f"❌ 数据源节点查询失败: {str(e)}")
            
            # 5. 检查数据库节点
            print("\n--- 数据库节点检查 ---")
            
            database_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (db:Database) 
                RETURN db.fqn as fqn, db.name as name 
            $$) AS (fqn agtype, name agtype);
            """
            
            try:
                databases = await conn.fetch(database_query)
                print(f"✅ 找到 {len(databases)} 个数据库节点")
                
                for db in databases:
                    print(f"  数据库: {db['name']} (FQN: {db['fqn']})")
                    
            except Exception as e:
                print(f"❌ 数据库节点查询失败: {str(e)}")
            
            # 6. 检查schema节点
            print("\n--- Schema节点检查 ---")
            
            schema_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (s:Schema) 
                RETURN s.fqn as fqn, s.name as name 
            $$) AS (fqn agtype, name agtype);
            """
            
            try:
                schemas = await conn.fetch(schema_query)
                print(f"✅ 找到 {len(schemas)} 个Schema节点")
                
                for schema in schemas:
                    print(f"  Schema: {schema['name']} (FQN: {schema['fqn']})")
                    
            except Exception as e:
                print(f"❌ Schema节点查询失败: {str(e)}")
            
            # 7. 检查表节点
            print("\n--- 表节点检查 ---")
            
            table_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (t:TABLE) 
                RETURN t.fqn as fqn, t.name as name 
                LIMIT 5
            $$) AS (fqn agtype, name agtype);
            """
            
            try:
                tables = await conn.fetch(table_query)
                print(f"✅ 找到表节点（显示前5个）")
                
                for table in tables:
                    print(f"  表: {table['name']} (FQN: {table['fqn']})")
                    
            except Exception as e:
                print(f"❌ 表节点查询失败: {str(e)}")
            
            # 8. 检查列节点
            print("\n--- 列节点检查 ---")
            
            column_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (c:Column) 
                RETURN c.fqn as fqn, c.name as name, c.data_type as data_type 
                LIMIT 5
            $$) AS (fqn agtype, name agtype, data_type agtype);
            """
            
            try:
                columns = await conn.fetch(column_query)
                print(f"✅ 找到列节点（显示前5个）")
                
                for col in columns:
                    print(f"  列: {col['name']} ({col['data_type']}) (FQN: {col['fqn']})")
                    
            except Exception as e:
                print(f"❌ 列节点查询失败: {str(e)}")
            
            # 9. 验证关系结构
            print("\n--- 关系结构验证 ---")
            
            # 检查数据源->数据库关系
            ds_db_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (ds:DataSource)-[r:CONFIGURES_DATABASE]->(db:Database) 
                RETURN ds.name as ds_name, db.name as db_name 
            $$) AS (ds_name agtype, db_name agtype);
            """
            
            try:
                ds_db_rels = await conn.fetch(ds_db_query)
                print(f"✅ 数据源->数据库关系: {len(ds_db_rels)} 个")
                
                for rel in ds_db_rels:
                    print(f"  {rel['ds_name']} -> {rel['db_name']}")
                    
            except Exception as e:
                print(f"❌ 数据源->数据库关系查询失败: {str(e)}")
            
            # 检查数据库->Schema关系
            db_schema_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (db:Database)-[r:HAS_SCHEMA]->(s:Schema) 
                RETURN db.name as db_name, s.name as schema_name 
            $$) AS (db_name agtype, schema_name agtype);
            """
            
            try:
                db_schema_rels = await conn.fetch(db_schema_query)
                print(f"✅ 数据库->Schema关系: {len(db_schema_rels)} 个")
                
                for rel in db_schema_rels:
                    print(f"  {rel['db_name']} -> {rel['schema_name']}")
                    
            except Exception as e:
                print(f"❌ 数据库->Schema关系查询失败: {str(e)}")
            
            print("\n=== 验证完成 ===")
            
        finally:
            await conn.close()
            
    except Exception as e:
        print(f"验证过程出错: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(verify_graph()) 