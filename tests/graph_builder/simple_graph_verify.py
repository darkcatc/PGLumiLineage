#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
简化的图谱准确性验证脚本

验证AGE图谱中表与字段关系的准确性
"""

import asyncio
import asyncpg
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

try:
    from tests.graph_builder.test_settings import get_settings_instance
    settings = get_settings_instance()
except ImportError:
    print("无法导入测试设置，使用默认配置")
    settings = None

async def verify_graph_accuracy():
    """验证图谱准确性"""
    try:
        if settings:
            # 使用配置文件的数据库设置
            age_db_config = {
                'user': settings.INTERNAL_DB.USER,
                'password': settings.INTERNAL_DB.PASSWORD.get_secret_value(),
                'host': settings.INTERNAL_DB.HOST,
                'port': settings.INTERNAL_DB.PORT,
                'database': settings.INTERNAL_DB.DB_AGE,
            }
            
            metadata_db_config = {
                'user': settings.INTERNAL_DB.USER,
                'password': settings.INTERNAL_DB.PASSWORD.get_secret_value(),
                'host': settings.INTERNAL_DB.HOST,
                'port': settings.INTERNAL_DB.PORT,
                'database': settings.INTERNAL_DB.DB_RAW_LOGS,
            }
        else:
            # 使用硬编码配置
            age_db_config = {
                'user': 'lumiadmin',
                'password': 'password',  # 需要实际密码
                'host': 'localhost',
                'port': 5432,
                'database': 'iwdb',
            }
            metadata_db_config = age_db_config
        
        print("=== AGE图谱准确性验证 ===\n")
        
        # 1. 验证图是否存在
        print("1. 检查AGE图状态...")
        age_conn = await asyncpg.connect(**age_db_config)
        try:
            await age_conn.execute("SET search_path = ag_catalog, \"$user\", public;")
            
            graphs = await age_conn.fetch("SELECT name FROM ag_graph WHERE name = 'lumi_graph';")
            if graphs:
                print("✅ AGE图 'lumi_graph' 存在")
            else:
                print("❌ AGE图 'lumi_graph' 不存在")
                return
            
            # 2. 统计图谱节点数量
            print("\n2. 统计图谱节点...")
            
            # 数据源节点
            ds_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (ds:DataSource) 
                RETURN count(ds) as count
            $$) AS (count agtype);
            """
            ds_result = await age_conn.fetch(ds_query)
            ds_count = ds_result[0]['count'] if ds_result else 0
            print(f"  数据源节点: {ds_count}")
            
            # 数据库节点
            db_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (db:Database) 
                RETURN count(db) as count
            $$) AS (count agtype);
            """
            db_result = await age_conn.fetch(db_query)
            db_count = db_result[0]['count'] if db_result else 0
            print(f"  数据库节点: {db_count}")
            
            # Schema节点
            schema_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (s:Schema) 
                RETURN count(s) as count
            $$) AS (count agtype);
            """
            schema_result = await age_conn.fetch(schema_query)
            schema_count = schema_result[0]['count'] if schema_result else 0
            print(f"  Schema节点: {schema_count}")
            
            # 表节点（所有对象类型）
            table_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (t)
                WHERE t.object_type IS NOT NULL
                RETURN count(t) as count
            $$) AS (count agtype);
            """
            table_result = await age_conn.fetch(table_query)
            table_count = table_result[0]['count'] if table_result else 0
            print(f"  表/视图节点: {table_count}")
            
            # 列节点
            col_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (c:Column) 
                RETURN count(c) as count
            $$) AS (count agtype);
            """
            col_result = await age_conn.fetch(col_query)
            col_count = col_result[0]['count'] if col_result else 0
            print(f"  列节点: {col_count}")
            
            # 3. 验证表-列关系
            print("\n3. 验证表-列关系...")
            
            table_col_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (obj)-[:HAS_COLUMN]->(col:Column)
                RETURN obj.name as table_name, count(col) as column_count
                ORDER BY column_count DESC
                LIMIT 10
            $$) AS (table_name agtype, column_count agtype);
            """
            
            table_col_results = await age_conn.fetch(table_col_query)
            print("  表与列的关系（前10个表）:")
            for result in table_col_results:
                table_name = str(result['table_name']).strip('"')
                col_count = result['column_count']
                print(f"    {table_name}: {col_count} 列")
            
            # 4. 验证层次结构关系
            print("\n4. 验证层次结构关系...")
            
            hierarchy_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (ds:DataSource)-[:CONFIGURES_DATABASE]->(db:Database)-[:HAS_SCHEMA]->(s:Schema)-[:HAS_OBJECT]->(obj)
                RETURN ds.name as ds_name, db.name as db_name, s.name as schema_name, obj.name as obj_name
                LIMIT 5
            $$) AS (ds_name agtype, db_name agtype, schema_name agtype, obj_name agtype);
            """
            
            hierarchy_results = await age_conn.fetch(hierarchy_query)
            print("  完整层次结构（前5个对象）:")
            for result in hierarchy_results:
                ds_name = str(result['ds_name']).strip('"')
                db_name = str(result['db_name']).strip('"')
                schema_name = str(result['schema_name']).strip('"')
                obj_name = str(result['obj_name']).strip('"')
                print(f"    {ds_name} → {db_name} → {schema_name} → {obj_name}")
            
            # 5. 检查FQN格式
            print("\n5. 检查FQN格式...")
            
            fqn_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (col:Column)
                RETURN col.fqn as fqn
                LIMIT 3
            $$) AS (fqn agtype);
            """
            
            fqn_results = await age_conn.fetch(fqn_query)
            print("  列FQN示例:")
            for result in fqn_results:
                fqn = str(result['fqn']).strip('"')
                print(f"    {fqn}")
                
                # 验证FQN格式 (应该是: source.database.schema.table.column)
                parts = fqn.split('.')
                if len(parts) == 5:
                    print(f"      ✅ FQN格式正确: {len(parts)} 层级")
                else:
                    print(f"      ⚠️ FQN格式异常: {len(parts)} 层级")
            
            # 6. 关系统计
            print("\n6. 关系类型统计...")
            
            rel_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH ()-[r]->()
                RETURN type(r) as rel_type, count(*) as count
            $$) AS (rel_type agtype, count agtype);
            """
            
            rel_results = await age_conn.fetch(rel_query)
            print("  关系类型统计:")
            for result in rel_results:
                rel_type = str(result['rel_type']).strip('"')
                count = result['count']
                print(f"    {rel_type}: {count} 个")
                
        finally:
            await age_conn.close()
        
        print("\n=== 验证完成 ===")
        
    except Exception as e:
        print(f"验证出错: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(verify_graph_accuracy()) 