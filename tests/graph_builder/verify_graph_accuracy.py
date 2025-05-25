#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
验证AGE图谱的准确性

该脚本验证构建的图谱中的关系是否与实际数据库结构一致：
1. 验证表与列的对应关系
2. 验证外键关系的准确性
3. 验证FQN层次结构的正确性
4. 对比元数据存储与图谱数据的一致性

使用方法:
    python -m tests.graph_builder.verify_graph_accuracy
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Dict, Any, List, Set

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from tests.graph_builder.test_settings import get_settings_instance
from pglumilineage.graph_builder.metadata_graph_builder import MetadataGraphBuilder
from pglumilineage.graph_builder.common_graph_utils import execute_cypher as common_execute_cypher
import asyncpg

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GraphAccuracyVerifier:
    """图谱准确性验证器"""
    
    def __init__(self):
        self.settings = get_settings_instance()
        
        # 元数据数据库配置
        self.metadata_db_config = {
            'user': self.settings.INTERNAL_DB.USER,
            'password': self.settings.INTERNAL_DB.PASSWORD.get_secret_value(),
            'host': self.settings.INTERNAL_DB.HOST,
            'port': self.settings.INTERNAL_DB.PORT,
            'database': self.settings.INTERNAL_DB.DB_RAW_LOGS,
        }
        
        # AGE数据库配置
        self.age_db_config = {
            'user': self.settings.INTERNAL_DB.USER,
            'password': self.settings.INTERNAL_DB.PASSWORD.get_secret_value(),
            'host': self.settings.INTERNAL_DB.HOST,
            'port': self.settings.INTERNAL_DB.PORT,
            'database': self.settings.INTERNAL_DB.DB_AGE,
        }
        
        self.graph_name = "lumi_graph"
    
    async def get_metadata_tables_and_columns(self) -> Dict[str, List[Dict]]:
        """从元数据存储获取表和列的信息"""
        conn = await asyncpg.connect(**self.metadata_db_config)
        try:
            # 获取表信息
            tables_query = """
            SELECT object_id, source_id, database_name, schema_name, object_name, object_type
            FROM lumi_metadata_store.objects_metadata
            WHERE source_id = 1
            ORDER BY database_name, schema_name, object_name
            """
            tables = await conn.fetch(tables_query)
            
            # 获取列信息
            columns_query = """
            SELECT c.object_id, c.column_name, c.ordinal_position, c.data_type, c.is_nullable,
                   c.is_primary_key, c.is_unique,
                   o.database_name, o.schema_name, o.object_name
            FROM lumi_metadata_store.columns_metadata c
            JOIN lumi_metadata_store.objects_metadata o ON c.object_id = o.object_id
            WHERE o.source_id = 1
            ORDER BY o.database_name, o.schema_name, o.object_name, c.ordinal_position
            """
            columns = await conn.fetch(columns_query)
            
            return {
                'tables': [dict(row) for row in tables],
                'columns': [dict(row) for row in columns]
            }
        finally:
            await conn.close()
    
    async def get_graph_tables_and_columns(self) -> Dict[str, List[Dict]]:
        """从AGE图谱获取表和列的信息"""
        conn = await asyncpg.connect(**self.age_db_config)
        try:
            await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
            
            # 获取表节点
            tables_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (t)
                WHERE t.object_type IS NOT NULL
                RETURN t.fqn as fqn, t.name as name, t.object_type as object_type, t.schema_fqn as schema_fqn
            $$) AS (fqn agtype, name agtype, object_type agtype, schema_fqn agtype);
            """
            
            graph_tables = await conn.fetch(tables_query)
            
            # 获取列节点及其关系
            columns_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (obj)-[:HAS_COLUMN]->(col:Column)
                RETURN obj.fqn as object_fqn, obj.name as object_name,
                       col.fqn as column_fqn, col.name as column_name, 
                       col.ordinal_position as ordinal_position, col.data_type as data_type,
                       col.is_nullable as is_nullable, col.is_primary_key as is_primary_key
            $$) AS (object_fqn agtype, object_name agtype, column_fqn agtype, column_name agtype,
                    ordinal_position agtype, data_type agtype, is_nullable agtype, is_primary_key agtype);
            """
            
            graph_columns = await conn.fetch(columns_query)
            
            return {
                'tables': [dict(row) for row in graph_tables],
                'columns': [dict(row) for row in graph_columns]
            }
        finally:
            await conn.close()
    
    async def verify_table_structure(self):
        """验证表结构的一致性"""
        print("\n=== 表结构一致性验证 ===")
        
        # 获取元数据和图谱数据
        metadata = await self.get_metadata_tables_and_columns()
        graph_data = await self.get_graph_tables_and_columns()
        
        metadata_tables = metadata['tables']
        graph_tables = graph_data['tables']
        
        print(f"元数据存储中的表数量: {len(metadata_tables)}")
        print(f"图谱中的表节点数量: {len(graph_tables)}")
        
        # 创建元数据表的映射 (以object_name为key)
        metadata_table_map = {}
        for table in metadata_tables:
            key = f"{table['database_name']}.{table['schema_name']}.{table['object_name']}"
            metadata_table_map[key] = table
        
        # 验证每个图谱表是否在元数据中存在
        missing_in_metadata = []
        matched_tables = 0
        
        for graph_table in graph_tables:
            # 从FQN中提取表信息
            fqn = str(graph_table['fqn']).strip('"')
            # FQN格式: tpcds.tpcds.public.table_name
            parts = fqn.split('.')
            if len(parts) >= 4:
                source_name, db_name, schema_name, table_name = parts[0], parts[1], parts[2], parts[3]
                metadata_key = f"{db_name}.{schema_name}.{table_name}"
                
                if metadata_key in metadata_table_map:
                    matched_tables += 1
                    # 验证对象类型是否一致
                    metadata_type = metadata_table_map[metadata_key]['object_type']
                    graph_type = str(graph_table['object_type']).strip('"')
                    
                    if metadata_type != graph_type:
                        print(f"  ⚠️  类型不匹配: {table_name} - 元数据:{metadata_type} vs 图谱:{graph_type}")
                else:
                    missing_in_metadata.append(fqn)
        
        print(f"✅ 匹配的表: {matched_tables}/{len(graph_tables)}")
        if missing_in_metadata:
            print(f"❌ 图谱中存在但元数据中缺失的表: {len(missing_in_metadata)}")
            for fqn in missing_in_metadata[:5]:  # 只显示前5个
                print(f"  - {fqn}")
    
    async def verify_column_relationships(self):
        """验证列关系的准确性"""
        print("\n=== 列关系准确性验证 ===")
        
        # 获取元数据和图谱数据
        metadata = await self.get_metadata_tables_and_columns()
        graph_data = await self.get_graph_tables_and_columns()
        
        metadata_columns = metadata['columns']
        graph_columns = graph_data['columns']
        
        print(f"元数据存储中的列数量: {len(metadata_columns)}")
        print(f"图谱中的列节点数量: {len(graph_columns)}")
        
        # 按表分组的元数据列映射
        metadata_columns_by_table = {}
        for col in metadata_columns:
            table_key = f"{col['database_name']}.{col['schema_name']}.{col['object_name']}"
            if table_key not in metadata_columns_by_table:
                metadata_columns_by_table[table_key] = []
            metadata_columns_by_table[table_key].append(col)
        
        # 验证图谱中的列关系
        column_relationship_issues = []
        matched_columns = 0
        
        for graph_col in graph_columns:
            # 从对象FQN中提取表信息
            object_fqn = str(graph_col['object_fqn']).strip('"')
            column_name = str(graph_col['column_name']).strip('"')
            
            # 对象FQN格式: tpcds.tpcds.public.table_name
            parts = object_fqn.split('.')
            if len(parts) >= 4:
                source_name, db_name, schema_name, table_name = parts[0], parts[1], parts[2], parts[3]
                table_key = f"{db_name}.{schema_name}.{table_name}"
                
                if table_key in metadata_columns_by_table:
                    # 查找匹配的列
                    metadata_col = None
                    for meta_col in metadata_columns_by_table[table_key]:
                        if meta_col['column_name'] == column_name:
                            metadata_col = meta_col
                            break
                    
                    if metadata_col:
                        matched_columns += 1
                        # 验证列属性
                        issues = []
                        
                        # 检查数据类型
                        meta_type = metadata_col['data_type']
                        graph_type = str(graph_col['data_type']).strip('"') if graph_col['data_type'] else None
                        if meta_type != graph_type:
                            issues.append(f"数据类型不匹配: {meta_type} vs {graph_type}")
                        
                        # 检查是否可空
                        meta_nullable = metadata_col['is_nullable']
                        graph_nullable = graph_col['is_nullable']
                        if meta_nullable != graph_nullable:
                            issues.append(f"可空性不匹配: {meta_nullable} vs {graph_nullable}")
                        
                        if issues:
                            column_relationship_issues.append({
                                'column': f"{table_key}.{column_name}",
                                'issues': issues
                            })
                    else:
                        column_relationship_issues.append({
                            'column': f"{table_key}.{column_name}",
                            'issues': ['图谱中存在但元数据中不存在']
                        })
        
        print(f"✅ 匹配的列: {matched_columns}/{len(graph_columns)}")
        
        if column_relationship_issues:
            print(f"⚠️  发现 {len(column_relationship_issues)} 个列关系问题（显示前10个）:")
            for issue in column_relationship_issues[:10]:
                print(f"  - {issue['column']}: {', '.join(issue['issues'])}")
    
    async def verify_fqn_hierarchy(self):
        """验证FQN层次结构"""
        print("\n=== FQN层次结构验证 ===")
        
        conn = await asyncpg.connect(**self.age_db_config)
        try:
            await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
            
            # 验证数据源->数据库->Schema->表->列的层次结构
            hierarchy_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (ds:DataSource)-[:CONFIGURES_DATABASE]->(db:Database)-[:HAS_SCHEMA]->(s:Schema)-[:HAS_OBJECT]->(obj)-[:HAS_COLUMN]->(col:Column)
                RETURN ds.fqn as ds_fqn, db.fqn as db_fqn, s.fqn as schema_fqn, obj.fqn as obj_fqn, col.fqn as col_fqn
                LIMIT 10
            $$) AS (ds_fqn agtype, db_fqn agtype, schema_fqn agtype, obj_fqn agtype, col_fqn agtype);
            """
            
            hierarchy_results = await conn.fetch(hierarchy_query)
            
            print(f"✅ 完整层次结构路径: {len(hierarchy_results)} 条（显示前5条）")
            
            fqn_issues = []
            for i, result in enumerate(hierarchy_results[:5], 1):
                ds_fqn = str(result['ds_fqn']).strip('"')
                db_fqn = str(result['db_fqn']).strip('"')
                schema_fqn = str(result['schema_fqn']).strip('"')
                obj_fqn = str(result['obj_fqn']).strip('"')
                col_fqn = str(result['col_fqn']).strip('"')
                
                print(f"  路径 {i}:")
                print(f"    数据源: {ds_fqn}")
                print(f"    数据库: {db_fqn}")
                print(f"    Schema: {schema_fqn}")
                print(f"    对象: {obj_fqn}")
                print(f"    列: {col_fqn}")
                
                # 验证FQN的层次包含关系
                if not db_fqn.startswith(ds_fqn.split('_')[-1]):  # 数据源名应该在数据库FQN中
                    fqn_issues.append(f"数据库FQN不包含数据源名: {ds_fqn} -> {db_fqn}")
                
                if not schema_fqn.startswith(db_fqn):
                    fqn_issues.append(f"Schema FQN不以数据库FQN开头: {db_fqn} -> {schema_fqn}")
                
                if not obj_fqn.startswith(schema_fqn):
                    fqn_issues.append(f"对象FQN不以Schema FQN开头: {schema_fqn} -> {obj_fqn}")
                
                if not col_fqn.startswith(obj_fqn):
                    fqn_issues.append(f"列FQN不以对象FQN开头: {obj_fqn} -> {col_fqn}")
            
            if fqn_issues:
                print(f"❌ FQN层次结构问题: {len(fqn_issues)} 个")
                for issue in fqn_issues:
                    print(f"  - {issue}")
            else:
                print("✅ FQN层次结构正确")
                
        finally:
            await conn.close()
    
    async def verify_relationship_integrity(self):
        """验证关系完整性"""
        print("\n=== 关系完整性验证 ===")
        
        conn = await asyncpg.connect(**self.age_db_config)
        try:
            await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
            
            # 检查孤立节点（没有入边或出边的节点）
            orphan_nodes_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH (n)
                WHERE NOT (n)--()
                RETURN labels(n)[0] as node_type, count(*) as count
            $$) AS (node_type agtype, count agtype);
            """
            
            try:
                orphan_results = await conn.fetch(orphan_nodes_query)
                if orphan_results:
                    print("⚠️  发现孤立节点:")
                    for result in orphan_results:
                        node_type = str(result['node_type']).strip('"')
                        count = result['count']
                        print(f"  - {node_type}: {count} 个")
                else:
                    print("✅ 没有发现孤立节点")
            except Exception as e:
                print(f"❌ 孤立节点检查失败: {str(e)}")
            
            # 检查关系类型统计
            relationship_stats_query = """
            SELECT * FROM cypher('lumi_graph', $$ 
                MATCH ()-[r]->()
                RETURN type(r) as rel_type, count(*) as count
            $$) AS (rel_type agtype, count agtype);
            """
            
            try:
                rel_stats = await conn.fetch(relationship_stats_query)
                print("\n关系类型统计:")
                for stat in rel_stats:
                    rel_type = str(stat['rel_type']).strip('"')
                    count = stat['count']
                    print(f"  - {rel_type}: {count} 个")
            except Exception as e:
                print(f"❌ 关系统计失败: {str(e)}")
                
        finally:
            await conn.close()

async def main():
    """主函数"""
    try:
        print("=== AGE图谱准确性验证开始 ===")
        
        verifier = GraphAccuracyVerifier()
        
        # 1. 验证表结构一致性
        await verifier.verify_table_structure()
        
        # 2. 验证列关系准确性
        await verifier.verify_column_relationships()
        
        # 3. 验证FQN层次结构
        await verifier.verify_fqn_hierarchy()
        
        # 4. 验证关系完整性
        await verifier.verify_relationship_integrity()
        
        print("\n=== 验证完成 ===")
        
    except Exception as e:
        logger.error(f"验证过程出错: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    asyncio.run(main()) 