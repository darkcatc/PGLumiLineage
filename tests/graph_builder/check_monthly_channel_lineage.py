#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查monthly_channel_returns_analysis_report表的血缘关系

与JSON文件中的原始关系进行对比验证

作者: Vance Chen
"""

import asyncio
import asyncpg
import json
import logging
from pathlib import Path

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def check_table_lineage():
    """检查表的血缘关系"""
    
    # AGE数据库连接配置
    age_db_config = {
        'user': 'lumiadmin',
        'password': 'lumiadmin',
        'host': 'localhost',
        'port': 5432,
        'database': 'iwdb'
    }
    
    conn = await asyncpg.connect(**age_db_config)
    
    try:
        # 设置搜索路径
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        
        target_table = "monthly_channel_returns_analysis_report"
        
        logger.info(f"检查表 {target_table} 的血缘关系...")
        
        # 1. 检查目标表节点是否存在
        table_query = """
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (t) 
            WHERE t.name = 'monthly_channel_returns_analysis_report' 
            RETURN t.name as table_name, t.schema_name as schema_name, t.object_type as object_type
        $$) AS (table_name agtype, schema_name agtype, object_type agtype);
        """
        
        table_rows = await conn.fetch(table_query)
        logger.info(f"找到目标表节点: {len(table_rows)} 个")
        for row in table_rows:
            logger.info(f"  表名: {row['table_name']}, 模式: {row['schema_name']}, 类型: {row['object_type']}")
        
        # 2. 检查目标表的列
        columns_query = """
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (t)-[:has_column]->(c) 
            WHERE t.name = 'monthly_channel_returns_analysis_report'
            RETURN c.name as column_name, c.fqn as column_fqn
            ORDER BY c.name
        $$) AS (column_name agtype, column_fqn agtype);
        """
        
        column_rows = await conn.fetch(columns_query)
        logger.info(f"找到目标表列: {len(column_rows)} 个")
        actual_columns = []
        for row in column_rows:
            column_name = str(row['column_name']).strip('"')
            logger.info(f"  列: {column_name}")
            actual_columns.append(column_name)
        
        # 3. 检查数据流关系
        data_flow_query = """
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (src)-[df:data_flow]->(tgt) 
            WHERE tgt.fqn CONTAINS 'monthly_channel_returns_analysis_report'
            RETURN 
                src.name as src_name, 
                src.fqn as src_fqn,
                df.transformation_logic as transformation,
                df.derivation_type as derivation_type,
                tgt.name as tgt_column,
                tgt.fqn as tgt_fqn
            ORDER BY tgt.name, src.name
        $$) AS (src_name agtype, src_fqn agtype, transformation agtype, derivation_type agtype, tgt_column agtype, tgt_fqn agtype);
        """
        
        flow_rows = await conn.fetch(data_flow_query)
        logger.info(f"找到数据流关系: {len(flow_rows)} 个")
        
        # 按目标列分组
        flows_by_target = {}
        for row in flow_rows:
            tgt_col = str(row['tgt_column']).strip('"')
            src_name = str(row['src_name']).strip('"')
            src_fqn = str(row['src_fqn']).strip('"')
            transformation = str(row['transformation']).strip('"') if row['transformation'] else ""
            derivation_type = str(row['derivation_type']).strip('"') if row['derivation_type'] else ""
            
            if tgt_col not in flows_by_target:
                flows_by_target[tgt_col] = []
            
            flows_by_target[tgt_col].append({
                'src_name': src_name,
                'src_fqn': src_fqn,
                'transformation': transformation,
                'derivation_type': derivation_type
            })
        
        # 4. 检查SQL模式关系
        sql_pattern_query = """
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (sp:sqlpattern)-[r]->(obj) 
            WHERE obj.name = 'monthly_channel_returns_analysis_report' OR 
                  obj.fqn CONTAINS 'monthly_channel_returns_analysis_report'
            RETURN 
                sp.sql_hash as sql_hash,
                type(r) as relation_type,
                obj.name as obj_name,
                obj.fqn as obj_fqn
        $$) AS (sql_hash agtype, relation_type agtype, obj_name agtype, obj_fqn agtype);
        """
        
        sql_rows = await conn.fetch(sql_pattern_query)
        logger.info(f"找到SQL模式关系: {len(sql_rows)} 个")
        for row in sql_rows:
            logger.info(f"  SQL模式: {str(row['sql_hash']).strip('\"')}, 关系: {str(row['relation_type']).strip('\"')}, 对象: {str(row['obj_name']).strip('\"')}")
        
        # 5. 与JSON文件对比
        logger.info("=" * 60)
        logger.info("与JSON文件对比分析:")
        
        # 加载JSON数据
        json_file = "data/llm/relations/8ceac254_20250516_151213.json"
        with open(json_file, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        
        # 比较列级血缘
        json_lineage = json_data['column_level_lineage']
        expected_columns = [item['target_column'] for item in json_lineage]
        
        logger.info(f"JSON中期望的目标列: {len(expected_columns)} 个")
        for col in expected_columns:
            logger.info(f"  {col}")
        
        logger.info(f"AGE中实际的目标列: {len(actual_columns)} 个")
        for col in actual_columns:
            logger.info(f"  {col}")
        
        # 检查缺失的列
        missing_columns = set(expected_columns) - set(actual_columns)
        extra_columns = set(actual_columns) - set(expected_columns)
        
        if missing_columns:
            logger.error(f"缺失的列: {missing_columns}")
        if extra_columns:
            logger.warning(f"额外的列: {extra_columns}")
        
        # 详细比较每个列的血缘关系
        logger.info("=" * 60)
        logger.info("列级血缘关系详细对比:")
        
        for json_item in json_lineage:
            target_col = json_item['target_column']
            logger.info(f"\n【{target_col}】")
            
            # JSON中的源
            json_sources = json_item['sources']
            logger.info(f"  JSON中的源数量: {len(json_sources)}")
            for src in json_sources:
                src_obj = src.get('source_object', {})
                src_col = src.get('source_column')
                transformation = src.get('transformation_logic', '')
                logger.info(f"    {src_obj.get('name', 'Unknown')}.{src_col or 'NULL'} -> {transformation}")
            
            # AGE中的源
            age_sources = flows_by_target.get(target_col, [])
            logger.info(f"  AGE中的源数量: {len(age_sources)}")
            for src in age_sources:
                logger.info(f"    {src['src_name']} -> {src['transformation']}")
            
            # 比较
            if len(json_sources) != len(age_sources):
                logger.error(f"  ❌ 源数量不匹配: JSON={len(json_sources)}, AGE={len(age_sources)}")
            else:
                logger.info(f"  ✅ 源数量匹配: {len(json_sources)}")
        
        logger.info("=" * 60)
        logger.info("检查完成")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(check_table_lineage()) 