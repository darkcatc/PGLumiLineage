#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
调试血缘关系数据

检查write_to关系的源节点和列级血缘关系

作者: Vance Chen
"""

import asyncio
import asyncpg
import json
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def parse_age_data(raw_data):
    """解析AGE返回的数据"""
    if isinstance(raw_data, str):
        if "::vertex" in raw_data:
            raw_data = raw_data.replace("::vertex", "")
        elif "::edge" in raw_data:
            raw_data = raw_data.replace("::edge", "")
    return json.loads(raw_data)


async def debug_lineage_data():
    """调试血缘关系数据"""
    
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
        
        logger.info("=" * 60)
        logger.info("调试血缘关系数据")
        logger.info("=" * 60)
        
        # 1. 检查目标表的基本信息
        logger.info("\n1. 检查目标表信息:")
        table_query = f"""
        SELECT * FROM cypher('{graph_name}', $$ 
            MATCH (n:table)
            WHERE n.name = 'monthly_channel_returns_analysis_report'
            RETURN n
        $$) as (n agtype);
        """
        
        table_result = await conn.fetch(table_query)
        if table_result:
            table_data = parse_age_data(table_result[0]['n'])
            logger.info(f"目标表: {table_data}")
            table_id = table_data['id']
        else:
            logger.error("未找到目标表")
            return
        
        # 2. 检查所有write_to关系
        logger.info("\n2. 检查所有write_to关系:")
        write_to_query = f"""
        SELECT * FROM cypher('{graph_name}', $$ 
            MATCH (source)-[r:writes_to]->(target)
            RETURN source, r, target
        $$) as (source agtype, r agtype, target agtype);
        """
        
        write_to_result = await conn.fetch(write_to_query)
        logger.info(f"找到 {len(write_to_result)} 个write_to关系")
        
        for i, row in enumerate(write_to_result):
            source_data = parse_age_data(row['source'])
            rel_data = parse_age_data(row['r'])
            target_data = parse_age_data(row['target'])
            
            logger.info(f"\nwrite_to关系 {i+1}:")
            logger.info(f"  源节点: {source_data}")
            logger.info(f"  关系: {rel_data}")
            logger.info(f"  目标节点: {target_data}")
        
        # 3. 检查目标表的所有入边关系
        logger.info(f"\n3. 检查目标表(ID: {table_id})的所有入边关系:")
        incoming_query = f"""
        SELECT * FROM cypher('{graph_name}', $$ 
            MATCH (source)-[r]->(target)
            WHERE id(target) = {table_id}
            RETURN source, r, target
        $$) as (source agtype, r agtype, target agtype);
        """
        
        incoming_result = await conn.fetch(incoming_query)
        logger.info(f"找到 {len(incoming_result)} 个入边关系")
        
        for i, row in enumerate(incoming_result):
            source_data = parse_age_data(row['source'])
            rel_data = parse_age_data(row['r'])
            target_data = parse_age_data(row['target'])
            
            logger.info(f"\n入边关系 {i+1}:")
            logger.info(f"  源节点: {source_data}")
            logger.info(f"  关系类型: {rel_data.get('label', 'unknown')}")
            logger.info(f"  目标节点: {target_data}")
        
        # 4. 检查目标表的列
        logger.info(f"\n4. 检查目标表的列:")
        columns_query = f"""
        SELECT * FROM cypher('{graph_name}', $$ 
            MATCH (table)-[r:has_column]->(col)
            WHERE id(table) = {table_id}
            RETURN col
        $$) as (col agtype);
        """
        
        columns_result = await conn.fetch(columns_query)
        logger.info(f"找到 {len(columns_result)} 个列")
        
        column_ids = []
        for i, row in enumerate(columns_result):
            column_data = parse_age_data(row['col'])
            column_ids.append(column_data['id'])
            logger.info(f"  列 {i+1}: {column_data}")
        
        # 5. 检查列级血缘关系
        logger.info(f"\n5. 检查列级血缘关系:")
        if column_ids:
            column_ids_str = ", ".join([str(id) for id in column_ids])
            column_lineage_query = f"""
            SELECT * FROM cypher('{graph_name}', $$ 
                MATCH (source)-[r]->(target)
                WHERE id(target) IN [{column_ids_str}]
                RETURN source, r, target
            $$) as (source agtype, r agtype, target agtype);
            """
            
            column_lineage_result = await conn.fetch(column_lineage_query)
            logger.info(f"找到 {len(column_lineage_result)} 个列级血缘关系")
            
            for i, row in enumerate(column_lineage_result):
                source_data = parse_age_data(row['source'])
                rel_data = parse_age_data(row['r'])
                target_data = parse_age_data(row['target'])
                
                logger.info(f"\n列级血缘关系 {i+1}:")
                logger.info(f"  源节点: {source_data}")
                logger.info(f"  关系类型: {rel_data.get('label', 'unknown')}")
                logger.info(f"  目标列: {target_data}")
        
        # 6. 检查所有SQL模式节点
        logger.info(f"\n6. 检查所有SQL模式节点:")
        sql_patterns_query = f"""
        SELECT * FROM cypher('{graph_name}', $$ 
            MATCH (n:sqlpattern)
            RETURN n
        $$) as (n agtype);
        """
        
        sql_patterns_result = await conn.fetch(sql_patterns_query)
        logger.info(f"找到 {len(sql_patterns_result)} 个SQL模式节点")
        
        for i, row in enumerate(sql_patterns_result):
            pattern_data = parse_age_data(row['n'])
            logger.info(f"  SQL模式 {i+1}: {pattern_data}")
        
        # 7. 检查SQL模式的关系
        logger.info(f"\n7. 检查SQL模式的关系:")
        sql_pattern_rels_query = f"""
        SELECT * FROM cypher('{graph_name}', $$ 
            MATCH (source)-[r]-(target)
            WHERE source.label = 'sqlpattern' OR target.label = 'sqlpattern'
            RETURN source, r, target
        $$) as (source agtype, r agtype, target agtype);
        """
        
        sql_pattern_rels_result = await conn.fetch(sql_pattern_rels_query)
        logger.info(f"找到 {len(sql_pattern_rels_result)} 个SQL模式相关关系")
        
        for i, row in enumerate(sql_pattern_rels_result):
            source_data = parse_age_data(row['source'])
            rel_data = parse_age_data(row['r'])
            target_data = parse_age_data(row['target'])
            
            logger.info(f"\nSQL模式关系 {i+1}:")
            logger.info(f"  源节点: {source_data}")
            logger.info(f"  关系类型: {rel_data.get('label', 'unknown')}")
            logger.info(f"  目标节点: {target_data}")
        
        await conn.close()
        
        logger.info("\n" + "=" * 60)
        logger.info("调试完成")
        
    except Exception as e:
        logger.error(f"调试失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


if __name__ == "__main__":
    asyncio.run(debug_lineage_data()) 