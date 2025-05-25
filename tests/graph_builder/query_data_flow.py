#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
使用AGE语法查询monthly_channel_returns_analysis_report的数据流关系

展示各种数据流查询方式

作者: Vance Chen
"""

import asyncio
import asyncpg
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def query_data_flows():
    """查询数据流关系"""
    
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
        
        logger.info(f"查询表 {target_table} 的数据流关系...")
        logger.info("=" * 80)
        
        # 1. 查询目标表所有列的入流数据流
        logger.info("【查询1】目标表所有列的入流数据流:")
        
        inbound_flows_query = f"""
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (source_col:column)-[:data_flow]->(target_col:column)-[:has_column]-(target_table:table)
            WHERE target_table.name = '{target_table}'
            RETURN 
                source_col.name as source_column,
                source_col.fqn as source_fqn,
                target_col.name as target_column,
                target_col.fqn as target_fqn
            ORDER BY target_col.name, source_col.name
        $$) AS (source_column agtype, source_fqn agtype, target_column agtype, target_fqn agtype);
        """
        
        result = await conn.fetch(inbound_flows_query)
        logger.info(f"找到 {len(result)} 个数据流关系:")
        
        current_target = None
        for row in result:
            source_col = str(row['source_column']).strip('"')
            source_fqn = str(row['source_fqn']).strip('"')
            target_col = str(row['target_column']).strip('"')
            target_fqn = str(row['target_fqn']).strip('"')
            
            if current_target != target_col:
                current_target = target_col
                logger.info(f"\n  目标列: {target_col}")
            
            logger.info(f"    ← {source_col} ({source_fqn})")
        
        logger.info("\n" + "=" * 80)
        
        # 2. 查询特定列的数据流（以sales_year_month为例）
        logger.info("【查询2】特定列的数据流 (sales_year_month):")
        
        specific_column_query = """
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (source_col:column)-[:data_flow]->(target_col:column)
            WHERE target_col.name = 'sales_year_month'
            RETURN 
                source_col.name as source_column,
                source_col.fqn as source_fqn,
                target_col.name as target_column
        $$) AS (source_column agtype, source_fqn agtype, target_column agtype);
        """
        
        result = await conn.fetch(specific_column_query)
        logger.info(f"sales_year_month 列有 {len(result)} 个数据源:")
        for row in result:
            source_col = str(row['source_column']).strip('"')
            source_fqn = str(row['source_fqn']).strip('"')
            logger.info(f"  ← {source_col} ({source_fqn})")
        
        logger.info("\n" + "=" * 80)
        
        # 3. 查询完整的数据流路径（从源表到目标表）
        logger.info("【查询3】完整的数据流路径 (从源表到目标列):")
        
        full_path_query = f"""
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (source_table:table)-[:has_column]->(source_col:column)-[:data_flow]->(target_col:column)-[:has_column]-(target_table:table)
            WHERE target_table.name = '{target_table}'
            RETURN 
                source_table.name as source_table,
                source_col.name as source_column,
                target_col.name as target_column,
                target_table.name as target_table
            ORDER BY target_col.name, source_table.name
        $$) AS (source_table agtype, source_column agtype, target_column agtype, target_table agtype);
        """
        
        result = await conn.fetch(full_path_query)
        logger.info(f"找到 {len(result)} 个完整数据流路径:")
        
        current_target = None
        for row in result:
            source_table = str(row['source_table']).strip('"')
            source_col = str(row['source_column']).strip('"')
            target_col = str(row['target_column']).strip('"')
            target_table_name = str(row['target_table']).strip('"')
            
            if current_target != target_col:
                current_target = target_col
                logger.info(f"\n  {target_table_name}.{target_col}:")
            
            logger.info(f"    ← {source_table}.{source_col}")
        
        logger.info("\n" + "=" * 80)
        
        # 4. 查询数据流统计信息
        logger.info("【查询4】数据流统计信息:")
        
        # 4.1 按目标列统计数据源数量 (AGE不支持GROUP BY，用简单查询)
        stats_query = f"""
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (source_col:column)-[:data_flow]->(target_col:column)-[:has_column]-(target_table:table)
            WHERE target_table.name = '{target_table}'
            RETURN 
                target_col.name as target_column,
                source_col.name as source_column
            ORDER BY target_col.name, source_col.name
        $$) AS (target_column agtype, source_column agtype);
        """
        
        result = await conn.fetch(stats_query)
        logger.info("各目标列的数据源统计:")
        
        # 手动统计（因为AGE不支持GROUP BY）
        target_stats = {}
        for row in result:
            target_col = str(row['target_column']).strip('"')
            source_col = str(row['source_column']).strip('"')
            if target_col not in target_stats:
                target_stats[target_col] = []
            target_stats[target_col].append(source_col)
        
        for target_col, sources in sorted(target_stats.items(), key=lambda x: len(x[1]), reverse=True):
            logger.info(f"  {target_col}: {len(sources)} 个数据源")
        
        # 4.2 按源表统计贡献的数据流数量
        logger.info("\n各源表贡献的数据流数量:")
        
        source_table_stats_query = f"""
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (source_table:table)-[:has_column]->(source_col:column)-[:data_flow]->(target_col:column)-[:has_column]-(target_table:table)
            WHERE target_table.name = '{target_table}'
            RETURN 
                source_table.name as source_table,
                source_col.name as source_column
            ORDER BY source_table.name, source_col.name
        $$) AS (source_table agtype, source_column agtype);
        """
        
        result = await conn.fetch(source_table_stats_query)
        
        # 手动统计源表数据流数量
        source_table_stats = {}
        for row in result:
            source_table = str(row['source_table']).strip('"')
            source_col = str(row['source_column']).strip('"')
            if source_table not in source_table_stats:
                source_table_stats[source_table] = []
            source_table_stats[source_table].append(source_col)
        
        for source_table, columns in sorted(source_table_stats.items(), key=lambda x: len(x[1]), reverse=True):
            logger.info(f"  {source_table}: {len(columns)} 个数据流")
        
        logger.info("\n" + "=" * 80)
        
        # 5. 查询双向数据流（如果存在）
        logger.info("【查询5】检查是否存在双向数据流:")
        
        bidirectional_query = f"""
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (col1:column)-[:data_flow]->(col2:column)-[:data_flow]->(col1)
            RETURN 
                col1.name as column1,
                col2.name as column2
        $$) AS (column1 agtype, column2 agtype);
        """
        
        result = await conn.fetch(bidirectional_query)
        if result:
            logger.info(f"发现 {len(result)} 个双向数据流:")
            for row in result:
                col1 = str(row['column1']).strip('"')
                col2 = str(row['column2']).strip('"')
                logger.info(f"  {col1} ↔ {col2}")
        else:
            logger.info("未发现双向数据流")
        
        logger.info("\n" + "=" * 80)
        logger.info("数据流查询完成！")
        
    finally:
        await conn.close()


async def show_query_examples():
    """展示AGE查询语法示例"""
    
    logger.info("AGE数据流查询语法示例:")
    logger.info("=" * 80)
    
    examples = [
        {
            "name": "基本数据流查询",
            "description": "查询从源列到目标列的直接数据流",
            "cypher": """
SELECT * FROM cypher('lumi_graph', $$
    MATCH (source:column)-[:data_flow]->(target:column)
    WHERE target.name = '目标列名'
    RETURN source.name, target.name
$$) AS (source_name agtype, target_name agtype);
"""
        },
        {
            "name": "表级数据流查询", 
            "description": "查询从源表到目标表的所有数据流",
            "cypher": """
SELECT * FROM cypher('lumi_graph', $$
    MATCH (st:table)-[:has_column]->(sc:column)-[:data_flow]->(tc:column)-[:has_column]-(tt:table)
    WHERE tt.name = '目标表名'
    RETURN st.name, sc.name, tc.name, tt.name
$$) AS (source_table agtype, source_column agtype, target_column agtype, target_table agtype);
"""
        },
        {
            "name": "数据流统计查询",
            "description": "统计每个目标列的数据源数量",
            "cypher": """
SELECT * FROM cypher('lumi_graph', $$
    MATCH (source:column)-[:data_flow]->(target:column)
    RETURN target.name, count(source) as source_count
    ORDER BY source_count DESC
$$) AS (target_column agtype, source_count agtype);
"""
        },
        {
            "name": "SQL模式查询",
            "description": "查询SQL模式与表的关系",
            "cypher": """
SELECT * FROM cypher('lumi_graph', $$
    MATCH (sql:sqlpattern)-[r]->(table:table)
    WHERE table.name = '目标表名'
    RETURN sql.sql_hash, type(r), table.name
$$) AS (sql_hash agtype, relation_type agtype, table_name agtype);
"""
        }
    ]
    
    for i, example in enumerate(examples, 1):
        logger.info(f"【示例{i}】{example['name']}:")
        logger.info(f"说明: {example['description']}")
        logger.info("查询语句:")
        logger.info(example['cypher'])
        logger.info("-" * 60)


if __name__ == "__main__":
    asyncio.run(query_data_flows())
    print("\n" + "="*80 + "\n")
    asyncio.run(show_query_examples()) 