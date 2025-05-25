#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查目标表和所需源表是否存在

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


async def check_target_table():
    """检查目标表和源表"""
    
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
        
        # 1. 检查目标表的列
        logger.info("检查目标表的列:")
        
        columns_query = """
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (t:table)-[:has_column]->(c:column) 
            WHERE t.name = 'monthly_channel_returns_analysis_report'
            RETURN c.name as column_name
            ORDER BY c.name
        $$) AS (column_name agtype);
        """
        
        column_rows = await conn.fetch(columns_query)
        actual_columns = []
        for row in column_rows:
            col_name = str(row['column_name']).strip('"')
            logger.info(f"  ✅ {col_name}")
            actual_columns.append(col_name)
        
        expected_columns = [
            "sales_year_month", "channel", "total_sold_quantity",
            "total_returned_quantity", "return_rate_by_quantity",
            "primary_return_reason_desc", "primary_reason_returned_quantity"
        ]
        
        missing_columns = set(expected_columns) - set(actual_columns)
        if missing_columns:
            logger.error(f"缺失的目标列: {missing_columns}")
        else:
            logger.info("✅ 所有目标列都存在")
        
        # 2. 检查所需的源表
        logger.info("\n检查所需的源表:")
        
        required_tables = [
            "date_dim", "store_sales", "catalog_sales", "web_sales",
            "store_returns", "catalog_returns", "web_returns", "reason"
        ]
        
        all_tables_exist = True
        for table_name in required_tables:
            check_table_query = f"""
            SELECT * FROM cypher('lumi_graph', $$
                MATCH (t:table) 
                WHERE t.name = '{table_name}'
                RETURN t.name as table_name
            $$) AS (table_name agtype);
            """
            
            check_rows = await conn.fetch(check_table_query)
            if check_rows:
                logger.info(f"  ✅ {table_name}")
            else:
                logger.error(f"  ❌ {table_name}: 未找到")
                all_tables_exist = False
        
        # 3. 检查所需的源列
        logger.info("\n检查所需的源列:")
        
        required_columns = [
            ("date_dim", "d_date"),
            ("store_sales", "ss_quantity"),
            ("catalog_sales", "cs_quantity"), 
            ("web_sales", "ws_quantity"),
            ("store_returns", "sr_return_quantity"),
            ("catalog_returns", "cr_return_quantity"),
            ("web_returns", "wr_return_quantity"),
            ("reason", "r_reason_desc")
        ]
        
        all_columns_exist = True
        for table_name, column_name in required_columns:
            check_column_query = f"""
            SELECT * FROM cypher('lumi_graph', $$
                MATCH (t:table)-[:has_column]->(c:column) 
                WHERE t.name = '{table_name}' AND c.name = '{column_name}'
                RETURN c.name as column_name
            $$) AS (column_name agtype);
            """
            
            check_rows = await conn.fetch(check_column_query)
            if check_rows:
                logger.info(f"  ✅ {table_name}.{column_name}")
            else:
                logger.error(f"  ❌ {table_name}.{column_name}: 未找到")
                all_columns_exist = False
        
        # 4. 总结
        logger.info("\n" + "="*60)
        logger.info("元数据检查结果:")
        
        if not missing_columns and all_tables_exist and all_columns_exist:
            logger.info("✅ 所有必需的元数据都存在，可以继续导入血缘关系!")
            return True
        else:
            logger.error("❌ 缺少必需的元数据，无法导入血缘关系")
            return False
        
    finally:
        await conn.close()


if __name__ == "__main__":
    result = asyncio.run(check_target_table())
    if result:
        print("\n可以继续执行血缘关系导入!")
    else:
        print("\n请先修复元数据问题!") 