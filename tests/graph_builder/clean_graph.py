#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
清除AGE图数据库中的所有数据

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


async def clean_graph():
    """清除图数据库"""
    
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
        
        logger.info("开始清除图数据库中的所有数据...")
        
        # 清除所有节点和关系
        clean_query = """
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (n) DETACH DELETE n
        $$) AS (result agtype);
        """
        
        await conn.execute(clean_query)
        logger.info("✅ 成功清除所有图数据")
        
        # 验证清除结果
        count_query = """
        SELECT * FROM cypher('lumi_graph', $$
            MATCH (n) RETURN count(n) as node_count
        $$) AS (node_count agtype);
        """
        
        result = await conn.fetch(count_query)
        node_count = str(result[0]['node_count']).strip('"')
        logger.info(f"剩余节点数量: {node_count}")
        
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(clean_graph()) 