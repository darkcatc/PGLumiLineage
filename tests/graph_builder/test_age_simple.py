#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
简单的AGE语法测试

测试基础的MERGE和SET语法
"""

import asyncio
import logging
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from pglumilineage.graph_builder.common_graph_utils import execute_cypher, ensure_age_graph_exists
from tests.graph_builder.test_settings import get_settings_instance
import asyncpg

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def test_age_syntax():
    """测试AGE基础语法"""
    settings = get_settings_instance()
    
    # 构建AGE数据库配置
    age_db_config = {
        'user': settings.INTERNAL_DB.USER,
        'password': settings.INTERNAL_DB.PASSWORD.get_secret_value(),
        'host': settings.INTERNAL_DB.HOST,
        'port': settings.INTERNAL_DB.PORT,
        'database': settings.INTERNAL_DB.DB_AGE,
    }
    
    conn = await asyncpg.connect(**age_db_config)
    graph_name = "test_syntax"
    
    try:
        # 确保图存在
        await ensure_age_graph_exists(conn, graph_name)
        
        # 测试1: 简单的MERGE + SET
        logger.info("测试1: 简单的MERGE + SET")
        cypher1 = """
        MERGE (n {label: "test", name: 'test1'})
        SET n.created_at = '2025-05-24T16:21:00'
        RETURN n
        """
        result1 = await execute_cypher(conn, cypher1, {}, graph_name)
        logger.info(f"结果1: {len(result1)} 个节点")
        
        # 测试2: 带参数的MERGE + SET
        logger.info("测试2: 带参数的MERGE + SET")
        cypher2 = """
        MERGE (n {label: "test", name: $name})
        SET n.created_at = $timestamp
        RETURN n
        """
        params2 = {"name": "test2", "timestamp": "2025-05-24T16:21:00"}
        result2 = await execute_cypher(conn, cypher2, params2, graph_name)
        logger.info(f"结果2: {len(result2)} 个节点")
        
        # 测试3: 关系创建
        logger.info("测试3: 关系创建")
        cypher3 = """
        MATCH (n1 {label: "test", name: 'test1'})
        MATCH (n2 {label: "test", name: 'test2'})
        MERGE (n1)-[:connects]->(n2)
        RETURN n1, n2
        """
        result3 = await execute_cypher(conn, cypher3, {}, graph_name)
        logger.info(f"结果3: {len(result3)} 个关系")
        
        logger.info("所有测试通过!")
        
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        raise
    finally:
        await conn.close()

if __name__ == "__main__":
    asyncio.run(test_age_syntax()) 