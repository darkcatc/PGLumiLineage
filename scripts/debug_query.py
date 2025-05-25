#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
调试查询脚本

检查数据库中节点的实际结构和FQN格式

作者: Vance Chen
"""

import asyncio
import asyncpg
import logging
from pathlib import Path
import sys
import json

# 添加项目根目录到Python路径
sys.path.append(str(Path(__file__).parent.parent))

from pglumilineage.common.config import get_settings

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def debug_node_structure():
    """调试节点结构"""
    
    settings = get_settings()
    
    try:
        # 连接AGE数据库
        age_dsn = str(settings.AGE_DSN)
        conn = await asyncpg.connect(age_dsn)
        
        # 设置搜索路径
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        
        # 查询月度渠道退货分析报告相关的节点
        result = await conn.fetch("""
            SELECT * FROM cypher('lumi_graph', $$
                MATCH (n)
                WHERE n.fqn CONTAINS 'monthly_channel_returns_analysis_report'
                RETURN n
            $$) as (n agtype)
        """)
        
        logger.info("包含目标表的节点详情:")
        for i, row in enumerate(result):
            node_data = row['n']
            logger.info(f"节点 {i+1}: {node_data}")
            
            # 尝试解析节点数据
            try:
                if isinstance(node_data, str) and "::vertex" in node_data:
                    node_data = node_data.replace("::vertex", "")
                    node_obj = json.loads(node_data)
                    logger.info(f"  解析后的节点对象: {node_obj}")
                    logger.info(f"  节点ID: {node_obj.get('id')}")
                    logger.info(f"  节点标签: {node_obj.get('label')}")
                    logger.info(f"  节点属性: {node_obj.get('properties', {})}")
                    
                    properties = node_obj.get('properties', {})
                    logger.info(f"  FQN: {properties.get('fqn', 'N/A')}")
                    logger.info(f"  名称: {properties.get('name', 'N/A')}")
                    logger.info(f"  数据库: {properties.get('database_name', 'N/A')}")
                    logger.info(f"  模式: {properties.get('schema_name', 'N/A')}")
                else:
                    logger.info(f"  直接使用节点数据: {node_data}")
            except Exception as e:
                logger.error(f"  解析节点数据失败: {e}")
        
        # 查询所有table类型的节点（前10个）
        logger.info("\n所有table类型节点示例:")
        result = await conn.fetch("""
            SELECT * FROM cypher('lumi_graph', $$
                MATCH (n:table)
                RETURN n
                LIMIT 10
            $$) as (n agtype)
        """)
        
        for i, row in enumerate(result):
            node_data = row['n']
            logger.info(f"表节点 {i+1}: {node_data}")
        
        # 测试特定的查询
        test_fqn = "tpcds.tpcds.public.monthly_channel_returns_analysis_report"
        logger.info(f"\n测试查询 FQN: {test_fqn}")
        
        result = await conn.fetch(f"""
            SELECT * FROM cypher('lumi_graph', $$
                MATCH (n:table)
                WHERE n.fqn = '{test_fqn}'
                RETURN n
            $$) as (n agtype)
        """)
        
        if result:
            logger.info(f"✅ 使用FQN查询成功: {result[0]['n']}")
        else:
            logger.warning("❌ 使用FQN查询失败")
            
            # 尝试其他查询方式
            logger.info("尝试使用CONTAINS查询...")
            result2 = await conn.fetch(f"""
                SELECT * FROM cypher('lumi_graph', $$
                    MATCH (n:table)
                    WHERE n.fqn CONTAINS 'monthly_channel_returns_analysis_report'
                    RETURN n
                $$) as (n agtype)
            """)
            
            if result2:
                logger.info(f"✅ 使用CONTAINS查询成功: {result2[0]['n']}")
            else:
                logger.warning("❌ 使用CONTAINS查询也失败")
        
        await conn.close()
        
    except Exception as e:
        logger.error(f"调试查询失败: {e}")
        import traceback
        logger.error(traceback.format_exc())


async def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("调试节点结构")
    logger.info("=" * 60)
    
    await debug_node_structure()


if __name__ == "__main__":
    asyncio.run(main()) 