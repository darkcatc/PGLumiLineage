#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
图谱验证脚本

验证AGE图谱构建的准确性：
1. 检查节点数量和类型
2. 验证关系完整性
3. 检查表与列的对应关系

使用方法:
    python -m tests.graph_builder.verify_graph_simple
"""

import asyncio
import logging
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

# 使用测试配置
from tests.graph_builder.test_settings import get_settings_instance
import asyncpg

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

async def verify_graph():
    """验证图谱构建结果"""
    try:
        # 获取配置
        settings = get_settings_instance()
        
        # 构建AGE数据库配置
        age_db_config = {
            'user': settings.INTERNAL_DB.USER,
            'password': settings.INTERNAL_DB.PASSWORD.get_secret_value(),
            'host': settings.INTERNAL_DB.HOST,
            'port': settings.INTERNAL_DB.PORT,
            'database': settings.INTERNAL_DB.DB_AGE,
        }
        
        # 构建元数据数据库配置
        metadata_db_config = {
            'user': settings.INTERNAL_DB.USER,
            'password': settings.INTERNAL_DB.PASSWORD.get_secret_value(),
            'host': settings.INTERNAL_DB.HOST,
            'port': settings.INTERNAL_DB.PORT,
            'database': settings.INTERNAL_DB.DB_RAW_LOGS,
        }
        
        graph_name = "lumi_graph"
        
        logger.info("=== 开始验证AGE图谱 ===")
        
        # 连接AGE数据库
        age_conn = await asyncpg.connect(**age_db_config)
        try:
            await age_conn.execute("SET search_path = ag_catalog, \"$user\", public;")
            
            # 1. 检查图是否存在
            logger.info("1. 检查AGE图状态...")
            graphs = await age_conn.fetch("SELECT name FROM ag_graph WHERE name = $1;", graph_name)
            if not graphs:
                logger.error(f"AGE图 '{graph_name}' 不存在！")
                return False
            logger.info(f"✅ AGE图 '{graph_name}' 存在")
            
            # 2. 统计总节点数
            logger.info("2. 统计节点数量...")
            
            total_nodes_result = await age_conn.fetch("""
                SELECT * FROM cypher('lumi_graph', $$ 
                    MATCH (n) 
                    RETURN count(n) as total_count
                $$) AS (total_count agtype);
            """)
            total_nodes = int(total_nodes_result[0]['total_count']) if total_nodes_result else 0
            
            # 3. 统计各个层级节点
            logger.info("3. 分析节点标签...")
            
            # 获取所有节点的标签信息（通过属性查询）
            datasource_result = await age_conn.fetch("""
                SELECT * FROM cypher('lumi_graph', $$ 
                    MATCH (n)
                    WHERE n.source_id IS NOT NULL
                    RETURN count(n) as count
                $$) AS (count agtype);
            """)
            datasource_count = int(datasource_result[0]['count']) if datasource_result else 0
            
            database_result = await age_conn.fetch("""
                SELECT * FROM cypher('lumi_graph', $$ 
                    MATCH (n)
                    WHERE n.database_name IS NOT NULL
                    RETURN count(n) as count
                $$) AS (count agtype);
            """)
            database_count = int(database_result[0]['count']) if database_result else 0
            
            schema_result = await age_conn.fetch("""
                SELECT * FROM cypher('lumi_graph', $$ 
                    MATCH (n)
                    WHERE n.schema_name IS NOT NULL
                    RETURN count(n) as count
                $$) AS (count agtype);
            """)
            schema_count = int(schema_result[0]['count']) if schema_result else 0
            
            # 具有object_type的节点（表/视图）
            object_result = await age_conn.fetch("""
                SELECT * FROM cypher('lumi_graph', $$ 
                    MATCH (n)
                    WHERE n.object_type IS NOT NULL
                    RETURN count(n) as count
                $$) AS (count agtype);
            """)
            object_count = int(object_result[0]['count']) if object_result else 0
            
            # 具有column_name的节点（列）
            column_result = await age_conn.fetch("""
                SELECT * FROM cypher('lumi_graph', $$ 
                    MATCH (n)
                    WHERE n.column_name IS NOT NULL OR n.data_type IS NOT NULL
                    RETURN count(n) as count
                $$) AS (count agtype);
            """)
            column_count = int(column_result[0]['count']) if column_result else 0
            
            # 输出节点统计
            logger.info("节点统计:")
            logger.info(f"  总节点数: {total_nodes}")
            logger.info(f"  数据源: {datasource_count}")
            logger.info(f"  数据库: {database_count}")
            logger.info(f"  Schema: {schema_count}")
            logger.info(f"  表/视图: {object_count}")
            logger.info(f"  列: {column_count}")
            
            # 4. 检查关系统计
            logger.info("4. 检查关系类型...")
            
            total_edges_result = await age_conn.fetch("""
                SELECT * FROM cypher('lumi_graph', $$ 
                    MATCH ()-[r]->()
                    RETURN count(r) as total_edges
                $$) AS (total_edges agtype);
            """)
            total_edges = int(total_edges_result[0]['total_edges']) if total_edges_result else 0
            
            logger.info(f"  总关系数: {total_edges}")
            
            # 5. 验证数据一致性
            logger.info("5. 验证数据一致性...")
            
            # 检查FQN示例
            sample_objects = await age_conn.fetch("""
                SELECT * FROM cypher('lumi_graph', $$ 
                    MATCH (n)
                    WHERE n.object_type IS NOT NULL
                    RETURN n.fqn as object_fqn
                    LIMIT 3
                $$) AS (object_fqn agtype);
            """)
            
            logger.info("对象FQN示例:")
            for row in sample_objects:
                fqn = str(row['object_fqn']).strip('"')
                parts = fqn.split('.')
                status = "✅" if len(parts) == 4 else "⚠️"
                logger.info(f"  {status} {fqn} ({len(parts)} 层级)")
            
            # 检查列FQN示例
            sample_columns = await age_conn.fetch("""
                SELECT * FROM cypher('lumi_graph', $$ 
                    MATCH (n)
                    WHERE n.column_name IS NOT NULL OR n.data_type IS NOT NULL
                    RETURN n.fqn as column_fqn
                    LIMIT 3
                $$) AS (column_fqn agtype);
            """)
            
            logger.info("列FQN示例:")
            for row in sample_columns:
                fqn = str(row['column_fqn']).strip('"')
                parts = fqn.split('.')
                status = "✅" if len(parts) == 5 else "⚠️"
                logger.info(f"  {status} {fqn} ({len(parts)} 层级)")
            
        finally:
            await age_conn.close()
        
        # 6. 对比元数据源
        logger.info("6. 对比元数据源...")
        
        metadata_conn = await asyncpg.connect(**metadata_db_config)
        try:
            # 获取元数据表和列数量
            metadata_tables = await metadata_conn.fetch("""
                SELECT COUNT(*) as table_count 
                FROM lumi_metadata_store.objects_metadata 
                WHERE source_id = 1
            """)
            
            metadata_columns = await metadata_conn.fetch("""
                SELECT COUNT(*) as column_count 
                FROM lumi_metadata_store.columns_metadata c
                JOIN lumi_metadata_store.objects_metadata o ON c.object_id = o.object_id
                WHERE o.source_id = 1
            """)
            
            meta_table_count = metadata_tables[0]['table_count'] if metadata_tables else 0
            meta_column_count = metadata_columns[0]['column_count'] if metadata_columns else 0
            
            logger.info("元数据源统计:")
            logger.info(f"  表/视图: {meta_table_count}")
            logger.info(f"  列: {meta_column_count}")
            
            logger.info("图谱 vs 元数据源对比:")
            table_diff = object_count - meta_table_count
            column_diff = column_count - meta_column_count
            
            logger.info(f"  表/视图差异: {table_diff} (图谱: {object_count}, 元数据: {meta_table_count})")
            logger.info(f"  列差异: {column_diff} (图谱: {column_count}, 元数据: {meta_column_count})")
            
        finally:
            await metadata_conn.close()
        
        logger.info("=== 验证完成 ===")
        
        # 生成验证报告
        success = True
        expected_total = meta_table_count + meta_column_count + 3  # 表+列+数据源+数据库+schema
        
        if datasource_count == 0:
            logger.error("❌ 没有数据源节点")
            success = False
        if object_count == 0:
            logger.error("❌ 没有表/视图节点")
            success = False
        if column_count == 0:
            logger.error("❌ 没有列节点")
            success = False
        if total_edges == 0:
            logger.error("❌ 没有关系")
            success = False
        
        if abs(object_count - meta_table_count) > 0:
            logger.warning(f"⚠️ 表/视图数量不匹配")
        if abs(column_count - meta_column_count) > 0:
            logger.warning(f"⚠️ 列数量不匹配")
        
        if success:
            logger.info("✅ 图谱验证通过！")
            logger.info(f"✅ 图谱包含 {total_nodes} 个节点和 {total_edges} 个关系")
        else:
            logger.error("❌ 图谱验证失败！")
        
        return success
        
    except Exception as e:
        logger.error(f"验证过程出错: {str(e)}", exc_info=True)
        return False

if __name__ == "__main__":
    try:
        success = asyncio.run(verify_graph())
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("用户中断操作")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        sys.exit(1) 