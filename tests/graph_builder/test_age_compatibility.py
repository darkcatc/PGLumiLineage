#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试AGE 1.5.0兼容性

使用方法:
    python -m tests.graph_builder.test_age_compatibility
"""

import asyncio
import logging
import sys
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from pglumilineage.graph_builder.common_graph_utils import convert_cypher_for_age, execute_cypher, ensure_age_graph_exists
from tests.graph_builder.test_settings import get_settings_instance
import asyncpg

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def test_cypher_conversion():
    """测试Cypher语句转换"""
    logger.info("=== 测试Cypher语句转换 ===")
    
    # 测试1: 节点标签转换
    original = "MERGE (ds:DataSource {fqn: 'test'})"
    converted = convert_cypher_for_age(original)
    logger.info(f"节点标签转换:")
    logger.info(f"  原始: {original}")
    logger.info(f"  转换后: {converted}")
    
    # 测试2: 关系类型转换
    original_rel = "MATCH (a:Table)-[:HAS_COLUMN]->(b:Column) RETURN a, b"
    converted_rel = convert_cypher_for_age(original_rel)
    logger.info(f"关系类型转换:")
    logger.info(f"  原始: {original_rel}")
    logger.info(f"  转换后: {converted_rel}")
    
    # 测试3: 带变量的关系转换
    original_rel_var = "MATCH (a:Table)-[r:HAS_COLUMN]->(b:Column) RETURN a, r, b"
    converted_rel_var = convert_cypher_for_age(original_rel_var)
    logger.info(f"带变量的关系转换:")
    logger.info(f"  原始: {original_rel_var}")
    logger.info(f"  转换后: {converted_rel_var}")
    
    # 测试4: ON CREATE SET转换
    original_with_set = """
    MERGE (ds:DataSource {fqn: $fqn})
    ON CREATE SET 
        ds.name = $name,
        ds.created_at = datetime(),
        ds.updated_at = datetime()
    ON MATCH SET
        ds.name = $name,
        ds.updated_at = datetime()
    RETURN ds
    """
    converted_with_set = convert_cypher_for_age(original_with_set)
    logger.info(f"ON CREATE/MATCH SET转换:")
    logger.info(f"  原始: {original_with_set}")
    logger.info(f"  转换后: {converted_with_set}")

async def test_basic_operations(conn, graph_name):
    """测试基本的图操作"""
    logger.info("=== 测试基本图操作 ===")
    
    # 测试1: 创建简单节点
    logger.info("1. 创建简单节点")
    cypher = "MERGE (n:TestNode {name: 'basic_test'}) RETURN n"
    result = await execute_cypher(conn, cypher, {}, graph_name)
    logger.info(f"   结果: {len(result)} 个节点创建")
    
    # 测试2: 创建带多个属性的节点
    logger.info("2. 创建带多个属性的节点")
    cypher = "MERGE (ds:DataSource {fqn: 'test_ds', name: 'Test DataSource', type: 'postgresql'}) RETURN ds"
    result = await execute_cypher(conn, cypher, {}, graph_name)
    logger.info(f"   结果: {len(result)} 个数据源节点创建")
    
    # 测试3: 创建第二个节点
    logger.info("3. 创建数据库节点")
    cypher = "MERGE (db:Database {fqn: 'test_ds.test_db', name: 'test_db'}) RETURN db"
    result = await execute_cypher(conn, cypher, {}, graph_name)
    logger.info(f"   结果: {len(result)} 个数据库节点创建")
    
    # 测试4: 创建关系（分步进行）
    logger.info("4. 创建节点间关系")
    cypher = """
    MATCH (ds:DataSource {fqn: 'test_ds'})
    MATCH (db:Database {fqn: 'test_ds.test_db'})
    MERGE (ds)-[:CONFIGURES_DATABASE]->(db)
    RETURN ds
    """
    result = await execute_cypher(conn, cypher, {}, graph_name)
    logger.info(f"   结果: {len(result)} 个关系创建")
    
    # 测试5: 验证关系
    logger.info("5. 验证关系创建")
    cypher = """
    MATCH (ds:DataSource)-[:CONFIGURES_DATABASE]->(db:Database)
    RETURN ds.name, db.name
    """
    result = await execute_cypher(conn, cypher, {}, graph_name)
    logger.info(f"   结果: {len(result)} 个关系找到")
    for row in result:
        logger.info(f"     {row}")

async def test_parameterized_queries(conn, graph_name):
    """测试参数化查询"""
    logger.info("=== 测试参数化查询 ===")
    
    # 测试1: 字符串参数
    logger.info("1. 测试字符串参数")
    cypher = "MERGE (t:Table {fqn: $fqn, name: $name}) RETURN t"
    params = {"fqn": "test_ds.test_db.test_schema.test_table", "name": "test_table"}
    result = await execute_cypher(conn, cypher, params, graph_name)
    logger.info(f"   结果: {len(result)} 个表节点创建")
    
    # 测试2: 数值参数
    logger.info("2. 测试数值参数")
    cypher = "MERGE (c:Column {fqn: $fqn, position: $pos, nullable: $nullable}) RETURN c"
    params = {"fqn": "test_ds.test_db.test_schema.test_table.id", "pos": 1, "nullable": False}
    result = await execute_cypher(conn, cypher, params, graph_name)
    logger.info(f"   结果: {len(result)} 个列节点创建")
    
    # 测试3: 空值参数
    logger.info("3. 测试空值参数")
    cypher = "MERGE (c:Column {fqn: $fqn, default_value: $default}) RETURN c"
    params = {"fqn": "test_ds.test_db.test_schema.test_table.name", "default": None}
    result = await execute_cypher(conn, cypher, params, graph_name)
    logger.info(f"   结果: {len(result)} 个列节点创建")

async def test_complex_queries(conn, graph_name):
    """测试复杂查询"""
    logger.info("=== 测试复杂查询 ===")
    
    # 测试1: 多跳路径查询
    logger.info("1. 测试多跳路径查询")
    cypher = """
    MATCH (ds:DataSource)-[:CONFIGURES_DATABASE]->(db:Database)
    -[:HAS_SCHEMA]->(s:Schema)-[:HAS_OBJECT]->(t:Table)
    RETURN ds.name, db.name, s.name, t.name
    """
    result = await execute_cypher(conn, cypher, {}, graph_name)
    logger.info(f"   结果: {len(result)} 条路径找到")
    
    # 测试2: 聚合查询
    logger.info("2. 测试聚合查询")
    cypher = """
    MATCH (t:Table)-[:HAS_COLUMN]->(c:Column)
    RETURN t.name, count(c) as column_count
    """
    result = await execute_cypher(conn, cypher, {}, graph_name)
    logger.info(f"   结果: {len(result)} 个表的列统计")
    
    # 测试3: 条件过滤查询
    logger.info("3. 测试条件过滤查询")
    cypher = """
    MATCH (c:Column)
    WHERE c.nullable = false
    RETURN c.fqn, c.position
    """
    result = await execute_cypher(conn, cypher, {}, graph_name)
    logger.info(f"   结果: {len(result)} 个非空列找到")

async def test_data_lineage_scenario(conn, graph_name):
    """测试数据血缘场景"""
    logger.info("=== 测试数据血缘场景 ===")
    
    # 创建完整的数据血缘结构
    logger.info("1. 创建数据血缘结构")
    
    # 创建数据源
    cypher = """
    MERGE (ds1:DataSource {fqn: 'source_db', name: 'Source Database', type: 'postgresql'})
    MERGE (ds2:DataSource {fqn: 'target_db', name: 'Target Database', type: 'postgresql'})
    RETURN ds1, ds2
    """
    await execute_cypher(conn, cypher, {}, graph_name)
    
    # 创建数据库和模式
    cypher = """
    MERGE (db1:Database {fqn: 'source_db.main', name: 'main'})
    MERGE (db2:Database {fqn: 'target_db.analytics', name: 'analytics'})
    MERGE (s1:Schema {fqn: 'source_db.main.public', name: 'public'})
    MERGE (s2:Schema {fqn: 'target_db.analytics.reporting', name: 'reporting'})
    RETURN db1, db2, s1, s2
    """
    await execute_cypher(conn, cypher, {}, graph_name)
    
    # 创建表和列
    cypher = """
    MERGE (t1:Table {fqn: 'source_db.main.public.users', name: 'users'})
    MERGE (t2:Table {fqn: 'target_db.analytics.reporting.user_summary', name: 'user_summary'})
    MERGE (c1:Column {fqn: 'source_db.main.public.users.id', name: 'id', type: 'integer'})
    MERGE (c2:Column {fqn: 'source_db.main.public.users.name', name: 'name', type: 'varchar'})
    MERGE (c3:Column {fqn: 'target_db.analytics.reporting.user_summary.user_id', name: 'user_id', type: 'integer'})
    MERGE (c4:Column {fqn: 'target_db.analytics.reporting.user_summary.user_name', name: 'user_name', type: 'varchar'})
    RETURN t1, t2, c1, c2, c3, c4
    """
    await execute_cypher(conn, cypher, {}, graph_name)
    
    # 创建血缘关系
    logger.info("2. 创建血缘关系")
    
    # 分别创建每个血缘关系
    cypher1 = """
    MATCH (c1:Column {fqn: 'source_db.main.public.users.id'})
    MATCH (c3:Column {fqn: 'target_db.analytics.reporting.user_summary.user_id'})
    MERGE (c1)-[:REFERENCES_COLUMN]->(c3)
    RETURN c1, c3
    """
    await execute_cypher(conn, cypher1, {}, graph_name)
    
    cypher2 = """
    MATCH (c2:Column {fqn: 'source_db.main.public.users.name'})
    MATCH (c4:Column {fqn: 'target_db.analytics.reporting.user_summary.user_name'})
    MERGE (c2)-[:REFERENCES_COLUMN]->(c4)
    RETURN c2, c4
    """
    result = await execute_cypher(conn, cypher2, {}, graph_name)
    logger.info(f"   血缘关系创建完成: 2 条关系")
    
    # 查询血缘路径
    logger.info("3. 查询血缘路径")
    cypher = """
    MATCH (source:Column)-[:REFERENCES_COLUMN]->(target:Column)
    RETURN source.fqn as source_column, target.fqn as target_column
    """
    result = await execute_cypher(conn, cypher, {}, graph_name)
    logger.info(f"   找到血缘路径: {len(result)} 条")
    for row in result:
        logger.info(f"     {row}")

async def test_age_connection():
    """测试AGE连接和所有操作"""
    settings = get_settings_instance()
    
    # 构建AGE数据库配置
    age_db_config = {
        'user': settings.INTERNAL_DB.USER,
        'password': settings.INTERNAL_DB.PASSWORD.get_secret_value(),
        'host': settings.INTERNAL_DB.HOST,
        'port': settings.INTERNAL_DB.PORT,
        'database': settings.INTERNAL_DB.DB_AGE,
    }
    
    try:
        conn = await asyncpg.connect(**age_db_config)
        logger.info("成功连接到AGE数据库")
        
        # 确保图存在
        graph_name = "test_graph"
        if await ensure_age_graph_exists(conn, graph_name):
            logger.info(f"图 {graph_name} 已准备就绪")
        else:
            logger.error(f"无法创建图 {graph_name}")
            return
        
        # 运行所有测试
        await test_basic_operations(conn, graph_name)
        await test_parameterized_queries(conn, graph_name)
        await test_complex_queries(conn, graph_name)
        await test_data_lineage_scenario(conn, graph_name)
        
        await conn.close()
        
    except Exception as e:
        logger.error(f"连接AGE数据库失败: {str(e)}")
        raise

async def main():
    """主测试函数"""
    logger.info("开始AGE 1.5.0兼容性测试")
    
    try:
        # 测试Cypher转换
        await test_cypher_conversion()
        
        # 测试AGE连接和操作
        await test_age_connection()
        
        logger.info("=== 所有测试完成 ===")
        
    except Exception as e:
        logger.error(f"测试过程中出现错误: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("用户中断测试")
        sys.exit(0)
    except Exception as e:
        logger.error(f"测试执行出错: {str(e)}")
        sys.exit(1) 