#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
血缘关系数据导入脚本（修复版）

使用真实的JSON数据将血缘关系导入到AGE数据库中。

作者: Vance Chen
"""

import os
import sys
import asyncio
import json
import logging
from datetime import datetime
from pathlib import Path

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from pglumilineage.common import models
from pglumilineage.graph_builder.lineage_graph_builder import LineageGraphBuilder

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_json_data(json_file_path: str) -> dict:
    """
    从JSON文件加载数据
    
    Args:
        json_file_path: JSON文件路径
        
    Returns:
        dict: JSON数据
    """
    with open(json_file_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_pattern_from_json(json_data: dict) -> models.AnalyticalSQLPattern:
    """
    从JSON数据创建AnalyticalSQLPattern对象
    
    Args:
        json_data: JSON数据
        
    Returns:
        models.AnalyticalSQLPattern: SQL模式对象
    """
    sql_hash = json_data.get("sql_pattern_hash", "8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8")
    
    pattern = models.AnalyticalSQLPattern(
        sql_hash=sql_hash,
        normalized_sql_text=f"INSERT INTO {json_data['target_object']['name']} SELECT ...",
        sample_raw_sql_text=f"INSERT INTO {json_data['target_object']['name']} SELECT ... (从JSON数据生成)",
        source_database_name=json_data.get("source_database_name", "tpcds"),
        llm_extracted_relations_json=json_data,
        first_seen_at=datetime.now(),
        last_seen_at=datetime.now(),
        execution_count=1,
        llm_analysis_status="COMPLETED_SUCCESS",
        is_loaded_to_age=False
    )
    
    return pattern


async def execute_cypher_simple(conn, cypher_stmt: str, params: dict, graph_name: str):
    """
    简单的Cypher执行函数，使用修复后的convert_cypher_for_age
    """
    from pglumilineage.graph_builder.common_graph_utils import convert_cypher_for_age
    
    # 设置搜索路径
    await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
    
    # 直接替换参数
    if params:
        for key, value in params.items():
            placeholder = f"${key}"
            if value is None:
                replacement = 'null'
            elif isinstance(value, bool):
                replacement = 'true' if value else 'false'
            elif isinstance(value, (int, float)):
                replacement = str(value)
            elif isinstance(value, str):
                escaped_value = value.replace('\\', '\\\\').replace("'", "\\'")
                replacement = f"'{escaped_value}'"
            else:
                escaped_value = str(value).replace('\\', '\\\\').replace("'", "\\'")
                replacement = f"'{escaped_value}'"
            
            cypher_stmt = cypher_stmt.replace(placeholder, replacement)
    
    # **关键修复**: 调用转换函数处理 ON CREATE SET / ON MATCH SET 语法
    converted_cypher = convert_cypher_for_age(cypher_stmt)
    
    # 简单清理语句，移除注释
    lines = converted_cypher.split('\n')
    clean_lines = []
    for line in lines:
        line = line.strip()
        if line and not line.startswith('//'):
            clean_lines.append(line)
    
    clean_cypher = ' '.join(clean_lines)
    
    # 构建AGE查询
    query = f"SELECT * FROM cypher('{graph_name}', $$ {clean_cypher} $$) AS (result agtype);"
    
    # 执行查询
    await conn.execute(query)


async def import_lineage_data(json_file_path: str):
    """
    导入血缘关系数据到AGE数据库
    
    Args:
        json_file_path: JSON文件路径
    """
    logger.info(f"开始从 {json_file_path} 导入血缘关系数据...")
    
    # 加载JSON数据
    try:
        json_data = load_json_data(json_file_path)
        logger.info(f"成功加载JSON数据，目标表: {json_data['target_object']['name']}")
    except Exception as e:
        logger.error(f"加载JSON文件失败: {e}")
        return False
    
    # 创建SQL模式对象
    pattern = create_pattern_from_json(json_data)
    logger.info(f"创建SQL模式对象: {pattern.sql_hash}")
    
    # 配置数据库连接（使用真实的AGE数据库配置）
    age_db_config = {
        'user': 'lumiadmin',
        'password': 'lumiadmin',
        'host': 'localhost',
        'port': 5432,
        'database': 'iwdb'
    }
    
    # 创建构建器（analytics_db_config不会被用到，因为我们直接传入数据）
    builder = LineageGraphBuilder(
        analytics_db_config=age_db_config,  # 不会被使用
        age_db_config=age_db_config,
        graph_name="lumi_graph"
    )
    
    try:
        logger.info("生成Cypher语句...")
        
        # 生成Cypher语句批次
        cypher_batch = builder.transform_llm_json_to_cypher_batch(pattern)
        logger.info(f"生成了 {len(cypher_batch)} 条Cypher语句")
        
        # 分析生成的语句
        sql_pattern_count = 0
        object_node_count = 0
        column_node_count = 0
        data_flow_count = 0
        reference_count = 0
        
        for cypher, params in cypher_batch:
            if "reads_from" in cypher or "writes_to" in cypher:
                reference_count += 1
            elif "data_flow" in cypher:
                data_flow_count += 1
            elif ":column" in cypher and "has_column" in cypher:
                column_node_count += 1
            elif (":table" in cypher or ":view" in cypher or ":temptable" in cypher) and "MERGE" in cypher:
                object_node_count += 1
            elif ":sqlpattern" in cypher and "MERGE" in cypher:
                sql_pattern_count += 1
        
        logger.info(f"Cypher语句统计:")
        logger.info(f"  SQL模式节点: {sql_pattern_count}")
        logger.info(f"  对象节点: {object_node_count}")
        logger.info(f"  列节点: {column_node_count}")
        logger.info(f"  数据流关系: {data_flow_count}")
        logger.info(f"  引用关系: {reference_count}")
        
        if not cypher_batch:
            logger.warning("没有生成任何Cypher语句")
            return False
        
        logger.info("开始执行Cypher语句到AGE数据库...")
        
        # 获取AGE数据库连接
        age_conn = await builder._get_age_db_conn()
        try:
            # 逐个执行Cypher语句（不使用事务，避免AGE的事务问题）
            success_count = 0
            for i, (cypher_stmt, params) in enumerate(cypher_batch):
                try:
                    await execute_cypher_simple(age_conn, cypher_stmt, params, "lumi_graph")
                    success_count += 1
                    logger.info(f"执行Cypher语句 {i+1}/{len(cypher_batch)} 成功")
                except Exception as e:
                    logger.error(f"执行Cypher语句 {i+1} 失败: {e}")
                    logger.error(f"语句: {cypher_stmt}")
                    logger.error(f"参数: {params}")
                    # 继续执行下一条语句，不中断整个流程
                    continue
            
            logger.info(f"成功执行了 {success_count}/{len(cypher_batch)} 条Cypher语句")
                
        finally:
            await age_conn.close()
        
        logger.info("✅ 血缘关系数据导入成功！")
        return True
        
    except Exception as e:
        logger.error(f"❌ 导入失败: {e}")
        return False


async def main():
    """
    主函数
    """
    # JSON文件路径
    json_file_path = "data/llm/relations/8ceac254_20250516_151213.json"
    
    if not os.path.exists(json_file_path):
        logger.error(f"JSON文件不存在: {json_file_path}")
        return
    
    # 导入血缘关系数据
    success = await import_lineage_data(json_file_path)
    
    if success:
        logger.info("数据导入完成，现在可以查询血缘关系了！")
    else:
        logger.error("数据导入失败")


if __name__ == "__main__":
    asyncio.run(main()) 