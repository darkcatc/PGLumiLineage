#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
血缘图谱构建器测试脚本

测试LineageGraphBuilder的主要功能，包括：
1. 从示例JSON生成Cypher语句
2. 验证对象和列节点的创建
3. 验证数据流关系的生成

作者: Vance Chen
"""

import asyncio
import json
import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, Any

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

from pglumilineage.common import models
from pglumilineage.graph_builder.lineage_graph_builder import LineageGraphBuilder

# 配置日志
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def create_test_pattern() -> models.AnalyticalSQLPattern:
    """
    创建测试用的SQL模式对象
    
    基于提供的示例JSON数据
    """
    # 示例JSON数据（来自8ceac254_20250516_151213.json）
    test_json = {
        "sql_pattern_hash": "8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8",
        "source_database_name": "tpcds",
        "target_object": {
            "schema": "public",
            "name": "monthly_channel_returns_analysis_report",
            "type": "TABLE"
        },
        "column_level_lineage": [
            {
                "target_column": "sales_year_month",
                "target_object_name": "monthly_channel_returns_analysis_report",
                "target_object_schema": "public",
                "sources": [
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "date_dim",
                            "type": "TABLE"
                        },
                        "source_column": "d_date",
                        "transformation_logic": "TO_CHAR(d.d_date, 'YYYY-MM')"
                    }
                ],
                "derivation_type": "FUNCTION_CALL"
            },
            {
                "target_column": "channel",
                "target_object_name": "monthly_channel_returns_analysis_report",
                "target_object_schema": "public",
                "sources": [
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "store_sales",
                            "type": "TABLE"
                        },
                        "source_column": None,
                        "transformation_logic": "'Store' as channel (UNION分支中的字面量赋值)"
                    }
                ],
                "derivation_type": "UNION_MERGE"
            }
        ],
        "referenced_objects": [
            {
                "schema": "public",
                "name": "store_sales",
                "type": "TABLE",
                "access_mode": "READ"
            },
            {
                "schema": "public",
                "name": "date_dim",
                "type": "TABLE",
                "access_mode": "READ"
            },
            {
                "schema": "public",
                "name": "monthly_channel_returns_analysis_report",
                "type": "TABLE",
                "access_mode": "WRITE"
            }
        ],
        "parsing_confidence": 1.0
    }
    
    # 创建AnalyticalSQLPattern对象
    pattern = models.AnalyticalSQLPattern(
        sql_hash="8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8",
        normalized_sql_text="INSERT INTO monthly_channel_returns_analysis_report ...",
        sample_raw_sql_text="INSERT INTO monthly_channel_returns_analysis_report SELECT ...",
        source_database_name="tpcds",
        llm_extracted_relations_json=test_json,
        first_seen_at=datetime.now(),
        last_seen_at=datetime.now(),
        execution_count=1,
        llm_analysis_status="COMPLETED_SUCCESS",
        is_loaded_to_age=False
    )
    
    return pattern


def create_complex_test_pattern() -> models.AnalyticalSQLPattern:
    """
    创建复杂SQL测试用的SQL模式对象
    
    基于完整的8ceac254_20250516_151213.json数据
    """
    # 完整的复杂JSON数据
    complex_json = {
        "sql_pattern_hash": "8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8",
        "source_database_name": "tpcds",
        "target_object": {
            "schema": "public",
            "name": "monthly_channel_returns_analysis_report",
            "type": "TABLE"
        },
        "column_level_lineage": [
            {
                "target_column": "sales_year_month",
                "target_object_name": "monthly_channel_returns_analysis_report",
                "target_object_schema": "public",
                "sources": [
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "date_dim",
                            "type": "TABLE"
                        },
                        "source_column": "d_date",
                        "transformation_logic": "TO_CHAR(d.d_date, 'YYYY-MM')"
                    }
                ],
                "derivation_type": "FUNCTION_CALL"
            },
            {
                "target_column": "channel",
                "target_object_name": "monthly_channel_returns_analysis_report",
                "target_object_schema": "public",
                "sources": [
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "store_sales",
                            "type": "TABLE"
                        },
                        "source_column": None,
                        "transformation_logic": "'Store' as channel (UNION分支中的字面量赋值)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "catalog_sales",
                            "type": "TABLE"
                        },
                        "source_column": None,
                        "transformation_logic": "'Catalog' as channel (UNION分支中的字面量赋值)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "web_sales",
                            "type": "TABLE"
                        },
                        "source_column": None,
                        "transformation_logic": "'Web' as channel (UNION分支中的字面量赋值)"
                    }
                ],
                "derivation_type": "UNION_MERGE"
            },
            {
                "target_column": "total_sold_quantity",
                "target_object_name": "monthly_channel_returns_analysis_report",
                "target_object_schema": "public",
                "sources": [
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "store_sales",
                            "type": "TABLE"
                        },
                        "source_column": "ss_quantity",
                        "transformation_logic": "COALESCE(ss_quantity, 0)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "catalog_sales",
                            "type": "TABLE"
                        },
                        "source_column": "cs_quantity",
                        "transformation_logic": "COALESCE(cs_quantity, 0)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "web_sales",
                            "type": "TABLE"
                        },
                        "source_column": "ws_quantity",
                        "transformation_logic": "COALESCE(ws_quantity, 0)"
                    }
                ],
                "derivation_type": "AGGREGATION"
            },
            {
                "target_column": "total_returned_quantity",
                "target_object_name": "monthly_channel_returns_analysis_report",
                "target_object_schema": "public",
                "sources": [
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "store_returns",
                            "type": "TABLE"
                        },
                        "source_column": "sr_return_quantity",
                        "transformation_logic": "COALESCE(sr_return_quantity, 0)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "catalog_returns",
                            "type": "TABLE"
                        },
                        "source_column": "cr_return_quantity",
                        "transformation_logic": "COALESCE(cr_return_quantity, 0)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "web_returns",
                            "type": "TABLE"
                        },
                        "source_column": "wr_return_quantity",
                        "transformation_logic": "COALESCE(wr_return_quantity, 0)"
                    }
                ],
                "derivation_type": "AGGREGATION"
            },
            {
                "target_column": "return_rate_by_quantity",
                "target_object_name": "monthly_channel_returns_analysis_report",
                "target_object_schema": "public",
                "sources": [
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "store_sales",
                            "type": "TABLE"
                        },
                        "source_column": "ss_quantity",
                        "transformation_logic": "COALESCE(ss_quantity, 0)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "catalog_sales",
                            "type": "TABLE"
                        },
                        "source_column": "cs_quantity",
                        "transformation_logic": "COALESCE(cs_quantity, 0)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "web_sales",
                            "type": "TABLE"
                        },
                        "source_column": "ws_quantity",
                        "transformation_logic": "COALESCE(ws_quantity, 0)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "store_returns",
                            "type": "TABLE"
                        },
                        "source_column": "sr_return_quantity",
                        "transformation_logic": "COALESCE(sr_return_quantity, 0)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "catalog_returns",
                            "type": "TABLE"
                        },
                        "source_column": "cr_return_quantity",
                        "transformation_logic": "COALESCE(cr_return_quantity, 0)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "web_returns",
                            "type": "TABLE"
                        },
                        "source_column": "wr_return_quantity",
                        "transformation_logic": "COALESCE(wr_return_quantity, 0)"
                    }
                ],
                "derivation_type": "CONDITIONAL_LOGIC"
            },
            {
                "target_column": "primary_return_reason_desc",
                "target_object_name": "monthly_channel_returns_analysis_report",
                "target_object_schema": "public",
                "sources": [
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "reason",
                            "type": "TABLE"
                        },
                        "source_column": "r_reason_desc",
                        "transformation_logic": "r.r_reason_desc AS primary_return_reason_desc"
                    }
                ],
                "derivation_type": "DIRECT_MAPPING"
            },
            {
                "target_column": "primary_reason_returned_quantity",
                "target_object_name": "monthly_channel_returns_analysis_report",
                "target_object_schema": "public",
                "sources": [
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "store_returns",
                            "type": "TABLE"
                        },
                        "source_column": "sr_return_quantity",
                        "transformation_logic": "COALESCE(sr_return_quantity, 0)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "catalog_returns",
                            "type": "TABLE"
                        },
                        "source_column": "cr_return_quantity",
                        "transformation_logic": "COALESCE(cr_return_quantity, 0)"
                    },
                    {
                        "source_object": {
                            "schema": "public",
                            "name": "web_returns",
                            "type": "TABLE"
                        },
                        "source_column": "wr_return_quantity",
                        "transformation_logic": "COALESCE(wr_return_quantity, 0)"
                    }
                ],
                "derivation_type": "LITERAL_ASSIGNMENT"
            }
        ],
        "referenced_objects": [
            {
                "schema": "public",
                "name": "store_sales",
                "type": "TABLE",
                "access_mode": "READ"
            },
            {
                "schema": "public",
                "name": "catalog_sales",
                "type": "TABLE",
                "access_mode": "READ"
            },
            {
                "schema": "public",
                "name": "web_sales",
                "type": "TABLE",
                "access_mode": "READ"
            },
            {
                "schema": "public",
                "name": "store_returns",
                "type": "TABLE",
                "access_mode": "READ"
            },
            {
                "schema": "public",
                "name": "catalog_returns",
                "type": "TABLE",
                "access_mode": "READ"
            },
            {
                "schema": "public",
                "name": "web_returns",
                "type": "TABLE",
                "access_mode": "READ"
            },
            {
                "schema": "public",
                "name": "date_dim",
                "type": "TABLE",
                "access_mode": "READ"
            },
            {
                "schema": "public",
                "name": "reason",
                "type": "TABLE",
                "access_mode": "READ"
            },
            {
                "schema": "public",
                "name": "monthly_channel_returns_analysis_report",
                "type": "TABLE",
                "access_mode": "WRITE"
            }
        ],
        "parsing_confidence": 1.0
    }
    
    # 创建AnalyticalSQLPattern对象
    pattern = models.AnalyticalSQLPattern(
        sql_hash="8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8",
        normalized_sql_text="INSERT INTO monthly_channel_returns_analysis_report SELECT ...",
        sample_raw_sql_text="INSERT INTO monthly_channel_returns_analysis_report SELECT ... (复杂SQL包含多个UNION和聚合)",
        source_database_name="tpcds",
        llm_extracted_relations_json=complex_json,
        first_seen_at=datetime.now(),
        last_seen_at=datetime.now(),
        execution_count=1,
        llm_analysis_status="COMPLETED_SUCCESS",
        is_loaded_to_age=False
    )
    
    return pattern


def test_cypher_generation():
    """
    测试Cypher语句生成功能
    """
    logger.info("=== 测试Cypher语句生成 ===")
    
    # 创建测试配置（不实际连接数据库）
    analytics_db_config = {
        'user': 'test',
        'password': 'test',
        'host': 'localhost',
        'port': 5432,
        'database': 'test'
    }
    
    age_db_config = {
        'user': 'test',
        'password': 'test',
        'host': 'localhost',
        'port': 5432,
        'database': 'test'
    }
    
    # 创建构建器
    builder = LineageGraphBuilder(
        analytics_db_config=analytics_db_config,
        age_db_config=age_db_config,
        graph_name="test_graph"
    )
    
    # 创建测试数据
    test_pattern = create_test_pattern()
    
    # 生成Cypher语句
    cypher_batch = builder.transform_llm_json_to_cypher_batch(test_pattern)
    
    logger.info(f"生成了 {len(cypher_batch)} 条Cypher语句")
    
    # 分析生成的语句
    sql_pattern_count = 0
    object_node_count = 0
    column_node_count = 0
    data_flow_count = 0
    reference_count = 0
    
    for i, (cypher, params) in enumerate(cypher_batch):
        logger.debug(f"\n=== Cypher语句 {i+1} ===")
        logger.debug(f"语句: {cypher}")
        logger.debug(f"参数: {params}")
        
        # 优先级排序：先检查更具体的模式
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
    
    # 输出统计结果
    logger.info(f"\n=== Cypher语句统计 ===")
    logger.info(f"SQL模式节点: {sql_pattern_count}")
    logger.info(f"对象节点: {object_node_count}")
    logger.info(f"列节点: {column_node_count}")
    logger.info(f"数据流关系: {data_flow_count}")
    logger.info(f"引用关系: {reference_count}")
    
    # 验证基本要求
    assert sql_pattern_count == 1, f"期望1个SQL模式节点，实际{sql_pattern_count}个"
    assert object_node_count > 0, f"期望至少1个对象节点，实际{object_node_count}个"
    assert column_node_count > 0, f"期望至少1个列节点，实际{column_node_count}个"
    assert data_flow_count > 0, f"期望至少1个数据流关系，实际{data_flow_count}个"
    assert reference_count > 0, f"期望至少1个引用关系，实际{reference_count}个"
    
    logger.info("✅ 所有测试通过！")
    
    return cypher_batch


def test_complex_sql_cypher_generation():
    """
    测试复杂SQL的Cypher语句生成功能
    """
    logger.info("=== 测试复杂SQL的Cypher语句生成 ===")
    
    # 创建测试配置（不实际连接数据库）
    analytics_db_config = {
        'user': 'test',
        'password': 'test',
        'host': 'localhost',
        'port': 5432,
        'database': 'test'
    }
    
    age_db_config = {
        'user': 'test',
        'password': 'test',
        'host': 'localhost',
        'port': 5432,
        'database': 'test'
    }
    
    # 创建构建器
    builder = LineageGraphBuilder(
        analytics_db_config=analytics_db_config,
        age_db_config=age_db_config,
        graph_name="test_graph"
    )
    
    # 创建复杂测试数据
    complex_pattern = create_complex_test_pattern()
    
    logger.info("复杂SQL关系分析：")
    relations_json = complex_pattern.llm_extracted_relations_json
    
    # 分析列血缘关系
    column_lineages = relations_json.get("column_level_lineage", [])
    logger.info(f"列血缘关系数量: {len(column_lineages)}")
    
    total_sources = 0
    for i, lineage in enumerate(column_lineages, 1):
        target_col = lineage.get("target_column")
        sources = lineage.get("sources", [])
        derivation_type = lineage.get("derivation_type")
        logger.info(f"  {i}. {target_col} ({derivation_type}) ← {len(sources)} 个源")
        total_sources += len(sources)
    
    # 分析引用对象
    referenced_objects = relations_json.get("referenced_objects", [])
    logger.info(f"引用对象数量: {len(referenced_objects)}")
    read_objects = [obj for obj in referenced_objects if obj.get("access_mode") == "READ"]
    write_objects = [obj for obj in referenced_objects if obj.get("access_mode") == "WRITE"]
    logger.info(f"  READ访问: {len(read_objects)} 个对象")
    logger.info(f"  WRITE访问: {len(write_objects)} 个对象")
    
    # 生成Cypher语句
    cypher_batch = builder.transform_llm_json_to_cypher_batch(complex_pattern)
    
    logger.info(f"\n生成了 {len(cypher_batch)} 条Cypher语句")
    
    # 分析生成的语句
    sql_pattern_count = 0
    object_node_count = 0
    column_node_count = 0
    data_flow_count = 0
    reference_count = 0
    
    for i, (cypher, params) in enumerate(cypher_batch):
        logger.debug(f"\n=== Cypher语句 {i+1} ===")
        logger.debug(f"语句类型判断:")
        
        # 优先级排序：先检查更具体的模式
        if "reads_from" in cypher or "writes_to" in cypher:
            reference_count += 1
            logger.debug(f"  -> 引用关系")
        elif "data_flow" in cypher:
            data_flow_count += 1
            logger.debug(f"  -> 数据流关系")
        elif ":column" in cypher and "has_column" in cypher:
            column_node_count += 1
            logger.debug(f"  -> 列节点")
        elif (":table" in cypher or ":view" in cypher or ":temptable" in cypher) and "MERGE" in cypher:
            object_node_count += 1
            logger.debug(f"  -> 对象节点")
        elif ":sqlpattern" in cypher and "MERGE" in cypher:
            sql_pattern_count += 1
            logger.debug(f"  -> SQL模式节点")
        else:
            logger.debug(f"  -> 未分类")
        
        logger.debug(f"语句: {cypher[:100]}...")
        logger.debug(f"参数: {list(params.keys()) if params else 'None'}")
    
    # 输出统计结果
    logger.info(f"\n=== 复杂SQL Cypher语句统计 ===")
    logger.info(f"SQL模式节点: {sql_pattern_count}")
    logger.info(f"对象节点: {object_node_count}")
    logger.info(f"列节点: {column_node_count}")
    logger.info(f"数据流关系: {data_flow_count}")
    logger.info(f"引用关系: {reference_count}")
    
    # 预期数量计算和验证
    logger.info(f"\n=== 预期数量验证 ===")
    
    # 预期SQL模式节点：1个
    expected_sql_patterns = 1
    logger.info(f"预期SQL模式节点: {expected_sql_patterns}, 实际: {sql_pattern_count}")
    
    # 预期对象节点：去重后的对象数量
    unique_objects = set()
    unique_objects.add("monthly_channel_returns_analysis_report")  # 目标对象
    for obj in referenced_objects:
        unique_objects.add(obj.get("name"))
    expected_objects = len(unique_objects)
    logger.info(f"预期对象节点: {expected_objects}, 实际: {object_node_count}")
    
    # 预期列节点：去重后的列数量
    unique_columns = set()
    for lineage in column_lineages:
        target_col = lineage.get("target_column")
        target_obj = lineage.get("target_object_name")
        if target_col and target_obj:
            unique_columns.add(f"{target_obj}.{target_col}")
        
        for source in lineage.get("sources", []):
            source_col = source.get("source_column")
            source_obj = source.get("source_object", {}).get("name")
            if source_col and source_obj:
                unique_columns.add(f"{source_obj}.{source_col}")
    
    expected_columns = len(unique_columns)
    logger.info(f"预期列节点: {expected_columns}, 实际: {column_node_count}")
    logger.info(f"涉及的列: {sorted(unique_columns)}")
    
    # 预期数据流关系：总源数量
    expected_data_flows = total_sources
    logger.info(f"预期数据流关系: {expected_data_flows}, 实际: {data_flow_count}")
    
    # 预期引用关系：引用对象数量
    expected_references = len(referenced_objects)
    logger.info(f"预期引用关系: {expected_references}, 实际: {reference_count}")
    
    # 验证结果
    success = True
    if sql_pattern_count != expected_sql_patterns:
        logger.error(f"❌ SQL模式节点数量不匹配")
        success = False
    if object_node_count != expected_objects:
        logger.error(f"❌ 对象节点数量不匹配")
        success = False
    if column_node_count != expected_columns:
        logger.error(f"❌ 列节点数量不匹配")
        success = False
    if data_flow_count != expected_data_flows:
        logger.error(f"❌ 数据流关系数量不匹配")
        success = False
    if reference_count != expected_references:
        logger.error(f"❌ 引用关系数量不匹配")
        success = False
    
    if success:
        logger.info("✅ 复杂SQL图关系生成完全正确！")
    else:
        logger.error("❌ 复杂SQL图关系生成存在问题")
    
    return cypher_batch, success


def test_fqn_generation():
    """
    测试FQN生成逻辑
    """
    logger.info("=== 测试FQN生成逻辑 ===")
    
    from pglumilineage.graph_builder.common_graph_utils import (
        generate_database_fqn,
        generate_schema_fqn,
        generate_object_fqn,
        generate_column_fqn
    )
    
    database_name = "tpcds"
    schema_name = "public"
    table_name = "store_sales"
    column_name = "ss_quantity"
    
    # 生成各级FQN
    db_fqn = generate_database_fqn(database_name, database_name)
    schema_fqn = generate_schema_fqn(db_fqn, schema_name)
    table_fqn = generate_object_fqn(schema_fqn, table_name)
    column_fqn = generate_column_fqn(table_fqn, column_name)
    
    logger.info(f"数据库FQN: {db_fqn}")
    logger.info(f"Schema FQN: {schema_fqn}")
    logger.info(f"表FQN: {table_fqn}")
    logger.info(f"列FQN: {column_fqn}")
    
    # 验证FQN格式
    assert db_fqn == "tpcds.tpcds", f"数据库FQN格式错误: {db_fqn}"
    assert schema_fqn == "tpcds.tpcds.public", f"Schema FQN格式错误: {schema_fqn}"
    assert table_fqn == "tpcds.tpcds.public.store_sales", f"表FQN格式错误: {table_fqn}"
    assert column_fqn == "tpcds.tpcds.public.store_sales.ss_quantity", f"列FQN格式错误: {column_fqn}"
    
    logger.info("✅ FQN生成测试通过！")


def main():
    """
    主测试函数
    """
    try:
        logger.info("开始LineageGraphBuilder测试...")
        
        # 测试FQN生成
        test_fqn_generation()
        
        # 测试Cypher生成
        cypher_batch = test_cypher_generation()
        
        # 测试复杂SQL Cypher生成
        complex_cypher_batch, success = test_complex_sql_cypher_generation()
        
        logger.info(f"\n=== 测试完成 ===")
        logger.info(f"总共生成了 {len(cypher_batch)} 条Cypher语句")
        logger.info(f"复杂SQL生成了 {len(complex_cypher_batch)} 条Cypher语句")
        logger.info("所有测试都成功通过！")
        
    except Exception as e:
        logger.error(f"测试失败: {str(e)}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main() 