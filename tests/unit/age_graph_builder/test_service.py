#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AGE图谱构建器服务测试

此模块用于测试AGE图谱构建器的核心功能，包括将LLM提取的关系转换为Cypher语句。

作者: Vance Chen
"""

import os
import json
import unittest
from datetime import datetime
from typing import Dict, Any, List

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pglumilineage.common import models
from pglumilineage.age_graph_builder import service as age_builder_service


class TestAgeGraphBuilderService(unittest.TestCase):
    """AGE图谱构建器服务测试类"""

    def setUp(self):
        """测试前的准备工作"""
        # 测试数据文件路径
        self.test_data_path = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
            'data', 'llm', 'relations', '8ceac254_20250516_151213.json'
        )
        
        # 确保测试数据文件存在
        self.assertTrue(os.path.exists(self.test_data_path), f"测试数据文件不存在: {self.test_data_path}")
        
        # 加载测试数据
        with open(self.test_data_path, 'r', encoding='utf-8') as f:
            self.test_relations_json = json.load(f)
        
        # 创建测试用的AnalyticalSQLPattern对象
        self.test_pattern = models.AnalyticalSQLPattern(
            sql_hash="8ceac2546d35e6f8a2bccba875a63e42421836e92171e76de9ee33b24f238fb8",
            normalized_sql_text="SELECT /* 规范化后的SQL */ ...",
            sample_raw_sql_text="SELECT /* 原始SQL示例 */ ...",
            source_database_name="tpcds",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now(),
            execution_count=1,
            total_duration_ms=0,
            avg_duration_ms=0.0,
            max_duration_ms=0,
            min_duration_ms=0,
            llm_analysis_status="COMPLETED",
            llm_extracted_relations_json=self.test_relations_json
        )

    def test_transform_json_to_cypher(self):
        """测试transform_json_to_cypher函数"""
        # 调用被测函数
        cypher_statements = age_builder_service.transform_json_to_cypher(self.test_pattern)
        
        # 基本验证
        self.assertIsInstance(cypher_statements, list, "返回值应该是列表")
        self.assertTrue(len(cypher_statements) > 0, "应该生成至少一条Cypher语句")
        
        # 验证生成的Cypher语句
        # 1. 验证数据库节点创建语句
        db_statement_found = False
        for stmt in cypher_statements:
            if "MERGE (db:Database" in stmt and "tpcds" in stmt:
                db_statement_found = True
                break
        self.assertTrue(db_statement_found, "应该包含创建数据库节点的Cypher语句")
        
        # 2. 验证Schema节点创建语句
        schema_statement_found = False
        for stmt in cypher_statements:
            if "MERGE (schema:Schema" in stmt and "public" in stmt:
                schema_statement_found = True
                break
        self.assertTrue(schema_statement_found, "应该包含创建Schema节点的Cypher语句")
        
        # 3. 验证表节点创建语句
        table_statement_found = False
        for stmt in cypher_statements:
            if ("MERGE (table:Table" in stmt or "MERGE (view:View" in stmt) and "monthly_channel_returns_analysis_report" in stmt:
                table_statement_found = True
                break
        self.assertTrue(table_statement_found, "应该包含创建表/视图节点的Cypher语句")
        
        # 4. 验证列节点创建语句
        column_statement_found = False
        for stmt in cypher_statements:
            if "MERGE (tgt_col:Column" in stmt and "channel" in stmt:
                column_statement_found = True
                break
        self.assertTrue(column_statement_found, "应该包含创建列节点的Cypher语句")
        
        # 5. 验证SQL模式节点创建语句
        sql_pattern_statement_found = False
        for stmt in cypher_statements:
            if "MERGE (sp:SqlPattern" in stmt and self.test_pattern.sql_hash in stmt:
                sql_pattern_statement_found = True
                break
        self.assertTrue(sql_pattern_statement_found, "应该包含创建SQL模式节点的Cypher语句")
        
        # 6. 验证数据流关系创建语句
        data_flow_statement_found = False
        for stmt in cypher_statements:
            if "MERGE" in stmt and "DATA_FLOW" in stmt:
                data_flow_statement_found = True
                break
        self.assertTrue(data_flow_statement_found, "应该包含创建数据流关系的Cypher语句")
        
        # 7. 验证SQL模式与对象的引用关系创建语句
        reference_statement_found = False
        for stmt in cypher_statements:
            if "MERGE (sp)-[:READS_FROM]->(obj)" in stmt or "MERGE (sp)-[:WRITES_TO]->(obj)" in stmt:
                reference_statement_found = True
                break
        self.assertTrue(reference_statement_found, "应该包含创建SQL模式与对象引用关系的Cypher语句")
        
        # 输出生成的Cypher语句，便于查看
        print(f"\n共生成 {len(cypher_statements)} 条Cypher语句:")
        for i, stmt in enumerate(cypher_statements):
            if isinstance(stmt, dict):
                print(f"{i+1}. {stmt['query']} (带参数)")
            else:
                print(f"{i+1}. {stmt}")

    def test_transform_json_to_cypher_with_empty_relations(self):
        """测试transform_json_to_cypher函数处理空关系的情况"""
        # 创建没有关系JSON的测试对象
        empty_pattern = models.AnalyticalSQLPattern(
            sql_hash="empty_hash",
            normalized_sql_text="SELECT 1",
            sample_raw_sql_text="SELECT 1",
            source_database_name="test_db",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now(),
            execution_count=1,
            total_duration_ms=0,
            avg_duration_ms=0.0,
            max_duration_ms=0,
            min_duration_ms=0,
            llm_analysis_status="PENDING",
            llm_extracted_relations_json=None
        )
        
        # 调用被测函数
        cypher_statements = age_builder_service.transform_json_to_cypher(empty_pattern)
        
        # 验证结果
        self.assertIsInstance(cypher_statements, list, "返回值应该是列表")
        self.assertEqual(len(cypher_statements), 0, "空关系应该返回空列表")

    def test_special_cases_handling(self):
        """测试特殊情况处理"""
        # 创建一个包含特殊情况的测试关系JSON
        special_relations_json = {
            "column_level_lineage": [
                {
                    "target_column": "special_column",
                    "target_object_name": "special_table",
                    "target_object_schema": "public",
                    "sources": [
                        {
                            "source_object": {
                                "schema": "public",
                                "name": "source_table",
                                "type": "TABLE"
                            },
                            "source_column": None,  # 字面量或表达式
                            "transformation_logic": "'Literal Value' as special_column"
                        }
                    ],
                    "derivation_type": "LITERAL_ASSIGNMENT"
                }
            ],
            "referenced_objects": [
                {
                    "schema": "public",
                    "name": "source_table",
                    "type": "TABLE",
                    "access_mode": "READ"
                },
                {
                    "schema": "public",
                    "name": "special_table",
                    "type": "TABLE",
                    "access_mode": "WRITE"
                }
            ]
        }
        
        # 创建测试对象
        special_pattern = models.AnalyticalSQLPattern(
            sql_hash="special_hash",
            normalized_sql_text="SELECT 'Literal Value' as special_column",
            sample_raw_sql_text="SELECT 'Literal Value' as special_column",
            source_database_name="test_db",
            first_seen_at=datetime.now(),
            last_seen_at=datetime.now(),
            execution_count=1,
            total_duration_ms=0,
            avg_duration_ms=0.0,
            max_duration_ms=0,
            min_duration_ms=0,
            llm_analysis_status="COMPLETED",
            llm_extracted_relations_json=special_relations_json
        )
        
        # 调用被测函数
        cypher_statements = age_builder_service.transform_json_to_cypher(special_pattern)
        
        # 验证结果
        self.assertIsInstance(cypher_statements, list, "返回值应该是列表")
        self.assertTrue(len(cypher_statements) > 0, "应该生成至少一条Cypher语句")
        
        # 验证字面量处理
        literal_handling_found = False
        for stmt in cypher_statements:
            if isinstance(stmt, str) and "MATCH (src_obj)" in stmt and "MATCH (tgt_col:Column" in stmt and "MERGE (src_obj)-[df:DATA_FLOW" in stmt and "special_column" in stmt:
                literal_handling_found = True
                break
        self.assertTrue(literal_handling_found, "应该包含处理字面量的Cypher语句")


if __name__ == '__main__':
    unittest.main()
