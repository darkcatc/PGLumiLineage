#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AGE图谱构建器测试配置

此模块提供AGE图谱构建器测试所需的共享夹具和配置。

作者: Vance Chen
"""

import os
import json
import pytest
from datetime import datetime
from typing import Dict, Any

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pglumilineage.common import models


@pytest.fixture
def test_data_path():
    """返回测试数据文件路径"""
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
        'data', 'llm', 'relations', '8ceac254_20250516_151213.json'
    )


@pytest.fixture
def test_relations_json(test_data_path):
    """加载测试关系JSON数据"""
    with open(test_data_path, 'r', encoding='utf-8') as f:
        return json.load(f)


@pytest.fixture
def test_pattern(test_relations_json):
    """创建测试用的AnalyticalSQLPattern对象"""
    return models.AnalyticalSQLPattern(
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
        llm_extracted_relations_json=test_relations_json
    )


@pytest.fixture
def special_relations_json():
    """创建一个包含特殊情况的测试关系JSON"""
    return {
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


@pytest.fixture
def special_pattern(special_relations_json):
    """创建包含特殊情况的测试模式"""
    return models.AnalyticalSQLPattern(
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


@pytest.fixture
def empty_pattern():
    """创建没有关系JSON的测试对象"""
    return models.AnalyticalSQLPattern(
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
