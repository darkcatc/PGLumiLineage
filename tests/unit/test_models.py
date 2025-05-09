#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据模型单元测试

测试Pydantic数据模型的验证和序列化功能。

作者: Vance Chen
"""

from datetime import datetime
import json

import pytest
from pydantic import ValidationError

from pglumilineage.common.models import RawSQLLog, AnalyticalSQLPattern, LLMAnalysisStatus


class TestRawSQLLog:
    """RawSQLLog模型测试"""
    
    def test_create_raw_sql_log(self):
        """测试创建原始SQL日志模型"""
        log_time = datetime.now()
        raw_log = RawSQLLog(
            log_id=1,
            log_time=log_time,
            username="test_user",
            database_name="test_db",
            client_addr="127.0.0.1",
            application_name="test_app",
            duration_ms=100,
            raw_sql_text="SELECT * FROM test_table",
            normalized_sql_hash="abc123",
            is_processed_for_analysis=False
        )
        
        # 验证字段值
        assert raw_log.log_id == 1
        assert raw_log.log_time == log_time
        assert raw_log.username == "test_user"
        assert raw_log.database_name == "test_db"
        assert raw_log.client_addr == "127.0.0.1"
        assert raw_log.application_name == "test_app"
        assert raw_log.duration_ms == 100
        assert raw_log.raw_sql_text == "SELECT * FROM test_table"
        assert raw_log.normalized_sql_hash == "abc123"
        assert raw_log.is_processed_for_analysis is False
    
    def test_raw_sql_log_optional_fields(self):
        """测试原始SQL日志模型的可选字段"""
        raw_log = RawSQLLog(
            log_id=1,
            log_time=datetime.now(),
            duration_ms=100,
            raw_sql_text="SELECT * FROM test_table"
        )
        
        # 验证可选字段默认值
        assert raw_log.username is None
        assert raw_log.database_name is None
        assert raw_log.client_addr is None
        assert raw_log.application_name is None
        assert raw_log.normalized_sql_hash is None
        assert raw_log.is_processed_for_analysis is False
    
    def test_raw_sql_log_validation(self):
        """测试原始SQL日志模型的验证"""
        # 缺少必填字段
        with pytest.raises(ValidationError):
            RawSQLLog(
                log_id=1,
                # 缺少log_time
                duration_ms=100,
                # 缺少raw_sql_text
            )
        
        # 类型错误
        with pytest.raises(ValidationError):
            RawSQLLog(
                log_id="not_an_int",  # 应该是int
                log_time=datetime.now(),
                duration_ms=100,
                raw_sql_text="SELECT * FROM test_table"
            )


class TestAnalyticalSQLPattern:
    """AnalyticalSQLPattern模型测试"""
    
    def test_create_analytical_sql_pattern(self):
        """测试创建分析SQL模式模型"""
        now = datetime.now()
        pattern = AnalyticalSQLPattern(
            sql_hash="abc123",
            normalized_sql_text="SELECT * FROM table",
            sample_raw_sql_text="SELECT * FROM my_table",
            first_seen_at=now,
            last_seen_at=now,
            execution_count=10,
            total_duration_ms=1000,
            avg_duration_ms=100.0,
            max_duration_ms=200,
            min_duration_ms=50,
            llm_analysis_status=LLMAnalysisStatus.COMPLETED,
            llm_extracted_relations_json={"tables": ["my_table"], "columns": ["*"]},
            last_llm_analysis_at=now
        )
        
        # 验证字段值
        assert pattern.sql_hash == "abc123"
        assert pattern.normalized_sql_text == "SELECT * FROM table"
        assert pattern.sample_raw_sql_text == "SELECT * FROM my_table"
        assert pattern.first_seen_at == now
        assert pattern.last_seen_at == now
        assert pattern.execution_count == 10
        assert pattern.total_duration_ms == 1000
        assert pattern.avg_duration_ms == 100.0
        assert pattern.max_duration_ms == 200
        assert pattern.min_duration_ms == 50
        assert pattern.llm_analysis_status == LLMAnalysisStatus.COMPLETED
        assert pattern.llm_extracted_relations_json == {"tables": ["my_table"], "columns": ["*"]}
        assert pattern.last_llm_analysis_at == now
    
    def test_analytical_sql_pattern_defaults(self):
        """测试分析SQL模式模型的默认值"""
        now = datetime.now()
        pattern = AnalyticalSQLPattern(
            sql_hash="abc123",
            normalized_sql_text="SELECT * FROM table",
            sample_raw_sql_text="SELECT * FROM my_table",
            first_seen_at=now,
            last_seen_at=now,
            total_duration_ms=1000,
            avg_duration_ms=100.0,
            max_duration_ms=200,
            min_duration_ms=50
        )
        
        # 验证默认值
        assert pattern.execution_count == 1
        assert pattern.llm_analysis_status == LLMAnalysisStatus.PENDING
        assert pattern.llm_extracted_relations_json is None
        assert pattern.last_llm_analysis_at is None
    
    def test_analytical_sql_pattern_serialization(self):
        """测试分析SQL模式模型的序列化"""
        now = datetime.now()
        pattern = AnalyticalSQLPattern(
            sql_hash="abc123",
            normalized_sql_text="SELECT * FROM table",
            sample_raw_sql_text="SELECT * FROM my_table",
            first_seen_at=now,
            last_seen_at=now,
            total_duration_ms=1000,
            avg_duration_ms=100.0,
            max_duration_ms=200,
            min_duration_ms=50,
            llm_extracted_relations_json={"tables": ["my_table"], "columns": ["*"]}
        )
        
        # 序列化为JSON
        json_str = pattern.model_dump_json()
        data = json.loads(json_str)
        
        # 验证序列化结果
        assert data["sql_hash"] == "abc123"
        assert data["normalized_sql_text"] == "SELECT * FROM table"
        assert data["llm_analysis_status"] == LLMAnalysisStatus.PENDING
        assert data["llm_extracted_relations_json"] == {"tables": ["my_table"], "columns": ["*"]}
        
        # 验证datetime字段的序列化
        assert "first_seen_at" in data
        assert "last_seen_at" in data
