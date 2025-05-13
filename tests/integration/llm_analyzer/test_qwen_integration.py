#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLM 分析器模块集成测试

该模块测试 LLM 分析器与 Qwen 模型的实际集成情况。
注意：这些测试会实际调用 Qwen API，可能会产生费用。

作者: Vance Chen
"""

import asyncio
import unittest
import pytest
from typing import Dict, List, Optional

from pglumilineage.common import config
from pglumilineage.llm_analyzer.service import (
    call_qwen_api,
    construct_prompt_for_qwen,
    parse_llm_response,
    analyze_sql_pattern_with_llm
)


@pytest.mark.integration
class TestQwenIntegration(unittest.IsolatedAsyncioTestCase):
    """测试与 Qwen 模型的实际集成"""
    
    @pytest.mark.skipif(True, reason="集成测试，会实际调用 API 并产生费用")
    async def test_call_qwen_api_integration(self):
        """测试实际调用 Qwen API"""
        # 构造简单的测试消息
        messages = [
            {"role": "system", "content": "你是一个SQL分析助手。"},
            {"role": "user", "content": "分析这个SQL: SELECT * FROM users WHERE id = 1"}
        ]
        
        # 调用 Qwen API
        response = await call_qwen_api(messages)
        
        # 检查响应
        self.assertIsNotNone(response)
        self.assertTrue(len(response) > 0)
        print(f"\n实际 Qwen API 响应: {response[:100]}...")
    
    @pytest.mark.skipif(True, reason="集成测试，会实际调用 API 并产生费用")
    async def test_analyze_sql_pattern_integration(self):
        """测试使用 LLM 分析 SQL 模式的完整流程"""
        # 测试 SQL
        sql = """
        WITH user_data AS (
            SELECT id, name, email FROM users WHERE status = 'active'
        )
        INSERT INTO user_reports (user_id, report_date, email)
        SELECT id, CURRENT_DATE, email FROM user_data
        """
        
        # 元数据上下文
        metadata_context = {
            "tables": [
                {
                    "name": "users",
                    "schema": "public",
                    "columns": [
                        {"name": "id", "type": "integer"},
                        {"name": "name", "type": "text"},
                        {"name": "email", "type": "text"},
                        {"name": "status", "type": "text"}
                    ]
                },
                {
                    "name": "user_reports",
                    "schema": "public",
                    "columns": [
                        {"name": "id", "type": "integer"},
                        {"name": "user_id", "type": "integer"},
                        {"name": "report_date", "type": "date"},
                        {"name": "email", "type": "text"}
                    ]
                }
            ]
        }
        
        # 分析 SQL 模式
        result = await analyze_sql_pattern_with_llm(sql, metadata_context)
        
        # 检查分析结果
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        
        # 检查结果中是否包含预期的键
        self.assertIn("input_tables", result)
        self.assertIn("output_tables", result)
        self.assertIn("relations", result)
        
        # 检查分析是否正确识别了输入和输出表
        self.assertIn("users", result["input_tables"])
        self.assertIn("user_reports", result["output_tables"])
        
        # 打印分析结果
        import json
        print(f"\nSQL 分析结果:\n{json.dumps(result, indent=2)}")


if __name__ == "__main__":
    unittest.main()
