#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLM 分析器模块单元测试

该模块测试 LLM 分析器与 Qwen 模型的连通性。

作者: Vance Chen
"""

import asyncio
import unittest
from unittest import mock
from typing import List, Dict, Optional

import pytest
from pydantic import SecretStr

from pglumilineage.common import config
from pglumilineage.llm_analyzer.service import (
    call_qwen_api,
    construct_prompt_for_qwen,
    parse_llm_response
)


class TestQwenConnectivity(unittest.IsolatedAsyncioTestCase):
    """测试与 Qwen 模型的连通性"""
    
    async def test_api_key_exists(self):
        """测试 Qwen API 密钥是否存在"""
        # 获取配置
        settings = config.get_settings_instance()
        
        # 检查 API 密钥是否存在
        self.assertIsNotNone(settings.DASHSCOPE_API_KEY)
        self.assertIsInstance(settings.DASHSCOPE_API_KEY, SecretStr)
        
        # 检查 API 密钥是否有值
        api_key = settings.DASHSCOPE_API_KEY.get_secret_value()
        self.assertTrue(len(api_key) > 0)
    
    async def test_qwen_model_config(self):
        """测试 Qwen 模型配置是否正确"""
        # 获取配置
        settings = config.get_settings_instance()
        
        # 检查 Qwen 模型配置
        self.assertIsNotNone(settings.QWEN_BASE_URL)
        self.assertIsNotNone(settings.QWEN_MODEL_NAME)
        
        # 检查 Qwen 模型名称是否有效
        self.assertTrue(len(settings.QWEN_MODEL_NAME) > 0)
        
        # 检查 Qwen API 基础 URL 是否有效
        self.assertTrue("dashscope.aliyuncs.com" in str(settings.QWEN_BASE_URL))
    
    @pytest.mark.skip(reason="需要真实的API密钥，可能会产生费用")
    async def test_qwen_api_call(self):
        """测试调用 Qwen API（需要真实的API密钥）"""
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
    
    async def test_qwen_api_call_mock(self):
        """使用 mock 测试 Qwen API 调用流程"""
        # 构造简单的测试消息
        messages = [
            {"role": "system", "content": "你是一个SQL分析助手。"},
            {"role": "user", "content": "分析这个SQL: SELECT * FROM users WHERE id = 1"}
        ]
        
        # 模拟 AsyncOpenAI 客户端
        mock_response = mock.MagicMock()
        mock_response.choices = [mock.MagicMock()]
        mock_response.choices[0].message.content = "这是一个简单的查询，从 users 表中选择 id 为 1 的记录。"
        
        mock_client = mock.MagicMock()
        mock_client.chat.completions.create = mock.AsyncMock(return_value=mock_response)
        
        # 模拟 AsyncOpenAI 类
        with mock.patch('openai.AsyncOpenAI', return_value=mock_client):
            # 调用 Qwen API
            response = await call_qwen_api(messages)
            
            # 检查响应
            self.assertIsNotNone(response)
            self.assertEqual(response, "这是一个简单的查询，从 users 表中选择 id 为 1 的记录。")
            
            # 验证 API 调用参数
            mock_client.chat.completions.create.assert_called_once()
            args, kwargs = mock_client.chat.completions.create.call_args
            self.assertEqual(kwargs['messages'], messages)
    
    async def test_construct_prompt(self):
        """测试构造 Qwen 模型的 prompt"""
        # 构造测试数据
        sql_mode = "SELECT"
        sample_sql = "SELECT * FROM users WHERE id = 1"
        metadata_context = {
            "tables": [
                {
                    "name": "users",
                    "schema": "public",
                    "columns": [
                        {"name": "id", "type": "integer"},
                        {"name": "name", "type": "text"},
                        {"name": "email", "type": "text"}
                    ]
                }
            ]
        }
        
        # 构造 prompt
        messages = construct_prompt_for_qwen(sql_mode, sample_sql, metadata_context)
        
        # 检查 prompt 结构
        self.assertIsInstance(messages, list)
        self.assertTrue(len(messages) > 0)
        
        # 检查 prompt 内容
        system_message = messages[0]
        self.assertEqual(system_message["role"], "system")
        self.assertTrue("SQL" in system_message["content"])
        
        user_message = messages[1]
        self.assertEqual(user_message["role"], "user")
        self.assertTrue(sample_sql in user_message["content"])
        self.assertTrue("users" in user_message["content"])
    
    async def test_parse_llm_response(self):
        """测试解析 LLM 响应"""
        # 构造测试响应
        response_content = """
        分析结果：
        
        ```json
        {
            "input_tables": ["users"],
            "output_tables": [],
            "relations": [
                {
                    "source_table": "users",
                    "source_columns": ["id"],
                    "target_table": null,
                    "target_columns": [],
                    "operation": "READ"
                }
            ]
        }
        ```
        
        这是一个简单的查询操作，从 users 表中读取 id 为 1 的记录。
        """
        
        # 解析响应
        result = parse_llm_response(response_content)
        
        # 检查解析结果
        self.assertIsNotNone(result)
        self.assertIsInstance(result, dict)
        self.assertIn("input_tables", result)
        self.assertIn("output_tables", result)
        self.assertIn("relations", result)
        
        # 检查解析内容
        self.assertEqual(result["input_tables"], ["users"])
        self.assertEqual(len(result["relations"]), 1)
        self.assertEqual(result["relations"][0]["source_table"], "users")
        self.assertEqual(result["relations"][0]["operation"], "READ")


if __name__ == "__main__":
    unittest.main()
