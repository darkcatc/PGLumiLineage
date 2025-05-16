#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AGE图谱构建器集成测试

此模块用于测试AGE图谱构建器的端到端功能，包括从LLM提取的关系到Cypher语句执行的完整流程。

作者: Vance Chen
"""

import os
import json
import unittest
from datetime import datetime
from typing import Dict, Any, List
from unittest.mock import patch, MagicMock

import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../..')))

from pglumilineage.common import models
from pglumilineage.age_graph_builder import service as age_builder_service


class TestAgeGraphBuilderIntegration(unittest.TestCase):
    """AGE图谱构建器集成测试类"""

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

    @patch('pglumilineage.age_graph_builder.service._execute_cypher_statements')
    async def test_build_graph_for_pattern(self, mock_execute):
        """测试build_graph_for_pattern函数"""
        # 设置模拟函数返回值
        mock_execute.return_value = True
        
        # 调用被测函数
        success = await age_builder_service.build_graph_for_pattern(self.test_pattern)
        
        # 验证结果
        self.assertTrue(success, "build_graph_for_pattern应该返回True")
        
        # 验证模拟函数是否被调用
        mock_execute.assert_called_once()
        
        # 验证传递给模拟函数的参数
        args, _ = mock_execute.call_args
        cypher_statements = args[0]
        self.assertIsInstance(cypher_statements, list, "应该传递Cypher语句列表")
        self.assertTrue(len(cypher_statements) > 0, "应该生成至少一条Cypher语句")

    @patch('pglumilineage.age_graph_builder.service.build_graph_for_pattern')
    async def test_build_graph_for_patterns(self, mock_build):
        """测试build_graph_for_patterns函数"""
        # 设置模拟函数返回值
        mock_build.return_value = True
        
        # 创建测试用的模式列表
        patterns = [self.test_pattern]
        
        # 调用被测函数
        results = await age_builder_service.build_graph_for_patterns(patterns)
        
        # 验证结果
        self.assertIsInstance(results, list, "应该返回结果列表")
        self.assertEqual(len(results), 1, "结果列表长度应该与输入模式列表长度相同")
        self.assertTrue(results[0], "结果应该是True")
        
        # 验证模拟函数是否被调用
        mock_build.assert_called_once_with(self.test_pattern)

    @patch('pglumilineage.age_graph_builder.service._execute_cypher_statements')
    async def test_error_handling(self, mock_execute):
        """测试错误处理"""
        # 设置模拟函数抛出异常
        mock_execute.side_effect = Exception("测试异常")
        
        # 调用被测函数
        success = await age_builder_service.build_graph_for_pattern(self.test_pattern)
        
        # 验证结果
        self.assertFalse(success, "发生异常时应该返回False")
        
        # 验证模拟函数是否被调用
        mock_execute.assert_called_once()

    def test_save_cypher_to_file(self):
        """测试将Cypher语句保存到文件"""
        # 调用被测函数生成Cypher语句
        cypher_statements = age_builder_service.transform_json_to_cypher(self.test_pattern)
        
        # 确保有语句生成
        self.assertTrue(len(cypher_statements) > 0, "应该生成至少一条Cypher语句")
        
        # 创建输出目录
        output_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(__file__)))),
            'data', 'cypher'
        )
        os.makedirs(output_dir, exist_ok=True)
        
        # 保存Cypher语句到文件
        output_file = os.path.join(output_dir, f"{self.test_pattern.sql_hash}_cypher.txt")
        with open(output_file, 'w', encoding='utf-8') as f:
            for i, stmt in enumerate(cypher_statements):
                if isinstance(stmt, dict):
                    f.write(f"-- 语句 {i+1} (带参数)\n")
                    f.write(f"{stmt['query']}\n\n")
                    f.write(f"-- 参数: {json.dumps(stmt['params'], ensure_ascii=False)}\n\n")
                else:
                    f.write(f"-- 语句 {i+1}\n")
                    f.write(f"{stmt}\n\n")
        
        # 验证文件是否创建
        self.assertTrue(os.path.exists(output_file), f"应该创建输出文件: {output_file}")
        
        print(f"\nCypher语句已保存到: {output_file}")


if __name__ == '__main__':
    unittest.main()
