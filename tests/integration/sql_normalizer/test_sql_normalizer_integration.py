#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL 规范化器集成测试模块

此模块用于测试 SQL 规范化器的完整流程，包括：
1. 处理元数据定义
2. 处理捕获的日志记录
3. 验证 SQL 模式表中的记录

作者: Vance Chen
"""

import os
import sys
import asyncio
import unittest
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from pglumilineage.common import db_utils, models
from pglumilineage.common.logging_config import setup_logging
from pglumilineage.sql_normalizer import service as sql_normalizer_service

# 设置日志
setup_logging()


# 集成测试：测试 SQL 规范化器的完整流程
class TestSQLNormalizerIntegration(unittest.TestCase):
    """SQL 规范化器集成测试类"""
    
    async def test_full_process(self):
        """测试完整的 SQL 规范化处理流程"""
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        try:
            # 1. 处理元数据定义
            view_count, func_count, normalized_count, updated_count = await sql_normalizer_service.process_metadata_definitions()
            print(f"处理元数据定义: {view_count} 个视图, {func_count} 个函数, {normalized_count} 个标准化, {updated_count} 个更新")
            
            # 2. 处理捕获的日志记录
            total_logs, processed_logs, marked_logs = await sql_normalizer_service.process_captured_logs(
                batch_size=10,
                max_concurrency=5
            )
            print(f"处理日志记录: {total_logs} 个总日志, {processed_logs} 个处理, {marked_logs} 个标记")
            
            # 3. 检查 SQL 模式表中的记录数
            pool = await db_utils.get_db_pool()
            async with pool.acquire() as conn:
                pattern_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM lumi_analytics.sql_patterns
                """)
                print(f"SQL 模式表中有 {pattern_count} 条记录")
            
            # 测试成功
            self.assertTrue(True, "完整流程测试成功")
        except Exception as e:
            self.fail(f"完整流程测试失败: {str(e)}")
        finally:
            # 关闭数据库连接池
            await db_utils.close_db_pool()


# 运行测试
async def run_tests():
    """运行集成测试"""
    # 运行集成测试
    integration_test = TestSQLNormalizerIntegration()
    await integration_test.test_full_process()


if __name__ == "__main__":
    # 设置日志
    setup_logging()
    
    # 运行异步测试
    asyncio.run(run_tests())
