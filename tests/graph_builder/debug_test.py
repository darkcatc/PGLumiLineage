#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
调试测试脚本
"""

import sys
import os
from pathlib import Path

print("Python版本:", sys.version)
print("当前目录:", os.getcwd())
print("Python路径:", sys.path[:3])

# 检查项目根目录
project_root = str(Path(__file__).parent.parent.parent)
print("项目根目录:", project_root)

try:
    # 尝试导入asyncpg
    import asyncpg
    print("✅ asyncpg导入成功")
except ImportError as e:
    print("❌ asyncpg导入失败:", e)

try:
    # 尝试导入测试设置
    from tests.graph_builder.test_settings import get_settings_instance
    settings = get_settings_instance()
    print("✅ 测试设置导入成功")
    print("数据库配置存在:", hasattr(settings, 'INTERNAL_DB'))
except ImportError as e:
    print("❌ 测试设置导入失败:", e)

print("调试完成") 