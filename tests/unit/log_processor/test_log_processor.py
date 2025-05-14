#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志处理器测试脚本

用于测试配置文件变更后的日志处理器功能是否正常工作

作者: Vance Chen
"""

import asyncio
import os
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common.config import get_settings_instance
from pglumilineage.log_processor import debug_service

async def main():
    """主函数"""
    print("=== 日志处理器测试脚本 ===\n")
    
    # 设置日志
    setup_logging()
    
    # 获取配置实例
    settings = get_settings_instance()
    print(f"配置文件加载成功: {settings.PROJECT_NAME}")
    
    # 确保内部数据库配置有效
    if not settings.INTERNAL_DB.USER or not settings.INTERNAL_DB.PASSWORD.get_secret_value():
        print("错误: 内部数据库配置不完整，请检查配置文件")
        return
    
    print(f"内部数据库配置: {settings.INTERNAL_DB.HOST}:{settings.INTERNAL_DB.PORT}/{settings.INTERNAL_DB.DB_NAME}")
    
    # 运行日志处理器调试脚本
    await debug_service.main()
    
    print("\n=== 测试完成 ===")

if __name__ == "__main__":
    asyncio.run(main())
