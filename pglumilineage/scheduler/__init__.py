#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
任务调度模块

此模块包含两个主要的调度器主入口：
1. log_processor_main - 日志收集和解析的主入口
2. metadata_collector_main - 元数据收集的主入口

为了保持向后兼容性，我们提供了从旧文件名到新文件名的导入映射。
"""

# 为了向后兼容性，提供旧文件名到新文件名的映射
import sys
from importlib import import_module

# 动态导入新模块
_log_processor_main = import_module('pglumilineage.scheduler.log_processor_main')
_metadata_collector_main = import_module('pglumilineage.scheduler.metadata_collector_main')

# 设置别名，保持向后兼容性
sys.modules['pglumilineage.scheduler.service_orchestrator'] = _log_processor_main
sys.modules['pglumilineage.scheduler.log_scheduler'] = _log_processor_main
sys.modules['pglumilineage.scheduler.metadata_scheduler'] = _metadata_collector_main

# 导出主要函数，使它们可以直接从 scheduler 包导入
from pglumilineage.scheduler.log_processor_main import start_log_processor, shutdown
from pglumilineage.scheduler.metadata_collector_main import start_metadata_collector
