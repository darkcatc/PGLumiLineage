#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志配置模块

提供标准日志配置功能，包括日志级别、格式和输出目标的设置。

作者: Vance Chen
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from pglumilineage.common.config import settings


def setup_logging(
    log_level: Optional[str] = None,
    log_file: Optional[str] = None,
    log_format: Optional[str] = None,
) -> None:
    """
    配置标准logging模块
    
    Args:
        log_level: 日志级别，默认从settings获取
        log_file: 可选的日志文件路径，如果提供则同时输出到文件
        log_format: 自定义日志格式，默认为标准格式
    """
    # 获取日志级别，优先使用参数传入的值，其次使用配置
    level_name = log_level or settings.LOG_LEVEL
    level_num = getattr(logging, level_name.upper(), logging.INFO)
    
    # 设置日志格式
    log_format = log_format or '%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s'
    formatter = logging.Formatter(log_format)
    
    # 配置根日志记录器
    root_logger = logging.getLogger()
    root_logger.setLevel(level_num)
    
    # 清除现有处理器，避免重复配置
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)
    
    # 添加控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level_num)
    root_logger.addHandler(console_handler)
    
    # 如果提供了日志文件路径，添加文件处理器
    if log_file:
        # 确保日志目录存在
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file, encoding='utf-8')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level_num)
        root_logger.addHandler(file_handler)
    
    # 设置常用库的日志级别
    for logger_name in ['urllib3', 'asyncio', 'sqlalchemy.engine', 'sqlalchemy.pool']:
        logging.getLogger(logger_name).setLevel(max(level_num, logging.INFO))
    
    # 记录初始日志
    logging.info(f"日志系统已初始化，级别: {level_name}")


def get_logger(name: str) -> logging.Logger:
    """
    获取指定名称的日志记录器
    
    Args:
        name: 日志记录器名称，通常为模块名
        
    Returns:
        logging.Logger: 配置好的日志记录器
    """
    return logging.getLogger(name)


# 初始化日志配置
# 注意：这里不自动调用setup_logging()，由应用程序在启动时显式调用
