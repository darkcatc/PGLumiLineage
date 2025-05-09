#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志配置模块单元测试

测试日志配置功能，包括日志级别、格式和输出目标的设置。

作者: Vance Chen
"""

import logging
import os
import tempfile
from pathlib import Path
from unittest.mock import patch

import pytest

from pglumilineage.common.logging_config import setup_logging, get_logger


class TestLoggingConfig:
    """日志配置测试类"""
    
    def test_setup_logging_default(self):
        """测试默认日志配置"""
        # 保存原始处理器
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers.copy()
        original_level = root_logger.level
        
        try:
            # 清除现有处理器
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
            
            # 配置日志
            with patch('pglumilineage.common.config.settings.LOG_LEVEL', 'DEBUG'):
                setup_logging()
            
            # 验证配置
            assert root_logger.level == logging.DEBUG
            assert len(root_logger.handlers) == 1
            assert isinstance(root_logger.handlers[0], logging.StreamHandler)
            
            # 验证格式
            formatter = root_logger.handlers[0].formatter
            assert '%(levelname)s' in formatter._fmt
            assert '%(module)s' in formatter._fmt
            assert '%(funcName)s' in formatter._fmt
            assert '%(lineno)d' in formatter._fmt
        finally:
            # 恢复原始处理器
            for handler in root_logger.handlers[:]:
                root_logger.removeHandler(handler)
            
            for handler in original_handlers:
                root_logger.addHandler(handler)
            
            root_logger.setLevel(original_level)
    
    def test_setup_logging_with_file(self):
        """测试带文件的日志配置"""
        # 保存原始处理器
        root_logger = logging.getLogger()
        original_handlers = root_logger.handlers.copy()
        original_level = root_logger.level
        
        # 创建临时日志文件
        with tempfile.TemporaryDirectory() as temp_dir:
            log_file = os.path.join(temp_dir, 'test.log')
            
            try:
                # 清除现有处理器
                for handler in root_logger.handlers[:]:
                    root_logger.removeHandler(handler)
                
                # 配置日志
                setup_logging(log_level='INFO', log_file=log_file)
                
                # 验证配置
                assert root_logger.level == logging.INFO
                assert len(root_logger.handlers) == 2
                
                # 验证是否有文件处理器
                file_handlers = [h for h in root_logger.handlers 
                                if isinstance(h, logging.FileHandler)]
                assert len(file_handlers) == 1
                assert file_handlers[0].baseFilename == log_file
                
                # 写入日志并验证
                logger = get_logger(__name__)
                test_message = 'Test log message'
                logger.info(test_message)
                
                # 验证日志文件内容
                with open(log_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    assert test_message in content
            finally:
                # 恢复原始处理器
                for handler in root_logger.handlers[:]:
                    root_logger.removeHandler(handler)
                
                for handler in original_handlers:
                    root_logger.addHandler(handler)
                
                root_logger.setLevel(original_level)
    
    def test_get_logger(self):
        """测试获取日志记录器"""
        logger = get_logger('test_logger')
        assert isinstance(logger, logging.Logger)
        assert logger.name == 'test_logger'
