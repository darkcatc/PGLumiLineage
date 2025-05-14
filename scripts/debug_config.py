#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
配置调试脚本

该脚本用于调试配置文件的加载和解析，帮助排查配置相关问题。

作者: Vance Chen
"""

import sys
import os
import json
import toml
from typing import Any, Dict, Optional

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pglumilineage.common.config import get_settings_instance
from pglumilineage.common.logging_config import setup_logging, get_logger

# 设置日志
setup_logging()
logger = get_logger(__name__)

def read_toml_file(file_path: str) -> Optional[Dict]:
    """
    直接读取TOML文件内容
    
    Args:
        file_path: TOML文件路径
        
    Returns:
        Optional[Dict]: TOML文件内容，如果读取失败则返回None
    """
    try:
        if not os.path.exists(file_path):
            logger.error(f"配置文件不存在: {file_path}")
            return None
            
        with open(file_path, 'r', encoding='utf-8') as f:
            return toml.load(f)
    except Exception as e:
        logger.error(f"读取TOML文件失败: {str(e)}")
        return None

def inspect_config() -> None:
    """
    检查配置文件的加载情况，打印关键配置项
    """
    try:
        # 直接读取配置文件
        config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'settings.toml')
        logger.info(f"尝试直接读取配置文件: {config_path}")
        direct_config = read_toml_file(config_path)
        
        if direct_config:
            logger.info("=== 直接读取的配置文件内容 ===")
            # 检查LLM配置
            if 'llm' in direct_config:
                llm_config = direct_config['llm']
                logger.info(f"发现LLM配置: {list(llm_config.keys())}")
                
                # 检查API密钥
                api_key = llm_config.get('API_KEY', None)
                api_key_str = "******" if api_key else "未设置"
                logger.info(f"LLM API密钥: {api_key_str}")
                
                # 检查模型名称
                logger.info(f"LLM模型名称: {llm_config.get('MODEL_NAME', '未设置')}")
                
                # 检查DashScope API密钥
                dashscope_api_key = llm_config.get('DASHSCOPE_API_KEY', None)
                dashscope_api_key_str = "******" if dashscope_api_key else "未设置"
                logger.info(f"DashScope API密钥: {dashscope_api_key_str}")
                
                # 检查Qwen配置
                if 'qwen' in llm_config:
                    qwen_config = llm_config['qwen']
                    logger.info(f"发现Qwen配置: {list(qwen_config.keys())}")
                    
                    # 检查API密钥
                    qwen_api_key = qwen_config.get('api_key', None)
                    qwen_api_key_str = "******" if qwen_api_key else "未设置"
                    logger.info(f"Qwen API密钥: {qwen_api_key_str}")
                    
                    # 检查其他Qwen配置
                    logger.info(f"Qwen基础URL: {qwen_config.get('base_url', '未设置')}")
                    logger.info(f"Qwen模型名称: {qwen_config.get('model_name', '未设置')}")
            
            # 检查旧配置结构
            logger.info("=== 旧配置结构检查 ===")
            old_api_keys = [
                ('DASHSCOPE_API_KEY', direct_config.get('DASHSCOPE_API_KEY', None)),
                ('LLM_API_KEY', direct_config.get('LLM_API_KEY', None)),
                ('API_KEY', direct_config.get('API_KEY', None)),
                ('QWEN_API_KEY', direct_config.get('QWEN_API_KEY', None))
            ]
            
            for key_name, key_value in old_api_keys:
                key_str = "******" if key_value else "未设置"
                logger.info(f"{key_name}: {key_str}")
        
        # 获取配置实例
        logger.info("\n=== 通过配置模块加载的配置 ===")
        settings = get_settings_instance()
        
        # 打印配置路径
        logger.info(f"配置文件路径: {getattr(settings, '_settings_file_path', '未知')}")
        
        # 打印基本配置
        logger.info(f"项目名称: {getattr(settings, 'PROJECT_NAME', '未设置')}")
        logger.info(f"日志级别: {getattr(settings, 'LOG_LEVEL', '未设置')}")
        
        # 检查LLM配置
        logger.info("=== LLM配置 ===")
        
        # 检查新配置结构
        if hasattr(settings, 'llm') and settings.llm:
            logger.info("发现新配置结构 (settings.llm)")
            
            # 检查API密钥
            api_key = None
            if hasattr(settings.llm, 'api_key'):
                api_key = settings.llm.api_key
                api_key_str = "******" if api_key else "未设置"
                logger.info(f"LLM API密钥: {api_key_str}")
            
            # 检查模型名称
            if hasattr(settings.llm, 'model_name'):
                logger.info(f"LLM模型名称: {settings.llm.model_name}")
            
            # 检查Qwen配置
            if hasattr(settings.llm, 'qwen'):
                logger.info("发现Qwen子配置")
                qwen_config = settings.llm.qwen
                
                # 检查API密钥
                qwen_api_key = getattr(qwen_config, 'api_key', None)
                qwen_api_key_str = "******" if qwen_api_key else "未设置"
                logger.info(f"Qwen API密钥: {qwen_api_key_str}")
                
                # 检查其他Qwen配置
                logger.info(f"Qwen基础URL: {getattr(qwen_config, 'base_url', '未设置')}")
                logger.info(f"Qwen模型名称: {getattr(qwen_config, 'model_name', '未设置')}")
            
            # 检查DashScope配置
            dashscope_api_key = getattr(settings.llm, 'dashscope_api_key', None)
            if dashscope_api_key:
                dashscope_api_key_str = "******"
                logger.info(f"DashScope API密钥: {dashscope_api_key_str}")
        
        # 检查旧配置结构
        logger.info("=== 旧配置结构检查 ===")
        old_api_keys = [
            ('DASHSCOPE_API_KEY', getattr(settings, 'DASHSCOPE_API_KEY', None)),
            ('LLM_API_KEY', getattr(settings, 'LLM_API_KEY', None)),
            ('API_KEY', getattr(settings, 'API_KEY', None)),
            ('QWEN_API_KEY', getattr(settings, 'QWEN_API_KEY', None))
        ]
        
        for key_name, key_value in old_api_keys:
            key_str = "******" if key_value else "未设置"
            logger.info(f"{key_name}: {key_str}")
        
        # 检查其他旧配置
        logger.info(f"QWEN_BASE_URL: {getattr(settings, 'QWEN_BASE_URL', '未设置')}")
        logger.info(f"QWEN_MODEL_NAME: {getattr(settings, 'QWEN_MODEL_NAME', '未设置')}")
        logger.info(f"BASE_URL: {getattr(settings, 'BASE_URL', '未设置')}")
        logger.info(f"MODEL_NAME: {getattr(settings, 'MODEL_NAME', '未设置')}")
        
        # 检查数据库配置
        logger.info("=== 数据库配置 ===")
        if hasattr(settings, 'internal_db'):
            logger.info("发现内部数据库配置")
            db_config = settings.internal_db
            logger.info(f"数据库名称: {getattr(db_config, 'DB_NAME', '未设置')}")
            logger.info(f"数据库主机: {getattr(db_config, 'HOST', '未设置')}")
            logger.info(f"数据库端口: {getattr(db_config, 'PORT', '未设置')}")
            logger.info(f"数据库用户: {getattr(db_config, 'USER', '未设置')}")
            password = getattr(db_config, 'PASSWORD', None)
            password_str = "******" if password else "未设置"
            logger.info(f"数据库密码: {password_str}")
        
    except Exception as e:
        logger.error(f"检查配置时出错: {str(e)}")

if __name__ == "__main__":
    inspect_config()
