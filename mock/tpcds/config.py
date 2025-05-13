#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
TPC-DS 模拟数据生成配置文件

此配置文件用于指定TPC-DS数据生成的参数和目标数据库连接信息。
使用项目统一的配置文件，避免硬编码敏感信息。

作者: Vance Chen
"""

import os
import sys
from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field

# 添加项目根目录到系统路径，确保可以导入项目模块
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, project_root)

# 导入项目的配置模块
from pglumilineage.common.config import get_settings_instance


class DatabaseConfig(BaseModel):
    """数据库连接配置"""
    host: str = Field(..., description="数据库主机地址")
    port: int = Field(..., description="数据库端口")
    user: str = Field(..., description="数据库用户名")
    password: str = Field(..., description="数据库密码")
    database: str = Field(..., description="目标数据库名称")
    db_type: str = Field("postgresql", description="数据库类型")
    

class TPCDSConfig(BaseModel):
    """TPC-DS数据生成配置"""
    scale_factor: float = Field(1.0, description="数据规模因子，单位为GB")
    num_streams: int = Field(1, description="并行生成的数据流数量")
    seed: int = Field(19761215, description="随机数种子")
    tables: List[str] = Field([], description="要生成的表名列表，为空则生成所有表")
    

class LogGenerationConfig(BaseModel):
    """日志生成配置"""
    num_queries: int = Field(100, description="生成的查询数量")
    query_templates_path: str = Field("mock/tpcds/templates", description="查询模板路径")
    log_output_path: str = Field("mock/tpcds/logs", description="生成的日志输出路径")
    min_execution_time_ms: int = Field(10, description="最小执行时间（毫秒）")
    max_execution_time_ms: int = Field(5000, description="最大执行时间（毫秒）")
    

class MockConfig(BaseModel):
    """模拟数据生成总配置"""
    database: Optional[DatabaseConfig] = None
    tpcds: TPCDSConfig = Field(default_factory=TPCDSConfig, description="TPC-DS数据生成配置")
    log_generation: LogGenerationConfig = Field(default_factory=LogGenerationConfig, description="日志生成配置")
    
    class Config:
        # 允许部分字段缺失
        validate_assignment = True
        extra = "ignore"
        arbitrary_types_allowed = True
    

def get_default_config() -> MockConfig:
    """
    获取默认配置，从项目统一配置文件中加载
    
    Returns:
        MockConfig: 配置对象
    """
    # 获取项目统一配置
    settings = get_settings_instance()
    
    # 从统一配置中获取生产数据库配置
    try:
        db_config = DatabaseConfig(
            host=settings.PRODUCTION_DB.HOST,
            port=settings.PRODUCTION_DB.PORT,
            user=settings.PRODUCTION_DB.USER,
            password=settings.PRODUCTION_DB.PASSWORD.get_secret_value(),
            database=settings.PRODUCTION_DB.DB_NAME,
            db_type=getattr(settings.PRODUCTION_DB, "DB_TYPE", "postgresql")
        )
        
        # 创建 TPC-DS 配置
        tpcds_config = TPCDSConfig(
            # 使用配置文件中的值，如果有的话
            scale_factor=getattr(settings.PRODUCTION_DB, "SCALE_FACTOR", 1.0),
            num_streams=getattr(settings.PRODUCTION_DB, "NUM_STREAMS", 1),
            seed=getattr(settings.PRODUCTION_DB, "SEED", 19761215),
            tables=getattr(settings.PRODUCTION_DB, "TABLES", [])
        )
        
        # 创建日志生成配置
        log_config = LogGenerationConfig(
            # 使用配置文件中的值，如果有的话
            num_queries=getattr(settings.PRODUCTION_DB, "NUM_QUERIES", 100),
            query_templates_path=getattr(settings.PRODUCTION_DB, "QUERY_TEMPLATES_PATH", "mock/tpcds/templates"),
            log_output_path=getattr(settings.PRODUCTION_DB, "LOG_OUTPUT_PATH", "mock/tpcds/logs"),
            min_execution_time_ms=getattr(settings.PRODUCTION_DB, "MIN_EXECUTION_TIME_MS", 10),
            max_execution_time_ms=getattr(settings.PRODUCTION_DB, "MAX_EXECUTION_TIME_MS", 5000)
        )
        
        # 创建默认配置
        return MockConfig(
            database=db_config,
            tpcds=tpcds_config,
            log_generation=log_config
        )
    except Exception as e:
        import logging
        logging.error(f"Error loading configuration from settings file: {e}")
        logging.error("Using fallback configuration. This is not recommended for production use.")
        
        # 使用备用配置
        return MockConfig()


# 默认配置延迟初始化
_DEFAULT_CONFIG = None


def get_mock_config() -> MockConfig:
    """获取模拟数据生成配置，延迟初始化"""
    global _DEFAULT_CONFIG
    if _DEFAULT_CONFIG is None:
        _DEFAULT_CONFIG = get_default_config()
    return _DEFAULT_CONFIG


def load_config(config_path: Optional[str] = None) -> MockConfig:
    """
    加载配置文件
    
    Args:
        config_path: 配置文件路径，为None则使用默认配置
        
    Returns:
        MockConfig: 配置对象
    """
    if config_path is None:
        return get_mock_config()
    
    # 从文件加载配置
    import json
    with open(config_path, "r", encoding="utf-8") as f:
        config_data = json.load(f)
    
    return MockConfig.model_validate(config_data)
