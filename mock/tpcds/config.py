#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
TPC-DS 模拟数据生成配置文件

此配置文件用于指定TPC-DS数据生成的参数和目标数据库连接信息。

作者: Vance Chen
"""

from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class DatabaseConfig(BaseModel):
    """数据库连接配置"""
    host: str = Field("127.0.0.1", description="数据库主机地址")
    port: int = Field(5432, description="数据库端口")
    user: str = Field("postgres", description="数据库用户名")
    password: str = Field("postgres", description="数据库密码")
    database: str = Field("tpcds", description="目标数据库名称")
    

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
    database: DatabaseConfig = Field(default_factory=DatabaseConfig, description="数据库连接配置")
    tpcds: TPCDSConfig = Field(default_factory=TPCDSConfig, description="TPC-DS数据生成配置")
    log_generation: LogGenerationConfig = Field(default_factory=LogGenerationConfig, description="日志生成配置")
    

# 默认配置
DEFAULT_CONFIG = MockConfig()


def load_config(config_path: Optional[str] = None) -> MockConfig:
    """
    加载配置文件
    
    Args:
        config_path: 配置文件路径，为None则使用默认配置
        
    Returns:
        MockConfig: 配置对象
    """
    if config_path is None:
        return DEFAULT_CONFIG
    
    # 从文件加载配置
    import json
    with open(config_path, "r", encoding="utf-8") as f:
        config_data = json.load(f)
    
    return MockConfig.model_validate(config_data)
