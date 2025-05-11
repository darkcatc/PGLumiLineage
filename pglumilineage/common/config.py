#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
配置管理模块

使用Pydantic V2的BaseSettings创建配置类，支持从TOML文件和环境变量加载配置。
该模块提供了一个统一的配置接口，用于管理应用程序的各种配置项。

作者: Vance Chen
"""

import os
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, Optional

import toml
from pydantic import (
    AnyHttpUrl,
    Field,
    PostgresDsn,
    SecretStr,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    应用配置类
    
    从环境变量和配置文件加载配置项
    """
    
    # 基本配置
    LOG_LEVEL: str = "INFO"
    PROJECT_NAME: str = "PGLumiLineage"
    
    # PostgreSQL配置
    POSTGRES_USER: str = Field(..., description="PostgreSQL用户名")
    POSTGRES_PASSWORD: SecretStr = Field(..., description="PostgreSQL密码")
    POSTGRES_HOST: str = Field(..., description="PostgreSQL主机地址")
    POSTGRES_PORT: int = Field(5432, description="PostgreSQL端口")
    POSTGRES_DB_RAW_LOGS: str = Field(..., description="原始日志数据库名")
    POSTGRES_DB_ANALYTICAL_PATTERNS: str = Field(..., description="分析模式数据库名")
    POSTGRES_DB_AGE: str = Field(..., description="AGE图数据库名")
    
    # 数据库连接字符串
    RAW_LOGS_DSN: Optional[PostgresDsn] = None
    ANALYTICAL_PATTERNS_DSN: Optional[PostgresDsn] = None
    AGE_DSN: Optional[PostgresDsn] = None
    
    # LLM配置
    LLM_API_KEY: SecretStr = Field(..., description="LLM API密钥")
    LLM_MODEL_NAME: str = Field("gpt-4o", description="LLM模型名称")
    
    # Qwen API配置
    DASHSCOPE_API_KEY: SecretStr = Field("sk-add1fe773eb44685a3aeee14d89c19a4", description="DashScope API密钥")
    QWEN_BASE_URL: AnyHttpUrl = Field("https://dashscope.aliyuncs.com/compatible-mode/v1", description="Qwen API基础URL")
    QWEN_MODEL_NAME: str = Field("qwen-plus", description="Qwen模型名称")
    
    # 日志文件配置
    PG_LOG_FILE_PATTERN: str = Field(..., description="PostgreSQL日志文件模式，例如: '/var/log/postgresql/postgresql-*.csv'")
    
    # 配置模型设置
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=True,
    )
    
    @model_validator(mode="after")
    def build_dsn_uris(self) -> "Settings":
        """构建数据库连接字符串"""
        # 构建原始日志数据库DSN
        if not self.RAW_LOGS_DSN:
            # 使用字符串格式化直接构建DSN，避免PostgresDsn.build产生的双斜杠问题
            dsn = f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD.get_secret_value()}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB_RAW_LOGS}"
            self.RAW_LOGS_DSN = PostgresDsn(dsn)
        
        # 构建分析模式数据库DSN
        if not self.ANALYTICAL_PATTERNS_DSN:
            dsn = f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD.get_secret_value()}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB_ANALYTICAL_PATTERNS}"
            self.ANALYTICAL_PATTERNS_DSN = PostgresDsn(dsn)
        
        # 构建AGE图数据库DSN
        if not self.AGE_DSN:
            dsn = f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD.get_secret_value()}@{self.POSTGRES_HOST}:{self.POSTGRES_PORT}/{self.POSTGRES_DB_AGE}"
            self.AGE_DSN = PostgresDsn(dsn)
        
        return self
    
    @classmethod
    def from_toml(cls, toml_path: str) -> "Settings":
        """从TOML文件加载配置"""
        if not os.path.exists(toml_path):
            return cls()
        
        with open(toml_path, "r", encoding="utf-8") as f:
            toml_data = toml.load(f)
        
        # 将TOML数据转换为字典
        config_dict = {}
        for section, values in toml_data.items():
            if isinstance(values, dict):
                for key, value in values.items():
                    config_key = f"{section.upper()}_{key.upper()}"
                    config_dict[config_key] = value
            else:
                config_dict[section.upper()] = values
        
        return cls(**config_dict)


@lru_cache()
def get_settings(config_path: Optional[str] = None) -> Settings:
    """
    获取应用配置实例
    
    使用LRU缓存避免重复加载配置
    
    Args:
        config_path: TOML配置文件路径，默认为None
        
    Returns:
        Settings: 配置实例
    """
    if config_path:
        return Settings.from_toml(config_path)
    
    # 尝试查找默认配置文件
    default_paths = [
        Path("config/settings.toml"),
        Path("../config/settings.toml"),
        Path("/etc/pglumilineage/settings.toml"),
    ]
    
    for path in default_paths:
        if path.exists():
            return Settings.from_toml(str(path))
    
    # 如果没有找到配置文件，则从环境变量加载
    return Settings()


# 全局可访问的配置实例
# 使用延迟初始化模式，避免在模块导入时就需要所有必填字段
_settings = None


def get_settings_instance():
    """获取全局设置实例，延迟初始化"""
    global _settings
    if _settings is None:
        try:
            _settings = get_settings()
        except Exception as e:
            import sys
            # 在测试环境中，如果无法加载设置，使用测试默认值
            if 'pytest' in sys.modules:
                _settings = Settings(
                    POSTGRES_USER="test_user",
                    POSTGRES_PASSWORD="test_password",
                    POSTGRES_HOST="test_host",
                    POSTGRES_DB_RAW_LOGS="test_raw_logs",
                    POSTGRES_DB_ANALYTICAL_PATTERNS="test_analytical",
                    POSTGRES_DB_AGE="test_age",
                    LLM_API_KEY="test_api_key",
                    PG_LOG_FILE_PATTERN="/test/path/*.csv",
                )
            else:
                # 非测试环境，则抛出异常
                raise e
    return _settings


# 全局可访问的配置实例
class LazySettings:
    """延迟加载的设置类，只有在实际访问时才加载配置"""
    
    def __getattr__(self, name):
        return getattr(get_settings_instance(), name)
    
    def __str__(self):
        return str(get_settings_instance())
    
    def __repr__(self):
        return repr(get_settings_instance())


# 全局可访问的配置实例
settings = LazySettings()
