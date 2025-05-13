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
from typing import Any, Dict, Optional, List

import toml
from pydantic import (
    AnyHttpUrl,
    PostgresDsn,
    SecretStr,
    model_validator,
)


class InternalDBSettings:
    """
    内部数据库配置类
    
    用于配置项目内部使用的数据库连接信息
    """
    def __init__(self, **kwargs):
        self.USER = kwargs.get("USER", "")
        self.PASSWORD = SecretStr(kwargs.get("PASSWORD", ""))
        self.HOST = kwargs.get("HOST", "")
        self.PORT = kwargs.get("PORT", None)
        self.DB_NAME = kwargs.get("DB_NAME", "")
        self.DB_RAW_LOGS = kwargs.get("DB_RAW_LOGS", "")
        self.DB_ANALYTICAL_PATTERNS = kwargs.get("DB_ANALYTICAL_PATTERNS", "")
        self.DB_AGE = kwargs.get("DB_AGE", "")


class ProductionDBSettings:
    """
    生产数据库配置类
    
    用于配置要连接解析的生产数据库连接信息
    """
    def __init__(self, **kwargs):
        self.USER = kwargs.get("USER", "")
        self.PASSWORD = SecretStr(kwargs.get("PASSWORD", ""))
        self.HOST = kwargs.get("HOST", "")
        self.PORT = kwargs.get("PORT", None)
        self.DB_NAME = kwargs.get("DB_NAME", "")
        self.SSL = kwargs.get("SSL", False)
        self.TIMEOUT = kwargs.get("TIMEOUT", None)
        self.DB_TYPE = kwargs.get("DB_TYPE", "")


class QwenSettings:
    """
    Qwen API配置类
    
    用于配置通义千问API的连接信息
    """
    def __init__(self, **kwargs):
        self.DASHSCOPE_API_KEY = SecretStr(kwargs.get("DASHSCOPE_API_KEY", ""))
        self.BASE_URL = kwargs.get("BASE_URL", "")
        self.MODEL_NAME = kwargs.get("MODEL_NAME", "")


class LLMSettings:
    """
    LLM配置类
    
    用于配置大语言模型的连接信息
    """
    def __init__(self, **kwargs):
        self.API_KEY = SecretStr(kwargs.get("API_KEY", ""))
        self.MODEL_NAME = kwargs.get("MODEL_NAME", "")
        qwen_data = kwargs.get("QWEN", {})
        if isinstance(qwen_data, dict):
            self.QWEN = QwenSettings(**qwen_data)
        else:
            self.QWEN = qwen_data


class Settings:
    """
    应用配置类
    
    从 TOML 配置文件加载配置项
    """
    
    def __init__(self, **kwargs):
        # 基本配置
        self.LOG_LEVEL = kwargs.get("LOG_LEVEL", "")
        self.PROJECT_NAME = kwargs.get("PROJECT_NAME", "")
        
        # 数据库配置
        internal_db_data = kwargs.get("INTERNAL_DB", {})
        if isinstance(internal_db_data, dict):
            self.INTERNAL_DB = InternalDBSettings(**internal_db_data)
        else:
            self.INTERNAL_DB = internal_db_data
            
        production_db_data = kwargs.get("PRODUCTION_DB", {})
        if isinstance(production_db_data, dict):
            self.PRODUCTION_DB = ProductionDBSettings(**production_db_data)
        else:
            self.PRODUCTION_DB = production_db_data
        
        # LLM配置
        llm_data = kwargs.get("LLM", {})
        if isinstance(llm_data, dict):
            self.LLM = LLMSettings(**llm_data)
        else:
            self.LLM = llm_data
        
        # 日志文件配置
        self.PG_LOG_FILE_PATTERN = kwargs.get("PG_LOG_FILE_PATTERN", "")
        
        # 数据库连接字符串
        self.RAW_LOGS_DSN = kwargs.get("RAW_LOGS_DSN", None)
        self.ANALYTICAL_PATTERNS_DSN = kwargs.get("ANALYTICAL_PATTERNS_DSN", None)
        self.AGE_DSN = kwargs.get("AGE_DSN", None)
        
        # 兼容旧版配置
        self.POSTGRES_USER = kwargs.get("POSTGRES_USER", None)
        self.POSTGRES_PASSWORD = kwargs.get("POSTGRES_PASSWORD", None)
        self.POSTGRES_HOST = kwargs.get("POSTGRES_HOST", None)
        self.POSTGRES_PORT = kwargs.get("POSTGRES_PORT", None)
        self.POSTGRES_DB_RAW_LOGS = kwargs.get("POSTGRES_DB_RAW_LOGS", None)
        self.POSTGRES_DB_ANALYTICAL_PATTERNS = kwargs.get("POSTGRES_DB_ANALYTICAL_PATTERNS", None)
        self.POSTGRES_DB_AGE = kwargs.get("POSTGRES_DB_AGE", None)
    
    def build_dsn_uris(self) -> "Settings":
        """构建数据库连接字符串"""
        # 兼容旧版配置
        # 如果使用了旧版配置字段，将其同步到新的嵌套配置中
        if self.POSTGRES_USER is not None:
            self.INTERNAL_DB.USER = self.POSTGRES_USER
        if self.POSTGRES_PASSWORD is not None:
            self.INTERNAL_DB.PASSWORD = self.POSTGRES_PASSWORD
        if self.POSTGRES_HOST is not None:
            self.INTERNAL_DB.HOST = self.POSTGRES_HOST
        if self.POSTGRES_PORT is not None:
            self.INTERNAL_DB.PORT = self.POSTGRES_PORT
        if self.POSTGRES_DB_RAW_LOGS is not None:
            self.INTERNAL_DB.DB_RAW_LOGS = self.POSTGRES_DB_RAW_LOGS
        if self.POSTGRES_DB_ANALYTICAL_PATTERNS is not None:
            self.INTERNAL_DB.DB_ANALYTICAL_PATTERNS = self.POSTGRES_DB_ANALYTICAL_PATTERNS
        if self.POSTGRES_DB_AGE is not None:
            self.INTERNAL_DB.DB_AGE = self.POSTGRES_DB_AGE
        
        # 构建原始日志数据库DSN
        if not self.RAW_LOGS_DSN:
            # 使用字符串格式化直接构建 DSN，避免 PostgresDsn.build 产生的双斜杠问题
            dsn = f"postgresql://{self.INTERNAL_DB.USER}:{self.INTERNAL_DB.PASSWORD.get_secret_value()}@{self.INTERNAL_DB.HOST}:{self.INTERNAL_DB.PORT}/{self.INTERNAL_DB.DB_RAW_LOGS}"
            self.RAW_LOGS_DSN = PostgresDsn(dsn)
        
        # 构建分析模式数据库DSN
        if not self.ANALYTICAL_PATTERNS_DSN:
            dsn = f"postgresql://{self.INTERNAL_DB.USER}:{self.INTERNAL_DB.PASSWORD.get_secret_value()}@{self.INTERNAL_DB.HOST}:{self.INTERNAL_DB.PORT}/{self.INTERNAL_DB.DB_ANALYTICAL_PATTERNS}"
            self.ANALYTICAL_PATTERNS_DSN = PostgresDsn(dsn)
        
        # 构建 AGE 图数据库 DSN
        if not self.AGE_DSN:
            dsn = f"postgresql://{self.INTERNAL_DB.USER}:{self.INTERNAL_DB.PASSWORD.get_secret_value()}@{self.INTERNAL_DB.HOST}:{self.INTERNAL_DB.PORT}/{self.INTERNAL_DB.DB_AGE}"
            self.AGE_DSN = PostgresDsn(dsn)
        
        return self
    

    
    @classmethod
    def from_toml(cls, toml_path: str) -> "Settings":
        """从TOML文件加载配置"""
        if not os.path.exists(toml_path):
            return cls()
        
        with open(toml_path, "r", encoding="utf-8") as f:
            toml_data = toml.load(f)
        
        # 处理嵌套结构
        config_dict = {}
        
        # 处理顶级字段
        for key, value in toml_data.items():
            if not isinstance(value, dict):
                config_dict[key.upper()] = value
        
        # 处理内部数据库配置
        if "internal_db" in toml_data:
            config_dict["INTERNAL_DB"] = toml_data["internal_db"]
        
        # 处理生产数据库配置
        if "production_db" in toml_data:
            config_dict["PRODUCTION_DB"] = toml_data["production_db"]
        
        # 处理 LLM 配置
        if "llm" in toml_data:
            llm_data = toml_data["llm"]
            # 如果有 Qwen 配置，将其保留在 llm 下
            config_dict["LLM"] = llm_data
        
        # 兼容旧版配置格式
        if "postgres" in toml_data:
            postgres_data = toml_data["postgres"]
            for key, value in postgres_data.items():
                config_dict[f"POSTGRES_{key.upper()}"] = value
        
        # 创建设置实例
        settings = cls(**config_dict)
        
        # 构建数据库连接字符串
        settings.build_dsn_uris()
        
        return settings


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
                # 创建测试用的设置实例
                # 尝试从测试配置文件加载
                test_config_paths = [
                    Path("config/settings.test.toml"),
                    Path("../config/settings.test.toml"),
                    Path("tests/config/settings.test.toml")
                ]
                
                test_config_loaded = False
                for path in test_config_paths:
                    if path.exists():
                        try:
                            _settings = Settings.from_toml(str(path))
                            test_config_loaded = True
                            break
                        except Exception:
                            pass
                
                # 如果没有测试配置文件，使用环境变量或默认值
                if not test_config_loaded:
                    import os
                    _settings = Settings(
                        INTERNAL_DB={
                            "USER": os.environ.get("TEST_DB_USER", ""),
                            "PASSWORD": os.environ.get("TEST_DB_PASSWORD", ""),
                            "HOST": os.environ.get("TEST_DB_HOST", ""),
                            "PORT": os.environ.get("TEST_DB_PORT", None),
                            "DB_NAME": os.environ.get("TEST_DB_NAME", ""),
                            "DB_RAW_LOGS": os.environ.get("TEST_DB_RAW_LOGS", ""),
                            "DB_ANALYTICAL_PATTERNS": os.environ.get("TEST_DB_ANALYTICAL_PATTERNS", ""),
                            "DB_AGE": os.environ.get("TEST_DB_AGE", "")
                        },
                        PRODUCTION_DB={
                            "USER": os.environ.get("TEST_PROD_DB_USER", ""),
                            "PASSWORD": os.environ.get("TEST_PROD_DB_PASSWORD", ""),
                            "HOST": os.environ.get("TEST_PROD_DB_HOST", ""),
                            "PORT": os.environ.get("TEST_PROD_DB_PORT", None),
                            "DB_NAME": os.environ.get("TEST_PROD_DB_NAME", "")
                        },
                        LLM={
                            "API_KEY": os.environ.get("TEST_LLM_API_KEY", ""),
                            "MODEL_NAME": os.environ.get("TEST_LLM_MODEL_NAME", ""),
                            "QWEN": {
                                "DASHSCOPE_API_KEY": os.environ.get("TEST_QWEN_API_KEY", ""),
                                "BASE_URL": os.environ.get("TEST_QWEN_BASE_URL", ""),
                                "MODEL_NAME": os.environ.get("TEST_QWEN_MODEL_NAME", "")
                            }
                        },
                        PG_LOG_FILE_PATTERN=os.environ.get("TEST_PG_LOG_FILE_PATTERN", "")
                    )
                
                # 构建数据库连接字符串
                _settings.build_dsn_uris()
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
