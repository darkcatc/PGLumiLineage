#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
配置模块单元测试

测试配置加载、验证和DSN构建功能。

作者: Vance Chen
"""

import os
from pathlib import Path
from unittest.mock import patch, MagicMock

import pytest
from pydantic import PostgresDsn, SecretStr

from pglumilineage.common.config import Settings, get_settings, LazySettings


@pytest.fixture
def mock_env_vars():
    """模拟环境变量"""
    env_vars = {
        "POSTGRES_USER": "test_user",
        "POSTGRES_PASSWORD": "test_password",
        "POSTGRES_HOST": "test_host",
        "POSTGRES_PORT": "5433",
        "POSTGRES_DB_RAW_LOGS": "test_raw_logs",
        "POSTGRES_DB_ANALYTICAL_PATTERNS": "test_analytical",
        "POSTGRES_DB_AGE": "test_age",
        "LLM_API_KEY": "test_api_key",
        "LLM_MODEL_NAME": "test_model",
        "PG_LOG_FILE_PATTERN": "/test/path/*.csv",
    }
    with patch.dict(os.environ, env_vars):
        yield env_vars


class TestSettings:
    """Settings类测试"""

    def test_default_values(self):
        """测试默认值"""
        settings = Settings(
            POSTGRES_USER="user",
            POSTGRES_PASSWORD="password",
            POSTGRES_HOST="host",
            POSTGRES_DB_RAW_LOGS="raw_logs",
            POSTGRES_DB_ANALYTICAL_PATTERNS="analytical",
            POSTGRES_DB_AGE="age",
            LLM_API_KEY="api_key",
            PG_LOG_FILE_PATTERN="/path/*.csv",
        )
        
        # 检查默认值
        assert settings.LOG_LEVEL == "INFO"
        assert settings.PROJECT_NAME == "PGLumiLineage"
        assert settings.POSTGRES_PORT == 5432
        assert settings.LLM_MODEL_NAME == "gpt-4o"

    def test_build_dsn_uris(self):
        """测试DSN URI构建"""
        settings = Settings(
            POSTGRES_USER="test_user",
            POSTGRES_PASSWORD="test_password",
            POSTGRES_HOST="test_host",
            POSTGRES_PORT=5433,
            POSTGRES_DB_RAW_LOGS="test_raw_logs",
            POSTGRES_DB_ANALYTICAL_PATTERNS="test_analytical",
            POSTGRES_DB_AGE="test_age",
            LLM_API_KEY="test_api_key",
            PG_LOG_FILE_PATTERN="/test/path/*.csv",
        )
        
        # 检查DSN构建
        assert settings.RAW_LOGS_DSN is not None
        assert "postgresql://test_user:test_password@test_host:5433/test_raw_logs" == str(settings.RAW_LOGS_DSN)
        
        assert settings.ANALYTICAL_PATTERNS_DSN is not None
        assert "postgresql://test_user:test_password@test_host:5433/test_analytical" == str(settings.ANALYTICAL_PATTERNS_DSN)
        
        assert settings.AGE_DSN is not None
        assert "postgresql://test_user:test_password@test_host:5433/test_age" == str(settings.AGE_DSN)

    def test_from_env_vars(self, mock_env_vars):
        """测试从环境变量加载配置"""
        settings = Settings()
        
        # 检查从环境变量加载的值
        assert settings.POSTGRES_USER == "test_user"
        assert settings.POSTGRES_PASSWORD.get_secret_value() == "test_password"
        assert settings.POSTGRES_HOST == "test_host"
        assert settings.POSTGRES_PORT == 5433
        assert settings.POSTGRES_DB_RAW_LOGS == "test_raw_logs"
        assert settings.POSTGRES_DB_ANALYTICAL_PATTERNS == "test_analytical"
        assert settings.POSTGRES_DB_AGE == "test_age"
        assert settings.LLM_API_KEY.get_secret_value() == "test_api_key"
        assert settings.LLM_MODEL_NAME == "test_model"
        assert settings.PG_LOG_FILE_PATTERN == "/test/path/*.csv"


class TestGetSettings:
    """get_settings函数测试"""

    @patch("pglumilineage.common.config.Settings.from_toml")
    def test_get_settings_with_config_path(self, mock_from_toml, mock_env_vars):
        """测试指定配置文件路径"""
        mock_from_toml.return_value = Settings()
        
        settings = get_settings("test_config.toml")
        
        # 验证调用了from_toml方法
        mock_from_toml.assert_called_once_with("test_config.toml")

    def test_get_settings_default_paths(self, mock_env_vars):
        """测试默认配置文件路径"""
        # 直接使用替代函数来模拟整个行为
        original_get_settings = get_settings
        
        # 保存原始的get_settings函数
        try:
            # 模拟调用过程
            called_paths = []
            
            def mock_from_toml(path):
                called_paths.append(path)
                return Settings()
            
            # 替换from_toml方法
            Settings.from_toml = mock_from_toml
            
            # 模拟路径存在检查
            original_exists = Path.exists
            
            def mock_exists(self):
                # 只有第二个路径存在
                return str(self) == "../config/settings.toml"
            
            # 替换exists方法
            Path.exists = mock_exists
            
            # 调用被测试的函数
            # 清除缓存，确保重新运行
            get_settings.cache_clear()
            settings = get_settings()
            
            # 验证调用了from_toml方法，使用了第二个路径
            assert len(called_paths) == 1
            assert called_paths[0] == "../config/settings.toml"
        finally:
            # 恢复原始方法
            Path.exists = original_exists
            Settings.from_toml = lambda cls, path: cls()  # 简单的替代实现
