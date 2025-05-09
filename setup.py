#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PGLumiLineage 安装脚本
"""

from setuptools import setup, find_packages

setup(
    name="pglumilineage",
    version="0.1.0",
    description="从PostgreSQL日志分析SQL血缘关系的工具",
    author="Vance Chen",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.10",
    install_requires=[
        "asyncpg>=0.25.0",
        "sqlglot>=10.0.0",
        "transformers>=4.51.0",
        "pydantic>=2.0.0",
        "pydantic-settings>=2.0.0",
        "structlog>=22.3.0",
        "python-dotenv>=0.21.0",
        "toml>=0.10.2",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.20.0",
            "black>=22.12.0",
            "isort>=5.10.0",
            "flake8>=6.0.0",
            "mypy>=1.0.0",
        ],
        "api": [
            "fastapi>=0.95.0",
            "uvicorn>=0.20.0",
        ],
        "scheduler": [
            "apscheduler>=3.9.0",
        ],
    },
)
