#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
数据库初始化脚本

执行SQL初始化脚本，创建数据库、角色、模式和表。
支持从配置文件或环境变量加载数据库连接信息。
支持两种初始化方式：
1. 使用psql命令执行SQL脚本文件
2. 使用asyncpg直接执行DDL语句

作者: Vance Chen
"""

import argparse
import os
import subprocess
import sys
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.absolute()
sys.path.insert(0, str(project_root))

# 导入项目配置和工具
from pglumilineage.common.config import get_settings_instance, settings
from pglumilineage.common.logging_config import setup_logging, get_logger
# 保留db_utils以供后续数据同步功能使用
from pglumilineage.common.db_utils import init_db_pool, close_db_pool, execute_ddl


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="初始化PGLumiLineage数据库")
    parser.add_argument(
        "--config", "-c", 
        type=str, 
        help="配置文件路径 (默认: config/settings.toml)",
        default=str(project_root / "config" / "settings.toml")
    )
    parser.add_argument(
        "--superuser", "-u", 
        type=str, 
        help="PostgreSQL超级用户名 (默认: postgres)",
        default=None
    )
    parser.add_argument(
        "--password", "-p", 
        type=str, 
        help="PostgreSQL超级用户密码 (如不提供，将使用环境变量PGPASSWORD或提示输入)",
        default=None
    )
    parser.add_argument(
        "--admin-password", "-a", 
        type=str, 
        help="lumiadmin用户密码 (默认与超级用户密码相同)",
        default=None
    )
    parser.add_argument(
        "--host", 
        type=str, 
        help="PostgreSQL主机地址 (默认: 使用配置文件中的值)",
        default=None
    )
    parser.add_argument(
        "--port", 
        type=int, 
        help="PostgreSQL端口 (默认: 使用配置文件中的值)",
        default=None
    )
    parser.add_argument(
        "--skip-db-creation", 
        action="store_true", 
        help="跳过数据库和角色创建步骤，仅执行表结构创建"
    )
    # 移除异步模式参数
    parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="显示详细输出"
    )
    return parser.parse_args()


def run_psql_command(
    sql_file: str, 
    db_name: str = "postgres", 
    user: str = "postgres", 
    host: Optional[str] = None, 
    port: Optional[int] = None,
    env: Optional[dict] = None
) -> Tuple[int, str, str]:
    """
    执行psql命令运行SQL文件
    
    Args:
        sql_file: SQL文件路径
        db_name: 数据库名称
        user: 用户名
        host: 主机地址
        port: 端口
        env: 环境变量
        
    Returns:
        Tuple[int, str, str]: 返回码、标准输出和标准错误
    """
    logger = get_logger(__name__)
    
    cmd = ["psql", "-U", user, "-d", db_name, "-f", sql_file, "-v", "ON_ERROR_STOP=1"]
    
    if host:
        cmd.extend(["-h", host])
    
    if port:
        cmd.extend(["-p", str(port)])
    
    logger.info(f"执行命令: {' '.join(cmd)}")
    
    # 创建环境变量副本
    process_env = os.environ.copy()
    if env:
        process_env.update(env)
    
    # 执行命令
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=process_env,
        text=True
    )
    
    stdout, stderr = process.communicate()
    return process.returncode, stdout, stderr


def setup_db(args):
    """数据库初始化函数"""
    # 设置日志
    log_level = "DEBUG" if args.verbose else "INFO"
    setup_logging(log_level=log_level)
    logger = get_logger(__name__)
    
    logger.info("开始初始化PGLumiLineage数据库")
    
    try:
        # 处理SQL脚本
        scripts_dir = project_root / "scripts" / "initdb"
        
        # 如果不跳过数据库创建步骤
        if not args.skip_db_creation:
            # 创建数据库和角色
            logger.info("创建数据库和角色...")
            
            # 执行SQL脚本
            sql_file = scripts_dir / "00_setup_db_and_roles.sql"
            logger.info(f"执行{sql_file}...")
            
            # 使用psql执行脚本
            returncode, stdout, stderr = run_psql_command(
                sql_file=str(sql_file),
                user=args.superuser or settings.POSTGRES_USER,
                host=args.host or settings.POSTGRES_HOST,
                port=args.port or settings.POSTGRES_PORT,
                env={"PGPASSWORD": args.password or settings.POSTGRES_PASSWORD.get_secret_value()}
            )
            
            if returncode != 0:
                logger.error(f"执行{sql_file}失败")
                logger.error(f"stdout: {stdout}")
                logger.error(f"stderr: {stderr}")
                raise RuntimeError(f"执行{sql_file}失败")
            
            logger.info("数据库和角色创建成功")
        
        # 创建表结构
        logger.info("创建表结构...")
        
        # 执行SQL脚本
        sql_file = scripts_dir / "01_setup_schemas_and_tables.sql"
        logger.info(f"执行{sql_file}...")
        
        # 使用psql执行脚本
        logger.info("执行数据库和角色创建脚本...")
        returncode, stdout, stderr = run_psql_command(
            init_db_script, 
            "postgres", 
            superuser, 
            host, 
            port, 
            env
        )
        
        if returncode != 0:
            logger.error(f"数据库和角色创建失败: {stderr}")
            return returncode
        
        if args.verbose:
            logger.debug(stdout)
        
        logger.info("数据库和角色创建成功")
    
    # 执行模式和表创建脚本
    logger.info("执行模式和表创建脚本...")
    
    # 设置lumiadmin用户的密码环境变量
    admin_env = os.environ.copy()
    admin_env["PGPASSWORD"] = admin_password
    
    # 使用lumiadmin用户执行脚本
    logger.info(f"使用 lumiadmin 用户连接到 {settings.POSTGRES_DB_RAW_LOGS} 数据库...")
    returncode, stdout, stderr = run_psql_command(
        setup_schema_script, 
        settings.POSTGRES_DB_RAW_LOGS, 
        "lumiadmin" if not args.skip_db_creation else superuser, 
        host, 
        port, 
        admin_env if not args.skip_db_creation else env
    )
    
    if returncode != 0:
        logger.error(f"模式和表创建失败: {stderr}")
        return returncode
    
    if args.verbose:
        logger.debug(stdout)
    
    logger.info("模式和表创建成功")
    logger.info("PGLumiLineage数据库初始化完成")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())
