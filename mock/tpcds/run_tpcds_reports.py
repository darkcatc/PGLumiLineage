#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
TPC-DS 报表执行脚本

此脚本用于执行TPC-DS报表SQL，模拟生产环境中的日常数据加工作业，
以便在数据库中生成SQL执行日志，用于血缘分析。

作者: Vance Chen
"""

import os
import sys
import argparse
import subprocess
import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# 导入配置模块
from mock.tpcds.config import load_config, MockConfig


# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_command(cmd: List[str], env: Optional[Dict[str, str]] = None) -> Tuple[int, str, str]:
    """
    执行命令并返回结果
    
    Args:
        cmd: 命令及参数列表
        env: 环境变量
        
    Returns:
        Tuple[int, str, str]: 返回码, 标准输出, 标准错误
    """
    logger.info(f"执行命令: {' '.join(cmd)}")
    
    if env is None:
        env = os.environ.copy()
    
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        text=True
    )
    
    stdout, stderr = process.communicate()
    
    return process.returncode, stdout, stderr





def run_tpcds_reports(config: MockConfig, sql_file: str, verbose: bool = False) -> bool:
    """
    执行TPC-DS报表SQL
    
    Args:
        config: 数据库配置
        sql_file: SQL文件路径
        verbose: 是否显示详细输出
        
    Returns:
        bool: 是否成功
    """
    logger.info(f"执行TPC-DS报表SQL: {sql_file}")
    
    # 检查SQL文件是否存在
    if not Path(sql_file).exists():
        logger.error(f"SQL文件不存在: {sql_file}")
        return False
    
    # 设置环境变量以避免密码提示
    env = os.environ.copy()
    env["PGPASSWORD"] = config.database.password
    
    # 执行SQL文件
    cmd = [
        "psql",
        "-U", config.database.user,
        "-h", config.database.host,
        "-p", str(config.database.port),
        "-d", config.database.database,
        "-f", sql_file
    ]
    
    returncode, stdout, stderr = run_command(cmd, env=env)
    
    if returncode != 0:
        logger.error(f"执行TPC-DS报表SQL失败: {stderr}")
        return False
    
    if verbose:
        logger.info(f"SQL执行输出:\n{stdout}")
    
    logger.info("TPC-DS报表SQL执行成功")
    return True


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="执行TPC-DS报表SQL")
    parser.add_argument(
        "--config", "-c", 
        type=str, 
        help="配置文件路径 (默认使用内置配置)",
        default=None
    )
    parser.add_argument(
        "--sql-file", "-f", 
        type=str, 
        help="SQL文件路径 (默认: mock/tpcds/run_reports.sql)",
        default=None
    )

    parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="显示详细输出"
    )
    return parser.parse_args()


def main():
    """主函数"""
    args = parse_args()
    
    # 加载配置
    config = load_config(args.config)
    
    # 设置SQL文件路径
    project_root = Path(__file__).parent.parent.parent
    sql_file = args.sql_file or str(project_root / "mock" / "tpcds" / "run_reports.sql")
    
    # 日志参数已在tpcds.sql中配置，此处不再需要配置
    
    # 执行TPC-DS报表SQL
    if not run_tpcds_reports(config, sql_file, args.verbose):
        logger.error("TPC-DS报表SQL执行失败")
        return 1
    
    logger.info("所有操作完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
