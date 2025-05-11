#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
TPC-DS 工具安装和数据生成脚本

此脚本用于下载、编译TPC-DS工具，并使用它生成指定规模的数据。
然后创建TPC-DS表结构，并将生成的数据导入到PostgreSQL数据库中。

作者: Vance Chen
"""

import argparse
import os
import subprocess
import sys
import tempfile
import shutil
import zipfile
from pathlib import Path
from typing import Optional, Tuple, List

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent.parent.absolute()
sys.path.insert(0, str(project_root))

# 导入配置
from mock.tpcds.config import load_config, MockConfig


def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description="安装TPC-DS工具并生成数据")
    parser.add_argument(
        "--config", "-c", 
        type=str, 
        help="配置文件路径 (默认使用内置配置)",
        default=None
    )
    parser.add_argument(
        "--scale", "-s", 
        type=float, 
        help="数据规模因子，单位为GB (默认: 1.0)",
        default=1.0
    )
    parser.add_argument(
        "--threads", "-t", 
        type=int, 
        help="并行线程数 (默认: 4)",
        default=4
    )
    parser.add_argument(
        "--tpcds-dir", 
        type=str, 
        help="TPC-DS工具目录 (默认: mock/tpcds/tools)",
        default=None
    )
    parser.add_argument(
        "--data-dir", 
        type=str, 
        help="生成数据的输出目录 (默认: /tmp/tpcds-data)",
        default=None
    )
    parser.add_argument(
        "--processed-data-dir", 
        type=str, 
        help="处理后数据的输出目录 (默认: /tmp/tpcds-data-processed)",
        default=None
    )
    parser.add_argument(
        "--skip-tools-setup", 
        action="store_true", 
        help="跳过TPC-DS工具的下载和编译步骤"
    )
    parser.add_argument(
        "--skip-data-generation", 
        action="store_true", 
        help="跳过数据生成步骤"
    )
    parser.add_argument(
        "--skip-data-processing", 
        action="store_true", 
        help="跳过数据处理步骤"
    )
    parser.add_argument(
        "--skip-db-setup", 
        action="store_true", 
        help="跳过数据库初始化步骤"
    )
    parser.add_argument(
        "--skip-db-load", 
        action="store_true", 
        help="跳过数据库加载步骤"
    )
    parser.add_argument(
        "--only-db-load", 
        action="store_true", 
        help="只执行数据库加载步骤"
    )
    parser.add_argument(
        "--only-data-generation", 
        action="store_true", 
        help="只执行数据生成步骤"
    )
    parser.add_argument(
        "--only-data-processing", 
        action="store_true", 
        help="只执行数据处理步骤"
    )
    parser.add_argument(
        "--verbose", "-v", 
        action="store_true", 
        help="显示详细输出"
    )
    return parser.parse_args()


def check_command_exists(command: str) -> bool:
    """
    检查命令是否存在
    
    Args:
        command: 要检查的命令
        
    Returns:
        bool: 命令是否存在
    """
    try:
        subprocess.run(["which", command], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        return True
    except FileNotFoundError:
        return False


def print_build_tools_installation_guide():
    """打印编译工具安装指南"""
    print("\n\u7f16译TPC-DS工具需要安装以下工具：")
    print("1. make - GNU make工具")
    print("2. gcc-9 - GNU C编译器版本9")
    print("3. flex - 词法分析器生成器")
    print("4. bison - 语法分析器生成器")
    
    print("\n在Ubuntu/Debian系统上安装gcc-9：")
    print("sudo apt-get update")
    print("sudo apt-get install -y build-essential flex bison")
    print("sudo apt-get install -y gcc-9 g++-9")
    print("sudo update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-9 90 --slave /usr/bin/g++ g++ /usr/bin/g++-9")
    
    print("\n在CentOS/RHEL系统上安装gcc-9：")
    print("sudo yum install -y centos-release-scl")
    print("sudo yum install -y devtoolset-9")
    print("scl enable devtoolset-9 bash")
    print("sudo yum install -y flex bison")
    
    print("\n在macOS上安装：")
    print("brew install make gcc@9 flex bison")
    
    print("\n安装完成后请重新运行此脚本\n")


def run_command(cmd: List[str], cwd: Optional[str] = None, env: Optional[dict] = None) -> Tuple[int, str, str]:
    """
    执行命令
    
    Args:
        cmd: 命令列表
        cwd: 工作目录
        env: 环境变量
        
    Returns:
        Tuple[int, str, str]: 返回码、标准输出和标准错误
    """
    print(f"执行命令: {' '.join(cmd)}")
    
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
        cwd=cwd,
        text=True
    )
    
    stdout, stderr = process.communicate()
    return process.returncode, stdout, stderr


def setup_tpcds_tools(tpcds_dir: str, verbose: bool = False) -> bool:
    """
    解压和编译TPC-DS工具
    
    Args:
        tpcds_dir: TPC-DS工具目录
        verbose: 是否显示详细输出
        
    Returns:
        bool: 是否成功
    """
    print("开始安装TPC-DS工具...")
    
    # 检查编译工具是否安装
    required_tools = ["make", "flex", "bison"]
    missing_tools = [tool for tool in required_tools if not check_command_exists(tool)]
    
    # 检查gcc-9是否安装
    gcc9_installed = False
    try:
        returncode, stdout, stderr = run_command(["gcc-9", "--version"])
        if returncode == 0:
            gcc9_installed = True
            print("gcc-9已安装: " + stdout.split('\n')[0])
    except:
        pass
    
    if not gcc9_installed:
        missing_tools.append("gcc-9")
    
    if missing_tools:
        print(f"缺少必要的编译工具: {', '.join(missing_tools)}")
        print_build_tools_installation_guide()
        return False
    
    # 创建TPC-DS工具目录
    tpcds_dir_path = Path(tpcds_dir)
    tpcds_dir_path.mkdir(parents=True, exist_ok=True)
    
    # 检查是否已经编译了TPC-DS工具
    if (tpcds_dir_path / "dsdgen").exists():
        print("TPC-DS工具已编译，跳过编译步骤")
        return True
    
    # 检查zip文件是否存在
    zip_path = Path(project_root) / "mock" / "tpcds" / "tools" / "TPC-DS-Tool.zip"
    if not zip_path.exists():
        print(f"未找到TPC-DS工具包: {zip_path}")
        return False
    
    # 创建tmp目录
    tmp_dir = Path(project_root) / "tmp"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    
    # 解压TPC-DS工具包到tmp目录
    print(f"解压TPC-DS工具包到 {tmp_dir}...")
    try:
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(tmp_dir)
        print("解压完成")
    except Exception as e:
        print(f"解压TPC-DS工具包失败: {e}")
        return False
    
    # 定位工具目录
    tools_dir = None
    for item in tmp_dir.glob("**/tools"):
        if item.is_dir():
            tools_dir = item
            break
    
    if not tools_dir:
        print("在解压后的目录中未找到tools目录")
        return False
    
    # 编译TPC-DS工具
    print("编译TPC-DS工具（使用gcc-9）...")
    
    # 首先复制Makefile.suite到Makefile
    returncode, stdout, stderr = run_command(
        ["cp", "Makefile.suite", "Makefile"],
        cwd=str(tools_dir)
    )
    
    if returncode != 0:
        print(f"复制Makefile失败: {stderr}")
        return False
    
    # 清理之前的编译文件
    returncode, stdout, stderr = run_command(
        ["make", "clean"],
        cwd=str(tools_dir)
    )
    
    # 使用gcc-9编译
    returncode, stdout, stderr = run_command(
        ["make", "CC=gcc-9"],
        cwd=str(tools_dir)
    )
    
    if returncode != 0:
        print(f"编译TPC-DS工具失败: {stderr}")
        return False
    
    if verbose:
        print(stdout)
    
    # 复制编译好的工具到指定目录
    print(f"复制编译好的工具到 {tpcds_dir_path}...")
    for item in tools_dir.glob("*"):
        if item.is_file() and (item.name == "dsdgen" or item.name == "dsqgen" or item.name == "tpcds.idx"):
            shutil.copy2(item, tpcds_dir_path)
            print(f"复制 {item.name} 到 {tpcds_dir_path}")
    
    print("TPC-DS工具安装完成")
    return True


def generate_tpcds_data(tpcds_dir: str, data_dir: str, scale: float, num_threads: int = 4, verbose: bool = False) -> bool:
    """
    生成TPC-DS数据
    
    Args:
        tpcds_dir: TPC-DS工具目录
        data_dir: 数据输出目录
        scale: 数据规模因子，单位为GB
        num_threads: 并行线程数 (默认: 4)
        verbose: 是否显示详细输出
        
    Returns:
        bool: 是否成功
    """
    print(f"开始生成TPC-DS数据，规模因子: {scale}GB...")
    
    # 创建数据输出目录
    data_dir_path = Path(data_dir)
    data_dir_path.mkdir(parents=True, exist_ok=True)
    
    # 生成数据
    dsdgen_path = Path(tpcds_dir) / "dsdgen"
    if not dsdgen_path.exists():
        print(f"未找到dsdgen工具: {dsdgen_path}")
        return False
    
    # 将tpcds.idx复制到数据目录
    idx_path = Path(tpcds_dir) / "tpcds.idx"
    if idx_path.exists():
        shutil.copy2(idx_path, data_dir_path)
    
    # 设置环境变量
    env = {
        "DSS_PATH": str(data_dir_path),
        "DSS_CONFIG": str(idx_path)
    }
    
    # 设置工作目录为工具所在目录
    tpcds_dir_path = Path(tpcds_dir)
    
    # 确保线程数至少为1
    if num_threads <= 0:
        num_threads = 4
    
    print(f"使用 {num_threads} 个线程并行生成数据...")
    
    # 使用dsdgen自带的并行功能生成数据
    cmd = [
        str(dsdgen_path), 
        "-scale", str(scale), 
        "-force", 
        "-dir", str(data_dir_path),
        "-parallel", str(num_threads)
    ]
    
    print("生成数据文件...")
    returncode, stdout, stderr = run_command(
        cmd,
        cwd=str(tpcds_dir_path),
        env=env
    )
    
    if returncode != 0:
        print(f"生成数据失败: {stderr}")
        return False
    
    if verbose:
        print(stdout)
    
    print("TPC-DS数据生成完成")
    return True


def setup_tpcds_db(config: MockConfig, verbose: bool = False) -> bool:
    """
    创建TPC-DS数据库和表结构
    
    Args:
        config: 配置对象
        verbose: 是否显示详细输出
        
    Returns:
        bool: 是否成功
    """
    print("开始创建TPC-DS数据库和表结构...")
    
    # 数据库连接信息
    host = config.database.host
    port = config.database.port
    user = config.database.user
    password = config.database.password
    database = config.database.database
    
    # 设置环境变量
    env = {"PGPASSWORD": password}
    
    # 1. 创建数据库
    print(f"创建数据库 {database}...")
    returncode, stdout, stderr = run_command(
        ["psql", "-U", user, "-h", host, "-p", str(port), "-d", "postgres", "-c", f"CREATE DATABASE {database} WITH OWNER = {user};"],
        env=env
    )
    
    if returncode != 0:
        if "already exists" in stderr:
            print(f"数据库 {database} 已存在，跳过创建")
        else:
            print(f"创建数据库失败: {stderr}")
            return False
    
    # 2. 创建表结构
    print("创建TPC-DS表结构...")
    tpcds_sql_path = Path(project_root) / "mock" / "tpcds" / "tools" / "tpcds.sql"
    
    # 检查tpcds.sql文件是否存在
    if not tpcds_sql_path.exists():
        print(f"未找到TPC-DS表结构SQL文件: {tpcds_sql_path}")
        return False
    
    returncode, stdout, stderr = run_command(
        ["psql", "-U", user, "-h", host, "-p", str(port), "-d", database, "-f", str(tpcds_sql_path)],
        env=env
    )
    
    if returncode != 0:
        print(f"创建表结构失败: {stderr}")
        return False
    
    if verbose:
        print(stdout)
    
    print("TPC-DS表结构创建成功")
    return True


def process_data_files(data_dir: str, processed_data_dir: str, verbose: bool = False) -> bool:
    """
    处理数据文件，去除每行末尾的'|'字符
    
    Args:
        data_dir: 原始数据目录
        processed_data_dir: 处理后数据输出目录
        verbose: 是否显示详细输出
        
    Returns:
        bool: 是否成功
    """
    print("开始处理数据文件...")
    
    # 创建处理后数据目录
    processed_dir_path = Path(processed_data_dir)
    processed_dir_path.mkdir(parents=True, exist_ok=True)
    
    # 获取原始数据目录中的所有.dat文件
    data_dir_path = Path(data_dir)
    data_files = list(data_dir_path.glob("*.dat"))
    
    if not data_files:
        print(f"在 {data_dir} 目录下没有找到.dat文件")
        return False
    
    # 处理每个数据文件
    for data_file in data_files:
        # 提取文件名
        file_name = data_file.name
        processed_file_path = processed_dir_path / file_name
        
        print(f"处理文件: {file_name}")
        
        try:
            # 读取原始数据文件并去除每行末尾的'|'字符
            with open(data_file, "r", encoding="iso-8859-1") as fin:
                lines = fin.readlines()
                with open(processed_file_path, "w", encoding="iso-8859-1") as fout:
                    for line in lines:
                        if line.endswith("|\n"):
                            # 去除末尾的'|'字符
                            fout.write(line[:-2] + "\n")
                        else:
                            # 如果不是以'|\n'结尾，则保持原样
                            fout.write(line)
            
            if verbose:
                print(f"  成功处理文件: {file_name}")
        
        except Exception as e:
            print(f"  处理文件 {file_name} 失败: {str(e)}")
            return False
    
    print("数据文件处理完成")
    return True


def load_tpcds_data(config: MockConfig, data_dir: str, verbose: bool = False) -> bool:
    """
    将TPC-DS数据导入到数据库
    
    Args:
        config: 数据库配置
        data_dir: 数据目录
        verbose: 是否显示详细输出
        
    Returns:
        bool: 是否成功
    """
    print("开始导入TPC-DS数据到数据库...")
    
    # 设置环境变量以避免密码提示
    env = os.environ.copy()
    env["PGPASSWORD"] = config.database.password
    
    # 获取数据目录中的所有.dat文件
    data_dir_path = Path(data_dir)
    data_files = list(data_dir_path.glob("*.dat"))
    
    # 存储已成功导入的表
    imported_tables = set()
    
    # 使用psql导入数据
    for data_file in data_files:
        # 从文件名中提取表名，并去除并行后缀
        file_stem = data_file.stem
        # 匹配表名模式，如 'call_center_1_4' -> 'call_center'
        import re
        match = re.match(r'([a-z_]+?)(?:_\d+_\d+)?$', file_stem)
        if match:
            table_name = match.group(1)
        else:
            table_name = file_stem
        
        # 如果表已经导入过，则跳过
        if table_name in imported_tables:
            print(f"跳过已导入的表 {table_name} (文件: {file_stem})")
            continue
        
        print(f"导入 {table_name} 表数据 (文件: {file_stem})...")
        
        # 构建导入命令
        cmd = [
            "psql",
            "-U", config.database.user,
            "-h", config.database.host,
            "-p", str(config.database.port),
            "-d", config.database.database,
            "-c", f"\\COPY {table_name} FROM '{data_file}' WITH (FORMAT csv, DELIMITER '|');"
        ]
        
        returncode, stdout, stderr = run_command(cmd, env=env)
        
        if returncode != 0:
            print(f"导入 {table_name} 表数据失败: {stderr}")
        else:
            imported_tables.add(table_name)
            if verbose:
                print(stdout)
    
    print("TPC-DS数据导入完成")
    return True


def main():
    """主函数"""
    args = parse_args()
    
    # 加载配置
    config = load_config(args.config)
    
    # 设置目录
    project_root = Path(__file__).parent.parent.parent
    tpcds_dir = args.tpcds_dir or str(project_root / "tmp" / "tpcds-tools")
    data_dir = args.data_dir or str(project_root / "tmp" / "tpcds-data")
    processed_data_dir = args.processed_data_dir or str(project_root / "tmp" / "tpcds-data-processed")
    scale = args.scale or config.tpcds.scale_factor
    
    # 处理只执行特定步骤的情况
    if args.only_db_load:
        # 只执行数据库加载步骤
        if not load_tpcds_data(config, processed_data_dir, args.verbose):
            print("TPC-DS数据导入失败")
            return 1
        print("数据导入完成")
        return 0
    
    if args.only_data_generation:
        # 只执行数据生成步骤
        if not generate_tpcds_data(tpcds_dir, data_dir, scale, args.threads, args.verbose):
            print("TPC-DS数据生成失败")
            return 1
        print("数据生成完成")
        return 0
        
    if args.only_data_processing:
        # 只执行数据处理步骤
        if not process_data_files(data_dir, processed_data_dir, args.verbose):
            print("数据文件处理失败")
            return 1
        print("数据处理完成")
        return 0
    
    # 正常执行所有步骤
    # 1. 安装TPC-DS工具
    if not args.skip_tools_setup:
        if not setup_tpcds_tools(tpcds_dir, args.verbose):
            print("TPC-DS工具安装失败")
            return 1
    
    # 2. 生成TPC-DS数据
    if not args.skip_data_generation:
        if not generate_tpcds_data(tpcds_dir, data_dir, scale, args.threads, args.verbose):
            print("TPC-DS数据生成失败")
            return 1
    
    # 3. 处理数据文件，去除每行末尾的'|'字符
    if not args.skip_data_processing:
        if not process_data_files(data_dir, processed_data_dir, args.verbose):
            print("数据文件处理失败")
            return 1
    
    # 4. 初始化TPC-DS数据库
    if not args.skip_db_setup:
        if not setup_tpcds_db(config, args.verbose):
            print("TPC-DS数据库初始化失败")
            return 1
    
    # 5. 将数据导入到数据库
    if not args.skip_db_load:
        if not load_tpcds_data(config, processed_data_dir, args.verbose):
            print("TPC-DS数据导入失败")
            return 1
    
    print("所有操作完成")
    return 0


if __name__ == "__main__":
    sys.exit(main())
