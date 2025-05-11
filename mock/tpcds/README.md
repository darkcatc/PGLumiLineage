# TPC-DS 工具与数据生成操作手册

本文档提供了 TPC-DS 工具安装、数据生成和报表执行的完整操作指南，用于 PGLumiLineage 项目的测试和开发。

## 目录

1. [环境要求](#环境要求)
2. [工具安装](#工具安装)
3. [数据生成](#数据生成)
4. [数据库加载](#数据库加载)
5. [报表执行](#报表执行)
6. [常见问题](#常见问题)

## 环境要求

### 系统依赖

- Linux 操作系统 (Ubuntu/Debian 推荐)
- gcc-9 或更高版本
- make
- flex
- bison
- PostgreSQL 客户端工具 (psql)
- Python 3.10 或更高版本

### Python 依赖

确保已安装 PGLumiLineage 项目的 Python 环境：

```bash
# 激活虚拟环境
source ../../lumiEnv/bin/activate
```

## 工具安装

TPC-DS 工具的安装和编译通过 `setup_tpcds_tools.py` 脚本完成：

```bash
# 安装 TPC-DS 工具
python setup_tpcds_tools.py --install-tools-only
```

这个命令会：
1. 解压 `tools/TPC-DS-Tool.zip` 到临时目录
2. 编译 TPC-DS 工具 (使用 gcc-9)
3. 将编译好的工具复制到 `tools/tpcds_tools` 目录

## 数据生成

### 生成 TPC-DS 数据

```bash
# 生成 1GB 的 TPC-DS 数据
python setup_tpcds_tools.py --only-data-generation --scale 1 --threads 4
```

参数说明：
- `--only-data-generation`: 仅生成数据，不加载到数据库
- `--scale`: 数据规模，单位为 GB (默认为 1GB)
- `--threads`: 并行生成数据的线程数 (默认为 4)

### 处理数据文件

生成的数据文件可能包含多余的分隔符，需要进行处理：

```bash
# 处理数据文件，移除多余的分隔符
python setup_tpcds_tools.py --only-process-data
```

参数说明：
- `--only-process-data`: 仅处理数据文件，不生成数据也不加载到数据库

## 数据库加载

### 创建 TPC-DS 数据库

首先确保已创建 TPC-DS 数据库：

```bash
# 创建 TPC-DS 数据库
psql -U postgres -h 127.0.0.1 -c "CREATE DATABASE tpcds"
```

### 加载数据到数据库

```bash
# 加载数据到数据库
python setup_tpcds_tools.py --only-db-load
```

参数说明：
- `--only-db-load`: 仅将数据加载到数据库，不生成数据

### 一键完成所有步骤

如果希望一次性完成所有步骤 (生成数据、处理数据文件、加载数据到数据库)：

```bash
# 一键完成所有步骤
python setup_tpcds_tools.py --scale 1 --threads 4
```

## 报表执行

TPC-DS 报表的执行通过 `run_tpcds_reports.py` 脚本完成：

```bash
# 执行 TPC-DS 报表
python run_tpcds_reports.py --verbose
```

或者使用提供的 shell 脚本：

```bash
# 执行 TPC-DS 报表
./run_reports.sh --verbose
```

参数说明：
- `--verbose`: 显示详细输出，包括 SQL 执行结果
- `--sql-file`: 指定 SQL 文件路径 (默认为 `run_reports.sql`)
- `--config`: 指定配置文件路径 (默认使用内置配置)

## 常见问题

### 编译工具失败

如果编译 TPC-DS 工具失败，请确保已安装所需的系统依赖：

```bash
# Ubuntu/Debian
sudo apt-get install gcc-9 make flex bison

# CentOS/RHEL
sudo yum install gcc make flex bison
```

### 数据生成速度慢

数据生成速度与系统配置有关，可以通过增加线程数来提高速度：

```bash
# 使用 8 个线程生成数据
python setup_tpcds_tools.py --only-data-generation --threads 8
```

### 数据库连接失败

如果连接数据库失败，请检查数据库配置：

1. 确保 PostgreSQL 服务正在运行
2. 检查 `config.py` 中的数据库连接参数是否正确
3. 确保数据库用户有足够的权限

### 报表执行错误

如果报表执行出现错误：

1. 确保已成功加载 TPC-DS 数据到数据库
2. 检查 PostgreSQL 日志设置是否正确配置
3. 查看 PostgreSQL 日志文件，了解具体错误信息

## 脚本参数完整列表

### setup_tpcds_tools.py

```
用法: setup_tpcds_tools.py [选项]

选项:
  --install-tools-only       仅安装 TPC-DS 工具，不生成数据
  --only-data-generation     仅生成数据，不加载到数据库
  --only-process-data        仅处理数据文件，移除多余的分隔符
  --only-db-load             仅将数据加载到数据库
  --scale SCALE              数据规模，单位为 GB (默认: 1)
  --threads THREADS          并行生成数据的线程数 (默认: 4)
  --config CONFIG            配置文件路径 (默认使用内置配置)
  --skip-data-processing     跳过数据文件处理步骤
  --verbose, -v              显示详细输出
```

### run_tpcds_reports.py

```
用法: run_tpcds_reports.py [选项]

选项:
  --config CONFIG, -c CONFIG  配置文件路径 (默认使用内置配置)
  --sql-file SQL_FILE, -f SQL_FILE
                            SQL 文件路径 (默认: mock/tpcds/run_reports.sql)
  --verbose, -v              显示详细输出
```

## 文件说明

- `setup_tpcds_tools.py`: TPC-DS 工具安装和数据生成脚本
- `run_tpcds_reports.py`: TPC-DS 报表执行脚本
- `run_reports.sh`: 报表执行的 shell 脚本封装
- `config.py`: 配置文件，包含数据库连接参数和 TPC-DS 配置
- `run_reports.sql`: TPC-DS 报表 SQL 文件
- `tools/`: 目录，包含 TPC-DS 工具和相关文件
  - `TPC-DS-Tool.zip`: TPC-DS 工具源码包
  - `tpcds.sql`: TPC-DS 表结构定义 SQL 文件
  - `tpcds_tools/`: 编译好的 TPC-DS 工具目录
