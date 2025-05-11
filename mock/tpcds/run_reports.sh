#!/bin/bash

# 设置项目根目录
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# 激活Python虚拟环境
source "$PROJECT_ROOT/lumiEnv/bin/activate"

# 执行TPC-DS报表SQL
python "$PROJECT_ROOT/mock/tpcds/run_tpcds_reports.py" "$@"
