#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
展示解析后的日志数据

这个脚本用于解析 PostgreSQL CSV 日志文件并展示解析后的数据。
"""

import asyncio
import csv
import logging
import os
import sys
from datetime import datetime
from typing import List, Dict, Any, Optional

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common.models import RawSQLLog
from pglumilineage.log_processor import service

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)


async def parse_log_file_improved(log_file_path: str) -> List[RawSQLLog]:
    """
    改进的日志文件解析函数，能够处理 PostgreSQL CSV 日志格式
    
    Args:
        log_file_path: 日志文件路径
        
    Returns:
        List[RawSQLLog]: 解析后的 SQL 日志条目列表
    """
    logger.info(f"开始解析日志文件: {log_file_path}")
    log_entries = []
    
    try:
        with open(log_file_path, 'r', newline='') as csvfile:
            # PostgreSQL CSV 日志没有标题行，需要手动指定字段名
            # 参考 PostgreSQL 文档中的 CSV 日志格式
            # https://www.postgresql.org/docs/current/runtime-config-logging.html#RUNTIME-CONFIG-LOGGING-CSVLOG
            fieldnames = [
                'log_time', 'user_name', 'database_name', 'process_id', 'connection_from', 
                'session_id', 'session_line_num', 'command_tag', 'session_start_time', 
                'virtual_transaction_id', 'transaction_id', 'error_severity', 'sql_state_code', 
                'message', 'detail', 'hint', 'internal_query', 'internal_query_pos', 
                'context', 'query', 'query_pos', 'location', 'application_name', 'backend_type',
                'leader_pid', 'query_id'
            ]
            
            csv_reader = csv.DictReader(csvfile, fieldnames=fieldnames)
            
            for row in csv_reader:
                # 检查是否包含 SQL 语句
                # PostgreSQL CSV 日志中，SQL 语句可能在 'message' 字段中，且以 'statement:' 开头
                sql_text = None
                
                if row.get('message', '').startswith('statement:'):
                    sql_text = row['message'][len('statement:'):].strip()
                elif row.get('query') and row['query'].strip():
                    sql_text = row['query'].strip()
                
                if sql_text:
                    try:
                        # 创建 RawSQLLog 对象
                        log_entry = RawSQLLog(
                            log_time=datetime.fromisoformat(row.get('log_time', '').split('.')[0]) if row.get('log_time') else datetime.now(),
                            source_database_name='tpcds',  # 使用固定值，实际应用中应该从参数获取
                            username=row.get('user_name', ''),
                            database_name_logged=row.get('database_name', ''),
                            client_addr=row.get('connection_from', ''),
                            application_name=row.get('application_name', ''),
                            session_id=row.get('session_id', ''),
                            query_id=int(row.get('query_id', 0)) if row.get('query_id', '').isdigit() else None,
                            duration_ms=0,  # PostgreSQL CSV 日志中可能没有持续时间信息
                            raw_sql_text=sql_text,
                            log_source_identifier=os.path.basename(log_file_path)
                        )
                        log_entries.append(log_entry)
                    except Exception as e:
                        logger.error(f"解析日志行时出错: {str(e)}, 行数据: {row}")
                        continue
    
    except Exception as e:
        logger.error(f"解析日志文件 {log_file_path} 时出错: {str(e)}")
    
    logger.info(f"从日志文件 {log_file_path} 中解析出 {len(log_entries)} 条 SQL 日志")
    return log_entries


def print_log_entries(log_entries: List[RawSQLLog]) -> None:
    """
    打印日志条目
    
    Args:
        log_entries: 日志条目列表
    """
    if not log_entries:
        print("没有找到 SQL 日志条目")
        return
    
    print(f"\n找到 {len(log_entries)} 条 SQL 日志条目：\n")
    print("-" * 100)
    
    for i, entry in enumerate(log_entries, 1):
        print(f"[{i}] 日志时间: {entry.log_time}")
        print(f"    用户名: {entry.username}")
        print(f"    数据库: {entry.database_name_logged}")
        print(f"    客户端地址: {entry.client_addr}")
        print(f"    应用名称: {entry.application_name}")
        print(f"    会话 ID: {entry.session_id}")
        print(f"    SQL 语句: {entry.raw_sql_text[:200]}..." if len(entry.raw_sql_text) > 200 else f"    SQL 语句: {entry.raw_sql_text}")
        print("-" * 100)


async def main():
    """
    主函数
    """
    # 设置日志
    setup_logging()
    logger.info("开始展示解析后的日志数据")
    
    # 解析日志文件
    log_file_path = "/mnt/e/Projects/PGLumiLineage/tmp/tpcds-log/postgresql-2025-05-10_133314.csv"
    
    # 使用改进的解析函数
    log_entries = await parse_log_file_improved(log_file_path)
    
    # 打印日志条目
    print_log_entries(log_entries)
    
    logger.info("展示完成")


if __name__ == "__main__":
    asyncio.run(main())
