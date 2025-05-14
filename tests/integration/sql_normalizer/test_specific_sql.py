#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
特定SQL规范化测试模块

此模块用于测试特定SQL语句的规范化过程，特别是log_id=189的SQL语句。

作者: Vance Chen
"""

import os
import sys
import asyncio
import unittest
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent.parent
sys.path.append(str(project_root))

from pglumilineage.common import db_utils, models
from pglumilineage.common.logging_config import setup_logging
from pglumilineage.sql_normalizer import service as sql_normalizer_service

# 设置日志
setup_logging()


async def test_specific_sql():
    """测试特定SQL语句的规范化过程"""
    # 初始化数据库连接池
    await db_utils.init_db_pool()
    
    try:
        # 获取log_id=189的SQL语句
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 获取SQL语句
            row = await conn.fetchrow("""
                SELECT log_id, raw_sql_text, source_database_name, log_time, duration_ms 
                FROM lumi_logs.captured_logs 
                WHERE log_id = 189
            """)
            
            if not row:
                print("未找到log_id=189的SQL语句")
                return
            
            log_id = row['log_id']
            raw_sql = row['raw_sql_text']
            source_database_name = row['source_database_name']
            log_time = row['log_time']
            duration_ms = row['duration_ms'] or 0
            
            print(f"测试log_id={log_id}的SQL语句")
            print(f"SQL类型: {'数据流SQL' if sql_normalizer_service.is_data_flow_sql(raw_sql) else '非数据流SQL'}")
            
            # 直接调用normalize_sql函数
            normalized_sql = sql_normalizer_service.normalize_sql(raw_sql)
            
            if normalized_sql:
                print(f"规范化成功: {normalized_sql[:100]}...")
                
                # 生成SQL哈希
                sql_hash = sql_normalizer_service.generate_sql_hash(normalized_sql)
                print(f"SQL哈希: {sql_hash}")
                
                # 将SQL模式写入数据库
                pattern_id = await sql_normalizer_service.upsert_sql_pattern_from_log(
                    sql_hash=sql_hash,
                    normalized_sql=normalized_sql,
                    sample_raw_sql=raw_sql,
                    source_database_name=source_database_name,
                    log_time=log_time,
                    duration_ms=duration_ms
                )
                
                if pattern_id:
                    print(f"SQL模式已写入数据库，哈希值: {pattern_id}")
                    
                    # 更新日志记录
                    success = await sql_normalizer_service.update_log_sql_hash(log_id, sql_hash)
                    if success:
                        print(f"日志记录已更新，log_id={log_id}")
                    else:
                        print(f"日志记录更新失败，log_id={log_id}")
                else:
                    print(f"SQL模式写入数据库失败")
            else:
                print("规范化失败")
                
                # 检查是否为数据流SQL
                is_data_flow = sql_normalizer_service.is_data_flow_sql(raw_sql)
                print(f"是否为数据流SQL: {is_data_flow}")
                
                # 记录错误
                error_reason = "非数据流转SQL或解析失败"
                await sql_normalizer_service.record_sql_normalization_error(
                    source_type="LOG",
                    source_id=log_id,
                    raw_sql_text=raw_sql,
                    error_reason=error_reason,
                    source_database_name=source_database_name
                )
                print(f"错误已记录: {error_reason}")
    finally:
        # 关闭数据库连接池
        await db_utils.close_db_pool()


if __name__ == "__main__":
    # 运行测试
    asyncio.run(test_specific_sql())
