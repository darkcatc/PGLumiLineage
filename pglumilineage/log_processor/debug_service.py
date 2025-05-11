#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
日志处理器调试脚本

用于测试和调试 log_processor 模块，确保它能正确地从 PostgreSQL 日志文件中读取数据
并存储到数据库中。

作者: Vance Chen
"""

import asyncio
import logging
import os
import sys
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from pglumilineage.common.logging_config import setup_logging
from pglumilineage.common import config, db_utils
from pglumilineage.log_processor import service

# 设置临时配置
async def setup_test_config():
    """设置测试配置"""
    # 设置日志文件路径
    # 这里我们假设 PostgreSQL 的 CSV 日志文件位于 mock/tpcds/logs 目录下
    log_files_pattern = str(project_root / "mock" / "tpcds" / "logs" / "postgresql-*.csv")
    
    # 设置源数据库名称
    source_database_name = "tpcds"
    
    # 创建临时配置
    config.settings.log_processor = type('LogProcessorConfig', (), {
        'log_files_pattern': log_files_pattern,
        'source_database_name': source_database_name,
        'batch_size': 1000
    })
    
    print(f"已设置日志文件模式: {log_files_pattern}")
    print(f"已设置源数据库名称: {source_database_name}")
    
    return config.settings


async def verify_db_connection():
    """验证数据库连接"""
    try:
        pool = await db_utils.get_db_pool()
        async with pool.acquire() as conn:
            # 检查 lumi_logs.captured_logs 表是否存在
            result = await conn.fetchval(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'lumi_logs' AND table_name = 'captured_logs')"
            )
            
            if result:
                print("✅ 成功连接到数据库，lumi_logs.captured_logs 表存在")
                
                # 获取表中的记录数
                count = await conn.fetchval("SELECT COUNT(*) FROM lumi_logs.captured_logs")
                print(f"当前表中有 {count} 条记录")
            else:
                print("❌ lumi_logs.captured_logs 表不存在，请先运行数据库初始化脚本")
                return False
            
            return True
    except Exception as e:
        print(f"❌ 数据库连接失败: {str(e)}")
        return False


async def verify_log_files():
    """验证日志文件是否存在"""
    log_files_pattern = config.settings.log_processor.log_files_pattern
    
    # 确保日志目录存在
    log_dir = Path(log_files_pattern).parent
    if not log_dir.exists():
        print(f"❌ 日志目录不存在: {log_dir}")
        print(f"正在创建日志目录: {log_dir}")
        os.makedirs(log_dir, exist_ok=True)
    
    # 检查是否有匹配的日志文件
    processed_log_files = set()
    new_log_files = await service.find_new_log_files(processed_log_files)
    
    if new_log_files:
        print(f"✅ 找到 {len(new_log_files)} 个日志文件:")
        for log_file in new_log_files:
            print(f"  - {log_file}")
    else:
        print(f"❌ 没有找到匹配的日志文件: {log_files_pattern}")
        print("提示: 请确保日志文件存在，或者修改日志文件模式")
        
        # 如果没有找到日志文件，创建一个示例日志文件
        sample_log_file = Path(log_dir) / "postgresql-sample.csv"
        if not sample_log_file.exists():
            print(f"正在创建示例日志文件: {sample_log_file}")
            create_sample_log_file(sample_log_file)
    
    return new_log_files


def create_sample_log_file(file_path):
    """创建示例日志文件"""
    # PostgreSQL CSV 日志的标题行
    header = "log_time,user_name,database_name,process_id,connection_from,session_id,session_line_num,command_tag,session_start_time,virtual_transaction_id,transaction_id,error_severity,sql_state_code,message,detail,hint,internal_query,internal_query_pos,context,query,query_pos,location,application_name,backend_type,leader_pid,query_id,duration_ms"
    
    # 示例日志记录
    sample_logs = [
        '2025-05-10 20:00:00.123 CST,postgres,tpcds,1234,"127.0.0.1:5432",6543210987.765432,1,"SELECT",2025-05-10 19:59:00.000 CST,"2/16",0,LOG,00000,"duration: 1.234 ms  statement: SELECT * FROM customer LIMIT 10",,,,,,"SELECT * FROM customer LIMIT 10",0,,psql,client backend,,0,1.234',
        '2025-05-10 20:01:00.456 CST,postgres,tpcds,1234,"127.0.0.1:5432",6543210987.765432,2,"SELECT",2025-05-10 19:59:00.000 CST,"2/17",0,LOG,00000,"duration: 2.345 ms  statement: SELECT * FROM store_sales WHERE ss_customer_sk = 12345",,,,,,"SELECT * FROM store_sales WHERE ss_customer_sk = 12345",0,,psql,client backend,,0,2.345',
        '2025-05-10 20:02:00.789 CST,postgres,tpcds,1234,"127.0.0.1:5432",6543210987.765432,3,"INSERT",2025-05-10 19:59:00.000 CST,"2/18",0,LOG,00000,"duration: 3.456 ms  statement: INSERT INTO customer_segment_state_spending_report VALUES (''CA'', ''Advanced Degree'', ''M'', 100, 10000.00, 100.00)",,,,,,"INSERT INTO customer_segment_state_spending_report VALUES (''CA'', ''Advanced Degree'', ''M'', 100, 10000.00, 100.00)",0,,psql,client backend,,0,3.456',
        '2025-05-10 20:03:00.012 CST,postgres,tpcds,1234,"127.0.0.1:5432",6543210987.765432,4,"UPDATE",2025-05-10 19:59:00.000 CST,"2/19",0,LOG,00000,"duration: 4.567 ms  statement: UPDATE customer SET c_current_addr_sk = 54321 WHERE c_customer_sk = 12345",,,,,,"UPDATE customer SET c_current_addr_sk = 54321 WHERE c_customer_sk = 12345",0,,psql,client backend,,0,4.567',
        '2025-05-10 20:04:00.345 CST,postgres,tpcds,1234,"127.0.0.1:5432",6543210987.765432,5,"DELETE",2025-05-10 19:59:00.000 CST,"2/20",0,LOG,00000,"duration: 5.678 ms  statement: DELETE FROM customer WHERE c_customer_sk = 54321",,,,,,"DELETE FROM customer WHERE c_customer_sk = 54321",0,,psql,client backend,,0,5.678'
    ]
    
    with open(file_path, 'w') as f:
        f.write(header + '\n')
        for log in sample_logs:
            f.write(log + '\n')
    
    print(f"已创建示例日志文件，包含 {len(sample_logs)} 条日志记录")


async def test_parse_log_file(log_file_path):
    """测试解析日志文件"""
    print(f"\n正在测试解析日志文件: {log_file_path}")
    
    log_entries = await service.parse_log_file(log_file_path)
    
    if log_entries:
        print(f"✅ 成功解析 {len(log_entries)} 条日志记录")
        
        # 显示前 3 条记录的详细信息
        max_display = min(3, len(log_entries))
        print(f"\n显示前 {max_display} 条记录的详细信息:")
        
        for i, entry in enumerate(log_entries[:max_display]):
            print(f"\n记录 #{i+1}:")
            print(f"  log_time: {entry.log_time}")
            print(f"  source_database_name: {entry.source_database_name}")
            print(f"  username: {entry.username}")
            print(f"  database_name_logged: {entry.database_name_logged}")
            print(f"  client_addr: {entry.client_addr}")
            print(f"  application_name: {entry.application_name}")
            print(f"  session_id: {entry.session_id}")
            print(f"  query_id: {entry.query_id}")
            print(f"  duration_ms: {entry.duration_ms}")
            print(f"  raw_sql_text: {entry.raw_sql_text[:100]}..." if len(entry.raw_sql_text) > 100 else f"  raw_sql_text: {entry.raw_sql_text}")
            print(f"  log_source_identifier: {entry.log_source_identifier}")
    else:
        print("❌ 未能解析出任何日志记录")
    
    return log_entries


async def test_batch_insert_logs(log_entries):
    """测试批量插入日志记录"""
    if not log_entries:
        print("没有日志记录可供插入")
        return 0
    
    print(f"\n正在测试批量插入 {len(log_entries)} 条日志记录")
    
    # 先获取当前表中的记录数
    pool = await db_utils.get_db_pool()
    async with pool.acquire() as conn:
        before_count = await conn.fetchval("SELECT COUNT(*) FROM lumi_logs.captured_logs")
    
    # 执行批量插入
    inserted_count = await service.batch_insert_logs(log_entries)
    
    if inserted_count > 0:
        print(f"✅ 成功插入 {inserted_count} 条日志记录")
        
        # 获取插入后的记录数
        async with pool.acquire() as conn:
            after_count = await conn.fetchval("SELECT COUNT(*) FROM lumi_logs.captured_logs")
            
            # 验证记录数增加了
            if after_count > before_count:
                print(f"✅ 表中记录数从 {before_count} 增加到 {after_count}")
                
                # 查询最新插入的记录
                latest_records = await conn.fetch(
                    "SELECT log_id, log_time, source_database_name, username, raw_sql_text FROM lumi_logs.captured_logs ORDER BY log_id DESC LIMIT 3"
                )
                
                print("\n最新插入的记录:")
                for record in latest_records:
                    print(f"  log_id: {record['log_id']}")
                    print(f"  log_time: {record['log_time']}")
                    print(f"  source_database_name: {record['source_database_name']}")
                    print(f"  username: {record['username']}")
                    print(f"  raw_sql_text: {record['raw_sql_text'][:50]}..." if len(record['raw_sql_text']) > 50 else f"  raw_sql_text: {record['raw_sql_text']}")
                    print()
            else:
                print(f"❌ 表中记录数没有增加: {before_count} -> {after_count}")
    else:
        print("❌ 未能插入任何日志记录")
    
    return inserted_count


async def main():
    """主函数"""
    print("=== 日志处理器调试脚本 ===\n")
    
    # 设置日志
    setup_logging()
    
    # 设置测试配置
    await setup_test_config()
    
    # 验证数据库连接
    if not await verify_db_connection():
        print("请先确保数据库连接正常，并且 lumi_logs.captured_logs 表已创建")
        return
    
    # 验证日志文件
    log_files = await verify_log_files()
    
    if not log_files:
        # 如果没有找到日志文件，使用示例日志文件
        log_dir = Path(config.settings.log_processor.log_files_pattern).parent
        sample_log_file = str(log_dir / "postgresql-sample.csv")
        
        if os.path.exists(sample_log_file):
            log_files = [sample_log_file]
            print(f"将使用示例日志文件: {sample_log_file}")
        else:
            print("未找到任何日志文件，无法继续测试")
            return
    
    # 测试解析日志文件
    for log_file in log_files[:1]:  # 只测试第一个日志文件
        log_entries = await test_parse_log_file(log_file)
        
        if log_entries:
            # 测试批量插入日志记录
            await test_batch_insert_logs(log_entries)
    
    print("\n=== 调试完成 ===")


if __name__ == "__main__":
    asyncio.run(main())
