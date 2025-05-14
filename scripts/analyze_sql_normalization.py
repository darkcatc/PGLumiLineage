#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
SQL规范化全量分析脚本

此脚本用于对全量日志数据进行SQL泛化执行分析，包括：
1. 统计日志总量和已处理数量
2. 分析规范化成功和失败的比例
3. 分析错误原因分布
4. 检测可能被错误跳过的数据流SQL
5. 生成详细的分析报告

作者: Vance Chen
"""

import os
import sys
import asyncio
import argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Tuple, Set

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from pglumilineage.common import db_utils, models
from pglumilineage.common.logging_config import setup_logging
from pglumilineage.sql_normalizer import service as sql_normalizer_service

# 设置日志
setup_logging()


async def analyze_log_data(sample_size: int = 0, process_unprocessed: bool = False):
    """
    分析日志数据的SQL规范化情况
    
    Args:
        sample_size: 样本大小，0表示分析全部数据
        process_unprocessed: 是否处理未处理的日志
    """
    print("\n===== SQL规范化全量分析 =====")
    
    # 初始化数据库连接池
    await db_utils.init_db_pool()
    
    try:
        pool = await db_utils.get_db_pool()
        
        # 1. 统计日志总量和已处理数量
        async with pool.acquire() as conn:
            # 获取日志总量
            total_logs = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_logs.captured_logs
            """)
            
            # 获取已处理日志数量
            processed_logs = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_logs.captured_logs
                WHERE is_processed_for_analysis = TRUE
            """)
            
            # 获取规范化成功的日志数量
            normalized_logs = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_logs.captured_logs
                WHERE is_processed_for_analysis = TRUE
                AND normalized_sql_hash IS NOT NULL
            """)
            
            # 获取错误表中的记录数
            error_logs = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_analytics.sql_normalization_errors
                WHERE source_type = 'LOG'
            """)
            
            # 获取SQL模式表中的记录数
            sql_patterns = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_analytics.sql_patterns
            """)
            
            print(f"日志总量: {total_logs}")
            print(f"已处理日志: {processed_logs} ({processed_logs/total_logs*100:.2f}%)")
            print(f"规范化成功: {normalized_logs} ({normalized_logs/processed_logs*100:.2f}% of processed)")
            print(f"错误记录数: {error_logs}")
            print(f"SQL模式数: {sql_patterns}")
            
            # 2. 分析错误原因分布
            error_reasons = await conn.fetch("""
                SELECT error_reason, COUNT(*) as count
                FROM lumi_analytics.sql_normalization_errors
                WHERE source_type = 'LOG'
                GROUP BY error_reason
                ORDER BY count DESC
            """)
            
            print("\n错误原因分布:")
            for reason in error_reasons:
                print(f"  - {reason['error_reason']}: {reason['count']} ({reason['count']/error_logs*100:.2f}%)")
            
            # 3. 分析未处理的日志
            unprocessed_logs = total_logs - processed_logs
            if unprocessed_logs > 0:
                print(f"\n未处理日志: {unprocessed_logs} ({unprocessed_logs/total_logs*100:.2f}%)")
                
                # 获取未处理日志的样本
                sample_limit = sample_size if sample_size > 0 else 10
                unprocessed_samples = await conn.fetch(f"""
                    SELECT log_id, raw_sql_text
                    FROM lumi_logs.captured_logs
                    WHERE is_processed_for_analysis = FALSE
                    ORDER BY log_id
                    LIMIT {sample_limit}
                """)
                
                print(f"未处理日志样本 (前{len(unprocessed_samples)}条):")
                for sample in unprocessed_samples:
                    log_id = sample['log_id']
                    raw_sql = sample['raw_sql_text']
                    sql_type = "数据流SQL" if sql_normalizer_service.is_data_flow_sql(raw_sql) else "非数据流SQL"
                    print(f"  - Log ID: {log_id}, 类型: {sql_type}")
                    print(f"    SQL片段: {raw_sql[:100]}...")
                
                # 4. 处理未处理的日志（如果需要）
                if process_unprocessed:
                    print("\n开始处理未处理的日志...")
                    batch_size = min(100, unprocessed_logs)
                    total, processed, marked = await sql_normalizer_service.process_captured_logs(
                        batch_size=batch_size,
                        max_concurrency=10
                    )
                    print(f"处理结果: 总数={total}, 成功处理={processed}, 标记为已处理={marked}")
            
            # 5. 分析可能被错误跳过的数据流SQL
            print("\n检查可能被错误跳过的数据流SQL...")
            
            # 获取被标记为非数据流SQL的日志样本
            sample_limit = sample_size if sample_size > 0 else 20
            skipped_samples = await conn.fetch(f"""
                SELECT source_id, raw_sql_text
                FROM lumi_analytics.sql_normalization_errors
                WHERE source_type = 'LOG'
                AND error_reason LIKE '%非数据流%'
                ORDER BY created_at DESC
                LIMIT {sample_limit}
            """)
            
            # 检查这些SQL是否包含可能被错误跳过的数据流SQL
            potential_data_flow = []
            for sample in skipped_samples:
                log_id = sample['source_id']
                raw_sql = sample['raw_sql_text']
                
                # 检查是否包含可能的数据流关键字
                keywords = ['INSERT', 'UPDATE', 'DELETE', 'MERGE', 'CREATE TABLE', 'TRUNCATE', 'ALTER TABLE']
                for keyword in keywords:
                    if keyword.lower() in raw_sql.lower():
                        potential_data_flow.append({
                            'log_id': log_id,
                            'keyword': keyword,
                            'sql': raw_sql[:100] + '...'
                        })
                        break
            
            if potential_data_flow:
                print(f"发现 {len(potential_data_flow)} 条可能被错误跳过的数据流SQL:")
                for item in potential_data_flow:
                    print(f"  - Log ID: {item['log_id']}, 关键字: {item['keyword']}")
                    print(f"    SQL片段: {item['sql']}")
            else:
                print("未发现可能被错误跳过的数据流SQL")
            
            # 6. 分析SQL模式分布
            print("\nSQL模式分布分析:")
            
            # 获取执行次数最多的SQL模式
            top_patterns = await conn.fetch("""
                SELECT sql_hash, normalized_sql_text, execution_count, avg_duration_ms
                FROM lumi_analytics.sql_patterns
                ORDER BY execution_count DESC
                LIMIT 5
            """)
            
            print("执行次数最多的SQL模式:")
            for pattern in top_patterns:
                print(f"  - 哈希: {pattern['sql_hash'][:8]}..., 执行次数: {pattern['execution_count']}, 平均耗时: {pattern['avg_duration_ms']:.2f}ms")
                print(f"    SQL片段: {pattern['normalized_sql_text'][:100]}...")
            
            # 获取平均耗时最长的SQL模式
            slow_patterns = await conn.fetch("""
                SELECT sql_hash, normalized_sql_text, execution_count, avg_duration_ms
                FROM lumi_analytics.sql_patterns
                WHERE execution_count > 1
                ORDER BY avg_duration_ms DESC
                LIMIT 5
            """)
            
            print("\n平均耗时最长的SQL模式:")
            for pattern in slow_patterns:
                print(f"  - 哈希: {pattern['sql_hash'][:8]}..., 执行次数: {pattern['execution_count']}, 平均耗时: {pattern['avg_duration_ms']:.2f}ms")
                print(f"    SQL片段: {pattern['normalized_sql_text'][:100]}...")
    
    finally:
        # 关闭数据库连接池
        await db_utils.close_db_pool()


async def process_specific_logs(log_ids: List[int]):
    """
    处理指定的日志记录
    
    Args:
        log_ids: 要处理的日志ID列表
    """
    print(f"\n===== 处理指定的日志记录 (IDs: {log_ids}) =====")
    
    # 初始化数据库连接池
    await db_utils.init_db_pool()
    
    try:
        pool = await db_utils.get_db_pool()
        
        # 获取指定日志的信息
        async with pool.acquire() as conn:
            logs = []
            for log_id in log_ids:
                row = await conn.fetchrow("""
                    SELECT log_id, raw_sql_text, source_database_name, log_time, duration_ms, is_processed_for_analysis
                    FROM lumi_logs.captured_logs
                    WHERE log_id = $1
                """, log_id)
                
                if row:
                    logs.append({
                        'log_id': row['log_id'],
                        'raw_sql': row['raw_sql_text'],
                        'source_database_name': row['source_database_name'],
                        'log_time': row['log_time'],
                        'duration_ms': row['duration_ms'] or 0,
                        'is_processed': row['is_processed_for_analysis']
                    })
        
        # 处理每条日志
        for log in logs:
            log_id = log['log_id']
            raw_sql = log['raw_sql']
            
            print(f"\n处理 Log ID: {log_id}")
            print(f"处理状态: {'已处理' if log['is_processed'] else '未处理'}")
            
            # 检查是否为数据流SQL
            is_data_flow = sql_normalizer_service.is_data_flow_sql(raw_sql)
            print(f"SQL类型: {'数据流SQL' if is_data_flow else '非数据流SQL'}")
            
            # 尝试规范化SQL
            normalized_sql = sql_normalizer_service.normalize_sql(raw_sql)
            
            if normalized_sql:
                print("规范化结果: 成功")
                print(f"规范化SQL: {normalized_sql[:100]}...")
                
                # 生成SQL哈希
                sql_hash = sql_normalizer_service.generate_sql_hash(normalized_sql)
                print(f"SQL哈希: {sql_hash}")
                
                # 将SQL模式写入数据库
                pattern_id = await sql_normalizer_service.upsert_sql_pattern_from_log(
                    sql_hash=sql_hash,
                    normalized_sql=normalized_sql,
                    sample_raw_sql=raw_sql,
                    source_database_name=log['source_database_name'],
                    log_time=log['log_time'],
                    duration_ms=log['duration_ms']
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
                print("规范化结果: 失败")
                
                # 记录错误
                error_reason = "非数据流转SQL或解析失败"
                await sql_normalizer_service.record_sql_normalization_error(
                    source_type="LOG",
                    source_id=log_id,
                    raw_sql_text=raw_sql,
                    error_reason=error_reason,
                    source_database_name=log['source_database_name']
                )
                print(f"错误已记录: {error_reason}")
    
    finally:
        # 关闭数据库连接池
        await db_utils.close_db_pool()


async def analyze_error_patterns():
    """分析错误模式，找出可能需要改进的地方"""
    print("\n===== 错误模式分析 =====")
    
    # 初始化数据库连接池
    await db_utils.init_db_pool()
    
    try:
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 获取错误样本
            error_samples = await conn.fetch("""
                SELECT source_type, source_id, raw_sql_text, error_reason
                FROM lumi_analytics.sql_normalization_errors
                ORDER BY created_at DESC
                LIMIT 50
            """)
            
            # 分析错误模式
            error_patterns = {}
            for sample in error_samples:
                raw_sql = sample['raw_sql_text'].lower()
                
                # 检查SQL开头
                first_word = raw_sql.split()[0] if raw_sql.split() else ""
                if first_word not in error_patterns:
                    error_patterns[first_word] = 0
                error_patterns[first_word] += 1
                
                # 检查是否包含特定关键字
                keywords = ['create table', 'truncate', 'alter table', 'drop table', 'create index']
                for keyword in keywords:
                    if keyword in raw_sql:
                        if keyword not in error_patterns:
                            error_patterns[keyword] = 0
                        error_patterns[keyword] += 1
            
            # 输出错误模式统计
            print("错误SQL模式统计:")
            for pattern, count in sorted(error_patterns.items(), key=lambda x: x[1], reverse=True):
                if pattern and count > 1:  # 只显示出现多次的模式
                    print(f"  - {pattern}: {count} 次")
            
            # 提出改进建议
            print("\n改进建议:")
            improvement_suggestions = []
            
            # 检查CREATE TABLE是否被错误跳过
            if 'create' in error_patterns and error_patterns['create'] > 1:
                improvement_suggestions.append("修改is_data_flow_sql函数，将CREATE TABLE语句视为数据流SQL")
            
            # 检查TRUNCATE是否被错误跳过
            if 'truncate' in error_patterns and error_patterns['truncate'] > 1:
                improvement_suggestions.append("修改is_data_flow_sql函数，将TRUNCATE语句视为数据流SQL")
            
            # 检查ALTER TABLE是否被错误跳过
            if 'alter' in error_patterns and error_patterns['alter'] > 1:
                improvement_suggestions.append("修改is_data_flow_sql函数，将ALTER TABLE语句视为数据流SQL")
            
            # 输出改进建议
            for suggestion in improvement_suggestions:
                print(f"  - {suggestion}")
            
            if not improvement_suggestions:
                print("  - 未发现明显的改进点")
    
    finally:
        # 关闭数据库连接池
        await db_utils.close_db_pool()


async def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='SQL规范化全量分析工具')
    parser.add_argument('--sample', type=int, default=0, help='样本大小，0表示分析全部数据')
    parser.add_argument('--process', action='store_true', help='是否处理未处理的日志')
    parser.add_argument('--log-ids', type=int, nargs='+', help='指定要处理的日志ID列表')
    parser.add_argument('--analyze-errors', action='store_true', help='分析错误模式')
    
    args = parser.parse_args()
    
    if args.log_ids:
        await process_specific_logs(args.log_ids)
    else:
        await analyze_log_data(args.sample, args.process)
        
        if args.analyze_errors:
            await analyze_error_patterns()


if __name__ == "__main__":
    # 运行主函数
    asyncio.run(main())
