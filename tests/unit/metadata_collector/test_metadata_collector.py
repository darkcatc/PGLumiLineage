#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试元数据收集器

此脚本用于测试元数据收集器的功能，包括：
1. 测试数据库连接
2. 测试元数据同步调度规则获取
3. 测试元数据收集功能
4. 测试元数据存储

作者: Vance Chen
"""

import os
import sys
import asyncio
import argparse
import logging
from pathlib import Path
from typing import Dict, Any, List, Optional

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from pglumilineage.common import db_utils, models, config
from pglumilineage.common.logging_config import setup_logging
from pglumilineage.metadata_collector import service as metadata_collector_service

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)


async def test_database_connection() -> None:
    """测试数据库连接"""
    logger.info("测试数据库连接...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        logger.info("数据库连接池初始化成功")
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 测试连接
            version = await conn.fetchval("SELECT version()")
            logger.info(f"数据库连接成功，版本: {version}")
            
            # 检查 lumi_metadata_store 模式是否存在
            schema_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.schemata 
                    WHERE schema_name = 'lumi_metadata_store'
                )
            """)
            
            if schema_exists:
                logger.info("lumi_metadata_store 模式存在")
                
                # 检查表是否存在
                tables = await conn.fetch("""
                    SELECT table_name 
                    FROM information_schema.tables 
                    WHERE table_schema = 'lumi_metadata_store'
                """)
                
                if tables:
                    logger.info(f"lumi_metadata_store 模式中找到 {len(tables)} 个表:")
                    for table in tables:
                        logger.info(f"  - {table['table_name']}")
                else:
                    logger.warning("lumi_metadata_store 模式中没有找到表")
            else:
                logger.error("lumi_metadata_store 模式不存在")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
        logger.info("数据库连接池已关闭")
    
    except Exception as e:
        logger.error(f"测试数据库连接时出错: {str(e)}")
        raise


async def test_metadata_sync_schedules() -> None:
    """测试元数据同步调度规则获取"""
    logger.info("测试元数据同步调度规则获取...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 获取元数据同步调度规则
        schedules = await metadata_collector_service.get_metadata_sync_schedules()
        
        if schedules:
            logger.info(f"找到 {len(schedules)} 个元数据同步调度规则:")
            for i, schedule in enumerate(schedules, 1):
                source_config = schedule['source_config']
                last_sync_at = schedule.get('last_sync_success_at', '从未')
                logger.info(f"  {i}. {source_config.source_name} - 间隔: {schedule.get('sync_interval_seconds', 86400)}秒, 状态: {'活跃' if schedule.get('is_schedule_active', False) else '不活跃'}")
                logger.info(f"     上次同步: {last_sync_at}")
        else:
            logger.warning("未找到元数据同步调度规则")
            
            # 检查表是否存在
            pool = await db_utils.get_db_pool()
            async with pool.acquire() as conn:
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'lumi_config' 
                        AND table_name = 'source_sync_schedules'
                    )
                """)
                
                if table_exists:
                    logger.info("lumi_config.source_sync_schedules 表存在，但没有找到活跃的调度规则")
                    
                    # 检查是否有任何调度规则
                    count = await conn.fetchval("""
                        SELECT COUNT(*) FROM lumi_config.source_sync_schedules
                    """)
                    
                    if count > 0:
                        logger.info(f"表中有 {count} 个调度规则，但可能都不是活跃状态")
                    else:
                        logger.info("表中没有任何调度规则")
                else:
                    logger.error("lumi_config.source_sync_schedules 表不存在")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
    
    except Exception as e:
        logger.error(f"测试元数据同步调度规则获取时出错: {str(e)}")
        raise


async def test_metadata_collection(source_name: Optional[str] = None) -> None:
    """
    测试元数据收集功能
    
    Args:
        source_name: 指定要测试的数据源名称，如果为 None 则测试所有数据源
    """
    logger.info(f"测试元数据收集功能{f' (数据源: {source_name})' if source_name else ''}...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 获取元数据同步调度规则
        schedules = await metadata_collector_service.get_metadata_sync_schedules()
        
        if not schedules:
            logger.warning("未找到元数据同步调度规则，无法测试元数据收集")
            return
        
        # 如果指定了数据源名称，则只测试该数据源
        if source_name:
            schedules = [s for s in schedules if s['source_config'].source_name == source_name]
            if not schedules:
                logger.warning(f"未找到名为 {source_name} 的数据源，无法测试元数据收集")
                return
        
        # 测试每个数据源的元数据收集
        for schedule in schedules:
            source_config = schedule['source_config']
            schedule_id = schedule['schedule_id']
            
            logger.info(f"测试数据源 {source_config.source_name} 的元数据收集...")
            
            # 测试元数据收集
            success, message = await metadata_collector_service.collect_metadata_for_source(source_config)
            
            if success:
                logger.info(f"数据源 {source_config.source_name} 的元数据收集成功")
            else:
                logger.error(f"数据源 {source_config.source_name} 的元数据收集失败: {message}")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
    
    except Exception as e:
        logger.error(f"测试元数据收集功能时出错: {str(e)}")
        raise


async def test_metadata_storage() -> None:
    """测试元数据存储"""
    logger.info("测试元数据存储...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 检查对象元数据表
            objects_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_metadata_store.objects_metadata
            """)
            
            logger.info(f"lumi_metadata_store.objects_metadata 表中有 {objects_count} 条记录")
            
            # 检查列元数据表
            columns_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_metadata_store.columns_metadata
            """)
            
            logger.info(f"lumi_metadata_store.columns_metadata 表中有 {columns_count} 条记录")
            
            # 检查函数元数据表
            functions_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_metadata_store.functions_metadata
            """)
            
            logger.info(f"lumi_metadata_store.functions_metadata 表中有 {functions_count} 条记录")
            
            # 检查同步状态表
            sync_status_count = await conn.fetchval("""
                SELECT COUNT(*) FROM lumi_metadata_store.metadata_sync_status
            """)
            
            logger.info(f"lumi_metadata_store.metadata_sync_status 表中有 {sync_status_count} 条记录")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
    
    except Exception as e:
        logger.error(f"测试元数据存储时出错: {str(e)}")
        raise


async def main() -> None:
    """主函数"""
    parser = argparse.ArgumentParser(description="测试元数据收集器")
    parser.add_argument("--db", action="store_true", help="测试数据库连接")
    parser.add_argument("--schedules", action="store_true", help="测试元数据同步调度规则获取")
    parser.add_argument("--collect", action="store_true", help="测试元数据收集功能")
    parser.add_argument("--source", type=str, help="指定要测试的数据源名称")
    parser.add_argument("--storage", action="store_true", help="测试元数据存储")
    parser.add_argument("--all", action="store_true", help="测试所有功能")
    
    args = parser.parse_args()
    
    # 如果没有指定任何参数，则显示帮助信息
    if not any([args.db, args.schedules, args.collect, args.storage, args.all]):
        parser.print_help()
        return
    
    try:
        # 测试数据库连接
        if args.db or args.all:
            await test_database_connection()
            print()
        
        # 测试元数据同步调度规则获取
        if args.schedules or args.all:
            await test_metadata_sync_schedules()
            print()
        
        # 测试元数据收集功能
        if args.collect or args.all:
            await test_metadata_collection(args.source)
            print()
        
        # 测试元数据存储
        if args.storage or args.all:
            await test_metadata_storage()
            print()
        
        logger.info("测试完成")
    
    except Exception as e:
        logger.error(f"测试过程中出错: {str(e)}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"\n程序出错: {str(e)}")
        import traceback
        traceback.print_exc()
