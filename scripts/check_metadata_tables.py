#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查元数据表结构

此脚本用于检查元数据表的结构，特别是 lumi_metadata_store.objects_metadata 和
lumi_metadata_store.functions_metadata 表，以确认它们是否包含 database_name 列。

作者: Vance Chen
"""

import os
import sys
import asyncio
import logging
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from pglumilineage.common import db_utils
from pglumilineage.common.logging_config import setup_logging

# 设置日志
setup_logging()
logger = logging.getLogger(__name__)


async def check_table_structure(table_name: str) -> None:
    """检查表结构"""
    logger.info(f"正在检查表 {table_name} 的结构...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 查询表结构
        query = f"""
        SELECT 
            column_name, 
            data_type, 
            is_nullable
        FROM 
            information_schema.columns
        WHERE 
            table_schema = $1 AND table_name = $2
        ORDER BY 
            ordinal_position
        """
        
        schema, table = table_name.split(".")
        
        async with pool.acquire() as conn:
            # 检查表是否存在
            exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = $1 AND table_name = $2
                )
            """, schema, table)
            
            if not exists:
                logger.error(f"表 {table_name} 不存在")
                return
            
            # 查询表结构
            rows = await conn.fetch(query, schema, table)
            
            logger.info(f"表 {table_name} 的结构:")
            for row in rows:
                nullable = "可空" if row["is_nullable"] == "YES" else "非空"
                logger.info(f"  {row['column_name']}: {row['data_type']} ({nullable})")
            
            # 检查是否存在 database_name 列
            has_database_name = any(row["column_name"] == "database_name" for row in rows)
            if has_database_name:
                logger.info(f"表 {table_name} 包含 database_name 列")
            else:
                logger.warning(f"表 {table_name} 不包含 database_name 列")
            
            # 检查是否存在 source_database_name 列
            has_source_database_name = any(row["column_name"] == "source_database_name" for row in rows)
            if has_source_database_name:
                logger.info(f"表 {table_name} 包含 source_database_name 列")
            else:
                logger.warning(f"表 {table_name} 不包含 source_database_name 列")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
    
    except Exception as e:
        logger.error(f"检查表 {table_name} 结构时出错: {str(e)}")


async def main() -> None:
    """主函数"""
    try:
        # 检查元数据表结构
        await check_table_structure("lumi_metadata_store.objects_metadata")
        print()
        await check_table_structure("lumi_metadata_store.functions_metadata")
    
    except Exception as e:
        logger.error(f"检查表结构时出错: {str(e)}")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"\n程序出错: {str(e)}")
