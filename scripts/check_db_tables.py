#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查数据库表和权限

此脚本用于检查数据库中是否存在特定的表，以及当前用户对这些表的权限。

作者: Vance Chen
"""

import os
import sys
import asyncio
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root))

from pglumilineage.common import db_utils


async def check_tables_and_permissions():
    """检查数据库表和权限"""
    print("检查数据库表和权限...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        print("数据库连接池初始化成功")
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 检查当前用户
            current_user = await conn.fetchval("SELECT current_user")
            print(f"当前用户: {current_user}")
            
            # 检查 lumi_config schema 是否存在
            schema_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.schemata 
                    WHERE schema_name = 'lumi_config'
                )
            """)
            print(f"lumi_config schema 是否存在: {schema_exists}")
            
            if schema_exists:
                # 检查 data_sources 表是否存在
                table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'lumi_config' 
                        AND table_name = 'data_sources'
                    )
                """)
                print(f"lumi_config.data_sources 表是否存在: {table_exists}")
                
                if table_exists:
                    # 检查用户对表的权限
                    permissions = await conn.fetch("""
                        SELECT grantee, privilege_type
                        FROM information_schema.table_privileges
                        WHERE table_schema = 'lumi_config'
                        AND table_name = 'data_sources'
                        ORDER BY grantee, privilege_type
                    """)
                    
                    print("表权限:")
                    for row in permissions:
                        print(f"  用户: {row['grantee']}, 权限: {row['privilege_type']}")
                    
                    # 检查用户对 schema 的权限
                    schema_permissions = await conn.fetch("""
                        SELECT grantee, privilege_type
                        FROM information_schema.usage_privileges
                        WHERE object_schema = 'lumi_config'
                        ORDER BY grantee, privilege_type
                    """)
                    
                    print("Schema 权限:")
                    for row in schema_permissions:
                        print(f"  用户: {row['grantee']}, 权限: {row['privilege_type']}")
                    
                    # 尝试查询表中的数据
                    try:
                        data_sources = await conn.fetch("""
                            SELECT source_id, source_name, log_retrieval_method, is_active
                            FROM lumi_config.data_sources
                            LIMIT 5
                        """)
                        
                        print(f"表中数据 (最多5条):")
                        for row in data_sources:
                            print(f"  ID: {row['source_id']}, 名称: {row['source_name']}, " 
                                  f"方式: {row['log_retrieval_method']}, 状态: {row['is_active']}")
                    except Exception as e:
                        print(f"查询表数据时出错: {str(e)}")
            
            # 检查 lumi_logs schema 是否存在
            logs_schema_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.schemata 
                    WHERE schema_name = 'lumi_logs'
                )
            """)
            print(f"lumi_logs schema 是否存在: {logs_schema_exists}")
            
            if logs_schema_exists:
                # 检查 processed_log_files 表是否存在
                processed_files_table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'lumi_logs' 
                        AND table_name = 'processed_log_files'
                    )
                """)
                print(f"lumi_logs.processed_log_files 表是否存在: {processed_files_table_exists}")
                
                # 检查 raw_sql_logs 表是否存在
                raw_logs_table_exists = await conn.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'lumi_logs' 
                        AND table_name = 'raw_sql_logs'
                    )
                """)
                print(f"lumi_logs.raw_sql_logs 表是否存在: {raw_logs_table_exists}")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
        print("数据库连接池已关闭")
    
    except Exception as e:
        print(f"检查数据库表和权限时出错: {str(e)}")


if __name__ == "__main__":
    try:
        asyncio.run(check_tables_and_permissions())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"\n程序出错: {str(e)}")
        import traceback
        traceback.print_exc()
