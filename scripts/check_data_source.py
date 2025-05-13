#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
检查数据源配置

此脚本用于检查数据库中的数据源配置，并显示详细信息。

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
from pglumilineage.log_processor import service


async def check_data_source_config():
    """检查数据源配置"""
    print("检查数据源配置...")
    
    try:
        # 初始化数据库连接池
        await db_utils.init_db_pool()
        print("数据库连接池初始化成功")
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 检查 lumi_config.data_sources 表是否存在
            table_exists = await conn.fetchval("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'lumi_config' 
                    AND table_name = 'data_sources'
                )
            """)
            
            if not table_exists:
                print("lumi_config.data_sources 表不存在")
                return
            
            # 查询数据源配置
            data_sources = await conn.fetch("""
                SELECT * FROM lumi_config.data_sources
            """)
            
            if not data_sources:
                print("未找到数据源配置")
                return
            
            print(f"找到 {len(data_sources)} 个数据源配置:")
            for i, row in enumerate(data_sources, 1):
                print(f"\n数据源 {i}:")
                print(f"  ID: {row['source_id']}")
                print(f"  名称: {row['source_name']}")
                print(f"  类型: {row.get('source_type', '未设置')}")
                print(f"  日志检索方式: {row.get('log_retrieval_method', '未设置')}")
                print(f"  日志路径模式: {row.get('log_path_pattern', '未设置')}")
                print(f"  状态: {'活跃' if row.get('is_active', False) else '不活跃'}")
                
                # 检查路径是否存在
                log_path = row.get('log_path_pattern')
                if log_path:
                    # 如果路径包含通配符，只检查目录部分
                    if '*' in log_path:
                        dir_path = str(Path(log_path).parent)
                        print(f"  路径目录: {dir_path}")
                        print(f"  目录是否存在: {os.path.exists(dir_path)}")
                    else:
                        print(f"  路径是否存在: {os.path.exists(log_path)}")
        
        # 关闭数据库连接池
        await db_utils.close_db_pool()
        print("\n数据库连接池已关闭")
    
    except Exception as e:
        print(f"检查数据源配置时出错: {str(e)}")


if __name__ == "__main__":
    try:
        asyncio.run(check_data_source_config())
    except KeyboardInterrupt:
        print("\n程序被用户中断")
    except Exception as e:
        print(f"\n程序出错: {str(e)}")
        import traceback
        traceback.print_exc()
