#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
重置 AGE 图

此脚本用于删除并重新创建 AGE 图，以便清空之前的测试内容。

作者: Cascade Assistant
"""

import os
import sys
import asyncio
import asyncpg
import argparse
from typing import List, Dict, Any, Optional

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


async def reset_graph(host: str, port: int, user: str, password: str, database: str, graph_name: str) -> bool:
    """
    删除并重新创建 AGE 图

    Args:
        host: 数据库主机
        port: 数据库端口
        user: 数据库用户名
        password: 数据库密码
        database: 数据库名称
        graph_name: 图名称

    Returns:
        bool: 是否成功重置图
    """
    try:
        # 连接数据库
        conn = await asyncpg.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database
        )
        print(f"成功连接到数据库 {database}")
        
        # 设置搜索路径
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        print("已设置搜索路径")
        
        # 检查AGE版本
        version_result = await conn.fetchrow("SELECT extversion FROM pg_extension WHERE extname = 'age'")
        if not version_result:
            print("AGE扩展未安装")
            await conn.close()
            return False
            
        age_version = version_result['extversion']
        print(f"AGE版本: {age_version}")
        
        # 尝试删除图
        try:
            # 使用 drop_graph 函数删除图
            await conn.execute(f"SELECT * FROM ag_catalog.drop_graph('{graph_name}', true)")
            print(f"已删除图 {graph_name}")
        except Exception as e:
            if "does not exist" in str(e):
                print(f"图 {graph_name} 不存在，无需删除")
            else:
                print(f"删除图时出错: {e}")
        
        # 创建新图
        try:
            await conn.execute(f"SELECT * FROM ag_catalog.create_graph('{graph_name}')")
            print(f"已创建新图 {graph_name}")
        except Exception as e:
            print(f"创建图时出错: {e}")
            await conn.close()
            return False
        
        # 关闭数据库连接
        await conn.close()
        print("数据库连接已关闭")
        
        return True
    except Exception as e:
        print(f"重置图时出错: {e}")
        return False


async def main_async():
    """异步主函数"""
    parser = argparse.ArgumentParser(description='重置 AGE 图')
    parser.add_argument('--host', default='localhost', help='数据库主机')
    parser.add_argument('--port', type=int, default=5432, help='数据库端口')
    parser.add_argument('--user', default='postgres', help='数据库用户名')
    parser.add_argument('--password', default='postgres', help='数据库密码')
    parser.add_argument('--database', default='postgres', help='数据库名称')
    parser.add_argument('--graph', default='cypher_graph', help='图名称')
    
    args = parser.parse_args()
    
    success = await reset_graph(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        database=args.database,
        graph_name=args.graph
    )
    
    if success:
        print("图重置成功")
    else:
        print("图重置失败")


def main():
    """主函数"""
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
