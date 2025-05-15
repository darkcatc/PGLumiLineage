#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试元数据定义收集

此脚本用于测试修改后的元数据收集器是否能正确收集和存储表、视图和物化视图的定义。
它会连接到TPC-DS数据库，收集元数据，并验证definition字段是否被正确设置和保存。

作者: Vance Chen
"""

import asyncio
import logging
import sys
import os
from typing import Dict, Any, List, Optional
from pydantic import SecretStr

# 添加项目根目录到 Python 路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pglumilineage.common import config, db_utils
from pglumilineage.common.logging_config import setup_logging
from pglumilineage.metadata_collector import service as metadata_service
from pglumilineage.common.models import DataSourceConfig

# 配置日志
setup_logging()
logger = logging.getLogger(__name__)


async def test_metadata_definition_collection():
    """测试元数据定义收集功能"""
    logger.info("开始测试元数据定义收集功能")
    
    # 创建一个测试用的数据源配置（TPC-DS）
    source_config = DataSourceConfig(
        source_id=1,
        source_name="TPC-DS",
        host="localhost",
        port=5432,
        database="tpcds",
        username="postgres",
        password=SecretStr("postgres")
    )
    
    try:
        # 连接到源数据库
        conn = await metadata_service.get_source_db_connection(source_config)
        
        try:
            # 获取对象元数据
            logger.info("获取对象元数据")
            objects_metadata = await metadata_service.fetch_objects_metadata(conn, source_config)
            
            # 验证对象元数据中的 definition 字段
            if objects_metadata:
                logger.info(f"获取到 {len(objects_metadata)} 个对象元数据")
                
                # 按对象类型分类
                tables = [obj for obj in objects_metadata if obj.object_type == 'TABLE']
                views = [obj for obj in objects_metadata if obj.object_type == 'VIEW']
                mat_views = [obj for obj in objects_metadata if obj.object_type == 'MATERIALIZED VIEW']
                
                logger.info(f"表: {len(tables)}个, 视图: {len(views)}个, 物化视图: {len(mat_views)}个")
                
                # 检查表定义
                if tables:
                    logger.info("=== 表定义示例 ===")
                    for i, table in enumerate(tables[:3]):  # 只显示前3个
                        logger.info(f"表 {i+1}: {table.schema_name}.{table.object_name}")
                        if table.definition:
                            definition_preview = table.definition[:500] + "..." if len(table.definition) > 500 else table.definition
                            logger.info(f"定义: {definition_preview}")
                        else:
                            logger.warning(f"表 {table.schema_name}.{table.object_name} 没有定义")
                
                # 检查视图定义
                if views:
                    logger.info("=== 视图定义示例 ===")
                    for i, view in enumerate(views[:3]):  # 只显示前3个
                        logger.info(f"视图 {i+1}: {view.schema_name}.{view.object_name}")
                        if view.definition:
                            definition_preview = view.definition[:500] + "..." if len(view.definition) > 500 else view.definition
                            logger.info(f"定义: {definition_preview}")
                        else:
                            logger.warning(f"视图 {view.schema_name}.{view.object_name} 没有定义")
                
                # 检查物化视图定义
                if mat_views:
                    logger.info("=== 物化视图定义示例 ===")
                    for i, mat_view in enumerate(mat_views[:3]):  # 只显示前3个
                        logger.info(f"物化视图 {i+1}: {mat_view.schema_name}.{mat_view.object_name}")
                        if mat_view.definition:
                            definition_preview = mat_view.definition[:500] + "..." if len(mat_view.definition) > 500 else mat_view.definition
                            logger.info(f"定义: {definition_preview}")
                        else:
                            logger.warning(f"物化视图 {mat_view.schema_name}.{mat_view.object_name} 没有定义")
            else:
                logger.warning("没有获取到对象元数据")
            
            # 保存所有元数据以测试保存功能
            if objects_metadata:
                logger.info("保存对象元数据")
                object_ids = await metadata_service.save_objects_metadata(objects_metadata)
                logger.info(f"成功保存 {len(object_ids)} 个对象元数据")
            
            # 验证保存的元数据
            logger.info("验证保存的元数据定义")
            
            # 获取内部数据库连接
            pool = await db_utils.get_db_pool()
            
            try:
                async with pool.acquire() as metadata_conn:
                    # 验证表定义
                    if tables:
                        table = tables[0]
                        query = """
                        SELECT definition FROM lumi_metadata_store.objects_metadata
                        WHERE source_id = $1 AND schema_name = $2 AND object_name = $3 AND object_type = 'TABLE'
                        """
                        table_def = await metadata_conn.fetchval(
                            query, table.source_id, table.schema_name, table.object_name
                        )
                        if table_def:
                            definition_preview = table_def[:500] + "..." if len(table_def) > 500 else table_def
                            logger.info(f"从数据库中验证表定义: {definition_preview}")
                        else:
                            logger.warning(f"表 {table.schema_name}.{table.object_name} 在数据库中没有定义")
                    
                    # 验证视图定义
                    if views:
                        view = views[0]
                        query = """
                        SELECT definition FROM lumi_metadata_store.objects_metadata
                        WHERE source_id = $1 AND schema_name = $2 AND object_name = $3 AND object_type = 'VIEW'
                        """
                        view_def = await metadata_conn.fetchval(
                            query, view.source_id, view.schema_name, view.object_name
                        )
                        if view_def:
                            definition_preview = view_def[:500] + "..." if len(view_def) > 500 else view_def
                            logger.info(f"从数据库中验证视图定义: {definition_preview}")
                        else:
                            logger.warning(f"视图 {view.schema_name}.{view.object_name} 在数据库中没有定义")
                    
                    # 验证物化视图定义
                    if mat_views:
                        mat_view = mat_views[0]
                        query = """
                        SELECT definition FROM lumi_metadata_store.objects_metadata
                        WHERE source_id = $1 AND schema_name = $2 AND object_name = $3 AND object_type = 'MATERIALIZED VIEW'
                        """
                        mat_view_def = await metadata_conn.fetchval(
                            query, mat_view.source_id, mat_view.schema_name, mat_view.object_name
                        )
                        if mat_view_def:
                            definition_preview = mat_view_def[:500] + "..." if len(mat_view_def) > 500 else mat_view_def
                            logger.info(f"从数据库中验证物化视图定义: {definition_preview}")
                        else:
                            logger.warning(f"物化视图 {mat_view.schema_name}.{mat_view.object_name} 在数据库中没有定义")
            finally:
                await db_utils.close_db_pool()
                
        finally:
            await conn.close()
            
        logger.info("元数据定义收集测试完成")
        
    except Exception as e:
        logger.error(f"测试过程中发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())


async def main():
    """主函数"""
    try:
        await test_metadata_definition_collection()
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
