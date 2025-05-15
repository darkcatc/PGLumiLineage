#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试列注释收集功能

此脚本用于验证列注释收集功能是否正常工作，包括：
1. 从源数据库中收集列注释
2. 将列注释保存到元数据存储中
3. 验证保存的列注释与源数据库中的一致
"""

import asyncio
import logging
import sys
from typing import List, Optional, Dict, Any

# 添加项目根目录到 Python 路径
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pglumilineage.metadata_collector import service as metadata_service
from pglumilineage.common import models
from pydantic import Field, SecretStr
from pglumilineage.common import db_utils, logging_config

# 设置日志
logging_config.setup_logging()
logger = logging.getLogger(__name__)

async def test_column_description_collection():
    """
    测试列注释收集功能
    
    此函数执行以下步骤：
    1. 创建测试表并添加列注释
    2. 收集对象和列元数据
    3. 保存元数据到存储中
    4. 验证列注释是否被正确收集和保存
    5. 清理测试数据
    """
    logger.info("开始测试列注释收集功能")
    
    # 创建数据源配置
    source_config = models.DataSourceConfig(
        source_id=1,
        source_name="TPC-DS",
        host="localhost",
        port=5432,
        database="tpcds",
        username="postgres",
        password=SecretStr("postgres")
    )
    
    try:
        # 连接到数据源
        logger.info(f"正在连接到数据源: {source_config.source_name} ({source_config.host}:{source_config.port}/{source_config.database})")
        conn = await metadata_service.get_source_db_connection(source_config)
        logger.info(f"成功连接到数据源: {source_config.source_name}")
        
        # 获取对象元数据
        logger.info("获取对象元数据")
        objects_metadata = await metadata_service.fetch_objects_metadata(conn, source_config)
        logger.info(f"获取到 {len(objects_metadata)} 个对象元数据")
        
        # 统计对象类型
        tables = [obj for obj in objects_metadata if obj.object_type == "TABLE"]
        views = [obj for obj in objects_metadata if obj.object_type == "VIEW"]
        materialized_views = [obj for obj in objects_metadata if obj.object_type == "MATERIALIZED_VIEW"]
        logger.info(f"表: {len(tables)}个, 视图: {len(views)}个, 物化视图: {len(materialized_views)}个")
        
        # 保存对象元数据以获取object_id
        logger.info("保存对象元数据")
        object_ids = await metadata_service.save_objects_metadata(objects_metadata)
        logger.info(f"成功保存 {len(object_ids)} 个对象元数据")
        
        # 查询已保存的对象ID
        await db_utils.init_db_pool()
        try:
            # 使用全局连接池
            for obj in objects_metadata:
                query = """
                SELECT object_id FROM lumi_metadata_store.objects_metadata
                WHERE source_id = $1 AND schema_name = $2 AND object_name = $3
                """
                obj.object_id = await db_utils.db_pool.fetchval(query, obj.source_id, obj.schema_name, obj.object_name)
                logger.info(f"对象 {obj.schema_name}.{obj.object_name} 的ID为 {obj.object_id}")
        finally:
            await db_utils.close_db_pool()
        
        # 创建测试表并添加列注释
        logger.info("创建测试表并添加列注释")
        test_schema = "public"
        test_table_name = "test_column_comments"
        test_column = "id"
        test_comment = "测试列注释 - 这是一个自动化测试添加的注释"
        
        # 删除可能存在的测试表
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {test_schema}.{test_table_name}")
            logger.info(f"已删除可能存在的测试表 {test_schema}.{test_table_name}")
        except Exception as e:
            logger.warning(f"删除测试表失败: {str(e)}")
        
        # 创建测试表
        try:
            await conn.execute(f"""
            CREATE TABLE {test_schema}.{test_table_name} (
                {test_column} INT PRIMARY KEY,
                name VARCHAR(100),
                value NUMERIC(10,2)
            )
            """)
            logger.info(f"已创建测试表 {test_schema}.{test_table_name}")
            
            # 添加列注释
            await conn.execute(f"""
            COMMENT ON COLUMN {test_schema}.{test_table_name}.{test_column} IS '{test_comment}';
            """)
            logger.info(f"已为列 {test_schema}.{test_table_name}.{test_column} 添加注释: {test_comment}")
        except Exception as e:
            logger.error(f"创建测试表或添加注释失败: {str(e)}")
            return
        
        # 重新获取对象元数据，确保包含新创建的测试表
        logger.info("重新获取对象元数据，包含新创建的测试表")
        objects_metadata = await metadata_service.fetch_objects_metadata(conn, source_config)
        logger.info(f"获取到 {len(objects_metadata)} 个对象元数据")
        
        # 保存对象元数据以获取object_id
        logger.info("保存对象元数据")
        object_ids = await metadata_service.save_objects_metadata(objects_metadata)
        logger.info(f"成功保存 {len(object_ids)} 个对象元数据")
        
        # 查询已保存的对象ID
        await db_utils.init_db_pool()
        try:
            # 使用全局连接池
            for obj in objects_metadata:
                query = """
                SELECT object_id FROM lumi_metadata_store.objects_metadata
                WHERE source_id = $1 AND schema_name = $2 AND object_name = $3
                """
                obj.object_id = await db_utils.db_pool.fetchval(query, obj.source_id, obj.schema_name, obj.object_name)
        finally:
            await db_utils.close_db_pool()
        
        # 找到测试表的元数据
        test_table = next((obj for obj in objects_metadata 
                          if obj.schema_name == test_schema and obj.object_name == test_table_name), None)
        
        if not test_table:
            logger.error(f"未找到测试表 {test_schema}.{test_table_name} 的元数据")
            return
            
        logger.info(f"测试表 {test_schema}.{test_table_name} 的ID为 {test_table.object_id}")
        
        # 只获取测试表的列元数据
        logger.info(f"获取测试表 {test_schema}.{test_table_name} 的列元数据")
        columns_metadata = await metadata_service.fetch_columns_metadata(
            conn, 
            test_table.object_id, 
            test_table.schema_name, 
            test_table.object_name, 
            source_config
        )
        
        # 打印带有注释的列
        columns_with_description = [col for col in columns_metadata if col.description]
        if columns_with_description:
            logger.info(f"测试表有 {len(columns_with_description)} 列带有注释")
            for col in columns_with_description:
                logger.info(f"  列 {col.column_name}: {col.description}")
        else:
            logger.warning("测试表没有列带有注释，这可能表明列注释收集功能不正常")
        
        # 保存列元数据
        logger.info("保存列元数据")
        column_ids = await metadata_service.save_columns_metadata(columns_metadata)
        logger.info(f"成功保存 {len(column_ids)} 个列元数据")
        
        # 验证保存的列注释
        logger.info("验证保存的列注释")
        await db_utils.init_db_pool()
        try:
            # 查询测试表的列注释
            query = """
            SELECT 
                o.schema_name, 
                o.object_name, 
                c.column_name, 
                c.description
            FROM 
                lumi_metadata_store.columns_metadata c
                JOIN lumi_metadata_store.objects_metadata o ON c.object_id = o.object_id
            WHERE 
                o.schema_name = $1 AND o.object_name = $2 AND c.column_name = $3
            """
            
            row = await db_utils.db_pool.fetchrow(query, test_schema, test_table_name, test_column)
            
            if row:
                saved_comment = row['description']
                logger.info(f"测试列 {test_schema}.{test_table_name}.{test_column} 的保存注释: {saved_comment}")
                
                # 验证注释是否一致
                if saved_comment == test_comment:
                    logger.info("✓ 验证通过: 保存的列注释与源数据库中的一致")
                else:
                    logger.error(f"✗ 验证失败: 保存的列注释与源数据库中的不一致")
                    logger.error(f"  期望: '{test_comment}'")
                    logger.error(f"  实际: '{saved_comment}'")
            else:
                logger.error(f"✗ 验证失败: 未找到测试列 {test_schema}.{test_table_name}.{test_column} 的元数据")
            
            # 查询所有带有注释的列（用于参考）
            all_comments_query = """
            SELECT 
                o.schema_name, 
                o.object_name, 
                c.column_name, 
                c.description
            FROM 
                lumi_metadata_store.columns_metadata c
                JOIN lumi_metadata_store.objects_metadata o ON c.object_id = o.object_id
            WHERE 
                c.description IS NOT NULL
            ORDER BY 
                o.schema_name, 
                o.object_name, 
                c.ordinal_position
            LIMIT 10
            """
            
            rows = await db_utils.db_pool.fetch(all_comments_query)
            logger.info(f"从数据库中查询到 {len(rows)} 个带有注释的列（参考信息）")
            
            for row in rows:
                logger.info(f"列 {row['schema_name']}.{row['object_name']}.{row['column_name']} 的注释: {row['description']}")
            
        finally:
            await db_utils.close_db_pool()
            
        # 清理测试数据
        logger.info("清理测试数据")
        try:
            await conn.execute(f"DROP TABLE IF EXISTS {test_schema}.{test_table_name}")
            logger.info(f"已删除测试表 {test_schema}.{test_table_name}")
        except Exception as e:
            logger.warning(f"删除测试表失败: {str(e)}")
        
        logger.info("列注释收集测试完成")
        
    except Exception as e:
        logger.error(f"测试过程中发生错误: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
    finally:
        # 关闭连接
        if 'conn' in locals() and conn:
            await conn.close()

if __name__ == "__main__":
    asyncio.run(test_column_description_collection())
