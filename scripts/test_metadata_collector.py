#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
测试元数据收集器

此脚本用于测试修改后的元数据收集器是否能正确处理 database_name 字段。
它会连接到数据库，收集一些元数据，并验证 database_name 字段是否被正确设置和保存。

作者: Vance Chen
"""

import asyncio
import logging
import sys
import os
from typing import Dict, Any, List

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pglumilineage.common import config, db_utils
from pglumilineage.metadata_collector import service as metadata_service
from pglumilineage.common.models import DataSourceConfig

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(module)s.%(funcName)s:%(lineno)d] - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_metadata_collection():
    """测试元数据收集功能"""
    logger.info("开始测试元数据收集功能")
    
    # 创建一个测试用的数据源配置
    source_config = DataSourceConfig(
        source_id=1,
        source_name="测试数据源",
        source_type="postgresql",
        host="localhost",
        port=5432,
        database="tpcds",  # 使用tpcds数据库名称来测试
        username="postgres",
        password="postgres",
        enabled=True
    )
    
    try:
        # 连接到源数据库
        conn = await metadata_service.get_source_db_connection(source_config)
        
        try:
            # 获取对象元数据
            logger.info("获取对象元数据")
            objects_metadata = await metadata_service.fetch_objects_metadata(conn, source_config)
            
            # 验证对象元数据中的 database_name 字段
            if objects_metadata:
                logger.info(f"获取到 {len(objects_metadata)} 个对象元数据")
                for i, obj in enumerate(objects_metadata[:5]):  # 只显示前5个
                    logger.info(f"对象 {i+1}: {obj.schema_name}.{obj.object_name} (database_name: {obj.database_name})")
            else:
                logger.warning("没有获取到对象元数据")
            
            # 获取函数元数据
            logger.info("获取函数元数据")
            functions_metadata = await metadata_service.fetch_functions_metadata(conn, source_config)
            
            # 验证函数元数据中的 database_name 字段
            if functions_metadata:
                logger.info(f"获取到 {len(functions_metadata)} 个函数元数据")
                for i, func in enumerate(functions_metadata[:5]):  # 只显示前5个
                    logger.info(f"函数 {i+1}: {func.schema_name}.{func.function_name} (database_name: {func.database_name})")
            else:
                logger.warning("没有获取到函数元数据")
            
            # 保存所有元数据以测试保存功能
            if objects_metadata:
                logger.info("测试保存对象元数据")
                object_ids = await metadata_service.save_objects_metadata(objects_metadata)
                logger.info(f"成功保存 {len(object_ids)} 个对象元数据，IDs: {object_ids}")
            
            if functions_metadata:
                logger.info("测试保存函数元数据")
                function_ids = await metadata_service.save_functions_metadata(functions_metadata[:5])
                logger.info(f"成功保存 {len(function_ids)} 个函数元数据，IDs: {function_ids}")
            
            # 验证保存的元数据
            logger.info("验证保存的元数据")
            
            # 使用RAW_LOGS_DSN创建连接
            dsn = str(config.settings.RAW_LOGS_DSN)
            metadata_conn = await asyncpg.connect(dsn=dsn)
            
            try:
                # 验证对象元数据
                if objects_metadata:
                    obj = objects_metadata[0]
                    query = """
                    SELECT database_name FROM lumi_metadata_store.objects_metadata
                    WHERE source_id = $1 AND schema_name = $2 AND object_name = $3 AND object_type = $4
                    """
                    db_name = await metadata_conn.fetchval(
                        query, obj.source_id, obj.schema_name, obj.object_name, obj.object_type
                    )
                    logger.info(f"从数据库中验证对象元数据的 database_name: {db_name}")
                
                # 验证函数元数据
                if functions_metadata:
                    func = functions_metadata[0]
                    query = """
                    SELECT database_name FROM lumi_metadata_store.functions_metadata
                    WHERE source_id = $1 AND schema_name = $2 AND function_name = $3
                    """
                    db_name = await metadata_conn.fetchval(
                        query, func.source_id, func.schema_name, func.function_name
                    )
                    logger.info(f"从数据库中验证函数元数据的 database_name: {db_name}")
            finally:
                await metadata_conn.close()
                
        finally:
            await conn.close()
            
        logger.info("元数据收集测试完成")
        
    except Exception as e:
        logger.error(f"测试过程中发生错误: {str(e)}")
        raise


async def main():
    """主函数"""
    try:
        # 初始化配置 - 使用全局配置实例
        # config.settings 会自动通过 LazySettings 类的 __getattr__ 方法加载配置
        logger.info(f"使用配置: {config.settings}")
        
        # 运行测试
        await test_metadata_collection()
        
    except Exception as e:
        logger.error(f"测试失败: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    # 导入 asyncpg，放在这里避免循环导入
    import asyncpg
    
    # 运行主函数
    asyncio.run(main())
