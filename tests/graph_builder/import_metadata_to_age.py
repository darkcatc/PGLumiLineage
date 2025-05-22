#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
将lumi_metadata_store中的元数据导入到AGE图中

使用方法:
    python -m tests.graph_builder.import_metadata_to_age
"""

import asyncio
import logging
import sys
from pathlib import Path
from typing import Dict, Any

# 添加项目根目录到Python路径
project_root = str(Path(__file__).parent.parent.parent)
if project_root not in sys.path:
    sys.path.append(project_root)

# 使用测试配置
from tests.graph_builder.test_settings import get_settings_instance
from pglumilineage.graph_builder.metadata_graph_builder import MetadataGraphBuilder

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

async def import_metadata_to_age():
    """将元数据从lumi_metadata_store导入到AGE图中"""
    try:
        # 获取配置
        settings = get_settings_instance()
        
        # 构建元数据数据库配置
        metadata_db_config = {
            'user': settings.INTERNAL_DB.USER,
            'password': settings.INTERNAL_DB.PASSWORD.get_secret_value(),
            'host': settings.INTERNAL_DB.HOST,
            'port': settings.INTERNAL_DB.PORT,
            'database': settings.INTERNAL_DB.DB_RAW_LOGS,
        }
        
        # 构建AGE数据库配置
        age_db_config = {
            'user': settings.INTERNAL_DB.USER,
            'password': settings.INTERNAL_DB.PASSWORD.get_secret_value(),
            'host': settings.INTERNAL_DB.HOST,
            'port': settings.INTERNAL_DB.PORT,
            'database': settings.INTERNAL_DB.DB_AGE,
        }
        
        # 创建MetadataGraphBuilder实例
        logger.info("正在初始化MetadataGraphBuilder...")
        graph_name = "lumi_graph"
        builder = MetadataGraphBuilder(metadata_db_config, age_db_config, graph_name)
        
        # 获取所有激活的数据源
        logger.info("正在获取激活的数据源...")
        sources = await builder.get_active_data_sources()
        logger.info(f"找到 {len(sources)} 个激活的数据源")
        
        # 遍历每个数据源，获取其元数据并构建图
        for source in sources:
            source_id = source['source_id']
            source_name = source['source_name']
            logger.info(f"正在处理数据源: {source_name} (ID: {source_id})")
            
            # 获取对象元数据
            objects = await builder.get_objects_metadata(source_id)
            logger.info(f"找到 {len(objects)} 个数据库对象")
            
            # 获取对象ID列表以获取列元数据
            object_ids = [obj['object_id'] for obj in objects]
            if object_ids:
                # 获取列元数据
                columns = await builder.get_columns_metadata(object_ids)
                logger.info(f"找到 {len(columns)} 个列定义")
            
            # 获取函数元数据
            functions = await builder.get_functions_metadata(source_id)
            logger.info(f"找到 {len(functions)} 个函数定义")
        
        logger.info("元数据处理完成！")
        
    except Exception as e:
        logger.error(f"导入元数据到AGE图时出错: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    try:
        asyncio.run(import_metadata_to_age())
    except KeyboardInterrupt:
        logger.info("用户中断操作")
        sys.exit(0)
    except Exception as e:
        logger.error(f"程序执行出错: {str(e)}")
        sys.exit(1)
