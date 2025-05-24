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
from pglumilineage.graph_builder.common_graph_utils import ensure_age_graph_exists, execute_cypher as common_execute_cypher
import asyncpg

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
        
        # 确保AGE图存在
        conn = await asyncpg.connect(**age_db_config)
        try:
            if not await ensure_age_graph_exists(conn, graph_name):
                logger.error(f"无法创建或验证AGE图: {graph_name}")
                return
            logger.info(f"已确保AGE图存在: {graph_name}")
        finally:
            await conn.close()
            
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
            
            # 1. 创建数据源节点
            cypher, params = builder.generate_datasource_node_cypher(source)
            await builder.execute_cypher(cypher, params)
            logger.info(f"已创建数据源节点: {source_name} (ID: {source_id})")
            
            # 获取对象元数据
            objects = await builder.get_objects_metadata(source_id)
            logger.info(f"找到 {len(objects)} 个数据库对象")
            
            if not objects:
                logger.warning(f"数据源 {source_name} 没有对象元数据，跳过")
                continue
                
            # 2. 创建数据库和schema节点
            db_schemas = {}
            for obj in objects:
                db_name = obj['database_name']
                schema_name = obj['schema_name']
                
                # 创建数据库节点（如果还没创建）
                if db_name not in db_schemas:
                    db_cypher, db_params = builder.generate_database_node_cypher(
                        source_name, db_name, source_id
                    )
                    await builder.execute_cypher(db_cypher, db_params)
                    logger.info(f"已创建数据库节点: {db_name}")
                    db_schemas[db_name] = set()
                
                # 创建schema节点（如果还没创建）
                if schema_name not in db_schemas[db_name]:
                    db_fqn = f"{source_name}.{db_name}"
                    schema_cypher, schema_params = builder.generate_schema_node_cypher(
                        db_fqn, schema_name, obj.get('owner')
                    )
                    await builder.execute_cypher(schema_cypher, schema_params)
                    logger.info(f"已创建schema节点: {db_name}.{schema_name}")
                    db_schemas[db_name].add(schema_name)
            
            # 3. 创建对象节点
            for obj in objects:
                db_name = obj['database_name']
                schema_name = obj['schema_name']
                schema_fqn = f"{source_name}.{db_name}.{schema_name}"
                
                obj_cypher, obj_params = builder.generate_object_node_cypher(
                    schema_fqn, obj
                )
                await builder.execute_cypher(obj_cypher, obj_params)
                logger.info(f"已创建对象节点: {schema_fqn}.{obj['object_name']}")
            
            # 4. 创建列节点
            object_ids = [obj['object_id'] for obj in objects]
            if object_ids:
                columns = await builder.get_columns_metadata(object_ids)
                logger.info(f"找到 {len(columns)} 个列定义")
                
                # 创建对象ID到FQN的映射
                object_fqn_map = {}
                for obj in objects:
                    db_name = obj['database_name']
                    schema_name = obj['schema_name']
                    object_name = obj['object_name']
                    object_fqn = f"{source_name}.{db_name}.{schema_name}.{object_name}"
                    object_fqn_map[obj['object_id']] = object_fqn
                
                # 创建列节点
                for col in columns:
                    object_id = col['object_id']
                    if object_id in object_fqn_map:
                        object_fqn = object_fqn_map[object_id]
                        col_cypher, col_params = builder.generate_column_node_cypher(
                            object_fqn, col
                        )
                        await builder.execute_cypher(col_cypher, col_params)
                        logger.info(f"已创建列节点: {object_fqn}.{col['column_name']}")
            
            # 获取函数元数据
            functions = await builder.get_functions_metadata(source_id)
            logger.info(f"找到 {len(functions)} 个函数定义")
        
        # 图空间已在脚本开始时通过ensure_age_graph_exists()创建
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
