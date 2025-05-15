#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
单元测试：列注释收集功能
"""

import asyncio
import logging
import os
import sys
import unittest
from typing import Dict, Any, List, Optional

# 添加项目根目录到 Python 路径
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from pglumilineage.metadata_collector import service as metadata_service
from pglumilineage.common import models, db_utils, logging_config
from pydantic import SecretStr

# 设置日志
logging_config.setup_logging()
logger = logging.getLogger(__name__)


class TestColumnDescriptionCollector(unittest.TestCase):
    """测试列注释收集功能"""

    @classmethod
    def setUpClass(cls):
        """设置测试环境"""
        # 创建数据源配置
        cls.source_config = models.DataSourceConfig(
            source_id=1,
            source_name="TPC-DS",
            host="localhost",
            port=5432,
            database="tpcds",
            username="postgres",
            password=SecretStr("postgres")
        )
        
        # 初始化数据库连接池
        asyncio.run(db_utils.init_db_pool())
        
        # 创建测试表和列注释
        cls.test_table = "test_column_comments"
        cls.test_schema = "public"
        cls.test_column = "id"
        cls.test_comment = "这是一个测试列注释"
        
        # 创建测试表并添加注释
        asyncio.run(cls._setup_test_data())

    @classmethod
    async def _setup_test_data(cls):
        """设置测试数据"""
        # 连接到数据源
        conn = await metadata_service.get_source_db_connection(cls.source_config)
        try:
            # 删除可能存在的测试表
            await conn.execute(f"DROP TABLE IF EXISTS {cls.test_schema}.{cls.test_table}")
            
            # 创建测试表
            await conn.execute(f"""
            CREATE TABLE {cls.test_schema}.{cls.test_table} (
                {cls.test_column} INT PRIMARY KEY,
                name VARCHAR(100),
                value NUMERIC(10,2)
            )
            """)
            
            # 添加列注释
            await conn.execute(f"""
            COMMENT ON COLUMN {cls.test_schema}.{cls.test_table}.{cls.test_column} IS '{cls.test_comment}'
            """)
            
            logger.info(f"已创建测试表 {cls.test_schema}.{cls.test_table} 并添加列注释")
        finally:
            await conn.close()

    @classmethod
    def tearDownClass(cls):
        """清理测试环境"""
        # 删除测试表
        async def cleanup():
            conn = await metadata_service.get_source_db_connection(cls.source_config)
            try:
                await conn.execute(f"DROP TABLE IF EXISTS {cls.test_schema}.{cls.test_table}")
                logger.info(f"已删除测试表 {cls.test_schema}.{cls.test_table}")
            finally:
                await conn.close()
            
            # 清理元数据存储中的测试数据
            await db_utils.db_pool.execute(f"""
            DELETE FROM lumi_metadata_store.columns_metadata
            WHERE column_id IN (
                SELECT cm.column_id
                FROM lumi_metadata_store.columns_metadata cm
                JOIN lumi_metadata_store.objects_metadata om ON cm.object_id = om.object_id
                WHERE om.schema_name = '{cls.test_schema}' AND om.object_name = '{cls.test_table}'
            )
            """)
            
            await db_utils.db_pool.execute(f"""
            DELETE FROM lumi_metadata_store.objects_metadata
            WHERE schema_name = '{cls.test_schema}' AND object_name = '{cls.test_table}'
            """)
            
            logger.info("已清理元数据存储中的测试数据")
            
            # 关闭数据库连接池
            await db_utils.close_db_pool()
        
        asyncio.run(cleanup())

    def test_column_description_collection(self):
        """测试列注释收集功能"""
        # 执行元数据收集
        asyncio.run(self._run_metadata_collection())
        
        # 验证列注释是否正确收集
        result = asyncio.run(self._verify_column_description())
        
        # 断言结果
        self.assertIsNotNone(result, "未找到测试列的元数据")
        self.assertEqual(result["description"], self.test_comment, 
                         f"列注释不匹配，期望: '{self.test_comment}', 实际: '{result['description']}'")
        
        logger.info("列注释收集功能测试通过")

    async def _run_metadata_collection(self):
        """执行元数据收集"""
        # 连接到数据源
        conn = await metadata_service.get_source_db_connection(self.source_config)
        try:
            # 获取对象元数据
            objects_metadata = await metadata_service.fetch_objects_metadata(conn, self.source_config)
            
            # 保存对象元数据
            await metadata_service.save_objects_metadata(objects_metadata)
            
            # 查找测试表的对象ID
            test_object = next((obj for obj in objects_metadata 
                               if obj.schema_name == self.test_schema and obj.object_name == self.test_table), None)
            
            if not test_object:
                self.fail(f"未找到测试表 {self.test_schema}.{self.test_table}")
            
            # 查询已保存的对象ID
            query = """
            SELECT object_id FROM lumi_metadata_store.objects_metadata
            WHERE source_id = $1 AND schema_name = $2 AND object_name = $3
            """
            test_object.object_id = await db_utils.db_pool.fetchval(
                query, test_object.source_id, test_object.schema_name, test_object.object_name
            )
            
            # 获取列元数据
            columns_metadata = await metadata_service.fetch_columns_metadata(
                conn, test_object.object_id, test_object.schema_name, test_object.object_name, self.source_config
            )
            
            # 保存列元数据
            await metadata_service.save_columns_metadata(columns_metadata)
            
            logger.info(f"已完成测试表 {self.test_schema}.{self.test_table} 的元数据收集")
        finally:
            await conn.close()

    async def _verify_column_description(self) -> Optional[Dict[str, Any]]:
        """验证列注释是否正确收集"""
        # 查询列元数据
        query = """
        SELECT cm.* 
        FROM lumi_metadata_store.columns_metadata cm
        JOIN lumi_metadata_store.objects_metadata om ON cm.object_id = om.object_id
        WHERE om.schema_name = $1 AND om.object_name = $2 AND cm.column_name = $3
        """
        
        result = await db_utils.db_pool.fetchrow(
            query, self.test_schema, self.test_table, self.test_column
        )
        
        if result:
            return dict(result)
        return None


if __name__ == "__main__":
    unittest.main()
