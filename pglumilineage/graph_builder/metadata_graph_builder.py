#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
元数据图谱构建模块

该模块负责从元数据存储中读取数据库对象元数据，并将其转换为AGE图数据库中的节点和关系。
主要功能包括：
1. 从lumi_config.data_sources读取数据源配置
2. 从lumi_metadata_store读取对象元数据
3. 生成Cypher语句构建图谱
4. 执行Cypher语句更新AGE图数据库

作者: Vance Chen
"""

import asyncio
import logging
import re
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple, Set

import asyncpg

from pglumilineage.common import models
from pglumilineage.graph_builder.common_graph_utils import (
    generate_datasource_fqn,
    generate_database_fqn,
    generate_schema_fqn,
    generate_object_fqn,
    generate_column_fqn,
    generate_function_fqn,
    execute_cypher as common_execute_cypher,
    NODE_LABEL_DATASOURCE,
    NODE_LABEL_DATABASE,
    NODE_LABEL_SCHEMA,
    NODE_LABEL_TABLE,
    NODE_LABEL_VIEW,
    NODE_LABEL_MATERIALIZED_VIEW,
    NODE_LABEL_COLUMN,
    NODE_LABEL_FUNCTION,
    REL_TYPE_CONFIGURES,
    REL_TYPE_HAS_SCHEMA,
    REL_TYPE_HAS_OBJECT,
    REL_TYPE_HAS_COLUMN,
    REL_TYPE_REFERENCES,
    REL_TYPE_HAS_FUNCTION
)

# 设置日志
logger = logging.getLogger(__name__)


class MetadataGraphBuilder:
    """
    元数据图谱构建器
    
    负责从元数据存储中读取数据，并将其转换为AGE图数据库中的节点和关系。
    """
    
    def __init__(self, 
                 metadata_db_config: Dict[str, Any],
                 age_db_config: Dict[str, Any],
                 graph_name: str = "metadata_graph"):
        """
        初始化元数据图谱构建器
        
        Args:
            metadata_db_config: 元数据数据库连接配置
            age_db_config: AGE图数据库连接配置
            graph_name: AGE图名称
        """
        self.metadata_db_config = metadata_db_config
        self.age_db_config = age_db_config
        self.graph_name = graph_name
        
    async def _get_metadata_db_conn(self) -> asyncpg.Connection:
        """获取元数据数据库连接"""
        return await asyncpg.connect(**self.metadata_db_config)
    
    async def _get_age_db_conn(self) -> asyncpg.Connection:
        """获取AGE图数据库连接"""
        return await asyncpg.connect(**self.age_db_config)
    
    async def get_active_data_sources(self) -> List[Dict[str, Any]]:
        """
        获取所有激活的数据源配置
        
        Returns:
            List[Dict[str, Any]]: 数据源配置列表
            
        Raises:
            Exception: 如果无法获取数据源配置或表结构不匹配
        """
        conn = await self._get_metadata_db_conn()
        try:
            # 首先检查表结构
            table_info = await conn.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'lumi_config' 
                AND table_name = 'data_sources'
            """)
            
            if not table_info:
                raise Exception("表 lumi_config.data_sources 不存在")
                
            # 获取所有列名
            columns = [row['column_name'] for row in table_info]
            logger.debug(f"lumi_config.data_sources 表结构: {columns}")
            
            # 构建基础查询
            query = """
            SELECT *
            FROM lumi_config.data_sources
            WHERE is_active = TRUE
            """
            
            rows = await conn.fetch(query)
            
            # 将结果转换为字典列表
            result = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(row.keys()):
                    row_dict[col] = row[i]
                result.append(row_dict)
                
            logger.info(f"成功获取 {len(result)} 个激活的数据源")
            return result
            
        except Exception as e:
            logger.error(f"获取数据源配置时出错: {str(e)}")
            raise
        finally:
            await conn.close()
    
    async def get_objects_metadata(self, source_id: int) -> List[Dict[str, Any]]:
        """
        获取指定数据源的对象元数据
        
        Args:
            source_id: 数据源ID
            
        Returns:
            List[Dict[str, Any]]: 对象元数据列表
        """
        conn = await self._get_metadata_db_conn()
        try:
            query = """
            SELECT object_id, source_id, database_name, schema_name, object_name, 
                   object_type, owner, description, definition, row_count, 
                   last_analyzed, properties
            FROM lumi_metadata_store.objects_metadata
            WHERE source_id = $1
            """
            rows = await conn.fetch(query, source_id)
            return [dict(row) for row in rows]
        finally:
            await conn.close()
    
    async def get_columns_metadata(self, object_ids: List[int]) -> List[Dict[str, Any]]:
        """
        获取指定对象的列元数据
        
        Args:
            object_ids: 对象ID列表
            
        Returns:
            List[Dict[str, Any]]: 列元数据列表
            
        Raises:
            Exception: 如果无法获取列元数据或表结构不匹配
        """
        if not object_ids:
            return []
            
        conn = await self._get_metadata_db_conn()
        try:
            # 首先检查表结构
            table_info = await conn.fetch("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'lumi_metadata_store' 
                AND table_name = 'columns_metadata'
            """)
            
            if not table_info:
                raise Exception("表 lumi_metadata_store.columns_metadata 不存在")
                
            # 获取所有列名
            columns = [row['column_name'] for row in table_info]
            logger.debug(f"lumi_metadata_store.columns_metadata 表结构: {columns}")
            
            # 构建基础查询
            query = """
            SELECT *
            FROM lumi_metadata_store.columns_metadata
            WHERE object_id = ANY($1::bigint[])
            ORDER BY object_id, ordinal_position
            """
            
            rows = await conn.fetch(query, object_ids)
            
            # 将结果转换为字典列表
            result = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(row.keys()):
                    row_dict[col] = row[i]
                result.append(row_dict)
                
            logger.info(f"成功获取 {len(result)} 个列定义")
            return result
            
        except Exception as e:
            logger.error(f"获取列元数据时出错: {str(e)}")
            raise
        finally:
            await conn.close()
    
    async def get_functions_metadata(self, source_id: int) -> List[Dict[str, Any]]:
        """
        获取指定数据源的函数元数据
        
        Args:
            source_id: 数据源ID
            
        Returns:
            List[Dict[str, Any]]: 函数元数据列表
        """
        conn = await self._get_metadata_db_conn()
        try:
            query = """
            SELECT function_id, source_id, database_name, schema_name, function_name,
                   function_type, return_type, parameters, parameter_types, 
                   definition, language, owner, description, properties
            FROM lumi_metadata_store.functions_metadata
            WHERE source_id = $1
            """
            rows = await conn.fetch(query, source_id)
            return [dict(row) for row in rows]
        finally:
            await conn.close()
    
    # FQN生成函数已移至common_graph_utils模块
    
    # Cypher相关工具函数已移至common_graph_utils模块
    
    async def execute_cypher(self, cypher_stmt: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        执行Cypher语句
        
        这是对common_graph_utils.execute_cypher的包装方法，自动处理数据库连接的获取和释放。
        主要功能：
        1. 获取数据库连接
        2. 使用common_execute_cypher执行Cypher语句
        3. 确保连接被正确关闭
        
        Args:
            cypher_stmt: Cypher语句
            params: 参数字典
            
        Returns:
            List[Dict[str, Any]]: 查询结果
            
        See Also:
            common_graph_utils.execute_cypher: 底层的Cypher执行函数
        """
        conn = await self._get_age_db_conn()
        try:
            return await common_execute_cypher(conn, cypher_stmt, params, self.graph_name)
        finally:
            await conn.close()
    
    def generate_datasource_node_cypher(self, source: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        生成数据源节点的Cypher语句
        
        Args:
            source: 数据源信息
            
        Returns:
            Tuple[str, Dict[str, Any]]: Cypher语句和参数字典
        """
        source_id = source['source_id']
        source_name = source['source_name']
        fqn = generate_datasource_fqn(source_id, source_name)
        
        cypher = """
        MERGE (ds:DataSource {fqn: $fqn})
        ON CREATE SET 
            ds.name = $name,
            ds.source_id = $source_id,
            ds.host = $host,
            ds.port = $port,
            ds.description = $description,
            ds.is_active = $is_active,
            ds.created_at = datetime(),
            ds.updated_at = datetime()
        ON MATCH SET
            ds.name = $name,
            ds.host = $host,
            ds.port = $port,
            ds.description = $description,
            ds.is_active = $is_active,
            ds.updated_at = datetime()
        RETURN ds
        """
        
        params = {
            "fqn": fqn,
            "name": source_name,
            "source_id": source_id,
            "host": source.get('host'),
            "port": source.get('port'),
            "description": source.get('description'),
            "is_active": source.get('is_active', True)
        }
        
        return cypher, params
    
    def generate_database_node_cypher(self, source_name: str, database_name: str, source_id: int) -> Tuple[str, Dict[str, Any]]:
        """
        生成数据库节点的Cypher语句
        
        Args:
            source_name: 数据源名称
            database_name: 数据库名称
            source_id: 数据源ID
            
        Returns:
            Tuple[str, Dict[str, Any]]: Cypher语句和参数字典
        """
        db_fqn = generate_database_fqn(source_name, database_name)
        datasource_fqn = generate_datasource_fqn(source_id, source_name)
        
        cypher = """
        MERGE (db:Database {fqn: $fqn})
        ON CREATE SET 
            db.name = $name,
            db.datasource_name = $datasource_name,
            db.source_id = $source_id,
            db.created_at = datetime(),
            db.updated_at = datetime()
        ON MATCH SET
            db.name = $name,
            db.datasource_name = $datasource_name,
            db.source_id = $source_id,
            db.updated_at = datetime()
        WITH db
        MATCH (ds:DataSource {fqn: $datasource_fqn})
        MERGE (ds)-[r:CONFIGURES_DATABASE]->(db)
        ON CREATE SET
            r.created_at = datetime(),
            r.updated_at = datetime()
        ON MATCH SET
            r.updated_at = datetime()
        RETURN db
        """
        
        params = {
            "fqn": db_fqn,
            "name": database_name,
            "datasource_name": source_name,
            "source_id": source_id,
            "datasource_fqn": datasource_fqn
        }
        
        return cypher, params
    
    def generate_schema_node_cypher(self, database_fqn: str, schema_name: str, owner: str = None) -> Tuple[str, Dict[str, Any]]:
        """
        生成模式节点的Cypher语句
        
        Args:
            database_fqn: 数据库FQN
            schema_name: 模式名称
            owner: 所有者
            
        Returns:
            Tuple[str, Dict[str, Any]]: Cypher语句和参数字典
        """
        schema_fqn = generate_schema_fqn(database_fqn, schema_name)
        
        cypher = """
        MERGE (schema:Schema {fqn: $fqn})
        ON CREATE SET
            schema.name = $name,
            schema.database_fqn = $database_fqn,
            schema.owner = $owner,
            schema.created_at = datetime(),
            schema.updated_at = datetime()
        ON MATCH SET
            schema.name = $name,
            schema.owner = $owner,
            schema.updated_at = datetime()
        WITH schema
        MATCH (db:Database {fqn: $database_fqn})
        MERGE (db)-[r:HAS_SCHEMA]->(schema)
        ON CREATE SET
            r.created_at = datetime(),
            r.updated_at = datetime()
        ON MATCH SET
            r.updated_at = datetime()
        RETURN schema
        """
        
        params = {
            "fqn": schema_fqn,
            "name": schema_name,
            "database_fqn": database_fqn,
            "owner": owner
        }
        
        return cypher, params
    
    def generate_object_node_cypher(self, schema_fqn: str, object_info: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        生成对象节点的Cypher语句
        
        Args:
            schema_fqn: 模式FQN
            object_info: 对象信息
            
        Returns:
            Tuple[str, Dict[str, Any]]: Cypher语句和参数字典
        """
        object_name = object_info['object_name']
        object_type = object_info['object_type'].upper()  # TABLE, VIEW, MATERIALIZED_VIEW
        object_fqn = generate_object_fqn(schema_fqn, object_name)
        
        # 将对象类型转换为有效的节点标签
        node_label = object_type.replace(' ', '_')
        
        cypher = f"""
        MERGE (obj:{node_label} {{fqn: $fqn}})
        ON CREATE SET
            obj.name = $name,
            obj.schema_fqn = $schema_fqn,
            obj.object_type = $object_type,
            obj.owner = $owner,
            obj.description = $description,
            obj.definition_sql = $definition_sql,
            obj.properties = $properties,
            obj.row_count = $row_count,
            obj.size_bytes = $size_bytes,
            obj.last_analyzed = $last_analyzed,
            obj.created_at = datetime(),
            obj.updated_at = datetime()
        ON MATCH SET
            obj.name = $name,
            obj.owner = $owner,
            obj.description = $description,
            obj.definition_sql = $definition_sql,
            obj.properties = $properties,
            obj.row_count = $row_count,
            obj.size_bytes = $size_bytes,
            obj.last_analyzed = $last_analyzed,
            obj.updated_at = datetime()
        WITH obj
        MATCH (schema:Schema {{fqn: $schema_fqn}})
        MERGE (schema)-[r:HAS_OBJECT]->(obj)
        ON CREATE SET
            r.created_at = datetime(),
            r.updated_at = datetime()
        ON MATCH SET
            r.updated_at = datetime()
        RETURN obj
        """
        
        params = {
            "fqn": object_fqn,
            "name": object_name,
            "schema_fqn": schema_fqn,
            "object_type": object_type,
            "owner": object_info.get('owner'),
            "description": object_info.get('description'),
            "definition_sql": object_info.get('definition_sql'),
            "properties": object_info.get('properties', {}),
            "row_count": object_info.get('row_count'),
            "size_bytes": object_info.get('size_bytes'),
            "last_analyzed": object_info.get('last_analyzed')
        }
        
        return cypher, params
        
    def generate_column_node_cypher(self, object_fqn: str, column_info: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        """
        生成列节点的Cypher语句
        
        Args:
            object_fqn: 所属对象的FQN
            column_info: 列信息
            
        Returns:
            Tuple[str, Dict[str, Any]]: Cypher语句和参数字典
        """
        column_name = column_info['column_name']
        column_fqn = f"{object_fqn}.{column_name}"
        
        # 处理外键关系
        fk_cypher = ""
        fk_params = {}
        
        if all(key in column_info for key in ['foreign_key_to_table_schema', 
                                             'foreign_key_to_table_name',
                                             'foreign_key_to_column_name']):
            # 生成目标列的FQN
            target_schema_fqn = f"{object_fqn.rsplit('.', 1)[0]}.{column_info['foreign_key_to_table_schema']}"
            target_object_fqn = f"{target_schema_fqn}.{column_info['foreign_key_to_table_name']}"
            target_column_fqn = f"{target_object_fqn}.{column_info['foreign_key_to_column_name']}"
            
            fk_cypher = """
            WITH col
            MATCH (target_col:Column {fqn: $target_column_fqn})
            MERGE (col)-[fk_rel:REFERENCES_COLUMN {constraint_name: $constraint_name}]->(target_col)
            ON CREATE SET
                fk_rel.created_at = datetime(),
                fk_rel.updated_at = datetime()
            ON MATCH SET
                fk_rel.updated_at = datetime()
            """
            
            fk_params = {
                'target_column_fqn': target_column_fqn,
                'constraint_name': column_info.get('constraint_name', '') or f"fk_{column_name}"
            }
        
        # 构建列节点的Cypher语句
        cypher = """
        MERGE (col:Column {fqn: $fqn})
        ON CREATE SET
            col.name = $name,
            col.object_fqn = $object_fqn,
            col.data_type = $data_type,
            col.ordinal_position = $ordinal_position,
            col.character_maximum_length = $character_maximum_length,
            col.numeric_precision = $numeric_precision,
            col.numeric_scale = $numeric_scale,
            col.is_nullable = $is_nullable,
            col.default_value = $default_value,
            col.is_primary_key = $is_primary_key,
            col.is_unique = $is_unique,
            col.description = $description,
            col.created_at = datetime(),
            col.updated_at = datetime()
        ON MATCH SET
            col.name = $name,
            col.data_type = $data_type,
            col.ordinal_position = $ordinal_position,
            col.character_maximum_length = $character_maximum_length,
            col.numeric_precision = $numeric_precision,
            col.numeric_scale = $numeric_scale,
            col.is_nullable = $is_nullable,
            col.default_value = $default_value,
            col.is_primary_key = $is_primary_key,
            col.is_unique = $is_unique,
            col.description = $description,
            col.updated_at = datetime()
        WITH col
        MATCH (obj {fqn: $object_fqn})
        MERGE (obj)-[r:HAS_COLUMN]->(col)
        ON CREATE SET
            r.created_at = datetime(),
            r.updated_at = datetime()
        ON MATCH SET
            r.updated_at = datetime()
        """ + fk_cypher + """
        RETURN col
        """
        
        # 准备参数
        params = {
            'fqn': column_fqn,
            'name': column_name,
            'object_fqn': object_fqn,
            'data_type': column_info.get('data_type'),
            'ordinal_position': column_info.get('ordinal_position'),
            'character_maximum_length': column_info.get('character_maximum_length'),
            'numeric_precision': column_info.get('numeric_precision'),
            'numeric_scale': column_info.get('numeric_scale'),
            'is_nullable': column_info.get('is_nullable', True),
            'default_value': column_info.get('default_value'),
            'is_primary_key': column_info.get('is_primary_key', False),
            'is_unique': column_info.get('is_unique', False),
            'description': column_info.get('description'),
            **fk_params
        }
        
        return cypher, params
