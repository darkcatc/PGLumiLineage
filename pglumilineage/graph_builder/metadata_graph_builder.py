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
from .common_graph_utils import (
    generate_datasource_fqn,
    generate_database_fqn,
    generate_schema_fqn,
    generate_object_fqn,
    generate_column_fqn,
    generate_function_fqn,
    convert_cypher_for_age,
    execute_cypher as common_execute_cypher
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
        """
        conn = await self._get_metadata_db_conn()
        try:
            query = """
            SELECT source_id, source_name, host, port, database_name, username, 
                   description, is_active, properties
            FROM lumi_config.data_sources
            WHERE is_active = TRUE
            """
            rows = await conn.fetch(query)
            return [dict(row) for row in rows]
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
        """
        if not object_ids:
            return []
            
        conn = await self._get_metadata_db_conn()
        try:
            query = """
            SELECT column_id, object_id, column_name, ordinal_position, data_type,
                   is_nullable, column_default, is_primary_key, is_unique, 
                   description, properties
            FROM lumi_metadata_store.columns_metadata
            WHERE object_id = ANY($1::bigint[])
            ORDER BY object_id, ordinal_position
            """
            rows = await conn.fetch(query, object_ids)
            return [dict(row) for row in rows]
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
        fqn = self.generate_datasource_fqn(source_id, source_name)
        
        cypher = """
        MERGE (ds {label: "datasource", fqn: $fqn}) 
        SET ds.name = $name,
            ds.source_id = $source_id,
            ds.host = $host,
            ds.port = $port,
            ds.description = $description,
            ds.is_active = $is_active,
            ds.updated_at = $updated_at,
            ds.created_at = COALESCE(ds.created_at, $created_at)
        RETURN ds
        """
        
        params = {
            "fqn": fqn,
            "name": source_name,
            "source_id": source_id,
            "host": source.get('host'),
            "port": source.get('port'),
            "description": source.get('description'),
            "is_active": source.get('is_active', True),
            "updated_at": datetime.now().isoformat(),
            "created_at": datetime.now().isoformat()
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
        db_fqn = self.generate_database_fqn(source_name, database_name)
        datasource_fqn = self.generate_datasource_fqn(source_id, source_name)
        
        cypher = """
        MERGE (db {label: "database", fqn: $fqn}) 
        SET db.name = $name,
            db.datasource_name = $datasource_name,
            db.source_id = $source_id,
            db.updated_at = $updated_at,
            db.created_at = COALESCE(db.created_at, $created_at)
        WITH db
        MATCH (ds {label: "datasource", fqn: $datasource_fqn})
        MERGE (ds)-[r {label: "configures_database"}]->(db)
        SET r.updated_at = $updated_at,
            r.created_at = COALESCE(r.created_at, $created_at)
        RETURN db
        """
        
        params = {
            "fqn": db_fqn,
            "name": database_name,
            "datasource_name": source_name,
            "source_id": source_id,
            "datasource_fqn": datasource_fqn,
            "updated_at": datetime.now().isoformat(),
            "created_at": datetime.now().isoformat()
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
        schema_fqn = self.generate_schema_fqn(database_fqn, schema_name)
        
        cypher = """
        MERGE (schema {label: "schema", fqn: $fqn}) 
        SET schema.name = $name,
            schema.database_fqn = $database_fqn,
            schema.owner = $owner,
            schema.updated_at = $updated_at,
            schema.created_at = COALESCE(schema.created_at, $created_at)
        WITH schema
        MATCH (db {label: "database", fqn: $database_fqn})
        MERGE (db)-[r {label: "has_schema"}]->(schema)
        SET r.updated_at = $updated_at,
            r.created_at = COALESCE(r.created_at, $created_at)
        RETURN schema
        """
        
        params = {
            "fqn": schema_fqn,
            "name": schema_name,
            "database_fqn": database_fqn,
            "owner": owner,
            "updated_at": datetime.now().isoformat(),
            "created_at": datetime.now().isoformat()
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
        object_type = object_info['object_type'].lower()
        object_fqn = self.generate_object_fqn(schema_fqn, object_name)
        
        cypher = """
        MERGE (obj {label: $object_type, fqn: $fqn}) 
        SET obj.name = $name,
            obj.schema_fqn = $schema_fqn,
            obj.owner = $owner,
            obj.description = $description,
            obj.definition = $definition,
            obj.row_count = $row_count,
            obj.last_analyzed = $last_analyzed,
            obj.updated_at = $updated_at,
            obj.created_at = COALESCE(obj.created_at, $created_at)
        WITH obj
        MATCH (schema {label: "schema", fqn: $schema_fqn})
        MERGE (schema)-[r {label: "has_object"}]->(obj)
        SET r.updated_at = $updated_at,
            r.created_at = COALESCE(r.created_at, $created_at)
        RETURN obj
        """
        
        params = {
            "fqn": object_fqn,
            "name": object_name,
            "object_type": object_type,
            "schema_fqn": schema_fqn,
            "owner": object_info.get('owner'),
            "description": object_info.get('description'),
            "definition": object_info.get('definition'),
            "row_count": object_info.get('row_count'),
            "last_analyzed": object_info.get('last_analyzed').isoformat() if object_info.get('last_analyzed') else None,
            "updated_at": datetime.now().isoformat(),
            "created_at": datetime.now().isoformat()
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
        column_fqn = self.generate_column_fqn(object_fqn, column_name)
        
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
            MATCH (target_col {label: 'column', fqn: $target_column_fqn})
            MERGE (col)-[r:REFERENCES_COLUMN {constraint_name: $constraint_name}]->(target_col)
            SET r.updated_at = $updated_at,
                r.created_at = COALESCE(r.created_at, $created_at)
            """
            
            fk_params = {
                'target_column_fqn': target_column_fqn,
                'constraint_name': column_info.get('constraint_name', '') or f"fk_{column_name}",
            }
        
        cypher = f"""
        MERGE (col {{label: 'column', fqn: $fqn}}) 
        SET col.name = $name,
            col.parent_object_fqn = $parent_object_fqn,
            col.ordinal_position = $ordinal_position,
            col.data_type = $data_type,
            col.max_length = $max_length,
            col.numeric_precision = $numeric_precision,
            col.numeric_scale = $numeric_scale,
            col.is_nullable = $is_nullable,
            col.default_value = $default_value,
            col.is_primary_key = $is_primary_key,
            col.is_unique = $is_unique,
            col.description = $description,
            col.updated_at = $updated_at,
            col.created_at = COALESCE(col.created_at, $created_at)
        WITH col
        MATCH (obj {{fqn: $parent_object_fqn}})
        MERGE (obj)-[r:HAS_COLUMN]->(col)
        SET r.updated_at = $updated_at,
            r.created_at = COALESCE(r.created_at, $created_at)
        {fk_cypher}
        RETURN col
        """
        
        params = {
            'fqn': column_fqn,
            'name': column_name,
            'parent_object_fqn': object_fqn,
            'ordinal_position': column_info.get('ordinal_position'),
            'data_type': column_info.get('data_type'),
            'max_length': column_info.get('max_length'),
            'numeric_precision': column_info.get('numeric_precision'),
            'numeric_scale': column_info.get('numeric_scale'),
            'is_nullable': column_info.get('is_nullable', True),
            'default_value': column_info.get('default_value'),
            'is_primary_key': column_info.get('is_primary_key', False),
            'is_unique': column_info.get('is_unique', False),
            'description': column_info.get('description'),
            'updated_at': datetime.now().isoformat(),
            'created_at': datetime.now().isoformat(),
            **fk_params
        }
        
        return cypher, params
