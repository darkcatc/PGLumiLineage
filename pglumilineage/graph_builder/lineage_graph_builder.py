#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
血缘图谱构建模块

该模块负责从分析完成的SQL模式中构建数据血缘关系图谱。
主要功能包括：
1. 从lumi_analytics.sql_patterns读取待处理的SQL模式
2. 将LLM提取的关系JSON转换为AGE图数据库中的节点和关系
3. 创建SqlPattern节点和DATA_FLOW关系
4. 建立SQL与数据库对象的引用关系

作者: Vance Chen
"""

import asyncio
import logging
import json
from datetime import datetime
from typing import List, Dict, Any, Optional, Tuple

import asyncpg

from pglumilineage.common import models
from pglumilineage.graph_builder.common_graph_utils import (
    execute_cypher as common_execute_cypher,
    generate_column_fqn,
    generate_object_fqn,
    generate_schema_fqn,
    generate_database_fqn,
    escape_cypher_string,
    format_properties,
    DEFAULT_GRAPH_NAME,
    NODE_LABEL_TABLE,
    NODE_LABEL_VIEW,
    NODE_LABEL_COLUMN,
    NODE_LABEL_SQL_PATTERN,
    NODE_LABEL_TEMP_TABLE,
    REL_TYPE_HAS_COLUMN,
    REL_TYPE_DATA_FLOW,
    REL_TYPE_READS_FROM,
    REL_TYPE_WRITES_TO
)

# 设置日志
logger = logging.getLogger(__name__)


class LineageGraphBuilder:
    """
    血缘图谱构建器
    
    负责从分析完成的SQL模式中读取数据，并将其转换为AGE图数据库中的血缘关系。
    """
    
    def __init__(self, 
                 analytics_db_config: Dict[str, Any],
                 age_db_config: Dict[str, Any],
                 graph_name: str = DEFAULT_GRAPH_NAME):
        """
        初始化血缘图谱构建器
        
        Args:
            analytics_db_config: 分析数据库连接配置
            age_db_config: AGE图数据库连接配置
            graph_name: AGE图名称
        """
        self.analytics_db_config = analytics_db_config
        self.age_db_config = age_db_config
        self.graph_name = graph_name
        self._analytics_pool: Optional[asyncpg.Pool] = None
        
    async def _get_analytics_db_conn(self) -> asyncpg.Connection:
        """获取分析数据库连接"""
        if self._analytics_pool is None:
            self._analytics_pool = await asyncpg.create_pool(**self.analytics_db_config)
        return await self._analytics_pool.acquire()
    
    async def _release_analytics_db_conn(self, conn: asyncpg.Connection):
        """释放分析数据库连接"""
        if self._analytics_pool:
            await self._analytics_pool.release(conn)
    
    async def _get_age_db_conn(self) -> asyncpg.Connection:
        """获取AGE图数据库连接"""
        return await asyncpg.connect(**self.age_db_config)
    
    async def close_analytics_pool(self):
        """关闭分析数据库连接池"""
        if self._analytics_pool:
            await self._analytics_pool.close()
            self._analytics_pool = None
    
    async def get_pending_sql_patterns_for_lineage(self, limit: int = 100) -> List[models.AnalyticalSQLPattern]:
        """
        获取待处理的SQL模式用于血缘分析
        
        从 lumi_analytics.sql_patterns 读取 llm_analysis_status = 'COMPLETED_SUCCESS' 
        且 is_loaded_to_age = FALSE 的记录。
        
        Args:
            limit: 批次处理数量限制
            
        Returns:
            List[models.AnalyticalSQLPattern]: 待处理的SQL模式列表
        """
        conn = await self._get_analytics_db_conn()
        try:
            query = """
            SELECT sql_hash, normalized_sql_text, sample_raw_sql_text, 
                   source_database_name, llm_extracted_relations_json,
                   first_seen_at, last_seen_at, execution_count, 
                   llm_analysis_status, is_loaded_to_age
            FROM lumi_analytics.sql_patterns
            WHERE llm_analysis_status = 'COMPLETED_SUCCESS' 
              AND (is_loaded_to_age = FALSE OR is_loaded_to_age IS NULL)
            ORDER BY last_seen_at DESC
            LIMIT $1
            """
            
            rows = await conn.fetch(query, limit)
            
            # 转换为Pydantic模型
            patterns = []
            for row in rows:
                try:
                    pattern = models.AnalyticalSQLPattern(
                        sql_hash=row['sql_hash'],
                        normalized_sql_text=row['normalized_sql_text'],
                        sample_raw_sql_text=row['sample_raw_sql_text'],
                        source_database_name=row['source_database_name'],
                        llm_extracted_relations_json=row['llm_extracted_relations_json'],
                        first_seen_at=row['first_seen_at'],
                        last_seen_at=row['last_seen_at'],
                        execution_count=row['execution_count'],
                        llm_analysis_status=row['llm_analysis_status'],
                        is_loaded_to_age=row['is_loaded_to_age']
                    )
                    patterns.append(pattern)
                except Exception as e:
                    logger.error(f"无法解析SQL模式 {row['sql_hash']}: {str(e)}")
                    continue
            
            logger.info(f"获取到 {len(patterns)} 个待处理的SQL模式")
            return patterns
            
        finally:
            await self._release_analytics_db_conn(conn)
    
    async def execute_cypher(self, cypher_stmt: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        执行Cypher语句
        
        这是对common_graph_utils.execute_cypher的包装方法，自动处理数据库连接的获取和释放。
        
        Args:
            cypher_stmt: Cypher语句
            params: 参数字典
            
        Returns:
            List[Dict[str, Any]]: 查询结果
        """
        conn = await self._get_age_db_conn()
        try:
            return await common_execute_cypher(conn, cypher_stmt, params, self.graph_name)
        finally:
            await conn.close()
    
    def _generate_cypher_for_sql_pattern_node(self, pattern_info: models.AnalyticalSQLPattern) -> Tuple[str, Dict[str, Any]]:
        """
        为SQL模式生成节点的Cypher语句
        
        Args:
            pattern_info: SQL模式信息
            
        Returns:
            Tuple[str, Dict[str, Any]]: Cypher语句和参数字典
        """
        cypher = f"""
        MERGE (sp:{NODE_LABEL_SQL_PATTERN} {{sql_hash: $sql_hash}})
        ON CREATE SET 
            sp.normalized_sql = $normalized_sql,
            sp.sample_sql = $sample_sql,
            sp.source_database_name = $source_database_name,
            sp.first_seen_at = $first_seen_at,
            sp.last_seen_at = $last_seen_at,
            sp.execution_count = $execution_count,
            sp.created_at = datetime(),
            sp.updated_at = datetime()
        ON MATCH SET
            sp.normalized_sql = $normalized_sql,
            sp.sample_sql = $sample_sql,
            sp.source_database_name = $source_database_name,
            sp.last_seen_at = $last_seen_at,
            sp.execution_count = $execution_count,
            sp.updated_at = datetime()
        RETURN sp
        """
        
        params = {
            "sql_hash": pattern_info.sql_hash,
            "normalized_sql": pattern_info.normalized_sql_text,
            "sample_sql": pattern_info.sample_raw_sql_text,
            "source_database_name": pattern_info.source_database_name,
            "first_seen_at": pattern_info.first_seen_at.isoformat() if pattern_info.first_seen_at else None,
            "last_seen_at": pattern_info.last_seen_at.isoformat() if pattern_info.last_seen_at else None,
            "execution_count": pattern_info.execution_count or 0
        }
        
        return cypher, params
    
    def _generate_cypher_for_object_node(self, obj_info: Dict[str, Any], database_name: str) -> Tuple[str, Dict[str, Any]]:
        """
        为对象节点生成Cypher语句
        
        优先尝试MATCH metadata_graph_builder已创建的节点，
        如果不存在则创建临时对象节点。
        
        Args:
            obj_info: 对象信息（包含schema, name, type）
            database_name: 数据库名称
            
        Returns:
            Tuple[str, Dict[str, Any]]: Cypher语句和参数字典
        """
        schema_name = obj_info.get("schema", "public")
        obj_name = obj_info.get("name")
        obj_type = obj_info.get("type", "TABLE")
        
        # 生成对象FQN
        db_fqn = generate_database_fqn(database_name, database_name)
        schema_fqn = generate_schema_fqn(db_fqn, schema_name)
        object_fqn = generate_object_fqn(schema_fqn, obj_name)
        
        if obj_type.upper() == "TABLE":
            # 优先尝试匹配metadata_graph_builder创建的Table节点
            cypher = f"""
            MERGE (obj:{NODE_LABEL_TABLE} {{fqn: $fqn}})
            ON CREATE SET
                obj.name = $name,
                obj.schema_name = $schema_name,
                obj.database_name = $database_name,
                obj.object_type = $object_type,
                obj.is_temporary = true,
                obj.created_at = datetime(),
                obj.updated_at = datetime()
            ON MATCH SET
                obj.updated_at = datetime()
            RETURN obj
            """
        elif obj_type.upper() == "VIEW":
            # 优先尝试匹配metadata_graph_builder创建的View节点
            cypher = f"""
            MERGE (obj:{NODE_LABEL_VIEW} {{fqn: $fqn}})
            ON CREATE SET
                obj.name = $name,
                obj.schema_name = $schema_name,
                obj.database_name = $database_name,
                obj.object_type = $object_type,
                obj.is_temporary = true,
                obj.created_at = datetime(),
                obj.updated_at = datetime()
            ON MATCH SET
                obj.updated_at = datetime()
            RETURN obj
            """
        else:
            # 对于其他类型或临时表，创建TempTable节点
            cypher = f"""
            MERGE (obj:{NODE_LABEL_TEMP_TABLE} {{fqn: $fqn}})
            ON CREATE SET
                obj.name = $name,
                obj.schema_name = $schema_name,
                obj.database_name = $database_name,
                obj.object_type = $object_type,
                obj.is_temporary = true,
                obj.created_at = datetime(),
                obj.updated_at = datetime()
            ON MATCH SET
                obj.updated_at = datetime()
            RETURN obj
            """
        
        params = {
            "fqn": object_fqn,
            "name": obj_name,
            "schema_name": schema_name,
            "database_name": database_name,
            "object_type": obj_type.upper()
        }
        
        return cypher, params
    
    def _generate_cypher_for_column_node(self, column_name: str, object_fqn: str, database_name: str) -> Tuple[str, Dict[str, Any]]:
        """
        为列节点生成Cypher语句
        
        优先尝试MATCH metadata_graph_builder已创建的列节点，
        如果不存在则创建临时列节点。
        
        Args:
            column_name: 列名
            object_fqn: 所属对象的FQN
            database_name: 数据库名称
            
        Returns:
            Tuple[str, Dict[str, Any]]: Cypher语句和参数字典
        """
        column_fqn = generate_column_fqn(object_fqn, column_name)
        
        cypher = f"""
        // 首先确保父对象存在
        MATCH (parent_obj {{fqn: $object_fqn}})
        
        // 创建或匹配列节点
        MERGE (col:{NODE_LABEL_COLUMN} {{fqn: $column_fqn}})
        ON CREATE SET
            col.name = $column_name,
            col.object_fqn = $object_fqn,
            col.database_name = $database_name,
            col.is_temporary = true,
            col.created_at = datetime(),
            col.updated_at = datetime()
        ON MATCH SET
            col.updated_at = datetime()
        
        // 确保HAS_COLUMN关系存在
        MERGE (parent_obj)-[r:{REL_TYPE_HAS_COLUMN}]->(col)
        ON CREATE SET
            r.created_at = datetime(),
            r.updated_at = datetime()
        ON MATCH SET
            r.updated_at = datetime()
        
        RETURN col
        """
        
        params = {
            "column_fqn": column_fqn,
            "column_name": column_name,
            "object_fqn": object_fqn,
            "database_name": database_name
        }
        
        return cypher, params
    
    def _generate_cypher_for_data_flow(self, pattern_info: models.AnalyticalSQLPattern) -> List[Tuple[str, Dict[str, Any]]]:
        """
        为数据流生成Cypher语句
        
        处理column_level_lineage，创建列之间的DATA_FLOW关系。
        
        Args:
            pattern_info: SQL模式信息
            
        Returns:
            List[Tuple[str, Dict[str, Any]]]: Cypher语句和参数字典列表
        """
        cypher_statements = []
        
        if not pattern_info.llm_extracted_relations_json:
            return cypher_statements
        
        relations_json = pattern_info.llm_extracted_relations_json
        column_lineage = relations_json.get("column_level_lineage", [])
        
        database_name = pattern_info.source_database_name or "unknown_db"
        
        for lineage_entry in column_lineage:
            target_column = lineage_entry.get("target_column")
            target_object_name = lineage_entry.get("target_object_name")
            target_schema = lineage_entry.get("target_object_schema", "public")
            derivation_type = lineage_entry.get("derivation_type", "UNKNOWN")
            
            if not target_column or not target_object_name:
                continue
            
            # 生成目标列FQN
            target_db_fqn = generate_database_fqn(database_name, database_name)
            target_schema_fqn = generate_schema_fqn(target_db_fqn, target_schema)
            target_object_fqn = generate_object_fqn(target_schema_fqn, target_object_name)
            target_column_fqn = generate_column_fqn(target_object_fqn, target_column)
            
            # 处理每个源
            for source in lineage_entry.get("sources", []):
                source_object = source.get("source_object")
                source_column = source.get("source_column")
                transformation_logic = source.get("transformation_logic", "")
                
                if not source_object or not source_object.get("name"):
                    continue
                
                source_schema = source_object.get("schema", "public")
                source_name = source_object.get("name")
                
                # 如果有源列，创建列到列的数据流
                if source_column:
                    source_schema_fqn = generate_schema_fqn(target_db_fqn, source_schema)
                    source_object_fqn = generate_object_fqn(source_schema_fqn, source_name)
                    source_column_fqn = generate_column_fqn(source_object_fqn, source_column)
                    
                    cypher = f"""
                    MATCH (src_col:{NODE_LABEL_COLUMN} {{fqn: $source_column_fqn}})
                    MATCH (tgt_col:{NODE_LABEL_COLUMN} {{fqn: $target_column_fqn}})
                    MERGE (src_col)-[df:{REL_TYPE_DATA_FLOW} {{sql_hash: $sql_hash}}]->(tgt_col)
                    ON CREATE SET
                        df.transformation_logic = $transformation_logic,
                        df.derivation_type = $derivation_type,
                        df.created_at = datetime(),
                        df.last_seen_at = $last_seen_at
                    ON MATCH SET
                        df.transformation_logic = $transformation_logic,
                        df.derivation_type = $derivation_type,
                        df.last_seen_at = $last_seen_at
                    RETURN df
                    """
                    
                    params = {
                        "source_column_fqn": source_column_fqn,
                        "target_column_fqn": target_column_fqn,
                        "sql_hash": pattern_info.sql_hash,
                        "transformation_logic": transformation_logic,
                        "derivation_type": derivation_type,
                        "last_seen_at": pattern_info.last_seen_at.isoformat() if pattern_info.last_seen_at else None
                    }
                    
                    cypher_statements.append((cypher, params))
                else:
                    # 处理没有源列的情况（字面量、表达式等）
                    # 创建从源对象到目标列的数据流关系
                    source_schema_fqn = generate_schema_fqn(target_db_fqn, source_schema)
                    source_object_fqn = generate_object_fqn(source_schema_fqn, source_name)
                    
                    cypher = f"""
                    MATCH (src_obj {{fqn: $source_object_fqn}})
                    MATCH (tgt_col:{NODE_LABEL_COLUMN} {{fqn: $target_column_fqn}})
                    MERGE (src_obj)-[df:{REL_TYPE_DATA_FLOW} {{sql_hash: $sql_hash}}]->(tgt_col)
                    ON CREATE SET
                        df.transformation_logic = $transformation_logic,
                        df.derivation_type = $derivation_type,
                        df.created_at = datetime(),
                        df.last_seen_at = $last_seen_at
                    ON MATCH SET
                        df.transformation_logic = $transformation_logic,
                        df.derivation_type = $derivation_type,
                        df.last_seen_at = $last_seen_at
                    RETURN df
                    """
                    
                    params = {
                        "source_object_fqn": source_object_fqn,
                        "target_column_fqn": target_column_fqn,
                        "sql_hash": pattern_info.sql_hash,
                        "transformation_logic": transformation_logic,
                        "derivation_type": derivation_type,
                        "last_seen_at": pattern_info.last_seen_at.isoformat() if pattern_info.last_seen_at else None
                    }
                    
                    cypher_statements.append((cypher, params))
        
        return cypher_statements
    
    def _generate_cypher_for_sql_object_references(self, pattern_info: models.AnalyticalSQLPattern) -> List[Tuple[str, Dict[str, Any]]]:
        """
        为SQL对象引用生成Cypher语句
        
        处理referenced_objects，创建SQL模式与数据库对象的引用关系。
        
        Args:
            pattern_info: SQL模式信息
            
        Returns:
            List[Tuple[str, Dict[str, Any]]]: Cypher语句和参数字典列表
        """
        cypher_statements = []
        
        if not pattern_info.llm_extracted_relations_json:
            return cypher_statements
        
        relations_json = pattern_info.llm_extracted_relations_json
        referenced_objects = relations_json.get("referenced_objects", [])
        
        database_name = pattern_info.source_database_name or "unknown_db"
        
        for ref_obj in referenced_objects:
            schema_name = ref_obj.get("schema", "public")
            obj_name = ref_obj.get("name")
            obj_type = ref_obj.get("type", "TABLE")
            access_mode = ref_obj.get("access_mode", "READ").upper()
            
            if not obj_name:
                continue
            
            # 生成对象FQN
            db_fqn = generate_database_fqn(database_name, database_name)
            schema_fqn = generate_schema_fqn(db_fqn, schema_name)
            object_fqn = generate_object_fqn(schema_fqn, obj_name)
            
            # 根据访问模式创建不同的关系
            if access_mode in ["READ", "READ_WRITE"]:
                read_cypher = f"""
                MATCH (sp:{NODE_LABEL_SQL_PATTERN} {{sql_hash: $sql_hash}})
                MATCH (obj {{fqn: $object_fqn}})
                MERGE (sp)-[r:{REL_TYPE_READS_FROM}]->(obj)
                ON CREATE SET
                    r.created_at = datetime(),
                    r.last_seen_at = $last_seen_at
                ON MATCH SET
                    r.last_seen_at = $last_seen_at
                RETURN r
                """
                
                params = {
                    "sql_hash": pattern_info.sql_hash,
                    "object_fqn": object_fqn,
                    "last_seen_at": pattern_info.last_seen_at.isoformat() if pattern_info.last_seen_at else None
                }
                
                cypher_statements.append((read_cypher, params))
            
            if access_mode in ["WRITE", "READ_WRITE"]:
                write_cypher = f"""
                MATCH (sp:{NODE_LABEL_SQL_PATTERN} {{sql_hash: $sql_hash}})
                MATCH (obj {{fqn: $object_fqn}})
                MERGE (sp)-[r:{REL_TYPE_WRITES_TO}]->(obj)
                ON CREATE SET
                    r.created_at = datetime(),
                    r.last_seen_at = $last_seen_at
                ON MATCH SET
                    r.last_seen_at = $last_seen_at
                RETURN r
                """
                
                params = {
                    "sql_hash": pattern_info.sql_hash,
                    "object_fqn": object_fqn,
                    "last_seen_at": pattern_info.last_seen_at.isoformat() if pattern_info.last_seen_at else None
                }
                
                cypher_statements.append((write_cypher, params))
        
        return cypher_statements
    
    def transform_llm_json_to_cypher_batch(self, pattern_info: models.AnalyticalSQLPattern) -> List[Tuple[str, Dict[str, Any]]]:
        """
        将LLM提取的JSON转换为Cypher语句批次
        
        按照设计要求的编排顺序：
        1. 先生成SQL模式节点
        2. 确保所有涉及的对象节点存在
        3. 确保所有涉及的列节点存在
        4. 创建数据流关系
        5. 创建对象引用关系
        
        Args:
            pattern_info: SQL模式信息
            
        Returns:
            List[Tuple[str, Dict[str, Any]]]: Cypher语句和参数字典列表
        """
        cypher_batch = []
        
        if not pattern_info.llm_extracted_relations_json:
            logger.warning(f"SQL模式 {pattern_info.sql_hash} 没有LLM提取的关系JSON")
            return cypher_batch
        
        relations_json = pattern_info.llm_extracted_relations_json
        database_name = pattern_info.source_database_name or "unknown_db"
        
        # 收集所有涉及的对象和列
        objects_to_ensure = set()
        columns_to_ensure = set()
        
        # 从target_object收集
        target_object = relations_json.get("target_object")
        if target_object and target_object.get("name"):
            objects_to_ensure.add((
                target_object.get("schema", "public"),
                target_object.get("name"),
                target_object.get("type", "TABLE")
            ))
        
        # 从column_level_lineage收集
        for lineage_entry in relations_json.get("column_level_lineage", []):
            # 收集目标对象和列
            target_object_name = lineage_entry.get("target_object_name")
            target_schema = lineage_entry.get("target_object_schema", "public")
            target_column = lineage_entry.get("target_column")
            
            if target_object_name:
                objects_to_ensure.add((target_schema, target_object_name, "TABLE"))
                
                if target_column:
                    db_fqn = generate_database_fqn(database_name, database_name)
                    schema_fqn = generate_schema_fqn(db_fqn, target_schema)
                    object_fqn = generate_object_fqn(schema_fqn, target_object_name)
                    columns_to_ensure.add((target_column, object_fqn))
            
            # 收集源对象和列
            for source in lineage_entry.get("sources", []):
                source_object = source.get("source_object")
                if source_object and source_object.get("name"):
                    source_schema = source_object.get("schema", "public")
                    source_name = source_object.get("name")
                    source_type = source_object.get("type", "TABLE")
                    
                    objects_to_ensure.add((source_schema, source_name, source_type))
                    
                    source_column = source.get("source_column")
                    if source_column:
                        db_fqn = generate_database_fqn(database_name, database_name)
                        schema_fqn = generate_schema_fqn(db_fqn, source_schema)
                        object_fqn = generate_object_fqn(schema_fqn, source_name)
                        columns_to_ensure.add((source_column, object_fqn))
        
        # 从referenced_objects收集
        for ref_obj in relations_json.get("referenced_objects", []):
            if ref_obj.get("name"):
                objects_to_ensure.add((
                    ref_obj.get("schema", "public"),
                    ref_obj.get("name"),
                    ref_obj.get("type", "TABLE")
                ))
        
        # 1. 生成SQL模式节点
        sql_pattern_cypher = self._generate_cypher_for_sql_pattern_node(pattern_info)
        cypher_batch.append(sql_pattern_cypher)
        
        # 2. 确保所有对象节点存在
        for schema_name, obj_name, obj_type in objects_to_ensure:
            obj_info = {
                "schema": schema_name,
                "name": obj_name,
                "type": obj_type
            }
            obj_cypher = self._generate_cypher_for_object_node(obj_info, database_name)
            cypher_batch.append(obj_cypher)
        
        # 3. 确保所有列节点存在
        for column_name, object_fqn in columns_to_ensure:
            col_cypher = self._generate_cypher_for_column_node(column_name, object_fqn, database_name)
            cypher_batch.append(col_cypher)
        
        # 4. 生成数据流关系
        data_flow_cypher_list = self._generate_cypher_for_data_flow(pattern_info)
        cypher_batch.extend(data_flow_cypher_list)
        
        # 5. 生成对象引用关系
        object_ref_cypher_list = self._generate_cypher_for_sql_object_references(pattern_info)
        cypher_batch.extend(object_ref_cypher_list)
        
        logger.debug(f"为SQL模式 {pattern_info.sql_hash} 生成了 {len(cypher_batch)} 条Cypher语句")
        logger.debug(f"确保存在 {len(objects_to_ensure)} 个对象，{len(columns_to_ensure)} 个列")
        
        return cypher_batch
    
    async def mark_pattern_as_loaded_to_age(self, sql_hash: str, success: bool = True, error_message: str = None):
        """
        标记SQL模式已加载到AGE或记录错误
        
        Args:
            sql_hash: SQL哈希值
            success: 是否成功
            error_message: 错误消息（如果失败）
        """
        conn = await self._get_analytics_db_conn()
        try:
            if success:
                query = """
                UPDATE lumi_analytics.sql_patterns
                SET is_loaded_to_age = TRUE,
                    age_load_error_message = NULL,
                    updated_at = NOW()
                WHERE sql_hash = $1
                """
                await conn.execute(query, sql_hash)
            else:
                query = """
                UPDATE lumi_analytics.sql_patterns
                SET is_loaded_to_age = FALSE,
                    age_load_error_message = $2,
                    updated_at = NOW()
                WHERE sql_hash = $1
                """
                await conn.execute(query, sql_hash, error_message)
        finally:
            await self._release_analytics_db_conn(conn)
    
    async def build_lineage_graphs(self, batch_size: int = 10) -> Dict[str, Any]:
        """
        构建血缘图谱
        
        主流程：获取待处理SQL模式 -> 转换JSON为Cypher批次 -> 执行Cypher -> 更新状态
        
        Args:
            batch_size: 批次大小
            
        Returns:
            Dict[str, Any]: 处理结果统计
        """
        logger.info("开始构建血缘图谱...")
        
        # 获取待处理的SQL模式
        patterns = await self.get_pending_sql_patterns_for_lineage(batch_size)
        
        if not patterns:
            logger.info("没有待处理的SQL模式")
            return {"processed": 0, "success": 0, "failed": 0, "errors": []}
        
        processed = 0
        success_count = 0
        failed_count = 0
        errors = []
        
        for pattern in patterns:
            try:
                logger.info(f"处理SQL模式: {pattern.sql_hash}")
                
                # 转换为Cypher批次
                cypher_batch = self.transform_llm_json_to_cypher_batch(pattern)
                
                if not cypher_batch:
                    logger.warning(f"SQL模式 {pattern.sql_hash} 没有生成任何Cypher语句")
                    await self.mark_pattern_as_loaded_to_age(pattern.sql_hash, True)
                    success_count += 1
                    processed += 1
                    continue
                
                # 在AGE数据库中执行事务
                age_conn = await self._get_age_db_conn()
                try:
                    # 开始事务
                    async with age_conn.transaction():
                        # 执行所有Cypher语句
                        for i, (cypher_stmt, params) in enumerate(cypher_batch):
                            try:
                                await common_execute_cypher(age_conn, cypher_stmt, params, self.graph_name)
                                logger.debug(f"执行Cypher语句 {i+1}/{len(cypher_batch)} 成功")
                            except Exception as e:
                                logger.error(f"执行Cypher语句失败: {str(e)}")
                                logger.error(f"语句: {cypher_stmt}")
                                logger.error(f"参数: {params}")
                                raise
                    
                    # 标记为成功
                    await self.mark_pattern_as_loaded_to_age(pattern.sql_hash, True)
                    success_count += 1
                    logger.info(f"成功处理SQL模式 {pattern.sql_hash}")
                    
                except Exception as e:
                    error_msg = f"执行Cypher事务失败: {str(e)}"
                    logger.error(error_msg)
                    errors.append({"sql_hash": pattern.sql_hash, "error": error_msg})
                    
                    # 标记为失败
                    await self.mark_pattern_as_loaded_to_age(pattern.sql_hash, False, error_msg)
                    failed_count += 1
                    
                finally:
                    await age_conn.close()
                
                processed += 1
                
            except Exception as e:
                error_msg = f"处理SQL模式失败: {str(e)}"
                logger.error(error_msg)
                errors.append({"sql_hash": pattern.sql_hash, "error": error_msg})
                
                try:
                    await self.mark_pattern_as_loaded_to_age(pattern.sql_hash, False, error_msg)
                except Exception as mark_error:
                    logger.error(f"标记失败状态时出错: {str(mark_error)}")
                
                failed_count += 1
                processed += 1
        
        logger.info(f"血缘图谱构建完成: 处理 {processed} 个，成功 {success_count} 个，失败 {failed_count} 个")
        
        return {
            "processed": processed,
            "success": success_count,
            "failed": failed_count,
            "errors": errors
        }


async def main():
    """
    主函数示例
    """
    # 配置示例（实际使用时应从配置文件读取）
    analytics_db_config = {
        'user': 'lumiadmin',
        'password': 'your_password',
        'host': 'localhost',
        'port': 5432,
        'database': 'iwdb'
    }
    
    age_db_config = {
        'user': 'lumiadmin',
        'password': 'lumiadmin',
        'host': 'localhost',
        'port': 5432,
        'database': 'iwdb'
    }
    
    # 创建构建器
    builder = LineageGraphBuilder(
        analytics_db_config=analytics_db_config,
        age_db_config=age_db_config,
        graph_name="lumi_graph"
    )
    
    try:
        # 构建血缘图谱
        result = await builder.build_lineage_graphs(batch_size=10)
        logger.info(f"构建结果: {result}")
        
    finally:
        # 关闭连接池
        await builder.close_analytics_pool()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    asyncio.run(main()) 