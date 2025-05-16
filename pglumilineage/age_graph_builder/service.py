#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
AGE图谱构建服务

此模块负责将LLM提取的关系转换为Cypher语句，并构建数据血缘图谱。

作者: Vance Chen
"""

import logging
import json
import re
from typing import List, Dict, Any, Optional, Union
from datetime import datetime

from pglumilineage.common import models

# 设置日志
logger = logging.getLogger(__name__)


def convert_cypher_for_age(cypher_stmt: str) -> str:
    """
    转换Cypher语句以适应AGE 1.5.0版本
    
    Args:
        cypher_stmt: 原始Cypher语句
        
    Returns:
        str: 转换后的Cypher语句
    """
    # 替换标签语法
    # 例如：MERGE (db:Database {name: 'tpcds'})
    # 转换为：MERGE (db {name: 'tpcds', label: 'Database'})
    converted_stmt = re.sub(r'(\w+):(\w+)\s+({[^}]+})', r'\1 \3, label: "\2"', cypher_stmt)
    
    # 替换WHERE条件中的标签语法
    # 例如：WHERE (obj:Table OR obj:View)
    # 转换为：WHERE (obj.label = 'Table' OR obj.label = 'View')
    converted_stmt = re.sub(r'WHERE\s+\((\w+):(\w+)\s+OR\s+\w+:(\w+)\)', r'WHERE (\1.label = "\2" OR \1.label = "\3")', converted_stmt)
    
    # 替换关系类型语法
    # 例如：MERGE (a)-[:REL_TYPE]->(b)
    # 转换为：MERGE (a)-[r {label: 'REL_TYPE'}]->(b)
    converted_stmt = re.sub(r'-\[:(\w+)\]->', r'-[r {label: "\1"}]->', converted_stmt)
    
    # 替换ON CREATE SET和ON MATCH SET语法
    # AGE 1.5.0可能不支持这些语法
    if "ON CREATE SET" in converted_stmt:
        # 提取ON CREATE SET部分的属性设置
        create_set_match = re.search(r'ON\s+CREATE\s+SET\s+([^O]+?)(?:ON\s+MATCH\s+SET|$)', converted_stmt, re.DOTALL)
        if create_set_match:
            create_set = create_set_match.group(1).strip()
            # 移除ON CREATE SET部分
            converted_stmt = re.sub(r'ON\s+CREATE\s+SET\s+([^O]+?)(?:ON\s+MATCH\s+SET|$)', '', converted_stmt, flags=re.DOTALL)
            # 添加普通的SET语句
            if not converted_stmt.strip().endswith(';'):
                converted_stmt = converted_stmt.strip() + " SET " + create_set
    
    # 移除ON MATCH SET部分
    converted_stmt = re.sub(r'ON\s+MATCH\s+SET\s+.+', '', converted_stmt, flags=re.DOTALL)
    
    # 替换datetime()函数
    converted_stmt = converted_stmt.replace('datetime()', 'current_timestamp')
    
    return converted_stmt


def transform_json_to_cypher(pattern_info: models.AnalyticalSQLPattern) -> List[str]:
    """
    将LLM提取的关系转换为Cypher语句
    
    根据pattern_info.llm_extracted_relations_json和其他pattern_info字段，
    生成一系列幂等的Cypher语句（使用MERGE）来构建图谱。
    
    Args:
        pattern_info: AnalyticalSQLPattern对象，包含LLM提取的关系和SQL模式信息
        
    Returns:
        List[str]: Cypher语句列表
    """
    # 初始化Cypher语句列表
    cypher_statements = []
    
    # 获取关系JSON
    relations_json = pattern_info.llm_extracted_relations_json
    if not relations_json:
        logger.warning(f"SQL模式 {pattern_info.sql_hash} 没有LLM提取的关系")
        return []
    
    # 获取数据库名称
    database_name = pattern_info.source_database_name
    if not database_name:
        logger.warning(f"SQL模式 {pattern_info.sql_hash} 没有源数据库名称")
        database_name = "unknown_db"
    
    # 1. 创建/合并数据库节点
    db_cypher = f"""
    MERGE (db:Database {{name: '{database_name}'}})
    """
    cypher_statements.append(db_cypher)
    
    # 收集所有涉及的schema、表/视图和列
    schemas = set()
    tables_views = []
    
    # 添加目标对象（如果存在）
    target_object = relations_json.get("target_object")
    if target_object:
        schema_name = target_object.get("schema", "public")
        schemas.add(schema_name)
        tables_views.append({
            "schema": schema_name,
            "name": target_object.get("name"),
            "type": target_object.get("type", "TABLE")
        })
    
    # 添加列级血缘中涉及的对象
    column_lineage = relations_json.get("column_level_lineage", [])
    for lineage_entry in column_lineage:
        # 添加目标对象schema
        target_schema = lineage_entry.get("target_object_schema", "public")
        schemas.add(target_schema)
        
        # 添加目标对象
        target_object_name = lineage_entry.get("target_object_name")
        if target_object_name:
            tables_views.append({
                "schema": target_schema,
                "name": target_object_name,
                "type": "TABLE"  # 假设目标对象是表，如果需要可以从其他地方获取类型
            })
        
        # 添加源对象
        for source in lineage_entry.get("sources", []):
            source_object = source.get("source_object")
            if source_object:
                schema_name = source_object.get("schema", "public")
                schemas.add(schema_name)
                tables_views.append({
                    "schema": schema_name,
                    "name": source_object.get("name"),
                    "type": source_object.get("type", "TABLE")
                })
    
    # 添加引用对象
    referenced_objects = relations_json.get("referenced_objects", [])
    for ref_obj in referenced_objects:
        schema_name = ref_obj.get("schema", "public")
        schemas.add(schema_name)
        tables_views.append({
            "schema": schema_name,
            "name": ref_obj.get("name"),
            "type": ref_obj.get("type", "TABLE")
        })
    
    # 2. 创建/合并Schema节点
    for schema_name in schemas:
        schema_cypher = f"""
        MERGE (schema:Schema {{name: '{schema_name}', database_name: '{database_name}'}})
        WITH schema
        MATCH (db:Database {{name: '{database_name}'}})
        MERGE (db)-[:HAS_SCHEMA]->(schema)
        """
        cypher_statements.append(schema_cypher)
    
    # 3. 创建/合并表/视图节点
    for obj in tables_views:
        if not obj.get("name"):
            continue
            
        obj_type = obj.get("type", "TABLE")
        schema_name = obj.get("schema", "public")
        obj_name = obj.get("name")
        
        # 根据对象类型选择标签
        label = "Table" if obj_type == "TABLE" else "View"
        
        table_view_cypher = f"""
        MERGE ({label.lower()}:{label} {{name: '{obj_name}', schema_name: '{schema_name}', database_name: '{database_name}', object_type: '{obj_type}'}})
        WITH {label.lower()}
        MATCH (schema:Schema {{name: '{schema_name}', database_name: '{database_name}'}})
        MERGE (schema)-[:HAS_OBJECT]->({label.lower()})
        """
        cypher_statements.append(table_view_cypher)
    
    # 4. 创建/合并SqlPattern节点
    sql_pattern_cypher = f"""
    MERGE (sp:SqlPattern {{sql_hash: '{pattern_info.sql_hash}'}})
    SET sp.normalized_sql = $normalized_sql,
        sp.sample_sql = $sample_sql,
        sp.source_database_name = '{database_name}',
        sp.updated_at = datetime()
    """
    # 使用参数避免SQL和样本SQL中的特殊字符问题
    cypher_statements.append({
        "query": sql_pattern_cypher,
        "params": {
            "normalized_sql": pattern_info.normalized_sql_text,
            "sample_sql": pattern_info.sample_raw_sql_text
        }
    })
    
    # 5. 创建/合并列节点和数据流关系
    for lineage_entry in column_lineage:
        target_column = lineage_entry.get("target_column")
        target_object_name = lineage_entry.get("target_object_name")
        target_schema = lineage_entry.get("target_object_schema", "public")
        derivation_type = lineage_entry.get("derivation_type", "UNKNOWN")
        
        # 如果没有目标列或目标对象，则跳过
        if not target_column or not target_object_name:
            continue
        
        # 创建目标列节点
        target_fqn = f"{database_name}.{target_schema}.{target_object_name}.{target_column}"
        target_column_cypher = f"""
        MERGE (tgt_col:Column {{fqn: '{target_fqn}', name: '{target_column}'}})
        WITH tgt_col
        MATCH (tgt_obj) WHERE (tgt_obj:Table OR tgt_obj:View) AND tgt_obj.name = '{target_object_name}' AND tgt_obj.schema_name = '{target_schema}' AND tgt_obj.database_name = '{database_name}'
        MERGE (tgt_obj)-[:HAS_COLUMN]->(tgt_col)
        """
        cypher_statements.append(target_column_cypher)
        
        # 处理每个源
        for source in lineage_entry.get("sources", []):
            source_object = source.get("source_object")
            source_column = source.get("source_column")
            transformation_logic = source.get("transformation_logic", "")
            
            # 如果没有源对象，则跳过
            if not source_object or not source_object.get("name"):
                continue
            
            source_schema = source_object.get("schema", "public")
            source_name = source_object.get("name")
            
            # 如果有源列，创建源列节点和数据流关系
            if source_column:
                source_fqn = f"{database_name}.{source_schema}.{source_name}.{source_column}"
                source_column_cypher = f"""
                MERGE (src_col:Column {{fqn: '{source_fqn}', name: '{source_column}'}})
                WITH src_col
                MATCH (src_obj) WHERE (src_obj:Table OR src_obj:View) AND src_obj.name = '{source_name}' AND src_obj.schema_name = '{source_schema}' AND src_obj.database_name = '{database_name}'
                MERGE (src_obj)-[:HAS_COLUMN]->(src_col)
                """
                cypher_statements.append(source_column_cypher)
                
                # 创建数据流关系
                data_flow_cypher = f"""
                MATCH (src_col:Column {{fqn: '{source_fqn}'}})
                MATCH (tgt_col:Column {{fqn: '{target_fqn}'}})
                MERGE (src_col)-[df:DATA_FLOW {{sql_hash: '{pattern_info.sql_hash}'}}]->(tgt_col)
                ON CREATE SET df.transformation_logic = $transformation_logic,
                              df.derivation_type = '{derivation_type}',
                              df.created_at = datetime(),
                              df.last_seen_at = datetime()
                ON MATCH SET df.transformation_logic = $transformation_logic,
                             df.derivation_type = '{derivation_type}',
                             df.last_seen_at = datetime()
                """
                cypher_statements.append({
                    "query": data_flow_cypher,
                    "params": {
                        "transformation_logic": transformation_logic
                    }
                })
                
                # 创建SQL模式与数据流的关联
                sql_flow_cypher = f"""
                MATCH (sp:SqlPattern {{sql_hash: '{pattern_info.sql_hash}'}})
                MATCH (src_col:Column {{fqn: '{source_fqn}'}})-[df:DATA_FLOW {{sql_hash: '{pattern_info.sql_hash}'}}]->(tgt_col:Column {{fqn: '{target_fqn}'}})
                MERGE (sp)-[:GENERATES_FLOW]->(df)
                """
                cypher_statements.append(sql_flow_cypher)
            else:
                # 处理字面量或表达式（没有源列的情况）
                # 在这种情况下，我们直接从源对象到目标列创建数据流
                data_flow_from_object_cypher = f"""
                MATCH (src_obj) WHERE (src_obj:Table OR src_obj:View) AND src_obj.name = '{source_name}' AND src_obj.schema_name = '{source_schema}' AND src_obj.database_name = '{database_name}'
                MATCH (tgt_col:Column {{fqn: '{target_fqn}'}})
                MERGE (src_obj)-[df:DATA_FLOW {{sql_hash: '{pattern_info.sql_hash}'}}]->(tgt_col)
                ON CREATE SET df.transformation_logic = $transformation_logic,
                              df.derivation_type = '{derivation_type}',
                              df.created_at = datetime(),
                              df.last_seen_at = datetime()
                ON MATCH SET df.transformation_logic = $transformation_logic,
                             df.derivation_type = '{derivation_type}',
                             df.last_seen_at = datetime()
                """
                cypher_statements.append({
                    "query": data_flow_from_object_cypher,
                    "params": {
                        "transformation_logic": transformation_logic
                    }
                })
                
                # 创建SQL模式与数据流的关联
                sql_flow_obj_cypher = f"""
                MATCH (sp:SqlPattern {{sql_hash: '{pattern_info.sql_hash}'}})
                MATCH (src_obj)-[df:DATA_FLOW {{sql_hash: '{pattern_info.sql_hash}'}}]->(tgt_col:Column {{fqn: '{target_fqn}'}})
                WHERE (src_obj:Table OR src_obj:View) AND src_obj.name = '{source_name}' AND src_obj.schema_name = '{source_schema}' AND src_obj.database_name = '{database_name}'
                MERGE (sp)-[:GENERATES_FLOW]->(df)
                """
                cypher_statements.append(sql_flow_obj_cypher)
    
    # 6. 创建SQL模式与引用对象的关系
    for ref_obj in referenced_objects:
        schema_name = ref_obj.get("schema", "public")
        obj_name = ref_obj.get("name")
        obj_type = ref_obj.get("type", "TABLE")
        access_mode = ref_obj.get("access_mode", "READ")
        
        if not obj_name:
            continue
        
        # 根据访问模式创建不同的关系
        if access_mode == "READ" or access_mode == "READ_WRITE":
            read_cypher = f"""
            MATCH (sp:SqlPattern {{sql_hash: '{pattern_info.sql_hash}'}})
            MATCH (obj) WHERE (obj:Table OR obj:View) AND obj.name = '{obj_name}' AND obj.schema_name = '{schema_name}' AND obj.database_name = '{database_name}'
            MERGE (sp)-[:READS_FROM]->(obj)
            """
            cypher_statements.append(read_cypher)
        
        if access_mode == "WRITE" or access_mode == "READ_WRITE":
            write_cypher = f"""
            MATCH (sp:SqlPattern {{sql_hash: '{pattern_info.sql_hash}'}})
            MATCH (obj) WHERE (obj:Table OR obj:View) AND obj.name = '{obj_name}' AND obj.schema_name = '{schema_name}' AND obj.database_name = '{database_name}'
            MERGE (sp)-[:WRITES_TO]->(obj)
            """
            cypher_statements.append(write_cypher)
    
    # 处理Cypher语句列表，将带参数的语句转换为可执行的形式
    executable_statements = []
    for stmt in cypher_statements:
        if isinstance(stmt, dict):
            # 这是一个带参数的查询
            query = stmt["query"]
            params = stmt["params"]
            
            # 简单处理：将参数直接替换到查询中
            # 注意：在实际执行时应使用参数化查询以避免注入问题
            for key, value in params.items():
                if isinstance(value, str):
                    # 转义单引号
                    value = value.replace("'", "\\'")
                query = query.replace(f"${key}", f"'{value}'")
            
            executable_statements.append(query)
        else:
            executable_statements.append(stmt)
    
    # 转换为AGE 1.5.0兼容的Cypher语句
    age_compatible_statements = []
    for stmt in executable_statements:
        if isinstance(stmt, str):
            age_compatible_statements.append(convert_cypher_for_age(stmt))
        else:
            # 如果是带参数的语句，转换query部分
            converted_query = convert_cypher_for_age(stmt['query'])
            stmt['query'] = converted_query
            age_compatible_statements.append(stmt)
    
    return age_compatible_statements


async def build_graph_for_pattern(pattern_info: models.AnalyticalSQLPattern) -> bool:
    """
    为指定的SQL模式构建图谱
    
    Args:
        pattern_info: AnalyticalSQLPattern对象
        
    Returns:
        bool: 是否成功构建图谱
    """
    try:
        # 转换为Cypher语句
        cypher_statements = transform_json_to_cypher(pattern_info)
        
        if not cypher_statements:
            logger.warning(f"SQL模式 {pattern_info.sql_hash} 没有生成Cypher语句")
            return False
        
        # TODO: 执行Cypher语句
        # 这里需要实现与AGE图数据库的交互逻辑
        # 可以使用asyncpg或其他适合的客户端库
        
        logger.info(f"成功为SQL模式 {pattern_info.sql_hash} 构建图谱")
        return True
        
    except Exception as e:
        logger.error(f"为SQL模式 {pattern_info.sql_hash} 构建图谱时出错: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        return False


async def build_graph_for_patterns(patterns: List[models.AnalyticalSQLPattern]) -> Dict[str, bool]:
    """
    为多个SQL模式构建图谱
    
    Args:
        patterns: AnalyticalSQLPattern对象列表
        
    Returns:
        Dict[str, bool]: 每个SQL模式的构建结果，键为sql_hash，值为是否成功
    """
    results = {}
    
    for pattern in patterns:
        success = await build_graph_for_pattern(pattern)
        results[pattern.sql_hash] = success
    
    return results
