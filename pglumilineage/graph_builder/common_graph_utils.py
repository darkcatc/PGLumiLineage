#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
图数据库通用工具函数

该模块包含用于操作Apache AGE图数据库的通用工具函数，
包括连接管理、Cypher语句执行、FQN生成等功能。

作者: Vance Chen
"""

import logging
import hashlib
from typing import Any, Dict, List, Optional, Tuple, Union
import json
import asyncpg

# 设置日志
logger = logging.getLogger(__name__)

# 默认的AGE图名称
DEFAULT_GRAPH_NAME = "lumi_graph"

# 节点标签常量
NODE_LABEL_DATASOURCE = "datasource"
NODE_LABEL_DATABASE = "database"
NODE_LABEL_SCHEMA = "schema"
NODE_LABEL_TABLE = "table"
NODE_LABEL_VIEW = "view"
NODE_LABEL_MATERIALIZED_VIEW = "materialized_view"
NODE_LABEL_COLUMN = "column"
NODE_LABEL_FUNCTION = "function"

# 关系类型常量
REL_TYPE_CONFIGURES = "configures_database"
REL_TYPE_HAS_SCHEMA = "has_schema"
REL_TYPE_HAS_OBJECT = "has_object"
REL_TYPE_HAS_COLUMN = "has_column"
REL_TYPE_REFERENCES = "references_column"
REL_TYPE_HAS_FUNCTION = "has_function"


def generate_datasource_fqn(source_id: int, source_name: str) -> str:
    """
    生成数据源的完全限定名(FQN)
    
    Args:
        source_id: 数据源ID
        source_name: 数据源名称
        
    Returns:
        str: 数据源的FQN
    """
    return f"datasource_{source_id}_{source_name}"


def generate_database_fqn(source_name: str, database_name: str) -> str:
    """
    生成数据库的完全限定名(FQN)
    
    Args:
        source_name: 数据源名称
        database_name: 数据库名称
        
    Returns:
        str: 数据库的FQN
    """
    return f"{source_name}.{database_name}"


def generate_schema_fqn(database_fqn: str, schema_name: str) -> str:
    """
    生成模式的完全限定名(FQN)
    
    Args:
        database_fqn: 数据库的FQN
        schema_name: 模式名称
        
    Returns:
        str: 模式的FQN
    """
    return f"{database_fqn}.{schema_name}"


def generate_object_fqn(schema_fqn: str, object_name: str) -> str:
    """
    生成数据库对象(表/视图等)的完全限定名(FQN)
    
    Args:
        schema_fqn: 模式的FQN
        object_name: 对象名称
        
    Returns:
        str: 对象的FQN
    """
    return f"{schema_fqn}.{object_name}"


def generate_column_fqn(object_fqn: str, column_name: str) -> str:
    """
    生成列的完全限定名(FQN)
    
    Args:
        object_fqn: 所属对象的FQN
        column_name: 列名
        
    Returns:
        str: 列的FQN
    """
    return f"{object_fqn}.{column_name}"


def generate_function_fqn(schema_fqn: str, function_name: str, parameter_types: List[str] = None) -> str:
    """
    生成函数的完全限定名(FQN)
    
    Args:
        schema_fqn: 模式的FQN
        function_name: 函数名
        parameter_types: 参数类型列表
        
    Returns:
        str: 函数的FQN
    """
    if parameter_types:
        param_str = ','.join(parameter_types)
        return f"{schema_fqn}.{function_name}({param_str})"
    else:
        return f"{schema_fqn}.{function_name}"


def convert_cypher_for_age(cypher_stmt: str) -> str:
    """
    转换Cypher语句以适应AGE 1.5.0版本
    
    此函数将标准Cypher语法转换为AGE 1.5.0兼容的语法。主要转换包括：
    1. 将所有标签名转换为小写，避免PostgreSQL创建带双引号的对象名
    2. 将节点标签语法 (n:Label) 转换为属性语法 (n {label: 'label'})
    3. 将关系类型语法 -[:TYPE]-> 转换为 -[:type]->，并确保关系属性正确
    4. 处理WHERE条件中的标签语法
    5. 替换保留关键字变量名
    6. 转换 MERGE 语句中的 ON CREATE SET ... ON MATCH SET ... 语法
    
    Args:
        cypher_stmt: 原始Cypher语句
        
    Returns:
        str: 转换后的Cypher语句，兼容AGE 1.5.0
    """
    # 检查并替换保留关键字变量名
    reserved_keywords = ['table', 'group', 'order', 'limit', 'match', 'where', 'return']
    for keyword in reserved_keywords:
        # 替换形如 (table {properties}) 的模式
        cypher_stmt = re.sub(r'\((' + keyword + r')\s+({[^}]+})\)', r'(t_\1 \2)', cypher_stmt, flags=re.IGNORECASE)
        # 替换形如 MATCH (table) 的模式
        cypher_stmt = re.sub(r'MATCH\s+\((' + keyword + r')\)', r'MATCH (t_\1)', cypher_stmt, flags=re.IGNORECASE)
        # 替换形如 (table:Label) 的模式
        cypher_stmt = re.sub(r'\((' + keyword + r'):([\w]+)\)', r'(t_\1:\2)', cypher_stmt, flags=re.IGNORECASE)
    
    # 将节点标签语法 (n:Label) 转换为属性语法 (n {label: 'label'})
    cypher_stmt = re.sub(r'\((\w+):([\w]+)\)', r'(\1 {label: "\2"})', cypher_stmt)
    
    # 将关系类型语法 -[:TYPE]-> 转换为 -[r {label: 'type'}]->
    cypher_stmt = re.sub(r'-\[:(\w+)\]->', r'-[r {label: "\1"}]->', cypher_stmt)
    
    # 处理带变量的关系 -[r:TYPE]-> 转换为 -[r {label: 'type'}]->
    cypher_stmt = re.sub(r'-\[(\w+):(\w+)\]->', r'-[\1 {label: "\2"}]->', cypher_stmt)
    
    # 处理带属性的关系 -[r:TYPE {prop: val}]-> 转换为 -[r {label: 'type', prop: val}]->
    cypher_stmt = re.sub(r'-\[(\w+):(\w+)\s+({[^}]+})\]->', r'-[\1 {label: "\2", \3}]->', cypher_stmt)
    
    # 处理 MERGE 语句中的 ON CREATE SET ... ON MATCH SET ... 语法
    if 'MERGE' in cypher_stmt and ('ON CREATE SET' in cypher_stmt or 'ON MATCH SET' in cypher_stmt):
        # 提取 MERGE 部分
        merge_match = re.search(r'(MERGE\s+\([^)]+\)(?:-\[.*?\]->\([^)]+\))*)', cypher_stmt, re.DOTALL | re.IGNORECASE)
        if merge_match:
            merge_part = merge_match.group(1)
            
            # 提取 WITH 子句（如果有）
            with_part = ''
            with_match = re.search(r'(WITH\s+[^\n]+(?:,\s*[^\n]+)*)\s*(?:ON\s+CREATE\s+SET|ON\s+MATCH\s+SET|$)', 
                                 cypher_stmt[merge_match.end():], re.DOTALL | re.IGNORECASE)
            if with_match:
                with_part = with_match.group(1)
            
            # 提取 ON CREATE SET 和 ON MATCH SET 部分
            on_create_set = ''
            on_match_set = ''
            
            # 提取 ON CREATE SET 部分
            create_match = re.search(r'ON\s+CREATE\s+SET\s+([^;]+?)(?:\s*ON\s+MATCH\s+SET|$)', 
                                   cypher_stmt, re.DOTALL | re.IGNORECASE)
            if create_match:
                on_create_set = create_match.group(1).strip()
            
            # 提取 ON MATCH SET 部分
            match_match = re.search(r'ON\s+MATCH\s+SET\s+([^;]+?)(?:\s*RETURN|$)', 
                                  cypher_stmt, re.DOTALL | re.IGNORECASE)
            if match_match:
                on_match_set = match_match.group(1).strip()
            
            # 构建新的 Cypher 语句
            new_cypher = merge_part
            
            # 添加 WITH 子句（如果有）
            if with_part:
                new_cypher += '\n' + with_part
            
            # 添加 SET 语句
            if on_create_set or on_match_set:
                set_clauses = []
                
                # 处理 ON CREATE SET
                if on_create_set:
                    set_clauses.append(f"SET {on_create_set}")
                
                # 处理 ON MATCH SET - 使用 COALESCE 来模拟
                if on_match_set:
                    # 对于 ON MATCH SET，我们需要检查属性是否已存在
                    # 这里简化处理，直接添加 SET 语句
                    set_clauses.append(f"SET {on_match_set}")
                
                new_cypher += '\n' + '\n'.join(set_clauses)
            
            # 保留 RETURN 子句
            return_match = re.search(r'RETURN\s+.*$', cypher_stmt, re.DOTALL | re.IGNORECASE)
            if return_match:
                new_cypher += '\n' + return_match.group(0)
            
            return new_cypher
    
    # 将所有标签名转换为小写
    def lower_label(match):
        label_part = match.group(2).lower()
        return f'{match.group(1)}{label_part}{match.group(3)}'
    
    cypher_stmt = re.sub(r'(label:\s*["\'])([\w]+)(["\'])', lower_label, cypher_stmt)
    
    return cypher_stmt


async def execute_cypher(conn: asyncpg.Connection, cypher_stmt: str, 
                        params: Dict[str, Any] = None, 
                        graph_name: str = DEFAULT_GRAPH_NAME) -> List[Dict[str, Any]]:
    """
    执行Cypher语句
    
    Args:
        conn: 数据库连接
        cypher_stmt: Cypher语句
        params: 参数字典
        graph_name: 图名称
        
    Returns:
        List[Dict[str, Any]]: 查询结果
    """
    # 转换为AGE 1.5.0兼容的Cypher语句
    age_cypher = convert_cypher_for_age(cypher_stmt)
    
    # 构造AGE查询
    age_query = f"SELECT * FROM cypher('{graph_name}', $$ {age_cypher} $$) AS (result agtype);"
    
    try:
        rows = await conn.fetch(age_query, *(params or {}).values())
        return [dict(row) for row in rows]
    except Exception as e:
        logger.error(f"执行Cypher语句出错: {str(e)}\nCypher: {age_cypher}")
        raise


async def ensure_age_graph_exists(conn: asyncpg.Connection, graph_name: str = DEFAULT_GRAPH_NAME) -> bool:
    """
    确保AGE图存在
    
    Args:
        conn: 数据库连接
        graph_name: 图名称
        
    Returns:
        bool: 图是否存在或创建成功
    """
    try:
        # 检查图是否存在
        check_query = f"SELECT * FROM ag_catalog.ag_graph WHERE name = '{graph_name}';"
        result = await conn.fetch(check_query)
        
        if not result:
            # 创建图
            create_query = f"SELECT * FROM ag_catalog.create_graph('{graph_name}');"
            await conn.execute(create_query)
            logger.info(f"创建了新的AGE图: {graph_name}")
        
        return True
    except Exception as e:
        logger.error(f"确保AGE图存在时出错: {str(e)}")
        return False


def generate_timestamp() -> str:
    """
    生成当前时间戳字符串
    
    Returns:
        str: ISO格式的当前时间戳
    """
    from datetime import datetime
    return datetime.now().isoformat()


def generate_hash(*args) -> str:
    """
    生成参数的哈希值
    
    Args:
        *args: 要哈希的参数
        
    Returns:
        str: 16进制表示的哈希值
    """
    hash_obj = hashlib.md5()
    for arg in args:
        if arg is not None:
            hash_obj.update(str(arg).encode('utf-8'))
    return hash_obj.hexdigest()


def escape_cypher_string(s: str) -> str:
    """
    转义Cypher字符串中的特殊字符
    
    Args:
        s: 要转义的字符串
        
    Returns:
        str: 转义后的字符串
    """
    if not s:
        return s
    return s.replace('\\', '\\\\').replace('"', '\\"').replace("'", "\\'")


def format_properties(properties: Dict[str, Any]) -> str:
    """
    将属性字典格式化为Cypher属性字符串
    
    Args:
        properties: 属性字典
        
    Returns:
        str: 格式化的属性字符串，如 "{key1: 'value1', key2: 123}"
    """
    if not properties:
        return "{}"
    
    items = []
    for k, v in properties.items():
        if v is None:
            items.append(f"{k}: null")
        elif isinstance(v, bool):
            items.append(f"{k}: {'true' if v else 'false'}")
        elif isinstance(v, (int, float)):
            items.append(f"{k}: {v}")
        elif isinstance(v, dict):
            items.append(f"{k}: {json.dumps(v, ensure_ascii=False)}")
        else:
            items.append(f"{k}: '{escape_cypher_string(str(v))}'")
    
    return "{" + ", ".join(items) + "}"
