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
import re
import json
import asyncpg
from typing import Any, Dict, List, Optional, Tuple, Union

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
NODE_LABEL_SQL_PATTERN = "sqlpattern"
NODE_LABEL_TEMP_TABLE = "temptable"

# 关系类型常量
REL_TYPE_CONFIGURES = "configures_database"
REL_TYPE_HAS_SCHEMA = "has_schema"
REL_TYPE_HAS_OBJECT = "has_object"
REL_TYPE_HAS_COLUMN = "has_column"
REL_TYPE_REFERENCES = "references_column"
REL_TYPE_HAS_FUNCTION = "has_function"
REL_TYPE_DATA_FLOW = "data_flow"
REL_TYPE_READS_FROM = "reads_from"
REL_TYPE_WRITES_TO = "writes_to"


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
    1. 转换 MERGE 语句中的 ON CREATE SET ... ON MATCH SET ... 语法
    2. 将所有标签名和关系类型转换为小写
    3. 替换datetime()函数为字符串
    
    Args:
        cypher_stmt: 原始Cypher语句
        
    Returns:
        str: 转换后的Cypher语句，兼容AGE 1.5.0
    """
    
    # 1. 标签名转换为小写（AGE要求小写避免自动加引号）
    # 处理节点标签: (n:Label) -> (n:label)
    cypher_stmt = re.sub(r'\((\w+):([\w_]+)\)', lambda m: f'({m.group(1)}:{m.group(2).lower()})', cypher_stmt)
    # 处理带属性的节点标签: (n:Label {prop: value}) -> (n:label {prop: value})
    cypher_stmt = re.sub(r'\((\w+):([\w_]+)(\s*{[^}]*})\)', lambda m: f'({m.group(1)}:{m.group(2).lower()}{m.group(3)})', cypher_stmt)
    
    # 2. 关系类型转换为小写
    # 处理关系类型: -[:TYPE]-> -> -[:type]->
    cypher_stmt = re.sub(r'-\[:(\w+)\]->', lambda m: f'-[:{m.group(1).lower()}]->', cypher_stmt)
    # 处理带变量的关系: -[r:TYPE]-> -> -[r:type]->
    cypher_stmt = re.sub(r'-\[(\w+):(\w+)\]->', lambda m: f'-[{m.group(1)}:{m.group(2).lower()}]->', cypher_stmt)
    
    # 3. 处理 MERGE 语句中的 ON CREATE SET ... ON MATCH SET ... 语法
    # AGE 1.5.0 不支持这些语法，需要转换为等效的SET语句
    if 'ON CREATE SET' in cypher_stmt or 'ON MATCH SET' in cypher_stmt:
        # 使用逐行处理的方法，能够处理多个MERGE块
        lines = cypher_stmt.split('\n')
        result_lines = []
        i = 0
        
        while i < len(lines):
            line = lines[i].strip()
            
            # 如果遇到MERGE语句
            if line.startswith('MERGE'):
                # 收集MERGE语句（可能跨多行）
                merge_lines = [line]
                i += 1
                
                # 继续收集MERGE语句的剩余部分，直到遇到ON CREATE SET或其他关键字
                while i < len(lines):
                    current_line = lines[i].strip()
                    if (current_line.startswith('ON CREATE SET') or 
                        current_line.startswith('ON MATCH SET') or
                        current_line.startswith(('WITH', 'MATCH', 'MERGE', 'RETURN', 'CREATE', 'DELETE'))):
                        break
                    if current_line:
                        merge_lines.append(current_line)
                    i += 1
                
                # 收集ON CREATE SET部分
                create_sets = []
                if i < len(lines) and lines[i].strip().startswith('ON CREATE SET'):
                    i += 1  # 跳过"ON CREATE SET"行
                    while i < len(lines):
                        current_line = lines[i].strip()
                        if (current_line.startswith('ON MATCH SET') or
                            current_line.startswith(('WITH', 'MATCH', 'MERGE', 'RETURN', 'CREATE', 'DELETE'))):
                            break
                        if current_line and not current_line.startswith('//'):
                            # 移除末尾逗号
                            stmt = current_line.rstrip(',').strip()
                            if stmt:
                                create_sets.append(stmt)
                        i += 1
                
                # 收集ON MATCH SET部分
                match_sets = []
                if i < len(lines) and lines[i].strip().startswith('ON MATCH SET'):
                    i += 1  # 跳过"ON MATCH SET"行
                    while i < len(lines):
                        current_line = lines[i].strip()
                        if current_line.startswith(('WITH', 'MATCH', 'MERGE', 'RETURN', 'CREATE', 'DELETE')):
                            break
                        if current_line and not current_line.startswith('//'):
                            # 移除末尾逗号
                            stmt = current_line.rstrip(',').strip()
                            if stmt:
                                match_sets.append(stmt)
                        i += 1
                
                # 生成转换后的语句
                # 1. 添加MERGE语句
                result_lines.extend(merge_lines)
                
                # 2. 生成SET语句
                all_sets = []
                
                # 处理CREATE SET - 对created_at使用COALESCE确保幂等性
                for stmt in create_sets:
                    if 'created_at' in stmt:
                        # 提取变量名 (如 r.created_at = datetime() -> r)
                        var_name = stmt.split('.')[0].strip()
                        # AGE中使用时间戳字符串而不是函数
                        from datetime import datetime
                        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        all_sets.append(f"{var_name}.created_at = COALESCE({var_name}.created_at, '{current_time}')")
                    else:
                        all_sets.append(stmt)
                
                # 处理MATCH SET - 跳过created_at因为已经处理过了，同时去重
                for stmt in match_sets:
                    if 'created_at' not in stmt and stmt not in all_sets:
                        all_sets.append(stmt)
                
                # 添加SET子句
                if all_sets:
                    result_lines.append('SET ' + ', '.join(all_sets))
                
            else:
                # 非MERGE行，直接添加（但不要添加空行）
                if line:
                    result_lines.append(line)
                i += 1
        
        cypher_stmt = '\n'.join(result_lines)
    
    # 4. 替换datetime()函数为AGE兼容函数
    # AGE中使用NOW()函数代替datetime()函数
    from datetime import datetime
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    # 简单替换：将所有datetime()替换为NOW()或时间戳字符串
    # 对于COALESCE中的datetime()，保留为NOW()；对于其他的，替换为时间戳字符串
    placeholder = "___NOW_PLACEHOLDER___"
    cypher_stmt = re.sub(r'COALESCE\([^,]+,\s*datetime\(\)', 
                        lambda m: m.group(0).replace('datetime()', placeholder), 
                        cypher_stmt)
    cypher_stmt = re.sub(r'datetime\(\)', f"'{current_time}'", cypher_stmt)
    cypher_stmt = cypher_stmt.replace(placeholder, 'NOW()')
    
    return cypher_stmt


async def execute_cypher(conn: asyncpg.Connection, cypher_stmt: str, 
                     params: Dict[str, Any] = None, 
                     graph_name: str = DEFAULT_GRAPH_NAME) -> List[Dict[str, Any]]:
    """
    执行Cypher语句
    
    在AGE 1.5.0中，我们需要将参数直接嵌入到Cypher查询中，而不是使用参数化查询。
    
    Args:
        conn: 数据库连接
        cypher_stmt: Cypher语句
        params: 参数字典
        graph_name: 图名称
        
    Returns:
        List[Dict[str, Any]]: 查询结果
    """
    try:
        # 设置搜索路径
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        
        # 记录原始Cypher语句
        logger.debug(f"原始Cypher语句: {cypher_stmt}")
        
        # 如果有参数，将参数直接嵌入到Cypher语句中
        if params:
            for key, value in params.items():
                placeholder = f"${key}"
                if value is None:
                    replacement = 'null'
                elif isinstance(value, bool):
                    replacement = 'true' if value else 'false'
                elif isinstance(value, (int, float)):
                    replacement = str(value)
                elif isinstance(value, str):
                    # 转义字符串中的引号和反斜杠
                    escaped_value = value.replace('\\', '\\\\').replace("'", "\\'")
                    replacement = f"'{escaped_value}'"
                else:
                    # 对于其他类型，转换为字符串并转义
                    escaped_value = str(value).replace('\\', '\\\\').replace("'", "\\'")
                    replacement = f"'{escaped_value}'"
                
                cypher_stmt = cypher_stmt.replace(placeholder, replacement)
        
        # 记录参数替换后的Cypher语句
        logger.debug(f"参数替换后的Cypher语句: {cypher_stmt}")
        
        # 转换Cypher语句为AGE 1.5.0兼容格式
        converted_cypher = convert_cypher_for_age(cypher_stmt)
        
        # 记录转换后的Cypher语句
        logger.debug(f"AGE转换后的Cypher语句: {converted_cypher}")
        
        # 清理Cypher语句，移除多余的空白和注释，但保持语句结构
        lines = converted_cypher.split('\n')
        clean_lines = []
        for line in lines:
            line = line.strip()
            if line and not line.startswith('//'):
                clean_lines.append(line)
        
        # 不要将所有行合并为一行，保持语句结构
        clean_cypher = '\n'.join(clean_lines)
        
        logger.debug(f"执行Cypher语句: {clean_cypher}")
        
        # 分析RETURN子句来确定列定义
        return_match = re.search(r'RETURN\s+(.+?)(?:\s+(?:ORDER|LIMIT)|$)', clean_cypher, re.IGNORECASE)
        if return_match:
            return_clause = return_match.group(1).strip()
            logger.debug(f"RETURN子句: {return_clause}")
            
            # 解析返回的变量
            return_vars = [var.strip() for var in return_clause.split(',')]
            logger.debug(f"返回变量: {return_vars}")
            
            # 为每个返回变量创建列定义
            column_defs = []
            used_names = set()
            for i, var in enumerate(return_vars):
                # 处理别名 (如 count(c) as column_count)
                if ' as ' in var.lower():
                    alias = var.lower().split(' as ')[-1].strip()
                    column_name = alias
                else:
                    # 对于属性访问 (如 ds.name)，使用完整的变量名作为列名
                    if '.' in var:
                        # 使用完整的变量名，替换点为下划线
                        column_name = var.replace('.', '_').strip()
                    else:
                        # 变量名 (如 ds)
                        column_name = var.strip()
                
                # 确保列名唯一
                original_name = column_name
                counter = 1
                while column_name in used_names:
                    column_name = f"{original_name}_{counter}"
                    counter += 1
                
                used_names.add(column_name)
                column_defs.append(f"{column_name} agtype")
            
            column_def_str = ', '.join(column_defs)
            logger.debug(f"列定义: {column_def_str}")
        else:
            # 如果没有RETURN子句，使用默认的单列定义
            column_def_str = "result agtype"
            logger.debug(f"使用默认列定义: {column_def_str}")
        
        # 构建SQL查询来执行Cypher
        sql_query = f"SELECT * FROM cypher('{graph_name}', $$ {clean_cypher} $$) AS ({column_def_str});"
        
        logger.debug(f"SQL查询: {sql_query}")
        
        # 执行查询
        rows = await conn.fetch(sql_query)
        
        # 转换结果为字典列表
        return [dict(row) for row in rows]
        
    except Exception as e:
        logger.error(f"执行Cypher语句出错: {str(e)}\nCypher: {clean_cypher}")
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
        # 设置搜索路径
        await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
        
        # 检查图是否存在
        check_query = "SELECT * FROM ag_graph WHERE name = $1;"
        result = await conn.fetch(check_query, graph_name)
        
        if not result:
            # 创建图
            create_query = "SELECT * FROM create_graph($1);"
            await conn.execute(create_query, graph_name)
            logger.info(f"创建了新的AGE图: {graph_name}")
        
        return True
    except Exception as e:
        logger.error(f"确保AGE图存在时出错: {str(e)}")
        return False


def generate_timestamp() -> str:
    """
    生成当前时间戳字符串
    
    Returns:
        str: 标准格式的当前时间戳
    """
    from datetime import datetime
    return datetime.now().strftime('%Y-%m-%d %H:%M:%S')


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
