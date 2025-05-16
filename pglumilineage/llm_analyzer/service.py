#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLM 分析器模块

该模块负责使用大型语言模型（LLM）分析 SQL 模式，提取血缘关系信息，
并将结果更新到 lumi_analytics.sql_patterns 表中。
"""

import asyncio
import hashlib
import json
import logging
import os
import re
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple, Set

import asyncpg
import openai
from pydantic import BaseModel

from pglumilineage.common import config, db_utils, models
from pglumilineage.common.logging_config import setup_logging

# 设置日志记录器
logger = logging.getLogger(__name__)

# 初始化 Qwen client
def init_qwen_client():
    """
    初始化 Qwen 客户端
    
    从配置中加载 API Key 和 base_url，初始化 OpenAI 客户端
    """
    try:
        # 从配置中加载 API Key 和 base_url
        api_key = config.settings.llm_api_key
        base_url = config.settings.llm_api_base_url
        
        if not api_key or not base_url:
            logger.error("缺少 LLM API 配置，请检查 API Key 和 base_url 设置")
            return None
        
        # 初始化 OpenAI 客户端
        client = openai.OpenAI(
            api_key=api_key,
            base_url=base_url
        )
        
        logger.info("Qwen 客户端初始化成功")
        return client
    except Exception as e:
        logger.error(f"初始化 Qwen 客户端失败: {str(e)}")
        return None

# 全局 Qwen 客户端
qwen_client = None

async def fetch_pending_sql_patterns(limit: int = 10) -> List[models.AnalyticalSQLPattern]:
    """
    从分析模式表中获取待分析的 SQL 模式
    
    获取 llm_analysis_status 为 'PENDING' 或 'NEEDS_REANALYSIS' 的记录
    并将它们的状态更新为 'PROCESSING'
    
    Args:
        limit: 返回的最大记录数
        
    Returns:
        List[models.AnalyticalSQLPattern]: SQL 模式对象列表
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 从配置中获取模式表名
        from pglumilineage.common.config import get_settings_instance
        settings = get_settings_instance()
        
        # 默认使用lumi_analytics模式
        schema_name = "lumi_analytics"
        
        async with pool.acquire() as conn:
            # 开始事务
            async with conn.transaction():
                # 1. 查询待分析的 SQL 模式
                query = f"""
                SELECT 
                    sql_hash,
                    normalized_sql_text,
                    sample_raw_sql_text,
                    source_database_name,
                    first_seen_at,
                    last_seen_at,
                    execution_count,
                    total_duration_ms,
                    avg_duration_ms,
                    max_duration_ms,
                    min_duration_ms,
                    llm_analysis_status,
                    llm_extracted_relations_json,
                    last_llm_analysis_at,
                    tags
                FROM 
                    {schema_name}.sql_patterns
                WHERE 
                    llm_analysis_status IN ('PENDING', 'NEEDS_REANALYSIS')
                ORDER BY 
                    execution_count DESC, last_seen_at DESC
                LIMIT $1
                FOR UPDATE SKIP LOCKED
                """
                
                rows = await conn.fetch(query, limit)
                
                if not rows:
                    logger.info("没有找到待分析的 SQL 模式")
                    return []
                
                # 2. 将这些记录的状态更新为 'PROCESSING'
                sql_hashes = [row['sql_hash'] for row in rows]
                update_query = f"""
                UPDATE {schema_name}.sql_patterns
                SET 
                    llm_analysis_status = 'PROCESSING'
                WHERE 
                    sql_hash = ANY($1)
                """
                
                await conn.execute(update_query, sql_hashes)
                logger.info(f"将 {len(sql_hashes)} 条 SQL 模式的状态更新为 'PROCESSING'")
                
                # 3. 将查询结果转换为 AnalyticalSQLPattern 对象列表
                patterns = []
                for row in rows:
                    pattern = models.AnalyticalSQLPattern(
                        sql_hash=row['sql_hash'],
                        normalized_sql_text=row['normalized_sql_text'],
                        sample_raw_sql_text=row['sample_raw_sql_text'],
                        source_database_name=row['source_database_name'],
                        first_seen_at=row['first_seen_at'],
                        last_seen_at=row['last_seen_at'],
                        execution_count=row['execution_count'],
                        total_duration_ms=row['total_duration_ms'],
                        avg_duration_ms=row['avg_duration_ms'],
                        max_duration_ms=row['max_duration_ms'],
                        min_duration_ms=row['min_duration_ms'],
                        # 在对象中已经将状态更新为 'PROCESSING'
                        llm_analysis_status='PROCESSING',
                        llm_extracted_relations_json=row['llm_extracted_relations_json'],
                        last_llm_analysis_at=row['last_llm_analysis_at'],
                        tags=row['tags']
                    )
                    patterns.append(pattern)
                
                logger.info(f"获取到 {len(patterns)} 条待分析的 SQL 模式")
                return patterns
            
    except Exception as e:
        logger.error(f"获取待分析的 SQL 模式失败: {str(e)}")
        return []

async def fetch_metadata_context_for_sql(sql_pattern: models.AnalyticalSQLPattern) -> Dict:
    """
    从元数据存储中获取 SQL 相关的元数据上下文
    
    包括表、视图、物化视图的定义、列信息及其关系等
    
    Args:
        sql_pattern: SQL 模式对象，包含 normalized_sql_text, sample_raw_sql_text, source_database_name
        
    Returns:
        Dict: 元数据上下文字典，包含以下信息：
            - source_database_name: 源数据库名称
            - tables_metadata: 表和视图的元数据列表，每个元素包含：
                - schema: 模式名
                - name: 表/视图名
                - type: 对象类型（TABLE, VIEW, MATERIALIZED VIEW）
                - columns: 列信息列表，每个元素包含：
                    - name: 列名
                    - type: 数据类型
                    - nullable: 是否可为空
                    - primary_key: 是否为主键
                    - description: 列描述
                    - foreign_key_to: 外键关系信息（如果存在）
                - row_count: 行数（如果可用）
                - description: 表/视图描述
            - view_definitions: 视图定义列表，每个元素包含：
                - schema: 模式名
                - name: 视图名
                - definition: 视图定义SQL
    """
    try:
        # 初始化返回结果
        metadata_context = {
            "source_database_name": sql_pattern.source_database_name,
            "tables_metadata": [],
            "view_definitions": []
        }
        
        # 获取源数据库的配置信息
        source_db_name = sql_pattern.source_database_name
        
        # 从配置中获取模式名
        from pglumilineage.common.config import get_settings_instance
        settings = get_settings_instance()
        
        # 默认模式名
        config_schema = "lumi_config"
        metadata_schema = "lumi_metadata_store"
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        async with pool.acquire() as conn:
            # 查询源数据库的 source_id
            source_id_query = f"""
            SELECT source_id 
            FROM {config_schema}.data_sources 
            WHERE source_name = $1
            """
            source_id_row = await conn.fetchrow(source_id_query, source_db_name)
            
            if not source_id_row:
                logger.warning(f"未找到源数据库 {source_db_name} 的配置信息")
                return metadata_context
            
            source_id = source_id_row['source_id']
            
            # 1. 使用 sqlglot 解析 SQL 语句中的表引用
            try:
                import sqlglot
                from sqlglot import exp
                
                # 解析 SQL 语句，只使用原始 SQL，不尝试解析泛化后的 SQL
                # 注意：我们使用 sample_raw_sql_text 而不是 normalized_sql_text，因为后者已经被泛化，包含占位符
                try:
                    # 尝试使用 sqlglot 解析原始 SQL
                    logger.info(f"尝试解析原始 SQL: {sql_pattern.sample_raw_sql_text[:100]}...")
                    parsed_sql = sqlglot.parse_one(sql_pattern.sample_raw_sql_text)
                except Exception as e:
                    # 如果解析失败，尝试使用简单的文本处理提取表名
                    logger.warning(f"SQL 解析失败: {str(e)}, 将使用正则表达式提取表名")
                    
                    # 使用正则表达式提取表名
                    import re
                    
                    # 尝试提取常见的SQL语句中的表名
                    table_patterns = [
                        # SELECT ... FROM table_name
                        r'\bFROM\s+([\w\.]+)',
                        # INSERT INTO table_name
                        r'\bINSERT\s+INTO\s+([\w\.]+)',
                        # UPDATE table_name
                        r'\bUPDATE\s+([\w\.]+)',
                        # DELETE FROM table_name
                        r'\bDELETE\s+FROM\s+([\w\.]+)',
                        # CREATE TABLE table_name
                        r'\bCREATE\s+TABLE\s+([\w\.]+)',
                        # ALTER TABLE table_name
                        r'\bALTER\s+TABLE\s+([\w\.]+)',
                        # JOIN table_name
                        r'\bJOIN\s+([\w\.]+)',
                        # MERGE INTO table_name
                        r'\bMERGE\s+INTO\s+([\w\.]+)'
                    ]
                    
                    # 将SQL转换为大写以便于匹配
                    sql_upper = sql_pattern.sample_raw_sql_text.upper()
                    
                    # 存储提取到的表名
                    tables_info = []
                    
                    for pattern in table_patterns:
                        matches = re.finditer(pattern, sql_upper)
                        for match in matches:
                            table_ref = match.group(1)
                            
                            # 处理模式名和表名
                            if '.' in table_ref:
                                schema_name, table_name = table_ref.split('.', 1)
                            else:
                                schema_name = 'public'  # 默认使用 public 模式
                                table_name = table_ref
                            
                            # 将表信息添加到列表中
                            tables_info.append({
                                "schema": schema_name,
                                "name": table_name,
                                "alias": None
                            })
                    
                    # 去重
                    unique_tables = []
                    for table in tables_info:
                        if table not in unique_tables:
                            unique_tables.append(table)
                    
                    logger.info(f"使用正则表达式从 SQL 中提取到 {len(unique_tables)} 个表/视图引用")
                    
                    # 创建一个空的对象来模拟解析结果
                    class MockParsedSQL:
                        def find_all(self, exp_type):
                            # 返回一个对象列表，每个对象模拟一个表引用
                            class MockTableRef:
                                def __init__(self, schema, name, alias):
                                    self.args = {
                                        'db': schema,
                                        'this': name,
                                        'alias': alias
                                    }
                            
                            return [MockTableRef(t["schema"], t["name"], t["alias"]) for t in unique_tables]
                    
                    # 使用模拟的解析结果
                    parsed_sql = MockParsedSQL()
                
                # 提取所有表引用
                table_refs = parsed_sql.find_all(exp.Table)
                
                # 处理每个表引用
                tables_info = []
                for table_ref in table_refs:
                    # 确保 schema_name 是字符串类型
                    schema_name = str(table_ref.args.get('db') or 'public')  # 默认使用 public schema
                    # 确保 table_name 是字符串类型
                    table_name = str(table_ref.args.get('this'))
                    # 确保 table_alias 是字符串类型（如果存在）
                    table_alias = str(table_ref.args.get('alias')) if table_ref.args.get('alias') else None
                    
                    if not table_name:
                        continue
                    
                    # 将表信息添加到列表中
                    tables_info.append({
                        "schema": schema_name,
                        "name": table_name,
                        "alias": table_alias
                    })
                
                # 去重，避免重复查询相同的表
                unique_tables = []
                for table in tables_info:
                    if table not in unique_tables:
                        unique_tables.append(table)
                
                logger.info(f"从 SQL 中提取到 {len(unique_tables)} 个表/视图引用")
                
                # 2. 为每个表/视图获取元数据
                for table_info in unique_tables:
                    schema_name = table_info["schema"]
                    table_name = table_info["name"]
                    
                    # a. 查询对象元数据
                    object_query = f"""
                    SELECT 
                        object_id, 
                        object_type, 
                        definition,
                        row_count,
                        description
                    FROM 
                        {metadata_schema}.objects_metadata 
                    WHERE 
                        source_id = $1 AND 
                        schema_name = $2 AND 
                        object_name = $3
                    """
                    
                    object_row = await conn.fetchrow(
                        object_query, 
                        source_id, 
                        schema_name, 
                        table_name
                    )
                    
                    if not object_row:
                        logger.warning(f"未找到对象 {schema_name}.{table_name} 的元数据")
                        continue
                    
                    object_id = object_row['object_id']
                    object_type = object_row['object_type']
                    definition = object_row['definition']
                    row_count = object_row['row_count']
                    description = object_row['description']
                    
                    # b. 查询列元数据（包括外键信息）
                    columns_query = f"""
                    SELECT 
                        column_name,
                        data_type,
                        is_nullable,
                        default_value,
                        is_primary_key,
                        is_unique,
                        foreign_key_to_table_schema,
                        foreign_key_to_table_name,
                        foreign_key_to_column_name,
                        description
                    FROM 
                        {metadata_schema}.columns_metadata 
                    WHERE 
                        object_id = $1
                    ORDER BY 
                        ordinal_position
                    """
                    
                    columns_rows = await conn.fetch(columns_query, object_id)
                    
                    # 构造列信息列表
                    columns = []
                    for col in columns_rows:
                        # 直接使用data_type字段
                        column_info = {
                            "name": col['column_name'],
                            "type": col['data_type'],  # 直接使用数据库中存储的数据类型
                            "nullable": col['is_nullable'],
                            "primary_key": col['is_primary_key'],
                            "description": col['description']
                        }
                        
                        # 添加默认值信息（如果有）
                        if col['default_value']:
                            column_info["default_value"] = col['default_value']
                        
                        # 添加唯一约束信息
                        if col['is_unique']:
                            column_info["unique"] = True
                        
                        # 添加外键信息（如果有）
                        if col['foreign_key_to_table_schema'] and col['foreign_key_to_table_name'] and col['foreign_key_to_column_name']:
                            # 保留旧的格式以保持兼容性
                            column_info["foreign_key_to"] = {
                                "schema": col['foreign_key_to_table_schema'],
                                "table": col['foreign_key_to_table_name'],
                                "column": col['foreign_key_to_column_name']
                            }
                            
                            # 添加新的外键引用格式
                            column_info["foreign_key_reference"] = {
                                "table_schema": col['foreign_key_to_table_schema'],
                                "table_name": col['foreign_key_to_table_name'],
                                "column_name": col['foreign_key_to_column_name']
                            }
                        
                        columns.append(column_info)
                    
                    # 构造对象元数据
                    object_metadata = {
                        "schema": schema_name,
                        "name": table_name,
                        "type": object_type,
                        "columns": columns,
                        "row_count": row_count,
                        "description": description
                    }
                    
                    # 将对象元数据添加到相应的列表中
                    metadata_context["tables_metadata"].append(object_metadata)
                    
                    # 如果是视图或物化视图，将定义添加到视图定义列表中
                    if (object_type == 'VIEW' or object_type == 'MATERIALIZED VIEW') and definition:
                        metadata_context["view_definitions"].append({
                            "schema": schema_name,
                            "name": table_name,
                            "type": object_type,
                            "definition": definition
                        })
            except sqlglot.errors.ParseError as e:
                logger.warning(f"SQL 解析失败: {str(e)}")
                # 如果解析失败，返回空的元数据上下文
                return metadata_context
                
        # 添加一些统计信息到日志
        tables_count = sum(1 for item in metadata_context['tables_metadata'] if item['type'] == 'TABLE')
        views_count = sum(1 for item in metadata_context['tables_metadata'] if item['type'] == 'VIEW')
        materialized_views_count = sum(1 for item in metadata_context['tables_metadata'] if item['type'] == 'MATERIALIZED VIEW')
        columns_count = sum(len(item['columns']) for item in metadata_context['tables_metadata'])
        
        logger.info(f"获取到 SQL 模式 {sql_pattern.sql_hash[:8]}... 的元数据上下文")
        logger.info(f"  - 表: {tables_count}个")
        logger.info(f"  - 视图: {views_count}个")
        logger.info(f"  - 物化视图: {materialized_views_count}个")
        logger.info(f"  - 总列数: {columns_count}个")
        logger.info(f"  - 视图定义: {len(metadata_context['view_definitions'])}个")
        
        return metadata_context
        
    except Exception as e:
        logger.error(f"获取 SQL 模式 {sql_pattern.sql_hash[:8]}... 的元数据上下文失败: {str(e)}")
        return {"error": str(e), "tables_metadata": [], "view_definitions": []}

def construct_prompt_for_qwen(sql_mode: str, sample_sql: str, metadata_context: Dict, sql_hash: str = None) -> List[Dict[str, str]]:
    """
    构造 Qwen 模型的 prompt
    
    Args:
        sql_mode: SQL 模式类型（如 INSERT, UPDATE, SELECT 等）
        sample_sql: 示例 SQL 语句
        metadata_context: 元数据上下文
        sql_hash: SQL 模式的哈希值，可选
        
    Returns:
        List[Dict[str, str]]: Qwen 模型的消息列表
    """
    try:
        # 构造系统提示
        system_prompt = """你是一位顶级的SQL数据血缘分析专家。
你的任务是基于用户提供的SQL语句、相关的数据库对象元数据（表结构、视图定义等），以及SQL的唯一标识哈希（如果提供），精确地分析出字段级别的数据血缘关系。
你需要识别数据是如何从源对象的源字段，经过可能的转换逻辑，最终写入到目标对象的特定目标字段。

请严格按照用户要求的JSON格式输出分析结果，不要包含任何解释性文字或Markdown标记，只输出纯JSON对象。
JSON中应包含以下关键信息：
- 'sql_pattern_hash': 提供的SQL模式的唯一哈希值（如果提供）。
- 'source_database_name': SQL所属的源数据库名称。
- 'target_object': 如果SQL有明确的写入目标（如INSERT的目标表，UPDATE的表，CREATE VIEW的视图名），请提供其 'schema', 'name', 'type'。若无明确写入目标（如纯SELECT），此字段可为null。
- 'column_level_lineage': 一个列表，每个元素描述一个目标字段的血缘：
    - 'target_column': 目标字段名。
    - 'target_object_name': (可选，如果target_object非null) 目标对象名，用于消除歧义。
    - 'target_object_schema': (可选) 目标对象schema。
    - 'sources': 一个列表，包含所有对此目标字段有贡献的源信息。每个源信息包含：
        - 'source_object': {'schema': '...', 'name': '...', 'type': 'TABLE'/'VIEW'}
        - 'source_column': 源字段名。
        - 'transformation_logic': 描述从源到目标所经历的简要转换逻辑或计算表达式（例如 'direct_copy', 'SUM(...)', 'COALESCE(col, 0)', 'CASE WHEN ...', 'd.d_year AS report_year'）。
    - 'derivation_type': 对目标字段值产生方式的分类（例如 'DIRECT_MAPPING', 'AGGREGATION', 'LITERAL_ASSIGNMENT', 'CONDITIONAL_LOGIC', 'FUNCTION_CALL', 'UNION_MERGE'）。
- 'referenced_objects': 一个列表，包含SQL中所有被引用（读取或写入）的数据库对象（表、视图）及其 'schema', 'name', 'type' 和 'access_mode' ('READ', 'WRITE', 'READ_WRITE')。
- 'unresolved_references': (可选) 一个列表，记录在提供的元数据中找不到对应定义的表或视图名。
- 'parsing_confidence': (可选) 你对本次解析准确度的信心评分 (0.0 - 1.0)。
- 'errors_or_warnings': (可选) 一个列表，记录解析过程中遇到的任何问题或警告。"""

        # 获取源数据库名称
        source_database_name = ""
        for table in metadata_context.get("tables_metadata", []):
            if table.get("schema") and table.get("name"):
                source_database_name = metadata_context.get("source_database_name", "")
                break
        
        # 构造元数据上下文的文本表示
        metadata_str_parts = [f"源数据库名称: {source_database_name}"]
        
        # 格式化表结构信息
        for table in metadata_context.get("tables_metadata", []):
            schema = table.get("schema", "public")
            name = table.get("name", "")
            table_type = table.get("type", "TABLE").upper()
            part = f"\n{table_type} '{schema}.{name}':"
            
            if table.get("description"):
                part += f"\n  描述: {table['description']}"
            
            if table.get("row_count") is not None:
                part += f"\n  行数: {table['row_count']}"
            
            # 添加列信息
            columns_str = []
            for col in table.get("columns", []):
                col_name = col.get("name", "")
                col_type = col.get("type", "")
                nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
                pk = "PRIMARY KEY" if col.get("primary_key", False) else ""
                col_desc = f"{col_name} ({col_type} {nullable} {pk})".strip()
                if col.get("description"):
                    col_desc += f" -- {col['description']}"
                columns_str.append(f"    - {col_desc}")
            
            if columns_str:
                part += "\n  列信息:\n" + "\n".join(columns_str)
            
            metadata_str_parts.append(part)
        
        # 格式化视图定义
        for view in metadata_context.get("view_definitions", []):
            schema = view.get("schema", "public")
            name = view.get("name", "")
            definition = view.get("definition", "").strip()
            view_part = f"\n视图 '{schema}.{name}':\n  定义SQL: {definition}"
            metadata_str_parts.append(view_part)
        
        # 确定SQL类型和目标对象
        target_object = None
        if sql_mode == "INSERT":
            import re
            match = re.search(r"INSERT\s+INTO\s+([\w\.]+)", sample_sql, re.IGNORECASE)
            if match:
                target_table = match.group(1)
                parts = target_table.split('.')
                if len(parts) > 1:
                    target_object = {"schema": parts[0], "name": parts[1], "type": "TABLE"}
                else:
                    target_object = {"schema": "public", "name": parts[0], "type": "TABLE"}
        elif sql_mode == "UPDATE":
            import re
            match = re.search(r"UPDATE\s+([\w\.]+)", sample_sql, re.IGNORECASE)
            if match:
                target_table = match.group(1)
                parts = target_table.split('.')
                if len(parts) > 1:
                    target_object = {"schema": parts[0], "name": parts[1], "type": "TABLE"}
                else:
                    target_object = {"schema": "public", "name": parts[0], "type": "TABLE"}
        elif sql_mode == "CREATE" and "TABLE" in sample_sql.upper():
            import re
            match = re.search(r"CREATE\s+TABLE\s+([\w\.]+)", sample_sql, re.IGNORECASE)
            if match:
                target_table = match.group(1)
                parts = target_table.split('.')
                if len(parts) > 1:
                    target_object = {"schema": parts[0], "name": parts[1], "type": "TABLE"}
                else:
                    target_object = {"schema": "public", "name": parts[0], "type": "TABLE"}
        elif sql_mode == "CREATE" and "VIEW" in sample_sql.upper():
            import re
            match = re.search(r"CREATE\s+VIEW\s+([\w\.]+)", sample_sql, re.IGNORECASE)
            if match:
                target_view = match.group(1)
                parts = target_view.split('.')
                if len(parts) > 1:
                    target_object = {"schema": parts[0], "name": parts[1], "type": "VIEW"}
                else:
                    target_object = {"schema": "public", "name": parts[0], "type": "VIEW"}
        
        # 构造用户提示
        user_prompt = f"""请分析以下SQL语句，并提取字段级血缘关系。

{f'SQL模式的唯一哈希: {sql_hash}' if sql_hash else ''}

原始SQL:
```sql
{sample_sql}
```

相关的数据库对象元数据:
{'\n'.join(metadata_str_parts)}

{f'目标对象: {json.dumps(target_object, ensure_ascii=False)}' if target_object else ''}

请分析SQL中的字段级数据血缘关系，并以JSON格式输出结果。JSON应包含：
1. 'sql_pattern_hash': {f'\'{sql_hash}\'' if sql_hash else 'null'}
2. 'source_database_name': 源数据库名称
3. 'target_object': 写入目标对象信息（如果有）
4. 'column_level_lineage': 字段级血缘关系列表
5. 'referenced_objects': SQL中引用的所有数据库对象
6. 'parsing_confidence': 解析准确度评分
7. 其他可选字段（如有必要）

请确保输出的是有效的JSON格式，不包含任何解释性文字或Markdown标记。
"""

        # 构造消息列表
        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]
        
        return messages
        
    except Exception as e:
        logger.error(f"构造 Qwen prompt 失败: {str(e)}")
        return [{"role": "system", "content": "Error constructing prompt"}]

async def call_qwen_api(messages: List[Dict[str, str]], model_name: str = None) -> Optional[str]:
    """
    调用 Qwen API
    
    Args:
        messages: 消息列表
        model_name: 模型名称，如果为 None 则使用配置中的默认值
        
    Returns:
        Optional[str]: API 响应内容
    """
    from pglumilineage.common.config import get_settings_instance
    from openai import AsyncOpenAI
    
    try:
        # 获取配置实例
        settings = get_settings_instance()
        
        # 获取配置信息
        api_key = None
        base_url = 'https://dashscope.aliyuncs.com/compatible-mode/v1'
        default_model = 'qwen-plus-latest'
        
        # 尝试从不同的配置路径获取API密钥
        # 1. 尝试直接从配置文件中获取
        # 读取配置文件
        config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', 'settings.toml')
        if os.path.exists(config_path):
            try:
                import toml
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = toml.load(f)
                    
                # 从配置文件中获取LLM相关配置
                if 'llm' in config:
                    llm_config = config['llm']
                    
                    # 尝试获取API密钥
                    if 'API_KEY' in llm_config:
                        api_key = llm_config['API_KEY']
                    elif 'DASHSCOPE_API_KEY' in llm_config:
                        api_key = llm_config['DASHSCOPE_API_KEY']
                    
                    # 尝试获取模型名称
                    if 'MODEL_NAME' in llm_config:
                        default_model = llm_config['MODEL_NAME']
                    
                    # 尝试获取基础URL
                    if 'BASE_URL' in llm_config:
                        base_url = llm_config['BASE_URL']
                    
                    logger.info(f"从配置文件中获取到LLM配置，模型: {default_model}")
            except Exception as e:
                logger.warning(f"读取配置文件失败: {str(e)}")
        
        # 2. 如果从配置文件中没有获取到API密钥，尝试从配置模块中获取
        if not api_key:
            # 尝试新配置结构
            if hasattr(settings, 'llm') and settings.llm:
                # 尝试qwen子配置
                if hasattr(settings.llm, 'qwen'):
                    api_key = getattr(settings.llm.qwen, 'api_key', None)
                    base_url = getattr(settings.llm.qwen, 'base_url', base_url)
                    default_model = getattr(settings.llm.qwen, 'model_name', default_model)
                
                # 如果没有qwen子配置，尝试直接从 llm 配置中获取
                if not api_key and hasattr(settings.llm, 'api_key'):
                    api_key = settings.llm.api_key
                    
                # 尝试从 dashscope 子配置中获取
                if not api_key and hasattr(settings.llm, 'dashscope_api_key'):
                    api_key = settings.llm.dashscope_api_key
            
            # 尝试旧配置结构
            if not api_key:
                # 尝试直接从设置中获取
                api_key = getattr(settings, 'DASHSCOPE_API_KEY', None)
                if not api_key:
                    api_key = getattr(settings, 'LLM_API_KEY', None)
                if not api_key:
                    api_key = getattr(settings, 'API_KEY', None)
                
                # 如果是 SecretStr 类型，获取实际值
                if hasattr(api_key, 'get_secret_value'):
                    api_key = api_key.get_secret_value()
                    
                # 获取其他配置
                base_url_from_settings = getattr(settings, 'QWEN_BASE_URL', None) or getattr(settings, 'BASE_URL', None)
                if base_url_from_settings:
                    base_url = str(base_url_from_settings)
                    
                model_from_settings = getattr(settings, 'QWEN_MODEL_NAME', None) or getattr(settings, 'MODEL_NAME', None)
                if model_from_settings:
                    default_model = model_from_settings
        
        if not api_key:
            logger.error("未找到通义千问API密钥，请检查配置")
            return None
            
        # 使用提供的模型名称或默认值
        model = model_name if model_name else default_model
        
        # 初始化 AsyncOpenAI 客户端
        client = AsyncOpenAI(
            api_key=api_key,
            base_url=base_url
        )
        
        logger.info(f"正在调用 Qwen API，模型: {model}")
        
        # 调用 Qwen API
        response = await client.chat.completions.create(
            model=model,
            messages=messages,
            temperature=0.2,  # 低温度以获得更确定性的结果
            max_tokens=4000
        )
        
        # 提取响应内容
        if response and response.choices and len(response.choices) > 0:
            content = response.choices[0].message.content
            logger.info("成功调用 Qwen API")
            return content
        else:
            logger.warning("Qwen API 返回空响应")
            return None
            
    except ImportError as e:
        logger.error(f"OpenAI 库导入失败，请确保安装了最新版本: {str(e)}")
        return None
    except Exception as e:
        # 处理各种可能的异常
        if "auth" in str(e).lower() or "key" in str(e).lower() or "401" in str(e):
            logger.error(f"Qwen API 认证失败: {str(e)}")
        elif "rate" in str(e).lower() or "limit" in str(e).lower() or "429" in str(e):
            logger.error(f"Qwen API 请求限流: {str(e)}")
        elif "timeout" in str(e).lower() or "connect" in str(e).lower():
            logger.error(f"Qwen API 网络连接问题: {str(e)}")
        else:
            logger.error(f"调用 Qwen API 失败: {str(e)}")
        return None

def parse_llm_response(response_content: str) -> Optional[Dict]:
    """
    解析 LLM 的响应，提取结构化的血缘关系信息
    
    Args:
        response_content: LLM 响应内容
        
    Returns:
        Optional[Dict]: 解析后的结构化血缘关系信息
    """
    try:
        if not response_content:
            logger.warning("LLM 响应内容为空")
            return None
        
        # 尝试提取 JSON 内容
        # 首先检查是否有 ```json ... ``` 格式
        import re
        json_pattern = r'```(?:json)?\s*([\s\S]*?)\s*```'
        json_match = re.search(json_pattern, response_content)
        
        if json_match:
            json_str = json_match.group(1)
            logger.debug(f"从 markdown 代码块中提取到 JSON 字符串")
        else:
            # 如果没有 markdown 格式，尝试直接解析整个响应
            json_str = response_content
            logger.debug(f"使用完整响应作为 JSON 字符串")
        
        # 清理 JSON 字符串，处理可能的格式问题
        # 替换单引号为双引号（如果存在）
        json_str = json_str.replace("'", "\"")
        
        # 删除可能的 JavaScript 注释
        json_str = re.sub(r'\s*//.*?[\r\n]', '\n', json_str)
        
        # 解析 JSON
        relations_json = json.loads(json_str)
        
        # 记录成功解析的信息
        logger.info(f"成功解析 LLM 响应为 JSON 对象")
        
        # 返回解析后的 JSON 对象
        return relations_json
        
    except json.JSONDecodeError as e:
        logger.error(f"解析 LLM 响应 JSON 失败: {str(e)}")
        logger.debug(f"尝试解析的内容: {response_content[:500]}...")
        return None
    except Exception as e:
        logger.error(f"解析 LLM 响应失败: {str(e)}")
        return None

async def update_sql_pattern_analysis_result(sql_hash: str, status: str, relations_json: Optional[Dict], error_message: Optional[str] = None):
    """
    更新 SQL 模式的分析结果
    
    Args:
        sql_hash: SQL 哈希值
        status: 分析状态，如 'SUCCESS', 'FAILED', 'PENDING'
        relations_json: 解析后的血缘关系信息
        error_message: 错误信息（如果有）
    """
    try:
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 从配置中获取模式表名
        from pglumilineage.common.config import get_settings_instance
        settings = get_settings_instance()
        
        # 默认使用lumi_analytics模式
        schema_name = "lumi_analytics"
        
        # 构建 UPDATE SQL 语句，包含错误信息字段
        query = f"""
        UPDATE {schema_name}.sql_patterns
        SET 
            llm_analysis_status = $1,
            llm_extracted_relations_json = $2,
            last_llm_analysis_at = CURRENT_TIMESTAMP,
            llm_error_message = $3
        WHERE 
            sql_hash = $4
        """
        
        # 如果 relations_json 存在，将其转换为 JSON 字符串
        relations_json_str = None
        if relations_json:
            try:
                relations_json_str = json.dumps(relations_json, ensure_ascii=False)
            except Exception as json_err:
                logger.error(f"序列化 relations_json 失败: {str(json_err)}")
                # 如果序列化失败，更新错误信息
                error_message = f"序列化关系 JSON 失败: {str(json_err)}" + (f", 原始错误: {error_message}" if error_message else "")
                status = "FAILED"
        
        async with pool.acquire() as conn:
            # 执行 UPDATE 操作
            await conn.execute(
                query,
                status,
                relations_json_str,
                error_message,
                sql_hash
            )
            
            logger.info(f"成功更新 SQL 模式 {sql_hash[:8]}... 的分析结果，状态: {status}")
            if error_message:
                logger.debug(f"SQL {sql_hash[:8]}... 错误信息: {error_message}")
            
    except Exception as e:
        logger.error(f"更新 SQL 模式 {sql_hash[:8]}... 的分析结果失败: {str(e)}")

async def analyze_sql_patterns_with_llm(batch_size: int = 10, poll_interval_seconds: int = 60, run_once: bool = False):
    """
    使用 LLM 分析 SQL 模式
    
    主函数，协调整个分析流程。定期轮询并处理待分析的 SQL 模式。
    
    Args:
        batch_size: 每批处理的 SQL 模式数量
        poll_interval_seconds: 轮询间隔（秒）
        run_once: 是否只运行一次（用于测试）
    """
    logger.info(f"启动 LLM 分析器服务，批大小: {batch_size}, 轮询间隔: {poll_interval_seconds}秒")
    
    # 初始化数据库连接池
    try:
        # 初始化数据库连接池
        pool = await db_utils.get_db_pool()
        logger.info("数据库连接池初始化成功")
    except Exception as e:
        logger.error(f"初始化数据库连接池失败: {str(e)}")
        return
    
    # 设置信号处理程序，以便优雅退出
    import signal
    running = True
    
    def signal_handler(sig, frame):
        nonlocal running
        logger.info(f"收到信号 {sig}，准备优雅退出...")
        running = False
    
    # 注册信号处理程序
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 统计信息
    total_processed = 0
    total_success = 0
    total_failed = 0
    
    # 主循环
    try:
        while running:
            cycle_start_time = time.time()
            try:
                # 1. 获取待分析的 SQL 模式
                patterns = await fetch_pending_sql_patterns(batch_size)
                
                if not patterns:
                    logger.info("没有找到待分析的 SQL 模式，等待下次轮询")
                    if run_once:
                        logger.info("运行模式为单次运行，退出程序")
                        break
                    await asyncio.sleep(poll_interval_seconds)
                    continue
                
                logger.info(f"获取到 {len(patterns)} 条待分析的 SQL 模式")
                
                # 2. 逐个处理 SQL 模式
                batch_success = 0
                batch_failed = 0
                
                for pattern in patterns:
                    if not running:
                        logger.info("收到退出信号，中断处理")
                        break
                    
                    try:
                        logger.info(f"开始处理 SQL 模式: {pattern.sql_hash[:8]}...")
                        
                        # 获取 SQL 模式的元数据上下文
                        metadata_context = await fetch_metadata_context_for_sql(pattern)
                        
                        # 确定 SQL 模式类型
                        sql_mode = "UNKNOWN"
                        normalized_sql_lower = pattern.normalized_sql_text.lower()
                        if normalized_sql_lower.startswith("insert"):
                            sql_mode = "INSERT"
                        elif normalized_sql_lower.startswith("update"):
                            sql_mode = "UPDATE"
                        elif normalized_sql_lower.startswith("select"):
                            sql_mode = "SELECT"
                        elif normalized_sql_lower.startswith("create"):
                            sql_mode = "CREATE"
                        elif normalized_sql_lower.startswith("delete"):
                            sql_mode = "DELETE"
                        elif normalized_sql_lower.startswith("merge"):
                            sql_mode = "MERGE"
                        
                        logger.info(f"SQL 模式类型: {sql_mode}, 哈希值: {pattern.sql_hash[:8]}...")
                        
                        # 构造 Qwen prompt
                        messages = construct_prompt_for_qwen(
                            sql_mode=sql_mode,
                            sample_sql=pattern.sample_raw_sql_text,
                            metadata_context=metadata_context
                        )
                        
                        # 调用 Qwen API
                        logger.info(f"调用 Qwen API 分析 SQL 模式: {pattern.sql_hash[:8]}...")
                        response_content = await call_qwen_api(messages)
                        
                        if not response_content:
                            # 更新分析状态为失败
                            await update_sql_pattern_analysis_result(
                                sql_hash=pattern.sql_hash,
                                status="FAILED",
                                relations_json=None,
                                error_message="LLM API 返回空响应"
                            )
                            logger.warning(f"SQL 模式 {pattern.sql_hash[:8]}... 分析失败: LLM API 返回空响应")
                            batch_failed += 1
                            continue
                        
                        # 解析 LLM 响应
                        logger.info(f"解析 LLM 响应: {pattern.sql_hash[:8]}...")
                        relations_json = parse_llm_response(response_content)
                        
                        if relations_json:
                            # 更新分析状态为成功
                            await update_sql_pattern_analysis_result(
                                sql_hash=pattern.sql_hash,
                                status="SUCCESS",
                                relations_json=relations_json
                            )
                            logger.info(f"SQL 模式 {pattern.sql_hash[:8]}... 分析成功")
                            batch_success += 1
                        else:
                            # 更新分析状态为失败
                            await update_sql_pattern_analysis_result(
                                sql_hash=pattern.sql_hash,
                                status="FAILED",
                                relations_json=None,
                                error_message="无法解析 LLM 响应"
                            )
                            logger.warning(f"SQL 模式 {pattern.sql_hash[:8]}... 分析失败: 无法解析 LLM 响应")
                            batch_failed += 1
                        
                    except Exception as e:
                        logger.error(f"处理 SQL 模式 {pattern.sql_hash[:8]}... 时出错: {str(e)}")
                        # 更新分析状态为失败
                        await update_sql_pattern_analysis_result(
                            sql_hash=pattern.sql_hash,
                            status="FAILED",
                            relations_json=None,
                            error_message=str(e)
                        )
                        batch_failed += 1
                
                # 更新统计信息
                total_processed += len(patterns)
                total_success += batch_success
                total_failed += batch_failed
                
                # 记录批处理结果
                logger.info(f"完成当前批 SQL 模式的 LLM 分析: 成功 {batch_success}, 失败 {batch_failed}")
                logger.info(f"总计: 已处理 {total_processed}, 成功 {total_success}, 失败 {total_failed}")
                
                # 如果是单次运行模式，则退出
                if run_once:
                    logger.info("运行模式为单次运行，退出程序")
                    break
                
                # 计算当前周期耗时
                cycle_duration = time.time() - cycle_start_time
                sleep_time = max(0, poll_interval_seconds - cycle_duration)
                
                if sleep_time > 0:
                    logger.info(f"当前周期耗时 {cycle_duration:.2f} 秒，休眠 {sleep_time:.2f} 秒后继续下一周期")
                    await asyncio.sleep(sleep_time)
                else:
                    logger.info(f"当前周期耗时 {cycle_duration:.2f} 秒，立即开始下一周期")
                
            except Exception as e:
                logger.error(f"LLM 分析器周期执行出错: {str(e)}")
                logger.info(f"等待 {poll_interval_seconds} 秒后重试")
                await asyncio.sleep(poll_interval_seconds)
        
        logger.info("服务正常退出")
        logger.info(f"总计: 已处理 {total_processed}, 成功 {total_success}, 失败 {total_failed}")
        
    except Exception as e:
        logger.error(f"LLM 分析器服务发生未知错误: {str(e)}")
    finally:
        # 关闭数据库连接池
        try:
            await db_utils.close_db_pool()
            logger.info("数据库连接池已关闭")
        except Exception as e:
            logger.error(f"关闭数据库连接池失败: {str(e)}")

async def main():
    """
    主函数
    """
    # 设置日志
    from pglumilineage.common.logging_config import setup_logging
    setup_logging()
    
    # 导入必要的模块
    import time
    
    # 从配置中获取参数
    from pglumilineage.common.config import settings
    
    # 启动 LLM 分析器服务
    logger.info("启动 LLM 分析器服务...")
    await analyze_sql_patterns_with_llm(
        batch_size=10,  # 可以从配置中读取
        poll_interval_seconds=60  # 可以从配置中读取
    )

if __name__ == "__main__":
    # 导入必要的模块
    import asyncio
    import time
    
    # 运行主函数
    asyncio.run(main())
