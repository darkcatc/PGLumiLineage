#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
LLM 分析器模块

该模块负责使用大型语言模型（LLM）分析 SQL 模式，提取血缘关系信息，
并将结果更新到 lumi_analytics.sql_patterns 表中。
"""

import asyncio
import os
import json
import logging
from typing import List, Dict, Optional, Any, Tuple

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
    从 lumi_analytics.sql_patterns 表中获取待分析的 SQL 模式
    
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
        
        async with pool.acquire() as conn:
            # 开始事务
            async with conn.transaction():
                # 1. 查询待分析的 SQL 模式
                query = """
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
                    lumi_analytics.sql_patterns
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
                update_query = """
                UPDATE lumi_analytics.sql_patterns
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

async def fetch_metadata_context_for_sql(conn_iwdb: asyncpg.Connection, sql_pattern: models.AnalyticalSQLPattern) -> Dict:
    """
    从元数据存储中获取 SQL 相关的元数据上下文
    
    包括表、视图、列的定义和关系等信息
    
    Args:
        conn_iwdb: iwdb 数据库连接
        sql_pattern: SQL 模式对象
        
    Returns:
        Dict: 元数据上下文字典
    """
    try:
        # 初始化返回结果
        metadata_context = {
            "tables_metadata": [],
            "view_definitions": []
        }
        
        # 获取源数据库的配置信息
        source_db_name = sql_pattern.source_database_name
        
        # 查询源数据库的 source_id
        source_id_query = """
        SELECT source_id 
        FROM lumi_config.data_sources 
        WHERE source_name = $1
        """
        source_id_row = await conn_iwdb.fetchrow(source_id_query, source_db_name)
        
        if not source_id_row:
            logger.warning(f"未找到源数据库 {source_db_name} 的配置信息")
            return metadata_context
        
        source_id = source_id_row['source_id']
        
        # 1. 使用 sqlglot 解析 SQL 语句中的表引用
        try:
            import sqlglot
            from sqlglot import exp
            
            # 解析 SQL 语句
            parsed_sql = sqlglot.parse_one(sql_pattern.normalized_sql_text)
            
            # 提取所有表引用
            table_refs = parsed_sql.find_all(exp.Table)
            
            # 处理每个表引用
            tables_info = []
            for table_ref in table_refs:
                schema_name = table_ref.args.get('db') or 'public'  # 默认使用 public schema
                table_name = table_ref.args.get('this')
                table_alias = table_ref.args.get('alias')
                
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
                object_query = """
                SELECT 
                    object_id, 
                    object_type, 
                    definition,
                    row_count,
                    description
                FROM 
                    lumi_metadata_store.objects_metadata 
                WHERE 
                    source_id = $1 AND 
                    schema_name = $2 AND 
                    object_name = $3
                """
                
                object_row = await conn_iwdb.fetchrow(
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
                
                # b. 查询列元数据
                columns_query = """
                SELECT 
                    column_name, 
                    data_type, 
                    is_nullable, 
                    is_primary_key,
                    description
                FROM 
                    lumi_metadata_store.columns_metadata 
                WHERE 
                    object_id = $1
                ORDER BY 
                    ordinal_position
                """
                
                columns_rows = await conn_iwdb.fetch(columns_query, object_id)
                
                # 构造列信息列表
                columns = []
                for col in columns_rows:
                    columns.append({
                        "name": col['column_name'],
                        "type": col['data_type'],
                        "nullable": col['is_nullable'],
                        "primary_key": col['is_primary_key'],
                        "description": col['description']
                    })
                
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
                
                # 如果是视图，将定义添加到视图定义列表中
                if object_type == 'VIEW' and definition:
                    metadata_context["view_definitions"].append({
                        "schema": schema_name,
                        "name": table_name,
                        "definition": definition
                    })
            
        except sqlglot.errors.ParseError as e:
            logger.warning(f"SQL 解析失败: {str(e)}")
            # 如果解析失败，返回空的元数据上下文
            return metadata_context
        
        logger.info(f"获取到 SQL 模式 {sql_pattern.sql_hash[:8]}... 的元数据上下文，包含 {len(metadata_context['tables_metadata'])} 个表/视图的元数据")
        return metadata_context
        
    except Exception as e:
        logger.error(f"获取 SQL 模式 {sql_pattern.sql_hash[:8]}... 的元数据上下文失败: {str(e)}")
        return {"error": str(e), "tables_metadata": [], "view_definitions": []}

def construct_prompt_for_qwen(sql_mode: str, sample_sql: str, metadata_context: Dict) -> List[Dict[str, str]]:
    """
    构造 Qwen 模型的 prompt
    
    Args:
        sql_mode: SQL 模式类型（如 INSERT, UPDATE, SELECT 等）
        sample_sql: 示例 SQL 语句
        metadata_context: 元数据上下文
        
    Returns:
        List[Dict[str, str]]: Qwen 模型的消息列表
    """
    try:
        # 构造系统提示
        system_prompt = """你是一个专业的SQL数据血缘分析助手。你的任务是分析给定的SQL语句和相关的数据库元数据，识别出数据是如何从源表/源字段流向目标表/目标字段的。请以JSON格式输出结果。"""

        # 构造用户提示
        
        # 格式化表结构信息以更清晰地展示
        tables_info = ""
        for table in metadata_context.get("tables_metadata", []):
            schema = table.get("schema", "")
            name = table.get("name", "")
            table_type = table.get("type", "TABLE")
            tables_info += f"\n\u8868 {schema}.{name} ({table_type}):\n"
            
            # 添加列信息
            for col in table.get("columns", []):
                col_name = col.get("name", "")
                col_type = col.get("type", "")
                nullable = "NULL" if col.get("nullable", True) else "NOT NULL"
                pk = "PRIMARY KEY" if col.get("primary_key", False) else ""
                tables_info += f"  - {col_name} ({col_type} {nullable} {pk})\n"
        
        # 格式化视图定义
        views_info = ""
        for view in metadata_context.get("view_definitions", []):
            schema = view.get("schema", "")
            name = view.get("name", "")
            definition = view.get("definition", "").strip()
            views_info += f"\n\u89c6\u56fe {schema}.{name}:\n{definition}\n"
        
        # 根据 SQL 类型调整任务描述
        task_description = ""
        target_table = ""
        
        if sql_mode == "INSERT":
            # 尝试从 SQL 中提取目标表名
            import re
            match = re.search(r"INSERT\s+INTO\s+([\w\.]+)", sample_sql, re.IGNORECASE)
            if match:
                target_table = match.group(1)
            
            task_description = f"""请分析以下 INSERT SQL 语句。识别出所有最终被插入到目标表 `{target_table}` 的每个字段的数据来源。请列出从源表的哪个字段，经过了怎样的转换（如果有的话），最终写入了目标表的哪个字段。"""
        elif sql_mode == "UPDATE":
            # 尝试从 SQL 中提取目标表名
            import re
            match = re.search(r"UPDATE\s+([\w\.]+)", sample_sql, re.IGNORECASE)
            if match:
                target_table = match.group(1)
            
            task_description = f"""请分析以下 UPDATE SQL 语句。识别出被更新的表 `{target_table}` 中每个被 SET 的字段的数据来源。请列出 SET 表达式右侧依赖的字段或计算逻辑。"""
        elif sql_mode == "CREATE" and "SELECT" in sample_sql.upper():
            task_description = """请分析以下 CREATE TABLE AS SELECT 语句。识别出新创建的表中每个字段的数据来源。请列出从源表的哪个字段，经过了怎样的转换，最终写入了新表的哪个字段。"""
        else:
            task_description = """请分析以下 SQL 语句。识别出数据流转的源和目标，以及字段级别的映射关系。"""
        
        # 构造输出格式要求
        output_format = """请严格按照以下JSON格式输出结果。只输出JSON内容，不要有任何其他解释性文字。
{
  'target_table': 'annual_channel_performance_report',
  'lineage': [
    {
      'target_column': 'report_year',
      'source_columns': [{'table': 'date_dim', 'column': 'd_year', 'transformation': 'direct_mapping'}],
      'logic_description': 'd.d_year AS report_year'
    },
    {
      'target_column': 'channel',
      'source_columns': [], // 如果是字面量或固定值
      'transformation': 'literal_value_assignment',
      'logic_description': "'?' AS channel"
    },
    {
      'target_column': 'total_sales_amount_inc_tax_ship',
      'source_columns': [
        {'table': 'store_sales', 'column': 'ss_net_paid_inc_tax', 'transformation': 'COALESCE(ss.ss_net_paid_inc_tax, ?)'},
        {'table': 'catalog_sales', 'column': 'cs_net_paid_inc_ship_tax', 'transformation': 'COALESCE(cs.cs_net_paid_inc_ship_tax, ?)'},
        {'table': 'web_sales', 'column': 'ws_net_paid_inc_ship_tax', 'transformation': 'COALESCE(ws.ws_net_paid_inc_ship_tax, ?)'}
      ],
      'aggregation': 'SUM', // 新增，表示聚合
      'logic_description': 'SUM(COALESCE(ss.ss_net_paid_inc_tax, ?)) ... UNION ALL ...'
    }
  ],
  'source_tables_involved': ['store_sales', 'date_dim', 'catalog_sales', 'web_sales'] // 列出所有读取的源表
}

如果SQL是UPDATE语句，'target_table' 仍然是被更新的表，'target_column' 是被SET的列，'source_columns' 是SET表达式右侧依赖的字段。
如果SQL是纯SELECT或者VIEW定义，可以将 'target_table' 设为 null 或 'VIEW_OUTPUT', 'target_column' 对应SELECT列表中的别名，'source_columns' 对应其来源。
如果无法解析或SQL不涉及数据写入，可以返回一个空的 'lineage' 列表或特定错误标记。"""
        
        # 组合完整的用户提示
        user_prompt = f"""请分析以下SQL语句的血缘关系：

```sql
{sample_sql}
```

{task_description}

相关表结构如下：{tables_info}

{views_info if views_info else ''}

{output_format}
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
    from pglumilineage.common.config import settings
    from openai import AsyncOpenAI
    
    try:
        # 获取配置信息
        api_key = settings.DASHSCOPE_API_KEY.get_secret_value()
        base_url = str(settings.QWEN_BASE_URL)
        default_model = settings.QWEN_MODEL_NAME
        
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
        
        # 构建 UPDATE SQL 语句
        query = """
        UPDATE lumi_analytics.sql_patterns
        SET 
            llm_analysis_status = $1,
            llm_extracted_relations_json = $2,
            last_llm_analysis_at = CURRENT_TIMESTAMP
        WHERE 
            sql_hash = $3
        """
        
        async with pool.acquire() as conn:
            # 执行 UPDATE 操作
            await conn.execute(
                query,
                status,
                json.dumps(relations_json) if relations_json else None,
                sql_hash
            )
            
            logger.info(f"成功更新 SQL 模式 {sql_hash[:8]}... 的分析结果，状态: {status}")
            
    except Exception as e:
        logger.error(f"更新 SQL 模式 {sql_hash[:8]}... 的分析结果失败: {str(e)}")

async def analyze_sql_patterns_with_llm(batch_size: int = 10):
    """
    使用 LLM 分析 SQL 模式
    
    主函数，协调整个分析流程
    
    Args:
        batch_size: 每批处理的 SQL 模式数量
    """
    logger.info(f"开始使用 LLM 分析 SQL 模式，批大小: {batch_size}")
    
    try:
        # 1. 获取待分析的 SQL 模式
        patterns = await fetch_pending_sql_patterns(batch_size)
        
        if not patterns:
            logger.info("没有找到待分析的 SQL 模式")
            return
        
        # 获取数据库连接池
        pool = await db_utils.get_db_pool()
        
        # 2. 逐个处理 SQL 模式
        for pattern in patterns:
            try:
                # 获取 SQL 模式的元数据上下文
                async with pool.acquire() as conn:
                    metadata_context = await fetch_metadata_context_for_sql(conn, pattern)
                
                # 确定 SQL 模式类型
                sql_mode = "UNKNOWN"
                if pattern.normalized_sql_text.lower().startswith("insert"):
                    sql_mode = "INSERT"
                elif pattern.normalized_sql_text.lower().startswith("update"):
                    sql_mode = "UPDATE"
                elif pattern.normalized_sql_text.lower().startswith("select"):
                    sql_mode = "SELECT"
                elif pattern.normalized_sql_text.lower().startswith("create"):
                    sql_mode = "CREATE"
                
                # 构造 Qwen prompt
                messages = construct_prompt_for_qwen(
                    sql_mode=sql_mode,
                    sample_sql=pattern.sample_raw_sql_text,
                    metadata_context=metadata_context
                )
                
                # 调用 Qwen API
                response_content = await call_qwen_api(messages)
                
                if not response_content:
                    # 更新分析状态为失败
                    await update_sql_pattern_analysis_result(
                        sql_hash=pattern.sql_hash,
                        status="FAILED",
                        relations_json=None,
                        error_message="LLM API 返回空响应"
                    )
                    continue
                
                # 解析 LLM 响应
                relations_json = parse_llm_response(response_content)
                
                if relations_json:
                    # 更新分析状态为成功
                    await update_sql_pattern_analysis_result(
                        sql_hash=pattern.sql_hash,
                        status="SUCCESS",
                        relations_json=relations_json
                    )
                else:
                    # 更新分析状态为失败
                    await update_sql_pattern_analysis_result(
                        sql_hash=pattern.sql_hash,
                        status="FAILED",
                        relations_json=None,
                        error_message="无法解析 LLM 响应"
                    )
                
            except Exception as e:
                logger.error(f"处理 SQL 模式 {pattern.sql_hash[:8]}... 时出错: {str(e)}")
                # 更新分析状态为失败
                await update_sql_pattern_analysis_result(
                    sql_hash=pattern.sql_hash,
                    status="FAILED",
                    relations_json=None,
                    error_message=str(e)
                )
        
        logger.info(f"完成 {len(patterns)} 条 SQL 模式的 LLM 分析")
        
    except Exception as e:
        logger.error(f"LLM 分析 SQL 模式过程中出现未知错误: {str(e)}")

async def main():
    """
    主函数
    """
    # 设置日志
    setup_logging()
    
    # 分析 SQL 模式
    await analyze_sql_patterns_with_llm()

if __name__ == "__main__":
    asyncio.run(main())
