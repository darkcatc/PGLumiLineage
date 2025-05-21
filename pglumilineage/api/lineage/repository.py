#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import logging
import os
import re
import subprocess
import tempfile
from typing import Any, Dict, List, Optional, Tuple

import asyncpg

from pglumilineage.age_graph_builder.service import convert_cypher_for_age
from .models import NodeType

# 设置日志
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class LineageRepository:
    """血缘关系存储库，负责与图数据库交互。"""

    def __init__(self, db_config: Dict[str, Any] = None):
        """
        初始化存储库。

        Args:
            db_config: 数据库配置
        """
        self.db_config = db_config or {
            "user": os.environ.get("DB_USER", "lumiadmin"),
            "password": os.environ.get("DB_PASSWORD", "lumiadmin"),
            "host": os.environ.get("DB_HOST", "localhost"),
            "port": int(os.environ.get("DB_PORT", 5432)),
            "database": os.environ.get("DB_NAME", "iwdb")
        }
        self.graph_name = os.environ.get("GRAPH_NAME", "pglumilineage_graph")

    def _parse_age_vertex(self, vertex_str: str) -> Dict[str, Any]:
        """
        解析AGE返回的节点数据
        
        Args:
            vertex_str: AGE返回的节点字符串
            
        Returns:
            Dict[str, Any]: 解析后的节点数据
        """
        try:
            # 如果是AGE的vertex类型字符串，需要先处理
            if isinstance(vertex_str, str) and "::vertex" in vertex_str:
                vertex_str = vertex_str.replace("::vertex", "")
            
            # 尝试解析JSON
            return json.loads(vertex_str)
        except json.JSONDecodeError as e:
            logger.error(f"解析节点数据失败: {e}, 数据: {vertex_str}")
            return None
    
    def _parse_age_edge(self, edge_str: str) -> Dict[str, Any]:
        """
        解析AGE返回的关系数据
        
        Args:
            edge_str: AGE返回的关系字符串
            
        Returns:
            Dict[str, Any]: 解析后的关系数据
        """
        try:
            # 如果是AGE的edge类型字符串，需要先处理
            if isinstance(edge_str, str) and "::edge" in edge_str:
                edge_str = edge_str.replace("::edge", "")
            
            # 尝试解析JSON
            return json.loads(edge_str)
        except json.JSONDecodeError as e:
            logger.error(f"解析关系数据失败: {e}, 数据: {edge_str}")
            return None

    async def _execute_cypher_core_async(self, cypher_query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        执行Cypher查询的核心方法 (异步)

        Args:
            cypher_query: Cypher查询语句
            params: 查询参数

        Returns:
            List[Dict[str, Any]]: 查询结果
        """
        # 创建连接
        conn = await asyncpg.connect(**self.db_config)
        
        try:
            # 设置搜索路径
            await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
            
            # 插值参数
            interpolated_query = self._interpolate_params(cypher_query, params)
            
            # 构建SQL查询
            cypher_sql = f"SELECT * FROM cypher('{self.graph_name}', $${interpolated_query}$$) as (result agtype);"
            
            try:
                # 执行查询
                result = await conn.fetch(cypher_sql)
            except Exception as e:
                logger.error(f"执行 SQL 查询时发生错误: {e}\n查询: {cypher_sql!r}")
                raise
            
            # 处理结果
            processed_result = []
            for row in result:
                row_dict = {}
                for column_name, agtype_value in row.items():
                    if agtype_value is None:
                        row_dict[column_name] = None
                    else:
                        row_dict[column_name] = agtype_value
                processed_result.append(row_dict)
            
            return processed_result
        finally:
            # 关闭连接
            await conn.close()

    def _interpolate_params(self, query: str, params: Dict[str, Any] = None) -> str:
        """
        将参数插值到查询字符串中

        Args:
            query: 查询字符串
            params: 参数字典

        Returns:
            str: 插值后的查询字符串
        """
        if not params:
            return query
        
        interpolated_query = query
        
        for key, value in params.items():
            param_placeholder = "${" + key + "}"
            
            if param_placeholder not in interpolated_query:
                continue
            
            if value is None:
                replacement = "NULL"
            elif isinstance(value, bool):
                replacement = str(value).lower()
            elif isinstance(value, (int, float)):
                replacement = str(value)
            elif isinstance(value, str):
                # 如果是字符串，添加引号并转义引号
                replacement = f"'{value.replace('\'', '\\\'')}'" 
            elif isinstance(value, (list, tuple)):
                # 如果是列表或元组，转换为 Cypher 数组
                items = []
                for item in value:
                    if isinstance(item, str):
                        items.append(f"'{item.replace('\'', '\\\'')}'")
                    elif isinstance(item, (int, float)):
                        items.append(str(item))
                    elif item is None:
                        items.append("NULL")
                    else:
                        items.append(str(item))
                replacement = f"[{', '.join(items)}]"
            elif isinstance(value, dict):
                # 如果是字典，转换为 Cypher 对象
                properties = []
                for k, v in value.items():
                    if isinstance(v, str):
                        properties.append(f"{k}: '{v.replace('\'', '\\\'')}'")
                    elif isinstance(v, (int, float)):
                        properties.append(f"{k}: {v}")
                    elif v is None:
                        properties.append(f"{k}: NULL")
                    elif isinstance(v, bool):
                        properties.append(f"{k}: {str(v).lower()}")
                    else:
                        properties.append(f"{k}: '{str(v)}'")
                replacement = f"{{{', '.join(properties)}}}"
            else:
                # 其他类型，转换为字符串
                replacement = f"'{str(value)}'"
            
            # 替换查询中的占位符
            interpolated_query = interpolated_query.replace(param_placeholder, replacement)
        
        return interpolated_query

    def _execute_cypher(self, cypher_query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        执行Cypher查询 (同步包装器)

        Args:
            cypher_query: Cypher查询语句
            params: 查询参数

        Returns:
            List[Dict[str, Any]]: 查询结果
        """
        try:
            # 使用asyncio.run调用异步核心方法
            return asyncio.run(self._execute_cypher_core_async(cypher_query, params))
        except RuntimeError as e:
            if "cannot be called from a running event loop" in str(e).lower():
                logger.warning("尝试在已运行的事件循环中调用 asyncio.run()。尝试使用当前循环。")
                # 如果在已运行的事件循环中，尝试使用当前循环
                try:
                    loop = asyncio.get_event_loop()
                    return loop.run_until_complete(self._execute_cypher_core_async(cypher_query, params))
                except Exception as inner_e:
                    logger.error(f"使用当前事件循环执行异步查询时发生错误: {inner_e}")
                    raise Exception(f"执行Cypher查询失败 (事件循环错误): {inner_e}")
            elif "There is no current event loop in thread" in str(e).lower():
                logger.info("当前线程没有事件循环，尝试创建一个新的事件循环")
                # 创建新的事件循环
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(self._execute_cypher_core_async(cypher_query, params))
                finally:
                    loop.close()
            else:
                logger.error(f"运行 _execute_cypher_core_async 时发生 RuntimeError: {e}")
                raise Exception(f"执行Cypher查询失败 (RuntimeError): {e}")
        except Exception as e:
            logger.error(f"执行Cypher查询 '{cypher_query}' 时发生错误: {e}")
            raise Exception(f"执行Cypher查询失败: {e}")

    async def query_subgraph(self, root_node_type: NodeType, root_node_fqn: str, depth: int = 1,
                        relationship_types: List[str] = None) -> Dict[str, Any]:
        """
        查询子图。

        Args:
            root_node_type: 根节点类型
            root_node_fqn: 根节点的全限定名称
            depth: 查询的深度
            relationship_types: 关系类型列表

        Returns:
            子图数据，包含节点和关系
        """
        logger.info(f"查询子图: root_node_type={root_node_type}, root_node_fqn={root_node_fqn}, depth={depth}")
        
        try:
            # 解析完全限定名
            parts = root_node_fqn.split('.')
            if len(parts) >= 3:
                database_name = parts[0]
                schema_name = parts[1]
                object_name = parts[2]
            else:
                object_name = root_node_fqn.split(".")[-1]
                database_name = ""
                schema_name = ""
            
            logger.debug(f"解析FQN: database_name={database_name}, schema_name={schema_name}, object_name={object_name}")
            
            # 使用直接SQL查询获取节点
            conn = await asyncpg.connect(**self.db_config)
            
            # 设置搜索路径
            await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
            
            # 使用正确的 AGE 1.5.0 Cypher 查询语法
            # 在 AGE 中，第一层定义的 properties 是不用写的，直接使用 n.name 而不是 n.properties.name
            check_node_query = f"""
            SELECT * FROM cypher('{self.graph_name}', $$ 
                MATCH (n)
                WHERE n.label = '{root_node_type.value}' AND 
                      n.name = '{object_name}'
                {f"AND n.schema_name = '{schema_name}'" if len(parts) >= 3 else ""}
                {f"AND n.database_name = '{database_name}'" if len(parts) >= 3 else ""}
                RETURN n
                LIMIT 1
            $$) as (n agtype);
            """
            
            logger.debug(f"检查节点存在性查询: {check_node_query}")
            
            check_result = await conn.fetch(check_node_query)
            
            if not check_result or len(check_result) == 0:
                logger.warning(f"未找到节点: type={root_node_type.value}, name={object_name}, schema={schema_name if len(parts) >= 3 else 'N/A'}, db={database_name if len(parts) >= 3 else 'N/A'}")
                await conn.close()
                return {"nodes": [], "relationships": []}
            
            logger.info(f"找到节点: {check_result[0]['n']}")
            
            # 添加根节点
            root_node = self._parse_age_vertex(check_result[0]['n'])
            root_node_id = root_node['id']
            nodes = [root_node]
            
            logger.debug(f"根节点ID: {root_node_id}")
            
            # 查询与根节点直接相连的关系，使用正确的 AGE 1.5.0 Cypher 查询语法
            rel_query = f"""
            SELECT * FROM cypher('{self.graph_name}', $$ 
                MATCH (n)-[r]->(m) 
                WHERE id(n) = {root_node_id} OR id(m) = {root_node_id}
                RETURN r
            $$) as (r agtype);
            """
            
            logger.debug(f"关系查询SQL: {rel_query}")
            rel_result = await conn.fetch(rel_query)
            logger.debug(f"关系查询结果行数: {len(rel_result)}")
            
            # 解析关系并收集相关节点ID
            related_node_ids = set()
            relationships = []
            
            for row in rel_result:
                rel_str = row['r']
                logger.debug(f"原始关系数据: {rel_str}")
                if rel_str:
                    rel = self._parse_age_edge(rel_str)
                    if rel and 'id' in rel:
                        logger.debug(f"解析后的关系数据: {rel}")
                        
                        # 收集关系中的节点ID
                        start_id = rel.get('start_id')
                        end_id = rel.get('end_id')
                        
                        if start_id and start_id != root_node_id:
                            related_node_ids.add(start_id)
                        if end_id and end_id != root_node_id:
                            related_node_ids.add(end_id)
                        
                        # 将关系添加到结果中
                        relationships.append(rel)
            
            # 查询相关节点
            if related_node_ids:
                related_ids_str = ", ".join([str(id) for id in related_node_ids])
                node_query = f"""
                SELECT * FROM cypher('{self.graph_name}', $$ 
                    MATCH (n) 
                    WHERE id(n) IN [{related_ids_str}]
                    RETURN n
                $$) as (n agtype);
                """
                
                logger.debug(f"相关节点查询SQL: {node_query}")
                node_result = await conn.fetch(node_query)
                logger.debug(f"相关节点查询结果行数: {len(node_result)}")
                
                # 解析相关节点
                for row in node_result:
                    node_str = row['n']
                    logger.debug(f"原始节点数据: {node_str}")
                    if node_str:
                        node = self._parse_age_vertex(node_str)
                        if node and 'id' in node:
                            logger.debug(f"解析后的节点数据: {node}")
                            nodes.append(node)
            
            await conn.close()
            
            logger.info(f"查询子图完成: 找到 {len(nodes)} 个节点和 {len(relationships)} 个关系")
            return {
                "nodes": nodes,
                "relationships": relationships
            }
        except Exception as e:
            logger.error(f"查询子图时发生错误: {e}")
            import traceback
            logger.error(traceback.format_exc())
            # 返回空结果
            return {
                "nodes": [],
                "relationships": []
            }

    def query_node_details(self, node_fqn: str) -> Dict[str, Any]:
        """
        查询节点详细信息。

        Args:
            node_fqn: 节点的完全限定名

        Returns:
            Dict[str, Any]: 节点详细信息
        """
        return None

    def query_direct_neighbors(self, node_fqn: str, direction: str = "both",
                          relationship_types: List[str] = None) -> Dict[str, Any]:
        """
        查询节点的直接邻居。

        Args:
            node_fqn: 节点的完全限定名
            direction: 方向，可选值为 "in", "out", "both"
            relationship_types: 关系类型过滤

        Returns:
            Dict[str, Any]: 邻居节点和关系
        """
        return {
            "neighbors": [],
            "relationships": []
        }
