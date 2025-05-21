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
        查询以指定节点为起点的子图。

        Args:
            root_node_type: 根节点类型
            root_node_fqn: 根节点的完全限定名
            depth: 最大深度
            relationship_types: 关系类型过滤

        Returns:
            Dict[str, Any]: 子图数据
        """
        try:
            logger.info(f"查询子图: root_node_type={root_node_type}, root_node_fqn={root_node_fqn}, depth={depth}")
            
            # 使用直接SQL查询获取节点
            conn = await asyncpg.connect(**self.db_config)
            
            # 设置搜索路径
            await conn.execute("SET search_path = ag_catalog, \"$user\", public;")
            
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
            
            # 查询节点
            query = f"""
            SELECT * FROM cypher('{self.graph_name}', $$ 
                MATCH (n)
                WHERE n.label = '{root_node_type.value}'
            """    
            
            if len(parts) >= 3:
                query += f"""
                  AND n.name = '{object_name}'
                  AND n.schema_name = '{schema_name}'
                  AND n.database_name = '{database_name}'
                """
            else:
                query += f"""
                  AND n.name = '{object_name}'
                """
            
            query += """
                RETURN n LIMIT 1
            $$) as (n agtype);
            """
            
            result = await conn.fetch(query)
            nodes = []
            node_ids = set()
            
            for row in result:
                node_str = row['n']
                if node_str:
                    node = self._parse_age_vertex(node_str)
                    if node and 'id' in node:
                        nodes.append(node)
                        node_ids.add(node['id'])
            
            # 如果找到了节点，查询其关系
            relationships = []
            if nodes and depth > 0:
                # 查询节点的关系
                for node in nodes:
                    if 'id' in node:
                        # 查询has_column关系
                        rel_query = f"""
                        SELECT * FROM cypher('{self.graph_name}', $$ 
                            MATCH (n)-[r]->(m)
                            WHERE id(n) = {node['id']}
                            RETURN r, m
                        $$) as (r agtype, m agtype);
                        """
                        
                        rel_result = await conn.fetch(rel_query)
                        
                        for rel_row in rel_result:
                            rel_str = rel_row['r']
                            related_node_str = rel_row['m']
                            
                            if rel_str and related_node_str:
                                rel = self._parse_age_edge(rel_str)
                                related_node = self._parse_age_vertex(related_node_str)
                                
                                if rel and related_node and 'id' in related_node:
                                    relationships.append(rel)
                                    
                                    # 添加相关节点，如果还没有添加过
                                    if related_node['id'] not in node_ids:
                                        nodes.append(related_node)
                                        node_ids.add(related_node['id'])
            
            await conn.close()
            
            return {
                "nodes": nodes,
                "relationships": relationships
            }
        except Exception as e:
            logger.error(f"查询子图时发生错误: {e}")
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
