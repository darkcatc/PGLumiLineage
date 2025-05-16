#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
血缘关系数据访问层

此模块负责与Apache AGE数据库交互，执行Cypher查询。

作者: Vance Chen
"""

from typing import List, Dict, Any, Optional, Union
import asyncpg
import json
import re
import logging
from .models import NodeType

# 设置日志
logger = logging.getLogger(__name__)


class LineageRepository:
    """血缘关系数据访问层"""
    
    def __init__(self):
        """初始化数据访问层"""
        self.graph_name = "pglumilineage_graph"
    
    async def _get_connection(self) -> asyncpg.Connection:
        """
        获取数据库连接
        
        Returns:
            asyncpg.Connection: 数据库连接对象
        """
        from pglumilineage.config import get_settings
        
        settings = get_settings()
        conn = await asyncpg.connect(
            user=settings.database.user,
            password=settings.database.password,
            host=settings.database.host,
            port=settings.database.port,
            database=settings.database.name
        )
        
        # 设置AGE搜索路径
        await conn.execute("SET search_path = ag_catalog, public;")
        
        return conn
    
    async def _execute_cypher(self, cypher_query: str, params: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        执行Cypher查询
        
        Args:
            cypher_query: Cypher查询语句
            params: 查询参数
            
        Returns:
            List[Dict[str, Any]]: 查询结果
        """
        conn = await self._get_connection()
        try:
            logger.debug(f"执行Cypher查询: {cypher_query}")
            logger.debug(f"参数: {params}")
            
            # 使用AGE的cypher函数执行查询
            result = await conn.fetch(
                f"SELECT * FROM cypher('{self.graph_name}', $1, $2) as (result jsonb);",
                cypher_query,
                json.dumps(params) if params else '{}'
            )
            
            # 解析结果
            parsed_result = [json.loads(row['result']) for row in result]
            logger.debug(f"查询结果: {parsed_result}")
            
            return parsed_result
        except Exception as e:
            logger.error(f"执行Cypher查询失败: {str(e)}")
            raise
        finally:
            await conn.close()
    
    async def query_subgraph(self, root_node_type: NodeType, root_node_fqn: str, depth: int) -> Dict[str, Any]:
        """
        查询以某节点为中心的N层深度子图
        
        Args:
            root_node_type: 根节点类型
            root_node_fqn: 根节点全限定名
            depth: 查询深度
            
        Returns:
            Dict[str, Any]: 子图数据
        """
        # 为AGE 1.5.0适配的Cypher查询
        cypher_query = f"""
        MATCH (root {{fqn: $fqn, label: $node_type}})
        CALL {{
            WITH root
            MATCH path = (root)-[*1..{depth}]-(related)
            RETURN path
        }}
        WITH COLLECT(path) AS paths
        RETURN paths
        """
        
        params = {
            "fqn": root_node_fqn,
            "node_type": root_node_type.value
        }
        
        result = await self._execute_cypher(cypher_query, params)
        
        # 处理结果并转换为前端需要的格式
        return self._process_paths_result(result)
    
    async def query_node_details(self, node_type: NodeType, node_fqn: str) -> Dict[str, Any]:
        """
        查询节点详细信息
        
        Args:
            node_type: 节点类型
            node_fqn: 节点全限定名
            
        Returns:
            Dict[str, Any]: 节点详细信息
        """
        cypher_query = """
        MATCH (n {fqn: $fqn, label: $node_type})
        RETURN n
        """
        
        params = {
            "fqn": node_fqn,
            "node_type": node_type.value
        }
        
        result = await self._execute_cypher(cypher_query, params)
        
        # 处理结果
        if not result:
            raise ValueError(f"未找到节点: {node_fqn}")
        
        return self._process_node_result(result[0]['n'])
    
    async def query_direct_neighbors(self, node_type: NodeType, node_fqn: str) -> Dict[str, Any]:
        """
        查询节点的直接邻居
        
        Args:
            node_type: 节点类型
            node_fqn: 节点全限定名
            
        Returns:
            Dict[str, Any]: 邻居节点数据
        """
        cypher_query = """
        MATCH (n {fqn: $fqn, label: $node_type})-[r]-(neighbor)
        RETURN n, r, neighbor
        """
        
        params = {
            "fqn": node_fqn,
            "node_type": node_type.value
        }
        
        result = await self._execute_cypher(cypher_query, params)
        
        # 处理结果
        return self._process_neighbors_result(result)
    
    async def query_paths(self, source_node_fqn: str, target_node_fqn: str, max_depth: int) -> List[Dict[str, Any]]:
        """
        查询两点间路径
        
        Args:
            source_node_fqn: 源节点全限定名
            target_node_fqn: 目标节点全限定名
            max_depth: 最大查询深度
            
        Returns:
            List[Dict[str, Any]]: 路径数据列表
        """
        cypher_query = f"""
        MATCH (source {{fqn: $source_fqn}}), (target {{fqn: $target_fqn}}),
        p = (source)-[*1..{max_depth}]-(target)
        RETURN p
        LIMIT 10
        """
        
        params = {
            "source_fqn": source_node_fqn,
            "target_fqn": target_node_fqn
        }
        
        result = await self._execute_cypher(cypher_query, params)
        
        # 处理结果
        return [self._process_path_result(path['p']) for path in result]
    
    def _process_paths_result(self, result: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        处理路径查询结果
        
        Args:
            result: 查询结果
            
        Returns:
            Dict[str, Any]: 处理后的图数据
        """
        if not result or not result[0].get('paths'):
            return {"nodes": [], "edges": []}
        
        nodes_map = {}  # 用于去重节点
        edges_map = {}  # 用于去重边
        
        for path in result[0]['paths']:
            # 处理路径中的节点
            for node in path.get('nodes', []):
                node_id = node.get('id')
                if node_id and node_id not in nodes_map:
                    nodes_map[node_id] = self._extract_node_data(node)
            
            # 处理路径中的边
            for edge in path.get('relationships', []):
                edge_id = edge.get('id')
                if edge_id and edge_id not in edges_map:
                    edges_map[edge_id] = self._extract_edge_data(edge)
        
        return {
            "nodes": list(nodes_map.values()),
            "edges": list(edges_map.values())
        }
    
    def _process_node_result(self, node_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理节点查询结果
        
        Args:
            node_data: 节点数据
            
        Returns:
            Dict[str, Any]: 处理后的节点数据
        """
        return self._extract_node_data(node_data)
    
    def _process_neighbors_result(self, result: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        处理邻居查询结果
        
        Args:
            result: 查询结果
            
        Returns:
            Dict[str, Any]: 处理后的图数据
        """
        nodes_map = {}  # 用于去重节点
        edges_map = {}  # 用于去重边
        
        for item in result:
            # 处理中心节点
            center_node = item.get('n')
            if center_node:
                node_id = center_node.get('id')
                if node_id and node_id not in nodes_map:
                    nodes_map[node_id] = self._extract_node_data(center_node)
            
            # 处理邻居节点
            neighbor = item.get('neighbor')
            if neighbor:
                node_id = neighbor.get('id')
                if node_id and node_id not in nodes_map:
                    nodes_map[node_id] = self._extract_node_data(neighbor)
            
            # 处理关系
            relationship = item.get('r')
            if relationship:
                edge_id = relationship.get('id')
                if edge_id and edge_id not in edges_map:
                    edges_map[edge_id] = self._extract_edge_data(relationship)
        
        return {
            "nodes": list(nodes_map.values()),
            "edges": list(edges_map.values())
        }
    
    def _process_path_result(self, path_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        处理单个路径查询结果
        
        Args:
            path_data: 路径数据
            
        Returns:
            Dict[str, Any]: 处理后的图数据
        """
        nodes = []
        edges = []
        
        # 处理路径中的节点
        for node in path_data.get('nodes', []):
            nodes.append(self._extract_node_data(node))
        
        # 处理路径中的边
        for edge in path_data.get('relationships', []):
            edges.append(self._extract_edge_data(edge))
        
        return {
            "nodes": nodes,
            "edges": edges
        }
    
    def _extract_node_data(self, node: Dict[str, Any]) -> Dict[str, Any]:
        """
        提取节点数据
        
        Args:
            node: 原始节点数据
            
        Returns:
            Dict[str, Any]: 处理后的节点数据
        """
        properties = node.get('properties', {})
        node_type = properties.get('label', '').lower()
        
        return {
            "id": str(node.get('id', '')),
            "type": node_type,
            "label": properties.get('name', ''),
            "fqn": properties.get('fqn', ''),
            "properties": properties
        }
    
    def _extract_edge_data(self, edge: Dict[str, Any]) -> Dict[str, Any]:
        """
        提取边数据
        
        Args:
            edge: 原始边数据
            
        Returns:
            Dict[str, Any]: 处理后的边数据
        """
        properties = edge.get('properties', {})
        edge_type = properties.get('label', '').lower()
        
        return {
            "id": str(edge.get('id', '')),
            "source": str(edge.get('start', '')),
            "target": str(edge.get('end', '')),
            "type": edge_type,
            "label": properties.get('label', ''),
            "properties": properties
        }
