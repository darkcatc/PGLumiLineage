#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
血缘关系服务层

此模块负责处理血缘关系的业务逻辑。

作者: Vance Chen
"""

from typing import List, Dict, Any, Optional
import logging
from .models import GraphResponse, ObjectDetailsResponse, PathResponse, NodeType, Node, Edge, EdgeType
from .repository import LineageRepository

# 设置日志
logger = logging.getLogger(__name__)


class LineageService:
    """血缘关系服务"""
    
    def __init__(self):
        """初始化服务"""
        self.repository = LineageRepository()
    
    async def get_lineage_subgraph(self, root_node_type: NodeType, root_node_fqn: str, depth: int) -> GraphResponse:
        """
        获取以某节点为中心的N层深度血缘子图
        
        Args:
            root_node_type: 根节点类型
            root_node_fqn: 根节点全限定名
            depth: 查询深度
            
        Returns:
            GraphResponse: 子图数据
        """
        logger.info(f"获取血缘子图: 节点类型={root_node_type}, FQN={root_node_fqn}, 深度={depth}")
        
        try:
            graph_data = await self.repository.query_subgraph(root_node_type, root_node_fqn, depth)
            return self._format_graph_response(graph_data)
        except Exception as e:
            logger.error(f"获取血缘子图失败: {str(e)}")
            raise
    
    async def get_object_details(self, node_type: NodeType, node_fqn: str, include_related: bool) -> ObjectDetailsResponse:
        """
        获取某对象的详细信息
        
        Args:
            node_type: 节点类型
            node_fqn: 节点全限定名
            include_related: 是否包含相关对象
            
        Returns:
            ObjectDetailsResponse: 对象详情
        """
        logger.info(f"获取对象详情: 节点类型={node_type}, FQN={node_fqn}, 包含相关对象={include_related}")
        
        try:
            node_data = await self.repository.query_node_details(node_type, node_fqn)
            
            response = ObjectDetailsResponse(
                node=Node(
                    id=node_data["id"],
                    type=NodeType(node_data["type"]),
                    label=node_data["label"],
                    fqn=node_data.get("fqn"),
                    properties=node_data.get("properties", {})
                )
            )
            
            if include_related:
                related_data = await self.repository.query_direct_neighbors(node_type, node_fqn)
                response.related_objects = self._format_graph_response(related_data)
            
            return response
        except Exception as e:
            logger.error(f"获取对象详情失败: {str(e)}")
            raise
    
    async def find_paths(self, source_node_fqn: str, target_node_fqn: str, max_depth: int) -> PathResponse:
        """
        查找两点间路径
        
        Args:
            source_node_fqn: 源节点全限定名
            target_node_fqn: 目标节点全限定名
            max_depth: 最大查询深度
            
        Returns:
            PathResponse: 路径数据
        """
        logger.info(f"查找路径: 源节点={source_node_fqn}, 目标节点={target_node_fqn}, 最大深度={max_depth}")
        
        try:
            paths_data = await self.repository.query_paths(source_node_fqn, target_node_fqn, max_depth)
            
            paths = []
            for path_data in paths_data:
                paths.append(self._format_graph_response(path_data))
            
            return PathResponse(paths=paths)
        except Exception as e:
            logger.error(f"查找路径失败: {str(e)}")
            raise
    
    def _format_graph_response(self, graph_data: Dict[str, Any]) -> GraphResponse:
        """
        格式化图响应数据
        
        Args:
            graph_data: 图数据
            
        Returns:
            GraphResponse: 格式化后的图响应
        """
        nodes = []
        for node_data in graph_data["nodes"]:
            try:
                node_type = NodeType(node_data["type"])
            except ValueError:
                # 如果节点类型不在枚举中，使用默认值
                logger.warning(f"未知节点类型: {node_data['type']}, 使用TABLE作为默认值")
                node_type = NodeType.TABLE
            
            nodes.append(Node(
                id=node_data["id"],
                type=node_type,
                label=node_data["label"],
                fqn=node_data.get("fqn"),
                properties=node_data.get("properties", {})
            ))
        
        edges = []
        for edge_data in graph_data["edges"]:
            try:
                edge_type = EdgeType(edge_data["type"])
            except ValueError:
                # 如果边类型不在枚举中，使用默认值
                logger.warning(f"未知边类型: {edge_data['type']}, 使用DEPENDS_ON作为默认值")
                edge_type = EdgeType.DEPENDS_ON
            
            edges.append(Edge(
                id=edge_data["id"],
                source=edge_data["source"],
                target=edge_data["target"],
                type=edge_type,
                label=edge_data["label"],
                properties=edge_data.get("properties", {})
            ))
        
        return GraphResponse(nodes=nodes, edges=edges)
