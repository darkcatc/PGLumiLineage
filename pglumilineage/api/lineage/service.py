#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
血缘关系服务层

此模块负责处理血缘关系的业务逻辑。

作者: Vance Chen
"""

from typing import List, Dict, Any, Optional
import logging
import json
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
            
            # 从properties中获取节点信息
            properties = node_data.get("properties", {})
            node_label = properties.get("label", "")
            
            # 根据label确定节点类型
            if node_label == "table":
                actual_node_type = NodeType.TABLE
            elif node_label == "view":
                actual_node_type = NodeType.VIEW
            elif node_label == "column":
                actual_node_type = NodeType.COLUMN
            elif node_label == "schema":
                actual_node_type = NodeType.SCHEMA
            elif node_label == "database":
                actual_node_type = NodeType.DATABASE
            elif node_label == "sqlpattern":
                actual_node_type = NodeType.SQL_PATTERN
            else:
                # 使用传入的节点类型作为默认值
                actual_node_type = node_type
            
            # 构建FQN
            fqn = None
            if "fqn" in properties:
                fqn = properties["fqn"]
            elif all(k in properties for k in ["name", "schema_name", "database_name"]):
                fqn = f"{properties['database_name']}.{properties['schema_name']}.{properties['name']}"
            
            response = ObjectDetailsResponse(
                node=Node(
                    id=str(node_data["id"]),
                    type=actual_node_type,
                    label=properties.get("name", node_label),
                    fqn=fqn,
                    properties=properties
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
        logger.info(f"待格式化的节点数据类型: {type(graph_data['nodes'])}, 数量: {len(graph_data['nodes'])}")
        
        for node_data in graph_data["nodes"]:
            try:
                logger.info(f"处理节点数据: {node_data}, 类型: {type(node_data)}")
                
                # 如果节点数据是字符串，尝试解析为JSON
                if isinstance(node_data, str):
                    try:
                        # 如果是AGE的vertex类型字符串，需要先处理
                        if "::vertex" in node_data:
                            node_data = node_data.replace("::vertex", "")
                        node_data = json.loads(node_data)
                    except json.JSONDecodeError as e:
                        logger.error(f"解析节点JSON数据失败: {e}, 数据: {node_data}")
                        continue
                
                # 从AGE节点中提取数据
                node_id = str(node_data.get("id", ""))
                
                # 从AGE节点数据中获取节点类型
                properties = node_data.get("properties", {})
                # AGE中的label字段在节点的根级别，不在properties中
                node_label = node_data.get("label", "")
                
                # 根据label确定节点类型
                if node_label == "table":
                    node_type = NodeType.TABLE
                elif node_label == "view":
                    node_type = NodeType.VIEW
                elif node_label == "column":
                    node_type = NodeType.COLUMN
                elif node_label == "schema":
                    node_type = NodeType.SCHEMA
                elif node_label == "database":
                    node_type = NodeType.DATABASE
                elif node_label == "sqlpattern":
                    node_type = NodeType.SQL_PATTERN
                else:
                    # 默认为表
                    logger.warning(f"未知节点类型: {node_label}, 使用TABLE作为默认值")
                    node_type = NodeType.TABLE
                
                # 构建FQN
                fqn = None
                if "fqn" in properties:
                    fqn = properties["fqn"]
                elif all(k in properties for k in ["name", "schema_name", "database_name"]):
                    fqn = f"{properties['database_name']}.{properties['schema_name']}.{properties['name']}"
                
                # 创建节点对象
                nodes.append(Node(
                    id=node_id,
                    type=node_type,
                    label=properties.get("name", node_label),
                    fqn=fqn,
                    properties=properties
                ))
            except Exception as e:
                logger.error(f"处理节点数据时发生错误: {e}, 节点数据: {node_data}")
                continue
        
        edges = []
        logger.info(f"待格式化的关系数据类型: {type(graph_data.get('relationships', []))}, 数量: {len(graph_data.get('relationships', []))}")
        
        for edge_data in graph_data.get("relationships", []):
            try:
                logger.info(f"处理关系数据: {edge_data}, 类型: {type(edge_data)}")
                
                # 如果关系数据是字符串，尝试解析为JSON
                if isinstance(edge_data, str):
                    try:
                        # 如果是AGE的edge类型字符串，需要先处理
                        if "::edge" in edge_data:
                            edge_data = edge_data.replace("::edge", "")
                        edge_data = json.loads(edge_data)
                    except json.JSONDecodeError as e:
                        logger.error(f"解析关系JSON数据失败: {e}, 数据: {edge_data}")
                        continue
                
                # 从AGE关系中提取数据
                edge_id = str(edge_data.get("id", ""))
                start_id = str(edge_data.get("start_id", ""))
                end_id = str(edge_data.get("end_id", ""))
                
                # 从properties中获取关系类型
                properties = edge_data.get("properties", {})
                edge_label = edge_data.get("label", "").lower()
                
                # 根据关系类型确定边类型
                if edge_label == "has_schema" or edge_label == "has_object" or edge_label == "has_column":
                    edge_type = EdgeType.CONTAINS
                elif edge_label == "reads_from":
                    edge_type = EdgeType.READS
                elif edge_label == "writes_to":
                    edge_type = EdgeType.WRITES
                elif edge_label == "data_flow":
                    edge_type = EdgeType.DATA_FLOW
                elif edge_label == "generates":
                    edge_type = EdgeType.GENERATES_FLOW
                else:
                    # 默认为依赖关系
                    logger.warning(f"未知关系类型: {edge_label}, 使用DEPENDS_ON作为默认值")
                    edge_type = EdgeType.DEPENDS_ON
                
                # 创建边对象
                edges.append(Edge(
                    id=edge_id,
                    source=start_id,
                    target=end_id,
                    type=edge_type,
                    label=edge_data.get("label", ""),
                    properties=edge_data.get("properties", {})
                ))
            except Exception as e:
                logger.error(f"处理关系数据时发生错误: {e}, 关系数据: {edge_data}")
                continue
        
        return GraphResponse(nodes=nodes, edges=edges)
