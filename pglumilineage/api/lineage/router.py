#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
血缘关系API路由

此模块定义了血缘关系API的路由。

作者: Vance Chen
"""

from fastapi import APIRouter, Query, HTTPException, Depends, Path
from typing import Optional, List
import logging

from .models import GraphResponse, ObjectDetailsResponse, PathResponse, NodeType
from .service import LineageService

# 设置日志
logger = logging.getLogger(__name__)

# 创建路由
router = APIRouter(prefix="/api/lineage", tags=["lineage"])


@router.get("/graph", response_model=GraphResponse)
async def get_lineage_graph(
    root_node_type: NodeType,
    root_node_fqn: str,
    depth: int = Query(1, ge=1, le=5, description="查询深度，范围1-5"),
    service: LineageService = Depends()
):
    """
    获取以某节点为中心的N层深度血缘子图
    
    - **root_node_type**: 根节点类型
    - **root_node_fqn**: 根节点全限定名
    - **depth**: 查询深度，默认为1，最大为5
    
    返回包含节点和边的图结构。
    """
    try:
        return await service.get_lineage_subgraph(root_node_type, root_node_fqn, depth)
    except Exception as e:
        logger.error(f"获取血缘图失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取血缘图失败: {str(e)}")


@router.get("/object_details", response_model=ObjectDetailsResponse)
async def get_object_details(
    node_type: NodeType,
    node_fqn: str,
    include_related: bool = False,
    service: LineageService = Depends()
):
    """
    获取某对象的详细信息
    
    - **node_type**: 节点类型
    - **node_fqn**: 节点全限定名
    - **include_related**: 是否包含相关对象，默认为False
    
    返回对象的详细信息，如果include_related为True，还会返回相关对象。
    """
    try:
        return await service.get_object_details(node_type, node_fqn, include_related)
    except ValueError as e:
        logger.error(f"获取对象详情失败: {str(e)}")
        raise HTTPException(status_code=404, detail=f"未找到对象: {str(e)}")
    except Exception as e:
        logger.error(f"获取对象详情失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取对象详情失败: {str(e)}")


@router.get("/paths", response_model=PathResponse)
async def find_paths(
    source_node_fqn: str,
    target_node_fqn: str,
    max_depth: int = Query(5, ge=1, le=10, description="最大查询深度，范围1-10"),
    service: LineageService = Depends()
):
    """
    查找两点间路径
    
    - **source_node_fqn**: 源节点全限定名
    - **target_node_fqn**: 目标节点全限定名
    - **max_depth**: 最大查询深度，默认为5，最大为10
    
    返回两点间的路径列表。
    """
    try:
        return await service.find_paths(source_node_fqn, target_node_fqn, max_depth)
    except Exception as e:
        logger.error(f"查找路径失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"查找路径失败: {str(e)}")
