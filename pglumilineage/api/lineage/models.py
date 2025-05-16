#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
血缘关系API模型

此模块定义了血缘关系API的请求和响应模型。

作者: Vance Chen
"""

from typing import List, Dict, Any, Optional, Union
from enum import Enum
from pydantic import BaseModel, Field


class NodeType(str, Enum):
    """节点类型枚举"""
    DATABASE = "database"
    SCHEMA = "schema"
    TABLE = "table"
    VIEW = "view"
    COLUMN = "column"
    SQL_PATTERN = "sql_pattern"
    FUNCTION = "function"


class Node(BaseModel):
    """图节点模型"""
    id: str
    type: NodeType
    label: str
    fqn: Optional[str] = None
    properties: Dict[str, Any] = Field(default_factory=dict)


class EdgeType(str, Enum):
    """边类型枚举"""
    CONTAINS = "contains"
    REFERENCES = "references"
    DEPENDS_ON = "depends_on"
    DATA_FLOW = "data_flow"
    GENERATES_FLOW = "generates_flow"
    WRITES = "writes"
    READS = "reads"


class Edge(BaseModel):
    """图边模型"""
    id: str
    source: str
    target: str
    type: EdgeType
    label: str
    properties: Dict[str, Any] = Field(default_factory=dict)


class GraphResponse(BaseModel):
    """图响应模型"""
    nodes: List[Node]
    edges: List[Edge]


class ObjectDetailsResponse(BaseModel):
    """对象详情响应模型"""
    node: Node
    related_objects: Optional[GraphResponse] = None


class PathResponse(BaseModel):
    """路径响应模型"""
    paths: List[GraphResponse]
