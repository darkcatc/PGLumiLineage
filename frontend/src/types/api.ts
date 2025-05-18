/**
 * API 类型定义
 * 
 * 此文件定义了与后端 API 交互的数据类型
 * 
 * 作者: Vance Chen
 */

// 节点类型枚举
export enum NodeType {
  DATABASE = 'database',
  SCHEMA = 'schema',
  TABLE = 'table',
  VIEW = 'view',
  COLUMN = 'column',
  SQL_PATTERN = 'sql_pattern',
  FUNCTION = 'function'
}

// 边类型枚举
export enum EdgeType {
  CONTAINS = 'contains',
  REFERENCES = 'references',
  DEPENDS_ON = 'depends_on',
  DATA_FLOW = 'data_flow',
  GENERATES_FLOW = 'generates_flow',
  WRITES = 'writes',
  READS = 'reads'
}

// 节点接口
export interface Node {
  id: string;
  type: NodeType;
  label: string;
  fqn?: string;
  properties: Record<string, any>;
}

// 边接口
export interface Edge {
  id: string;
  source: string;
  target: string;
  type: EdgeType;
  label: string;
  properties: Record<string, any>;
}

// 图响应接口
export interface GraphResponse {
  nodes: Node[];
  edges: Edge[];
}

// 对象详情响应接口
export interface ObjectDetailsResponse {
  node: Node;
  related_objects?: GraphResponse;
}

// 路径响应接口
export interface PathResponse {
  paths: GraphResponse[];
}

// 对象列表项接口
export interface ObjectItem {
  id: string;
  type: NodeType;
  name: string;
  fqn: string;
  description?: string;
  created_at?: string;
  updated_at?: string;
  properties?: Record<string, any>;
}

// 对象列表响应接口
export interface ObjectsResponse {
  objects: ObjectItem[];
  total_count: number;
  page: number;
  page_size: number;
  total_pages: number;
}

// 图查询参数接口
export interface GraphQueryParams {
  root_node_type: NodeType;
  root_node_fqn: string;
  depth?: number;
}

// 对象详情查询参数接口
export interface ObjectDetailsQueryParams {
  node_type: NodeType;
  node_fqn: string;
  include_related?: boolean;
}

// 路径查询参数接口
export interface PathQueryParams {
  source_node_fqn: string;
  target_node_fqn: string;
  max_depth?: number;
}

// 对象列表查询参数接口
export interface ObjectsQueryParams {
  object_type?: string;
  keyword?: string;
  database?: string;
  schema?: string;
  page?: number;
  page_size?: number;
}
