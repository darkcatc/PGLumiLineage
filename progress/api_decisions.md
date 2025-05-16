# 血缘关系API设计决策

**最后更新时间**：2025-05-16

## 概述

血缘关系API模块负责从Apache AGE图数据库中查询血缘关系数据，并以适合前端可视化的格式提供给客户端。该模块采用FastAPI框架构建，提供RESTful API接口，支持血缘图查询、对象详情查询和路径查找等功能。

## 核心设计决策

### 1. 技术栈选择

| 决策日期 | 决策内容 | 主要理由 | 讨论的关键点 |
|---------|---------|---------|------------|
| 2025-05-16 | 选择FastAPI作为API框架 | 性能优秀、支持异步、自动生成API文档 | 考虑了Flask、Django REST、FastAPI，最终选择FastAPI因其异步支持和与项目其他部分的技术栈一致性 |
| 2025-05-16 | 使用Pydantic进行数据验证和序列化 | 类型安全、自动验证、与FastAPI无缝集成 | 确保API请求和响应的类型安全，减少运行时错误 |
| 2025-05-16 | 采用分层架构（路由、服务、数据访问层） | 关注点分离、便于维护和测试 | 讨论了不同层次的职责划分，确保代码的可维护性和可测试性 |

### 2. API设计

| 决策日期 | 决策内容 | 主要理由 | 讨论的关键点 |
|---------|---------|---------|------------|
| 2025-05-16 | 设计三个核心API端点：图查询、对象详情、路径查找 | 满足血缘关系可视化的基本需求 | 分析了前端可视化所需的数据结构和查询模式 |
| 2025-05-16 | 采用查询参数而非路径参数传递查询条件 | 提高灵活性，便于添加可选参数 | 讨论了RESTful API设计最佳实践 |
| 2025-05-16 | 响应格式设计为节点列表和边列表的组合 | 适配主流图可视化库的数据格式要求 | 分析了Vis.js、Cytoscape.js、AntV G6等图库的数据格式需求 |

### 3. 查询优化

| 决策日期 | 决策内容 | 主要理由 | 讨论的关键点 |
|---------|---------|---------|------------|
| 2025-05-16 | 限制查询深度（最大5层） | 防止过大的查询导致性能问题 | 平衡查询完整性和性能考虑 |
| 2025-05-16 | 实现节点和边的去重逻辑 | 避免重复数据，减少传输量 | 讨论了不同去重策略的性能影响 |
| 2025-05-16 | 适配AGE 1.5.0的Cypher语法 | 确保查询语句能在当前AGE版本中正确执行 | 分析了AGE 1.5.0的语法特点和限制 |

### 4. 错误处理

| 决策日期 | 决策内容 | 主要理由 | 讨论的关键点 |
|---------|---------|---------|------------|
| 2025-05-16 | 实现细粒度的错误捕获和HTTP状态码映射 | 提供清晰的错误信息，便于调试 | 讨论了不同类型错误应返回的HTTP状态码 |
| 2025-05-16 | 添加详细的日志记录 | 便于问题排查和性能分析 | 确定了日志级别和记录内容 |

## API端点详情

### 1. 获取血缘子图

```
GET /api/lineage/graph
```

**查询参数**：
- `root_node_type`: 根节点类型（表、列、视图等）
- `root_node_fqn`: 根节点全限定名
- `depth`: 查询深度（1-5）

**响应格式**：
```json
{
  "nodes": [
    {
      "id": "string",
      "type": "table",
      "label": "string",
      "fqn": "string",
      "properties": {}
    }
  ],
  "edges": [
    {
      "id": "string",
      "source": "string",
      "target": "string",
      "type": "contains",
      "label": "string",
      "properties": {}
    }
  ]
}
```

### 2. 获取对象详情

```
GET /api/lineage/object_details
```

**查询参数**：
- `node_type`: 节点类型
- `node_fqn`: 节点全限定名
- `include_related`: 是否包含相关对象（布尔值）

**响应格式**：
```json
{
  "node": {
    "id": "string",
    "type": "table",
    "label": "string",
    "fqn": "string",
    "properties": {}
  },
  "related_objects": {
    "nodes": [],
    "edges": []
  }
}
```

### 3. 查找两点间路径

```
GET /api/lineage/paths
```

**查询参数**：
- `source_node_fqn`: 源节点全限定名
- `target_node_fqn`: 目标节点全限定名
- `max_depth`: 最大查询深度（1-10）

**响应格式**：
```json
{
  "paths": [
    {
      "nodes": [],
      "edges": []
    }
  ]
}
```

## Cypher查询语句

### 1. 查询子图

```cypher
MATCH (root {fqn: $fqn, label: $node_type})
CALL {
    WITH root
    MATCH path = (root)-[*1..$depth]-(related)
    RETURN path
}
WITH COLLECT(path) AS paths
RETURN paths
```

### 2. 查询节点详情

```cypher
MATCH (n {fqn: $fqn, label: $node_type})
RETURN n
```

### 3. 查询直接邻居

```cypher
MATCH (n {fqn: $fqn, label: $node_type})-[r]-(neighbor)
RETURN n, r, neighbor
```

### 4. 查询两点间路径

```cypher
MATCH (source {fqn: $source_fqn}), (target {fqn: $target_fqn}),
p = (source)-[*1..$max_depth]-(target)
RETURN p
LIMIT 10
```

## 未来计划

1. **性能优化**：
   - 实现查询结果缓存
   - 优化大规模图数据的查询性能

2. **功能扩展**：
   - 添加更多图分析功能（如影响分析、依赖分析）
   - 支持更复杂的查询条件和过滤

3. **安全性增强**：
   - 实现API认证和授权机制
   - 添加访问控制和数据隐私保护

4. **前端集成**：
   - 与选定的前端图可视化库（如AntV G6）深度集成
   - 提供更丰富的交互功能
