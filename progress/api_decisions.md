# 血缘关系API设计决策

**最后更新时间**：2025-05-21

## 概述

血缘关系API模块负责从Apache AGE图数据库中查询血缘关系数据，并以适合前端可视化的格式提供给客户端。该模块采用FastAPI框架构建，提供RESTful API接口，支持血缘图查询、对象详情查询和路径查找等功能。核心目标是确保与 Apache AGE 1.5.0 的兼容性，并高效地将图数据转换为前端易于消费的结构。

## 核心设计决策

### 1. 技术栈选择

| 决策日期 | 决策内容 | 主要理由 | 讨论的关键点 |
|---------|---------|---------|------------|
| 2025-05-16 | 选择FastAPI作为API框架 | 性能优秀、支持异步、自动生成API文档、Python生态系统良好 | 考虑了Flask、Django REST、FastAPI，最终选择FastAPI因其现代特性和与项目异步I/O模型的一致性。 |
| 2025-05-16 | 使用Pydantic进行数据验证和序列化 | 类型安全、自动验证、与FastAPI无缝集成、清晰的数据模型定义 | 确保API请求和响应的类型安全，减少运行时错误，提升开发效率。 |
| 2025-05-16 | 采用分层架构（路由 `router.py`、服务 `service.py`、数据访问 `repository.py`） | 关注点分离、代码结构清晰、便于维护和单元测试 | 讨论了各层的职责：路由层处理HTTP请求和响应；服务层包含业务逻辑和数据转换；数据访问层封装与AGE数据库的直接交互。 |

### 2. API设计与端点

| 决策日期 | 决策内容 | 主要理由 | 讨论的关键点 |
|---------|---------|---------|------------|
| 2025-05-16 | 设计三个核心API端点：获取血缘子图 (`/api/lineage/graph`)、获取对象详情 (`/api/lineage/object_details`)、查找两点间路径 (`/api/lineage/paths`) | 满足血缘关系可视化的基本需求，提供不同粒度的图数据查询 | 分析了前端可视化场景所需的数据查询模式和参数。 |
| 2025-05-16 | 查询参数主要通过GET请求的Query Parameters传递 | 符合RESTful设计原则，灵活性高，便于添加可选参数和缓存 | 讨论了路径参数与查询参数的适用场景。 |
| 2025-05-21 | **响应格式适配前端图库**：统一将图数据格式化为包含节点列表 (`nodes`) 和边列表 (`edges`) 的JSON对象 (通过 `GraphResponse` Pydantic模型)。 | 适配主流图可视化库（如AntV G6）的数据格式要求，简化前端处理逻辑。 | 在 `LineageService._format_graph_response` 中实现，将AGE返回的原始数据（通常是`agtype`) 转换为结构化的节点和边对象。 |

### 3. Apache AGE 1.5.0 兼容性与数据处理

| 决策日期 | 决策内容 | 主要理由 | 讨论的关键点 |
|---------|---------|---------|------------|
| 2025-05-20 | **适配AGE 1.5.0的Cypher查询语法** | AGE 1.5.0对Cypher语法有特定要求，如节点标签表示为 `MATCH (n {label: 'NodeType'})`。 | `LineageRepository` 中的Cypher查询语句已更新以符合AGE 1.5.0规范。 |
| 2025-05-21 | **AGE数据解析与转换**：在 `LineageRepository.query_subgraph` (及其他查询方法) 和 `LineageService._format_graph_response` 中实现对AGE返回的 `agtype` 数据进行细致解析。 | AGE返回的节点和边数据是特定的 `agtype` 类型，需要从中准确提取其内部的属性 (`properties`)、ID (`id` 或 `gid`)、标签 (`label`)、开始节点ID (`start_id` 或 `start_vertex_gid`)、结束节点ID (`end_id` 或 `end_vertex_gid`)等信息。 | - 如何健壮地从 `apache_age.age_types.Vertex` (`agtype`) 中提取 `id` (通常是 `gid` 字符串), `label`, `properties` (通常是 `dict`)。
- 如何健壮地从 `apache_age.age_types.Edge` (`agtype`) 中提取 `id` (通常是 `gid` 字符串), `label`, `start_id` (通常是 `start_vertex_gid`), `end_id` (通常是 `end_vertex_gid`), `properties` (通常是 `dict`)。
- `LineageRepository` 中增加了 `_parse_age_vertex` 和 `_parse_age_edge` 辅助方法用于标准化解析逻辑。
- `LineageService` 中的 `_format_graph_response` 负责将解析后的 `ParsedVertex` 和 `ParsedEdge` 对象组装成前端需要的 `Node` 和 `Edge` 模型。 |
| 2025-05-16 | 限制查询深度（例如，图查询最大5层） | 防止因查询范围过大导致API超时或数据库性能问题。 | 平衡查询结果的完整性与系统性能。 |
| 2025-05-16 | 实现节点和边的去重逻辑（主要在服务层 `_format_graph_response` 中） | 避免在响应中包含重复的图元素，减少数据传输量。 | 讨论了基于节点/边 `id` 的去重策略，确保每个元素只出现一次。 |

### 4. 错误处理与日志

| 决策日期 | 决策内容 | 主要理由 | 讨论的关键点 |
|---------|---------|---------|------------|
| 2025-05-16 | 实现细粒度的错误捕获和HTTP状态码映射 | 通过FastAPI的异常处理器提供清晰、标准的错误信息给客户端。 | 讨论了不同业务异常（如节点未找到、查询超时、AGE数据库错误）应返回的HTTP状态码和错误详情。 |
| 2025-05-16 | 添加详细的日志记录 (使用 `loguru`) | 便于问题排查、API调用追踪和性能分析。 | 确定了关键操作（如API请求接收、数据库查询执行与耗时、数据转换过程、错误发生点）的日志级别和记录内容。 |

## API端点详情 (示例)

### 1. 获取血缘子图
`GET /api/lineage/graph`
- **参数**: `root_node_type: str`, `root_node_fqn: str`, `depth: int = 1`, `relationship_types: Optional[List[str]] = None`
- **响应 (`GraphResponse`)**: 
```json
{
  "nodes": [
    {"id": "vertex_gid_1", "type": "Table", "label": "table_name", "fqn": "db.schema.table_name", "properties": {"col_count": 10, ...}},
    ...
  ],
  "edges": [
    {"id": "edge_gid_1", "source": "vertex_gid_1", "target": "vertex_gid_2", "type": "CONTAINS_COLUMN", "label": "contains_column", "properties": {}},
    ...
  ]
}
```

*(其他端点 `/api/lineage/object_details` 和 `/api/lineage/paths` 结构类似，返回相应的数据模型)*

## Cypher查询语句示例 (适配AGE 1.5.0)

### 1. 查询子图 (简化概念示例，实际查询在 `repository.py`)
```cypher
// 概念：查询以 root 为起点，指定深度和关系类型的子图
MATCH (root {fqn: $root_node_fqn, label: $root_node_type})
CALL {
    WITH root
    // 构建动态的关系类型匹配，如果 relationship_types 提供了的话
    MATCH path = (root)-[r*1..$depth]-(related)
    // WHERE type(r) IN $relationship_types (如果需要过滤关系类型)
    RETURN path
}
WITH collect(path) as paths
CALL apoc.convert.toTree(paths, false, {nodes: {User: ['name']},rels: {FOLLOWS:['since']}}) YIELD value
RETURN value;
// 注意：AGE 可能不直接支持 apoc.convert.toTree。实际返回的是路径集合，
// 需要在Python代码中解析路径中的节点和关系。
// `LineageRepository.query_subgraph` 中的实际查询更复杂，用于提取所有不重复的节点和关系。
```

*(其他Cypher查询语句也需确保AGE 1.5.0兼容性，并由 `LineageRepository` 封装)*

## 未来计划

1. **性能优化**: 实现查询结果缓存 (如使用Redis)；进一步优化大规模图数据的Cypher查询，考虑使用 AGE 的图分析函数（如果适用）。
2. **功能扩展**: 添加更高级的图分析功能（如影响分析、最短路径算法的更多参数化选项）；支持更复杂的查询条件和过滤（如按属性过滤）。
3. **安全性增强**: 实现API认证和授权机制 (如OAuth2/JWT)。
4. **文档完善**: 持续更新和细化OpenAPI (Swagger/ReDoc) 文档，确保与代码实现同步。
