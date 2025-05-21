# AGE 图谱模块设计决策

## 基本信息
- 作者：Vance Chen
- 创建日期：2025-05-20
- 最后更新：2025-05-20

## 模块概述
AGE 图谱模块负责在 PostgreSQL 数据库中使用 Apache AGE 扩展构建和查询图数据。该模块是 PGLumiLineage 项目的核心组件，用于存储和分析数据血缘关系。

## 关键设计决策

### 1. AGE 1.5.0 配置要求 (2025-05-20)

**决策内容**：必须将 `shared_preload_libraries = 'age'` 写到 postgresql.conf 文件中，并且执行 `ALTER DATABASE iwdb SET search_path = ag_catalog, "$user", public;` 以解决各种莫名其妙的语法问题。

**主要理由**：
- AGE 1.5.0 需要将其函数库预加载到 PostgreSQL 中
- 设置正确的搜索路径确保 AGE 函数可以被正确访问，避免 "function xxx does not exist" 错误

**讨论的关键点**：
- 通过 Docker 配置挂载自定义的 postgresql.conf 文件
- 在数据库初始化脚本中添加搜索路径设置
- 这种方法比在每次连接时设置搜索路径更可靠

### 2. Cypher 查询语法适配 (2025-05-20)

**决策内容**：在 AGE 1.5.0 中，节点标签必须使用 `{label: 'Label'}` 格式，而不是 Neo4j 风格的 `:Label` 格式。

**主要理由**：
- AGE 1.5.0 内部存储节点标签为属性，而不是传统图数据库的标签
- 使用错误的语法格式会导致查询无法找到节点

**讨论的关键点**：
- 节点格式：`(n {label: 'NodeType', ...})`
- 关系格式：`-[r {label: 'RelationType'}]->`
- 不再需要 `convert_cypher_for_age` 函数进行复杂的语法转换

### 3. 多列返回值处理 (2025-05-20)

**决策内容**：改进 `_execute_cypher_core_async` 方法，使其能够处理多列返回的情况。

**主要理由**：
- 复杂的 Cypher 查询可能需要返回多个列
- 原有实现只能处理单列返回，导致 "return row and column definition list do not match" 错误

**讨论的关键点**：
- 完整解析 RETURN 子句，支持多列返回和别名处理
- 处理有别名和无别名的情况
- 构造正确的 SQL AS 子句

### 4. 结果格式统一 (2025-05-20)

**决策内容**：统一 API 返回结果格式，使用 `nodes`/`relationships` 或 `neighbors`/`relationships` 作为标准键名。

**主要理由**：
- 保持 API 一致性，避免混淆
- 与 AGE 的内部表示保持一致

**讨论的关键点**：
- `query_subgraph` 返回 `{"nodes": [...], "relationships": [...]}`
- `query_direct_neighbors` 返回 `{"neighbors": [...], "relationships": [...]}`
- 考虑在未来版本中统一为相同的键名

### 5. 移除冗余脚本与简化转换函数 (2025-05-20)

**决策内容**：移除 `scripts/convert_cypher_for_age.py` 脚本，并简化 `age_graph_builder/service.py` 中的 `convert_cypher_for_age` 函数。

**主要理由**：
- 通过正确的数据库配置，我们不再需要复杂的语法转换
- 直接使用 `{label: 'Label'}` 格式的查询可以正常工作
- 简化代码库，提高可维护性

**讨论的关键点**：
- 独立脚本不符合项目的开发规范
- 简化后的函数仅保留基本的标签和关系语法转换
- 新代码应直接使用 `{label: 'Label'}` 格式，而不依赖转换函数
- 添加了明确的文档注释，指导开发者使用正确的语法

## 技术债务与挑战

1. **语法兼容性**：当前实现仅支持 AGE 1.5.0 的语法格式，如果需要支持其他版本或 Neo4j，需要添加兼容层

2. **错误处理**：当前错误处理较为简单，可以增强错误信息的详细程度和可理解性

3. **性能优化**：大型图查询可能需要进一步优化，如添加索引、分页等机制

## 关键接口

1. `LineageRepository._execute_cypher_core_async` - 核心 Cypher 查询执行方法
2. `LineageRepository.query_subgraph` - 查询子图
3. `LineageRepository.query_node_details` - 查询节点详情
4. `LineageRepository.query_direct_neighbors` - 查询直接邻居

## 依赖关系

- PostgreSQL 14+
- Apache AGE 1.5.0
- asyncpg 库

## 重要注意事项

**必须将 `shared_preload_libraries = 'age'` 写到 postgresql.conf 文件中，并且执行 `ALTER DATABASE iwdb SET search_path = ag_catalog, "$user", public;` 从而规避各种莫名其妙的语法问题。**

在 Cypher 查询中，必须使用 `{label: 'Label'}` 格式而不是 `:Label` 格式来指定节点标签。例如：

```cypher
# 正确的格式
MATCH (n {label: 'Table', fqn: 'db.schema.table'}) RETURN n

# 错误的格式（在 AGE 1.5.0 中不起作用）
MATCH (n:Table {fqn: 'db.schema.table'}) RETURN n
```
