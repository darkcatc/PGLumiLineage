# AGE 图谱模块设计决策

## 基本信息
- 作者：Vance Chen
- 创建日期：2025-05-20
- 最后更新：2025-05-21

## 模块概述
AGE 图谱模块负责在 PostgreSQL 数据库中使用 Apache AGE 扩展构建和查询图数据。该模块是 PGLumiLineage 项目的核心组件，用于存储和分析数据血缘关系。

## 关键设计决策

### 6. `MERGE ... ON CREATE SET ...` 语法适配 (2025-05-21)

**决策内容**：由于 Apache AGE 1.5.0 不支持 Cypher 的 `MERGE ... ON CREATE SET ...` 和 `MERGE ... ON MATCH SET ...` 复合语句，在 `pglumilineage/age_graph_builder/service.py` (尤其是 `_create_or_update_node` 和 `_create_or_update_edge` 方法) 中采用分步操作来模拟其行为。通常是先尝试 `MATCH`，如果节点/边不存在，则执行 `CREATE`，然后单独执行 `SET` 来更新属性。

**主要理由**：
- AGE 1.5.0 对 Cypher 的支持尚不完整，缺乏对 `MERGE` 语句中 `ON CREATE` 和 `ON MATCH` 子句的直接支持。
- 为了实现节点的“存在则更新，不存在则创建”的逻辑，必须分解操作。

**讨论的关键点**：
- **节点创建/更新逻辑**：
  1. `MATCH (n {label: $label, fqn: $fqn}) RETURN n`
  2. 如果未找到，则 `CREATE (n {label: $label, fqn: $fqn}) SET n += $properties RETURN n`
  3. 如果找到，则 `MATCH (n {label: $label, fqn: $fqn}) SET n += $properties RETURN n` (注意属性合并的策略)
- **边创建/更新逻辑**：类似地，先 `MATCH` 边，然后根据情况 `CREATE` 边并 `SET` 属性。
- 这种分步操作可能引入多次数据库交互，对性能有潜在影响，但目前是兼容 AGE 1.5.0 的必要措施。
- 考虑了错误处理和并发场景下的原子性问题（尽管 AGE 本身的事务可以部分缓解）。



### 1. AGE 1.5.0 配置要求 (2025-05-20)

**决策内容**：必须将 `shared_preload_libraries = 'age'` 写到 postgresql.conf 文件中，并且执行 `ALTER DATABASE iwdb SET search_path = ag_catalog, "$user", public;` 以解决各种莫名其妙的语法问题。

**主要理由**：
- AGE 1.5.0 需要将其函数库预加载到 PostgreSQL 中。
- 设置正确的搜索路径确保 AGE 函数可以被正确访问，避免 "function xxx does not exist" 错误。

**讨论的关键点**：
- 通过 Docker 配置挂载自定义的 postgresql.conf 文件。
- 在数据库初始化脚本中添加搜索路径设置。
- 这种方法比在每次连接时设置搜索路径更可靠。

### 2. Cypher 查询语法适配 (2025-05-20)

**决策内容**：在 AGE 1.5.0 中，节点和关系的标签必须使用属性格式，例如 `MATCH (n {label: 'NodeType'})` 和 `MATCH ()-[r {label: 'RelationType'}]->()`，而不是 Neo4j 风格的 `:NodeType` 或 `:[RELATION_TYPE]`。

**主要理由**：
- AGE 1.5.0 将标签作为内部属性 `label` 进行存储和查询。
- 使用错误的语法格式会导致查询无法找到节点或关系。

**讨论的关键点**：
- 节点格式：`(n {label: 'NodeType', property1: 'value1', ...})`。
- 关系格式：`-[r {label: 'RelationType', property1: 'value1', ...}]->`。
- 这一变更使得之前用于语法转换的 `convert_cypher_for_age` 函数大部分功能不再需要，可以大幅简化或移除。

### 3. `MERGE` 语句的 `ON CREATE SET` 和 `ON MATCH SET` 适配 (2025-05-21)

**决策内容**：Apache AGE 1.5.0 不支持 Cypher `MERGE` 语句中的 `ON CREATE SET` 和 `ON MATCH SET` 子句。因此，在 `pglumilineage/age_graph_builder/service.py` 中，创建或更新节点/关系的逻辑需要重写，采用先尝试匹配，再根据匹配结果决定是创建新元素还是更新现有元素属性的策略。

**主要理由**：
- 直接使用带有 `ON CREATE SET` 或 `ON MATCH SET` 的 `MERGE` 语句会在 AGE 1.5.0 中导致语法错误。
- 需要一种兼容的替代方案来实现“如果不存在则创建，如果存在则更新”的逻辑。

**讨论的关键点**：
- **替代方案**：通常分两步操作：
    1.  使用 `MATCH` 尝试查找节点或关系。
    2.  如果未找到，则使用 `CREATE` 创建新的节点或关系并设置初始属性。
    3.  如果找到，则使用 `SET` 更新现有节点或关系的属性。
- **事务性**：确保这些多步操作在单个事务中执行，以保证数据一致性。
- **性能影响**：这种分离的读写操作相对于原生的 `MERGE ... ON ... SET` 可能会有轻微的性能差异，但为了兼容性是必要的。
- **代码实现**：在 `AgeGraphBuilderService` 中调整生成 Cypher 语句的逻辑，确保生成的语句符合 AGE 1.5.0 的语法约束。

### 4. 多列返回值处理 (2025-05-20)

**决策内容**：改进 `LineageRepository._execute_cypher_core_async` 方法，使其能够处理多列返回的情况。

**主要理由**：
- 复杂的 Cypher 查询可能需要返回多个列。
- 原有实现可能仅优化或测试了单列返回场景，导致 "return row and column definition list do not match" 等错误。

**讨论的关键点**：
- 确保 `_execute_cypher_core_async` 能够正确解析 Cypher `RETURN` 子句中指定的多列，并将其映射到 `asyncpg` 的结果中。
- 处理包含聚合函数、路径以及不同数据类型（如节点、关系、标量值）的混合返回列。

### 5. API 结果格式统一 (2025-05-20)

**决策内容**：统一API（如 `LineageService`）返回图数据的格式，使用如 `{"nodes": [...], "edges": [...]}` (或 `"relationships"`) 作为标准键名，以适配前端可视化库的需求。

**主要理由**：
- 保持 API 接口的一致性，方便前端处理。
- 对齐 AGE 返回的原始数据结构（通常是行和列）与图可视化库（如 AntV G6）期望的节点列表和边列表格式。

**讨论的关键点**：
- `LineageService._format_graph_response` 方法负责将 `LineageRepository` 查询到的原始 AGE 数据转换为统一的 `GraphResponse` Pydantic 模型。
- 确保节点和边的属性被正确提取和映射。

### 6. 移除冗余脚本与简化转换函数 (2025-05-20)

**决策内容**：鉴于对 AGE 1.5.0 配置和语法的深入理解，移除不再需要的 `scripts/convert_cypher_for_age.py` 脚本，并简化或移除 `pglumilineage/age_graph_builder/service.py` 中残留的 `convert_cypher_for_age` 函数。

**主要理由**：
- 正确配置数据库 (`search_path`) 和直接使用 AGE 1.5.0 兼容的 Cypher 语法（如 `{label: 'Label'}`）后，复杂的客户端语法转换不再必要。
- 简化代码库，减少维护负担，避免不必要的转换引入错误。

**讨论的关键点**：
- 确保所有新编写或修改的 Cypher 查询直接遵循 AGE 1.5.0 的规范。
- 在文档和代码注释中明确指出正确的 Cypher 语法，指导开发者。

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

### AGE 1.5.0中的$符号转义问题

| 决策日期 | 决策内容 | 主要理由 | 讨论的关键点 |
|---------|---------|---------|------------|
| 2024-07-21 | 解决AGE 1.5.0中的$符号转义问题 | 在PostgreSQL中执行Cypher查询时，$符号有特殊含义，需要正确转义 | 当使用psql命令行或其他SQL客户端执行Cypher查询时，$符号的转义处理不当会导致查询失败 |

#### 问题描述

在使用PostgreSQL执行AGE Cypher查询时，我们需要使用如下格式的SQL语句：

```sql
SELECT * FROM cypher('graph_name', $$ MATCH (n) RETURN n $$) as (n agtype);
```

这里的问题是：

1. PostgreSQL使用`$$`来包裹Cypher查询，以避免内部的单引号和双引号需要转义
2. 当Cypher查询中包含`$`符号时（如参数化查询），会与Psql的变量替换机制冲突
3. 当使用命令行执行包含`$`的查询时，可能需要额外的shell转义

#### 解决方案

我们采用了以下解决方案：

1. **在Python代码中执行Cypher查询时**：
   ```python
   query = f"SELECT * FROM cypher('{graph_name}', $$ {cypher_stmt} $$) AS (result agtype);"
   await conn.execute(query)
   ```
   这种方式不需要额外转义`$`符号，因为`$$`字符串字面量会正确处理。

2. **在命令行使用psql执行Cypher查询时**：
   - 如果直接在命令行使用`-c`参数，需要额外转义`$`符号：
     ```bash
     psql -U username -d dbname -c "SELECT * FROM cypher('graph_name', \$\$ MATCH (n) RETURN n \$\$) as (n agtype);"
     ```
   - 更好的方法是将Cypher查询保存到文件中，然后使用`-f`参数执行：
     ```bash
     psql -U username -d dbname -f query.sql
     ```
     其中`query.sql`文件内容为：
     ```sql
     SELECT * FROM cypher('graph_name', $$ MATCH (n) RETURN n $$) as (n agtype);
     ```

3. **在代码中构建Cypher查询时**：
   - 避免在Cypher查询中使用`$`符号作为参数占位符
   - 使用字符串插值来构建Cypher查询，而不是使用参数化查询
   - 对于需要动态构建的查询，使用如下方式：
     ```python
     def _interpolate_params(self, query: str, params: Dict[str, Any] = None) -> str:
         """Cypher查询参数插值函数"""
         if not params:
             return query
         
         interpolated_query = query
         
         for key, value in params.items():
             param_placeholder = "${" + key + "}"
             
             if param_placeholder not in interpolated_query:
                 continue
             
             # 根据参数类型进行适当的处理
             if value is None:
                 replacement = "NULL"
             elif isinstance(value, bool):
                 replacement = str(value).lower()
             elif isinstance(value, (int, float)):
                 replacement = str(value)
             elif isinstance(value, str):
                 replacement = f"'{value.replace('\'', '\\'')}'"
             # ...其他类型的处理...
             
             # 替换查询中的占位符
             interpolated_query = interpolated_query.replace(param_placeholder, replacement)
         
         return interpolated_query
     ```

通过以上方法，我们成功解决了AGE 1.5.0中的`$`符号转义问题，确保了Cypher查询的正确执行。

## 节点ID和属性访问的特殊处理

在AGE 1.5.0中，节点ID和属性的访问方式与之前版本有显著不同，需要特别注意以下几点：

1. **节点ID的访问**：
   - 在AGE 1.5.0中，必须使用`id(n)`函数来获取节点ID，而不是直接访问`n.id`
   - 错误示例：`WHERE n.id = 123`
   - 正确示例：`WHERE id(n) = 123`

2. **属性的直接访问**：
   - 在AGE 1.5.0中，属性可以直接通过点符号访问，不需要通过`properties`中间层，而且Return的时候也不能使用`properties`，否则会返回`null`，age针对第一层的`properties`会进行透明化处理，即`n.properties.name`等价于`n.name`
   - 错误示例：`WHERE n.properties.name = 'customer'`
   - 正确示例：`WHERE n.name = 'customer'`

3. **节点标签的处理**：
   - 在AGE 1.5.0中，节点标签通过`label`属性访问，而不是通过`:Label`语法
   - 错误示例：`MATCH (n:Table)`
   - 正确示例：`MATCH (n) WHERE n.label = 'table'`

这些变化要求我们在编写Cypher查询时必须遵循AGE 1.5.0的语法规范，否则会导致查询失败或返回错误的结果。在我们的实现中，已经针对这些变化进行了适配，确保查询能够正确执行。
