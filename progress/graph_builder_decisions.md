# AGE图谱构建器 (graph_builder) 设计决策文档

**最后更新时间**：2025-05-16

## 模块概述

AGE图谱构建器是PGLumiLineage项目的核心组件之一，负责将LLM分析器提取的数据血缘关系转换为Cypher语句，并在Apache AGE图数据库中构建数据血缘知识图谱。该模块将结构化的血缘关系信息作为输入，生成幂等的Cypher语句，并执行这些语句来创建和维护图谱中的节点和关系。

## 关键设计决策

### 1. 图模型设计

**决策**：采用多层次、语义丰富的图模型设计，包括数据库、模式、表/视图、列等节点类型，以及多种关系类型。

**原因**：
- 多层次的图模型能够准确表示数据库对象之间的层级关系
- 丰富的节点和关系类型能够表达复杂的数据血缘关系
- 语义明确的关系类型便于后续的查询和分析

**具体模型**：
- 节点类型：
  - `Database {name: TEXT}`：表示数据库
  - `Schema {name: TEXT, database_name: TEXT}`：表示模式
  - `Table {name: TEXT, schema_name: TEXT, database_name: TEXT, object_type: 'TABLE'}`：表示表
  - `View {name: TEXT, schema_name: TEXT, database_name: TEXT, object_type: 'VIEW'}`：表示视图
  - `Column {name: TEXT, fqn: TEXT}`：表示列，使用完全限定名（FQN）作为唯一标识
  - `SqlPattern {sql_hash: TEXT, normalized_sql: TEXT, sample_sql: TEXT}`：表示SQL模式

- 关系类型：
  - `(Database)-[:HAS_SCHEMA]->(Schema)`：数据库包含模式
  - `(Schema)-[:HAS_OBJECT]->(Table/View)`：模式包含表/视图
  - `(Table/View)-[:HAS_COLUMN]->(Column)`：表/视图包含列
  - `(SourceColumn:Column)-[df:DATA_FLOW]->(TargetColumn:Column)`：数据流关系，包含转换逻辑等属性
  - `(SqlPattern)-[:GENERATES_FLOW]->(df:DATA_FLOW)`：SQL模式生成数据流
  - `(SqlPattern)-[:WRITES_TO]->(TargetObjectOrColumn)`：SQL模式写入对象
  - `(SqlPattern)-[:READS_FROM]->(SourceObjectOrColumn)`：SQL模式读取对象

### 2. Cypher生成策略

**决策**：采用幂等的Cypher语句生成策略，使用MERGE而不是CREATE。

**原因**：
- 幂等性确保多次执行相同的语句不会产生重复的节点和关系
- 便于增量更新图谱，无需担心重复创建已存在的节点和关系
- 支持图谱的持续演进，能够反映数据血缘关系的变化

**实现方式**：
- 使用`MERGE`语句创建节点和关系
- 使用`ON CREATE SET`和`ON MATCH SET`区分首次创建和更新的情况
- 为节点和关系设置唯一标识属性，确保幂等性

### 3. 唯一性保证策略

**决策**：为不同类型的节点设计不同的唯一标识策略。

**原因**：
- 确保节点的唯一性是图谱正确性的基础
- 不同类型的节点有不同的唯一标识要求
- 完善的唯一标识策略能够避免重复节点和关系

**实现方式**：
- 数据库节点：使用`name`作为唯一标识
- 模式节点：使用`name`和`database_name`的组合作为唯一标识
- 表/视图节点：使用`name`、`schema_name`和`database_name`的组合作为唯一标识
- 列节点：使用完全限定名（FQN，格式为`database.schema.table.column`）作为唯一标识
- SQL模式节点：使用`sql_hash`作为唯一标识

### 4. 特殊情况处理

**决策**：为字面量、表达式和UNION操作等特殊情况设计专门的处理逻辑。

**原因**：
- 数据血缘关系中存在多种复杂情况，需要特殊处理
- 字面量和表达式没有源列，但仍然是数据的来源
- UNION操作涉及多个源，需要正确表示它们的关系

**实现方式**：
- 字面量和表达式：当源列为空时，直接从源对象到目标列创建数据流关系
- 转换逻辑：将LLM提取的转换逻辑作为数据流关系的属性
- 派生类型：使用LLM提取的派生类型（如`DIRECT_MAPPING`、`AGGREGATION`、`UNION_MERGE`等）作为数据流关系的属性

### 5. 时间戳管理

**决策**：为数据流关系添加时间戳属性，记录创建和最后更新时间。

**原因**：
- 时间戳能够反映数据血缘关系的变化历史
- 便于追踪数据血缘关系的演变过程
- 支持基于时间的查询和分析

**实现方式**：
- 使用`created_at`属性记录数据流关系的首次创建时间
- 使用`last_seen_at`属性记录数据流关系的最后更新时间
- 使用Cypher的`datetime()`函数获取当前时间

## 核心函数实现

### transform_json_to_cypher

**功能**：将LLM提取的关系转换为Cypher语句

**实现步骤**：
1. 从`pattern_info.llm_extracted_relations_json`中解析数据血缘关系信息
2. 创建/合并数据库节点
3. 收集所有涉及的模式、表/视图和列
4. 创建/合并模式节点，并与数据库建立关系
5. 创建/合并表/视图节点，并与模式建立关系
6. 创建/合并SQL模式节点
7. 创建/合并列节点，并与表/视图建立关系
8. 创建/合并数据流关系，设置转换逻辑、派生类型和时间戳
9. 创建/合并SQL模式与数据流的关联
10. 创建/合并SQL模式与引用对象的关系
11. 处理Cypher语句列表，将带参数的语句转换为可执行的形式

**关键技术点**：
- 使用参数化查询处理可能包含特殊字符的字段
- 处理字面量和表达式的特殊情况
- 确保节点和关系的唯一性
- 使用幂等的MERGE语句

### build_graph_for_pattern

**功能**：为单个SQL模式构建图谱

**实现步骤**：
1. 调用`transform_json_to_cypher`函数生成Cypher语句
2. 执行Cypher语句，与AGE图数据库交互
3. 记录执行结果和可能的错误

### build_graph_for_patterns

**功能**：批量处理多个SQL模式

**实现步骤**：
1. 遍历SQL模式列表
2. 为每个SQL模式调用`build_graph_for_pattern`函数
3. 收集和返回执行结果

## 演进历史

### 初始版本 (2025-05-16)

- 设计并实现了基本的图模型
- 实现了`transform_json_to_cypher`函数，将LLM提取的关系转换为Cypher语句
- 设计了幂等的Cypher生成策略
- 实现了特殊情况的处理逻辑
- 添加了时间戳管理

## 未来计划

### 短期计划

1. **执行引擎实现**：
   - 实现与AGE图数据库的交互逻辑
   - 支持批量执行Cypher语句
   - 添加事务支持，确保操作的原子性

2. **错误处理优化**：
   - 完善错误处理机制
   - 添加重试逻辑
   - 实现详细的错误日志和报告

3. **性能优化**：
   - 优化Cypher语句生成
   - 减少不必要的数据库操作
   - 实现批量处理机制

### 中长期计划

1. **图谱查询接口**：
   - 设计和实现图谱查询接口
   - 支持复杂的血缘关系查询
   - 提供可视化支持

2. **图谱演进支持**：
   - 实现图谱版本管理
   - 支持图谱的增量更新
   - 提供图谱比较功能

3. **集成扩展**：
   - 与其他图数据库集成
   - 支持更多的图查询语言
   - 提供API接口，便于与其他系统集成

## 技术债务与挑战

1. **大规模图谱性能**：
   - 当图谱规模增长时，可能面临性能挑战
   - 需要设计优化策略，如索引优化、查询优化等

2. **复杂SQL处理**：
   - 对于非常复杂的SQL语句，LLM提取的关系可能不完整或不准确
   - 需要设计验证和补充机制

3. **图模型演进**：
   - 随着需求的变化，图模型可能需要调整
   - 需要设计支持图模型演进的机制

## 关键接口

### 输入接口

- **pattern_info**：`AnalyticalSQLPattern`对象，包含LLM提取的关系和SQL模式信息

### 输出接口

- **cypher_statements**：Cypher语句列表，用于构建图谱
- **build_result**：构建结果，包含成功/失败状态和可能的错误信息

## 依赖关系

- **内部依赖**：
  - common模块：提供模型定义和工具函数
  - llm_analyzer模块：提供LLM提取的关系

- **外部依赖**：
  - Apache AGE：图数据库
  - asyncpg：PostgreSQL异步客户端
  - json：JSON处理
