# 2025-05-22 Task01
## 1. 创建metadata_graph_builder模块
pglumilineage/graph_builder/下创建一个新模式metadata_graph_builder.py，它要能被graph_builder/service调用，其核心职责: 
1. 从 lumi_config.data_sources 读取所有需要同步元数据到图谱的、激活的数据源。
2. 对于每个数据源，从 lumi_metadata_store 的 objects_metadata, columns_metadata, functions_metadata 表中读取其完整的、最新的元数据。
3. 将这些关系型数据转换为一系列幂等的 Cypher 语句。
4. 执行这些 Cypher 语句，在 Apache AGE 中创建或更新节点（代表数据库、Schema、表、视图、列、函数）和它们之间的结构性关系。
5. 它的连接库的方式参考graph_builder/service.py中的graph_builder模式，其age数据写入的schema也参考graph_builder/service.py中的graph_builder模式。
6. Age 1.5.0版本的Cypher语法与2.0.0版本有所不同，需要进行转换。其特殊性用法可以参考progress/age_graph_decisions.md中的说明。

## 2. AGE 元数据图谱模型设计
### 1. 节点标签 (Node Labels):
DataSource: 代表一个被监控的数据源配置实例。
唯一标识属性: source_id (来自 lumi_config.data_sources.source_id) 或 name (来自 lumi_config.data_sources.source_name)。
其他属性: type, description, db_host, is_active 等。
Database: 代表一个实际的数据库实例。
唯一标识属性: fqn (完全限定名，例如 tpcds_prod_db，可以由 data_sources.source_name + objects_metadata.database_name 组合或直接用 objects_metadata.database_name 如果 source_name 已包含集群信息)。为了简化，我们可以先用 {name: database_name, dataSourceName: source_name} 来唯一标识。
其他属性: source_id (关联到 DataSource 节点)。
Schema: 代表数据库中的一个Schema。
唯一标识属性: fqn (例如 tpcds_prod_db.public) 或 {name: schema_name, database_fqn: '...'}。
其他属性: owner, description。
Table: 代表一个持久化表。
唯一标识属性: fqn (例如 tpcds_prod_db.public.store_sales) 或 {name: object_name, schema_fqn: '...'}。
其他属性: object_type: 'TABLE', owner, description, row_count, last_analyzed, properties (来自 objects_metadata)。
View: 代表一个视图。
唯一标识属性: fqn (同上)。
其他属性: object_type: 'VIEW', owner, description, definition (视图的SQL定义), properties (来自 objects_metadata)。
MaterializedView: 代表一个物化视图。
唯一标识属性: fqn (同上)。
其他属性: object_type: 'MATERIALIZED VIEW', owner, description, definition, properties (来自 objects_metadata)。
Column: 代表表或视图中的一列。
唯一标识属性: fqn (例如 tpcds_prod_db.public.store_sales.ss_item_sk) 或 {name: column_name, parent_object_fqn: '...'}。
其他属性: ordinal_position, data_type, max_length, numeric_precision, numeric_scale, is_nullable, default_value, is_primary_key, is_unique, description, properties (来自 columns_metadata)。
Function: 代表一个用户定义的函数或过程。
唯一标识属性: fqn (例如 tpcds_prod_db.public.my_function(int,text)) 或 {name: function_name, schema_fqn: '...', signature_hash: '...'} (signature_hash是基于参数类型生成的哈希，因为函数可以重载)。或者直接使用 function_id from functions_metadata 作为唯一属性 fid。
其他属性: function_type, return_type, parameters (JSONB), definition (函数代码), language, owner, description, properties (来自 functions_metadata)。
其他对象类型 (INDEX, SEQUENCE, FOREIGN TABLE) 根据需要也可以建模为节点，MVP阶段可以先聚焦上述核心对象。

### 2. 边标签 (Edge Labels / Relationship Types):
(DataSource)-[:CONFIGURES_DATABASE]->(Database): 数据源配置指向它描述的数据库实例。
(Database)-[:HAS_SCHEMA]->(Schema): 数据库包含Schema。
(Schema)-[:HAS_OBJECT]->(Table)
(Schema)-[:HAS_OBJECT]->(View)
(Schema)-[:HAS_OBJECT]->(MaterializedView)
(Schema)-[:HAS_FUNCTION]->(Function)
(Table)-[:HAS_COLUMN]->(Column)
(View)-[:HAS_COLUMN]->(Column) (视图的输出列)
(MaterializedView)-[:HAS_COLUMN]->(Column)
(Column)-[:REFERENCES_COLUMN]->(Column): 表示外键关系。
属性: constraint_name (可选，外键约束名)。
metadata_graph_builder 的实现方法 (Python + asyncpg + Cypher):

这个服务会读取 lumi_metadata_store 中的表，然后生成并执行Cypher语句。

## 3.定义FQN逻辑: 
需要在Python中生成唯一FQN（完全限定名）的策略，用于Database, Schema, Table/View, Column, Function节点。例如，Column的FQN可以是 source_name.database_name.schema_name.object_name.column_name。这个FQN将作为AGE节点中核心的唯一标识属性。

## 4. 实现节点和关系MERGE
我们需要在 metadata_graph_builder中实现一个函数，它遍历从 lumi_metadata_store.objects_metadata 查询到的记录。对于每条记录：
1. 根据记录的 source_id 和 database_name，MERGE 一个 :Database {fqn: '...'} 节点。
2. 根据记录的 schema_name，MERGE 一个 :Schema {fqn: '...'} 节点，并 MERGE (Database)-[:HAS_SCHEMA]->(Schema) 关系。
3. 根据记录的 object_name 和 object_type (TABLE, VIEW, MATERIALIZED VIEW)，MERGE 一个对应的 :Table 或 :View 或 :MaterializedView 节点（标签直接用 object_type 的值），并设置其 fqn 和其他属性（owner, description, definition_sql 等）。然后 MERGE (Schema)-[:HAS_OBJECT]->(Object) 关系。 所有 MERGE 都应使用 ON CREATE SET ... ON MATCH SET ... 来处理属性初始化和更新，特别是 created_at 和 updated_at (使用Cypher的 datetime())。
4. 需要注意merge语句中的ON CREATE SET ... ON MATCH SET ... 的写法在age1.5中不被支持，需要在函数convert_cypher_for_age中进行转换，参考progress/age_graph_decisions.md中的说明

## 5. 实现列节点和关系的MERGE:
为 metadata_graph_builder实现一个函数，遍历 lumi_metadata_store.columns_metadata 的记录。对于每一列：
MERGE 一个 :Column {fqn: '...'} 节点，并设置其属性（name, ordinal_position, data_type, is_nullable, description 等）。
MATCH 其父对象节点（Table/View）和该列节点，然后 MERGE (ParentObject)-[:HAS_COLUMN]->(Column) 关系。
如果该列有外键信息 (foreign_key_to_... 字段非空)，则 MATCH 当前列节点和被引用的目标列节点，然后 MERGE (CurrentColumn)-[:REFERENCES_COLUMN]->(TargetForeignKeyColumn) 关系。"

## 6. 参数化Cypher: 将所有生成的Cypher语句构造成参数化的形式，例如： MERGE (n:Label {fqn: $fqn_val}) ON CREATE SET n.name = $name_val, n.created_at = datetime() ... 然后你的Python代码会准备一个参数字典 {"fqn_val": "...", "name_val": "..."} 传递给 asyncpg 执行。这比字符串拼接更安全、更清晰。

# 2025-05-22 Task02
## graph_builder/职能设计
### 1. metadata_graph_builder.py:
职责: 专门负责读取 lumi_metadata_store 中的数据，并在Apache AGE中构建和维护基础的元数据图谱（Database, Schema, Table, View, Column, FunctionDefinition 节点及其结构性关系如 HAS_SCHEMA, HAS_OBJECT, HAS_COLUMN, REFERENCES_COLUMN 等）。
触发时机: 在 metadata_collector 服务运行成功后，或者定期（如每日）运行。

### 2. lineage_graph_builder.py:
职责:
读取 lumi_analytics.sql_patterns 表中LLM已成功解析的记录（即包含 llm_extracted_relations_json）。
假设基础元数据节点（由 metadata_graph_builder.py 创建）已存在于AGE中。
专注于创建 SqlPattern 节点。
MATCH 相关的 Column 节点（或其他相关的元数据节点）。
在这些节点之间创建 DATA_FLOW 边（核心血缘关系），边的属性包含 sql_hash, transformation_logic, derivation_type, last_seen_at 等。
创建 SqlPattern 节点与其引用的 Table/View/Column 节点之间的关系（如 READS_FROM, WRITES_TO）。
触发时机: 在 llm_relationship_extractor 服务成功处理完一批SQL模式后，或者定期运行。

### 3. common_graph_utils.py:
职责: 存放一些被 metadata_graph_builder.py 和 service.py 共用的图操作辅助函数。例如：
获取和管理AGE数据库连接的函数 (确保已加载AGE扩展并设置好search_path)。
执行Cypher语句的函数 (可能包括批量执行、事务管理、错误处理等)。
生成节点唯一标识符 (FQN - Fully Qualified Name) 的辅助函数。
Cypher字符串转义函数。
一些通用的 MERGE 节点或边的模板函数（如果可以抽象出来）。