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

# 2025-05-24 Task03
## 1. 创建lineage_graph_builder模块
### 1 类职责和结构 (LineageGraphBuilder):
LineageGraphBuilder 的职责明确：它不负责创建基础的 Database, Schema, Table, View, Column (属于持久化表/视图的) 节点及其层级关系（HAS_SCHEMA, HAS_OBJECT, HAS_COLUMN）。这些是 metadata_graph_builder.py 的工作。
将相关逻辑封装在类中，便于管理数据库配置和图名称。
使用异步方法 (async/await) 进行数据库操作。
连接池管理: 在 __init__ 中不立即创建连接池，而是在首次需要连接时（_get_iwdb_conn）创建，并提供 close_iwdb_pool 方法在服务结束时关闭。这使得构建器实例可以先创建，连接池在实际工作时才建立。

### 2 获取待处理数据 (Workspace_pending_sql_patterns_for_lineage):
从 lumi_analytics.sql_patterns 读取 llm_analysis_status = 'COMPLETED_SUCCESS' 且 is_loaded_to_age = FALSE 的记录。
通过Pydantic模型 models.AnalyticalSQLPattern 实例化数据。
（未来增强）可以增加一个中间状态来标记正在被加载到AGE的记录，以支持并发处理（MVP阶段可以先不考虑）。

### 3 核心转换逻辑 (transform_llm_json_to_cypher_batch 及辅助函数):
1. _generate_cypher_for_object_node :
当LLM JSON中提到一个对象（表或视图）时，这个函数的主要目标是确保该对象节点在图中存在，以便后续的边可以连接到它。
策略：
如果LLM提供的对象 type 是 TABLE 或 VIEW: 它应该生成 MERGE (obj:Table/View {fqn: $fqn}) ON CREATE SET obj.name = $name, ... obj.is_metadata_sourced = true (或类似标记)。这里的 ON CREATE SET 是一个后备，以防 metadata_graph_builder 由于某种原因（比如时序问题，或者该对象确实是新出现的且 metadata_collector 还没扫到）尚未创建它。理想情况下，metadata_graph_builder 已经创建了这个节点并填充了丰富的元数据属性。lineage_graph_builder 的 MERGE 应该主要依赖FQN匹配，并且 ON MATCH 时不应该覆盖由 metadata_graph_builder 设置的权威元数据属性。
如果LLM提供的对象 type 是 TEMP_TABLE (或者基于我们的MVP策略：在lumi_metadata_store中找不到的表): 它应该生成 MERGE (obj:TempTable {fqn: $fqn}) ON CREATE SET obj.name = $name, obj.schema_name = $schema, obj.database_name = $db_name, obj.is_temporary = true, ...。fqn 可以包含一个特殊的schema名如 'session_temp' 或者如果LLM能提供一个临时的schema名。
2. _generate_cypher_for_column_node:
与对象节点类似，当LLM JSON提到一个列时，此函数确保该列节点存在。
策略:
MATCH 其父对象节点 (Table, View, 或 TempTable)。
MERGE (col:Column/TempColumn {fqn: $col_fqn}) ON CREATE SET col.name = $name, ...。
MERGE (parent_obj)-[:HAS_COLUMN]->(col)。
如果父对象是 TempTable，则创建的列节点可以是 :TempColumn 标签，或者 :Column {is_temporary: true}。
3. transform_llm_json_to_cypher_batch 的编排:
在真正生成 DATA_FLOW 边之前，它应该先遍历LLM JSON中所有涉及的对象和列（来自 target_object, column_level_lineage[].sources[].source_object, column_level_lineage[].target_column, referenced_objects），调用 _generate_cypher_for_object_node 和 _generate_cypher_for_column_node 来确保所有需要被血缘关系连接的端点节点都已在图中MERGE完毕。
这样做的好处是，即使LLM提到的某些对象是临时的或尚未被 metadata_graph_builder 处理，我们也能为它们创建占位符节点，使得 DATA_FLOW 边能够成功创建。
4. _generate_cypher_for_sql_pattern_node: 为当前处理的SQL模式 MERGE 一个 :SqlPattern 节点。属性包括 sql_hash（唯一标识）、normalized_sql、sample_sql、source_database_name、以及从 sql_patterns 表获取的统计信息如 first_seen_at, last_seen_at, execution_count等。使用 ON CREATE SET 和 ON MATCH SET 来处理属性的初始化和更新。
5. _generate_cypher_for_data_flow:
遍历LLM JSON中的 column_level_lineage。
在真正生成 DATA_FLOW 边之前，它应该先遍历LLM JSON中所有涉及的对象和列（来自 target_object, column_level_lineage[].sources[].source_object, column_level_lineage[].target_column, referenced_objects），调用 _generate_cypher_for_object_node 和 _generate_cypher_for_column_node 来确保所有需要被血缘关系连接的端点节点都已在图中MERGE完毕。
这样做的好处是，即使LLM提到的某些对象是临时的或尚未被 metadata_graph_builder 处理，我们也能为它们创建占位符节点，使得 DATA_FLOW 边能够成功创建。
6. _generate_cypher_for_sql_object_references:
遍历LLM JSON中的 referenced_objects。
MATCH :SqlPattern 节点和被引用的 :Table 或 :View 节点。
根据 access_mode (READ/WRITE) MERGE 对应的关系，如 (sp)-[:READS_FROM]->(obj) 或 (sp)-[:WRITES_TO]->(obj)。边的属性可以包含 last_seen_at (用 pattern_info.last_seen_at 更新)。
7. FQN使用: 所有对 Column, Table, View 等元数据节点的引用都应通过其FQN（完全限定名）来 MATCH，确保准确性。FQN的生成逻辑应与 metadata_graph_builder 保持一致。
返回: transform_llm_json_to_cypher_batch 返回一个 List[Tuple[str, Dict[str, Any]]]，每个元组包含一条Cypher语句和其对应的参数字典。
### 4 执行Cypher (common_graph_utils.execute_cypher):
这个共享函数负责实际执行Cypher。它需要处理AGE的上下文设置（LOAD 'age'; SET search_path ...; SELECT ag_catalog.set_graph_path(...);）。
强烈推荐使用参数化查询 来执行Cypher，以防止注入并提高效率。AGE的 cypher() 函数支持将一个JSONB对象作为参数，Cypher语句中可以用 $key 的形式引用JSONB中的键。
### 5 状态更新 (mark_pattern_as_loaded_to_age):
在成功（或失败）将一个SQL模式的血缘关系写入AGE后，更新 lumi_analytics.sql_patterns 表中对应记录的 is_loaded_to_age 和 age_load_error_message 字段。
### 6 主流程 (build_lineage_graphs):
编排整个过程：获取待处理SQL模式 -> 转换JSON为Cypher批次 -> 在事务中执行Cypher批次 -> 更新状态。
事务性: 对一个SQL模式生成的所有Cypher语句，应该在一个AGE（即PostgreSQL）事务中执行，以保证原子性。
### 7 main() 函数示例: 提供了一个如何初始化和运行 LineageGraphBuilder 的示例。