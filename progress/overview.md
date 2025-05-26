# PGLumiLineage 项目进度与设计决策追溯系统

**最后更新时间**：2025-05-26

## 项目当前状态

PGLumiLineage 是一个基于 PostgreSQL 的自动化数据血缘分析与知识图谱构建平台，目前处于活跃开发阶段。该项目利用大语言模型（LLM）和图数据库（Apache AGE）技术，从 PostgreSQL 的运行时日志及元数据中自动提取、分析并可视化数据血缘关系。

### 当前已实现的核心功能

- 自动化日志采集与处理
- 全面元数据收集
- SQL 范式化与模式聚合
- LLM 驱动的关系提取
- **知识图谱构建 (基于 Apache AGE 1.5.0)**：
    - 完成了对 Apache AGE 1.5.0 版本的核心适配，包括其特定的配置要求、Cypher 查询语法（如节点标签表示）以及数据返回格式。
    - **针对 AGE 1.5.0 不支持 `MERGE ... ON CREATE SET ...` 语法的问题，在 `graph_builder` 模块中实现了替代逻辑，确保节点和关系的正确创建与更新。**
- 灵活配置与调度系统
- **血缘关系可视化API接口 (FastAPI)**：
    - 后端API已适配Apache AGE 1.5.0的数据返回格式，能够正确解析和转换图数据。
- **血缘关系可视化UI (Vue3 + AntV G6)**：
    - 前端可视化界面已实现核心功能，包括：
      - 优化的血缘关系展示，使用自定义边类型实现优雅的曲线效果
      - 源端节点（表和字段）同级展示，提升血缘关系可读性
      - 支持多种布局算法和交互方式
      - 实现了基础的数据血缘导航和分析功能
    - 正在持续优化用户体验和性能表现

### 正在进行的工作

- **前端可视化模块功能完善与用户体验优化 (基于Vue3, AntV G6)**：
    - 优化血缘关系的曲线展示和节点布局
    - 实现源端节点同级展示，提升血缘关系可读性
    - 添加更多交互功能，如节点搜索、路径高亮等
    - 优化大规模图谱的渲染性能
- LLM 分析器提示词优化，提高数据血缘分析的准确性。
- 临时表处理逻辑完善。
- 持续完善进度日志与设计决策追溯系统。
- 后端API针对AGE数据解析的持续优化和健壮性提升。

## 主要模块索引

以下是项目的主要模块，点击链接可查看每个模块的详细进度与设计决策：

- [数据源配置 (lumi_config)](./lumi_config_decisions.md)
- [日志处理器 (log_processor)](./log_processor_decisions.md)
- [元数据收集器 (metadata_collector)](./metadata_collector_decisions.md)
- [SQL范式化与聚合器 (sql_normalizer)](./sql_normalizer_decisions.md)
- [LLM分析器 (llm_analyzer)](./llm_analyzer_decisions.md)
- [AGE图谱构建器 (graph_builder)](./age_graph_decisions.md)
- [血缘关系 API (api)](./api_decisions.md)
- [血缘关系可视化 UI (frontend)](./ui_decisions.md)
- [调度与编排服务 (scheduler)](./scheduler_decisions.md)
- [通用模块与配置 (common)](./common_decisions.md)
- [数据库设计 (iwdb_schemas)](./database_design_decisions.md)

## 全局设计原则与重要决策

### 技术栈选择

1. **Python 与 asyncio**：
   - 选择 Python 3.10+ 作为主要开发语言，利用其丰富的数据处理库和简洁的语法。
   - 采用 asyncio 进行异步处理，提高系统处理大量 I/O 操作的效率。
   - 使用 asyncpg 作为 PostgreSQL 异步客户端，实现高性能数据库交互。

2. **PostgreSQL 与 Apache AGE**：
   - 选择 PostgreSQL 15+ 作为中央存储系统，利用其稳定性和强大的扩展生态。
   - **采用 Apache AGE 1.5.0 作为图数据库扩展，并完成了针对该版本特性的适配工作。**
   - 未来计划支持基于 PG 内核的 MPP 数据库 Cloudberry，以满足大规模生产环境需求。

3. **LLM 技术选型**：
   - 初期集成阿里云通义千问（Qwen）系列，通过 OpenAI 兼容 API 调用。
   - 选择 Qwen 的原因是其在中文语境下的优秀表现和对结构化数据处理的能力。
   - 设计了可扩展的接口，便于未来集成其他 LLM 模型。

### 架构设计原则
(...内容保持不变...)

### 跨模块重要决策
(...内容保持不变...)

## 项目演进路线图

### 短期目标（1-3个月）

- **稳定并优化前后端集成，确保血缘图的准确、高效展示和基本交互功能完善。**
- 完善 LLM 分析器，提高数据血缘分析的准确性和覆盖率，特别是针对复杂SQL和特定业务场景。
- 增强对复杂 SQL 结构（如 UNION、子查询、CTE）的处理能力。
- 改进临时表和未知表的处理逻辑。
- 持续完善进度日志与设计决策追溯系统。
- **优化AGE图数据库的查询性能和数据模型，特别是针对大规模图谱的构建和查询效率。**

### 中期目标（3-6个月）
(...内容保持不变...)

### 长期目标（6个月以上）
(...内容保持不变...)
