# PGLumiLineage

PGLumiLineage 是一个用于从 PostgreSQL 日志分析 SQL 血缘关系的工具，结合了 LLM 和 Apache AGE 图数据库技术。

## 功能特点

- 从 PostgreSQL 日志中提取 SQL 查询
- 使用 LLM 分析 SQL 查询之间的关系
- 构建数据血缘图谱
- 通过 Apache AGE 图数据库存储和查询血缘关系

## 技术栈

- Python 3.10+
- PostgreSQL
- Apache AGE 图数据库
- 异步数据库操作: asyncpg
- SQL 解析: sqlglot
- LLM 交互: OpenAI API
- 配置管理: Pydantic

## 安装

1. 克隆仓库
2. 创建虚拟环境
   ```bash
   python3 -m venv lumiEnv
   source lumiEnv/bin/activate  # Linux/Mac
   # 或
   lumiEnv\Scripts\activate  # Windows
   ```
3. 安装依赖
   ```bash
   pip install -r requirements.txt
   ```
4. 复制环境变量模板并配置
   ```bash
   cp .env.example .env
   # 编辑 .env 文件，填入实际配置
   ```

## 使用方法

待补充

## 开发者

- Vance Chen

## 许可证

待定
