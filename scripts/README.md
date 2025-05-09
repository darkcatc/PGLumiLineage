# PGLumiLineage 脚本使用说明

本目录包含 PGLumiLineage 项目的各种实用脚本，用于数据库初始化、数据处理等操作。

## 数据库初始化脚本 (setup_db.py)

`setup_db.py` 脚本用于初始化 PGLumiLineage 所需的数据库环境，包括创建数据库、角色、模式和表结构。

### 前提条件

- Python 3.8+
- PostgreSQL 13+
- 已安装 `psql` 命令行工具并添加到 PATH
- 已安装项目依赖 (`pip install -r requirements.txt`)

### 使用方法

```bash
# 基本用法（使用默认配置）
python scripts/setup_db.py

# 指定配置文件
python scripts/setup_db.py --config /path/to/custom/settings.toml

# 指定超级用户和密码
python scripts/setup_db.py --superuser postgres --password your_password

# 指定 lumiadmin 用户密码
python scripts/setup_db.py --admin-password your_admin_password

# 指定主机和端口
python scripts/setup_db.py --host localhost --port 5432

# 跳过数据库和角色创建，只创建表结构
python scripts/setup_db.py --skip-db-creation

# 显示详细输出
python scripts/setup_db.py --verbose
```

### 参数说明

| 参数 | 简写 | 说明 | 默认值 |
|------|------|------|--------|
| `--config` | `-c` | 配置文件路径 | `config/settings.toml` |
| `--superuser` | `-u` | PostgreSQL 超级用户名 | `postgres` |
| `--password` | `-p` | PostgreSQL 超级用户密码 | 无（使用环境变量或提示输入） |
| `--admin-password` | `-a` | `lumiadmin` 用户密码 | 与超级用户密码相同 |
| `--host` | 无 | PostgreSQL 主机地址 | 配置文件中的值 |
| `--port` | 无 | PostgreSQL 端口 | 配置文件中的值 |
| `--skip-db-creation` | 无 | 跳过数据库和角色创建步骤 | `False` |
| `--verbose` | `-v` | 显示详细输出 | `False` |

### 配置优先级

1. 命令行参数（最高优先级）
2. 指定的配置文件（`--config` 参数）
3. 环境变量（`.env` 文件）

### 执行流程

1. 使用超级用户执行 `00_init_db_and_roles.sql`，创建数据库和角色
2. 使用 `lumiadmin` 用户执行 `01_setup_schemas_and_tables.sql`，创建模式和表结构

### 注意事项

- 脚本设计为幂等的，可以多次执行而不会产生错误
- 如果已经创建了数据库和角色，可以使用 `--skip-db-creation` 参数跳过第一步
- 密码可以通过命令行参数、环境变量 `PGPASSWORD` 或配置文件提供
- 如果遇到权限问题，请确保使用的超级用户具有创建数据库和角色的权限
