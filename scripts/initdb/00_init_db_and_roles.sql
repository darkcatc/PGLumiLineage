-- ============================================================================
-- PGLumiLineage 数据库初始化脚本
-- 
-- 此脚本用于初始化PGLumiLineage项目的PostgreSQL环境，包括创建数据库、角色和设置权限。
-- 脚本设计为幂等的，可以多次执行而不会产生错误。
--
-- 作者: Vance Chen
-- ============================================================================

-- 连接到默认的postgres数据库以执行管理操作
\connect postgres

-- ----------------------------------------------------------------------------
-- 1. 创建数据库 iwdb (如果它不存在)
-- ----------------------------------------------------------------------------
DO $$
BEGIN
    -- 检查数据库是否已存在
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'iwdb') THEN
        -- 创建数据库
        CREATE DATABASE iwdb OWNER postgres;
        RAISE NOTICE '数据库 iwdb 已创建';
    ELSE
        RAISE NOTICE '数据库 iwdb 已存在，跳过创建';
    END IF;
END
$$;

-- ----------------------------------------------------------------------------
-- 2. 创建角色 lumiadmin (如果它不存在)
-- ----------------------------------------------------------------------------
DO $$
BEGIN
    -- 检查角色是否已存在
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lumiadmin') THEN
        -- 创建角色
        -- 注意：在生产环境中应替换为实际的强密码
        CREATE ROLE lumiadmin WITH LOGIN PASSWORD 'lumiadmin';
        RAISE NOTICE '角色 lumiadmin 已创建';
    ELSE
        RAISE NOTICE '角色 lumiadmin 已存在，跳过创建';
        -- 注意：此脚本不会修改现有角色的密码
        -- 如需修改密码，请手动执行：ALTER ROLE lumiadmin WITH PASSWORD 'new_password';
    END IF;
END
$$;

-- ----------------------------------------------------------------------------
-- 3. 创建角色 lumiuser (如果它不存在)
-- ----------------------------------------------------------------------------
DO $$
BEGIN
    -- 检查角色是否已存在
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'lumiuser') THEN
        -- 创建角色
        -- 注意：在生产环境中应替换为实际的强密码
        CREATE ROLE lumiuser WITH LOGIN PASSWORD 'lumiuser';
        RAISE NOTICE '角色 lumiuser 已创建';
    ELSE
        RAISE NOTICE '角色 lumiuser 已存在，跳过创建';
        -- 注意：此脚本不会修改现有角色的密码
        -- 如需修改密码，请手动执行：ALTER ROLE lumiuser WITH PASSWORD 'new_password';
    END IF;
END
$$;

-- ----------------------------------------------------------------------------
-- 4. 授予 lumiadmin 连接到 iwdb 数据库的权限
-- ----------------------------------------------------------------------------
-- 注意：GRANT语句本身是幂等的，可以多次执行
GRANT CONNECT ON DATABASE iwdb TO lumiadmin;

-- ----------------------------------------------------------------------------
-- 5. 授予 lumiadmin 在 iwdb 数据库中创建 schema 的权限
-- ----------------------------------------------------------------------------
GRANT CREATE ON DATABASE iwdb TO lumiadmin;

-- ----------------------------------------------------------------------------
-- 6. 授予 lumiuser 连接到 iwdb 数据库的权限
-- ----------------------------------------------------------------------------
GRANT CONNECT ON DATABASE iwdb TO lumiuser;

-- ----------------------------------------------------------------------------
-- 7. 在 iwdb 数据库中修改 public schema 的权限
-- ----------------------------------------------------------------------------
-- 连接到iwdb数据库以执行schema权限操作
\connect iwdb

-- 撤销 PUBLIC 角色在 public schema 上的默认 CREATE 权限
-- 这是一个安全最佳实践，防止任何连接用户都能在public schema中创建对象
REVOKE CREATE ON SCHEMA public FROM PUBLIC;

-- 授予 USAGE 权限给 lumiadmin 和 lumiuser
-- 这允许这些角色看到public schema中的对象并使用它们（如果有适当的权限）
GRANT USAGE ON SCHEMA public TO lumiadmin, lumiuser;

-- 授予 lumiadmin 在 public schema 上的 CREATE 权限
-- 这允许 lumiadmin 在 public schema 中创建对象
GRANT CREATE ON SCHEMA public TO lumiadmin;

-- ----------------------------------------------------------------------------
-- 8. 在 iwdb 数据库中设置默认的 search_path
-- ----------------------------------------------------------------------------
ALTER DATABASE iwdb SET search_path = ag_catalog, public;
-- ----------------------------------------------------------------------------
-- 完成初始化
-- ----------------------------------------------------------------------------
\echo '数据库初始化完成'
\echo '注意：请确保在生产环境中使用安全的密码替换脚本中的示例密码'
