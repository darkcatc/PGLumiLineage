-- 启用 UUID 扩展 (如果尚未启用)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 启用pgvector扩展
CREATE EXTENSION IF NOT EXISTS vector;

-- 启用Apache AGE扩展
CREATE EXTENSION IF NOT EXISTS age;

-- AGE已安装，无需额外初始化

-- 创建一个测试表，用于验证pgvector是否正常工作
CREATE TABLE IF NOT EXISTS vector_test (
    id SERIAL PRIMARY KEY,
    embedding vector(3)
);

-- 插入一些测试数据
INSERT INTO vector_test (embedding) VALUES ('[1,2,3]'), ('[4,5,6]'), ('[7,8,9]');