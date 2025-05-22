"""
测试配置文件，包含测试用的fixture
"""
import pytest
import asyncio
from unittest.mock import Mock, AsyncMock, patch
from datetime import datetime
from pglumilineage.graph_builder.metadata_graph_builder import MetadataGraphBuilder

@pytest.fixture
def event_loop():
    """为异步测试创建事件循环"""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def mock_metadata_db_config():
    """模拟元数据数据库配置"""
    return {
        'host': 'localhost',
        'port': 5432,
        'user': 'test_user',
        'password': 'test_password',
        'database': 'test_db'
    }

@pytest.fixture
def mock_age_db_config():
    """模拟AGE图数据库配置"""
    return {
        'host': 'localhost',
        'port': 5432,
        'user': 'test_user',
        'password': 'test_password',
        'database': 'test_db'
    }

@pytest.fixture
def sample_source_data():
    """测试用的数据源数据"""
    return [
        {
            'source_id': 1,
            'source_name': 'test_source',
            'host': 'localhost',
            'port': 5432,
            'username': 'test_user',
            'database': 'test_db',
            'description': 'Test source',
            'is_active': True,
            'properties': {}
        }
    ]

@pytest.fixture
def sample_objects_metadata():
    """测试用的对象元数据"""
    return [
        {
            'object_id': 1,
            'source_id': 1,
            'database_name': 'test_db',
            'schema_name': 'public',
            'object_name': 'test_table',
            'object_type': 'table',
            'owner': 'postgres',
            'description': 'Test table',
            'definition': 'CREATE TABLE public.test_table (id int, name text)',
            'row_count': 100,
            'last_analyzed': datetime(2023, 1, 1),
            'properties': {}
        }
    ]

@pytest.fixture
def sample_columns_metadata():
    """测试用的列元数据"""
    return [
        {
            'column_id': 1,
            'object_id': 1,
            'column_name': 'id',
            'ordinal_position': 1,
            'data_type': 'integer',
            'is_nullable': False,
            'is_primary_key': True,
            'is_unique': True,
            'description': 'Primary key',
            'properties': {}
        },
        {
            'column_id': 2,
            'object_id': 1,
            'column_name': 'name',
            'ordinal_position': 2,
            'data_type': 'text',
            'is_nullable': True,
            'is_primary_key': False,
            'is_unique': False,
            'description': 'User name',
            'properties': {}
        }
    ]

@pytest.fixture
def sample_functions_metadata():
    """测试用的函数元数据"""
    return [
        {
            'function_id': 1,
            'source_id': 1,
            'database_name': 'test_db',
            'schema_name': 'public',
            'function_name': 'test_function',
            'function_type': 'FUNCTION',
            'return_type': 'integer',
            'parameters': 'p_id integer',
            'parameter_types': ['integer'],
            'definition': 'CREATE OR REPLACE FUNCTION public.test_function(p_id integer) RETURNS integer',
            'language': 'plpgsql',
            'owner': 'postgres',
            'description': 'Test function',
            'properties': {}
        }
    ]

@pytest.fixture
def mock_metadata_graph_builder(mock_metadata_db_config, mock_age_db_config):
    """创建并返回一个模拟的MetadataGraphBuilder实例"""
    with patch('asyncpg.connect', new_callable=AsyncMock) as mock_connect:
        builder = MetadataGraphBuilder(mock_metadata_db_config, mock_age_db_config)
        # 模拟数据库连接
        mock_conn = AsyncMock()
        mock_connect.return_value = mock_conn
        # 模拟游标
        mock_conn.fetch = AsyncMock()
        mock_conn.fetchrow = AsyncMock()
        mock_conn.execute = AsyncMock()
        
        yield builder, mock_connect, mock_conn
