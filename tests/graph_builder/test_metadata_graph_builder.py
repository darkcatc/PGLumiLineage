"""
测试元数据图谱构建模块
"""
import pytest
import asyncio
from unittest.mock import AsyncMock, patch, MagicMock, call
from datetime import datetime

from pglumilineage.graph_builder.metadata_graph_builder import MetadataGraphBuilder

# 测试获取激活的数据源
@pytest.mark.asyncio
async def test_get_active_data_sources(mock_metadata_graph_builder, sample_source_data):
    """测试获取激活的数据源"""
    builder, mock_connect, mock_conn = mock_metadata_graph_builder
    
    # 模拟数据库返回
    mock_conn.fetch.return_value = sample_source_data
    
    # 调用方法
    result = await builder.get_active_data_sources()
    
    # 验证结果
    assert len(result) == 1
    assert result[0]['source_name'] == 'test_source'
    assert result[0]['is_active'] is True
    
    # 验证SQL查询
    mock_conn.fetch.assert_awaited_once()
    args, _ = mock_conn.fetch.call_args
    assert "SELECT" in args[0] and "FROM lumi_config.data_sources" in args[0]
    assert "WHERE is_active = TRUE" in args[0]

# 测试获取对象元数据
@pytest.mark.asyncio
async def test_get_objects_metadata(mock_metadata_graph_builder, sample_objects_metadata):
    """测试获取对象元数据"""
    builder, mock_connect, mock_conn = mock_metadata_graph_builder
    
    # 模拟数据库返回
    mock_conn.fetch.return_value = sample_objects_metadata
    
    # 调用方法
    result = await builder.get_objects_metadata(source_id=1)
    
    # 验证结果
    assert len(result) == 1
    assert result[0]['object_name'] == 'test_table'
    assert result[0]['object_type'] == 'table'
    
    # 验证SQL查询
    mock_conn.fetch.assert_awaited_once()
    args, kwargs = mock_conn.fetch.call_args
    assert "SELECT" in args[0] and "FROM lumi_metadata_store.objects_metadata" in args[0]
    assert kwargs['source_id'] == 1

# 测试获取列元数据
@pytest.mark.asyncio
async def test_get_columns_metadata(mock_metadata_graph_builder, sample_columns_metadata):
    """测试获取列元数据"""
    builder, mock_connect, mock_conn = mock_metadata_graph_builder
    
    # 模拟数据库返回
    mock_conn.fetch.return_value = sample_columns_metadata
    
    # 调用方法
    object_ids = [1]
    result = await builder.get_columns_metadata(object_ids)
    
    # 验证结果
    assert len(result) == 2
    assert result[0]['column_name'] == 'id'
    assert result[1]['column_name'] == 'name'
    assert result[0]['is_primary_key'] is True
    
    # 验证SQL查询
    mock_conn.fetch.assert_awaited_once()
    args, kwargs = mock_conn.fetch.call_args
    assert "SELECT" in args[0] and "FROM lumi_metadata_store.columns_metadata" in args[0]
    assert kwargs['object_ids'] == object_ids

# 测试获取函数元数据
@pytest.mark.asyncio
async def test_get_functions_metadata(mock_metadata_graph_builder, sample_functions_metadata):
    """测试获取函数元数据"""
    builder, mock_connect, mock_conn = mock_metadata_graph_builder
    
    # 模拟数据库返回
    mock_conn.fetch.return_value = sample_functions_metadata
    
    # 调用方法
    source_id = 1
    result = await builder.get_functions_metadata(source_id)
    
    # 验证结果
    assert len(result) == 1
    assert result[0]['function_name'] == 'test_function'
    assert result[0]['return_type'] == 'integer'
    
    # 验证SQL查询
    mock_conn.fetch.assert_awaited_once()
    args, kwargs = mock_conn.fetch.call_args
    assert "SELECT" in args[0] and "FROM lumi_metadata_store.functions_metadata" in args[0]
    assert kwargs['source_id'] == source_id

# 测试生成数据源节点Cypher
@pytest.mark.asyncio
async def test_generate_datasource_node_cypher():
    """测试生成数据源节点的Cypher语句"""
    # 创建构建器实例
    builder = MetadataGraphBuilder(
        metadata_db_config={},
        age_db_config={},
        graph_name="test_graph"
    )
    
    # 测试数据
    source = {
        'source_id': 1,
        'source_name': 'test_source',
        'host': 'localhost',
        'port': 5432,
        'description': 'Test source',
        'is_active': True
    }
    
    # 调用方法
    cypher, params = builder.generate_datasource_node_cypher(source)
    
    # 验证结果
    assert 'MERGE (ds {label: "datasource", fqn: $fqn})' in cypher
    assert 'SET ds.name = $name' in cypher
    assert 'ds.source_id = $source_id' in cypher
    assert params['name'] == 'test_source'
    assert params['source_id'] == 1
    assert params['host'] == 'localhost'

# 测试执行Cypher语句
@pytest.mark.asyncio
async def test_execute_cypher(mock_metadata_graph_builder):
    """测试执行Cypher语句"""
    builder, mock_connect, mock_conn = mock_metadata_graph_builder
    
    # 模拟数据库返回
    expected_result = [{'result': 'success'}]
    mock_conn.fetch.return_value = expected_result
    
    # 调用方法
    cypher = "MATCH (n) RETURN n"
    result = await builder.execute_cypher(cypher)
    
    # 验证结果
    assert result == expected_result
    
    # 验证连接和查询
    mock_connect.assert_awaited_once()
    mock_conn.fetch.assert_awaited_once_with(cypher, None)
    mock_conn.close.assert_awaited_once()

# 测试生成数据库节点Cypher
@pytest.mark.asyncio
async def test_generate_database_node_cypher():
    """测试生成数据库节点的Cypher语句"""
    # 创建构建器实例
    builder = MetadataGraphBuilder(
        metadata_db_config={},
        age_db_config={},
        graph_name="test_graph"
    )
    
    # 调用方法
    source_name = "test_source"
    database_name = "test_db"
    source_id = 1
    
    cypher, params = builder.generate_database_node_cypher(source_name, database_name, source_id)
    
    # 验证结果
    assert 'MERGE (db {label: "database", fqn: $fqn})' in cypher
    assert 'MATCH (ds {label: "datasource", fqn: $datasource_fqn})' in cypher
    assert params['name'] == database_name
    assert params['datasource_name'] == source_name
    assert params['source_id'] == source_id

# 测试生成模式节点Cypher
@pytest.mark.asyncio
async def test_generate_schema_node_cypher():
    """测试生成模式节点的Cypher语句"""
    # 创建构建器实例
    builder = MetadataGraphBuilder(
        metadata_db_config={},
        age_db_config={},
        graph_name="test_graph"
    )
    
    # 调用方法
    database_fqn = "test_source.test_db"
    schema_name = "public"
    owner = "postgres"
    
    cypher, params = builder.generate_schema_node_cypher(database_fqn, schema_name, owner)
    
    # 验证结果
    assert 'MERGE (schema {label: "schema", fqn: $fqn})' in cypher
    assert 'MATCH (db {label: "database", fqn: $database_fqn})' in cypher
    assert params['name'] == schema_name
    assert params['database_fqn'] == database_fqn
    assert params['owner'] == owner

# 测试生成对象节点Cypher
@pytest.mark.asyncio
async def test_generate_object_node_cypher():
    """测试生成对象节点的Cypher语句"""
    # 创建构建器实例
    builder = MetadataGraphBuilder(
        metadata_db_config={},
        age_db_config={},
        graph_name="test_graph"
    )
    
    # 测试数据
    schema_fqn = "test_source.test_db.public"
    object_info = {
        'object_name': 'test_table',
        'object_type': 'table',
        'owner': 'postgres',
        'description': 'Test table',
        'definition': 'CREATE TABLE public.test_table (id int)',
        'row_count': 100,
        'last_analyzed': datetime(2023, 1, 1)
    }
    
    # 调用方法
    cypher, params = builder.generate_object_node_cypher(schema_fqn, object_info)
    
    # 验证结果
    assert 'MERGE (obj {label: $object_type, fqn: $fqn})' in cypher
    assert 'MATCH (schema {label: "schema", fqn: $schema_fqn})' in cypher
    assert params['name'] == 'test_table'
    assert params['object_type'] == 'table'
    assert params['schema_fqn'] == schema_fqn
    assert params['row_count'] == 100

# 测试生成列节点Cypher
@pytest.mark.asyncio
async def test_generate_column_node_cypher():
    """测试生成列节点的Cypher语句"""
    # 创建构建器实例
    builder = MetadataGraphBuilder(
        metadata_db_config={},
        age_db_config={},
        graph_name="test_graph"
    )
    
    # 测试数据
    object_fqn = "test_source.test_db.public.test_table"
    column_info = {
        'column_name': 'id',
        'ordinal_position': 1,
        'data_type': 'integer',
        'is_nullable': False,
        'is_primary_key': True,
        'is_unique': True,
        'description': 'Primary key',
        'foreign_key_to_table_schema': 'public',
        'foreign_key_to_table_name': 'other_table',
        'foreign_key_to_column_name': 'id'
    }
    
    # 调用方法
    cypher, params = builder.generate_column_node_cypher(object_fqn, column_info)
    
    # 验证结果
    assert 'MERGE (col {label: "column", fqn: $fqn})' in cypher
    assert 'MATCH (obj {fqn: $parent_object_fqn})' in cypher
    assert 'MATCH (target_col {label: "column", fqn: $target_column_fqn})' in cypher
    assert params['name'] == 'id'
    assert params['parent_object_fqn'] == object_fqn
    assert params['is_primary_key'] is True
    assert params['is_unique'] is True
    assert 'target_column_fqn' in params  # 验证外键关系

# 测试完整的构建流程
@pytest.mark.asyncio
async def test_build_metadata_graph(mock_metadata_graph_builder, sample_source_data, 
                                  sample_objects_metadata, sample_columns_metadata):
    """测试完整的元数据图谱构建流程"""
    builder, mock_connect, mock_conn = mock_metadata_graph_builder
    
    # 模拟数据库返回
    mock_conn.fetch.side_effect = [
        sample_source_data,  # get_active_data_sources
        sample_objects_metadata,  # get_objects_metadata
        sample_columns_metadata,  # get_columns_metadata
        [],  # get_functions_metadata
        [{'result': 'success'}]  # execute_cypher
    ]
    
    # 调用方法
    await builder.build_metadata_graph()
    
    # 验证数据库调用
    assert mock_conn.fetch.await_count == 4  # 4个查询
    assert mock_conn.execute.await_count > 0  # 至少执行了一次Cypher
    
    # 验证执行的Cypher语句
    cypher_calls = [call[0][0] for call in mock_conn.execute.call_args_list]
    assert any('MERGE (ds {label: "datasource"' in cypher for cypher in cypher_calls)
    assert any('MERGE (db {label: "database"' in cypher for cypher in cypher_calls)
    assert any('MERGE (schema {label: "schema"' in cypher for cypher in cypher_calls)
    assert any('MERGE (obj {label: "table"' in cypher for cypher in cypher_calls)
    assert any('MERGE (col {label: "column"' in cypher for cypher in cypher_calls)
