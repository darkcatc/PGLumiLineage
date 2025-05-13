#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
元数据收集服务单元测试

此模块测试元数据收集服务的功能，包括：
1. 数据源同步调度规则获取和缓存
2. 元数据收集处理
3. 批量处理和缓存机制

作者: Vance Chen
"""

import asyncio
import pytest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch, MagicMock, AsyncMock, call

from pglumilineage.metadata_collector import service
from pglumilineage.common import models


@pytest.fixture
def reset_caches():
    """重置测试前的缓存"""
    # 保存原始缓存
    original_schedules_cache = service._schedules_cache.copy()
    
    # 重置缓存
    service._schedules_cache["data"] = None
    service._schedules_cache["timestamp"] = None
    
    # 重置从数据库获取调度规则的LRU缓存
    if hasattr(service.get_metadata_sync_schedules_from_db, 'cache_clear'):
        service.get_metadata_sync_schedules_from_db.cache_clear()
    
    yield
    
    # 恢复原始缓存
    service._schedules_cache = original_schedules_cache


@pytest.fixture
def mock_db_pool():
    """模拟数据库连接池"""
    # 直接模拟 service 模块中的 get_db_pool 函数
    with patch('pglumilineage.metadata_collector.service.db_utils.get_db_pool') as mock_get_pool:
        # 创建一个正确的异步上下文管理器类
        class AsyncContextManager:
            async def __aenter__(self):
                return mock_conn
                
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                pass
        
        # 创建模拟对象
        mock_pool = MagicMock()
        mock_conn = MagicMock()
        
        # 设置模拟对象的返回值
        mock_conn.fetch = AsyncMock()
        mock_conn.execute = AsyncMock()
        
        # 设置 acquire 方法返回异步上下文管理器
        mock_pool.acquire.return_value = AsyncContextManager()
        
        # 设置 get_db_pool 函数的返回值
        mock_get_pool.return_value = mock_pool
        
        yield {
            'get_pool': mock_get_pool,
            'pool': mock_pool,
            'conn': mock_conn
        }


@pytest.fixture
def mock_source_db_connection():
    """模拟源数据库连接"""
    with patch('pglumilineage.metadata_collector.service.get_source_db_connection', new_callable=AsyncMock) as mock_get_conn:
        mock_conn = AsyncMock()
        mock_get_conn.return_value = mock_conn
        
        yield {
            'get_conn': mock_get_conn,
            'conn': mock_conn
        }


@pytest.mark.asyncio
async def test_get_metadata_sync_schedules_cache(mock_db_pool, reset_caches):
    """测试获取元数据同步调度规则的缓存机制"""
    # 模拟数据库返回结果
    mock_rows = [
        {
            'schedule_id': 1,
            'source_id': 1,
            'is_schedule_active': True,
            'sync_frequency_type': 'interval',
            'sync_interval_seconds': 86400,
            'cron_expression': None,
            'last_sync_attempt_at': None,
            'last_sync_success_at': None,
            'last_sync_status': None,
            'last_sync_message': None,
            'created_at': datetime.now(),
            'updated_at': datetime.now(),
            'source_name': 'test_source',
            'db_host': 'localhost',
            'db_port': 5432,
            'db_name': 'test_db',
            'db_user': 'test_user',
            'db_password': 'test_password'
        }
    ]
    
    # 使用直接模拟函数的方式，而不是依赖于异步上下文管理器
    with patch('pglumilineage.metadata_collector.service.get_metadata_sync_schedules_from_db', new_callable=AsyncMock) as mock_get_from_db:
        # 设置模拟函数的返回值
        mock_get_from_db.return_value = mock_rows
        
        # 第一次调用，应该查询数据库
        schedules1 = await service.get_metadata_sync_schedules()
        
        # 验证数据库查询
        mock_get_from_db.assert_called_once()
        assert len(schedules1) == 1
        assert schedules1[0]['schedule_id'] == 1
        
        # 重置模拟对象
        mock_get_from_db.reset_mock()
        
        # 第二次调用，应该使用缓存
        schedules2 = await service.get_metadata_sync_schedules()
        
        # 验证没有再次查询数据库
        mock_get_from_db.assert_not_called()
        assert len(schedules2) == 1
        assert schedules2[0]['schedule_id'] == 1
        
        # 模拟缓存过期
        service._schedules_cache["timestamp"] = datetime.now() - timedelta(seconds=service.SCHEDULES_CACHE_TTL + 10)
        
        # 第三次调用，应该再次查询数据库
        schedules3 = await service.get_metadata_sync_schedules()
        
        # 验证再次查询数据库
        mock_get_from_db.assert_called_once()
        assert len(schedules3) == 1


@pytest.mark.asyncio
async def test_update_schedule_sync_status(mock_db_pool, reset_caches):
    """测试更新调度规则同步状态"""
    # 设置缓存
    service._schedules_cache["data"] = {"test_source": {"schedule_id": 1}}
    service._schedules_cache["timestamp"] = datetime.now()
    
    # 调用函数
    await service.update_schedule_sync_status(1, True, "测试成功")
    
    # 验证数据库操作
    mock_db_pool['conn'].execute.assert_called_once()
    
    # 验证缓存被清除
    assert service._schedules_cache["data"] is None
    assert service._schedules_cache["timestamp"] is None


@pytest.mark.asyncio
async def test_calculate_next_run_time():
    """测试计算下次运行时间"""
    now = datetime.now(timezone.utc)
    
    # 测试间隔模式
    next_time = await service.calculate_next_run_time('interval', 3600, None, now)
    assert next_time == now + timedelta(seconds=3600)
    
    # 测试cron模式
    next_time = await service.calculate_next_run_time('cron', None, '0 0 * * *', now)
    assert next_time > now
    
    # 测试手动模式
    next_time = await service.calculate_next_run_time('manual', None, None, now)
    assert next_time > now + timedelta(days=364)
    
    # 测试未知模式
    next_time = await service.calculate_next_run_time('unknown', None, None, now)
    assert next_time == now + timedelta(days=1)


@pytest.mark.asyncio
async def test_process_metadata_collection(reset_caches):
    """测试处理元数据收集"""
    # 模拟get_metadata_sync_schedules
    mock_schedules = [
        {
            'schedule_id': 1,
            'source_id': 1,
            'is_schedule_active': True,
            'sync_frequency_type': 'interval',
            'sync_interval_seconds': 86400,
            'cron_expression': None,
            'last_sync_success_at': datetime.now(timezone.utc) - timedelta(days=2),
            'source_config': models.DataSourceConfig(
                source_id=1,
                source_name='test_source',
                host='localhost',
                port=5432,
                username='test_user',
                password=models.SecretStr('test_password'),
                database='test_db'
            )
        }
    ]
    
    # 模拟函数
    with patch('pglumilineage.metadata_collector.service.get_metadata_sync_schedules', new_callable=AsyncMock) as mock_get_schedules, \
         patch('pglumilineage.metadata_collector.service.process_single_source', new_callable=AsyncMock) as mock_process_source, \
         patch('asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
        
        # 设置模拟返回值
        mock_get_schedules.return_value = mock_schedules
        mock_process_source.return_value = None
        
        # 设置sleep抛出异常以结束循环
        mock_sleep.side_effect = [None, asyncio.CancelledError()]
        
        # 调用函数
        with pytest.raises(asyncio.CancelledError):
            await service.process_metadata_collection(interval_seconds=100, run_once=False)
        
        # 验证函数调用
        mock_get_schedules.assert_called()
        mock_process_source.assert_called_with(1, mock_schedules[0]['source_config'])
        assert mock_sleep.call_count == 2


@pytest.mark.asyncio
async def test_process_single_source():
    """测试处理单个数据源的元数据收集"""
    # 创建测试数据
    schedule_id = 1
    source_config = models.DataSourceConfig(
        source_id=1,
        source_name='test_source',
        host='localhost',
        port=5432,
        username='test_user',
        password=models.SecretStr('test_password'),
        database='test_db'
    )
    
    # 模拟函数
    with patch('pglumilineage.metadata_collector.service.collect_metadata_for_source', new_callable=AsyncMock) as mock_collect, \
         patch('pglumilineage.metadata_collector.service.update_schedule_sync_status', new_callable=AsyncMock) as mock_update:
        
        # 设置模拟返回值 - 成功情况
        mock_collect.return_value = (True, "")
        
        # 调用函数
        await service.process_single_source(schedule_id, source_config)
        
        # 验证函数调用
        mock_collect.assert_called_once_with(source_config)
        mock_update.assert_called_once_with(schedule_id, True, "元数据收集成功")
        
        # 重置模拟对象
        mock_collect.reset_mock()
        mock_update.reset_mock()
        
        # 设置模拟返回值 - 失败情况
        mock_collect.return_value = (False, "测试错误")
        
        # 调用函数
        await service.process_single_source(schedule_id, source_config)
        
        # 验证函数调用
        mock_collect.assert_called_once_with(source_config)
        mock_update.assert_called_once_with(schedule_id, False, "元数据收集失败: 测试错误")
        
        # 重置模拟对象
        mock_collect.reset_mock()
        mock_update.reset_mock()
        
        # 设置模拟返回值 - 异常情况
        mock_collect.side_effect = Exception("测试异常")
        
        # 调用函数
        await service.process_single_source(schedule_id, source_config)
        
        # 验证函数调用
        mock_collect.assert_called_once_with(source_config)
        mock_update.assert_called_once()
        assert "测试异常" in mock_update.call_args[0][2]


if __name__ == "__main__":
    pytest.main(["-v", __file__])
