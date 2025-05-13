#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
元数据收集调度器单元测试

此模块测试元数据收集调度器的功能，包括：
1. 启动元数据收集服务
2. 处理信号和优雅关闭
3. 命令行参数解析

作者: Vance Chen
"""

import asyncio
import pytest
import signal
from unittest.mock import patch, MagicMock, AsyncMock

from pglumilineage.scheduler import metadata_collector_main
from pglumilineage.metadata_collector import service as metadata_collector_service


@pytest.fixture
def mock_db_pool():
    """模拟数据库连接池"""
    with patch('pglumilineage.common.db_utils.init_db_pool', new_callable=AsyncMock) as mock_init_pool, \
         patch('pglumilineage.common.db_utils.close_db_pool', new_callable=AsyncMock) as mock_close_pool, \
         patch('pglumilineage.common.db_utils.get_db_pool', new_callable=AsyncMock) as mock_get_pool:
        
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_pool.acquire.return_value.__aenter__.return_value = mock_conn
        mock_get_pool.return_value = mock_pool
        
        yield {
            'init_pool': mock_init_pool,
            'close_pool': mock_close_pool,
            'get_pool': mock_get_pool,
            'pool': mock_pool,
            'conn': mock_conn
        }


@pytest.fixture
def mock_metadata_service():
    """模拟元数据收集服务"""
    with patch('pglumilineage.metadata_collector.service.process_metadata_collection', new_callable=AsyncMock) as mock_process:
        mock_process.return_value = None
        yield mock_process


@pytest.mark.asyncio
async def test_start_metadata_collector(mock_metadata_service):
    """测试启动元数据收集服务"""
    # 调用函数
    task = await metadata_collector_main.start_metadata_collector(interval_seconds=100, run_once=True)
    
    # 验证结果
    assert isinstance(task, asyncio.Task)
    assert task.get_name() == "metadata_collector"
    
    # 验证服务调用
    mock_metadata_service.assert_called_once_with(interval_seconds=100, run_once=True)
    
    # 清理
    if not task.done():
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@pytest.mark.asyncio
async def test_shutdown(mock_db_pool):
    """测试优雅关闭"""
    # 创建一个真正的协程任务
    async def dummy_task():
        return "test_complete"
    
    task = asyncio.create_task(dummy_task())
    task.set_name("test_task")
    
    # 先清空全局任务列表，然后设置
    metadata_collector_main.tasks = []
    metadata_collector_main.tasks.append(task)
    
    # 调用函数
    await metadata_collector_main.shutdown(signal.SIGINT)
    
    # 验证结果
    assert task.cancelled()
    mock_db_pool['close_pool'].assert_called_once()


@pytest.mark.asyncio
async def test_main_run_once(mock_db_pool, mock_metadata_service):
    """测试主函数（单次运行模式）"""
    # 模拟命令行参数
    with patch('argparse.ArgumentParser.parse_args', return_value=MagicMock(interval=100, run_once=True)):
        # 模拟asyncio.gather
        with patch('asyncio.gather', new_callable=AsyncMock) as mock_gather:
            # 清空全局任务列表
            metadata_collector_main.tasks = []
            
            # 调用函数
            await metadata_collector_main.main()
            
            # 验证结果
            mock_db_pool['init_pool'].assert_called_once()
            mock_gather.assert_called_once()
            assert len(metadata_collector_main.tasks) > 0  # 只验证有任务被添加


@pytest.mark.asyncio
async def test_main_continuous_run(mock_db_pool, mock_metadata_service):
    """测试主函数（持续运行模式）"""
    # 模拟命令行参数
    with patch('argparse.ArgumentParser.parse_args', return_value=MagicMock(interval=100, run_once=False)):
        # 模拟asyncio.gather抛出CancelledError
        with patch('asyncio.gather', new_callable=AsyncMock) as mock_gather:
            mock_gather.side_effect = asyncio.CancelledError()
            
            # 清空全局任务列表
            metadata_collector_main.tasks = []
            
            # 调用函数
            await metadata_collector_main.main()
            
            # 验证结果
            mock_db_pool['init_pool'].assert_called_once()
            mock_gather.assert_called_once()
            assert len(metadata_collector_main.tasks) > 0  # 只验证有任务被添加


if __name__ == "__main__":
    pytest.main(["-v", __file__])
