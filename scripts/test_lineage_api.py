#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
测试血缘图API功能

验证API能否正确返回血缘关系数据

作者: Vance Chen
"""

import asyncio
import aiohttp
import json
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def test_lineage_api():
    """测试血缘图API"""
    
    # API基础URL（假设后端运行在8000端口）
    base_url = "http://localhost:8000"
    
    # 测试参数 - 使用简短的表名
    test_params = {
        "root_node_type": "table",
        "root_node_fqn": "monthly_channel_returns_analysis_report",
        "depth": 2
    }
    
    logger.info("开始测试血缘图API...")
    logger.info(f"测试参数: {test_params}")
    
    try:
        async with aiohttp.ClientSession() as session:
            # 测试血缘图查询API
            url = f"{base_url}/api/lineage/graph"
            
            logger.info(f"请求URL: {url}")
            
            async with session.get(url, params=test_params) as response:
                logger.info(f"响应状态码: {response.status}")
                
                if response.status == 200:
                    data = await response.json()
                    
                    logger.info("✅ API调用成功!")
                    logger.info(f"返回数据结构: {type(data)}")
                    
                    if isinstance(data, dict):
                        nodes = data.get('nodes', [])
                        edges = data.get('edges', [])
                        
                        logger.info(f"节点数量: {len(nodes)}")
                        logger.info(f"边数量: {len(edges)}")
                        
                        if nodes:
                            logger.info("节点示例:")
                            for i, node in enumerate(nodes[:3]):  # 显示前3个节点
                                logger.info(f"  节点 {i+1}: {node}")
                        
                        if edges:
                            logger.info("边示例:")
                            for i, edge in enumerate(edges[:3]):  # 显示前3条边
                                logger.info(f"  边 {i+1}: {edge}")
                        
                        # 检查数据格式是否符合前端期望
                        if nodes and all('id' in node and 'type' in node and 'label' in node for node in nodes):
                            logger.info("✅ 节点数据格式正确")
                        else:
                            logger.warning("⚠️  节点数据格式可能有问题")
                        
                        if edges and all('id' in edge and 'source' in edge and 'target' in edge for edge in edges):
                            logger.info("✅ 边数据格式正确")
                        else:
                            logger.warning("⚠️  边数据格式可能有问题")
                    
                else:
                    error_text = await response.text()
                    logger.error(f"❌ API调用失败: {response.status}")
                    logger.error(f"错误信息: {error_text}")
                    
    except aiohttp.ClientConnectorError as e:
        logger.error(f"❌ 连接失败: {e}")
        logger.error("请确保后端服务正在运行在 http://localhost:8000")
    except Exception as e:
        logger.error(f"❌ 测试失败: {e}")


async def test_health_check():
    """测试健康检查API"""
    
    base_url = "http://localhost:8000"
    
    try:
        async with aiohttp.ClientSession() as session:
            # 尝试访问根路径
            async with session.get(f"{base_url}/") as response:
                logger.info(f"健康检查响应状态: {response.status}")
                if response.status == 200:
                    logger.info("✅ 后端服务正常运行")
                    return True
                else:
                    logger.warning(f"⚠️  后端服务状态异常: {response.status}")
                    return False
    except Exception as e:
        logger.error(f"❌ 无法连接到后端服务: {e}")
        return False


async def main():
    """主函数"""
    logger.info("=" * 60)
    logger.info("血缘图API测试")
    logger.info("=" * 60)
    
    # 先检查服务健康状态
    logger.info("1. 检查后端服务状态...")
    health_ok = await test_health_check()
    
    if health_ok:
        logger.info("\n2. 测试血缘图API...")
        await test_lineage_api()
    else:
        logger.error("后端服务未运行，请先启动后端服务")
    
    logger.info("\n" + "=" * 60)
    logger.info("测试完成")


if __name__ == "__main__":
    asyncio.run(main()) 