#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Qwen API 调用测试脚本

该脚本用于测试 Qwen API 的连通性和调用情况，帮助诊断可能的问题。

作者: Vance Chen
"""

import os
import sys
from openai import OpenAI

def test_qwen_api():
    """测试 Qwen API 调用"""
    try:
        # 获取环境变量中的 API 密钥
        api_key = os.getenv("DASHSCOPE_API_KEY")
        if not api_key:
            print("\033[91m错误: 环境变量 DASHSCOPE_API_KEY 未设置\033[0m")
            print("请设置环境变量或在下面直接指定 API 密钥")
            api_key = "sk-add1fe773eb44685a3aeee14d89c19a4"  # 使用配置文件中的默认值
        
        print(f"使用的 API 密钥: {api_key[:5]}...{api_key[-4:]}")
        print(f"API 密钥长度: {len(api_key)}")
        
        # 创建 OpenAI 客户端
        client = OpenAI(
            api_key=api_key,
            base_url="https://dashscope.aliyuncs.com/compatible-mode/v1",
        )
        
        print("正在调用 Qwen API...")
        
        # 调用 Qwen API
        completion = client.chat.completions.create(
            model="qwen-plus",  # 模型列表：https://help.aliyun.com/zh/model-studio/getting-started/models
            messages=[
                {'role': 'system', 'content': 'You are a helpful assistant.'},
                {'role': 'user', 'content': '你是谁？'}
            ]
        )
        
        print("\n\033[92m调用成功！\033[0m")
        print(f"响应内容: {completion.choices[0].message.content}")
        
    except Exception as e:
        print(f"\n\033[91m错误信息：{e}\033[0m")
        print("请参考文档：https://help.aliyun.com/zh/model-studio/developer-reference/error-code")
        
        # 打印更详细的错误信息
        import traceback
        print("\n详细错误信息:")
        traceback.print_exc()

if __name__ == "__main__":
    # 检查是否从项目根目录运行
    if not os.path.exists("pglumilineage"):
        print("\033[93m警告: 请从项目根目录运行此脚本\033[0m")
        print("例如: python scripts/test_qwen_api.py")
    
    # 添加项目根目录到 Python 路径
    sys.path.insert(0, os.path.abspath("."))
    
    # 尝试从项目配置中加载 API 密钥
    try:
        from pglumilineage.common.config import get_settings_instance
        settings = get_settings_instance()
        os.environ["DASHSCOPE_API_KEY"] = settings.DASHSCOPE_API_KEY.get_secret_value()
        print("已从项目配置中加载 API 密钥")
    except Exception as e:
        print(f"无法从项目配置中加载 API 密钥: {e}")
    
    # 运行测试
    test_qwen_api()
