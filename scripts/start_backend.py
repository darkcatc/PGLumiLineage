#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
启动后端服务器

作者: Vance Chen
"""

import subprocess
import sys
import os

def start_backend():
    """启动后端服务器"""
    
    # 切换到项目根目录
    os.chdir(os.path.join(os.path.dirname(__file__), '..'))
    
    print("启动 PGLumiLineage 后端服务器...")
    print("URL: http://localhost:8000")
    print("API文档: http://localhost:8000/docs")
    print()
    
    try:
        # 启动服务器
        subprocess.run([
            sys.executable, 
            "-m", "uvicorn", 
            "pglumilineage.api.main:app",
            "--reload",
            "--host", "0.0.0.0",
            "--port", "8000"
        ], check=True)
    except KeyboardInterrupt:
        print("\n服务器已停止")
    except Exception as e:
        print(f"启动服务器失败: {e}")

if __name__ == "__main__":
    start_backend() 