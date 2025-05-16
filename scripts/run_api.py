#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
启动PGLumiLineage API服务

此脚本用于启动PGLumiLineage API服务。

作者: Vance Chen
"""

import os
import sys
import uvicorn

# 添加项目根目录到Python路径
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

if __name__ == "__main__":
    uvicorn.run("pglumilineage.api.main:app", host="0.0.0.0", port=8000, reload=True)
