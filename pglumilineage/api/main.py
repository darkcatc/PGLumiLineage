#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
PGLumiLineage API 入口

此模块是PGLumiLineage API的入口点，用于启动FastAPI应用。

作者: Vance Chen
"""

import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from pglumilineage.api.lineage.router import router as lineage_router

# 设置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# 创建FastAPI应用
app = FastAPI(
    title="PGLumiLineage API",
    description="PostgreSQL数据血缘分析API",
    version="1.0.0"
)

# 配置CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 生产环境中应该限制为特定域名
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(lineage_router)


@app.get("/")
async def root():
    """API根路径"""
    return {
        "message": "欢迎使用PGLumiLineage API",
        "version": "1.0.0",
        "documentation": "/docs"
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("pglumilineage.api.main:app", host="0.0.0.0", port=8000, reload=True)
