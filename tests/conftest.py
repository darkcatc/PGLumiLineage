import sys
import os

# 将项目根目录添加到 Python 路径
# 这确保了 pglumilineage 模块可以被正确导入
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
