#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
修复JSON文件中的格式问题

作者: Vance Chen
"""

import json
import re
import sys

def fix_json_file(input_file, output_file):
    """
    修复JSON文件中的格式问题
    
    Args:
        input_file: 输入文件路径
        output_file: 输出文件路径
    """
    # 读取输入文件
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 查找问题所在行
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if 'TO_CHAR' in line and "'" in line:
            print(f"发现可能有问题的行 {i+1}: {line}")
    
    # 修复JSON字符串中的单引号问题
    # 将形如 "TO_CHAR(d.d_date, 'YYYY-MM')" 的字符串修复为 "TO_CHAR(d.d_date, \"YYYY-MM\")"
    fixed_content = re.sub(r'"([^"]*?)\'([^"]*?)\'([^"]*?)"', r'"\1\\"\2\\"\3"', content)
    
    # 写入输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(fixed_content)
    
    # 验证JSON是否有效
    try:
        json_obj = json.loads(fixed_content)
        print("JSON格式有效！")
        return json_obj
    except json.JSONDecodeError as e:
        print(f"JSON格式仍然无效: {str(e)}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("用法: python fix_json.py <输入文件> <输出文件>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    json_obj = fix_json_file(input_file, output_file)
    if json_obj:
        print("成功修复并加载JSON文件")
