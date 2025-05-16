#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
修复JSON文件中的Markdown代码块和单引号问题

作者: Vance Chen
"""

import json
import re
import sys

def fix_json_file(input_file, output_file):
    """
    修复JSON文件中的Markdown代码块和单引号问题
    
    Args:
        input_file: 输入文件路径
        output_file: 输出文件路径
    """
    # 读取输入文件
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 移除Markdown代码块标记
    content = re.sub(r'^```json\s*', '', content)
    content = re.sub(r'\s*```$', '', content)
    
    # 手动替换有问题的字符串
    # 将所有形如 'YYYY-MM' 的单引号字符串替换为 \"YYYY-MM\"
    content = re.sub(r"'([^']*?)'", r'"\1"', content)
    
    # 写入输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    # 验证JSON是否有效
    try:
        json_obj = json.loads(content)
        print("JSON格式有效！")
        
        # 将格式化后的JSON写入文件，使其更易读
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_obj, f, indent=2, ensure_ascii=False)
            
        return json_obj
    except json.JSONDecodeError as e:
        print(f"JSON格式仍然无效: {str(e)}")
        print(f"错误位置: 行 {e.lineno}, 列 {e.colno}")
        
        # 显示错误附近的内容
        lines = content.split('\n')
        start_line = max(0, e.lineno - 3)
        end_line = min(len(lines), e.lineno + 2)
        
        print("\n错误附近的内容:")
        for i in range(start_line, end_line):
            prefix = ">>> " if i == e.lineno - 1 else "    "
            print(f"{prefix}行 {i+1}: {lines[i]}")
            
            if i == e.lineno - 1:
                # 在错误位置添加指示符
                pointer = " " * (e.colno + 8) + "^"
                print(pointer)
        
        return None

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("用法: python fix_json_markdown.py <输入文件> <输出文件>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    json_obj = fix_json_file(input_file, output_file)
    if json_obj:
        print("成功修复并加载JSON文件")
        print(f"已将修复后的JSON保存到 {output_file}")
