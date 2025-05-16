#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
修复JSON文件中的单引号问题

作者: Vance Chen
"""

import json
import sys

def fix_json_file(input_file, output_file):
    """
    修复JSON文件中的单引号问题
    
    Args:
        input_file: 输入文件路径
        output_file: 输出文件路径
    """
    # 读取输入文件
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 手动替换有问题的字符串
    # 将 "TO_CHAR(d.d_date, 'YYYY-MM')" 替换为 "TO_CHAR(d.d_date, \"YYYY-MM\")"
    fixed_content = content.replace("'YYYY-MM'", "\\\"YYYY-MM\\\"")
    
    # 写入输出文件
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(fixed_content)
    
    # 验证JSON是否有效
    try:
        json_obj = json.loads(fixed_content)
        print("JSON格式有效！")
        
        # 将格式化后的JSON写入文件，使其更易读
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(json_obj, f, indent=2, ensure_ascii=False)
            
        return json_obj
    except json.JSONDecodeError as e:
        print(f"JSON格式仍然无效: {str(e)}")
        return None

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("用法: python fix_json_quotes.py <输入文件> <输出文件>")
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    json_obj = fix_json_file(input_file, output_file)
    if json_obj:
        print("成功修复并加载JSON文件")
        print(f"已将修复后的JSON保存到 {output_file}")
