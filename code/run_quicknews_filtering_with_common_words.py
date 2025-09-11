#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
股票快讯智能筛选系统

这个脚本用于从海量快讯数据中筛选出与股票相关的有价值信息。
主要功能包括：
1. 自动识别股票相关快讯（公告、新闻等）
2. 过滤常见词汇误识别问题（如"太阳能"、"机器人"等）
3. 支持严格模式和宽松模式两种筛选标准
4. 批量处理多个CSV文件
5. 生成详细的筛选报告和统计信息

作者：系统开发团队
版本：v1.1
创建时间：2025年
"""

import sys
import os
import json
import pandas as pd
import re
import argparse
import glob
from datetime import datetime
from pathlib import Path

# 添加核心模块路径
sys.path.append('src/core')

# 导入股票新闻筛选引擎
from data_preperation_rex_快讯_最终版 import StockNewsFilterRex

def process_single_file(input_file, filter_engine, ambiguous_company_names, strict_mode=False, detailed_output=False):
    """
    处理单个CSV文件的核心函数
    
    这个函数负责：
    1. 加载CSV文件中的快讯数据
    2. 逐条处理每个快讯，进行筛选和分类
    3. 识别和过滤常见词汇误识别问题
    4. 生成最终的筛选结果
    
    参数说明：
    - input_file: 输入的CSV文件路径
    - filter_engine: StockNewsFilterRex实例，用于执行筛选逻辑
    - ambiguous_company_names: 容易误识别的常见词汇集合
    - strict_mode: 是否使用严格模式（更高的筛选标准）
    - detailed_output: 是否输出详细信息（包含调试字段）
    
    返回值：
    - accepted_news: 通过筛选的快讯列表
    - filtered_out_news: 被筛选掉的快讯列表（用于分析）
    - total_count: 总处理数量
    """
    
    print(f"📊 加载数据文件: {input_file}")
    
    # 第一步：尝试加载CSV文件
    try:
        df = pd.read_csv(input_file)
        print(f"✅ 成功加载 {len(df)} 条快讯数据")
        print(f"📋 数据列: {list(df.columns)}")
    except Exception as e:
        print(f"❌ 加载数据失败: {e}")
        return None, None, None
    
    # 第二步：初始化结果存储容器
    accepted_news = []      # 存储通过筛选的快讯
    filtered_out_news = []  # 存储被筛选掉的快讯（用于后续分析）
    
    # 第三步：初始化统计计数器
    total_count = len(df)   # 总快讯数量
    processed_count = 0     # 已处理数量
    
    print(f"🔄 开始筛选 {total_count} 条快讯...")
    
    # 第四步：逐条处理每个快讯
    for idx, row in df.iterrows():
        processed_count += 1
        
        # 每1000条显示一次进度
        if processed_count % 1000 == 0:
            print(f"📊 已处理: {processed_count}/{total_count} ({processed_count/total_count*100:.1f}%)")
        
        # 第4.1步：提取快讯的基本信息
        title = str(row.get('title', ''))           # 标题
        content = str(row.get('content', ''))       # 内容
        pub_time = str(row.get('datetime', ''))     # 发布时间（快讯使用datetime列）
        source_file = str(row.get('source_file', ''))  # 来源文件
        
        # 第4.2步：数据预处理
        # 快讯数据通常比较干净，不需要HTML清理，只需要去除首尾空格
        cleaned_content = content.strip()
        
        # 第4.3步：过滤空内容
        if not cleaned_content:
            # 内容为空的快讯直接标记为被筛选，记录原因
            filtered_out_news.append({
                'title': title,
                'content': cleaned_content,
                'pub_time': pub_time,
                'source_file': source_file,
                'filter_result': 'content_empty',  # 筛选原因：内容为空
                'extracted_companies': [],
                'is_announcement': False,
                'is_stock_related': False
            })
            continue
        
        try:
            # 第4.4步：开始核心筛选逻辑
            
            # 第4.4.1步：检查是否是公告类快讯
            # 公告类快讯通常包含"公告"、"披露"、"发布"等关键词
            is_announcement = filter_engine.is_announcement(cleaned_content)
            
            # 第4.4.2步：从快讯内容中提取公司名称
            # 使用正则表达式和公司名称词典进行智能识别
            extracted_companies = filter_engine.extract_companies(cleaned_content)
            
            # 🔧 第4.4.3步：处理常见词汇误识别问题（核心创新功能）
            # 问题：某些常见词汇如"太阳能"、"机器人"既可能是行业词汇，也可能是公司名称
            # 解决方案：对这些词汇进行二次验证，确保它们真的指代具体公司
            filtered_companies = []
            
            for company in extracted_companies:
                if company in ambiguous_company_names:
                    # 对于容易误识别的常见词，使用更严格的验证逻辑
                    if _is_really_company_mention(cleaned_content, company, filter_engine):
                        filtered_companies.append(company)
                        # print(f"🎯 确认公司提及: '{company}' in '{title[:50]}...'")
                    else:
                        # 记录被过滤的常见词，用于调试和质量监控
                        print(f"🔍 过滤常见词误识别: '{company}' in '{title[:50]}...'")
                else:
                    # 对于非常见词汇（如"中国平安"、"招商银行"等），直接保留
                    filtered_companies.append(company)
            
            # 使用过滤后的公司列表作为最终结果
            final_companies = filtered_companies
            
            # 第4.5步：根据快讯类型进行分类处理
            
            # 第4.5.1分支：处理公告类快讯
            if is_announcement:
                if final_companies:
                    # 公告类快讯且包含有效公司信息 -> 直接接受
                    # 🔧 重要改进：为每个公司生成单独的记录
                    # 原因：一条快讯可能涉及多个公司，需要分别建立关联关系
                    for company in final_companies:
                        # 获取公司的股票代码
                        code = filter_engine.get_company_code(company)
                        if code:
                            if detailed_output:
                                # 详细输出模式：保留原有的调试和分析字段
                                # 用于后续质量分析和系统调优
                                accepted_news.append({
                                    'time': pub_time,  
                                    'title': title,
                                    'content': cleaned_content,
                                    'code_name': code,  
                                    'company_chn_name': company,  
                                    'type': 'announcement',
                                    # 以下为调试和分析用的额外字段
                                    'source_file': source_file,
                                    'filter_result': 'announcement_with_company',
                                    'original_companies': extracted_companies,
                                    'is_announcement': True,
                                    'is_stock_related': True
                                })
                            else:
                                # 简洁输出模式：与data_preperation_rex_快讯_最终版格式一致
                                # 生产环境推荐使用此模式，减少存储空间
                                accepted_news.append({
                                    'time': pub_time,  
                                    'title': title,
                                    'content': cleaned_content,
                                    'code_name': code,  
                                    'company_chn_name': company,  
                                    'type': 'announcement'
                                })
                else:
                    # 公告类快讯但没有找到相关公司 -> 被筛选
                    # 这种情况通常表示：1) 公司名识别失败 2) 涉及非上市公司
                    filtered_out_news.append({
                        'title': title,
                        'content': cleaned_content,
                        'pub_time': pub_time,
                        'source_file': source_file,
                        'filter_result': 'announcement_no_relevant_company',  # 筛选原因
                        'extracted_companies': final_companies,
                        'original_companies': extracted_companies,  # 保留原始提取结果用于调试
                        'is_announcement': True,
                        'is_stock_related': False
                    })
            else:
                # 第4.5.2分支：处理非公告类快讯
                # 对于非公告类快讯，需要进行更严格的股票相关性判断
                is_stock_related = filter_engine.is_stock_related_non_price_news(cleaned_content, title, strict_mode)
                
                if is_stock_related:
                    # 非公告类快讯但与股票相关 -> 接受
                    # 🔧 重要改进：为每个公司生成单独的记录
                    for company in final_companies:
                        # 获取公司的股票代码
                        code = filter_engine.get_company_code(company)
                        if code:
                            if detailed_output:
                                # 详细输出模式：保留原有的调试和分析字段
                                accepted_news.append({
                                    'time': pub_time,  
                                    'title': title,
                                    'content': cleaned_content,
                                    'code_name': code,  
                                    'company_chn_name': company,  
                                    'type': 'news',  # 标记为普通新闻而非公告
                                    # 以下为调试和分析用的额外字段
                                    'source_file': source_file,
                                    'filter_result': 'non_announcement_stock_related',
                                    'original_companies': extracted_companies,
                                    'is_announcement': False,
                                    'is_stock_related': True
                                })
                            else:
                                # 简洁输出模式：与data_preperation_rex_快讯_最终版格式一致
                                accepted_news.append({
                                    'time': pub_time,  
                                    'title': title,
                                    'content': cleaned_content,
                                    'code_name': code,  
                                    'company_chn_name': company,  
                                    'type': 'news'  # 标记为普通新闻
                                })
                else:
                    # 非公告且不具备股票相关性 -> 被筛选
                    # 这是最常见的筛选情况，包括娱乐、体育、国际新闻等
                    filtered_out_news.append({
                        'title': title,
                        'content': cleaned_content,
                        'pub_time': pub_time,
                        'source_file': source_file,
                        'filter_result': 'failed_stock_related_check',  # 筛选原因
                        'extracted_companies': final_companies,
                        'original_companies': extracted_companies,  # 保留原始提取结果
                        'is_announcement': False,
                        'is_stock_related': False
                    })
                    
        except Exception as e:
            # 第4.6步：异常处理
            # 当单条快讯处理过程中出现异常时，记录错误信息并继续处理下一条
            print(f"⚠️ 处理第 {idx} 条快讯时出错: {e}")
            
            # 将出错的快讯记录到筛选列表中，避免数据丢失
            filtered_out_news.append({
                'title': title,
                'content': cleaned_content if 'cleaned_content' in locals() else content,
                'pub_time': pub_time,
                'source_file': source_file,
                'filter_result': 'processing_error',  # 标记为处理错误
                'extracted_companies': [],
                'original_companies': [],
                'is_announcement': False,
                'is_stock_related': False,
                'error': str(e)  # 保留具体错误信息用于调试
            })
    
    # 第五步：输出处理结果统计
    print(f"✅ 文件 {Path(input_file).name} 筛选完成！")
    print(f"📊 总快讯数: {total_count}")
    print(f"✅ 被接受: {len(accepted_news)} ({len(accepted_news)/total_count*100:.1f}%)")
    print(f"❌ 被筛选: {len(filtered_out_news)} ({len(filtered_out_news)/total_count*100:.1f}%)")
    
    # 第六步：返回处理结果
    return accepted_news, filtered_out_news, total_count

def process_individual_files(input_path, output_dir="test", strict_mode=False, detailed_output=False):
    """
    文件对应模式处理函数
    
    这个函数实现"一对一"的文件处理模式：
    - 每个输入的CSV文件生成一个对应的输出JSON文件
    - 适用于需要保持文件边界的场景（如按日期分批处理）
    - 便于追踪每个文件的处理结果和质量
    
    参数说明：
    - input_path: 输入文件或文件夹路径
    - output_dir: 输出目录，默认为"test"
    - strict_mode: 严格模式开关，影响筛选标准的严格程度
    - detailed_output: 详细输出开关，决定是否包含调试字段
    
    工作流程：
    1. 初始化筛选引擎和常见词库
    2. 扫描输入路径，获取所有CSV文件
    3. 逐个处理每个文件
    4. 为每个文件生成独立的输出文件和统计报告
    """
    
    mode_name = "严格模式" if strict_mode else "宽松模式"
    print(f"🔧 运行快讯筛选 - {mode_name} (文件对应模式)")
    print("=" * 60)
    
    # 第一步：初始化筛选引擎
    print("🔄 初始化快讯筛选引擎...")
    filter_engine = StockNewsFilterRex()
    
    # 第二步：定义容易误识别的常见词汇型公司名
    # 这些词汇既可能是行业概念，也可能是具体公司名称，需要特别处理
    ambiguous_company_names = {
        '太阳能',    # 可能指太阳能行业或太阳能公司
        '机器人',    # 可能指机器人概念或机器人公司
        '新产业',    # 可能指新兴产业或新产业公司
        '驱动力',    # 可能指发展驱动力或驱动力公司
        '线上线下',  # 可能指商业模式或具体公司
        '国新能源',  # 相对明确，但仍需验证上下文
        '太平洋',    # 可能指地理区域或太平洋公司
        '陆家嘴',    # 可能指地名或陆家嘴公司
        '数字人',    # 可能指技术概念或相关公司
        '创新医疗',  # 可能指医疗创新概念或创新医疗公司
        '老百姓',    # 可能指民众概念或老百姓公司
        '智慧农业',  # 可能指农业概念或智慧农业公司
        '农产品'     # 可能指商品类别或农产品公司
    }
    
    # 第三步：确定要处理的文件列表
    input_path = Path(input_path)
    csv_files = []
    
    if input_path.is_file():
        # 情况1：输入是单个文件
        if input_path.suffix.lower() == '.csv':
            csv_files = [input_path]
            print(f"📄 将处理单个文件: {input_path}")
        else:
            print(f"❌ 错误：{input_path} 不是CSV文件")
            return
    elif input_path.is_dir():
        # 情况2：输入是文件夹
        csv_files = list(input_path.glob("*.csv"))
        if not csv_files:
            print(f"❌ 错误：文件夹 {input_path} 中没有找到CSV文件")
            return
        print(f"📁 找到 {len(csv_files)} 个CSV文件:")
        for f in csv_files:
            print(f"   - {f.name}")
    else:
        # 情况3：路径不存在
        print(f"❌ 错误：路径 {input_path} 不存在")
        return
    
    # 第四步：准备输出环境
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)  # 确保输出目录存在
    
    # 生成时间戳，用于文件名去重和版本标识
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    mode_suffix = "strict" if strict_mode else "relaxed"
    
    # 第五步：初始化批处理统计
    file_stats = []  # 存储每个文件的处理统计信息
    
    print(f"\n🚀 开始逐个文件处理...")
    
    # 第六步：逐个处理每个CSV文件
    for i, csv_file in enumerate(csv_files, 1):
        print(f"\n{'='*20} 处理文件 {i}/{len(csv_files)} {'='*20}")
        print(f"📂 文件: {csv_file}")
        
        # 第6.1步：调用核心处理函数处理单个文件
        accepted_news, filtered_out_news, total_count = process_single_file(
            str(csv_file), filter_engine, ambiguous_company_names, strict_mode, detailed_output
        )
        
        # 第6.2步：检查处理结果
        if accepted_news is None:
            # 文件处理失败（如文件损坏、格式错误等），跳过并继续处理下一个
            print(f"⚠️ 跳过文件 {csv_file.name}")
            continue
        
        # 第6.3步：保存筛选结果
        # 🔧 重要：文件命名格式与data_preperation_rex_快讯_最终版保持一致
        input_stem = csv_file.stem  # 获取不带扩展名的文件名
        
        # 根据输出模式确定文件后缀
        output_mode = "detailed" if detailed_output else "standard"
        
        # 生成主输出文件路径（包含通过筛选的快讯）
        accepted_file = output_path / f"{input_stem}_filter_stock_price_quicknews_rex_complete_{mode_suffix}_{output_mode}_{timestamp}.json"
        
        # 保存筛选通过的快讯数据
        with open(accepted_file, 'w', encoding='utf-8') as f:
            json.dump(accepted_news, f, ensure_ascii=False, indent=4)  # indent=4与原版保持一致
        
        # 第6.4步：可选保存筛选详情（用于质量分析和调试）
        if len(filtered_out_news) > 0:
            filtered_file = output_path / f"{input_stem}_filtered_details_{mode_suffix}_{timestamp}.json"
            with open(filtered_file, 'w', encoding='utf-8') as f:
                json.dump(filtered_out_news, f, ensure_ascii=False, indent=2)
        
        # 第6.5步：记录文件处理统计信息
        file_stats.append({
            'input_file': csv_file.name,           # 输入文件名
            'input_path': str(csv_file),           # 输入文件完整路径
            'total_count': total_count,            # 总快讯数
            'accepted_count': len(accepted_news),  # 通过筛选的数量
            'filtered_count': len(filtered_out_news), # 被筛选的数量
            'acceptance_rate': len(accepted_news) / total_count * 100 if total_count > 0 else 0, # 通过率
            'output_file': str(accepted_file)      # 主要输出文件路径
        })
        
        # 第6.6步：显示保存结果
        print(f"💾 保存结果:")
        print(f"   - 筛选结果: {accepted_file.name}")
        if len(filtered_out_news) > 0:
            print(f"   - 筛选详情: {Path(f'{input_stem}_filtered_details_{mode_suffix}_{timestamp}.json').name}")
    
    # 第七步：检查处理结果
    if not file_stats:
        print("❌ 没有成功处理任何文件")
        return
    
    # 第八步：生成综合统计报告
    total_news = sum(stat['total_count'] for stat in file_stats)       # 总快讯数
    total_accepted = sum(stat['accepted_count'] for stat in file_stats) # 总通过数
    total_filtered = sum(stat['filtered_count'] for stat in file_stats) # 总筛选数
    
    # 构建综合报告数据结构
    report = {
        'timestamp': timestamp,                   # 处理时间戳
        'mode': mode_name,                       # 处理模式（严格/宽松）
        'processing_type': 'individual_files',   # 处理类型（文件对应模式）
        'batch_info': {
            'input_path': str(input_path),       # 输入路径
            'processed_files_count': len(file_stats), # 成功处理的文件数
            'total_files_found': len(csv_files)  # 发现的总文件数
        },
        'overall_stats': {
            'total_news': total_news,            # 总快讯数
            'accepted_count': total_accepted,    # 通过筛选总数
            'filtered_count': total_filtered,    # 被筛选总数
            'acceptance_rate': total_accepted / total_news * 100 if total_news > 0 else 0 # 总通过率
        },
        'file_stats': file_stats                 # 各文件详细统计
    }
    
    # 第九步：保存综合报告到文件
    report_file = output_path / f"quicknews_individual_report_{mode_suffix}_{timestamp}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    # 第十步：显示处理结果摘要
    print(f"\n🎉 快讯逐文件处理完成！")
    print(f"📊 处理总览:")
    print(f"   - 模式: {mode_name}")
    print(f"   - 处理文件数: {len(file_stats)}/{len(csv_files)}")
    print(f"   - 总快讯数: {total_news:,}")
    print(f"   - 被接受: {total_accepted:,} ({total_accepted/total_news*100:.1f}%)")
    print(f"   - 被筛选: {total_filtered:,} ({total_filtered/total_news*100:.1f}%)")
    
    print(f"\n📁 综合报告: {report_file}")
    
    # 第十一步：显示各文件处理统计明细
    print(f"\n📋 各文件处理统计:")
    for stat in file_stats:
        print(f"   - {stat['input_file']}: "
              f"{stat['accepted_count']}/{stat['total_count']} "
              f"({stat['acceptance_rate']:.1f}%)")
        print(f"     输出: {Path(stat['output_file']).name}")

def run_batch_filtering(input_path, output_dir="test", strict_mode=False, detailed_output=False):
    """
    批量合并模式处理函数
    
    这个函数实现"多对一"的文件处理模式：
    - 将所有输入CSV文件的筛选结果合并到一个输出JSON文件中
    - 适用于需要统一分析所有数据的场景（如生成综合报告）
    - 便于进行跨文件的数据分析和统计
    
    与process_individual_files的区别：
    - process_individual_files: 每个输入文件 -> 一个输出文件（1:1模式）
    - run_batch_filtering: 所有输入文件 -> 一个合并输出文件（N:1模式）
    
    参数说明：
    - input_path: 输入文件或文件夹路径
    - output_dir: 输出目录，默认为"test"
    - strict_mode: 严格模式开关，影响筛选标准的严格程度
    - detailed_output: 详细输出开关，决定是否包含调试字段
    
    工作流程：
    1. 初始化筛选引擎和常见词库
    2. 扫描输入路径，获取所有CSV文件
    3. 逐个处理每个文件，将结果累积到总列表中
    4. 生成一个包含所有筛选结果的合并文件
    5. 生成包含各文件统计的综合报告
    """
    
    mode_name = "严格模式" if strict_mode else "宽松模式"
    print(f"🔧 运行快讯批量筛选 - {mode_name} (合并模式)")
    print("=" * 60)
    
    # 第一步：初始化筛选引擎
    print("🔄 初始化快讯筛选引擎...")
    filter_engine = StockNewsFilterRex()
    
    # 第二步：定义容易误识别的常见词汇型公司名
    # 这个词汇库与individual模式保持一致，确保筛选标准统一
    ambiguous_company_names = {
        '太阳能', '机器人', '新产业', '驱动力', '线上线下', '国新能源', 
        '太平洋', '陆家嘴', '数字人', '创新医疗','老百姓','驱动力',
        "智慧农业","农产品","太平洋"
    }
    
    # 第三步：确定要处理的文件列表（与individual模式相同的逻辑）
    input_path = Path(input_path)
    csv_files = []
    
    if input_path.is_file():
        # 情况1：输入是单个文件
        if input_path.suffix.lower() == '.csv':
            csv_files = [input_path]
            print(f"📄 将处理单个文件: {input_path}")
        else:
            print(f"❌ 错误：{input_path} 不是CSV文件")
            return
    elif input_path.is_dir():
        # 情况2：输入是文件夹
        csv_files = list(input_path.glob("*.csv"))
        if not csv_files:
            print(f"❌ 错误：文件夹 {input_path} 中没有找到CSV文件")
            return
        print(f"📁 找到 {len(csv_files)} 个CSV文件:")
        for f in csv_files:
            print(f"   - {f.name}")
    else:
        # 情况3：路径不存在
        print(f"❌ 错误：路径 {input_path} 不存在")
        return
    
    # 第四步：准备输出环境
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)  # 确保输出目录存在
    
    # 生成时间戳，用于文件名去重和版本标识
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 第五步：初始化批量合并的数据容器
    all_accepted_news = []    # 存储所有文件的通过筛选快讯（合并列表）
    all_filtered_out_news = [] # 存储所有文件的被筛选快讯（合并列表）
    file_stats = []           # 存储每个文件的处理统计信息
    
    print(f"\n🚀 开始批量处理...")
    
    # 第六步：逐个处理每个CSV文件并累积结果
    for i, csv_file in enumerate(csv_files, 1):
        print(f"\n{'='*20} 处理文件 {i}/{len(csv_files)} {'='*20}")
        print(f"📂 文件: {csv_file}")
        
        # 第6.1步：调用核心处理函数处理单个文件
        accepted_news, filtered_out_news, total_count = process_single_file(
            str(csv_file), filter_engine, ambiguous_company_names, strict_mode, detailed_output
        )
        
        # 第6.2步：检查处理结果
        if accepted_news is None:
            # 文件处理失败，跳过并继续处理下一个
            print(f"⚠️ 跳过文件 {csv_file.name}")
            continue
        
        # 第6.3步：将当前文件的结果合并到总结果中
        # 🔧 关键差异：这里是extend（追加列表），而非individual模式的单独保存
        all_accepted_news.extend(accepted_news)        # 合并通过筛选的快讯
        all_filtered_out_news.extend(filtered_out_news) # 合并被筛选的快讯
        
        # 第6.4步：记录单个文件的统计信息（用于生成报告）
        file_stats.append({
            'file_name': csv_file.name,            # 文件名
            'file_path': str(csv_file),            # 文件完整路径
            'total_count': total_count,            # 该文件的总快讯数
            'accepted_count': len(accepted_news),  # 该文件通过筛选的数量
            'filtered_count': len(filtered_out_news), # 该文件被筛选的数量
            'acceptance_rate': len(accepted_news) / total_count * 100 if total_count > 0 else 0 # 该文件的通过率
        })
    
    # 第七步：检查处理结果
    if not all_accepted_news and not all_filtered_out_news:
        print("❌ 没有成功处理任何文件")
        return
    
    # 第八步：保存合并后的筛选结果
    print(f"\n💾 保存批量筛选结果...")
    
    # 第8.1步：生成合并输出文件
    # 🔧 重要：文件命名格式与data_preperation_rex_快讯_最终版保持一致
    mode_suffix = "strict" if strict_mode else "relaxed"
    output_mode = "detailed" if detailed_output else "standard"
    
    # 主输出文件：包含所有通过筛选的快讯（所有文件合并）
    accepted_file = output_path / f"quicknews_batch_filter_stock_price_quicknews_rex_complete_{mode_suffix}_{output_mode}_{timestamp}.json"
    with open(accepted_file, 'w', encoding='utf-8') as f:
        json.dump(all_accepted_news, f, ensure_ascii=False, indent=4)  # 与原版保持一致的缩进
    
    # 第8.2步：可选保存筛选详情文件（用于质量分析和调试）
    if len(all_filtered_out_news) > 0:
        filtered_file = output_path / f"quicknews_batch_filtered_details_{mode_suffix}_{timestamp}.json"
        with open(filtered_file, 'w', encoding='utf-8') as f:
            json.dump(all_filtered_out_news, f, ensure_ascii=False, indent=2)
    
    # 第九步：生成总体统计报告
    total_news = sum(stat['total_count'] for stat in file_stats)  # 所有文件的总快讯数
    total_accepted = len(all_accepted_news)   # 合并后的通过筛选总数
    total_filtered = len(all_filtered_out_news) # 合并后的被筛选总数
    
    # 第9.1步：分析筛选原因分布（用于质量监控）
    filter_results = {}
    common_words_filtered = 0  # 统计因常见词过滤而被筛选的数量
    
    for item in all_filtered_out_news:
        # 统计各种筛选原因的数量
        reason = item['filter_result']
        if reason not in filter_results:
            filter_results[reason] = 0
        filter_results[reason] += 1
        
        # 专门统计常见词过滤的效果
        if 'original_companies' in item and 'extracted_companies' in item:
            if len(item['original_companies']) > len(item['extracted_companies']):
                common_words_filtered += 1
    
    # 第9.2步：构建综合报告数据结构
    report = {
        'timestamp': timestamp,                 # 处理时间戳
        'mode': mode_name,                     # 处理模式（严格/宽松）
        'processing_type': 'batch_merged',     # 处理类型（批量合并模式）
        'batch_info': {
            'input_path': str(input_path),     # 输入路径
            'processed_files_count': len(file_stats), # 成功处理的文件数
            'total_files_found': len(csv_files) # 发现的总文件数
        },
        'overall_stats': {
            'total_news': total_news,          # 总快讯数
            'accepted_count': total_accepted,  # 通过筛选总数
            'filtered_count': total_filtered,  # 被筛选总数
            'acceptance_rate': total_accepted / total_news * 100 if total_news > 0 else 0, # 总通过率
            'common_words_filtered': common_words_filtered  # 常见词过滤统计
        },
        'filter_results': filter_results,     # 筛选原因分布
        'file_stats': file_stats,             # 各文件详细统计
        'output_files': {
            'main_output': str(accepted_file)  # 主要输出文件路径
        }
    }
    
    # 第十步：保存综合报告到文件
    report_file = output_path / f"quicknews_batch_report_{mode_suffix}_{timestamp}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    # 第十一步：显示处理结果摘要
    print(f"\n🎉 快讯批量筛选完成！")
    print(f"📊 处理总览:")
    print(f"   - 模式: {mode_name}")
    print(f"   - 处理文件数: {len(file_stats)}/{len(csv_files)}")
    print(f"   - 总快讯数: {total_news:,}")
    print(f"   - 被接受: {total_accepted:,} ({total_accepted/total_news*100:.1f}%)")
    print(f"   - 被筛选: {total_filtered:,} ({total_filtered/total_news*100:.1f}%)")
    print(f"   - 常见词过滤: {common_words_filtered:,} 条")  # 显示常见词过滤的效果
    
    print(f"\n📁 输出文件:")
    print(f"   - 筛选结果: {accepted_file}")
    if len(all_filtered_out_news) > 0:
        print(f"   - 筛选详情: quicknews_batch_filtered_details_{mode_suffix}_{timestamp}.json")
    print(f"   - 批量报告: {report_file}")

def run_dual_mode_filtering(input_path, output_dir="test", individual_mode=True, detailed_output=False):
    """运行双模式筛选，同时生成严格和宽松两个版本"""
    
    print("🔧 运行快讯双模式筛选")
    print("=" * 60)
    
    if individual_mode:
        # 运行严格模式（文件对应）
        print("\n🔧 第一步：运行严格模式（文件对应）...")
        process_individual_files(input_path, output_dir, strict_mode=True, detailed_output=detailed_output)
        
        # 运行宽松模式（文件对应）
        print("\n🔧 第二步：运行宽松模式（文件对应）...")
        process_individual_files(input_path, output_dir, strict_mode=False, detailed_output=detailed_output)
    else:
        # 运行严格模式（批量合并）
        print("\n🔧 第一步：运行严格模式（批量合并）...")
        run_batch_filtering(input_path, output_dir, strict_mode=True, detailed_output=detailed_output)
        
        # 运行宽松模式（批量合并）
        print("\n🔧 第二步：运行宽松模式（批量合并）...")
        run_batch_filtering(input_path, output_dir, strict_mode=False, detailed_output=detailed_output)
    
    print("\n🎉 双模式筛选完成！")

def _is_really_company_mention(text, company_name, filter_engine):
    """
    智能判断常见词汇是否真正指代公司的核心算法
    
    【问题背景】
    在股票快讯中，某些词汇如"太阳能"、"机器人"等既可能是：
    1. 行业概念词汇：如"太阳能产业发展迅速"
    2. 具体公司名称：如"太阳能（股票代码000591）发布财报"
    
    传统的关键词匹配无法区分这两种情况，导致大量误识别。
    
    【解决方案】
    本函数通过多维度分析来判断常见词汇是否真正指代具体公司：
    1. 明确标识符检测：股票代码、公司全称等
    2. 上下文语境分析：是否出现股票相关关键词
    3. 词汇密度分析：公司名在文本中的出现频率
    4. 分层阈值判断：针对不同词汇设置不同的严格程度
    
    【参数说明】
    - text: 快讯正文内容
    - company_name: 待验证的公司名称（常见词汇）
    - filter_engine: 筛选引擎实例，用于获取公司代码等信息
    
    【返回值】
    - True: 该词汇确实指代具体公司
    - False: 该词汇可能只是行业概念，不指代具体公司
    """
    
    # 第一步：基础验证
    if not text or not company_name:
        return False
    
    # 第二步：检查是否有明确的公司标识符（最强证据）
    company_code = filter_engine.get_company_code(company_name)
    if company_code:
        # 第2.1步：检查是否包含股票代码
        # 如：文中出现"太阳能（000591）"，则明确指代太阳能公司
        if company_code in text:
            return True
        
        # 第2.2步：检查是否包含公司全称
        # 构建可能的公司全称变体
        company_full_names = [
            f"{company_name}股份",        # 如：太阳能股份
            f"{company_name}集团",        # 如：太阳能集团
            f"{company_name}有限公司",    # 如：太阳能有限公司
            f"{company_name}公司",        # 如：太阳能公司
            f"{company_name}股份有限公司" # 如：太阳能股份有限公司
        ]
        
        # 如果文中出现任何一种全称形式，则认为指代具体公司
        for full_name in company_full_names:
            if full_name in text:
                return True
    
    # 第三步：检查是否在明确的股票/投资上下文中（语境证据）
    # 定义股票相关的关键词库，覆盖各个方面：
    stock_context_keywords = [
        # 股价相关
        "股价", "涨停", "跌停", "股票", "证券", "上市公司", "A股", "沪深",
        # 公司治理相关
        "市值", "股东", "董事长", "CEO", "总经理", "董事会", "股东大会",
        # 财务相关
        "财报", "年报", "季报", "业绩", "营收", "利润", "净利润",
        # 资本运作相关
        "分红", "派息", "停牌", "复牌", "重组", "并购", "收购",
        # 信息披露相关
        "公告", "披露", "投资者关系", "宣布", "发布", "表示"
    ]
    
    # 检查文本中是否包含任何股票相关关键词
    has_stock_context = any(keyword in text for keyword in stock_context_keywords)
    
    # 第四步：计算公司名称在文本中的密度（频率证据）
    # 密度 = 公司名出现次数 / 总词数
    # 如果一个词多次出现，更可能是指具体公司而非泛指概念
    company_density = sum(text.count(c) for c in [company_name]) / len(text.split())
    
    # 第五步：基于词汇类型进行分层判断（核心决策逻辑）
    # 针对不同的常见词汇设置不同的严格程度
    if company_name in ['太阳能', '机器人', '新产业', '驱动力', '线上线下', 
                       '国新能源', '太平洋', '陆家嘴', '数字人', '创新医疗',
                       '老百姓', '智慧农业', '农产品']:
        # 这些词汇特别容易误识别，需要更严格的条件：
        # 1. 更高的密度阈值（0.02）：词汇必须在文本中有一定重要性
        # 2. 必须有明确的股票上下文：确保不是泛指行业概念
        return company_density > 0.02 and has_stock_context
    else:
        # 对于其他相对明确的词汇，使用较宽松的标准：
        # 1. 较低的密度阈值（0.01）：适度出现即可
        # 2. 仍需要股票上下文：保证基本的准确性
        return company_density > 0.01 and has_stock_context

def main():
    """
    主程序入口点 - 智能股票快讯筛选系统
    
    【功能概述】
    这是整个系统的命令行入口点，负责：
    1. 解析和验证命令行参数
    2. 根据参数选择适当的处理模式
    3. 协调各个功能模块的执行
    4. 提供友好的用户界面和帮助信息
    
    【支持的处理模式】
    1. 文件对应模式（individual）：每个CSV文件对应一个JSON输出
    2. 批量合并模式（batch）：所有CSV文件合并为一个JSON输出  
    3. 双模式对比（dual）：同时运行严格和宽松模式，便于效果对比
    
    【支持的筛选模式】
    1. 严格模式（strict）：更高的筛选标准，精确度优先
    2. 宽松模式（默认）：相对宽松的筛选标准，召回率优先
    
    【输出格式选项】
    1. 标准模式（默认）：与原版data_preperation_rex_快讯_最终版完全兼容
    2. 详细模式（detailed-output）：包含额外的调试和分析字段
    """
    
    # 第一步：创建命令行参数解析器
    parser = argparse.ArgumentParser(
        description="智能股票快讯筛选工具 - 支持常见词汇误识别过滤、多种处理模式和输出格式",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
【使用示例】

基础使用：
  # 处理单个文件（默认宽松模式，标准输出格式）
  python run_quicknews_filtering_with_common_words.py --input data/同花顺_快讯_2025.csv

高级功能：
  # 严格模式 + 详细输出（用于质量分析）
  python run_quicknews_filtering_with_common_words.py --input data/同花顺_快讯_2025.csv --strict --detailed-output

批量处理：
  # 文件对应模式：每个文件单独输出（适合按日期分析）
  python run_quicknews_filtering_with_common_words.py --input data/ --individual --detailed-output

  # 批量合并模式：所有文件合并输出（适合整体分析）
  python run_quicknews_filtering_with_common_words.py --input data/ --batch

模式对比：
  # 双模式对比（文件对应）：同时生成严格和宽松版本，便于效果对比
  python run_quicknews_filtering_with_common_words.py --input data/ --dual --individual --detailed-output

  # 双模式对比（批量合并）：生成两个合并文件进行对比
  python run_quicknews_filtering_with_common_words.py --input data/ --dual --batch

【输出格式说明】
  - 标准模式: 与data_preperation_rex_快讯_最终版.py输出格式完全一致，适用于生产环境
  - 详细模式: 保留source_file, filter_result, original_companies等调试字段，适用于开发调试

【文件命名规则】
  标准模式: {文件名}_filter_stock_price_quicknews_rex_complete_{模式}_{格式}_{时间戳}.json
  详细文件: {文件名}_filtered_details_{模式}_{时间戳}.json
  报告文件: quicknews_{处理类型}_report_{模式}_{时间戳}.json
        """
    )
    
    # 第二步：定义命令行参数
    
    # 基础参数：输入输出路径
    parser.add_argument(
        '--input', '-i',
        type=str,
        default="data/同花顺_快讯_2025.csv",
        help="输入文件或文件夹路径。支持单个CSV文件或包含多个CSV文件的文件夹 (默认: data/同花顺_快讯_2025.csv)"
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default="test",
        help="输出目录路径。所有生成的文件都会保存在此目录下，如果目录不存在会自动创建 (默认: test)"
    )
    
    # 筛选模式参数
    parser.add_argument(
        '--strict', '-s',
        action='store_true',
        help="启用严格模式。使用更严格的筛选标准，提高精确度但可能降低召回率"
    )
    
    # 处理模式参数
    parser.add_argument(
        '--dual', '-d',
        action='store_true',
        help="双模式对比。同时运行严格模式和宽松模式，生成两套结果便于效果对比"
    )
    
    parser.add_argument(
        '--individual',
        action='store_true',
        help="文件对应模式。为每个输入CSV文件生成对应的输出JSON文件，保持文件边界"
    )
    
    parser.add_argument(
        '--batch',
        action='store_true',
        help="批量合并模式。将所有输入文件的筛选结果合并到单个输出文件中"
    )
    
    # 输出格式参数
    parser.add_argument(
        '--detailed-output', '-D',
        action='store_true',
        help="详细输出模式。保留source_file、filter_result、original_companies等调试字段，用于质量分析"
    )
    
    # 版本信息
    parser.add_argument(
        '--version', '-v',
        action='version',
        version='智能股票快讯筛选工具 v1.1 - 支持常见词汇误识别过滤、严格/宽松模式、多种处理模式'
    )
    
    # 第三步：解析命令行参数
    args = parser.parse_args()
    
    # 第四步：根据参数确定处理模式并分发执行
    # 模式优先级：dual > individual > batch > 默认(individual)
    
    if args.dual:
        # 双模式对比：同时运行严格和宽松模式
        # 子模式确定：如果指定了--batch则使用批量合并，否则使用文件对应（默认）
        individual_mode = not args.batch  
        print(f"🔧 启动双模式对比 - {'文件对应' if individual_mode else '批量合并'}模式")
        run_dual_mode_filtering(args.input, args.output, individual_mode, args.detailed_output)
        
    elif args.individual:
        # 文件对应模式：每个CSV文件对应一个JSON输出
        print(f"🔧 启动文件对应模式 - {'严格' if args.strict else '宽松'}筛选")
        process_individual_files(args.input, args.output, args.strict, args.detailed_output)
        
    elif args.batch:
        # 批量合并模式：所有CSV文件合并为一个JSON输出
        print(f"🔧 启动批量合并模式 - {'严格' if args.strict else '宽松'}筛选")
        run_batch_filtering(args.input, args.output, args.strict, args.detailed_output)
        
    else:
        # 默认模式：使用文件对应模式
        # 这是最常用的模式，适合大多数应用场景
        print(f"🔧 启动默认模式（文件对应）- {'严格' if args.strict else '宽松'}筛选")
        process_individual_files(args.input, args.output, args.strict, args.detailed_output)


# 程序入口点
if __name__ == "__main__":
    """
    程序执行入口
    
    当脚本被直接运行时（而不是被导入时），会执行main()函数。
    这是Python脚本的标准做法，确保模块既可以被导入也可以独立运行。
    """
    main()
