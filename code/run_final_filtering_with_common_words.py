#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import re
import argparse
import glob
from datetime import datetime
from pathlib import Path
sys.path.append('src/core')

from data_preperation_rex_新闻_完整版 import StockNewsFilterRex, clean_html_content

def process_single_file(input_file, filter_engine, ambiguous_company_names):
    """处理单个CSV文件"""
    
    print(f"📊 加载数据文件: {input_file}")
    
    try:
        df = pd.read_csv(input_file)
        print(f"✅ 成功加载 {len(df)} 条新闻数据")
        print(f"📋 数据列: {list(df.columns)}")
    except Exception as e:
        print(f"❌ 加载数据失败: {e}")
        return None, None, None
    
    # 初始化结果列表
    accepted_news = []
    filtered_out_news = []
    
    # 统计信息
    total_count = len(df)
    processed_count = 0
    
    print(f"🔄 开始筛选 {total_count} 条新闻...")
    
    for idx, row in df.iterrows():
        processed_count += 1
        
        if processed_count % 1000 == 0:
            print(f"📊 已处理: {processed_count}/{total_count} ({processed_count/total_count*100:.1f}%)")
        
        # 获取新闻数据
        title = str(row.get('title', ''))
        content = str(row.get('content', ''))
        pub_time = str(row.get('pub_time', ''))
        source_file = str(row.get('source_file', ''))
        
        # 清理内容
        cleaned_content = clean_html_content(content)
        if not cleaned_content:
            # 内容为空，记录为被筛选
            filtered_out_news.append({
                'title': title,
                'content': cleaned_content,
                'pub_time': pub_time,
                'source_file': source_file,
                'filter_result': 'content_empty',
                'extracted_companies': [],
                'is_announcement': False,
                'is_stock_related': False
            })
            continue
        
        try:
            # 运行筛选
            # 检查是否是公告
            is_announcement = filter_engine.is_announcement(cleaned_content)
            
            # 提取公司信息
            extracted_companies = filter_engine.extract_companies(cleaned_content)
            
            # 🔧 新增：处理常见词误识别问题
            # 检查提取的公司是否包含容易误识别的常见词
            filtered_companies = []
            for company in extracted_companies:
                if company in ambiguous_company_names:
                    # 对于常见词，需要更严格的验证
                    if _is_really_company_mention(cleaned_content, company, filter_engine):
                        filtered_companies.append(company)
                    else:
                        # 记录被过滤的常见词
                        print(f"🔍 过滤常见词误识别: '{company}' in '{title[:50]}...'")
                else:
                    # 非常见词，直接保留
                    filtered_companies.append(company)
            
            # 使用过滤后的公司列表
            final_companies = filtered_companies
            
            # 如果是公告，直接接受
            if is_announcement:
                if final_companies:
                    # 公告且有相关公司
                    accepted_news.append({
                        'title': title,
                        'content': cleaned_content,
                        'pub_time': pub_time,
                        'source_file': source_file,
                        'filter_result': 'announcement_with_company',
                        'extracted_companies': final_companies,
                        'original_companies': extracted_companies,  # 保留原始提取结果
                        'is_announcement': True,
                        'is_stock_related': True
                    })
                else:
                    # 公告但无相关公司
                    filtered_out_news.append({
                        'title': title,
                        'content': cleaned_content,
                        'pub_time': pub_time,
                        'source_file': source_file,
                        'filter_result': 'announcement_no_relevant_company',
                        'extracted_companies': final_companies,
                        'original_companies': extracted_companies,  # 保留原始提取结果
                        'is_announcement': True,
                        'is_stock_related': False
                    })
            else:
                # 如果不是公告，检查是否通过非公告筛选
                is_stock_related = filter_engine.is_stock_related_non_price_news(cleaned_content, title)
                
                if is_stock_related:
                    # 非公告但股票相关
                    accepted_news.append({
                        'title': title,
                        'content': cleaned_content,
                        'pub_time': pub_time,
                        'source_file': source_file,
                        'filter_result': 'non_announcement_stock_related',
                        'extracted_companies': final_companies,
                        'original_companies': extracted_companies,  # 保留原始提取结果
                        'is_announcement': False,
                        'is_stock_related': True
                    })
                else:
                    # 非公告且不股票相关
                    filtered_out_news.append({
                        'title': title,
                        'content': cleaned_content,
                        'pub_time': pub_time,
                        'source_file': source_file,
                        'filter_result': 'failed_stock_related_check',
                        'extracted_companies': final_companies,
                        'original_companies': extracted_companies,  # 保留原始提取结果
                        'is_announcement': False,
                        'is_stock_related': False
                    })
                    
        except Exception as e:
            print(f"⚠️ 处理第 {idx} 条新闻时出错: {e}")
            # 记录错误
            filtered_out_news.append({
                'title': title,
                'content': cleaned_content if 'cleaned_content' in locals() else content,
                'pub_time': pub_time,
                'source_file': source_file,
                'filter_result': 'processing_error',
                'extracted_companies': [],
                'original_companies': [],
                'is_announcement': False,
                'is_stock_related': False,
                'error': str(e)
            })
    
    print(f"✅ 文件 {Path(input_file).name} 筛选完成！")
    print(f"📊 总新闻数: {total_count}")
    print(f"✅ 被接受: {len(accepted_news)} ({len(accepted_news)/total_count*100:.1f}%)")
    print(f"❌ 被筛选: {len(filtered_out_news)} ({len(filtered_out_news)/total_count*100:.1f}%)")
    
    return accepted_news, filtered_out_news, total_count

def run_batch_filtering(input_path, output_dir="test"):
    """运行批量筛选，支持单个文件或文件夹"""
    
    print("🔧 运行包含常见词处理逻辑的批量筛选")
    print("=" * 60)
    
    # 初始化过滤器
    print("🔄 初始化筛选引擎...")
    filter_engine = StockNewsFilterRex()
    
    # 定义容易误识别的常见词汇型公司名
    ambiguous_company_names = {
        '太阳能', '机器人', '新产业', '驱动力', '线上线下', '国新能源', '太平洋', '陆家嘴', '数字人', '创新医疗','老百姓','驱动力',"智慧农业","农产品","太平洋"
    }
    
    # 确定要处理的文件列表
    input_path = Path(input_path)
    csv_files = []
    
    if input_path.is_file():
        if input_path.suffix.lower() == '.csv':
            csv_files = [input_path]
            print(f"📄 将处理单个文件: {input_path}")
        else:
            print(f"❌ 错误：{input_path} 不是CSV文件")
            return
    elif input_path.is_dir():
        csv_files = list(input_path.glob("*.csv"))
        if not csv_files:
            print(f"❌ 错误：文件夹 {input_path} 中没有找到CSV文件")
            return
        print(f"📁 找到 {len(csv_files)} 个CSV文件:")
        for f in csv_files:
            print(f"   - {f.name}")
    else:
        print(f"❌ 错误：路径 {input_path} 不存在")
        return
    
    # 创建输出目录
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    # 生成时间戳
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 批量处理文件
    all_accepted_news = []
    all_filtered_out_news = []
    file_stats = []
    
    print(f"\n🚀 开始批量处理...")
    
    for i, csv_file in enumerate(csv_files, 1):
        print(f"\n{'='*20} 处理文件 {i}/{len(csv_files)} {'='*20}")
        print(f"📂 文件: {csv_file}")
        
        # 处理单个文件
        accepted_news, filtered_out_news, total_count = process_single_file(
            str(csv_file), filter_engine, ambiguous_company_names
        )
        
        if accepted_news is None:
            print(f"⚠️ 跳过文件 {csv_file.name}")
            continue
        
        # 合并到总结果中
        all_accepted_news.extend(accepted_news)
        all_filtered_out_news.extend(filtered_out_news)
        
        # 记录文件统计
        file_stats.append({
            'file_name': csv_file.name,
            'file_path': str(csv_file),
            'total_count': total_count,
            'accepted_count': len(accepted_news),
            'filtered_count': len(filtered_out_news),
            'acceptance_rate': len(accepted_news) / total_count * 100 if total_count > 0 else 0
        })
    
    if not all_accepted_news and not all_filtered_out_news:
        print("❌ 没有成功处理任何文件")
        return
    
    # 保存合并结果
    print(f"\n💾 保存批量筛选结果...")
    
    # 保存被接受的新闻
    accepted_file = output_path / f"batch_accepted_news_common_words_{timestamp}.json"
    with open(accepted_file, 'w', encoding='utf-8') as f:
        json.dump(all_accepted_news, f, ensure_ascii=False, indent=2)
    
    # 保存被筛选的新闻
    filtered_file = output_path / f"batch_filtered_out_news_common_words_{timestamp}.json"
    with open(filtered_file, 'w', encoding='utf-8') as f:
        json.dump(all_filtered_out_news, f, ensure_ascii=False, indent=2)
    
    # 生成总体统计报告
    total_news = sum(stat['total_count'] for stat in file_stats)
    total_accepted = len(all_accepted_news)
    total_filtered = len(all_filtered_out_news)
    
    # 统计筛选原因
    filter_results = {}
    common_words_filtered = 0
    
    for item in all_filtered_out_news:
        reason = item['filter_result']
        if reason not in filter_results:
            filter_results[reason] = 0
        filter_results[reason] += 1
        
        # 统计常见词过滤
        if 'original_companies' in item and 'extracted_companies' in item:
            if len(item['original_companies']) > len(item['extracted_companies']):
                common_words_filtered += 1
    
    # 生成综合报告
    report = {
        'timestamp': timestamp,
        'batch_info': {
            'input_path': str(input_path),
            'processed_files_count': len(file_stats),
            'total_files_found': len(csv_files)
        },
        'overall_stats': {
            'total_news': total_news,
            'accepted_count': total_accepted,
            'filtered_count': total_filtered,
            'acceptance_rate': total_accepted / total_news * 100 if total_news > 0 else 0,
            'common_words_filtered': common_words_filtered
        },
        'filter_results': filter_results,
        'file_stats': file_stats,
        'output_files': {
            'accepted_news': str(accepted_file),
            'filtered_out_news': str(filtered_file)
        }
    }
    
    # 保存报告
    report_file = output_path / f"batch_filtering_report_common_words_{timestamp}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    # 显示结果
    print(f"\n🎉 批量筛选完成！")
    print(f"📊 处理总览:")
    print(f"   - 处理文件数: {len(file_stats)}/{len(csv_files)}")
    print(f"   - 总新闻数: {total_news:,}")
    print(f"   - 被接受: {total_accepted:,} ({total_accepted/total_news*100:.1f}%)")
    print(f"   - 被筛选: {total_filtered:,} ({total_filtered/total_news*100:.1f}%)")
    print(f"   - 常见词过滤: {common_words_filtered:,} 条")
    
    print(f"\n📁 输出文件:")
    print(f"   - 被接受新闻: {accepted_file}")
    print(f"   - 被筛选新闻: {filtered_file}")
    print(f"   - 批量报告: {report_file}")
    
    # 显示各文件统计
    print(f"\n📋 各文件处理统计:")
    for stat in file_stats:
        print(f"   - {stat['file_name']}: "
              f"{stat['accepted_count']}/{stat['total_count']} "
              f"({stat['acceptance_rate']:.1f}%)")
    
    # 显示筛选原因统计
    print(f"\n🔍 筛选原因统计:")
    for reason, count in filter_results.items():
        print(f"   - {reason}: {count:,} 条 ({count/total_filtered*100:.1f}%)")

def run_final_filtering_with_common_words():
    """兼容原有接口的单文件处理函数"""
    test_file = "test/test_dataset.csv"
    run_batch_filtering(test_file)

def _is_really_company_mention(text, company_name, filter_engine):
    """
    判断文本中的常见词是否真的是公司名称
    """
    if not text or not company_name:
        return False
    
    # 1. 检查是否有明确的公司标识符
    company_code = filter_engine.get_company_code(company_name)
    if company_code:
        # 检查是否包含股票代码
        if company_code in text:
            return True
        
        # 检查是否包含公司全称
        company_full_names = [
            f"{company_name}股份",
            f"{company_name}集团", 
            f"{company_name}有限公司",
            f"{company_name}公司",
            f"{company_name}股份有限公司"
        ]
        
        for full_name in company_full_names:
            if full_name in text:
                return True
    
    # 2. 检查是否在明确的股票/投资上下文中
    stock_context_keywords = [
        "股价", "涨停", "跌停", "股票", "证券", "上市公司", "A股", "沪深",
        "市值", "股东", "董事长", "CEO", "总经理", "董事会", "股东大会",
        "财报", "年报", "季报", "业绩", "营收", "利润", "净利润",
        "分红", "派息", "停牌", "复牌", "重组", "并购", "收购",
        "公告", "披露", "投资者关系", "宣布", "发布", "表示"
    ]
    
    has_stock_context = any(keyword in text for keyword in stock_context_keywords)
    
    # 3. 检查公司名称密度
    company_density = filter_engine.calculate_company_density(text, company_name)
    
    # 4. 对于常见词，需要更高的密度阈值和明确的股票上下文
    if company_name in ['太阳能', '机器人', '新产业', '驱动力', '线上线下', '国新能源', '太平洋', '陆家嘴', '数字人', '创新医疗','老百姓','驱动力',"智慧农业","农产品","太平洋"]:
        # 这些词需要更高的密度和明确的股票上下文
        return company_density > 0.02 and has_stock_context
    else:
        # 其他常见词也需要一定的密度和上下文
        return company_density > 0.01 and has_stock_context

def main():
    """主程序入口，支持命令行参数"""
    parser = argparse.ArgumentParser(
        description="运行包含常见词处理逻辑的新闻筛选",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  # 处理单个文件
  python run_final_filtering_with_common_words.py --input test/test_dataset.csv

  # 处理整个文件夹
  python run_final_filtering_with_common_words.py --input data/

  # 指定输出目录
  python run_final_filtering_with_common_words.py --input data/ --output results/

  # 使用默认设置（兼容原版）
  python run_final_filtering_with_common_words.py
        """
    )
    
    parser.add_argument(
        '--input', '-i',
        type=str,
        default="test/test_dataset.csv",
        help="输入文件或文件夹路径 (默认: test/test_dataset.csv)"
    )
    
    parser.add_argument(
        '--output', '-o',
        type=str,
        default="test",
        help="输出目录路径 (默认: test)"
    )
    
    parser.add_argument(
        '--version', '-v',
        action='version',
        version='新闻筛选工具 v2.0 - 支持批量处理'
    )
    
    args = parser.parse_args()
    
    # 运行批量筛选
    run_batch_filtering(args.input, args.output)

if __name__ == "__main__":
    main()
