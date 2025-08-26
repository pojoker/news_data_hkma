#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import os
import json
import pandas as pd
import re
from datetime import datetime
sys.path.append('src/core')

from data_preperation_rex_新闻_完整版 import StockNewsFilterRex, clean_html_content

def run_final_filtering_with_common_words():
    """运行包含常见词处理逻辑的筛选"""
    
    print("🔧 运行包含常见词处理逻辑的筛选")
    print("=" * 50)
    
    # 初始化过滤器
    print("🔄 初始化筛选引擎...")
    filter_engine = StockNewsFilterRex()
    
    # 定义容易误识别的常见词汇型公司名
    ambiguous_company_names = {
        '太阳能', '机器人', '新产业', '驱动力', '线上线下', '国新能源', '太平洋', '陆家嘴', '数字人', '创新医疗','老百姓','驱动力',"智慧农业","农产品","太平洋"
    }
    
    # 加载测试数据
    test_file = "test/test_dataset.csv"
    print(f"📊 加载测试数据: {test_file}")
    
    try:
        df = pd.read_csv(test_file)
        print(f"✅ 成功加载 {len(df)} 条新闻数据")
        print(f"📋 数据列: {list(df.columns)}")
    except Exception as e:
        print(f"❌ 加载测试数据失败: {e}")
        return
    
    # 初始化结果列表
    accepted_news = []
    filtered_out_news = []
    
    # 统计信息
    total_count = len(df)
    processed_count = 0
    
    print(f"\n🔄 开始筛选 {total_count} 条新闻...")
    
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
                'content': content,
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
                        'content': cleaned_content,  # 使用清理后的内容
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
                        'content': cleaned_content,  # 使用清理后的内容
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
                is_stock_related = filter_engine.is_stock_related_non_price_news(title, cleaned_content)
                
                if is_stock_related:
                    # 非公告但股票相关
                    accepted_news.append({
                        'title': title,
                        'content': cleaned_content,  # 使用清理后的内容
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
                        'content': cleaned_content,  # 使用清理后的内容
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
                'content': cleaned_content if 'cleaned_content' in locals() else content,  # 使用清理后的内容，如果没有则使用原始内容
                'pub_time': pub_time,
                'source_file': source_file,
                'filter_result': 'processing_error',
                'extracted_companies': [],
                'original_companies': [],
                'is_announcement': False,
                'is_stock_related': False,
                'error': str(e)
            })
    
    # 生成时间戳
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 保存结果
    print(f"\n💾 保存筛选结果...")
    
    # 保存被接受的新闻
    accepted_file = f"test/accepted_news_common_words_{timestamp}.json"
    with open(accepted_file, 'w', encoding='utf-8') as f:
        json.dump(accepted_news, f, ensure_ascii=False, indent=2)
    
    # 保存被筛选的新闻
    filtered_file = f"test/filtered_out_news_common_words_{timestamp}.json"
    with open(filtered_file, 'w', encoding='utf-8') as f:
        json.dump(filtered_out_news, f, ensure_ascii=False, indent=2)
    
    # 生成统计报告
    report = {
        'timestamp': timestamp,
        'total_news': total_count,
        'accepted_count': len(accepted_news),
        'filtered_count': len(filtered_out_news),
        'acceptance_rate': len(accepted_news) / total_count * 100,
        'files': {
            'accepted_news': accepted_file,
            'filtered_out_news': filtered_file
        },
        'filter_results': {},
        'common_words_filtered': 0
    }
    
    # 统计筛选原因和常见词过滤
    for item in filtered_out_news:
        reason = item['filter_result']
        if reason not in report['filter_results']:
            report['filter_results'][reason] = 0
        report['filter_results'][reason] += 1
        
        # 统计常见词过滤
        if 'original_companies' in item and 'extracted_companies' in item:
            if len(item['original_companies']) > len(item['extracted_companies']):
                report['common_words_filtered'] += 1
    
    # 保存报告
    report_file = f"test/filtering_report_common_words_{timestamp}.json"
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    
    print(f"✅ 筛选完成！")
    print(f"📊 总新闻数: {total_count}")
    print(f"✅ 被接受: {len(accepted_news)} ({len(accepted_news)/total_count*100:.1f}%)")
    print(f"❌ 被筛选: {len(filtered_out_news)} ({len(filtered_out_news)/total_count*100:.1f}%)")
    print(f"🔍 常见词过滤: {report['common_words_filtered']} 条")
    print(f"📁 结果文件:")
    print(f"   - 被接受新闻: {accepted_file}")
    print(f"   - 被筛选新闻: {filtered_file}")
    print(f"   - 统计报告: {report_file}")
    
    # 显示筛选原因统计
    print(f"\n🔍 筛选原因统计:")
    for reason, count in report['filter_results'].items():
        print(f"   - {reason}: {count} 条 ({count/len(filtered_out_news)*100:.1f}%)")

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
    
    # 🔧 新增：检查是否有其他公司名称占主导地位
    # 如果新闻中提到了其他公司名称，且这些公司名称出现次数更多，则可能是关于其他公司的新闻
    other_companies = []
    
    # 检查一些常见的公司名称
    common_companies = [
        "速腾聚创", "华为", "腾讯", "阿里巴巴", "百度", "京东", "美团", "小米",
        "联想", "比亚迪", "宁德时代", "中芯国际", "华大基因", "药明康德",
        "恒瑞医药", "迈瑞医疗", "海康威视", "大华股份", "科大讯飞", "寒武纪",
        "商汤科技", "旷视科技", "依图科技", "云从科技", "虹软科技", "当虹科技",
        "华中科技大学", "北京理工大学", "清华大学", "北京大学", "复旦大学"
    ]
    
    for other_company in common_companies:
        if other_company in text and other_company != company_name:
            other_count = text.count(other_company)
            current_count = text.count(company_name)
            
            # 🔧 改进：更智能的判断逻辑
            # 1. 如果其他公司出现次数明显更多（超过1.5倍），直接过滤
            if other_count > current_count * 1.5:
                other_companies.append((other_company, other_count, current_count))
            # 2. 如果其他公司出现次数相当（0.5-1.5倍），检查上下文
            elif other_count >= current_count * 0.5:
                # 检查其他公司是否在更重要的上下文中出现
                # 比如：标题、开头、作为主语等
                if (other_company in text[:100] or  # 在开头100字符内
                    f"{other_company}发布" in text or  # 作为发布主体
                    f"{other_company}表示" in text or  # 作为表示主体
                    f"{other_company}称" in text or    # 作为称述主体
                    f"{other_company}公告" in text):   # 作为公告主体
                    other_companies.append((other_company, other_count, current_count))
            # 3. 如果其他公司出现次数较少（0.3-0.5倍），但上下文非常重要，也要过滤
            elif other_count >= current_count * 0.3:
                # 检查是否在非常重要的上下文中出现（如标题、开头、多次作为主语）
                important_context_count = 0
                if other_company in text[:50]:  # 在开头50字符内
                    important_context_count += 1
                if f"{other_company}发布" in text:  # 作为发布主体
                    important_context_count += 1
                if f"{other_company}表示" in text:  # 作为表示主体
                    important_context_count += 1
                if f"{other_company}称" in text:  # 作为称述主体
                    important_context_count += 1
                
                # 如果有2个或以上重要上下文，认为这个公司是主导
                if important_context_count >= 2:
                    other_companies.append((other_company, other_count, current_count))
    
    # 如果发现其他公司占主导地位，则过滤掉当前公司
    if other_companies:
        # 按出现次数排序，取出现次数最多的其他公司
        other_companies.sort(key=lambda x: x[1], reverse=True)
        dominant_company, dominant_count, current_count = other_companies[0]
        print(f"🔍 发现主导公司: '{dominant_company}' (出现{dominant_count}次) vs '{company_name}' (出现{current_count}次)")
        return False
    
    # 4. 对于常见词，需要更高的密度阈值和明确的股票上下文
    if company_name in ['太阳能', '机器人', '新产业', '驱动力', '线上线下', '国新能源', '太平洋', '陆家嘴', '数字人', '创新医疗','老百姓','驱动力',"智慧农业","农产品","太平洋"]:
        # 这些词需要更高的密度和明确的股票上下文
        return company_density > 0.02 and has_stock_context
    else:
        # 其他常见词也需要一定的密度和上下文
        return company_density > 0.01 and has_stock_context

if __name__ == "__main__":
    run_final_filtering_with_common_words()
