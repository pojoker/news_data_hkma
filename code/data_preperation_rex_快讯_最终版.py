#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# =============================================================================
# 代码优化说明：
# 1. 已合并ST规则化版的历史名代码规则
# 2. 包含更谨慎的ST股票处理逻辑
# 3. 优化的ST股票映射规则，避免误识别
# 4. 完整的名称标准化和别名映射功能
# 5. 支持命令行参数和批处理
# =============================================================================

import os
import pandas as pd
import akshare as ak
import re
import json
import argparse
from bs4 import BeautifulSoup

def load_json(file_path):
    """从文件加载 JSON 数据"""
    with open(file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
    return data

def dump_json(filename, final_result):
    """将数据保存为 JSON 文件"""
    with open(filename, 'w', encoding='utf-8') as json_file:
        json.dump(final_result, json_file, ensure_ascii=False, indent=4)

def normalize_text(text):
    """标准化文本，将全角字符转换为半角字符"""
    if not text:
        return text
    
    # 全角到半角的映射
    full_to_half = {
        'Ａ': 'A', 'Ｂ': 'B', 'Ｃ': 'C', 'Ｄ': 'D', 'Ｅ': 'E', 'Ｆ': 'F', 'Ｇ': 'G', 'Ｈ': 'H', 'Ｉ': 'I', 'Ｊ': 'J',
        'Ｋ': 'K', 'Ｌ': 'L', 'Ｍ': 'M', 'Ｎ': 'N', 'Ｏ': 'O', 'Ｐ': 'P', 'Ｑ': 'Q', 'Ｒ': 'R', 'Ｓ': 'S', 'Ｔ': 'T',
        'Ｕ': 'U', 'Ｖ': 'V', 'Ｗ': 'W', 'Ｘ': 'X', 'Ｙ': 'Y', 'Ｚ': 'Z',
        'ａ': 'a', 'ｂ': 'b', 'ｃ': 'c', 'ｄ': 'd', 'ｅ': 'e', 'ｆ': 'f', 'ｇ': 'g', 'ｈ': 'h', 'ｉ': 'i', 'ｊ': 'j',
        'ｋ': 'k', 'ｌ': 'l', 'ｍ': 'm', 'ｎ': 'n', 'ｏ': 'o', 'ｐ': 'p', 'ｑ': 'q', 'ｒ': 'r', 'ｓ': 's', 'ｔ': 't',
        'ｕ': 'u', 'ｖ': 'v', 'ｗ': 'w', 'ｘ': 'x', 'ｙ': 'y', 'ｚ': 'z'
    }
    
    for full, half in full_to_half.items():
        text = text.replace(full, half)
    return text

def clean_html_content(html_content):
    """
    清理HTML内容，提取纯文本
    """
    if not html_content or pd.isna(html_content):
        return ""
    
    # 🔧 新增：检测是否为HTML内容
    def is_html_content(text):
        """检测文本是否包含HTML标签"""
        html_patterns = [
            r'<[^>]+>',  # HTML标签
            r'&[a-zA-Z0-9#]+;',  # HTML实体
            r'<script', r'<style', r'<div', r'<p', r'<span',  # 常见HTML标签
            r'</script>', r'</style>', r'</div>', r'</p>', r'</span>'
        ]
        return any(re.search(pattern, text, re.IGNORECASE) for pattern in html_patterns)
    
    # 检查是否为HTML内容
    is_html = is_html_content(html_content)
    
    if is_html:
        # HTML内容处理
        try:
            # 使用BeautifulSoup解析HTML
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # 移除不需要的标签（脚本、样式、iframe等）
            for tag in soup(['script', 'style', 'iframe', 'noscript']):
                tag.decompose()
            
            # 移除所有链接但保留文本内容
            for a in soup.find_all('a'):
                a.replace_with(a.get_text())
            
            # 提取纯文本
            text = soup.get_text()
            
            # 清理文本
            # 移除多余的空白和换行
            text = re.sub(r'\s+', ' ', text)
            # 移除HTML实体和特殊字符
            text = re.sub(r'&[a-zA-Z0-9#]+;', '', text)
            # 移除多余的标点符号
            text = re.sub(r'[^\w\s\u4e00-\u9fff，。！？；：""''（）【】、]', '', text)
            
            # 🔧 新增：移除同花顺标准结尾，避免误识别
            text = re.sub(r'关注同花顺财经[^。]*', '', text)
            text = re.sub(r'ths518[^。]*', '', text)
            text = re.sub(r'获取更多机会.*$', '', text)
            
            # 🔧 新增：字符标准化
            text = normalize_text(text)
            
            # 移除首尾空白
            text = text.strip()
            
            # 如果清理后文本太短，可能是无效内容
            if len(text) < 20:
                return ""
                
            return text
            
        except Exception as e:
            # 如果HTML解析失败，尝试简单的正则清理
            try:
                # 移除所有HTML标签
                text = re.sub(r'<[^>]+>', '', html_content)
                # 移除JavaScript相关内容
                text = re.sub(r'javascript:[^;]*;?', '', text)
                # 移除多余空白
                text = re.sub(r'\s+', ' ', text)
                # 移除HTML实体
                text = re.sub(r'&[a-zA-Z0-9#]+;', '', text)
                
                # 移除同花顺标准结尾
                text = re.sub(r'关注同花顺财经[^。]*', '', text)
                text = re.sub(r'ths518[^。]*', '', text)
                
                # 🔧 新增：字符标准化
                text = normalize_text(text)
                
                text = text.strip()
                return text if len(text) > 20 else ""
            except:
                return ""
    else:
        # 非HTML内容处理 - 只进行基本清理，保留符号
        text = html_content
        
        # 只移除同花顺标准结尾
        text = re.sub(r'关注同花顺财经[^。]*', '', text)
        text = re.sub(r'ths518[^。]*', '', text)
        text = re.sub(r'获取更多机会.*$', '', text)
        
        # 🔧 新增：字符标准化
        text = normalize_text(text)
        
        # 移除首尾空白
        text = text.strip()
        
        # 如果清理后文本太短，可能是无效内容
        if len(text) < 20:
            return ""
            
        return text

class StockNewsFilterRex:
    def __init__(self):
        # 获取 A 股公司列表并构建正则表达式
        self._init_company_patterns()
        # 构建涨跌关键词正则
        self._init_price_patterns()
        # 构建公告关键词正则
        self._init_announcement_patterns()
        
    def _init_company_patterns(self):
        """初始化公司名称正则表达式"""
        try:
            # 从akshare获取完整的A股公司信息
            print("正在从akshare获取完整A股公司列表...")
            a_share_df = ak.stock_info_a_code_name()
            
            # 过滤掉港股等非A股代码
            # A股代码格式：6位数字，以000、002、300、301、600、601、688开头

            # 🔧 新增：定义容易误识别的日常词汇型公司名
            self.ambiguous_company_names = {
                '太阳能', '机器人', '人工智能', '大数据', '新能源', '云计算', 
                '物联网', '区块链', '5G', '智能制造', '新材料', '生物医药',
                '节能环保', '高端装备', '新一代信息技术', '数字经济'
            }
            
            # 🔧 新增：处理股票名称前缀（XD、ST等）
            def normalize_stock_name(name):
                """标准化股票名称，去除XD、DR、ST等前缀"""
                # 去除除息除权等前缀
                prefixes_to_remove = ['XD', 'DR', 'XR', 'N', 'G']
                for prefix in prefixes_to_remove:
                    if name.startswith(prefix):
                        return name[len(prefix):]
                return name
            
            # 处理公司名称，建立原名和标准化名的映射
            self.name_normalization_map = {}
            for _, row in a_share_df.iterrows():
                original_name = row['name']
                normalized_name = normalize_stock_name(original_name)
                if normalized_name != original_name:
                    self.name_normalization_map[normalized_name] = original_name
            
            # 按长度排序，避免短名称匹配长名称的一部分
            company_names = sorted(a_share_df['name'].tolist(), key=len, reverse=True)
            
            # 构建公司名到代码的映射
            self.company_code_map = {}
            self.code_to_name_map = {}
            for _, row in a_share_df.iterrows():
                original_name = row['name']
                self.company_code_map[original_name] = row['code']
                self.code_to_name_map[row['code']] = original_name
                
                # 🔧 新增：为标准化名称也建立映射
                normalized_name = normalize_stock_name(original_name)
                if normalized_name != original_name:
                    self.company_code_map[normalized_name] = row['code']
                    print(f"🔧 标准化映射: {normalized_name} → {original_name} ({row['code']})")
            
            # 🔧 新增：子公司到母公司的映射
            self.subsidiary_mapping = {
                "平安人寿": "中国平安",
                "平安资管": "中国平安", 
                "平安证券": "中国平安",
                "平安信托": "中国平安",
                "平安银行": "平安银行",  # 平安银行是独立的上市公司
                # 🔧 新增：机场相关映射
                "浦东机场": "上海机场",
                "虹桥机场": "上海机场",
                "上海机场集团": "上海机场",
                "上海机场（集团）有限公司": "上海机场"
            }
            
            # 🔧 新增：规则化ST股票别名映射
            self._init_st_stock_mapping()
            
            # 扩展公司名称列表，包含子公司、ST别名和标准化名称
            normalized_names = list(self.name_normalization_map.keys())
            extended_company_names = company_names + list(self.subsidiary_mapping.keys()) + list(self.st_alias_mapping.keys()) + normalized_names
            
            # 构建正则表达式
            pattern = "|".join(map(re.escape, extended_company_names))
            self.company_pattern = re.compile(rf"({pattern})")
            
            print(f"✅ 成功加载 {len(company_names)} 家A股公司信息")
            print(f"🔍 识别出 {len(self.ambiguous_company_names)} 个容易误识别的日常词汇型公司名")
            print(f"🔗 添加了 {len(self.subsidiary_mapping)} 个子公司映射")
            print(f"🔗 添加了 {len(self.st_alias_mapping)} 个ST股票别名映射")
            print(f"代码范围示例: {list(self.code_to_name_map.keys())[:10]}")
            
        except Exception as e:
            print(f"❌ 无法加载A股公司信息: {e}")
            print("尝试使用备用方案...")
            # 备用方案：使用基本的A股公司列表
            self._init_backup_companies()
    
    def _init_st_stock_mapping(self):
        """初始化ST股票规则化映射"""
        # 获取所有ST股票
        a_share_df = ak.stock_info_a_code_name()
        # 注意：这里过滤A股代码，但在_init_company_patterns中已经获取了完整数据
        # 可以考虑复用之前的数据，避免重复调用akshare
        #a_share_df = a_share_df[
        #    a_share_df['code'].str.match(r'^(000|002|300|301|600|601|688)\d{3}$')
        #]
        st_stocks = a_share_df[a_share_df['name'].str.contains('ST')]
        
        # 构建ST股票别名映射（更谨慎的规则）
        self.st_alias_mapping = {}
        
        for _, row in st_stocks.iterrows():
            stock_name = row['name']
            # 注意：stock_code变量定义了但未使用，可以考虑移除
            # stock_code = row['code']
            
            # 提取公司名称（去掉ST前缀）
            if stock_name.startswith('*ST'):
                company_name = stock_name[3:]  # 去掉*ST
                # 只添加明确的ST别名，不添加纯公司名
                self.st_alias_mapping[f"ST{company_name}"] = stock_name
                # 不添加 company_name -> stock_name 的映射，避免误识别
            elif stock_name.startswith('ST'):
                company_name = stock_name[2:]  # 去掉ST
                # 只添加明确的*ST别名，不添加纯公司名
                self.st_alias_mapping[f"*ST{company_name}"] = stock_name
                # 不添加 company_name -> stock_name 的映射，避免误识别
        
        print(f"🔧 谨慎规则化处理了 {len(st_stocks)} 只ST股票")
        print(f"  其中*ST股票: {len(st_stocks[st_stocks['name'].str.startswith('*ST')])} 只")
        print(f"  其中ST股票: {len(st_stocks[st_stocks['name'].str.startswith('ST') & ~st_stocks['name'].str.startswith('*ST')])} 只")
        print(f"  别名映射数量: {len(self.st_alias_mapping)} 个（仅包含明确ST前缀的别名）")
    
    def _init_backup_companies(self):
        """备用公司列表初始化"""
        # 一些基本的A股公司作为备用
        backup_companies = {
            "000001": "平安银行", "000002": "万科A", "000858": "五粮液",
            "002594": "比亚迪", "002415": "海康威视", "300750": "宁德时代",
            "600519": "贵州茅台", "600036": "招商银行", "600000": "浦发银行",
            "000876": "新希望", "002304": "洋河股份", "000063": "中兴通讯",
            "601398": "工商银行", "601318": "中国平安", "000563": "陕国投Ａ",
            # 🔧 新增：上海机场
            "600009": "上海机场"
        }
        
        self.company_code_map = {v: k for k, v in backup_companies.items()}
        self.code_to_name_map = backup_companies
        
        # 🔧 新增：备用方案的名称标准化映射
        self.name_normalization_map = {}
        
        # 备用方案也要设置ambiguous_company_names和subsidiary_mapping
        self.ambiguous_company_names = {
            '太阳能', '机器人', '人工智能', '大数据', '新能源', '云计算', 
            '物联网', '区块链', '5G', '智能制造', '新材料', '生物医药',
            '节能环保', '高端装备', '新一代信息技术', '数字经济'
        }
        
        self.subsidiary_mapping = {
            "平安人寿": "中国平安",
            "平安资管": "中国平安", 
            "平安证券": "中国平安",
            "平安信托": "中国平安",
            "平安银行": "平安银行",
            # 🔧 新增：机场相关映射
            "浦东机场": "上海机场",
            "虹桥机场": "上海机场",
            "上海机场集团": "上海机场",
            "上海机场（集团）有限公司": "上海机场"
        }
        
        # 备用ST映射
        self.st_alias_mapping = {
            "陕国投A": "陕国投Ａ",
            "ST美古": "*ST美谷",
            "ST美谷": "*ST美谷"
        }
        
        company_names = list(self.company_code_map.keys())
        normalized_names = list(self.name_normalization_map.keys())
        extended_company_names = company_names + list(self.subsidiary_mapping.keys()) + list(self.st_alias_mapping.keys()) + normalized_names
        pattern = "|".join(map(re.escape, extended_company_names))
        self.company_pattern = re.compile(rf"({pattern})")
        
        print(f"⚠️ 使用备用公司列表: {len(company_names)} 家公司")
    
    def _init_price_patterns(self):
        """初始化价格变动关键词正则表达式"""
        # 涨跌相关关键词
        price_keywords = [
            "上涨", "下跌", "涨幅", "跌幅", "涨停", "跌停", 
            "涨", "跌", "升", "降", "飙升", "暴跌", "大涨", "大跌",
            "点数", "股价", "价格", "市值", "收盘价", "开盘价",
            "涨势", "跌势", "波动", "震荡", "反弹", "回调"
        ]
        self.price_pattern = re.compile("|".join(map(re.escape, price_keywords)))
    
    def _init_announcement_patterns(self):
        """初始化公告关键词正则表达式"""
        announcement_keywords = ["公告", "通告", "声明", "披露", "发布"]
        #announcement_keywords = []
        self.announcement_pattern = re.compile("|".join(map(re.escape, announcement_keywords)))
    
    def extract_companies(self, text: str) -> list[str]:
        """
        从文本中提取所有匹配的 A 股公司简称，去重后返回列表。
        包含子公司到母公司的映射和ST股票规则化映射。
        """
        if not text:
            return []
        
        # 🔧 新增：字符标准化
        normalized_text = normalize_text(text)
        
        matches = self.company_pattern.findall(normalized_text)
        unique_matches = list(set(matches))
        
        # 🔧 新增：处理子公司映射和ST股票映射
        final_companies = []
        for company in unique_matches:
            # 🔧 新增：更谨慎的ST股票处理
            if company in self.st_alias_mapping:
                # 检查文本中是否明确包含"ST"字样
                if self._contains_st_indicator(normalized_text):
                    standard_name = self.st_alias_mapping[company]
                    if standard_name not in final_companies:
                        final_companies.append(standard_name)
                # 如果文本中没有明确ST字样，则不进行ST股票映射
            # 处理子公司映射
            elif company in self.subsidiary_mapping:
                parent_company = self.subsidiary_mapping[company]
                if parent_company not in final_companies:
                    final_companies.append(parent_company)
            else:
                # 如果不是别名或子公司，直接添加
                if company not in final_companies:
                    final_companies.append(company)
        
        return final_companies
    
    def _contains_st_indicator(self, text: str) -> bool:
        """
        检查文本中是否明确包含ST相关指示符
        只有在文本中明确出现ST字样时，才进行ST股票判断
        """
        if not text:
            return False
        
        # 检查是否包含明确的ST指示符
        st_indicators = [
            'ST', '*ST', 'st', '*st',  # 基本ST标识
            'ST股票', '*ST股票', 'st股票', '*st股票',  # 带"股票"的ST标识
            'ST公司', '*ST公司', 'st公司', '*st公司',  # 带"公司"的ST标识
            '风险警示', '退市风险', '特别处理',  # ST相关术语
        ]
        
        for indicator in st_indicators:
            if indicator in text:
                return True
        
        return False
    
    def mentions_price_change(self, text: str) -> bool:
        """
        检查文本中是否出现涨跌关键词
        """
        if not text:
            return False
        return bool(self.price_pattern.search(text))
    
    def is_announcement(self, text: str) -> bool:
        """
        检查文本中是否包含公告关键词
        """
        if not text:
            return False
        return bool(self.announcement_pattern.search(text))
    
    def get_company_code(self, company_name: str) -> str:
        """
        根据公司名称获取股票代码
        """
        return self.company_code_map.get(company_name, "")
    
    def is_really_about_company(self, text: str, company_name: str) -> bool:
        """
        验证文本是否真正在讨论该上市公司，而不仅仅是包含相同的词汇
        对于容易误识别的日常词汇型公司名，需要更严格的验证
        """
        if not text or not company_name:
            return False
        
        # 对于非日常词汇型公司名，使用宽松验证
        if company_name not in self.ambiguous_company_names:
            return True
        
        # 对于日常词汇型公司名，需要更严格的验证
        company_code = self.get_company_code(company_name)
        if not company_code:
            return False
        
        # 🔧 改进：首先检查明确的上市公司标识符
        strict_indicators = [
            company_code,  # 股票代码 (如 000591)
            f"{company_name}股份",  # 公司全称
            f"{company_name}集团",  # 集团形式
            f"{company_name}有限公司",  # 完整公司名
            f"{company_name}（{company_code}）",  # 带代码的完整形式
            f"{company_name}({company_code})",  # 带代码的完整形式
        ]
        
        # 检查是否包含任何严格标识符
        for indicator in strict_indicators:
            if indicator in text:
                return True
        
        # 🔧 改进：更严格的上下文验证
        # 只有在明确的股票/投资/公司运营相关上下文中才认为是有效的
        strong_stock_context_keywords = [
            "股价", "涨停", "跌停", "股票", "证券", "上市公司", "A股", "沪深",
            "市值", "股东", "董事长", "CEO", "总经理", "董事会", "股东大会",
            "财报", "年报", "季报", "业绩", "营收", "利润", "净利润",
            "分红", "派息", "停牌", "复牌", "重组", "并购", "收购",
            "公告", "披露", "投资者关系", "IR", "路演", "机构调研",
            f"{company_name}宣布", f"{company_name}发布", f"{company_name}表示",
            f"{company_name}CEO", f"{company_name}董事长", f"{company_name}总裁"
        ]
        
        # 🔧 新增：检查是否在业务范围描述中（这种情况下应该过滤掉）
        business_scope_indicators = [
            "经营范围", "业务范围", "主营业务", "经营项目", "业务包括",
            "服务范围", "产品包括", "涉及领域", "业务涵盖", "主要产品",
            "经营范围含", "经营范围包括", "从事", "专业从事", "主要经营"
        ]
        
        # 如果是在业务范围描述中出现的公司名，很可能是误识别
        has_business_scope = any(indicator in text for indicator in business_scope_indicators)
        if has_business_scope:
            # 在业务范围中出现的公司名，除非有明确的公司标识符，否则认为是误识别
            for indicator in strict_indicators:
                if indicator in text:
                    return True
            return False
        
        # 检查是否在强上下文中提及
        has_strong_context = any(keyword in text for keyword in strong_stock_context_keywords)
        if has_strong_context:
            return True
        
        # 🔧 新增：如果是新闻标题中明确提及公司，且不是业务范围描述，可能是有效的
        # 但需要排除一些常见的误识别场景
        exclude_patterns = [
            "成立", "注册", "新设", "设立", "投资", "合资", "子公司", "分公司",
            "有限公司成立", "科技有限公司", "管理有限公司", "投资有限公司"
        ]
        
        is_company_establishment = any(pattern in text for pattern in exclude_patterns)
        if is_company_establishment:
            return False
        
        # 如果都没有匹配，可能是误识别
        return False
    
    def is_stock_related_non_price_news(self, text: str, title: str = "", strict_mode: bool = False) -> bool:
        """
        判断是否是涉及上市公司且包含除股价外内容的新闻
        相当于原来的 p_filter_stock_price_news
        
        Args:
            text: 快讯内容
            title: 快讯标题
            strict_mode: 是否使用严格模式（严格模式会禁用快速通过逻辑并加强筛选条件）
        """
        if not text:
            return False
        
        # 🔧 新增：标题过滤逻辑
        if title:
            # 检查标题是否包含涨跌幅信息
            title_price_patterns = [
                r"涨\d+\.?\d*%", r"跌\d+\.?\d*%", r"涨\d+\.?\d*点", r"跌\d+\.?\d*点",
                r"涨\d+\.?\d*元", r"跌\d+\.?\d*元", r"涨\d+\.?\d*块", r"跌\d+\.?\d*块",
                r"涨\d+\.?\d*个点", r"跌\d+\.?\d*个点", r"涨\d+\.?\d*个百分点", r"跌\d+\.?\d*个百分点"
            ]
            
            # 如果标题包含涨跌幅信息，直接过滤
            if any(re.search(pattern, title) for pattern in title_price_patterns):
                return False
            
            # 检查标题是否包含市场数据类词汇
            title_market_indicators = [
                "午评", "早评", "收评", "盘面", "盘面分析", "市场表现", "市场走势",
                "板块表现", "板块涨幅", "板块跌幅", "个股表现", "个股涨幅", "个股跌幅",
                "涨跌", "涨跌幅", "涨停", "跌停", "涨停潮", "跌停潮",
                "全线上涨", "全线下挫", "全线飘红", "全线飘绿", "全线收涨", "全线收跌"
            ]
            
            if any(indicator in title for indicator in title_market_indicators):
                return False
        
        # 🔧 新增：非常用词公司快速通过逻辑（仅在非严格模式下生效）
        # 如果标题不是涨跌类新闻，且包含非常用词公司，直接通过
        if not strict_mode and title:
            # 检查标题中是否有非常用词公司
            title_companies = self.extract_companies(title)
            title_companies = [c for c in title_companies if c != "同花顺"]
            
            # 检查是否有非常用词公司（不在ambiguous_company_names中的公司）
            non_ambiguous_companies = [c for c in title_companies if c not in self.ambiguous_company_names]
            
            if non_ambiguous_companies:
                # 标题中有非常用词公司，且标题不是涨跌类新闻，直接通过
                return True
        
        # 检查是否提到公司
        companies = self.extract_companies(text)
        if not companies:
            return False
        
        # 🔧 优化：过滤掉"同花顺"（因为它出现在所有新闻的标准结尾中）
        valid_companies = [c for c in companies if c != "同花顺"]
        if not valid_companies:
            return False
        
        # 🔧 优化：检查是否包含业务相关内容
        business_keywords = [
            "投资", "合作", "协议", "项目", "业务", "战略", "收购", "重组", 
            "产品", "技术", "研发", "销售", "营收", "利润", "增长", "发展",
            "市场", "客户", "服务", "创新", "转型", "扩张", "布局", "公告",
            "披露", "发布", "合同", "订单", "生产", "制造", "建设"
        ]
        business_pattern = re.compile("|".join(map(re.escape, business_keywords)))
        has_business_content = bool(business_pattern.search(text))
        
        # 如果没有业务相关内容，可能不是我们要的新闻
        if not has_business_content:
            return False
        
        # 🔧 严格模式：额外的筛选条件
        if strict_mode:
            # 严格模式下，要求更强的业务相关性
            strong_business_indicators = [
                "公告", "披露", "发布", "财报", "年报", "季报", "业绩",
                "董事会", "股东大会", "分红", "收购", "重组", "合作", "签署",
                "营收", "利润", "增长", "项目", "订单", "合同", "协议"
            ]
            
            has_strong_business = any(indicator in text for indicator in strong_business_indicators)
            
            # 严格模式下，如果没有强业务指标，需要更高的公司密度
            if not has_strong_business:
                # 计算公司密度（简化版本）
                company_density = sum(text.count(c) for c in valid_companies) / len(text.split())
                if company_density < 0.03:  # 严格模式要求3%以上的密度
                    return False
            
            # 严格模式下，过滤更多的市场分析类内容
            strict_filter_patterns = [
                "市场观点", "投资建议", "分析师", "机构观点", "预期", "预计",
                "建议", "推荐", "关注", "看好", "看空", "把握机会",
                "投资策略", "投资机会", "概念股", "主题投资"
            ]
            
            strict_filter_count = sum(text.count(pattern) for pattern in strict_filter_patterns)
            if strict_filter_count >= 2:  # 如果包含2个以上严格过滤词汇，则过滤
                return False
        
        # 检查是否主要是价格新闻
        if self.mentions_price_change(text):
            # 🔧 优化：调整价格词汇密度阈值，并考虑文本长度
            price_words = self.price_pattern.findall(text)
            total_words = len(text.split())
            price_ratio = len(price_words) / total_words if total_words > 0 else 0
            
            # 如果是短文本且价格词汇密度高，可能主要是价格新闻
            if len(text) < 200 and price_ratio > 0.15:
                return False
            elif price_ratio > 0.25:  # 长文本的价格词汇密度阈值更高
                return False
        
        return True


def filter_stock_price_news_rex(news_content_list, timestamp_list, title_list, strict_mode=False):
    """
    使用正则表达式筛选股票价格相关新闻
    """
    filter_engine = StockNewsFilterRex()
    output = []
    
    for index, content in enumerate(news_content_list):
        try:
            # 清理HTML内容
            cleaned_content = clean_html_content(content)
            if not cleaned_content:
                continue
            
            # 检查是否是公告
            if filter_engine.is_announcement(cleaned_content):
                # 对于公告，直接提取公司
                companies = filter_engine.extract_companies(cleaned_content)
                # 过滤掉"同花顺"
                valid_companies = [c for c in companies if c != "同花顺"]
                
                for company_name in valid_companies:
                    company_code = filter_engine.get_company_code(company_name)
                    if company_code and filter_engine.is_really_about_company(cleaned_content, company_name):
                        company_chn_name = filter_engine.code_to_name_map.get(company_code, company_name)
                        result_ = {
                            'time': timestamp_list[index], 
                            'title': title_list[index], 
                            'content': cleaned_content, 
                            'code_name': company_code, 
                            'company_chn_name': company_chn_name,
                            'type': 'announcement'
                        }
                        output.append(result_)
            else:
                # 对于非公告新闻，检查是否是相关的非纯股价新闻
                if filter_engine.is_stock_related_non_price_news(cleaned_content, title_list[index], strict_mode):
                    companies = filter_engine.extract_companies(cleaned_content)
                    # 过滤掉"同花顺"
                    valid_companies = [c for c in companies if c != "同花顺"]
                    
                    for company_name in valid_companies:
                        company_code = filter_engine.get_company_code(company_name)
                        if company_code and filter_engine.is_really_about_company(cleaned_content, company_name):
                            company_chn_name = filter_engine.code_to_name_map.get(company_code, company_name)
                            result_ = {
                                'time': timestamp_list[index], 
                                'title': title_list[index], 
                                'content': cleaned_content, 
                                'code_name': company_code, 
                                'company_chn_name': company_chn_name,
                                'type': 'news'
                            }
                            output.append(result_)
                            
        except Exception as e:
            print(f"处理第 {index} 条新闻时出错: {e}")
            continue
    
    return output


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='快讯股票新闻筛选工具')
    parser.add_argument('--input', '-i', default='data/同花顺_快讯_2023.csv', 
                       help='输入CSV文件路径 (默认: data/同花顺_快讯_2023.csv)')
    parser.add_argument('--output', '-o', default='data/ths_kx_2023_filter_stock_price_quicknews_rex_complete.json',
                       help='输出JSON文件路径 (默认: data/ths_kx_2023_filter_stock_price_quicknews_rex_complete.json)')
    parser.add_argument('--batch-size', '-b', type=int, default=10000,
                       help='批处理大小 (默认: 10000)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("🚀 快讯股票筛选 (完整版) 启动")
    print("=" * 60)
    
    # 检查输入文件是否存在
    if not os.path.exists(args.input):
        print(f"❌ 输入文件不存在: {args.input}")
        return
    
    # 加载快讯数据
    print(f"📰 加载快讯数据: {args.input}")
    df = pd.read_csv(args.input)
    print("📊 CSV列名:", df.columns.tolist())
    
    news_content_list = df['content'].tolist()
    timestamp_list = df['datetime'].tolist()  # 快讯文件使用 datetime 列名
    title_list = df['title'].tolist()
    
    total_count = len(news_content_list)
    print(f"✅ 成功加载 {total_count} 条快讯")
    
    # 批处理
    batch_size = args.batch_size
    all_output = []
    
    for start_idx in range(0, total_count, batch_size):
        end_idx = min(start_idx + batch_size, total_count)
        current_batch = end_idx - start_idx
        
        print(f"\n📈 处理批次: {start_idx} 到 {end_idx-1} (共 {current_batch} 条)")
        
        batch_output = filter_stock_price_news_rex(
            news_content_list[start_idx:end_idx], 
            timestamp_list[start_idx:end_idx], 
            title_list[start_idx:end_idx]
        )
        
        all_output.extend(batch_output)
        
        print(f"✅ 当前批次筛选结果: {len(batch_output)} 条")
        print(f"📊 累计筛选结果: {len(all_output)} 条")
    
    # 保存完整结果
    print(f"\n💾 保存完整结果到: {args.output}")
    dump_json(args.output, all_output)
    
    print("\n" + "=" * 60)
    print("📊 筛选完成 - 最终统计结果")
    print("=" * 60)
    print(f"总计处理: {total_count} 条快讯")
    print(f"✅ 通过筛选: {len(all_output)} 条 ({len(all_output)/total_count*100:.1f}%)")
    print(f"📁 结果保存到: {args.output}")
    
    # 显示样本结果
    if all_output:
        print(f"\n📋 前3条通过筛选的快讯:")
        for i, item in enumerate(all_output[:3]):
            print(f"\n第{i+1}条:")
            print(f"  标题: {item['title']}")
            print(f"  类型: {item['type']}")
            print(f"  公司: {item['company_chn_name']} ({item['code_name']})")
            print(f"  内容预览: {item['content'][:100]}...")
    
    # 统计公司分布
    if all_output:
        print(f"\n📈 公司分布统计:")
        company_counts = {}
        for item in all_output:
            company = item['company_chn_name']
            company_counts[company] = company_counts.get(company, 0) + 1
        
        # 显示前10家公司
        top_companies = sorted(company_counts.items(), key=lambda x: x[1], reverse=True)[:10]
        for i, (company, count) in enumerate(top_companies, 1):
            print(f"  {i:2d}. {company}: {count} 条")


if __name__ == "__main__":
    main()
