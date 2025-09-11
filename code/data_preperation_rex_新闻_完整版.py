#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
import akshare as ak
import re
import json
import argparse
import glob
from bs4 import BeautifulSoup
import jieba

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
        # 字母
        'Ａ': 'A', 'Ｂ': 'B', 'Ｃ': 'C', 'Ｄ': 'D', 'Ｅ': 'E', 'Ｆ': 'F', 'Ｇ': 'G', 'Ｈ': 'H', 'Ｉ': 'I', 'Ｊ': 'J',
        'Ｋ': 'K', 'Ｌ': 'L', 'Ｍ': 'M', 'Ｎ': 'N', 'Ｏ': 'O', 'Ｐ': 'P', 'Ｑ': 'Q', 'Ｒ': 'R', 'Ｓ': 'S', 'Ｔ': 'T',
        'Ｕ': 'U', 'Ｖ': 'V', 'Ｗ': 'W', 'Ｘ': 'X', 'Ｙ': 'Y', 'Ｚ': 'Z',
        'ａ': 'a', 'ｂ': 'b', 'ｃ': 'c', 'ｄ': 'd', 'ｅ': 'e', 'ｆ': 'f', 'ｇ': 'g', 'ｈ': 'h', 'ｉ': 'i', 'ｊ': 'j',
        'ｋ': 'k', 'ｌ': 'l', 'ｍ': 'm', 'ｎ': 'n', 'ｏ': 'o', 'ｐ': 'p', 'ｑ': 'q', 'ｒ': 'r', 'ｓ': 's', 'ｔ': 't',
        'ｕ': 'u', 'ｖ': 'v', 'ｗ': 'w', 'ｘ': 'x', 'ｙ': 'y', 'ｚ': 'z',
        # 数字
        '０': '0', '１': '1', '２': '2', '３': '3', '４': '4', '５': '5', '６': '6', '７': '7', '８': '8', '９': '9',
        # 标点符号
        '—': '-', '－': '-', '–': '-',  # 破折号
        '（': '(', '）': ')',  # 括号
        '【': '[', '】': ']',  # 方括号
        '｛': '{', '｝': '}',  # 花括号
        '《': '<', '》': '>',  # 尖括号
        '：': ':', '；': ';',  # 冒号分号
        '，': ',', '。': '.',  # 逗号句号
        '！': '!', '？': '?',  # 感叹号问号
        '＂': '"', '＇': "'",  # 引号
        '％': '%', '＃': '#',  # 百分号井号
        '＆': '&', '＊': '*',  # 与号星号
        '＋': '+', '＝': '=',  # 加号等号
        '／': '/', '＼': '\\',  # 斜杠反斜杠
        '｜': '|', '～': '~',  # 竖线波浪号
        '＠': '@', '＄': '$',  # 艾特美元符号
        # 全角空格
        '　': ' ',  # 全角空格转半角空格
        # 全角小数点
        '．': '.'   # 全角小数点转半角小数点
    }
    
    for full, half in full_to_half.items():
        text = text.replace(full, half)
    
    # 🔧 新增：处理连续空格，将多个空格合并为单个空格
    text = re.sub(r'\s+', ' ', text)
    
    return text

def clean_html_content(html_content):
    """
    清理HTML内容，提取纯文本（改进版本）
    """
    if not html_content or pd.isna(html_content):
        return ""
    
    try:
        # 使用BeautifulSoup解析HTML
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # 移除不需要的标签（脚本、样式、iframe、图片等）
        for tag in soup(['script', 'style', 'iframe', 'noscript', 'img', 'link', 'meta']):
            tag.decompose()
        
        # 移除所有链接但保留文本内容
        for a in soup.find_all('a'):
            a.replace_with(a.get_text())
        
        # 移除所有表单元素
        for form in soup.find_all(['form', 'input', 'button', 'select', 'textarea']):
            form.decompose()
        
        # 移除所有表格但保留文本内容
        for table in soup.find_all('table'):
            table.replace_with(table.get_text())
        
        # 提取纯文本
        text = soup.get_text()
        
        # 清理文本
        # 移除多余的空白和换行
        text = re.sub(r'\s+', ' ', text)
        
        # 移除HTML实体和特殊字符
        text = re.sub(r'&[a-zA-Z0-9#]+;', '', text)
        
        # 移除多余的标点符号（保留数字、字母、中文、常用标点）
        # 🔧 修复：保留括号，避免公司代码被错误处理
        text = re.sub(r'[^\w\s\u4e00-\u9fff，。！？；：""''（）【】、.%()]', '', text)
        
        # 🔧 新增：移除同花顺标准结尾，避免误识别
        text = re.sub(r'关注同花顺财经[^。]*', '', text)
        text = re.sub(r'ths518[^。]*', '', text)
        text = re.sub(r'获取更多机会.*$', '', text)
        
        # 🔧 新增：移除图片链接（改进版本）
        text = re.sub(r'image_address\s*"[^"]*"', '', text)
        text = re.sub(r'image_address\s*=\s*"[^"]*"', '', text)
        text = re.sub(r'image_address\s*=\s*[^)]*\)', '', text)
        
        # 🔧 新增：移除其他常见的HTML残留
        text = re.sub(r'data:image[^)]*\)', '', text)
        text = re.sub(r'http[s]?://[^\s]*\.(png|jpg|jpeg|gif|bmp|webp)', '', text)
        
        # 🔧 新增：移除CSS样式残留
        text = re.sub(r'font-family:[^;]*;', '', text)
        text = re.sub(r'font-size:[^;]*;', '', text)
        text = re.sub(r'color:[^;]*;', '', text)
        text = re.sub(r'background:[^;]*;', '', text)
        
        # 🔧 新增：移除JavaScript残留
        text = re.sub(r'javascript:[^;]*;?', '', text)
        text = re.sub(r'window\.[^;]*;?', '', text)
        text = re.sub(r'document\.[^;]*;?', '', text)
        
        # 🔧 新增：移除特殊字符序列
        text = re.sub(r'[A-Za-z0-9]{20,}', '', text)  # 移除长字符串（可能是编码残留）
        
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
            
            # 🔧 新增：移除图片链接
            text = re.sub(r'image_address\s*"[^"]*"', '', text)
            text = re.sub(r'http[s]?://[^\s]*\.(png|jpg|jpeg|gif|bmp|webp)', '', text)
            
            # 🔧 新增：字符标准化
            text = normalize_text(text)
            
            text = text.strip()
            return text if len(text) > 20 else ""
        except:
            return ""

class StockNewsFilterRex:
    def __init__(self):
        # 初始化jieba分词
        jieba.initialize()
        # 获取 A 股公司列表并构建正则表达式
        self._init_company_patterns()
        # 构建涨跌关键词正则
        self._init_price_patterns()
        # 构建公告关键词正则
        self._init_announcement_patterns()
        # 初始化业务关键词
        self._init_business_patterns()
        
    def _init_company_patterns(self):
        """初始化公司名称正则表达式"""
        try:
            # 从akshare获取完整的A股公司信息
            # 优先使用本地缓存的A股公司列表JSON文件，若不存在则从akshare获取并缓存
            import os
            import json

            cache_file = "data/ashare_name_to_code_20250826_1112_with_aliases.json"
            if os.path.exists(cache_file):
                print(f"🔄 正在从本地缓存加载A股公司列表: {cache_file}")
                with open(cache_file, "r", encoding="utf-8") as f:
                    a_share_data = json.load(f)
                import pandas as pd
                
                # 检查数据格式并转换为DataFrame
                if isinstance(a_share_data, list):
                    # 如果是列表格式，直接转换为DataFrame
                    a_share_df = pd.DataFrame(a_share_data)
                else:
                    # 如果是字典格式，转换为记录列表
                    records = []
                    for name, info in a_share_data.items():
                        records.append({
                            'name': name,  # 使用字典的key作为公司名称
                            'code': info['code']
                        })
                    a_share_df = pd.DataFrame(records)
                
                # 🔧 修复：不去重，保留所有别名，这样可以匹配不同格式的公司名称
                # a_share_df = a_share_df.drop_duplicates(subset=['code'])
            else:
                print("正在从akshare获取完整A股公司列表...")
                a_share_df = ak.stock_info_a_code_name()
                # 缓存到本地
                try:
                    a_share_df.to_json(cache_file, orient="records", force_ascii=False, indent=2)
                    print(f"✅ 已缓存A股公司列表到: {cache_file}")
                except Exception as e:
                    print(f"⚠️ 缓存A股公司列表失败: {e}")
            

            # 🔧 新增：定义容易误识别的日常词汇型公司名
            self.ambiguous_company_names = {'太阳能', '机器人', '新产业', '驱动力', '线上线下', '国新能源', '太平洋', '陆家嘴', '数字人', '创新医疗','老百姓','驱动力',"智慧农业","农产品"
            }
            
            # 按长度排序，避免短名称匹配长名称的一部分
            company_names = sorted(a_share_df['name'].tolist(), key=len, reverse=True)
            
            # 构建公司名到代码的映射
            self.company_code_map = {}
            self.code_to_name_map = {}
            for _, row in a_share_df.iterrows():
                self.company_code_map[row['name']] = row['code']
                self.code_to_name_map[row['code']] = row['name']
            
            # 🔧 修正：只映射到确实存在的A股公司
            self.subsidiary_mapping = {
                # 平安系（已验证）
                "平安人寿": "中国平安",
                "平安资管": "中国平安", 
                "平安证券": "中国平安",
                "平安信托": "中国平安",
                "平安银行": "平安银行",  # 平安银行是独立的上市公司
                
                # 汽车行业（只保留确认存在的）
                "广汽埃安": "广汽集团",  # 已确认：601238
                "埃安": "广汽集团",
                
                # 消费品（只保留确认存在的）
                "茅台": "贵州茅台",  # 应该存在
                "五粮液": "五粮液",   # 应该存在
            }
            
            # 🔧 新增：规则化ST股票别名映射
            self._init_st_stock_mapping()
            
            # 扩展公司名称列表，包含子公司和ST别名
            extended_company_names = company_names + list(self.subsidiary_mapping.keys()) + list(self.st_alias_mapping.keys())
            
            # 🔧 修复：处理空格问题，将公司名称中的空格替换为可选的空格模式
            # 这样"万  科Ａ"可以匹配"万科Ａ"、"万 科Ａ"、"万  科Ａ"等
            processed_names = []
            for name in extended_company_names:
                # 将连续空格替换为可选的空格模式
                processed_name = re.sub(r'\s+', r'\\s*', re.escape(name))
                processed_names.append(processed_name)
            
            # 构建正则表达式
            pattern = "|".join(processed_names)
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
        #a_share_df = a_share_df[
        #    a_share_df['code'].str.match(r'^(000|002|300|301|600|601|688)\d{3}$')
        #]
        st_stocks = a_share_df[a_share_df['name'].str.contains('ST')]
        
        # 构建ST股票别名映射（更谨慎的规则）
        self.st_alias_mapping = {}
        
        for _, row in st_stocks.iterrows():
            stock_name = row['name']
            stock_code = row['code']
            
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
            "601398": "工商银行", "601318": "中国平安", "000563": "陕国投Ａ"
        }
        
        self.company_code_map = {v: k for k, v in backup_companies.items()}
        self.code_to_name_map = backup_companies
        
        # 备用方案也要设置ambiguous_company_names和subsidiary_mapping
        self.ambiguous_company_names = {
            '太阳能', '机器人', '新产业','驱动力','线上线下','国新能源'
        }
        
        self.subsidiary_mapping = {
            "平安人寿": "中国平安",
            "平安资管": "中国平安", 
            "平安证券": "中国平安",
            "平安信托": "中国平安",
            "平安银行": "平安银行"
        }
        
        # 备用ST映射
        self.st_alias_mapping = {
            "陕国投A": "陕国投Ａ",
            "ST美古": "*ST美谷",
            "ST美谷": "*ST美谷"
        }
        
        company_names = list(self.company_code_map.keys())
        extended_company_names = company_names + list(self.subsidiary_mapping.keys()) + list(self.st_alias_mapping.keys())
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
        self.announcement_pattern = re.compile("|".join(map(re.escape, announcement_keywords)))
    
    def _init_business_patterns(self):
        """初始化业务关键词正则表达式"""
        self.business_keywords = [
            "投资", "合作", "协议", "项目", "业务", "战略", "收购", "重组", 
            "产品", "技术", "研发", "销售", "营收", "利润", "增长", "发展",
            "市场", "客户", "服务", "创新", "转型", "扩张", "布局", "公告",
            "披露", "发布", "合同", "订单", "生产", "制造", "建设",
            "董事会", "股东大会", "年报", "季报", "财报", "业绩", "分红"
        ]
        self.business_pattern = re.compile("|".join(map(re.escape, self.business_keywords)))
    
    def improved_word_segmentation(self, text: str) -> list:
        """改进的中文分词方法"""
        if not text:
            return []
        
        # 使用jieba进行中文分词
        words = list(jieba.cut(text))
        
        # 过滤掉空字符串和纯标点符号
        filtered_words = []
        for word in words:
            word = word.strip()
            if len(word) > 0 and not re.match(r'^[^\w\u4e00-\u9fff]+$', word):
                filtered_words.append(word)
        
        return filtered_words

    def calculate_price_density_improved(self, text: str) -> float:
        """改进的价格密度计算"""
        if not text:
            return 0.0
        
        # 使用改进的分词
        words = self.improved_word_segmentation(text)
        if not words:
            return 0.0
        
        # 统计价格相关词汇
        price_words = self.price_pattern.findall(text)
        
        # 计算密度
        price_density = len(price_words) / len(words) if len(words) > 0 else 0.0
        
        return price_density

    def calculate_company_density(self, text: str, company_name: str) -> float:
        """计算公司名在文本中的密度和重要性"""
        if not text or not company_name:
            return 0.0
        
        # 🔧 修复：处理全角半角问题和映射问题
        company_count = text.count(company_name)
        
        # 如果没找到，尝试全角半角转换
        if company_count == 0:
            # 半角转全角
            half_to_full = str.maketrans(
                'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
                'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ０１２３４５６７８９'
            )
            # 全角转半角  
            full_to_half = str.maketrans(
                'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ０１２３４５６７８９',
                'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
            )
            
            # 尝试不同的版本
            alt_names = [
                company_name.translate(half_to_full),  # 转全角
                company_name.translate(full_to_half),  # 转半角
            ]
            
            for alt_name in alt_names:
                if alt_name in text:
                    company_count = text.count(alt_name)
                    break
        
        # 如果还是没找到，尝试搜索原始匹配（子公司映射）
        if company_count == 0:
            original_matches = [key for key, value in self.subsidiary_mapping.items() if value == company_name]
            for original in original_matches:
                if original in text:
                    company_count = text.count(original)
                    break
        
        if company_count == 0:
            return 0.0
        
        # 使用改进的分词
        words = self.improved_word_segmentation(text)
        if not words:
            return 0.0
        
        # 基础密度：公司名出现次数 / 总词数
        basic_density = company_count / len(words)
        
        # 检查是否在列举或无关上下文中
        listing_contexts = [
            f"包括{company_name}", f"例如{company_name}", f"如{company_name}",
            f"{company_name}、", f"、{company_name}、", f"、{company_name}等",
            f"{company_name}等企业", f"{company_name}等公司", f"{company_name}等品牌",
            "配套产业", "相关企业", "相关公司", "同类企业", "类似公司"
        ]
        
        irrelevant_contexts = [
            "经营范围", "业务范围", "主营业务", "经营项目", "业务包括",
            "服务范围", "产品包括", "涉及领域", "业务涵盖", "主要产品",
            "类似", "例如", "包括但不限于", "等企业", "等公司", "等品牌",
            "成立", "注册", "新设", "设立", "子公司", "分公司",
            "历史上", "曾经", "过去", "以前", "当年", "那时候"
        ]
        
        has_listing_context = any(context in text for context in listing_contexts)
        has_irrelevant_context = any(context in text for context in irrelevant_contexts)
        
        # 如果在列举或无关上下文中，适度降权（不要过度）
        if has_listing_context or has_irrelevant_context:
            # 列举上下文中的公司名重要性较低，但不要降权太厉害
            weighted_density = basic_density * 0.7  # 降权处理 (从0.5提升到0.7)
        else:
            # 🔧 增强位置权重：考虑标题、开头位置
            position_weight = 1.0
            
            # 检查是否在前100字符（标题/开头）
            if company_name in text[:100]:
                position_weight = 3.0  # 提高开头权重
            # 检查全角半角版本
            elif any(alt in text[:100] for alt in [
                company_name.translate(str.maketrans('ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ')),
                company_name.translate(str.maketrans('ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ', 'ABCDEFGHIJKLMNOPQRSTUVWXYZ'))
            ]):
                position_weight = 3.0
            # 检查是否出现在"公司"一词附近
            elif any(context in text[:200] for context in [f"{company_name}传来", f"{company_name}发布", f"{company_name}公告"]):
                position_weight = 2.5
            
            # 上下文权重：在业务相关上下文中权重更高
            context_weight = 1.0
            if self.business_pattern.search(text):
                context_weight = 1.5
            
            # 长度权重：在短文本中出现权重更高
            length_weight = 1.0
            if len(text) < 200:
                length_weight = 1.5
            elif len(text) < 500:
                length_weight = 1.2
            
            # 综合密度分数
            weighted_density = basic_density * position_weight * context_weight * length_weight
        
        return weighted_density
    
    def extract_companies(self, text: str) -> list[str]:
        """
        从文本中提取所有匹配的 A 股公司简称，去重后返回列表。
        包含子公司到母公司的映射和ST股票规则化映射。
        """
        if not text:
            return []
        
        # 🔧 修复：字符标准化，统一全角半角（解决渝三峡A vs 渝三峡Ａ问题）
        # 由于A股库中大部分是半角字符，我们保持原文本不变，让正则表达式处理匹配
        normalized_text = text
        
        # 🔧 新增：处理空格问题，将连续空格替换为单个空格
        normalized_text = re.sub(r'\s+', ' ', normalized_text)
        
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
        根据公司名称获取股票代码，支持全角半角匹配
        """
        if not company_name:
            return ""
        
        # 直接查找
        if company_name in self.company_code_map:
            return self.company_code_map[company_name]
        
        # 🔧 新增：处理全角半角问题（如"渝三峡A" vs "渝三峡Ａ"）
        # 半角转全角
        half_to_full = str.maketrans(
            'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789',
            'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ０１２３４５６７８９'
        )
        full_width_name = company_name.translate(half_to_full)
        if full_width_name in self.company_code_map:
            return self.company_code_map[full_width_name]
        
        # 全角转半角
        full_to_half = str.maketrans(
            'ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ０１２３４５６７８９',
            'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
        )
        half_width_name = company_name.translate(full_to_half)
        if half_width_name in self.company_code_map:
            return self.company_code_map[half_width_name]
        
        return ""
    
    def is_really_about_company(self, text: str, company_name: str) -> bool:
        """
        验证文本是否真正在讨论该上市公司，而不仅仅是包含相同的词汇
        使用公司密度验证，对容易误识别的公司名更严格
        """
        if not text or not company_name:
            return False
        
        # 🔧 改进：使用公司密度验证
        company_density = self.calculate_company_density(text, company_name)
        
        # 🔧 对于普通公司（非常见词），不进行密度审查
        if company_name in self.ambiguous_company_names:
            # 容易误识别的公司名：2% → 1.5%
            base_threshold = 0.015  # 1.5%
            # 如果密度太低，认为只是偶然提及
            if company_density < base_threshold:
                return False
        else:
            # 普通公司名：不进行密度审查，直接进入后续检查
            pass
        
        # 🔧 改进：对于所有公司都进行适当的上下文验证
        company_code = self.get_company_code(company_name)
        if not company_code:
            return False
        
        # 检查是否在明确的股票/投资/公司运营相关上下文中
        strong_stock_context_keywords = [
            "股价", "涨停", "跌停", "股票", "证券", "上市公司", "A股", "沪深",
            "市值", "股东", "董事长", "CEO", "总经理", "董事会", "股东大会",
            "财报", "年报", "季报", "业绩", "营收", "利润", "净利润",
            "分红", "派息", "停牌", "复牌", "重组", "并购", "收购",
            "公告", "披露", "投资者关系", "宣布", "发布", "表示"
        ]
        
        # 检查明确的公司标识符
        strict_indicators = [
            company_code,  # 股票代码
            f"{company_name}股份", f"{company_name}集团", 
            f"{company_name}有限公司", f"{company_name}公司"
        ]
        
        # 如果有明确标识符或强上下文，认为相关
        has_strict_indicator = any(indicator in text for indicator in strict_indicators)
        has_strong_context = any(keyword in text for keyword in strong_stock_context_keywords)
        
        # 🔧 改进：对于容易误识别的公司名，需要更严格的验证
        if company_name in self.ambiguous_company_names:
            # 容易误识别的公司名需要明确的标识符，不能仅靠强上下文
            if not has_strict_indicator:
                return False
            # 还需要检查是否有其他公司名称可能造成混淆
            # 例如：如果新闻中提到了其他公司名称，可能是关于其他公司的新闻
            other_companies = [name for name in self.company_code_map.keys() if name != company_name]
            for other_company in other_companies:
                if other_company in text and len(other_company) > 2:  # 避免短名称干扰
                    # 如果其他公司名称密度更高，优先选择其他公司
                    other_density = self.calculate_company_density(text, other_company)
                    if other_density > company_density:
                        return False
        
        # 🔧 新增：处理战略名称与公司名称冲突的问题
        # 检查是否可能是其他公司的战略名称
        strategy_conflict_patterns = [
            rf"{company_name}.*战略",     # 如"日出东方2025战略"
            rf"{company_name}战略",       # 如"日出东方战略"
            rf"{company_name}计划",       # 如"日出东方计划"
            rf"{company_name}方案",       # 如"日出东方方案"
            rf"{company_name}项目",       # 如"日出东方项目"
        ]
        
        # 如果公司名称出现在战略相关上下文中，检查是否有其他公司名称
        has_strategy_context = any(re.search(pattern, text) for pattern in strategy_conflict_patterns)
        if has_strategy_context:
            # 🔧 改进：排除合作类新闻，合作新闻中非A股公司出现次数多是正常的
            cooperation_keywords = ["合作", "协议", "签署", "达成", "联手", "共同", "协同"]
            has_cooperation_context = any(keyword in text for keyword in cooperation_keywords)
            
            if has_cooperation_context:
                # 如果是合作类新闻，不进行非A股公司主导检查
                pass
            else:
                # 检查是否有其他公司名称（非A股公司）在新闻中占主导地位
                # 这里我们检查一些常见的非A股公司名称
                non_ashare_companies = [
                    "联想集团", "腾讯", "阿里巴巴", "百度", "京东", "美团", "小米", "华为",
                    "苹果", "微软", "谷歌", "亚马逊", "特斯拉", "比亚迪", "恒大", "碧桂园",
                    "万科", "保利", "中海", "华润", "招商", "中信", "平安", "中国人寿",
                    "中国移动", "中国联通", "中国电信", "中国石油", "中国石化", "中国海油"
                ]
                
                for non_ashare_company in non_ashare_companies:
                    if non_ashare_company in text:
                        non_ashare_count = text.count(non_ashare_company)
                        ashare_count = text.count(company_name)
                        # 如果非A股公司出现次数明显更多，认为这是关于非A股公司的新闻
                        if non_ashare_count > ashare_count * 2:  # 非A股公司出现次数是A股公司的2倍以上
                            return False
        
        if has_strict_indicator or has_strong_context:
            # 🔧 修复：公司成立类新闻过滤逻辑
            # 只过滤非A股公司的成立新闻，A股公司设立子公司的新闻应该保留
            exclude_patterns = [
                "成立，法定代表人", "成立，注册资本", "有限公司成立", "显示，近日",
                "天眼查App显示", "注册资本.*万人民币", "法定代表人为", "经营范围含"
            ]
            is_establishment = any(re.search(pattern, text) for pattern in exclude_patterns)
            
            if is_establishment:
                # 检查是否是A股公司设立子公司
                # 如果新闻中提到A股公司名称，且该名称在A股库中存在，则保留
                if company_name in self.company_code_map:
                    # 这是A股公司设立子公司的新闻，应该保留
                    return True
                else:
                    # 这是非A股公司的成立新闻，应该过滤
                    return False
            
            return True
        
        # 其他情况通过密度验证已经判断
        return True
    
    def is_stock_related_non_price_news(self, text: str, title: str = "", strict_mode: bool = True) -> bool:
        """
        判断是否是涉及上市公司且包含除股价外内容的新闻
        相当于原来的 p_filter_stock_price_news
        
        Args:
            text: 新闻内容
            title: 新闻标题  
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
                r"涨\d+\.?\d*个点", r"跌\d+\.?\d*个点", r"涨\d+\.?\d*个百分点", r"跌\d+\.?\d*个百分点",
                r"涨\d+\.?\d*个点", r"跌\d+\.?\d*个点", r"涨\d+\.?\d*个点", r"跌\d+\.?\d*个点"
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
        
        # 🔧 新增：验证公司相关性，过滤偶然提及
        has_relevant_company = False
        for company_name in valid_companies:
            if self.is_really_about_company(text, company_name):
                has_relevant_company = True
                break
        
        if not has_relevant_company:
            return False
        
        # 🔧 新增：过滤市场观点和市场数据类新闻
        # 市场观点类新闻特征
        market_opinion_indicators = [
            "券商晨会", "晨会精华", "券商观点", "投资建议", "分析师", "分析师认为",
            "机构观点", "机构认为", "投行观点", "投行认为", "研报", "研究报告",
            "投资策略", "投资机会", "把握机会", "关注", "建议关注", "推荐",
            "看好", "看空", "看涨", "看跌", "预期", "预计", "认为", "表示",
            "指出", "强调", "提醒", "警示", "风险提示", "投资风险",
            "市场分析", "行业分析", "板块分析", "主题投资", "概念股",
            "投资主线", "投资逻辑", "投资价值", "估值", "估值修复",
            "结构性机会", "成长性", "确定性", "景气度", "景气周期"
        ]
        
        # 市场数据类新闻特征
        market_data_indicators = [
            "数据看盘", "盘面分析", "资金流向", "主力资金", "北向资金", "南向资金",
            "沪深股通", "沪股通", "深股通", "龙虎榜", "机构买入", "机构卖出",
            "游资", "量化", "营业部", "期指", "期货", "期权", "ETF",
            "成交额", "成交量", "换手率", "涨跌幅", "涨停", "跌停",
            "板块表现", "板块涨幅", "板块跌幅", "个股表现", "个股涨幅",
            "资金净流入", "资金净流出", "主力净流入", "主力净流出",
            "持仓", "减仓", "增仓", "加仓", "融资余额", "融资净买入", "融资净卖出",
            "两融余额", "融资客", "做T", "T+0", "T+1", "北上资金", "净流入",
            "净流出", "净买入", "净卖出", "持股比例", "持仓比例", "仓位",
            "抄底", "增持", "减持", "举牌", "举牌线", "持股量", "流通股"
        ]
        
        # 检查是否是市场观点或市场数据类新闻
        has_market_opinion = any(indicator in text for indicator in market_opinion_indicators)
        has_market_data = any(indicator in text for indicator in market_data_indicators)
        
        # 如果包含市场观点或市场数据特征，需要更严格地检查公司业务内容
        if (has_market_opinion or has_market_data):
            # 🔧 修改：更严格的公司业务内容检查
            # 只检查明确的公司正式业务内容，避免市场分析中的业务词汇干扰
            
            # 1. 检查是否有明确的公司公告/财报类内容
            formal_company_indicators = [
                "公告", "披露", "发布", "通告", "声明", "通知",
                "财报", "年报", "季报", "中报", "业绩预告", "业绩快报",
                "分红", "派息", "配股", "增发", "回购", "减持",
                "董事会", "股东大会"
            ]
            
            has_formal_company_content = any(indicator in text for indicator in formal_company_indicators)
            
            # 2. 检查是否有明确的公司具体业务动作（不是市场分析）
            # 使用更具体的词汇组合，避免单独词汇的误判
            # 注意：排除市场分析中常见的词汇组合
            specific_business_patterns = [
                r"公司.*公告", r"公司.*发布", r"公司.*披露",
                r"公司.*签署", r"公司.*合作", r"公司.*收购", 
                r"公司.*重组", r"公司.*并购", r"公司.*生产", 
                r"公司.*销售", r"公司.*研发", r"公司.*技术", 
                r"公司.*产品", r"公司.*项目", r"公司.*订单", 
                r"公司.*合同", r"公司.*协议", r"公司.*营收", 
                r"公司.*业绩", r"公司.*财报", r"公司.*年报", 
                r"公司.*季报"
            ]
            
            # 排除市场分析中常见的模式
            market_analysis_patterns = [
                r"公司.*投资.*机会", r"公司.*投资.*建议", r"公司.*投资.*策略",
                r"公司.*生产.*成本", r"公司.*生产.*效率", r"公司.*生产.*能力",
                r"公司.*利润.*预期", r"公司.*利润.*增长", r"公司.*利润.*改善",
                r"公司.*业绩.*预期", r"公司.*业绩.*增长", r"公司.*业绩.*改善",
                r"公司.*营收.*预期", r"公司.*营收.*增长", r"公司.*营收.*改善",
                r"公司.*技术.*发展", r"公司.*技术.*进步", r"公司.*技术.*创新",
                r"公司.*产品.*推出", r"公司.*产品.*发布", r"公司.*产品.*上市"
            ]
            
            has_specific_business = any(re.search(pattern, text) for pattern in specific_business_patterns)
            
            # 检查是否包含市场分析模式
            has_market_analysis = any(re.search(pattern, text) for pattern in market_analysis_patterns)
            
            # 如果包含市场分析模式，则不认为是真正的公司业务内容
            if has_market_analysis:
                has_specific_business = False
            
            # 3. 检查是否有公司高管/管理层相关内容
            management_indicators = [
                "董事长", "总经理", "CEO", "CFO", "CTO", "总裁", "高管"
            ]
            has_management_content = any(indicator in text for indicator in management_indicators)
            
            # 综合判断：只有同时满足以下条件之一才认为是真正的公司业务内容
            has_real_company_business = (
                has_formal_company_content or 
                has_specific_business or 
                has_management_content
            )
            
            # 🔧 修改：更严格的过滤逻辑
            # 如果包含市场数据指标，需要更严格地检查
            if has_market_data:
                # 对于市场数据类新闻，要求必须有明确的公司正式公告内容
                # 而不是仅仅提到公司名称或一般性业务词汇
                if not has_formal_company_content:
                    return False
                # 即使有正式公司内容，也要检查是否主要是市场数据报道
                # 如果市场数据词汇密度过高，仍然过滤掉
                market_data_count = sum(text.count(indicator) for indicator in market_data_indicators)
                if market_data_count >= 3:  # 如果市场数据词汇出现3次以上，认为是市场数据新闻
                    return False
            
            # 如果没有真正的公司业务内容，则过滤掉市场观点/数据类新闻
            if not has_real_company_business:
                return False
        
        # 🔧 大幅扩展业务关键词，涵盖更多业务场景
        business_keywords = [
            # 基础业务词汇
            "投资", "合作", "协议", "项目", "业务", "战略", "收购", "重组", 
            "产品", "技术", "研发", "销售", "营收", "利润", "增长", "发展",
            "市场", "客户", "服务", "创新", "转型", "扩张", "布局", "公告",
            "披露", "发布", "合同", "订单", "生产", "制造", "建设", "管理",
            "经营", "运营", "竞争", "行业", "领域", "平台", "数据", "系统",
            "方案", "计划", "预计", "预期", "目标", "规划", "政策", "监管",
            "上市", "股票", "证券", "金融", "银行", "保险", "基金", "债券",
            
            # 销售和业绩词汇  
            "销量", "交付", "出货", "回款", "营业", "收入", "业绩", "财报",
            "年报", "季报", "中报", "净利润", "毛利", "净资产", "ROE",
            "同比", "环比", "增长率", "市占率", "份额",
            
            # 价格和商业词汇
            "降价", "涨价", "定价", "成本", "费用", "毛利率", "促销",
            "打折", "优惠", "活动", "上线", "发售", "推出", "首发",
            
            # 公司治理词汇
            "董事会", "股东大会", "分红", "配股", "增发", "回购", "减持",
            "高管", "CEO", "CFO", "CTO", "总裁", "董事长", "总经理",
            
            # 市场和竞争词汇  
            "竞争", "对手", "领先", "市场", "占有率", "排名", "第一",
            "龙头", "头部", "巨头", "独角兽", "上市公司",
            
            # 新兴业务词汇
            "数字化", "智能化", "自动化", "AI", "人工智能", "大数据",
            "云计算", "物联网", "5G", "新能源", "电池", "芯片", "半导体"
        ]
        business_pattern = re.compile("|".join(map(re.escape, business_keywords)))
        has_business_content = bool(business_pattern.search(text))
        
        # 🔧 大幅放宽业务内容要求：优先保留可能相关的新闻
        is_announcement_type = any(keyword in text for keyword in ["公告", "披露", "发布", "通告"])
        high_density_company = any(self.calculate_company_density(text, c) > 0.02 for c in valid_companies)  # 2%降低门槛
        
        # 🔧 新增：如果包含业绩、财务相关词汇，直接放行（即使没有其他业务内容）
        performance_keywords = ["销量", "交付", "营收", "利润", "业绩", "财报", "回款", "出货", "增长", "目标", "降价", "涨价", "万台", "万辆", "万元", "亿元"]
        has_performance_content = any(keyword in text for keyword in performance_keywords)
        
        # 🔧 如果有相关公司且有业绩内容，直接放行
        if valid_companies and has_performance_content:
            pass  # 继续处理，不在这里返回False
        elif not has_business_content and not is_announcement_type and not high_density_company and not has_performance_content:
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
                max_density = max(self.calculate_company_density(text, c) for c in valid_companies)
                if max_density < 0.03:  # 严格模式要求3%以上的密度
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
            # 🔧 修复：使用改进的价格密度计算
            price_density = self.calculate_price_density_improved(text)
            
            # 🔧 改进：考虑是否有业务内容来动态调整阈值
            has_business_content = self.business_pattern.search(text)
            
            if has_business_content:
                # 有业务内容时，允许更高的价格密度
                if len(text) < 100:
                    threshold = 0.15  # 15%
                elif len(text) < 200:
                    threshold = 0.20  # 20%
                elif len(text) < 500:
                    threshold = 0.30  # 30%
                else:
                    threshold = 0.40  # 40%
            else:
                # 没有业务内容时，严格限制价格密度
                if len(text) < 100:
                    threshold = 0.08  # 8%
                elif len(text) < 200:
                    threshold = 0.12  # 12%
                elif len(text) < 500:
                    threshold = 0.18  # 18%
                else:
                    threshold = 0.25  # 25%
            
            # 如果价格密度超过阈值，认为是纯价格新闻
            if price_density > threshold:
                return False
        
        return True


def filter_stock_price_news_rex(news_content_list, timestamp_list, title_list):
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
            
            # 🔧 早期过滤：直接过滤掉明显的公司成立类新闻和上市类新闻
            establishment_patterns = [
                r"天眼查App显示.*成立",
                r".*公司成立，法定代表人为",
                r".*公司成立，注册资本.*万人民币",
                r"显示，近日.*成立",
                r"经营范围含"
            ]
            
            # 🔧 新增：上市类新闻过滤（精确过滤IPO和新股上市，保留上市公司监管类新闻）
            listing_patterns = [
                r".*IPO.*",
                r".*首次公开发行.*",
                r".*申请上市.*",
                r".*登陆.*市场.*",
                r".*挂牌.*",
                r".*发行.*股票.*",
                r".*公开发行.*",
                r".*将.*上市$",  # "公司将于XX上市"
                r".*准备上市.*",
                r".*即将上市.*",
                r".*正式上市.*",
                r".*成功上市.*",
                r".*在.*上市$",   # "在上交所上市"
                r".*上市申请.*",
                r".*公司上市$"    # "XX公司上市"
            ]
            
            # 不过滤"上市公司"相关的监管、评论新闻
            excluded_from_listing_filter = [
                r".*上市公司.*监管.*",
                r".*上市公司.*规定.*", 
                r".*上市公司.*要求.*",
                r".*评.*上市公司.*"
            ]
            
            is_establishment_news = any(re.search(pattern, cleaned_content) for pattern in establishment_patterns)
            
            # 检查是否是上市新闻，但排除上市公司监管类新闻
            title = title_list[index]
            is_listing_news = any(re.search(pattern, title) for pattern in listing_patterns)
            is_excluded_from_listing_filter = any(re.search(pattern, title) for pattern in excluded_from_listing_filter)
            
            # 如果是上市类新闻且不在排除列表中，则过滤
            should_filter_listing = is_listing_news and not is_excluded_from_listing_filter
            
            if is_establishment_news or should_filter_listing:
                continue
            
            # 检查是否是公告
            title = title_list[index]
            
            # 🔧 新增：标题过滤逻辑 - 对于公告也要过滤市场数据类标题
            if title:
                # 检查标题是否包含涨跌幅信息
                title_price_patterns = [
                    r"涨\d+\.?\d*%", r"跌\d+\.?\d*%", r"涨\d+\.?\d*点", r"跌\d+\.?\d*点",
                    r"涨\d+\.?\d*元", r"跌\d+\.?\d*元", r"涨\d+\.?\d*块", r"跌\d+\.?\d*块",
                    r"涨\d+\.?\d*个点", r"跌\d+\.?\d*个点", r"涨\d+\.?\d*个百分点", r"跌\d+\.?\d*个百分点"
                ]
                
                # 如果标题包含涨跌幅信息，直接过滤
                if any(re.search(pattern, title) for pattern in title_price_patterns):
                    continue
                
                # 检查标题是否包含市场数据类词汇
                title_market_indicators = [
                    "午评", "早评", "收评", "盘面", "盘面分析", "市场表现", "市场走势",
                    "板块表现", "板块涨幅", "板块跌幅", "个股表现", "个股涨幅", "个股跌幅",
                    "涨跌", "涨跌幅", "涨停", "跌停", "涨停潮", "跌停潮",
                    "全线上涨", "全线下挫", "全线飘红", "全线飘绿", "全线收涨", "全线收跌"
                ]
                
                if any(indicator in title for indicator in title_market_indicators):
                    continue
            
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
                # 🔧 新增：标题优先策略 - 对于非公告新闻
                title = title_list[index]
                
                # 步骤1：检查标题中是否有上市公司
                title_companies = filter_engine.extract_companies(title)
                title_companies = [c for c in title_companies if c != "同花顺"]
                
                if title_companies and filter_engine.is_stock_related_non_price_news(cleaned_content, title):
                    # 标题优先策略：如果标题中有公司且内容相关，优先使用标题中的公司
                    main_company = title_companies[0]  # 取第一个A股公司
                    
                    if filter_engine.is_really_about_company(cleaned_content, main_company):
                        company_code = filter_engine.get_company_code(main_company)
                        if company_code:
                            company_chn_name = filter_engine.code_to_name_map.get(company_code, main_company)
                            result_ = {
                                'time': timestamp_list[index], 
                                'title': title_list[index], 
                                'content': cleaned_content, 
                                'code_name': company_code, 
                                'company_chn_name': company_chn_name,
                                'type': 'news'
                            }
                            output.append(result_)
                            continue  # 🔧 关键：标题优先成功，跳过内容分析
                
                # 步骤2：标题无公司或标题公司不相关，按原逻辑分析内容
                if filter_engine.is_stock_related_non_price_news(cleaned_content, title):
                    companies = filter_engine.extract_companies(cleaned_content)
                    # 过滤掉"同花顺"
                    valid_companies = [c for c in companies if c != "同花顺"]
                    
                    for company_name in valid_companies:
                        company_code = filter_engine.get_company_code(company_name)
                        # 🔧 修复：对普通新闻也需要进行公司相关性验证
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




# ================= 新增：单文件与批量文件夹处理辅助函数 =================

def process_single_file(input_path: str, output_path: str, batch_size: int, time_column: str):
    """处理单个CSV文件并输出JSON结果"""
    # 检查输入文件是否存在
    if not os.path.exists(input_path):
        print(f"❌ 输入文件不存在: {input_path}")
        return []

    # 加载新闻数据
    print(f"📰 加载新闻数据: {input_path}")
    df = pd.read_csv(input_path)
    print("📊 CSV列名:", df.columns.tolist())

    # 检查必要的列是否存在
    required_columns = ['content', 'title']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"❌ 缺少必要的列: {missing_columns}")
        return []

    # 检查时间列
    if time_column not in df.columns:
        print(f"❌ 时间列 '{time_column}' 不存在")
        print(f"可用的列: {df.columns.tolist()}")
        return []

    news_content_list = df['content'].tolist()
    timestamp_list = df[time_column].tolist()
    title_list = df['title'].tolist()

    total_count = len(news_content_list)
    print(f"✅ 成功加载 {total_count} 条新闻")

    # 批处理
    filter_engine_output = []
    batch_size_val = batch_size

    for start_idx in range(0, total_count, batch_size_val):
        end_idx = min(start_idx + batch_size_val, total_count)
        current_batch = end_idx - start_idx

        print(f"\n📈 处理批次: {start_idx} 到 {end_idx-1} (共 {current_batch} 条)")

        batch_output = filter_stock_price_news_rex(
            news_content_list[start_idx:end_idx],
            timestamp_list[start_idx:end_idx],
            title_list[start_idx:end_idx]
        )

        filter_engine_output.extend(batch_output)

        print(f"✅ 当前批次筛选结果: {len(batch_output)} 条")
        print(f"📊 累计筛选结果: {len(filter_engine_output)} 条")

    # 保存完整结果
    print(f"\n💾 保存完整结果到: {output_path}")
    dump_json(output_path, filter_engine_output)

    print("\n" + "=" * 60)
    print("📊 筛选完成 - 最终统计结果")
    print("=" * 60)
    print(f"总计处理: {total_count} 条新闻")
    print(f"✅ 通过筛选: {len(filter_engine_output)} 条 ({len(filter_engine_output)/total_count*100:.1f}%)")
    print(f"📁 结果保存到: {output_path}")

    # 显示样本结果
    if filter_engine_output:
        print(f"\n📋 前3条通过筛选的新闻:")
        for i, item in enumerate(filter_engine_output[:3]):
            print(f"\n第{i+1}条:")
            print(f"  标题: {item['title']}")
            print(f"  类型: {item['type']}")
            print(f"  公司: {item['company_chn_name']} ({item['code_name']})")
            print(f"  内容预览: {item['content'][:100]}...")

    return filter_engine_output


def process_directory(input_dir: str, output_dir: str | None, batch_size: int, time_column: str):
    """对文件夹中的所有 CSV 进行批量处理，递归遍历子目录"""
    if not os.path.isdir(input_dir):
        print(f"❌ 输入目录不存在或不是目录: {input_dir}")
        return

    # 若未指定输出目录，默认与输入文件同目录
    use_output_dir = output_dir if output_dir else None

    csv_files = sorted(glob.glob(os.path.join(input_dir, '**', '*.csv'), recursive=True))
    if not csv_files:
        print(f"⚠️ 在目录中未发现 CSV 文件: {input_dir}")
        return

    print(f"🔎 在目录 {input_dir} 中发现 {len(csv_files)} 个 CSV 文件，将逐一处理…")

    for csv_path in csv_files:
        # 生成输出文件名
        base_name = os.path.splitext(os.path.basename(csv_path))[0]
        out_dir = use_output_dir if use_output_dir else os.path.dirname(csv_path)
        # 确保输出目录存在
        os.makedirs(out_dir, exist_ok=True)
        out_path = os.path.join(out_dir, f"{base_name}_filtered.json")

        print("\n" + "-" * 60)
        print(f"📁 正在处理文件: {csv_path}")
        print(f"🗂 输出文件: {out_path}")

        try:
            process_single_file(csv_path, out_path, batch_size, time_column)
        except Exception as e:
            print(f"❌ 处理文件失败: {csv_path} -> {e}")
            continue


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='新闻股票筛选工具')
    parser.add_argument('--input', '-i', default='data/raw/news_data/2025/同花顺_新闻_2025.csv',
                       help='输入CSV文件路径 (默认: data/raw/news_data/2025/同花顺_新闻_2025.csv)')
    parser.add_argument('--output', '-o', default='data/ths_xw_2025_filter_stock_price_rex_complete_20250813.json',
                       help='输出JSON文件路径（当以单文件模式运行时使用）')
    parser.add_argument('--input-dir', '-I', default=None,
                       help='输入目录（递归处理目录下所有 .csv 文件）')
    parser.add_argument('--output-dir', '-O', default=None,
                       help='输出目录（可选；不提供则输出到各CSV所在目录）')
    parser.add_argument('--batch-size', '-b', type=int, default=10000,
                       help='批处理大小 (默认: 10000)')
    parser.add_argument('--time-column', '-t', default='pub_time',
                       help='时间列名 (默认: pub_time)')

    args = parser.parse_args()

    print("=" * 60)
    print("🚀 新闻股票筛选 (完整版) 启动")
    print("=" * 60)

    # 若指定了目录，则进行批量模式
    if args.input_dir:
        print(f"📂 批量模式：目录 = {args.input_dir}")
        if args.output_dir:
            print(f"📦 输出目录 = {args.output_dir}")
        process_directory(args.input_dir, args.output_dir, args.batch_size, args.time_column)
        return

    # 否则按单文件模式处理
    # 检查输入文件是否存在
    if not os.path.exists(args.input):
        print(f"❌ 输入文件不存在: {args.input}")
        return

    process_single_file(args.input, args.output, args.batch_size, args.time_column)


if __name__ == "__main__":
    main()
