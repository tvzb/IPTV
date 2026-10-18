# -*- coding: utf-8 -*-
"""
配置模块：包含目标频道定义、拼音别名表、URL清理、种子源裂变规则与运行参数
支持加载用户自主定制的 channels.json
"""
import os
import re
import json
import urllib.parse
from typing import List, Tuple, Dict

# 默认官方频道定义
DEFAULT_CCTV_CHANNELS = [
    "CCTV-1 综合",
    "CCTV-2 财经",
    "CCTV-3 综艺",
    "CCTV-4 中文国际",
    "CCTV-5 体育",
    "CCTV-6 电影",
    "CCTV-7 国防军事",
    "CCTV-8 电视剧",
    "CCTV-9 纪录",
    "CCTV-10 科教",
    "CCTV-11 戏曲",
    "CCTV-12 社会与法",
    "CCTV-13 新闻",
    "CCTV-14 少儿",
    "CCTV-15 音乐",
    "CCTV-16 奥林匹克",
    "CCTV-17 农业农村",
]

DEFAULT_SATELLITE_CHANNELS = [
    "江西卫视", "东方卫视", "北京卫视", "江苏卫视", "浙江卫视", "湖南卫视", "辽宁卫视",
    "安徽卫视", "重庆卫视", "东南卫视", "甘肃卫视", "广东卫视", "广西卫视", "贵州卫视",
    "海南卫视", "河北卫视", "河南卫视", "黑龙江卫视", "湖北卫视", "吉林卫视", "内蒙古卫视",
    "宁夏卫视", "青海卫视", "山东卫视", "山西卫视", "陕西卫视", "深圳卫视", "四川卫视",
    "天津卫视", "西藏卫视", "新疆卫视", "云南卫视"
]

def load_user_channel_structure(config_file: str = "channels.json") -> Dict[str, List[str]]:
    """加载用户自定义的分类与频道列表；若不存在则使用默认央视与卫视"""
    if os.path.exists(config_file):
        try:
            with open(config_file, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and data:
                    return data
        except Exception as e:
            print(f"[!] 读取 {config_file} 失败，将采用默认配置: {e}")
    
    return {
        "央视频道": DEFAULT_CCTV_CHANNELS,
        "卫视频道": DEFAULT_SATELLITE_CHANNELS,
    }

# 频道英文/拼音标识对照字典（用于 URL 规律推导与裂变生成）
CHANNEL_SLUGS: Dict[str, List[str]] = {
    "CCTV-1 综合": ["cctv1", "cctv-1"],
    "CCTV-2 财经": ["cctv2", "cctv-2"],
    "CCTV-3 综艺": ["cctv3", "cctv-3"],
    "CCTV-4 中文国际": ["cctv4", "cctv-4"],
    "CCTV-5 体育": ["cctv5", "cctv-5"],
    "CCTV-6 电影": ["cctv6", "cctv-6"],
    "CCTV-7 国防军事": ["cctv7", "cctv-7"],
    "CCTV-8 电视剧": ["cctv8", "cctv-8"],
    "CCTV-9 纪录": ["cctv9", "cctv-9"],
    "CCTV-10 科教": ["cctv10", "cctv-10"],
    "CCTV-11 戏曲": ["cctv11", "cctv-11"],
    "CCTV-12 社会与法": ["cctv12", "cctv-12"],
    "CCTV-13 新闻": ["cctv13", "cctv-13"],
    "CCTV-14 少儿": ["cctv14", "cctv-14"],
    "CCTV-15 音乐": ["cctv15", "cctv-15"],
    "CCTV-16 奥林匹克": ["cctv16", "cctv-16"],
    "CCTV-17 农业农村": ["cctv17", "cctv-17"],
    "江西卫视": ["jiangxi", "jxws"],
    "东方卫视": ["dongfang", "shanghai", "dfws"],
    "北京卫视": ["beijing", "bjws"],
    "江苏卫视": ["jiangsu", "jsws"],
    "浙江卫视": ["zhejiang", "zjws"],
    "湖南卫视": ["hunan", "hnws"],
    "辽宁卫视": ["liaoning", "lnws"],
    "安徽卫视": ["anhui", "ahws"],
    "重庆卫视": ["chongqing", "cqws"],
    "东南卫视": ["dongnan", "fujian", "dnws"],
    "甘肃卫视": ["gansu", "gsws"],
    "广东卫视": ["guangdong", "gdws"],
    "广西卫视": ["guangxi", "gxws"],
    "贵州卫视": ["guizhou", "gzws"],
    "海南卫视": ["hainan"],
    "河北卫视": ["hebei", "hbws"],
    "河南卫视": ["henan"],
    "黑龙江卫视": ["heilongjiang", "hljws"],
    "湖北卫视": ["hubei"],
    "吉林卫视": ["jilin", "jlws"],
    "内蒙古卫视": ["neimenggu", "nmgws"],
    "宁夏卫视": ["ningxia", "nxws"],
    "青海卫视": ["qinghai", "qhws"],
    "山东卫视": ["shandong", "sdws"],
    "山西卫视": ["shanxi"],
    "陕西卫视": ["shaanxi"],
    "深圳卫视": ["shenzhen", "szws"],
    "四川卫视": ["sichuan", "scws"],
    "天津卫视": ["tianjin", "tjws"],
    "西藏卫视": ["xizang", "xzws"],
    "新疆卫视": ["xinjiang", "xjws"],
    "云南卫视": ["yunnan", "ynws"],
}

def clean_stream_url(raw_url: str) -> str:
    """彻底清除 '$' 及其后面的任何文字修饰"""
    if not raw_url:
        return ""
    return raw_url.split("$")[0].strip()

def normalize_channel_name(raw_name: str) -> str:
    """统一名称清洗规则：过滤 CCTV5+，归一化央视与各省卫视"""
    if not raw_name:
        return ""
    clean_name = raw_name.strip()
    
    if re.search(r'(?i)cctv[-_ ]?5\+', clean_name):
        return ""

    cctv_match = re.search(r'(?i)cctv[-_ ]?(\d{1,2})', clean_name)
    if cctv_match:
        num = int(cctv_match.group(1))
        if 1 <= num <= 17:
            prefix = f"CCTV-{num} "
            for std_cctv in DEFAULT_CCTV_CHANNELS:
                if std_cctv.startswith(prefix):
                    return std_cctv

    for sat in DEFAULT_SATELLITE_CHANNELS:
        keyword = sat.replace("卫视", "")
        if keyword in clean_name and ("卫视" in clean_name or sat in clean_name or "台" in clean_name):
            return sat
        if sat == "东方卫视" and ("上海卫视" in clean_name or "东方台" in clean_name):
            return "东方卫视"

    return clean_name

def extrapolate_channels_from_seed(seed_url: str) -> List[Tuple[str, str]]:
    """根据种子源规律推导全套央视和卫视频道"""
    results: List[Tuple[str, str]] = []
    clean_seed = clean_stream_url(seed_url)
    if not clean_seed:
        return results

    cctv_pattern = re.search(r'(cctv[-_]?0?(\d{1,2}))([a-zA-Z0-9_\-]*)(\.m3u8|\.flv|\.ts)', clean_seed, re.IGNORECASE)
    if cctv_pattern:
        prefix = clean_seed[:cctv_pattern.start(1)]
        matched_token = cctv_pattern.group(1)
        suffix = clean_seed[cctv_pattern.end(3):]
        extra_tag = cctv_pattern.group(3)
        has_hyphen = "-" in matched_token
        has_zero = "0" in matched_token and len(cctv_pattern.group(2)) == 1

        for std_ch in DEFAULT_CCTV_CHANNELS:
            num = int(std_ch.split()[0].replace("CCTV-", ""))
            num_str = f"0{num}" if has_zero and num < 10 else str(num)
            token = f"cctv-{num_str}" if has_hyphen else f"cctv{num_str}"
            if matched_token.isupper():
                token = token.upper()
            extrapolated_url = f"{prefix}{token}{extra_tag}{suffix}"
            results.append((std_ch, extrapolated_url))

        for std_sat in DEFAULT_SATELLITE_CHANNELS:
            slug = CHANNEL_SLUGS.get(std_sat, [None])[0]
            if slug:
                extrapolated_url = f"{prefix}{slug}{suffix}"
                results.append((std_sat, extrapolated_url))
        return results

    for std_ch, slugs in CHANNEL_SLUGS.items():
        for slug in slugs:
            pattern = re.search(rf'({re.escape(slug)})([a-zA-Z0-9_\-]*)(\.m3u8|\.flv|\.ts)', clean_seed, re.IGNORECASE)
            if pattern:
                prefix = clean_seed[:pattern.start(1)]
                suffix = clean_seed[pattern.end(2):]
                for other_ch, other_slugs in CHANNEL_SLUGS.items():
                    target_slug = other_slugs[0]
                    extrapolated_url = f"{prefix}{target_slug}{suffix}"
                    results.append((other_ch, extrapolated_url))
                for std_cctv in DEFAULT_CCTV_CHANNELS:
                    cctv_num = std_cctv.split()[0].replace("CCTV-", "")
                    extrapolated_url = f"{prefix}cctv{cctv_num}{suffix}"
                    results.append((std_cctv, extrapolated_url))
                return results

    return results

# 运行参数
RAW_SOURCES_FILE = "raw_sources.txt"      # 采集工作流输出 / 测速工作流输入
DEFAULT_OUTPUT_FILE = "live.txt"          # 最终交付给电视盒子的优选文件
MIN_SPEED_KBS = float(os.environ.get("MIN_SPEED_KBS", 300.0))  # 流畅播放速率门槛 (KB/s，不限数量)
TVBOX_USER_AGENT = "okhttp/3.12.1"
REQUEST_TIMEOUT = 4.0
SPEED_TEST_TIMEOUT = 2.0
SPEED_TEST_MAX_BYTES = 512 * 1024
MAX_CONCURRENT_TASKS = 50
