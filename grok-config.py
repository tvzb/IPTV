# -*- coding: utf-8 -*-
"""
配置模块：目标频道、标准化清洗、种子裂变（数字序号+字母标识）、广播剔除、运行参数
支持加载用户自定义 channels.json
"""
import os
import re
import json
import urllib.parse
from typing import List, Tuple, Dict, Optional

# 默认官方频道定义（CCTV-1 至 CCTV-17，严格排除 CCTV-5+）
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

# 默认省级卫视频道：前7个置顶优先，后25个按拼音字母升序
DEFAULT_SATELLITE_CHANNELS = [
    # 7个置顶优先卫视
    "江西卫视",
    "东方卫视",
    "北京卫视",
    "江苏卫视",
    "浙江卫视",
    "湖南卫视",
    "辽宁卫视",
    # 其余省级卫视（按首字汉语拼音严格字母升序）
    "安徽卫视",
    "兵团卫视",
    "重庆卫视",
    "东南卫视",
    "甘肃卫视",
    "广东卫视",
    "广西卫视",
    "贵州卫视",
    "海南卫视",
    "河北卫视",
    "河南卫视",
    "黑龙江卫视",
    "湖北卫视",
    "吉林卫视",
    "康巴卫视",
    "内蒙古卫视",
    "宁夏卫视",
    "青海卫视",
    "山东卫视",
    "山西卫视",
    "陕西卫视",
    "深圳卫视",
    "四川卫视",
    "天津卫视",
    "西藏卫视",
    "新疆卫视",
    "云南卫视",
]

ALL_DEFAULT_TARGETS = DEFAULT_CCTV_CHANNELS + DEFAULT_SATELLITE_CHANNELS

def load_user_channel_structure(config_file: str = "channels.json") -> Dict[str, List[str]]:
    """加载用户自定义的分类与频道列表；不存在则使用默认央视+卫视"""
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


# 频道英文/拼音标识对照字典（用于 URL 字母规律裂变）
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
    "兵团卫视": ["bingtuan", "btws"],
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
    "康巴卫视": ["kangba"],
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


def is_radio_source(name: str, url: str = "") -> bool:
    """严格剔除广播/电台源，避免混入电视列表"""
    if not name:
        return True
    name_lower = name.lower()
    radio_keywords = [
        "广播", "电台", "之声", "fm", "am", "radio", "音频",
        "中国之声", "经济之声", "音乐之声", "文艺之声", "交通广播",
        "新闻广播", "都市广播", "故事广播", "老年广播", "少儿广播",
    ]
    for kw in radio_keywords:
        if kw in name_lower:
            return True
    # URL 中常见纯音频特征
    if url and re.search(r'(radio|fm\d+|am\d+|audio)', url, re.IGNORECASE):
        return True
    return False


def normalize_channel_name(raw_name: str) -> str:
    """统一频道名称归一化清洗规则（排除 CCTV-5+ 和广播源）"""
    if not raw_name:
        return ""
    clean_name = raw_name.strip()

    # 严格排除 CCTV-5+ 和广播
    if re.search(r'(?i)cctv[-_ ]?5\+', clean_name):
        return ""
    if is_radio_source(clean_name):
        return ""

    # 1. 匹配 CCTV-1 至 CCTV-17
    cctv_match = re.search(r'(?i)cctv[-_ ]?(\d{1,2})', clean_name)
    if cctv_match:
        num = int(cctv_match.group(1))
        if 1 <= num <= 17:
            prefix = f"CCTV-{num} "
            for std_cctv in DEFAULT_CCTV_CHANNELS:
                if std_cctv.startswith(prefix):
                    return std_cctv

    # 2. 匹配省级卫视
    for sat in DEFAULT_SATELLITE_CHANNELS:
        keyword = sat.replace("卫视", "")
        if keyword in clean_name and ("卫视" in clean_name or sat in clean_name or "台" in clean_name):
            return sat
        if sat == "东方卫视" and ("上海卫视" in clean_name or "东方台" in clean_name):
            return "东方卫视"
        if sat == "兵团卫视" and "新疆兵团" in clean_name:
            return "兵团卫视"

    return clean_name


def extract_ip_port(url: str) -> Optional[str]:
    """从流地址中提取 IP:端口（如 125.125.236.31:9901）"""
    try:
        parsed = urllib.parse.urlparse(url)
        netloc = parsed.netloc
        if not netloc:
            return None
        ip_match = re.match(r'^(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}(?::\d+)?)', netloc)
        if ip_match:
            return ip_match.group(1)
        if ":" in netloc:
            return netloc
    except Exception:
        pass
    return None


def extrapolate_channels_from_seed(seed_url: str) -> List[Tuple[str, str]]:
    """
    智能种子源裂变推导引擎：
    同时支持【数字序号规律】与【字母标识规律】两类裂变推导！
    """
    results: List[Tuple[str, str]] = []
    clean_seed = clean_stream_url(seed_url)
    if not clean_seed:
        return results

    # === 模式 A：四位/三位数字序号加后缀规律（如 0001_1.m3u8, 0001.m3u8, 001.m3u8） ===
    num_seq_match = re.search(r'([/_])(0*(\d{1,4}))(_\d+)?(\.m3u8|\.ts|\.flv)', clean_seed, re.IGNORECASE)
    if num_seq_match:
        delimiter = num_seq_match.group(1)
        full_num_str = num_seq_match.group(2)
        raw_num = int(num_seq_match.group(3))
        sub_tag = num_seq_match.group(4) or ""
        ext = num_seq_match.group(5)
        num_len = len(full_num_str)

        if 1 <= raw_num <= 17:
            prefix = clean_seed[:num_seq_match.start()]
            suffix = clean_seed[num_seq_match.end():]

            for std_ch in DEFAULT_CCTV_CHANNELS:
                cctv_num = int(std_ch.split()[0].replace("CCTV-", ""))
                formatted_num = f"{cctv_num:0{num_len}d}"
                extrapolated_url = f"{prefix}{delimiter}{formatted_num}{sub_tag}{ext}{suffix}"
                results.append((std_ch, extrapolated_url))

    # === 模式 B：标准 cctv1 / cctv-1 / cctv1hd 标识规律 ===
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

    # === 模式 C：卫视拼音别名标识规律（如 hunan.m3u8） ===
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


RAW_SOURCES_TXT = "raw_sources.txt"
RAW_SOURCES_M3U = "raw_sources.m3u"
OUTPUT_LIVE_TXT = "live.txt"
OUTPUT_LIVE_M3U = "live.m3u"

# 默认测速门槛（KB/s）。国内真实盒子建议 150~300，海外 Actions 可适当放宽
MIN_SPEED_KBS = float(os.environ.get("MIN_SPEED_KBS", 200.0))
TVBOX_USER_AGENT = "okhttp/3.12.1"
REQUEST_TIMEOUT = 5.0
CHUNK_DOWNLOAD_TIMEOUT = 2.5
CHUNK_DOWNLOAD_MAX_BYTES = 512 * 1024
MAX_WORKERS = 32
TARGET_PER_CHANNEL_RAW = 60
MAX_SAME_IP_PER_CHANNEL = 2
