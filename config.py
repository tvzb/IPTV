# -*- coding: utf-8 -*-
"""
IPTV 频道标准化、URL 清洗 ($尾缀剔除) 与智能裂变规则配置
"""
import re
from typing import Dict, List, Tuple

# 输出文件路径定义
RAW_SOURCES_TXT = "raw_sources.txt"
RAW_SOURCES_M3U = "raw_sources.m3u"
OUTPUT_LIVE_TXT = "live.txt"
OUTPUT_LIVE_M3U = "live.m3u"

# 仿真国内 TVBox (ExoPlayer / IjkPlayer) 标识
TVBOX_USER_AGENT = "okhttp/3.15"

# 测速与并发参数
REQUEST_TIMEOUT = 4.0
CHUNK_DOWNLOAD_TIMEOUT = 3.0
CHUNK_DOWNLOAD_MAX_BYTES = 1024 * 1024  # 1MB 采样上限
MAX_WORKERS = 32
MIN_SPEED_KBS = 200.0  # 最低有效速度阈值 (KB/s)
TARGET_PER_CHANNEL_RAW = 100

# 标准央视频道 (CCTV-1 ~ CCTV-17)
DEFAULT_CCTV_CHANNELS = [
    "CCTV-1", "CCTV-2", "CCTV-3", "CCTV-4", "CCTV-5", "CCTV-5+",
    "CCTV-6", "CCTV-7", "CCTV-8", "CCTV-9", "CCTV-10", "CCTV-11",
    "CCTV-12", "CCTV-13", "CCTV-14", "CCTV-15", "CCTV-16", "CCTV-17"
]

# 标准省级卫视频道
DEFAULT_SATELLITE_CHANNELS = [
    "湖南卫视", "浙江卫视", "江苏卫视", "东方卫视", "北京卫视", "广东卫视",
    "深圳卫视", "山东卫视", "安徽卫视", "湖北卫视", "四川卫视", "辽宁卫视",
    "黑龙江卫视", "天津卫视", "重庆卫视", "江西卫视", "河北卫视", "河南卫视",
    "东南卫视", "广西卫视", "贵州卫视", "云南卫视", "陕西卫视", "山西卫视",
    "吉林卫视", "海南卫视", "甘肃卫视", "宁夏卫视", "青海卫视", "新疆卫视",
    "内蒙古卫视", "西藏卫视", "兵团卫视"
]

ALL_DEFAULT_TARGETS = set(DEFAULT_CCTV_CHANNELS + DEFAULT_SATELLITE_CHANNELS)


def load_user_channel_structure() -> Dict[str, List[str]]:
    """返回标准分组与频道顺序结构"""
    return {
        "央视频道": list(DEFAULT_CCTV_CHANNELS),
        "卫视频道": list(DEFAULT_SATELLITE_CHANNELS),
    }


def clean_stream_url(raw_url: str) -> str:
    """
    第二步核心清洗函数：
    严格删除直播源地址（以 http 开头）中 '$' 及后面的汉字与标记（如图一中的 $订阅源），
    并清理首尾空白字符。
    """
    if not raw_url:
        return ""
    url = raw_url.strip()
    # 截取 http:// 或 https:// 起始部分
    m = re.search(r'(https?://[^\s"\'<>]+)', url)
    if m:
        url = m.group(1)
    # 删除 $ 及后面的所有汉字或附加说明 (例如: http://.../zbdq5.m3u8?id=cctv6hd$订阅源)
    if "$" in url:
        url = url.split("$", 1)[0].strip()
    return url


def normalize_channel_name(raw_name: str) -> str:
    """
    第二步频道统一命名函数：
    将各种异构写法（如 CCTV1综合、CCTV-06电影、中央6套、东方卫视HD）统一映射为标准名称。
    若不属于 CCTV-1~17 或省级卫视，则返回空字符串以便过滤。
    """
    if not raw_name:
        return ""
    name = raw_name.upper().strip()
    # 去除常见修饰后缀
    name = re.sub(r'(\[.*?\]|\(.*?\)|（.*?）|HD|FHD|4K|超清|高清|蓝光|标清|频道|直播|-|\s+)', '', name)

    # 匹配 CCTV-5+
    if re.search(r'(CCTV|中央)0?5\+', name) or "体育赛事" in name:
        return "CCTV-5+"

    # 匹配 CCTV-1 ~ CCTV-17
    m_cctv = re.search(r'(?:CCTV|中央)0?(\d{1,2})', name)
    if m_cctv:
        num = int(m_cctv.group(1))
        if 1 <= num <= 17:
            return f"CCTV-{num}"

    # 匹配省级卫视
    if "旅游卫视" in name:
        return "海南卫视"
    if "东南" in name or "福建" in name:
        if "卫视" in raw_name:
            return "东南卫视"

    for sat in DEFAULT_SATELLITE_CHANNELS:
        prefix = sat.replace("卫视", "")
        if prefix in name and ("卫视" in raw_name or prefix in ("东方", "东南", "兵团")):
            return sat

    return ""


def extract_ip_port(url: str) -> str:
    """提取 URL 中的 IP:Port 或域名，用于同服务器防独占与反向搜索"""
    m = re.search(r'https?://([^/]+)', url)
    return m.group(1) if m else ""


def extrapolate_channels_from_seed(seed_url: str) -> List[Tuple[str, str]]:
    """
    第四步智能裂变函数：
    根据已验证高速的种子 URL，自动推导同一服务器上的 CCTV-1 ~ CCTV-17 地址。
    支持图一、图二中最常见的三类主流酒店/广电 URL 模板：
      模板 A: /tsfile/live/0001_1.m3u8 -> 0001~0017 (智慧光迅酒店源)
      模板 B: id=cctv1hd / cctv1.m3u8 / cctv-1.m3u8 -> cctv1~cctv17
    """
    derived: List[Tuple[str, str]] = []
    if not seed_url:
        return derived

    # 模板 A: 智慧光迅酒店源 (如图二: http://101.66.195.243:9901/tsfile/live/0001_1.m3u8?key=txiptv...)
    m_tsfile = re.search(r'(/tsfile/live/)00(0[1-9]|1[0-7])(_\d+\.m3u8.*)', seed_url)
    if m_tsfile:
        prefix, _, suffix = m_tsfile.group(1), m_tsfile.group(2), m_tsfile.group(3)
        base_head = seed_url[:m_tsfile.start()]
        for i in range(1, 18):
            new_url = f"{base_head}{prefix}{i:04d}{suffix}"
            derived.append((f"CCTV-{i}", new_url))
        return derived

    # 模板 B: 常见拼音/ID 标识 (如图一: ?id=cctv6hd 或 /live/cctv6hd.m3u8 或 /cctv-6.m3u8)
    m_cctv_id = re.search(r'(cctv[-_]?)(\d{1,2}\+?)(hd|p|\b|\.m3u8)', seed_url, re.IGNORECASE)
    if m_cctv_id:
        c_prefix, _, c_suffix = m_cctv_id.group(1), m_cctv_id.group(2), m_cctv_id.group(3)
        start_idx, end_idx = m_cctv_id.span(2)
        for i in range(1, 18):
            new_url = seed_url[:start_idx] + str(i) + seed_url[end_idx:]
            derived.append((f"CCTV-{i}", new_url))

    return derived
