# -*- coding: utf-8 -*-
"""
IPTV 频道标准化、URL 铁血清洗 ($尾缀剔除 + 广播/Flash/假源黑名单) 与精准裂变配置
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
REQUEST_TIMEOUT = 4.5
CHUNK_DOWNLOAD_TIMEOUT = 3.5
CHUNK_DOWNLOAD_MAX_BYTES = 1024 * 1024  # 1MB 采样上限
MAX_WORKERS = 28
MIN_SPEED_KBS = 220.0  # 最低有效速度阈值 (KB/s)，确保高清流畅不卡顿
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

# 【核心防御】垃圾源、纯音频广播源、Flash插件、一次性动态Token源黑名单正则
BLACKLIST_URL_PATTERNS = re.compile(
    r'('
    r'\.swf(\?|$)|'                 # 拦截 player.cntv.cn/.../live_HLSDRM.swf 等 Flash 插件
    r'/audio/|'                     # 拦截 piccpndali.v.myalicdn.com/audio/ 等纯音频广播源
    r'audio\.|'                     # 拦截音频子域名
    r'/radio/|'                     # 拦截电台广播路径
    r'\.(mp3|aac|wav|ogg|flac)(\?|$)|'  # 拦截纯音频文件后缀
    r'jdshipin\.com|'               # 拦截 php.jdshipin.com 假源/垫片源
    r'264788\.xyz|'                 # 拦截绑定请求IP的一次性 livekey 代理源
    r'[?&](livekey|migutoken|auth_key|txSecret|wsSecret)=|'  # 拦截过期的动态防盗链Token
    r'P2PChannelID=pd://'           # 拦截远古 CNTV P2P 插件参数
    r')',
    re.IGNORECASE
)


def load_user_channel_structure() -> Dict[str, List[str]]:
    """返回标准分组与频道顺序结构"""
    return {
        "央视频道": list(DEFAULT_CCTV_CHANNELS),
        "卫视频道": list(DEFAULT_SATELLITE_CHANNELS),
    }


def is_blacklisted_url(url: str) -> bool:
    """检查 URL 是否命中广播源、Flash、假源或动态失效 Token 黑名单"""
    if not url:
        return True
    return bool(BLACKLIST_URL_PATTERNS.search(url))


def clean_stream_url(raw_url: str) -> str:
    """
    第二步核心清洗函数：
    1. 严格删除直播源地址（以 http 开头）中 '$' 及后面的汉字与标记；
    2. 自动拦截广播源 (.mp3 / /audio/)、Flash (.swf) 及假 PHP 源。
    """
    if not raw_url:
        return ""
    url = raw_url.strip()
    m = re.search(r'(https?://[^\s"\'<>]+)', url)
    if m:
        url = m.group(1)
    # 删除 $ 及后面的所有汉字或附加说明
    if "$" in url:
        url = url.split("$", 1)[0].strip()

    # 黑名单一票否决
    if is_blacklisted_url(url):
        return ""

    return url


def normalize_channel_name(raw_name: str) -> str:
    """
    第二步频道统一命名函数：
    将各种异构写法统一映射为 CCTV-1~17 或省级卫视标准名称。
    同时剔除名称中带“广播”、“电台”、“之声”的纯音频频道。
    """
    if not raw_name:
        return ""
    if any(k in raw_name for k in ("广播", "电台", "之声", "FM", "调频")):
        return ""

    name = raw_name.upper().strip()
    name = re.sub(r'(\[.*?\]|\(.*?\)|（.*?）|HD|FHD|4K|8K|超清|高清|蓝光|标清|频道|直播|-|\s+)', '', name)

    # 匹配 CCTV-5+
    if re.search(r'(CCTV|中央)0?5\+', name) or "体育赛事" in name:
        return "CCTV-5+"

    # 匹配 CCTV-1 ~ CCTV-17 (严防 CCTV-10 被识别成 CCTV-1)
    m_cctv = re.search(r'(?:CCTV|中央)0?(\d{1,2})(?!\d)', name)
    if m_cctv:
        num = int(m_cctv.group(1))
        if 1 <= num <= 17:
            return f"CCTV-{num}"

    # 匹配省级卫视
    if "旅游卫视" in name:
        return "海南卫视"
    if ("东南" in name or "福建" in name) and "卫视" in raw_name:
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
    第四步智能裂变函数（带严格边界锚定，防止把 cctv12 拆成 cctv1 + 2）：
    仅对高可靠性的酒店源 (/tsfile/live/) 与标准直连 m3u8 进行推导。
    """
    derived: List[Tuple[str, str]] = []
    if not seed_url or is_blacklisted_url(seed_url):
        return derived

    # 拒绝裂变带有复杂查询长串的动态链接
    if seed_url.count("&") >= 3 or ".swf" in seed_url.lower():
        return derived

    # 模板 A: 智慧光迅酒店源 (如: http://101.66.195.243:9901/tsfile/live/0001_1.m3u8?key=txiptv...)
    m_tsfile = re.search(r'(/tsfile/live/)00(0[1-9]|1[0-7])(_\d+\.m3u8.*)', seed_url)
    if m_tsfile:
        prefix, _, suffix = m_tsfile.group(1), m_tsfile.group(2), m_tsfile.group(3)
        base_head = seed_url[:m_tsfile.start()]
        for i in range(1, 18):
            new_url = f"{base_head}{prefix}{i:04d}{suffix}"
            derived.append((f"CCTV-{i}", new_url))
        return derived

    # 模板 B: 标准 m3u8 路径中的 cctv 编号 (增加 (?!\d) 严防匹配到 cctv12 的前五位)
    m_cctv_id = re.search(r'(?<=[/=_])(cctv[-_]?)(\d{1,2})(?!\d|\+)(hd|p)?(?=\.m3u8|\.ts|$|&)', seed_url, re.IGNORECASE)
    if m_cctv_id:
        start_idx, end_idx = m_cctv_id.span(2)
        for i in range(1, 18):
            new_url = seed_url[:start_idx] + str(i) + seed_url[end_idx:]
            derived.append((f"CCTV-{i}", new_url))

    return derived
