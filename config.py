# -*- coding: utf-8 -*-
"""
IPTV 核心配置模块：
- 直接采用本模块内置的官方频道名称与顺序定义：
  1. 央视频道：CCTV 1~17 + CCTV-4 中文国际美洲 / CCTV-4 中文国际欧洲 + CCTV-5+ 体育赛事
     （严格排除 CCTV-4K 和 CCTV-8K）
  2. 卫视频道：30 个省级卫视频道 (7个置顶优先卫视 + 23个按首字拼音升序排列卫视，福建统一命名为东南卫视)
  3. 电视频道：CHC家庭影院、CHC动作电影、CHC高清电影、经典电影
- 分辨率差异化速度门槛与四档排序规则：
  - 1920x1080 (1080P)：流畅播放要求 >= 650 KB/s，最低入围门槛 >= 350 KB/s
  - 1280x720  (720P) ：流畅播放要求 >= 420 KB/s，最低入围门槛 >= 240 KB/s
  - 720x576/574 (标清)：流畅播放要求 >= 200 KB/s，最低入围门槛 >= 120 KB/s
  - 同一频道排序优先级：1.高清且流畅 -> 2.流畅标清 -> 3.高清卡顿(备用) -> 4.标清卡顿(备用)
"""
import os
import re
import urllib.parse
from typing import Dict, List, Optional, Set, Tuple

# 输出文件路径定义（保留全部常量，防止跨脚本导入报错）
RAW_SOURCES_TXT = "raw_sources.txt"
RAW_SOURCES_M3U = "raw_sources.m3u"
OUTPUT_LIVE_TXT = "live.txt"
OUTPUT_LIVE_M3U = "live.m3u"

# 底部定制信息配置
CUSTOM_WECHAT_LABEL = "微信：bofutz"

# 仿真国内 TVBox (ExoPlayer / IjkPlayer) 标识与国内 IP 请求头
TVBOX_USER_AGENT = "okhttp/3.15"

# 搜索采集页数与每页条数配置（默认采集前 2 页）
DEFAULT_SEARCH_PAGES = int(os.environ.get("SEARCH_PAGES", "2") or "2")
RESULTS_PER_SEARCH_PAGE = 20

# GitHub 固定搜索关键词配置（只搜这两个词，默认搜 2026-10）
SEARCH_MONTH_STR = os.environ.get("SEARCH_MONTH", "2026-10").strip() or "2026-10"
GITHUB_SEARCH_QUERIES = [
    f"更新 {SEARCH_MONTH_STR} CCTV-6,http",
    f"更新 {SEARCH_MONTH_STR} CCTV6,http",
]

# 测速与并发参数
REQUEST_TIMEOUT = 6.0
CHUNK_DOWNLOAD_TIMEOUT = 12.0
CHUNK_DOWNLOAD_MAX_BYTES = 4 * 1024 * 1024
MIN_REQUIRED_BYTES = 384 * 1024
MAX_WORKERS = 8
MAX_CANDIDATES_PER_CHANNEL = 60
MAX_SAME_IP_PER_CHANNEL = 5

# ==============================================================================
# 按分辨率动态设定的【流畅播放速度】与【最低可播入围门槛】(单位: KB/s)
# ==============================================================================
SPEED_FLUENT_1080P = 650.0
SPEED_MIN_1080P = 350.0

SPEED_FLUENT_720P = 420.0
SPEED_MIN_720P = 240.0

SPEED_FLUENT_SD = 200.0
SPEED_MIN_SD = 120.0

MIN_SPEED_KBS = SPEED_MIN_SD

# 是否开启测速时视频画面台标 OCR 识别自动纠错
ENABLE_LOGO_OCR = os.environ.get("ENABLE_LOGO_OCR", "true").strip().lower() not in ("false", "0", "no")

# 官方默认央视频道（CCTV 1~17 + CCTV-4 中文国际美洲/欧洲 + CCTV-5+ 体育赛事）
DEFAULT_CCTV_CHANNELS = [
    "CCTV-1 综合", "CCTV-2 财经", "CCTV-3 综艺", "CCTV-4 中文国际",
    "CCTV-5 体育", "CCTV-6 电影", "CCTV-7 国防军事", "CCTV-8 电视剧",
    "CCTV-9 纪录", "CCTV-10 科教", "CCTV-11 戏曲", "CCTV-12 社会与法",
    "CCTV-13 新闻", "CCTV-14 少儿", "CCTV-15 音乐", "CCTV-16 奥林匹克",
    "CCTV-17 农业农村",
    "CCTV-4 中文国际美洲",
    "CCTV-4 中文国际欧洲",
    "CCTV-5+ 体育赛事",
]

# 官方默认省级卫视频道（7个置顶优先卫视 + 23个按首字拼音升序排列省级卫视，福建统一为东南卫视）
DEFAULT_SATELLITE_CHANNELS = [
    # 7个置顶优先卫视
    "江西卫视", "东方卫视", "北京卫视", "江苏卫视", "浙江卫视", "湖南卫视", "辽宁卫视",
    # 其余23个省级卫视（严格按首字汉语拼音字母升序排列）
    "安徽卫视",   # Anhui
    "重庆卫视",   # Chongqing
    "东南卫视",   # Dongnan
    "甘肃卫视",   # Gansu
    "广东卫视",   # Guangdong
    "广西卫视",   # Guangxi
    "贵州卫视",   # Guizhou
    "海南卫视",   # Hainan
    "河北卫视",   # Hebei
    "河南卫视",   # Henan
    "黑龙江卫视", # Heilongjiang
    "湖北卫视",   # Hubei
    "吉林卫视",   # Jilin
    "内蒙古卫视", # Neimenggu
    "宁夏卫视",   # Ningxia
    "青海卫视",   # Qinghai
    "山东卫视",   # Shandong
    "山西卫视",   # Shanxi
    "陕西卫视",   # Shaanxi
    "深圳卫视",   # Shenzhen
    "四川卫视",   # Sichuan
    "天津卫视",   # Tianjin
    "西藏卫视",   # Xizang
    "新疆卫视",   # Xinjiang
    "云南卫视",   # Yunnan
]

# 新增分类：电视频道
DEFAULT_MOVIE_CHANNELS = [
    "CHC家庭影院",
    "CHC动作电影",
    "CHC高清电影",
    "经典电影",
]

ALL_DEFAULT_TARGETS = DEFAULT_CCTV_CHANNELS + DEFAULT_SATELLITE_CHANNELS + DEFAULT_MOVIE_CHANNELS

# 央视台标副标题关键词辅助映射（长词优先）
CCTV_SUBTITLE_MAP: Dict[str, str] = {
    "体育赛事": "CCTV-5+ 体育赛事",
    "中文国际美洲": "CCTV-4 中文国际美洲",
    "中文国际欧洲": "CCTV-4 中文国际欧洲",
    "中文国际": "CCTV-4 中文国际",
    "国防军事": "CCTV-7 国防军事",
    "社会与法": "CCTV-12 社会与法",
    "奥林匹克": "CCTV-16 奥林匹克",
    "农业农村": "CCTV-17 农业农村",
    "电视剧": "CCTV-8 电视剧",
    "综合": "CCTV-1 综合",
    "财经": "CCTV-2 财经",
    "综艺": "CCTV-3 综艺",
    "体育": "CCTV-5 体育",
    "电影": "CCTV-6 电影",
    "军事": "CCTV-7 国防军事",
    "纪录": "CCTV-9 纪录",
    "科教": "CCTV-10 科教",
    "戏曲": "CCTV-11 戏曲",
    "新闻": "CCTV-13 新闻",
    "少儿": "CCTV-14 少儿",
    "音乐": "CCTV-15 音乐",
    "农业": "CCTV-17 农业农村",
}

# 卫视频道【拼音缩写】对照字典
SATELLITE_ABBR_SLUGS: Dict[str, List[str]] = {
    "江西卫视": ["jxws", "jx"],
    "东方卫视": ["dfws", "shws", "df"],
    "北京卫视": ["bjws", "btv1", "bj"],
    "江苏卫视": ["jsws", "js"],
    "浙江卫视": ["zjws", "zj"],
    "湖南卫视": ["hnws", "hunws", "hn"],
    "辽宁卫视": ["lnws", "ln"],
    "安徽卫视": ["ahws", "ah"],
    "重庆卫视": ["cqws", "cq"],
    "东南卫视": ["dnws", "fjws", "dn"],
    "甘肃卫视": ["gsws", "gs"],
    "广东卫视": ["gdws", "gd"],
    "广西卫视": ["gxws", "gx"],
    "贵州卫视": ["gzws", "gz"],
    "海南卫视": ["hnws2", "lyws", "hinws"],
    "河北卫视": ["hbws", "hews", "hebws"],
    "河南卫视": ["hnaws", "henws"],
    "黑龙江卫视": ["hljws", "hljs", "hlj"],
    "湖北卫视": ["hubws", "hbws"],
    "吉林卫视": ["jlws", "jl"],
    "内蒙古卫视": ["nmgws", "nmws", "nmg"],
    "宁夏卫视": ["nxws", "nx"],
    "青海卫视": ["qhws", "qh"],
    "山东卫视": ["sdws", "sd"],
    "山西卫视": ["sxaws", "shanxiws"],
    "陕西卫视": ["sxws", "shaanxiws"],
    "深圳卫视": ["szws", "sz"],
    "四川卫视": ["scws", "sc"],
    "天津卫视": ["tjws", "tj"],
    "西藏卫视": ["xzws", "xz"],
    "新疆卫视": ["xjws", "xj"],
    "云南卫视": ["ynws", "yn"],
}

# 卫视频道【全拼】对照字典
SATELLITE_PINYIN_SLUGS: Dict[str, List[str]] = {
    "江西卫视": ["jiangxi", "jiangxiws"],
    "东方卫视": ["dongfang", "dongfangws", "shanghai", "shanghaiws"],
    "北京卫视": ["beijing", "beijingws"],
    "江苏卫视": ["jiangsu", "jiangsuws"],
    "浙江卫视": ["zhejiang", "zhejiangws"],
    "湖南卫视": ["hunan", "hunanws"],
    "辽宁卫视": ["liaoning", "liaoningws"],
    "安徽卫视": ["anhui", "anhuiws"],
    "重庆卫视": ["chongqing", "chongqingws"],
    "东南卫视": ["dongnan", "dongnanws", "fujian", "fujianws"],
    "甘肃卫视": ["gansu", "gansuws"],
    "广东卫视": ["guangdong", "guangdongws"],
    "广西卫视": ["guangxi", "guangxiws"],
    "贵州卫视": ["guizhou", "guizhouws"],
    "海南卫视": ["hainan", "hainanws", "lvyou"],
    "河北卫视": ["hebei", "hebeiws"],
    "河南卫视": ["henan", "henanws"],
    "黑龙江卫视": ["heilongjiang", "heilongjiangws"],
    "湖北卫视": ["hubei", "hubeiws"],
    "吉林卫视": ["jilin", "jilinws"],
    "内蒙古卫视": ["neimenggu", "neimengguws"],
    "宁夏卫视": ["ningxia", "ningxiaws"],
    "青海卫视": ["qinghai", "qinghaiws"],
    "山东卫视": ["shandong", "shandongws"],
    "山西卫视": ["shanxi", "shanxiws"],
    "陕西卫视": ["shaanxi", "shaanxiws"],
    "深圳卫视": ["shenzhen", "shenzhenws"],
    "四川卫视": ["sichuan", "sichuanws"],
    "天津卫视": ["tianjin", "tianjinws"],
    "西藏卫视": ["xizang", "xizangws"],
    "新疆卫视": ["xinjiang", "xinjiangws"],
    "云南卫视": ["yunnan", "yunnanws"],
}

# 电视频道（CHC系列）裂变标识字典
MOVIE_CHANNEL_SLUGS: Dict[str, List[str]] = {
    "CHC家庭影院": ["chcjtyy", "chcjt", "jtyy"],
    "CHC动作电影": ["chcdzdy", "chcdz", "dzdy"],
    "CHC高清电影": ["chcgqdy", "chchd", "chcgq", "gqdy"],
}

# 黑名单正则规则
BLACKLIST_URL_PATTERNS = re.compile(
    r'('
    r'/(rtp|udp)/|'                     # 拦截两IP中间以/rtp/或/udp/相连的组播假源
    r'\.swf(\?|$)|'                     # 拦截 Flash 插件
    r'/audio/|'                         # 拦截纯音频广播源
    r'audio\.|'                         # 拦截音频子域名
    r'/radio/|'                         # 拦截电台广播路径
    r'\.(mp3|aac|wav|ogg|flac|apk|jpg|png|gif)(\?|$)|'
    r'jdshipin\.com|'                   # 拦截假源
    r'264788\.xyz|'                     # 拦截一次性 livekey 代理源
    r'[?&](livekey|migutoken|auth_key|txSecret|wsSecret|token|sign)=|'
    r'P2PChannelID=pd://'
    r')',
    re.IGNORECASE
)


def get_resolution_speed_thresholds(width: int, height: int) -> Tuple[float, float, bool]:
    if height >= 1000 or width >= 1800:
        return SPEED_MIN_1080P, SPEED_FLUENT_1080P, True
    if height >= 700 or width >= 1200:
        return SPEED_MIN_720P, SPEED_FLUENT_720P, True
    return SPEED_MIN_SD, SPEED_FLUENT_SD, False


def get_domestic_headers(extra_headers: Dict[str, str] = None) -> Dict[str, str]:
    headers = {
        "User-Agent": TVBOX_USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
    }
    if extra_headers:
        headers.update(extra_headers)
    return headers


def load_user_channel_structure() -> Dict[str, List[str]]:
    """直接沿用 config.py 内置官方央卫视与电视频道定义与排序"""
    print(
        f"[*] 已加载 config.py 官方频道列表 (央视频道 {len(DEFAULT_CCTV_CHANNELS)} 个 + 卫视频道 {len(DEFAULT_SATELLITE_CHANNELS)} 个 + 电视频道 {len(DEFAULT_MOVIE_CHANNELS)} 个)",
        flush=True
    )
    return {
        "央视频道": list(DEFAULT_CCTV_CHANNELS),
        "卫视频道": list(DEFAULT_SATELLITE_CHANNELS),
        "电视频道": list(DEFAULT_MOVIE_CHANNELS),
    }


def is_blacklisted_url(url: str) -> bool:
    if not url:
        return True
    return bool(BLACKLIST_URL_PATTERNS.search(url))


def clean_stream_url(raw_url: str) -> str:
    if not raw_url:
        return ""
    url = raw_url.strip()
    m = re.search(r'(https?://[^\s"\'<>]+)', url)
    if m:
        url = m.group(1)
    if "$" in url:
        url = url.split("$", 1)[0].strip()
    if "/rtp/" in url.lower() or "/udp/" in url.lower():
        return ""
    if is_blacklisted_url(url):
        return ""
    return url


def normalize_channel_name(raw_name: str, custom_targets: Set[str] = None) -> str:
    """
    标准化频道名称，并严格执行：
    - 彻底过滤不采集 CCTV-8K 和 CCTV-4K（绝不让其变成 CCTV-8 或 CCTV-4）
    - 精准区分 CCTV-4 中文国际 / CCTV-4 中文国际美洲 / CCTV-4 中文国际欧洲
    - 精准识别 CCTV-5+ 体育赛事
    - 精准识别 CHC家庭影院 / CHC动作电影 / CHC高清电影 / 经典电影
    """
    if not raw_name:
        return ""
    if any(k in raw_name for k in ("广播", "电台", "之声", "FM", "调频")):
        return ""

    clean_name = raw_name.strip()

    # 1. 【最高优先级】彻底拒收 CCTV-4K 和 CCTV-8K 频道内容！
    if (
        re.search(r'(?i)(?:cctv|中央)[-_ ]?0?(?:4k|8k)', clean_name)
        or re.search(r'(?i)\b(?:4k|8k)\b', clean_name)
        or "4K" in clean_name.upper()
        or "8K" in clean_name.upper()
        or "超高清" in clean_name
    ):
        return ""

    # 2. 若与自定义目标完全一致，直接返回
    if custom_targets and clean_name in custom_targets:
        return clean_name

    # 3. 优先识别 CCTV-5+ 体育赛事（防止被归入 CCTV-5 体育）
    if (
        re.search(r'(?i)(?:cctv|中央)[-_ ]?0?5\s*(?:\+|plus|p)', clean_name)
        or "5+体育" in clean_name
        or "体育赛事" in clean_name
    ):
        return "CCTV-5+ 体育赛事"

    # 4. 优先识别 CCTV-4 中文国际欧洲 / CCTV-4 中文国际美洲 / CCTV-4 中文国际
    if re.search(r'(?i)(?:cctv|中央)[-_ ]?0?4(?![0-9kK])', clean_name) or "中文国际" in clean_name:
        if any(k in clean_name for k in ("欧洲", "欧", "EURO", "Euro", "euro")):
            return "CCTV-4 中文国际欧洲"
        if any(k in clean_name for k in ("美洲", "美", "AMER", "Amer", "amer")):
            return "CCTV-4 中文国际美洲"
        return "CCTV-4 中文国际"

    # 5. 识别电视频道（CHC系列与经典电影）
    if re.search(r'(?i)chc', clean_name) or any(k in clean_name for k in ("家庭影院", "动作电影", "高清电影")):
        if "家庭" in clean_name:
            return "CHC家庭影院"
        if "动作" in clean_name:
            return "CHC动作电影"
        if "高清" in clean_name or re.search(r'(?i)chc[-_ ]?hd', clean_name):
            return "CHC高清电影"

    if "经典电影" in clean_name:
        return "经典电影"

    # 6. 福建/东南卫视归一
    if any(k in clean_name for k in ("福建卫视", "福建台", "福建省台", "海峡卫视")):
        return "东南卫视"

    # 7. 常规 CCTV 1~17 匹配（负向断言排除 K/k/+，彻底杜绝 4K/8K/5+ 串台）
    cctv_match = re.search(r'(?i)(?:cctv|中央)[-_ ]?0?(\d{1,2})(?!\d|k|K|\+)', clean_name)
    if cctv_match:
        num = int(cctv_match.group(1))
        if 1 <= num <= 17:
            prefix = f"CCTV-{num} "
            for std_cctv in DEFAULT_CCTV_CHANNELS:
                if std_cctv.startswith(prefix):
                    return std_cctv
            return f"CCTV-{num}"

    # 8. 30个省级卫视匹配
    for sat in DEFAULT_SATELLITE_CHANNELS:
        keyword = sat.replace("卫视", "")
        if keyword in clean_name and ("卫视" in clean_name or sat in clean_name or "台" in clean_name):
            return sat
        if sat == "东方卫视" and ("上海卫视" in clean_name or "东方台" in clean_name):
            return "东方卫视"

    return ""


def parse_ocr_text_to_channel(ocr_raw_text: str, custom_targets: Set[str] = None) -> Optional[str]:
    """
    将画面台标 OCR 识别出的文字转化为精确的标准频道名：
    - 只要识别出 CCTV-4K 或 CCTV-8K，立即返回 "__BLOCK_4K_8K__"，由调用方直接剔除该源！
    - 精准识别 CCTV-4 中文国际美洲 / CCTV-4 中文国际欧洲 / CCTV-4 中文国际
    - 精准识别 CCTV-5+ 体育赛事 与 CHC 电影频道、经典电影
    """
    if not ocr_raw_text:
        return None

    compact = re.sub(r'[\s\r\n]+', ' ', ocr_raw_text).strip()
    upper_compact = re.sub(r'[\s\-_.:：·]+', '', compact.upper())

    # 1. 【最高优先级】检测画面是否为 CCTV-8K 或 CCTV-4K 超高清（不采集，直接标记拦截）
    if (
        re.search(r'(?:CCTV|CCIV|CCT|GCTV|OCTV|0CTV|中央)0?(?:4K|8K)', upper_compact)
        or "8K超高清" in upper_compact
        or "4K超高清" in upper_compact
        or "4K高清" in upper_compact
        or "超高清" in compact
    ):
        return "__BLOCK_4K_8K__"

    # 2. 检测是否为 CCTV-5+ 体育赛事
    if (
        re.search(r'(?:CCTV|CCIV|CCT|GCTV|OCTV|0CTV|中央)0?5(?:\+|PLUS)', upper_compact)
        or "5+体育" in upper_compact
        or "体育赛事" in compact
    ):
        return "CCTV-5+ 体育赛事"

    # 3. 检测是否为 CCTV-4（细分 欧洲 / 美洲 / 中文国际）
    if (
        re.search(r'(?:CCTV|CCIV|CCT|GCTV|OCTV|0CTV|中央)0?4(?![0-9K])', upper_compact)
        or "中文国际" in compact
    ):
        if any(k in compact for k in ("欧洲", "欧")) or "EURO" in upper_compact:
            return "CCTV-4 中文国际欧洲"
        if any(k in compact for k in ("美洲", "美")) or "AMER" in upper_compact:
            return "CCTV-4 中文国际美洲"
        return "CCTV-4 中文国际"

    # 4. 检测电视频道（CHC家庭影院 / CHC动作电影 / CHC高清电影 / 经典电影）
    if "家庭影院" in compact or "CHC家庭" in upper_compact:
        return "CHC家庭影院"
    if "动作电影" in compact or "CHC动作" in upper_compact:
        return "CHC动作电影"
    if "高清电影" in compact or "CHC高清" in upper_compact:
        return "CHC高清电影"
    if "经典电影" in compact:
        return "经典电影"

    # 5. 检测常规 CCTV 1~17（严格排除数字后紧跟 K 或 + 的情况）
    m_cctv = re.search(r'(?:CCTV|CCIV|CCT|GCTV|OCTV|0CTV|中央)0?(\d{1,2})(?![0-9K\+])', upper_compact)
    if m_cctv:
        num = int(m_cctv.group(1))
        if 1 <= num <= 17:
            prefix = f"CCTV-{num} "
            for std_cctv in DEFAULT_CCTV_CHANNELS:
                if std_cctv.startswith(prefix):
                    return std_cctv
            return f"CCTV-{num}"

    # 6. 若 CCTV 台标数字模糊，通过下方中文副标题识别
    if any(tag in upper_compact for tag in ("CCTV", "CCIV", "GCTV", "OCTV", "中央")):
        for sub_kw, std_cctv in CCTV_SUBTITLE_MAP.items():
            if sub_kw in compact:
                return std_cctv

    # 7. 识别省级卫视台标
    if any(k in compact for k in ("福建卫视", "东南卫视", "福建")):
        if "卫视" in compact or "东南" in compact:
            return "东南卫视"

    for sat in DEFAULT_SATELLITE_CHANNELS:
        prov = sat.replace("卫视", "")
        if sat in compact:
            return sat
        if prov in compact and ("卫视" in compact or "衛視" in compact):
            return sat

    return None


def build_channel_Lookup_map(user_struct: Dict[str, List[str]]) -> Tuple[Set[str], Dict[str, str]]:
    target_norm_set: Set[str] = set()
    norm_to_display: Dict[str, str] = {}
    raw_custom_set: Set[str] = set()

    for ch_list in user_struct.values():
        for ch in ch_list:
            raw_custom_set.add(ch.strip())

    for ch_list in user_struct.values():
        for display_ch in ch_list:
            display_ch = display_ch.strip()
            if not display_ch:
                continue
            norm_key = normalize_channel_name(display_ch, raw_custom_set) or display_ch
            target_norm_set.add(norm_key)
            if norm_key not in norm_to_display:
                norm_to_display[norm_key] = display_ch

    return target_norm_set, norm_to_display


def extract_ip_port(url: str) -> Optional[str]:
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


def extrapolate_channels_from_seed(seed_url: str, seed_channel: str = "") -> List[Tuple[str, str]]:
    results: List[Tuple[str, str]] = []
    clean_seed = clean_stream_url(seed_url)
    if not clean_seed or is_blacklisted_url(clean_seed):
        return results

    if "/rtp/" in clean_seed.lower() or "/udp/" in clean_seed.lower():
        return results

    if clean_seed.count("&") >= 4 or ".swf" in clean_seed.lower():
        return results

    # 1. CCTV 系列裂变（包含 CCTV 1~17 + CCTV-5+ + CCTV-4 中文国际美洲/欧洲）
    cctv_pattern = re.search(
        r'([/=_.-])(cctv[-_]?0?(\d{1,2}|\d\+|5plus|5p))([a-zA-Z0-9_\-]*)(\.m3u8|\.flv|\.ts|/|$|\?)',
        clean_seed,
        re.IGNORECASE
    )
    if cctv_pattern:
        prefix = clean_seed[:cctv_pattern.start(2)]
        matched_token = cctv_pattern.group(2)
        num_part = cctv_pattern.group(3)
        extra_tag = cctv_pattern.group(4)
        suffix = clean_seed[cctv_pattern.end(4):]

        if extra_tag.lower().startswith("k"):
            return results

        has_hyphen = "-" in matched_token
        has_underscore = "_" in matched_token
        has_zero = num_part.startswith("0") and len(num_part) == 2
        is_upper = matched_token.isupper()

        def fmt_token(raw_tok: str) -> str:
            return raw_tok.upper() if is_upper else raw_tok.lower()

        for num in range(1, 18):
            std_ch = next(c for c in DEFAULT_CCTV_CHANNELS if c.startswith(f"CCTV-{num} "))
            num_str = f"0{num}" if has_zero and num < 10 else str(num)
            if has_hyphen:
                token = fmt_token(f"cctv-{num_str}")
            elif has_underscore:
                token = fmt_token(f"cctv_{num_str}")
            else:
                token = fmt_token(f"cctv{num_str}")
            results.append((std_ch, f"{prefix}{token}{extra_tag}{suffix}"))

        for p_tok in ("cctv5plus", "cctv5p", "cctv5+", "cctv-5plus", "cctv-5+"):
            results.append(("CCTV-5+ 体育赛事", f"{prefix}{fmt_token(p_tok)}{extra_tag}{suffix}"))

        for am_tok in ("cctv4m", "cctv4a", "cctv4amer", "cctv4meizhou", "cctv-4m"):
            results.append(("CCTV-4 中文国际美洲", f"{prefix}{fmt_token(am_tok)}{extra_tag}{suffix}"))
        for eu_tok in ("cctv4o", "cctv4e", "cctv4euro", "cctv4ouzhou", "cctv-4o"):
            results.append(("CCTV-4 中文国际欧洲", f"{prefix}{fmt_token(eu_tok)}{extra_tag}{suffix}"))

        return results

    # 2. CHC 电影系列裂变
    for chc_ch, chc_slugs in MOVIE_CHANNEL_SLUGS.items():
        for slug in chc_slugs:
            m_chc = re.search(
                rf'([/=_.-])({re.escape(slug)})((?:[-_]?(?:hd|sd|fhd|1080p|720p))?)(\.m3u8|\.flv|\.ts|/|$|\?)',
                clean_seed,
                re.IGNORECASE
            )
            if m_chc:
                prefix = clean_seed[:m_chc.start(2)]
                matched_slug = m_chc.group(2)
                q_tag = m_chc.group(3)
                suffix = clean_seed[m_chc.end(3):]
                for target_chc, target_slugs in MOVIE_CHANNEL_SLUGS.items():
                    for cand_slug in target_slugs[:2]:
                        s_tok = cand_slug.upper() if matched_slug.isupper() else cand_slug.lower()
                        results.append((target_chc, f"{prefix}{s_tok}{q_tag}{suffix}"))
                return results

    norm_seed_ch = normalize_channel_name(seed_channel) if seed_channel else ""

    sat_check_order = []
    if norm_seed_ch in DEFAULT_SATELLITE_CHANNELS:
        sat_check_order.append(norm_seed_ch)
    for ch in DEFAULT_SATELLITE_CHANNELS:
        if ch not in sat_check_order:
            sat_check_order.append(ch)

    for sat_ch in sat_check_order:
        abbr_list = SATELLITE_ABBR_SLUGS.get(sat_ch, [])
        for slug in abbr_list:
            m = re.search(
                rf'([/=_.-])({re.escape(slug)})((?:[-_]?(?:hd|sd|fhd|uhd|4k|1080p|720p|1|2))?)(\.m3u8|\.flv|\.ts|/|$|\?)',
                clean_seed,
                re.IGNORECASE
            )
            if m:
                prefix = clean_seed[:m.start(2)]
                matched_slug = m.group(2)
                quality_tag = m.group(3)
                suffix = clean_seed[m.end(3):]

                use_ws_style = matched_slug.lower().endswith("ws") or len(matched_slug) >= 4
                for target_sat in DEFAULT_SATELLITE_CHANNELS:
                    target_candidates = SATELLITE_ABBR_SLUGS.get(target_sat, [])
                    if not target_candidates:
                        continue
                    chosen_slug = target_candidates[0]
                    if not use_ws_style and len(target_candidates) > 1:
                        chosen_slug = target_candidates[-1]
                    if matched_slug.isupper():
                        chosen_slug = chosen_slug.upper()
                    extrapolated_url = f"{prefix}{chosen_slug}{quality_tag}{suffix}"
                    results.append((target_sat, extrapolated_url))
                return results

    for sat_ch in sat_check_order:
        pinyin_list = SATELLITE_PINYIN_SLUGS.get(sat_ch, [])
        for slug in pinyin_list:
            m = re.search(
                rf'([/=_.-])({re.escape(slug)})((?:[-_]?(?:ws|hd|sd|fhd|uhd|4k|1080p|720p|1|2))*)(\.m3u8|\.flv|\.ts|/|$|\?)',
                clean_seed,
                re.IGNORECASE
            )
            if m:
                prefix = clean_seed[:m.start(2)]
                matched_slug = m.group(2)
                quality_tag = m.group(3)
                suffix = clean_seed[m.end(3):]

                has_ws_in_slug = matched_slug.lower().endswith("ws")
                for target_sat in DEFAULT_SATELLITE_CHANNELS:
                    target_candidates = SATELLITE_PINYIN_SLUGS.get(target_sat, [])
                    if not target_candidates:
                        continue
                    chosen_slug = target_candidates[1] if (has_ws_in_slug and len(target_candidates) > 1) else target_candidates[0]
                    if matched_slug.isupper():
                        chosen_slug = chosen_slug.upper()
                    extrapolated_url = f"{prefix}{chosen_slug}{quality_tag}{suffix}"
                    results.append((target_sat, extrapolated_url))
                return results

    if not norm_seed_ch or norm_seed_ch.startswith("CCTV"):
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
                for num in range(1, 18):
                    std_ch = next(c for c in DEFAULT_CCTV_CHANNELS if c.startswith(f"CCTV-{num} "))
                    formatted_num = f"{num:0{num_len}d}"
                    extrapolated_url = f"{prefix}{delimiter}{formatted_num}{sub_tag}{ext}{suffix}"
                    results.append((std_ch, extrapolated_url))
                return results

    return results
