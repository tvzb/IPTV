# -*- coding: utf-8 -*-
"""
IPTV 频道标准化、网页自定义频道联动、URL 铁血清洗 ($尾缀剔除 + 黑名单)
与央卫视双向智能裂变引擎
"""
import json
import os
import re
import urllib.parse
from typing import Dict, List, Set, Tuple

# 输出文件路径定义
CHANNELS_JSON_FILE = "channels.json"
RAW_SOURCES_TXT = "raw_sources.txt"
RAW_SOURCES_M3U = "raw_sources.m3u"
OUTPUT_LIVE_TXT = "live.txt"
OUTPUT_LIVE_M3U = "live.m3u"

# 仿真国内 TVBox 标识与国内 IP 请求头
TVBOX_USER_AGENT = "okhttp/3.15"
DOMESTIC_SIM_IPS = [
    "114.114.114.114",
    "223.5.5.5",
    "119.29.29.29",
    "202.96.128.86",
    "61.139.2.69",
]

# 测速与并发参数
REQUEST_TIMEOUT = 4.5
CHUNK_DOWNLOAD_TIMEOUT = 3.5
CHUNK_DOWNLOAD_MAX_BYTES = 1024 * 1024  # 1MB 采样上限
MAX_WORKERS = 28
MIN_SPEED_KBS = 200.0  # 默认最低有效速度阈值 (KB/s)

# 默认官方频道定义（CCTV-1 至 CCTV-17，严格排除 CCTV-5+）
DEFAULT_CCTV_CHANNELS = [
    "CCTV-1 综合", "CCTV-2 财经", "CCTV-3 综艺", "CCTV-4 中文国际",
    "CCTV-5 体育", "CCTV-6 电影", "CCTV-7 国防军事", "CCTV-8 电视剧",
    "CCTV-9 纪录", "CCTV-10 科教", "CCTV-11 戏曲", "CCTV-12 社会与法",
    "CCTV-13 新闻", "CCTV-14 少儿", "CCTV-15 音乐", "CCTV-16 奥林匹克",
    "CCTV-17 农业农村"
]

# 默认省级卫视频道定义
DEFAULT_SATELLITE_CHANNELS = [
    "江西卫视", "东方卫视", "北京卫视", "江苏卫视", "浙江卫视", "湖南卫视", "辽宁卫视",
    "安徽卫视", "兵团卫视", "重庆卫视", "东南卫视", "甘肃卫视", "广东卫视", "广西卫视", "贵州卫视",
    "海南卫视", "河北卫视", "河南卫视", "黑龙江卫视", "湖北卫视", "吉林卫视", "康巴卫视",
    "内蒙古卫视", "宁夏卫视", "青海卫视", "山东卫视", "山西卫视", "陕西卫视", "深圳卫视", "四川卫视",
    "天津卫视", "西藏卫视", "新疆卫视", "云南卫视"
]

# 卫视频道拼音首字母缩写与全拼映射表（用于智能裂变）
SATELLITE_CODE_MAP: Dict[str, Tuple[str, str]] = {
    "江西卫视": ("jxws", "jiangxi"),
    "湖南卫视": ("hnws", "hunan"),
    "浙江卫视": ("zjws", "zhejiang"),
    "江苏卫视": ("jsws", "jiangsu"),
    "东方卫视": ("dfws", "dongfang"),
    "北京卫视": ("bjws", "beijing"),
    "广东卫视": ("gdws", "guangdong"),
    "深圳卫视": ("szws", "shenzhen"),
    "山东卫视": ("sdws", "shandong"),
    "安徽卫视": ("ahws", "anhui"),
    "湖北卫视": ("hbws", "hubei"),
    "四川卫视": ("scws", "sichuan"),
    "辽宁卫视": ("lnws", "liaoning"),
    "黑龙江卫视": ("hljs", "heilongjiang"),
    "天津卫视": ("tjws", "tianjin"),
    "重庆卫视": ("cqws", "chongqing"),
    "河北卫视": ("hews", "hebei"),
    "河南卫视": ("hnaws", "henan"),
    "东南卫视": ("dnws", "dongnan"),
    "广西卫视": ("gxws", "guangxi"),
    "贵州卫视": ("gzws", "guizhou"),
    "云南卫视": ("ynws", "yunnan"),
    "陕西卫视": ("sxws", "shaanxi"),
    "山西卫视": ("sxaws", "shanxi"),
    "吉林卫视": ("jlws", "jilin"),
    "海南卫视": ("lyws", "hainan"),
    "甘肃卫视": ("gsws", "gansu"),
    "宁夏卫视": ("nxws", "ningxia"),
    "青海卫视": ("qhws", "qinghai"),
    "新疆卫视": ("xjws", "xinjiang"),
    "内蒙古卫视": ("nmws", "neimenggu"),
    "西藏卫视": ("xzws", "xizang"),
    "兵团卫视": ("btws", "bingtuan"),
}

ABBR_TO_SAT: Dict[str, str] = {}
FULL_PY_TO_SAT: Dict[str, str] = {}
for sat_name, (abbr_code, full_py) in SATELLITE_CODE_MAP.items():
    ABBR_TO_SAT[abbr_code] = sat_name
    FULL_PY_TO_SAT[full_py] = sat_name

ABBR_TO_SAT["hljws"] = "黑龙江卫视"
ABBR_TO_SAT["nmgws"] = "内蒙古卫视"
ABBR_TO_SAT["hainws"] = "海南卫视"
ABBR_TO_SAT["shws"] = "东方卫视"

# 垃圾源、纯音频广播源、Flash插件黑名单正则
BLACKLIST_URL_PATTERNS = re.compile(
    r'('
    r'\.swf(\?|$)|'
    r'/audio/|'
    r'audio\.|'
    r'/radio/|'
    r'\.(mp3|aac|wav|ogg|flac|apk|jpg|png|gif)(\?|$)|'
    r'jdshipin\.com|'
    r'264788\.xyz|'
    r'[?&](livekey|migutoken|auth_key|txSecret|wsSecret)=|'
    r'P2PChannelID=pd://'
    r')',
    re.IGNORECASE
)

def get_domestic_headers(extra_headers: Dict[str, str] = None) -> Dict[str, str]:
    ip = DOMESTIC_SIM_IPS[0]
    headers = {
        "User-Agent": TVBOX_USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "zh-CN,zh;q=0.9",
        "Connection": "keep-alive",
        "X-Forwarded-For": ip,
        "X-Real-IP": ip,
        "Client-IP": ip,
    }
    if extra_headers:
        headers.update(extra_headers)
    return headers

def load_user_channel_structure() -> Dict[str, List[str]]:
    """
    【核心逻辑修改】：
    - 只有当通过外部网页端触发工作流并传入非空的 CUSTOM_CHANNELS_JSON 环境变量时，才使用网页提交的自定义 JSON。
    - 手动在 GitHub Actions 界面点 Run workflow、或定时/推送触发时，默认直接采用本文件内置的默认央视与卫视配置！
    """
    env_json = os.environ.get("CUSTOM_CHANNELS_JSON", "").strip()
    if env_json:
        try:
            data = json.loads(env_json)
            if isinstance(data, dict) and data:
                print("[*] 已从网页端 workflow_dispatch 参数加载自定义频道分类", flush=True)
                return data
            elif isinstance(data, list) and data:
                parsed_dict = {}
                for item in data:
                    if isinstance(item, dict) and "category" in item and "channels" in item:
                        parsed_dict[item["category"]] = list(item["channels"])
                if parsed_dict:
                    print("[*] 已从网页端 workflow_dispatch 数组加载自定义频道分类", flush=True)
                    return parsed_dict
        except Exception as e:
            print(f"[!] 解析环境变量 CUSTOM_CHANNELS_JSON 失败，将采用默认内置配置: {e}", flush=True)

    print("[*] 正在使用默认内置频道配置 (config.py)", flush=True)
    return {
        "央视频道": DEFAULT_CCTV_CHANNELS,
        "卫视频道": DEFAULT_SATELLITE_CHANNELS,
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
    if is_blacklisted_url(url):
        return ""
    return url

def normalize_channel_name(raw_name: str, custom_targets: Set[str] = None) -> str:
    if not raw_name:
        return ""
    if any(k in raw_name for k in ("广播", "电台", "之声", "FM", "调频")):
        return ""

    clean_raw = raw_name.strip()
    if custom_targets and clean_raw in custom_targets:
        return clean_raw

    name = clean_raw.upper()
    name_stripped = re.sub(
        r'(\[.*?\]|\(.*?\)|（.*?）|HD|FHD|UHD|4K|8K|超高清|超清|高清|蓝光|标清|频道|直播|综合|财经|综艺|中文国际|体育赛事|体育|电影|国防军事|电视剧|纪录|科教|戏曲|社会与法|新闻|少儿|音乐|奥林匹克|农业农村|-|_|\s+)',
        '',
        name
    )

    if "CCTV4K" in name.replace("-", "").replace(" ", "") or "央视4K" in name:
        return "CCTV-4K"
    if "CCTV8K" in name.replace("-", "").replace(" ", "") or "央视8K" in name:
        return "CCTV-8K"
    if re.search(r'(CCTV|中央)0?5\+', name) or "体育赛事" in clean_raw:
        return "CCTV-5+"

    m_cctv = re.search(r'(?:CCTV|中央)0?(\d{1,2})(?!\d)', name_stripped)
    if m_cctv:
        num = int(m_cctv.group(1))
        if 1 <= num <= 17:
            return f"CCTV-{num}"

    if "旅游卫视" in clean_raw:
        return "海南卫视"
    if ("东南" in clean_raw or "福建" in clean_raw) and "卫视" in clean_raw:
        return "东南卫视"

    for sat in DEFAULT_SATELLITE_CHANNELS:
        prefix = sat.replace("卫视", "")
        if prefix in clean_raw and ("卫视" in clean_raw or prefix in ("东方", "东南", "兵团")):
            return sat

    if custom_targets:
        for target in custom_targets:
            t_norm = re.sub(r'[\s\-_高清超清HD]', '', target.upper())
            if t_norm and t_norm == name_stripped:
                return target

    return ""

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

def extract_ip_port(url: str) -> str:
    m = re.search(r'https?://([^/]+)', url)
    return m.group(1) if m else ""

def extrapolate_channels_from_seed(seed_url: str, seed_channel: str = "") -> List[Tuple[str, str]]:
    derived: List[Tuple[str, str]] = []
    if not seed_url or is_blacklisted_url(seed_url):
        return derived

    if seed_url.count("&") >= 4 or ".swf" in seed_url.lower():
        return derived

    m_zero4 = re.search(r'(/)00(0[1-9]|1[0-7])(_\d+\.m3u8|\.m3u8)(.*)', seed_url, re.IGNORECASE)
    if m_zero4:
        slash_prefix = m_zero4.group(1)
        tail_ext = m_zero4.group(3) + m_zero4.group(4)
        base_head = seed_url[:m_zero4.start()]
        for i in range(1, 18):
            new_url = f"{base_head}{slash_prefix}{i:04d}{tail_ext}"
            derived.append((f"CCTV-{i}", new_url))
        return derived

    m_cctv_id = re.search(
        r'(?<=[/=_])(cctv[-_]?)(0?[1-9]|1[0-7])(?!\d|\+)(hd|p|fhd)?(?=\.m3u8|\.ts|\.flv|/|$|\?|&)',
        seed_url,
        re.IGNORECASE
    )
    if m_cctv_id:
        num_str = m_cctv_id.group(2)
        is_zero_padded = len(num_str) == 2 and num_str.startswith("0")
        start_idx, end_idx = m_cctv_id.span(2)
        for i in range(1, 18):
            rep_num = f"{i:02d}" if is_zero_padded else str(i)
            new_url = seed_url[:start_idx] + rep_num + seed_url[end_idx:]
            derived.append((f"CCTV-{i}", new_url))
        return derived

    for abbr_key, sat_std_name in ABBR_TO_SAT.items():
        pattern = re.compile(
            rf'(?<=[/=_.-])({re.escape(abbr_key)})(hd|fhd|_1|_hd)?(?=\.m3u8|\.ts|\.flv|/|$|\?|&)',
            re.IGNORECASE
        )
        m_abbr = pattern.search(seed_url)
        if m_abbr:
            matched_token = m_abbr.group(1)
            is_upper = matched_token.isupper()
            start_idx, end_idx = m_abbr.span(1)
            for target_sat, (target_abbr, _) in SATELLITE_CODE_MAP.items():
                rep_code = target_abbr.upper() if is_upper else target_abbr.lower()
                new_url = seed_url[:start_idx] + rep_code + seed_url[end_idx:]
                derived.append((target_sat, new_url))
            return derived

    for py_key, sat_std_name in FULL_PY_TO_SAT.items():
        pattern_py = re.compile(
            rf'(?<=[/=_.-])({re.escape(py_key)})(ws|tv|hd|fhd|_1)?(?=\.m3u8|\.ts|\.flv|/|$|\?|&)',
            re.IGNORECASE
        )
        m_py = pattern_py.search(seed_url)
        if m_py:
            matched_token = m_py.group(1)
            is_upper = matched_token.isupper()
            start_idx, end_idx = m_py.span(1)
            for target_sat, (_, target_py) in SATELLITE_CODE_MAP.items():
                rep_py = target_py.upper() if is_upper else target_py.lower()
                new_url = seed_url[:start_idx] + rep_py + seed_url[end_idx:]
                derived.append((target_sat, new_url))
            return derived

    return derived
