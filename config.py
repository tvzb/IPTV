# -*- coding: utf-8 -*-
"""
IPTV 配置模块：
- 支持双模自动识别：
  1. 网页点击运行：优先读取环境变量 CUSTOM_CHANNELS_JSON 或 channels.json；
  2. GitHub 页面手动点击 "Run workflow"：自动采用上传的 config1.py 官方定义（CCTV-1~17 排除 5+，7个置顶卫视+25个拼音排序卫视）；
- 核心提速与配额优化：
  每个频道最多收集 30 条待测源，同 IP 最多保留 3 条，避免无意义死循环测速（解决 4979 条测速超时被取消问题）。
"""
import json
import os
import re
import urllib.parse
from typing import Dict, List, Optional, Set, Tuple

# 输出文件路径定义
CHANNELS_JSON_FILE = "channels.json"
RAW_SOURCES_TXT = "raw_sources.txt"
RAW_SOURCES_M3U = "raw_sources.m3u"
OUTPUT_LIVE_TXT = "live.txt"
OUTPUT_LIVE_M3U = "live.m3u"

# 仿真国内 TVBox (ExoPlayer / IjkPlayer) 标识与国内 IP 请求头
TVBOX_USER_AGENT = "okhttp/3.15"
DOMESTIC_SIM_IPS = [
    "114.114.114.114",
    "223.5.5.5",
    "119.29.29.29",
    "202.96.128.86",
    "61.139.2.69",
]

# 测速与并发参数（关键调优：单请求 2.5 秒快速超时，提高到 40 线程并发，每个频道保留前 30 条优质候选）
REQUEST_TIMEOUT = 2.5
CHUNK_DOWNLOAD_TIMEOUT = 2.0
CHUNK_DOWNLOAD_MAX_BYTES = 512 * 1024  # 512KB 采样即可完整读取 TS 头与 ffprobe 视频轨
MAX_WORKERS = 40
MIN_SPEED_KBS = 200.0  # 默认最低有效速度阈值 (KB/s)
MAX_CANDIDATES_PER_CHANNEL = 30  # 每个频道最多候选测速源数（防止测速总量膨胀到 5000+ 条被 GitHub 取消）
MAX_SAME_IP_PER_CHANNEL = 3

DEFAULT_CCTV_CHANNELS = [
    "CCTV-1 综合", "CCTV-2 财经", "CCTV-3 综艺", "CCTV-4 中文国际",
    "CCTV-5 体育", "CCTV-6 电影", "CCTV-7 国防军事", "CCTV-8 电视剧",
    "CCTV-9 纪录", "CCTV-10 科教", "CCTV-11 戏曲", "CCTV-12 社会与法",
    "CCTV-13 新闻", "CCTV-14 少儿", "CCTV-15 音乐", "CCTV-16 奥林匹克",
    "CCTV-17 农业农村",
]

DEFAULT_SATELLITE_CHANNELS = [
    # 7个置顶优先卫视
    "江西卫视", "东方卫视", "北京卫视", "江苏卫视", "浙江卫视", "湖南卫视", "辽宁卫视",
    # 其余25个省级卫视
    "安徽卫视", "兵团卫视", "重庆卫视", "东南卫视", "甘肃卫视", "广东卫视", "广西卫视",
    "贵州卫视", "海南卫视", "河北卫视", "河南卫视", "黑龙江卫视", "湖北卫视", "吉林卫视",
    "康巴卫视", "内蒙古卫视", "宁夏卫视", "青海卫视", "山东卫视", "山西卫视", "陕西卫视",
    "深圳卫视", "四川卫视", "天津卫视", "西藏卫视", "新疆卫视", "云南卫视",
]

ALL_DEFAULT_TARGETS = DEFAULT_CCTV_CHANNELS + DEFAULT_SATELLITE_CHANNELS

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
    "海南卫视": ["hainan", "lyws"],
    "河北卫视": ["hebei", "hbws", "hews"],
    "河南卫视": ["henan", "hnaws"],
    "黑龙江卫视": ["heilongjiang", "hljws", "hljs"],
    "湖北卫视": ["hubei"],
    "吉林卫视": ["jilin", "jlws"],
    "内蒙古卫视": ["neimenggu", "nmgws", "nmws"],
    "宁夏卫视": ["ningxia", "nxws"],
    "青海卫视": ["qinghai", "qhws"],
    "山东卫视": ["shandong", "sdws"],
    "山西卫视": ["shanxi", "sxaws"],
    "陕西卫视": ["shaanxi", "sxws"],
    "深圳卫视": ["shenzhen", "szws"],
    "四川卫视": ["sichuan", "scws"],
    "天津卫视": ["tianjin", "tjws"],
    "西藏卫视": ["xizang", "xzws"],
    "新疆卫视": ["xinjiang", "xjws"],
    "云南卫视": ["yunnan", "ynws"],
}

BLACKLIST_URL_PATTERNS = re.compile(
    r'('
    r'/(rtp|udp)/|'                     # 拦截两IP中间以/rtp/或/udp/相连的组播假源 (加载有数据但无图像/几秒跳转)
    r'\.swf(\?|$)|'                     # 拦截 Flash 插件
    r'/audio/|'                         # 拦截纯音频广播源
    r'audio\.|'                         # 拦截音频子域名
    r'/radio/|'                         # 拦截电台广播路径
    r'\.(mp3|aac|wav|ogg|flac|apk|jpg|png|gif)(\?|$)|'
    r'jdshipin\.com|'                   # 拦截假源
    r'264788\.xyz|'                     # 拦截一次性 livekey 代理源
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
    env_json = os.environ.get("CUSTOM_CHANNELS_JSON", "").strip()
    if env_json:
        try:
            data = json.loads(env_json)
            if isinstance(data, dict) and len(data) > 0:
                print(f"[*] 【运行模式】网页端触发：载入网站自定义分类 ({len(data)} 个分类)", flush=True)
                return data
            elif isinstance(data, list) and len(data) > 0:
                parsed_dict = {}
                for item in data:
                    if isinstance(item, dict) and "category" in item and "channels" in item:
                        parsed_dict[item["category"]] = list(item["channels"])
                if parsed_dict:
                    print(f"[*] 【运行模式】网页端触发：载入网站数组分类 ({len(parsed_dict)} 个分类)", flush=True)
                    return parsed_dict
        except Exception as e:
            print(f"[!] 解析网页端 CUSTOM_CHANNELS_JSON 异常: {e}", flush=True)

    try:
        import config1
        if hasattr(config1, "load_user_channel_structure"):
            cfg1_struct = config1.load_user_channel_structure()
            if isinstance(cfg1_struct, dict) and len(cfg1_struct) > 0:
                print(f"[*] 【运行模式】手动点击 Run workflow：成功载入 config1.py 官方定义", flush=True)
                return cfg1_struct
        elif hasattr(config1, "DEFAULT_CCTV_CHANNELS") and hasattr(config1, "DEFAULT_SATELLITE_CHANNELS"):
            print(f"[*] 【运行模式】手动点击 Run workflow：采用 config1.py 央视与卫视定义", flush=True)
            return {
                "央视频道": list(config1.DEFAULT_CCTV_CHANNELS),
                "卫视频道": list(config1.DEFAULT_SATELLITE_CHANNELS),
            }
    except ImportError:
        pass

    if os.path.exists(CHANNELS_JSON_FILE):
        try:
            with open(CHANNELS_JSON_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and len(data) > 0:
                print(f"[*] 已从 {CHANNELS_JSON_FILE} 加载频道列表 ({len(data)} 个分类)", flush=True)
                return data
        except Exception:
            pass

    print("[*] 采用内置默认官方频道列表 (CCTV 1~17 排除 5+，7优先+25拼音卫视)", flush=True)
    return {
        "央视频道": list(DEFAULT_CCTV_CHANNELS),
        "卫视频道": list(DEFAULT_SATELLITE_CHANNELS),
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
    if not raw_name:
        return ""
    if any(k in raw_name for k in ("广播", "电台", "之声", "FM", "调频")):
        return ""

    clean_name = raw_name.strip()
    if re.search(r'(?i)cctv[-_ ]?5\+', clean_name):
        return ""

    if custom_targets and clean_name in custom_targets:
        return clean_name

    cctv_match = re.search(r'(?i)(?:cctv|中央)[-_ ]?0?(\d{1,2})(?!\d)', clean_name)
    if cctv_match:
        num = int(cctv_match.group(1))
        if 1 <= num <= 17:
            prefix = f"CCTV-{num} "
            for std_cctv in DEFAULT_CCTV_CHANNELS:
                if std_cctv.startswith(prefix):
                    return std_cctv
            return f"CCTV-{num}"

    for sat in DEFAULT_SATELLITE_CHANNELS:
        keyword = sat.replace("卫视", "")
        if keyword in clean_name and ("卫视" in clean_name or sat in clean_name or "台" in clean_name):
            return sat
        if sat == "东方卫视" and ("上海卫视" in clean_name or "东方台" in clean_name):
            return "东方卫视"
        if sat == "兵团卫视" and "新疆兵团" in clean_name:
            return "兵团卫视"

    if custom_targets:
        name_stripped = re.sub(r'[\s\-_高清超清HD4K8K]', '', clean_name.upper())
        for target in custom_targets:
            t_norm = re.sub(r'[\s\-_高清超清HD4K8K]', '', target.upper())
            if t_norm and t_norm == name_stripped:
                return target

    return clean_name


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

    if clean_seed.count("&") >= 4 or ".swf" in clean_seed.lower():
        return results

    # 模式 A：四位/三位数字序号规律 (0006_1.m3u8 -> 0001_1 ~ 0017_1)
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
            return results

    # 模式 B：标准 cctv 标识规律
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
        return results

    # 模式 C：卫视拼音别名与缩写标识规律
    for std_ch, slugs in CHANNEL_SLUGS.items():
        for slug in slugs:
            pattern = re.search(rf'([/=_.-])({re.escape(slug)})([a-zA-Z0-9_\-]*)(\.m3u8|\.flv|\.ts|/|$|\?)', clean_seed, re.IGNORECASE)
            if pattern:
                prefix = clean_seed[:pattern.start(2)]
                suffix = clean_seed[pattern.end(3):]
                for other_ch, other_slugs in CHANNEL_SLUGS.items():
                    target_slug = other_slugs[0]
                    extrapolated_url = f"{prefix}{target_slug}{suffix}"
                    results.append((other_ch, extrapolated_url))
                return results

    return results
