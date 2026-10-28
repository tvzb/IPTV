# -*- coding: utf-8 -*-
"""
IPTV 配置模块：
- 支持双模自动识别：
  1. 网页点击运行：优先读取环境变量 CUSTOM_CHANNELS_JSON 或 channels.json；
  2. GitHub 页面手动点击 "Run workflow"：自动采用上传的 config1.py 官方定义（CCTV-1~17 排除 5+，7个置顶卫视+25个拼音排序卫视）；
- 完美融合 config1.py 中的三大裂变模式：
  模式 A: 数字序号规律 (0006_1.m3u8 -> 0001_1 ~ 0017_1)；
  模式 B: 字母标识规律 (cctv6.m3u8 -> cctv1 ~ cctv17)；
  模式 C: 卫视拼音与首字母规律 (jxws / jiangxi -> hnws / hunan 等 32 个省级卫视)；
- 提供国内 TVBox 仿真请求头 (okhttp/3.15 + 国内 IP) 与黑名单清洗规则。
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

# 测速与并发参数
REQUEST_TIMEOUT = 4.5
CHUNK_DOWNLOAD_TIMEOUT = 3.5
CHUNK_DOWNLOAD_MAX_BYTES = 1024 * 1024  # 1MB 采样上限
MAX_WORKERS = 28
MIN_SPEED_KBS = 200.0  # 默认最低有效速度阈值 (KB/s)

# =========================================================================
# 官方默认频道定义（与 config1.py 完全一致：排除 CCTV-5+，7置顶卫视 + 25拼音升序卫视）
# =========================================================================
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
    # 7个置顶优先卫视
    "江西卫视",
    "东方卫视",
    "北京卫视",
    "江苏卫视",
    "浙江卫视",
    "湖南卫视",
    "辽宁卫视",
    # 其余25个省级卫视（按首字汉语拼音严格字母升序排列）
    "安徽卫视",   # Anhui
    "兵团卫视",   # Bingtuan
    "重庆卫视",   # Chongqing
    "东南卫视",   # Dongnan (Fujian)
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
    "康巴卫视",   # Kangba
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

ALL_DEFAULT_TARGETS = DEFAULT_CCTV_CHANNELS + DEFAULT_SATELLITE_CHANNELS

# 频道英文/拼音标识对照字典（来自 config1.py，覆盖缩写与全拼）
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
    "海南卫视": ["hainan", "lyws"],
    "河北卫视": ["hebei", "hbws", "hews"],
    "河南卫视": ["henan", "hnaws"],
    "黑龙江卫视": ["heilongjiang", "hljws", "hljs"],
    "湖北卫视": ["hubei"],
    "吉林卫视": ["jilin", "jlws"],
    "康巴卫视": ["kangba"],
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

# 【核心防御】垃圾源、纯音频广播源、Flash插件、一次性动态Token源黑名单正则
BLACKLIST_URL_PATTERNS = re.compile(
    r'('
    r'\.swf(\?|$)|'                     # 拦截 Flash 插件
    r'/audio/|'                         # 拦截纯音频广播源
    r'audio\.|'                         # 拦截音频子域名
    r'/radio/|'                         # 拦截电台广播路径
    r'\.(mp3|aac|wav|ogg|flac|apk|jpg|png|gif)(\?|$)|'  # 拦截纯音频与静态文件
    r'jdshipin\.com|'                   # 拦截假源/垫片源
    r'264788\.xyz|'                     # 拦截绑定请求IP的一次性 livekey 代理源
    r'[?&](livekey|migutoken|auth_key|txSecret|wsSecret)=|'  # 拦截过期的动态防盗链Token
    r'P2PChannelID=pd://'               # 拦截远古 CNTV P2P 插件参数
    r')',
    re.IGNORECASE
)


def get_domestic_headers(extra_headers: Dict[str, str] = None) -> Dict[str, str]:
    """构造模拟国内 TVBox 播放器的请求头 (okhttp/3.15 + 国内运营商 IP)"""
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
    智能双模载入逻辑：
    1. 网页端点击运行：读取环境变量 CUSTOM_CHANNELS_JSON 中的网站 JSON
    2. GitHub 页面手动点击 "Run workflow"：环境变量为空，自动采用上传的 config1.py 规则
    3. 本地已有 channels.json 时自动读取 channels.json
    """
    # 模式 1：检查是否为网页端点击触发 (传有 CUSTOM_CHANNELS_JSON)
    env_json = os.environ.get("CUSTOM_CHANNELS_JSON", "").strip()
    if env_json:
        try:
            data = json.loads(env_json)
            if isinstance(data, dict) and len(data) > 0:
                print(f"[*] 【运行模式】检测到网页端触发：已载入网站自定义分类 ({len(data)} 个分类)", flush=True)
                return data
            elif isinstance(data, list) and len(data) > 0:
                parsed_dict = {}
                for item in data:
                    if isinstance(item, dict) and "category" in item and "channels" in item:
                        parsed_dict[item["category"]] = list(item["channels"])
                if parsed_dict:
                    print(f"[*] 【运行模式】检测到网页端触发：已载入网站数组分类 ({len(parsed_dict)} 个分类)", flush=True)
                    return parsed_dict
        except Exception as e:
            print(f"[!] 解析网页端 CUSTOM_CHANNELS_JSON 异常: {e}，将尝试回退", flush=True)

    # 模式 2：尝试动态从 config1.py 导入（针对手动点击 Run workflow 场景）
    try:
        import config1
        if hasattr(config1, "load_user_channel_structure"):
            cfg1_struct = config1.load_user_channel_structure()
            if isinstance(cfg1_struct, dict) and len(cfg1_struct) > 0:
                print(f"[*] 【运行模式】手动点击 Run workflow：成功载入 config1.py 官方定义配置", flush=True)
                return cfg1_struct
        elif hasattr(config1, "DEFAULT_CCTV_CHANNELS") and hasattr(config1, "DEFAULT_SATELLITE_CHANNELS"):
            print(f"[*] 【运行模式】手动点击 Run workflow：采用 config1.py 央视与卫视定义", flush=True)
            return {
                "央视频道": list(config1.DEFAULT_CCTV_CHANNELS),
                "卫视频道": list(config1.DEFAULT_SATELLITE_CHANNELS),
            }
    except ImportError:
        pass

    # 模式 3：若本地有 channels.json 文件，则载入该文件
    if os.path.exists(CHANNELS_JSON_FILE):
        try:
            with open(CHANNELS_JSON_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and len(data) > 0:
                print(f"[*] 已从 {CHANNELS_JSON_FILE} 加载频道列表 ({len(data)} 个分类)", flush=True)
                return data
        except Exception:
            pass

    # 模式 4：保底采用官方定义
    print("[*] 采用内置默认官方频道列表 (CCTV 1~17 排除 5+，7优先+25拼音卫视)", flush=True)
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
    """彻底清除 '$' 及其后面的任何文字修饰，并拦截黑名单链接"""
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
    """
    统一频道名称归一化清洗规则：
    - 严格排除 CCTV-5+；
    - 将 CCTV-1~17 映射到对应的官方全名（如 "CCTV-1 综合"、"CCTV-6 电影"）；
    - 将省级卫视映射为标准卫视名。
    """
    if not raw_name:
        return ""
    if any(k in raw_name for k in ("广播", "电台", "之声", "FM", "调频")):
        return ""

    clean_name = raw_name.strip()
    # 严格排除 CCTV-5+
    if re.search(r'(?i)cctv[-_ ]?5\+', clean_name):
        return ""

    if custom_targets and clean_name in custom_targets:
        return clean_name

    # 1. 匹配 CCTV-1 至 CCTV-17
    cctv_match = re.search(r'(?i)(?:cctv|中央)[-_ ]?0?(\d{1,2})(?!\d)', clean_name)
    if cctv_match:
        num = int(cctv_match.group(1))
        if 1 <= num <= 17:
            prefix = f"CCTV-{num} "
            for std_cctv in DEFAULT_CCTV_CHANNELS:
                if std_cctv.startswith(prefix):
                    return std_cctv
            return f"CCTV-{num}"

    # 2. 匹配省级卫视
    for sat in DEFAULT_SATELLITE_CHANNELS:
        keyword = sat.replace("卫视", "")
        if keyword in clean_name and ("卫视" in clean_name or sat in clean_name or "台" in clean_name):
            return sat
        if sat == "东方卫视" and ("上海卫视" in clean_name or "东方台" in clean_name):
            return "东方卫视"
        if sat == "兵团卫视" and "新疆兵团" in clean_name:
            return "兵团卫视"

    # 自定义目标精确/去修饰匹配
    if custom_targets:
        name_stripped = re.sub(r'[\s\-_高清超清HD4K8K]', '', clean_name.upper())
        for target in custom_targets:
            t_norm = re.sub(r'[\s\-_高清超清HD4K8K]', '', target.upper())
            if t_norm and t_norm == name_stripped:
                return target

    return clean_name


def build_channel_Lookup_map(user_struct: Dict[str, List[str]]) -> Tuple[Set[str], Dict[str, str]]:
    """构建目标频道归一化集合与显示名称映射表"""
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
    """提取 URL 中的 IP:Port 或域名"""
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
    """
    智能种子源裂变推导引擎（完美结合 config1.py 的三大裂变模式）：
    模式 A: 数字序号规律 (如 tsfile/live/0006_1.m3u8 裂变到 0001_1 ~ 0017_1)
    模式 B: 字母标识规律 (如 cctv6.m3u8 裂变到 cctv1 ~ cctv17)
    模式 C: 卫视全拼与缩写规律 (如 jxws / jiangxi 裂变到 hnws / hunan 等 32 个省级卫视)
    """
    results: List[Tuple[str, str]] = []
    clean_seed = clean_stream_url(seed_url)
    if not clean_seed or is_blacklisted_url(clean_seed):
        return results

    if clean_seed.count("&") >= 4 or ".swf" in clean_seed.lower():
        return results

    # === 模式 A：四位/三位数字序号加后缀规律 (0006_1.m3u8 -> 0001_1 ~ 0017_1) ===
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
        return results

    # === 模式 C：卫视拼音别名与缩写标识规律 (如 jxws -> hnws, jiangxi -> hunan) ===
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
