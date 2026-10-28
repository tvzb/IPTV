# -*- coding: utf-8 -*-
"""
IPTV 频道标准化、网页自定义频道联动、URL 铁血清洗 ($尾缀剔除 + 黑名单)
与央卫视双向智能裂变引擎 (CCTV 0006_1.m3u8 <-> 0001~0017 & 卫视 jxws <-> hnws 等全套缩写推导)
"""
import json
import os
import re
from typing import Dict, List, Set, Tuple

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

# 与网页端 index.html 完全一致的默认分类及频道
DEFAULT_CHANNEL_STRUCTURE: Dict[str, List[str]] = {
    "央视频道": [
        "CCTV-1 综合", "CCTV-2 财经", "CCTV-3 综艺", "CCTV-4 中文国际",
        "CCTV-5 体育", "CCTV-6 电影", "CCTV-7 国防军事", "CCTV-8 电视剧",
        "CCTV-9 纪录", "CCTV-10 科教", "CCTV-11 戏曲", "CCTV-12 社会与法",
        "CCTV-13 新闻", "CCTV-14 少儿", "CCTV-15 音乐", "CCTV-16 奥林匹克",
        "CCTV-17 农业农村"
    ],
    "卫视频道": [
        "江西卫视", "东方卫视", "北京卫视", "江苏卫视", "浙江卫视", "湖南卫视", "辽宁卫视",
        "安徽卫视", "重庆卫视", "东南卫视", "甘肃卫视", "广东卫视", "广西卫视", "贵州卫视",
        "海南卫视", "河北卫视", "河南卫视", "黑龙江卫视", "湖北卫视", "吉林卫视", "内蒙古卫视",
        "宁夏卫视", "青海卫视", "山东卫视", "山西卫视", "陕西卫视", "深圳卫视", "四川卫视",
        "天津卫视", "西藏卫视", "新疆卫视", "云南卫视"
    ]
}

# 标准央视频道内部归一化键名
DEFAULT_CCTV_CHANNELS = [
    "CCTV-1", "CCTV-2", "CCTV-3", "CCTV-4", "CCTV-5", "CCTV-5+",
    "CCTV-6", "CCTV-7", "CCTV-8", "CCTV-9", "CCTV-10", "CCTV-11",
    "CCTV-12", "CCTV-13", "CCTV-14", "CCTV-15", "CCTV-16", "CCTV-17",
    "CCTV-4K", "CCTV-8K"
]

# 标准省级卫视列表
DEFAULT_SATELLITE_CHANNELS = [
    "江西卫视", "东方卫视", "北京卫视", "江苏卫视", "浙江卫视", "湖南卫视", "辽宁卫视",
    "安徽卫视", "重庆卫视", "东南卫视", "甘肃卫视", "广东卫视", "广西卫视", "贵州卫视",
    "海南卫视", "河北卫视", "河南卫视", "黑龙江卫视", "湖北卫视", "吉林卫视", "内蒙古卫视",
    "宁夏卫视", "青海卫视", "山东卫视", "山西卫视", "陕西卫视", "深圳卫视", "四川卫视",
    "天津卫视", "西藏卫视", "新疆卫视", "云南卫视", "兵团卫视"
]

# 卫视频道拼音首字母缩写与全拼映射表（用于智能裂变：如 jxws -> hnws, zjws, dfws 等）
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
}

# 补充常见卫视缩写变体反向识别表
ABBR_TO_SAT: Dict[str, str] = {}
FULL_PY_TO_SAT: Dict[str, str] = {}
for sat_name, (abbr_code, full_py) in SATELLITE_CODE_MAP.items():
    ABBR_TO_SAT[abbr_code] = sat_name
    FULL_PY_TO_SAT[full_py] = sat_name
# 兼容 hljws / nmgws / hainws 等常见多字母首拼
ABBR_TO_SAT["hljws"] = "黑龙江卫视"
ABBR_TO_SAT["nmgws"] = "内蒙古卫视"
ABBR_TO_SAT["hainws"] = "海南卫视"
ABBR_TO_SAT["shws"] = "东方卫视"

# 常见地方台、体育电影及港澳频道别名映射（支持网页端用户自定义添加的频道）
CUSTOM_CHANNEL_ALIASES: Dict[str, List[str]] = {
    "CCTV-4K": ["CCTV4K", "CCTV-4K超高清", "央视4K"],
    "CCTV-8K": ["CCTV8K", "CCTV-8K超高清", "央视8K"],
    "广东体育": ["广东体育", "广体", "GDTV体育"],
    "五星体育": ["五星体育", "上海体育"],
    "劲爆体育": ["劲爆体育"],
    "风云足球": ["风云足球"],
    "咪咕体育1台": ["咪咕体育", "咪咕体育1"],
    "CHC高清电影": ["CHC高清电影", "CHC高清"],
    "CHC家庭影院": ["CHC家庭影院", "CHC家庭"],
    "CHC动作电影": ["CHC动作电影", "CHC动作"],
    "欢笑剧场": ["欢笑剧场", "上海欢笑剧场"],
    "都市剧场": ["都市剧场", "上海都市剧场"],
    "翡翠台": ["翡翠台", "TVB翡翠台", "香港翡翠台"],
    "明珠台": ["明珠台", "TVB明珠台"],
    "凤凰卫视中文台": ["凤凰中文", "凤凰卫视中文", "凤凰卫视中文台", "凤凰卫视"],
    "凤凰卫视资讯台": ["凤凰资讯", "凤凰卫视资讯", "凤凰卫视资讯台"],
    "凤凰卫视香港台": ["凤凰香港", "凤凰卫视香港", "凤凰卫视香港台"],
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
    """
    构造模拟国内 TVBox 播放器的请求头：
    携带 okhttp/3.15 User-Agent 及国内主流运营商 IP 的 X-Forwarded-For / X-Real-IP / Client-IP，
    最大化模拟国内机顶盒真实播放请求环境。
    """
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
    结合网页上设置的频道列表加载目标分类与频道：
    1. 优先读取环境变量 CUSTOM_CHANNELS_JSON (由前端页面触发 GitHub Action workflow_dispatch 传入)
    2. 其次读取仓库根目录下的 channels.json (由网页保存或导出提交)
    3. 若均不存在，则回退使用与网页完全一致的默认分类及频道列表
    """
    env_json = os.environ.get("CUSTOM_CHANNELS_JSON", "").strip()
    if env_json:
        try:
            data = json.loads(env_json)
            if isinstance(data, dict) and len(data) > 0:
                print(f"[*] 已从网页端 workflow_dispatch 参数加载自定义频道分类 ({len(data)} 个分类)", flush=True)
                return data
            elif isinstance(data, list) and len(data) > 0:
                parsed_dict = {}
                for item in data:
                    if isinstance(item, dict) and "category" in item and "channels" in item:
                        parsed_dict[item["category"]] = list(item["channels"])
                if parsed_dict:
                    print(f"[*] 已从网页端 workflow_dispatch 数组加载自定义频道分类 ({len(parsed_dict)} 个分类)", flush=True)
                    return parsed_dict
        except Exception as e:
            print(f"[!] 解析环境变量 CUSTOM_CHANNELS_JSON 失败，将尝试读取本地文件: {e}", flush=True)

    if os.path.exists(CHANNELS_JSON_FILE):
        try:
            with open(CHANNELS_JSON_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and len(data) > 0:
                print(f"[*] 已从 {CHANNELS_JSON_FILE} 加载网页端设置的频道列表 ({len(data)} 个分类)", flush=True)
                return data
            elif isinstance(data, list) and len(data) > 0:
                parsed_dict = {}
                for item in data:
                    if isinstance(item, dict) and "category" in item and "channels" in item:
                        parsed_dict[item["category"]] = list(item["channels"])
                if parsed_dict:
                    return parsed_dict
        except Exception as e:
            print(f"[!] 读取 {CHANNELS_JSON_FILE} 异常，使用默认频道列表: {e}", flush=True)

    return {k: list(v) for k, v in DEFAULT_CHANNEL_STRUCTURE.items()}


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
    if "$" in url:
        url = url.split("$", 1)[0].strip()
    if is_blacklisted_url(url):
        return ""
    return url


def normalize_channel_name(raw_name: str, custom_targets: Set[str] = None) -> str:
    """
    频道统一命名归一化函数：
    将各种异构写法（如 "CCTV-1 综合"、"CCTV1HD"、"中央1套"、"江西卫视高清"）
    统一映射为标准键名（如 "CCTV-1"、"江西卫视" 或网页自定义频道的标准键名）。
    """
    if not raw_name:
        return ""
    if any(k in raw_name for k in ("广播", "电台", "之声", "FM", "调频")):
        return ""

    clean_raw = raw_name.strip()
    if custom_targets and clean_raw in custom_targets:
        return clean_raw

    name = clean_raw.upper()
    # 剥离常见修饰词与台标后缀（包括网页端默认名称中的“综合”、“财经”、“综艺”等后缀）
    name_stripped = re.sub(
        r'(\[.*?\]|\(.*?\)|（.*?）|HD|FHD|UHD|4K|8K|超高清|超清|高清|蓝光|标清|频道|直播|综合|财经|综艺|中文国际|体育赛事|体育|电影|国防军事|电视剧|纪录|科教|戏曲|社会与法|新闻|少儿|音乐|奥林匹克|农业农村|-|_|\s+)',
        '',
        name
    )

    # 匹配 CCTV-4K / CCTV-8K
    if "CCTV4K" in name.replace("-", "").replace(" ", "") or "央视4K" in name:
        return "CCTV-4K"
    if "CCTV8K" in name.replace("-", "").replace(" ", "") or "央视8K" in name:
        return "CCTV-8K"

    # 匹配 CCTV-5+
    if re.search(r'(CCTV|中央)0?5\+', name) or "体育赛事" in clean_raw:
        return "CCTV-5+"

    # 匹配 CCTV-1 ~ CCTV-17 (严防 CCTV-10 被识别成 CCTV-1)
    m_cctv = re.search(r'(?:CCTV|中央)0?(\d{1,2})(?!\d)', name_stripped)
    if m_cctv:
        num = int(m_cctv.group(1))
        if 1 <= num <= 17:
            return f"CCTV-{num}"

    # 匹配省级卫视
    if "旅游卫视" in clean_raw:
        return "海南卫视"
    if ("东南" in clean_raw or "福建" in clean_raw) and "卫视" in clean_raw:
        return "东南卫视"

    for sat in DEFAULT_SATELLITE_CHANNELS:
        prefix = sat.replace("卫视", "")
        if prefix in clean_raw and ("卫视" in clean_raw or prefix in ("东方", "东南", "兵团")):
            return sat

    # 匹配常见自定义频道别名
    for std_custom, aliases in CUSTOM_CHANNEL_ALIASES.items():
        for alias in aliases:
            if alias.upper() in name.replace("-", "").replace(" ", ""):
                return std_custom

    # 如果网页自定义了其他专属频道名，进行去符号模糊匹配
    if custom_targets:
        for target in custom_targets:
            t_norm = re.sub(r'[\s\-_高清超清HD]', '', target.upper())
            if t_norm and t_norm == name_stripped:
                return target

    return ""


def build_channel_Lookup_map(user_struct: Dict[str, List[str]]) -> Tuple[Set[str], Dict[str, str]]:
    """
    根据网页端设置的分类与频道结构，构建：
    1. target_norm_set: 所有目标频道的归一化键名集合 (如 "CCTV-1", "江西卫视", "广东体育")
    2. norm_to_display: 归一化键名 -> 网页端设置的具体显示名称 (如 "CCTV-1" -> "CCTV-1 综合")
    """
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
    """提取 URL 中的 IP:Port 或域名，用于同服务器防独占与反向搜索"""
    m = re.search(r'https?://([^/]+)', url)
    return m.group(1) if m else ""


def extrapolate_channels_from_seed(seed_url: str, seed_channel: str = "") -> List[Tuple[str, str]]:
    """
    第四步【央卫视双向智能裂变与补齐引擎】：
    1. 央视频道裂变：
       - 例如：CCTV6,http://61.136.172.236:9901/tsfile/live/0006_1.m3u8
         “0006_1.m3u8” 对应 CCTV-6 -> 自动裂变出 CCTV-1 (“0001_1.m3u8”) ~ CCTV-17 (“0017_1.m3u8”) 全套央视频道！
       - 支持 `/0006_1.m3u8`、`/cctv6.m3u8`、`/cctv-6.m3u8`、`/cctv6hd.m3u8`、`/cctv06/` 等所有主流编号规律。
    2. 卫视频道裂变：
       - 例如：江西卫视,http://goo.bkpcp.top/mg/jxws
         识别出路径中的 `jxws` (江西卫视首字母缩写) -> 自动裂变推导出：
         湖南卫视 `http://goo.bkpcp.top/mg/hnws`、浙江卫视 `http://goo.bkpcp.top/mg/zjws`、
         东方卫视 `http://goo.bkpcp.top/mg/dfws`、江苏卫视 `http://goo.bkpcp.top/mg/jsws` 等全部省级卫视！
       - 同时支持全拼规律：如 `.../jiangxi.m3u8` 或 `.../live/jiangxi/` -> 自动裂变出 `hunan`、`zhejiang`、`dongfang` 等全部省级卫视！
    """
    derived: List[Tuple[str, str]] = []
    if not seed_url or is_blacklisted_url(seed_url):
        return derived

    if seed_url.count("&") >= 4 or ".swf" in seed_url.lower():
        return derived

    # =========================================================================
    # 规则 1：央视频道四位数字零填充编号裂变 (如 0006_1.m3u8 -> 0001_1.m3u8 ~ 0017_1.m3u8)
    # =========================================================================
    m_zero4 = re.search(r'(/)00(0[1-9]|1[0-7])(_\d+\.m3u8|\.m3u8)(.*)', seed_url, re.IGNORECASE)
    if m_zero4:
        slash_prefix = m_zero4.group(1)
        tail_ext = m_zero4.group(3) + m_zero4.group(4)
        base_head = seed_url[:m_zero4.start()]
        for i in range(1, 18):
            new_url = f"{base_head}{slash_prefix}{i:04d}{tail_ext}"
            derived.append((f"CCTV-{i}", new_url))
        return derived

    # =========================================================================
    # 规则 2：央视频道 cctv 标识路径裂变 (如 cctv6.m3u8 / cctv-6hd.m3u8 -> cctv1 ~ cctv17)
    # =========================================================================
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

    # =========================================================================
    # 规则 3：卫视频道拼音首字母缩写裂变 (如 http://goo.bkpcp.top/mg/jxws -> .../mg/hnws 等)
    # =========================================================================
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

    # =========================================================================
    # 规则 4：卫视频道全拼路径裂变 (如 .../1080p/jiangxi.m3u8 -> .../1080p/hunan.m3u8 等)
    # =========================================================================
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
