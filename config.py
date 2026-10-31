# -*- coding: utf-8 -*-
"""
IPTV 配置模块（TVBox 零黑屏 + 国内直连流畅度优先排序版）
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

# 搜索采集页数与每页条数配置
DEFAULT_SEARCH_PAGES = int(os.environ.get("SEARCH_PAGES", "1") or "1")
RESULTS_PER_SEARCH_PAGE = 20

# 测速与并发参数（升级为多切片稳态测速，彻底过滤假快源和黑屏跳台源）
REQUEST_TIMEOUT = 2.6
CHUNK_DOWNLOAD_TIMEOUT = 3.2          # 延长稳态采样窗口至 3.2 秒，测出真实持续带宽而非首包突发缓存
CHUNK_DOWNLOAD_MAX_BYTES = 1200 * 1024  # 采样上限提升至 1.2MB，确保连续拉取 2~3 个切片验证不断流
MIN_REQUIRED_BYTES = 260 * 1024       # 至少完整下载 260KB 真实视频流才算有效
MAX_WORKERS = 36
MIN_SPEED_KBS = 220.0                 # 默认最低持续有效速度阈值 (KB/s)
MAX_CANDIDATES_PER_CHANNEL = 65
MAX_SAME_IP_PER_CHANNEL = 5

# 是否开启测速时视频画面左上角台标 OCR 识别自动纠错
ENABLE_LOGO_OCR = os.environ.get("ENABLE_LOGO_OCR", "true").strip().lower() not in ("false", "0", "no")

# 官方默认央视频道（CCTV 1~17，严格排除 CCTV-5+）
DEFAULT_CCTV_CHANNELS = [
    "CCTV-1 综合", "CCTV-2 财经", "CCTV-3 综艺", "CCTV-4 中文国际",
    "CCTV-5 体育", "CCTV-6 电影", "CCTV-7 国防军事", "CCTV-8 电视剧",
    "CCTV-9 纪录", "CCTV-10 科教", "CCTV-11 戏曲", "CCTV-12 社会与法",
    "CCTV-13 新闻", "CCTV-14 少儿", "CCTV-15 音乐", "CCTV-16 奥林匹克",
    "CCTV-17 农业农村",
]

# 官方默认省级卫视频道
DEFAULT_SATELLITE_CHANNELS = [
    # 7个置顶优先卫视
    "江西卫视", "东方卫视", "北京卫视", "江苏卫视", "浙江卫视", "湖南卫视", "辽宁卫视",
    # 其余23个省级卫视（按首字拼音升序排列）
    "安徽卫视", "重庆卫视", "东南卫视", "甘肃卫视", "广东卫视",
    "广西卫视", "贵州卫视", "海南卫视", "河北卫视", "河南卫视",
    "黑龙江卫视", "湖北卫视", "吉林卫视", "内蒙古卫视", "宁夏卫视",
    "青海卫视", "山东卫视", "山西卫视", "陕西卫视", "深圳卫视",
    "四川卫视", "天津卫视", "西藏卫视", "新疆卫视", "云南卫视",
]

ALL_DEFAULT_TARGETS = DEFAULT_CCTV_CHANNELS + DEFAULT_SATELLITE_CHANNELS

# 央视台标副标题关键词辅助映射
CCTV_SUBTITLE_MAP: Dict[str, str] = {
    "综合": "CCTV-1 综合",
    "财经": "CCTV-2 财经",
    "综艺": "CCTV-3 综艺",
    "中文国际": "CCTV-4 中文国际",
    "体育": "CCTV-5 体育",
    "电影": "CCTV-6 电影",
    "国防军事": "CCTV-7 国防军事",
    "军事": "CCTV-7 国防军事",
    "电视剧": "CCTV-8 电视剧",
    "纪录": "CCTV-9 纪录",
    "科教": "CCTV-10 科教",
    "戏曲": "CCTV-11 戏曲",
    "社会与法": "CCTV-12 社会与法",
    "新闻": "CCTV-13 新闻",
    "少儿": "CCTV-14 少儿",
    "音乐": "CCTV-15 音乐",
    "奥林匹克": "CCTV-16 奥林匹克",
    "农业农村": "CCTV-17 农业农村",
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

# 黑名单正则规则：拦截组播假源、Flash、纯音频、一次性动态Token及海外不稳定代理源
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
    r'[?&](livekey|migutoken|auth_key|txSecret|wsSecret|token|sign)=|' # 拦截带时效签名易过期跳台的源
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

    if os.path.exists(CHANNELS_JSON_FILE):
        try:
            with open(CHANNELS_JSON_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict) and len(data) > 0:
                print(f"[*] 已从 {CHANNELS_JSON_FILE} 加载频道列表 ({len(data)} 个分类)", flush=True)
                return data
        except Exception:
            pass

    print("[*] 【运行模式】采用 config.py 官方定义 (CCTV 1~17 排除 5+，7优先+23拼音省级卫视)", flush=True)
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

    if any(bad in clean_name for bad in ("康巴", "兵团")):
        return ""

    if re.search(r'(?i)cctv[-_ ]?5\+', clean_name):
        return ""

    if any(k in clean_name for k in ("福建卫视", "福建台", "福建省台", "海峡卫视")):
        return "东南卫视"

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

    if custom_targets:
        name_stripped = re.sub(r'[\s\-_高清超清HD4K8K]', '', clean_name.upper())
        for target in custom_targets:
            t_norm = re.sub(r'[\s\-_高清超清HD4K8K]', '', target.upper())
            if t_norm and t_norm == name_stripped:
                return target

    return clean_name


def parse_ocr_text_to_channel(ocr_raw_text: str, custom_targets: Set[str] = None) -> Optional[str]:
    if not ocr_raw_text:
        return None

    compact = re.sub(r'[\s\r\n]+', ' ', ocr_raw_text).strip()
    upper_compact = re.sub(r'[\s\-_.:：·]+', '', compact.upper())

    if re.search(r'(?:CCTV|CCIV|CCT|GCTV|OCTV)0?5\+', upper_compact):
        return None

    m_cctv = re.search(r'(?:CCTV|CCIV|CCT|GCTV|OCTV|0CTV|中央)0?(\d{1,2})(?!\d)', upper_compact)
    if m_cctv:
        num = int(m_cctv.group(1))
        if 1 <= num <= 17:
            prefix = f"CCTV-{num} "
            for std_cctv in DEFAULT_CCTV_CHANNELS:
                if std_cctv.startswith(prefix):
                    if not custom_targets or std_cctv in custom_targets:
                        return std_cctv
            return f"CCTV-{num}"

    if any(tag in upper_compact for tag in ("CCTV", "CCIV", "GCTV", "中央")):
        for sub_kw, std_cctv in CCTV_SUBTITLE_MAP.items():
            if sub_kw in compact:
                if not custom_targets or std_cctv in custom_targets:
                    return std_cctv

    if any(k in compact for k in ("福建卫视", "东南卫视", "福建")):
        if "卫视" in compact or "东南" in compact:
            return "东南卫视"

    for sat in DEFAULT_SATELLITE_CHANNELS:
        prov = sat.replace("卫视", "")
        if sat in compact:
            return sat
        if prov in compact and ("卫视" in compact or "衛視" in compact):
            return sat

    if custom_targets:
        for t in custom_targets:
            t_clean = re.sub(r'[\s\-_高清超清HD4K8K]', '', t.upper())
            if len(t_clean) >= 3 and t_clean in upper_compact:
                return t

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

    cctv_pattern = re.search(
        r'([/=_.-])(cctv[-_]?0?(\d{1,2}))([a-zA-Z0-9_\-]*)(\.m3u8|\.flv|\.ts|/|$|\?)',
        clean_seed,
        re.IGNORECASE
    )
    if cctv_pattern:
        prefix = clean_seed[:cctv_pattern.start(2)]
        matched_token = cctv_pattern.group(2)
        num_part = cctv_pattern.group(3)
        extra_tag = cctv_pattern.group(4)
        suffix = clean_seed[cctv_pattern.end(4):]

        has_hyphen = "-" in matched_token
        has_underscore = "_" in matched_token
        has_zero = num_part.startswith("0") and len(num_part) == 2

        for std_ch in DEFAULT_CCTV_CHANNELS:
            num = int(std_ch.split()[0].replace("CCTV-", ""))
            num_str = f"0{num}" if has_zero and num < 10 else str(num)
            if has_hyphen:
                token = f"cctv-{num_str}"
            elif has_underscore:
                token = f"cctv_{num_str}"
            else:
                token = f"cctv{num_str}"
            if matched_token.isupper():
                token = token.upper()
            extrapolated_url = f"{prefix}{token}{extra_tag}{suffix}"
            results.append((std_ch, extrapolated_url))
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
                for std_ch in DEFAULT_CCTV_CHANNELS:
                    cctv_num = int(std_ch.split()[0].replace("CCTV-", ""))
                    formatted_num = f"{cctv_num:0{num_len}d}"
                    extrapolated_url = f"{prefix}{delimiter}{formatted_num}{sub_tag}{ext}{suffix}"
                    results.append((std_ch, extrapolated_url))
                return results

    return results
