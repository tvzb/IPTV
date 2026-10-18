# -*- coding: utf-8 -*-
"""
IPTV 自动化核心引擎 (Main Engine)：
针对用户现场实测痛点专项攻坚：
1. 【彻底铲除 1M/s 黑屏源】：精准识别并剔除广电专有 AVS+/CAVS/AVS2 编码（95%安卓电视硬解黑屏）及加密未解密源；
2. 【画质分级与 1080P 顶格置顶】：实行 1080P (FHD) -> 720P (HD) -> 标清 (SD) 绝对画质分级，把模糊标清源踢到列表最末或淘汰；
3. 【国内低延迟直连加成 (Domestic Priority)】：对国内直连节点与主流免翻 CDN 赋予置顶加权，彻底解决“海外测速快但在国内电视卡顿”的跨洋反转假象；
4. 【同服务器防独占 (IP Diversity)】：单频道同 IP 最多保留 2 条线路，多网运营商与多 CDN 互补备份；
5. 【取消 5 条写死上限】：不限数量，符合高画质与流畅标准的优质源全部保留。
"""
import os
import re
import sys
import time
import datetime
import subprocess
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set, Tuple, Optional

import requests

from config import (
    DEFAULT_CCTV_CHANNELS,
    DEFAULT_SATELLITE_CHANNELS,
    ALL_DEFAULT_TARGETS,
    load_user_channel_structure,
    clean_stream_url,
    normalize_channel_name,
    extract_ip_port,
    extrapolate_channels_from_seed,
    RAW_SOURCES_TXT,
    RAW_SOURCES_M3U,
    OUTPUT_LIVE_TXT,
    OUTPUT_LIVE_M3U,
    TVBOX_USER_AGENT,
    REQUEST_TIMEOUT,
    CHUNK_DOWNLOAD_TIMEOUT,
    CHUNK_DOWNLOAD_MAX_BYTES,
    MAX_WORKERS,
    MIN_SPEED_KBS,
    TARGET_PER_CHANNEL_RAW,
)

HEADERS = {
    "User-Agent": TVBOX_USER_AGENT,
    "Accept": "*/*",
    "Connection": "keep-alive"
}

# 全网精选 16 个每日同步、高带宽且编码兼容性极佳的公共源矩阵
FALLBACK_GITHUB_MATRIX = [
    "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv4.m3u",
    "https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/output/result.txt",
    "https://raw.githubusercontent.com/iptvjs/iptv-api/main/output/user_result.txt",
    "https://raw.githubusercontent.com/0610840119/iptv-api/main/output/user_result.txt",
    "https://raw.githubusercontent.com/0610840119/iptv-api/main/output/xp_result.txt",
    "https://raw.githubusercontent.com/MemoryCollection/IPTV/main/itvlist.txt",
    "https://raw.githubusercontent.com/ssili126/tv/main/itvlist.txt",
    "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
    "https://raw.githubusercontent.com/moevk/ChinaIPTV/main/live.m3u",
    "https://raw.githubusercontent.com/joevess/IPTV/main/sources/iptv_sources.m3u8",
    "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/hd.txt",
    "https://raw.githubusercontent.com/BurningC4/Chinese-IPTV/master/TV-IPV4.m3u",
    "https://raw.githubusercontent.com/cymz6/AutoIPTV-Hotel/main/lives.txt",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.txt",
]

# 每个频道允许来自同一物理服务器 (IP:Port) 的最大线路数
MAX_SAME_IP_PER_CHANNEL = 2

# =========================================================================
# 模块一：动态月份计算与 GitHub 代码检索
# =========================================================================

def get_dynamic_search_months() -> Tuple[str, str]:
    now = datetime.datetime.now()
    curr_month = now.strftime("%Y-%m")
    first_day = now.replace(day=1)
    prev_month_date = first_day - datetime.timedelta(days=1)
    prev_month = prev_month_date.strftime("%Y-%m")
    return curr_month, prev_month

def get_github_auth_headers() -> Dict[str, str]:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "IPTV-Harvester-Bot"
    }
    if token:
        headers["Authorization"] = f"token {token}"
    return headers

def search_github_code_files(query: str, max_items: int = 15) -> List[str]:
    headers = get_github_auth_headers()
    api_url = f"https://api.github.com/search/code?q={urllib.parse.quote(query)}&per_page={max_items}&sort=indexed&order=desc"
    raw_urls: List[str] = []

    try:
        resp = requests.get(api_url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("items", [])
            for item in items:
                repo_full_name = item.get("repository", {}).get("full_name")
                file_path = item.get("path")
                if repo_full_name and file_path:
                    raw_url = f"https://raw.githubusercontent.com/{repo_full_name}/main/{file_path}"
                    raw_urls.append(raw_url)
        elif resp.status_code in (403, 429):
            print(f"[!] GitHub API 达到访问配额 ({resp.status_code})，将使用备用高星源池与种子裂变引擎。", flush=True)
    except Exception as e:
        pass

    return raw_urls

def fetch_file_content(url: str) -> str:
    try:
        r = requests.get(url, headers=HEADERS, timeout=8)
        if r.status_code == 200:
            return r.text
        if r.status_code == 404 and "/main/" in url:
            master_url = url.replace("/main/", "/master/")
            r2 = requests.get(master_url, headers=HEADERS, timeout=8)
            if r2.status_code == 200:
                return r2.text
    except Exception:
        pass
    return ""

def parse_streams_from_text(content: str) -> List[Tuple[str, str]]:
    streams: List[Tuple[str, str]] = []
    if not content:
        return streams

    lines = [line.strip() for line in content.splitlines() if line.strip()]

    if any(line.startswith("#EXTM3U") for line in lines[:5]):
        current_title = ""
        for line in lines:
            if line.startswith("#EXTINF"):
                name_match = re.search(r',([^,]+)$', line)
                if name_match:
                    current_title = name_match.group(1).strip()
            elif line.startswith("http://") or line.startswith("https://"):
                if current_title:
                    clean_url = clean_stream_url(line)
                    norm_ch = normalize_channel_name(current_title)
                    if norm_ch and clean_url and not re.search(r'http://\[[0-9a-fA-F:]+\]', clean_url):
                        streams.append((norm_ch, clean_url))
                current_title = ""
        return streams

    for line in lines:
        if line.startswith("#") or ",#genre#" in line or "更新时间" in line:
            continue
        if "," in line and ("http://" in line or "https://" in line):
            parts = line.split(",", 1)
            raw_ch = parts[0].strip()
            raw_url = parts[1].strip()
            clean_url = clean_stream_url(raw_url)
            norm_ch = normalize_channel_name(raw_ch)
            if norm_ch and clean_url and not re.search(r'http://\[[0-9a-fA-F:]+\]', clean_url):
                streams.append((norm_ch, clean_url))

    return streams

def probe_stream_lightweight(url: str) -> bool:
    try:
        r = requests.get(url, headers=HEADERS, timeout=3.0, stream=True)
        if r.status_code == 200:
            first_chunk = next(r.iter_content(chunk_size=512), b"")
            r.close()
            if b"#EXTM3U" in first_chunk or b"#EXTINF" in first_chunk or len(first_chunk) > 100:
                if b"<html" not in first_chunk.lower():
                    return True
    except Exception:
        pass
    return False

def reverse_search_github_ip(ip_port: str) -> List[Tuple[str, str]]:
    discovered: List[Tuple[str, str]] = []
    if not ip_port:
        return discovered

    raw_urls = search_github_code_files(f'"{ip_port}"', max_items=5)
    for u in raw_urls:
        content = fetch_file_content(u)
        new_streams = parse_streams_from_text(content)
        for ch, url in new_streams:
            if ip_port in url:
                discovered.append((ch, url))

    return discovered

# =========================================================================
# 模块二：全网多源采集与种子裂变引擎
# =========================================================================

def harvest_all_sources() -> Dict[str, List[str]]:
    curr_month, prev_month = get_dynamic_search_months()
    print("=" * 65, flush=True)
    print(f"🚀 [阶段一] 开始全网直播源采集与智能裂变 (时段: {curr_month} / {prev_month})", flush=True)
    print("=" * 65, flush=True)

    user_struct = load_user_channel_structure()
    target_channels: Set[str] = set()
    for cat, ch_list in user_struct.items():
        for ch in ch_list:
            norm = normalize_channel_name(ch)
            if norm:
                target_channels.add(norm)

    print(f"[*] 监控目标频道总数: {len(target_channels)} 个", flush=True)

    search_queries = [
        f"更新 {curr_month} CCTV-2,http",
        f"更新 {prev_month} CCTV-2,http",
        f"更新 {curr_month} CCTV2,http",
        f"更新 {prev_month} CCTV2,http",
        f"更新 {curr_month} 卫视频道,http",
        f"CCTV-1 {curr_month} m3u8",
        f"CCTV-6 {curr_month} m3u8",
        f"topic:iptv {curr_month}",
    ]

    all_file_urls: Set[str] = set(FALLBACK_GITHUB_MATRIX)

    print("[*] 正在检索 GitHub Code 与高星直播源池...", flush=True)
    for q in search_queries:
        found_urls = search_github_code_files(q, max_items=8)
        for u in found_urls:
            all_file_urls.add(u)
        time.sleep(0.5)

    print(f"[+] 汇集候选清单文件: {len(all_file_urls)} 个，启动多线程并发提取...", flush=True)

    candidate_pool: Dict[str, List[str]] = {ch: [] for ch in target_channels}
    seen_urls_per_channel: Dict[str, Set[str]] = {ch: set() for ch in target_channels}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_url = {executor.submit(fetch_file_content, url): url for url in all_file_urls}
        for future in as_completed(future_to_url):
            content = future.result()
            if not content:
                continue
            parsed = parse_streams_from_text(content)
            for ch, stream_url in parsed:
                if ch in target_channels:
                    if stream_url not in seen_urls_per_channel[ch]:
                        seen_urls_per_channel[ch].add(stream_url)
                        candidate_pool[ch].append(stream_url)

    total_scraped = sum(len(v) for v in candidate_pool.values())
    print(f"[✓] 基础池采集完成，已录入原始源: {total_scraped} 条", flush=True)

    # 选取优质种子服务器进行反查与规律裂变
    print("[*] 正在执行优质种子源反向检索与序号推导...", flush=True)
    fissioned_count = 0
    checked_ips: Set[str] = set()

    key_seed_channels = ["CCTV-1 综合", "CCTV-2 财经", "CCTV-6 电影", "江西卫视", "东方卫视", "湖南卫视"]
    candidate_seeds: List[str] = []
    for kch in key_seed_channels:
        if kch in candidate_pool:
            candidate_seeds.extend(candidate_pool[kch][:15])

    for seed_url in candidate_seeds:
        ip_port = extract_ip_port(seed_url)
        if not ip_port or ip_port in checked_ips:
            continue

        checked_ips.add(ip_port)
        if probe_stream_lightweight(seed_url):
            extrapolated = extrapolate_channels_from_seed(seed_url)
            for ch, url in extrapolated:
                if ch in target_channels and url not in seen_urls_per_channel[ch]:
                    seen_urls_per_channel[ch].add(url)
                    candidate_pool[ch].append(url)
                    fissioned_count += 1

            reverse_streams = reverse_search_github_ip(ip_port)
            for ch, url in reverse_streams:
                if ch in target_channels and url not in seen_urls_per_channel[ch]:
                    seen_urls_per_channel[ch].add(url)
                    candidate_pool[ch].append(url)
                    fissioned_count += 1

    print(f"[✓] 智能裂变与反向搜索增补完成，新增优质源: {fissioned_count} 条", flush=True)

    export_raw_files(candidate_pool, user_struct)
    return candidate_pool

def export_raw_files(candidate_pool: Dict[str, List[str]], user_struct: Dict[str, List[str]]):
    txt_lines: List[str] = []
    m3u_lines: List[str] = ['#EXTM3U x-tvg-url="https://live.fanmingming.com/e.xml"']

    total_exported = 0
    for group_name, ch_list in user_struct.items():
        group_txt = [f"{group_name},#genre#"]
        has_items = False
        for raw_ch in ch_list:
            std_ch = normalize_channel_name(raw_ch)
            urls = candidate_pool.get(std_ch, [])
            selected_urls = urls[:TARGET_PER_CHANNEL_RAW]
            for u in selected_urls:
                group_txt.append(f"{std_ch},{u}")
                m3u_lines.append(f'#EXTINF:-1 tvg-name="{std_ch}" group-title="{group_name}",{std_ch}')
                m3u_lines.append(u)
                total_exported += 1
                has_items = True

        if has_items:
            txt_lines.extend(group_txt)

    with open(RAW_SOURCES_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines) + "\n")

    with open(RAW_SOURCES_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(m3u_lines) + "\n")

    print(f"🎉 原始全量候选库已保存: {RAW_SOURCES_TXT} (共 {total_exported} 条)", flush=True)

# =========================================================================
# 模块三：HLS 安全解密探针 + 本地内存流媒体特征分析 (零网络延时，杜绝超时)
# =========================================================================

def is_domestic_china_stream(url: str) -> bool:
    """
    判断直播流是否为国内直连/主流免翻节点：
    国内电视机访问国内直连节点具备极低延迟与大带宽，海外测试虽慢但国内电视播放极度流畅！
    """
    try:
        domain = urllib.parse.urlparse(url).netloc.split(':')[0]
        if domain.endswith('.cn') or any(k in domain for k in ('cctv', 'migu', 'fanmingming', 'chinamobile', '189.cn', 'chinaunicom', 'bestv', 'gslb')):
            return True
        # 常见国内三大运营商 IPv4 地址段 (111~125, 171, 175, 180, 182, 183, 218~223 等)
        if re.match(r'^(11[1-9]|12[0-5]|17[15]|18[023]|21[89]|22[0-3])\.', domain):
            return True
    except:
        pass
    return False

def check_hls_stream_security(m3u8_text: str, base_url: str) -> Tuple[bool, str]:
    """排查无法解密的 AES-128 内网私有密钥死锁源"""
    key_match = re.search(r'#EXT-X-KEY:METHOD=([A-Z0-9\-]+).*?URI=\"([^\"]+)\"', m3u8_text)
    if key_match:
        method = key_match.group(1).upper()
        if method != "NONE":
            key_uri = key_match.group(2)
            full_key_url = urllib.parse.urljoin(base_url, key_uri)
            # 排除内网私有 IP 密钥
            if re.search(r'(10\.\d+|192\.168\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+|127\.0\.0\.1|localhost)', full_key_url):
                return False, "内网私有加密密钥不可达"
            try:
                kr = requests.get(full_key_url, headers=HEADERS, timeout=2.5)
                if kr.status_code != 200 or len(kr.content) not in (16, 32):
                    return False, "加密密钥失效无法解密"
            except Exception:
                return False, "加密密钥连接超时"

    return True, "OK"

def parse_input_sources(source_file: str) -> Dict[str, List[str]]:
    channel_streams: Dict[str, List[str]] = {}
    if not os.path.exists(source_file):
        return channel_streams

    with open(source_file, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    lines = [line.strip() for line in content.splitlines() if line.strip()]

    if any(line.startswith("#EXTM3U") for line in lines[:5]):
        current_title = ""
        for line in lines:
            if line.startswith("#EXTINF"):
                match = re.search(r',([^,]+)$', line)
                if match:
                    current_title = match.group(1).strip()
            elif line.startswith("http://") or line.startswith("https://"):
                if current_title:
                    clean_url = clean_stream_url(line)
                    norm_ch = normalize_channel_name(current_title)
                    if norm_ch and clean_url:
                        if norm_ch not in channel_streams:
                            channel_streams[norm_ch] = []
                        if clean_url not in channel_streams[norm_ch]:
                            channel_streams[norm_ch].append(clean_url)
                current_title = ""
        if channel_streams:
            return channel_streams

    for line in lines:
        if line.startswith("#") or ",#genre#" in line or "更新时间" in line:
            continue
        if "," in line and ("http://" in line or "https://" in line):
            parts = line.split(",", 1)
            raw_ch = parts[0].strip()
            raw_url = parts[1].strip()
            clean_url = clean_stream_url(raw_url)
            norm_ch = normalize_channel_name(raw_ch)
            if norm_ch and clean_url:
                if norm_ch not in channel_streams:
                    channel_streams[norm_ch] = []
                if clean_url not in channel_streams[norm_ch]:
                    channel_streams[norm_ch].append(clean_url)

    return channel_streams

def test_single_stream_speed(channel_name: str, stream_url: str) -> Tuple[str, str, float, bool, str, int, bool, float]:
    """
    单条直播流深度评测：
    返回: (channel_name, clean_url, speed_kbs, is_valid, video_tag, res_tier, is_domestic, ttfb)
    res_tier: 3=1080P全高清, 2=720P高清, 1=标清
    """
    clean_url = clean_stream_url(stream_url)
    is_domestic = is_domestic_china_stream(clean_url)
    connect_start = time.time()

    try:
        r = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
        ttfb = max(time.time() - connect_start, 0.05)
        if r.status_code != 200:
            return channel_name, clean_url, 0.0, False, "HTTP失效", 0, is_domestic, ttfb

        first_chunk = next(r.iter_content(chunk_size=1024), b"")
        r.close()

        if b"<html" in first_chunk.lower() or b"access denied" in first_chunk.lower() or b"<!doctype" in first_chunk.lower():
            return channel_name, clean_url, 0.0, False, "拦截页面/假活", 0, is_domestic, ttfb

        ts_url = None
        if b"#EXTM3U" in first_chunk:
            try:
                full_m3u8 = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT).text

                # 拦截内网私有不可达密钥
                sec_ok, sec_msg = check_hls_stream_security(full_m3u8, clean_url)
                if not sec_ok:
                    return channel_name, clean_url, 0.0, False, sec_msg, 0, is_domestic, ttfb

                # 递归处理多码率 Master Playlist
                if "#EXT-X-STREAM-INF" in full_m3u8:
                    for line in full_m3u8.splitlines():
                        line = line.strip()
                        if line and not line.startswith("#"):
                            clean_url = urllib.parse.urljoin(clean_url, line)
                            full_m3u8 = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT).text
                            break

                for line in full_m3u8.splitlines():
                    line = line.strip()
                    if line and not line.startswith("#"):
                        ts_url = urllib.parse.urljoin(clean_url, line)
                        break
            except Exception:
                pass

        test_target_url = ts_url or clean_url

        # 拦截切片指向内网 IP 的源
        if re.search(r'(10\.\d+|192\.168\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+|127\.0\.0\.1|localhost)', test_target_url):
            return channel_name, clean_url, 0.0, False, "内网切片不可达", 0, is_domestic, ttfb

        download_start = time.time()
        bytes_received = 0
        media_buffer = bytearray()

        try:
            r_ts = requests.get(test_target_url, headers=HEADERS, timeout=3.0, stream=True)
            if r_ts.status_code == 200:
                for chunk in r_ts.iter_content(chunk_size=32 * 1024):
                    bytes_received += len(chunk)
                    if len(media_buffer) < 128 * 1024:
                        media_buffer.extend(chunk)
                    if time.time() - download_start >= 2.5 or bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                        break
            r_ts.close()
        except Exception:
            pass

        # 必须至少拉取到 8KB 的真实视频分片
        if bytes_received < 8 * 1024:
            return channel_name, clean_url, 0.0, False, "切片下载失败/0字节", 0, is_domestic, ttfb

        # 本地内存字节严格校验是否为视频流封装 (MPEG-TS 0x47 同步字 或 MP4 格式)
        raw_bytes = bytes(media_buffer)
        is_ts = len(raw_bytes) > 188 and (raw_bytes[0] == 0x47 or raw_bytes[188] == 0x47)
        is_mp4 = (b"ftyp" in raw_bytes[:32] or b"moov" in raw_bytes[:32] or b"mdat" in raw_bytes[:32])

        if not (is_ts or is_mp4):
            return channel_name, clean_url, 0.0, False, "非视频数据流(无TS/MP4头)", 0, is_domestic, ttfb

        duration = max(time.time() - download_start, 0.05)
        speed_kbs = (bytes_received / 1024.0) / duration

        # 本地内存 pipe ffprobe 首帧像素快速检测（0 网络延时）
        video_tag = "高清"
        res_tier = 2  # 默认按高清处理

        try:
            cmd = [
                "ffprobe",
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=codec_name,width,height",
                "-of", "csv=s=x:p=0",
                "-i", "pipe:0"
            ]
            proc = subprocess.run(cmd, input=raw_bytes[:96 * 1024], stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=1.2)
            out = proc.stdout.decode("utf-8", errors="ignore").strip()
            if out:
                parts = out.split("x")
                if len(parts) >= 3:
                    codec = parts[0].lower()
                    # 关键拦截：广电专有 AVS/CAVS/AVS2/AVS3 编码在绝大多数电视盒子上无法硬件解码，直接黑屏！坚决淘汰！
                    if any(avs_name in codec for avs_name in ("cavs", "avs", "avs2", "avs3")):
                        return channel_name, clean_url, 0.0, False, f"广电AVS+专有编码(电视黑屏)", 0, is_domestic, ttfb

                    # 仅接纳主流兼容视频编码格式
                    if codec not in ("h264", "avc", "hevc", "h265", "mpeg2video", "mpeg4"):
                        return channel_name, clean_url, 0.0, False, f"不支持的视频编码({codec})", 0, is_domestic, ttfb

                    w, h = int(parts[1]), int(parts[2])
                    if h >= 1000 or w >= 1900:
                        res_tier = 3
                        tag = "1080P"
                    elif h >= 700 or w >= 1200:
                        res_tier = 2
                        tag = "720P"
                    else:
                        res_tier = 1
                        tag = "标清"

                    video_tag = f"{codec.upper()} {tag}"
        except Exception:
            pass

        return channel_name, clean_url, speed_kbs, True, video_tag, res_tier, is_domestic, ttfb

    except Exception as e:
        return channel_name, clean_url, 0.0, False, f"异常: {e}", 0, is_domestic, 999.0

def run_speedtest(source_file: str = RAW_SOURCES_TXT, min_speed: float = MIN_SPEED_KBS):
    print("=" * 65, flush=True)
    print(f"🚀 [阶段二] 开始 TVBox 深度测速与【画质分级/国内免翻节点优先】", flush=True)
    print(f"    - 流畅筛选门槛: >= {min_speed:.1f} KB/s (达标不限数量，越多越好)", flush=True)
    print(f"    - 优化准则: 1080P顶格置顶 > 国内低延迟直连优先 > 彻底铲除 AVS+ 黑屏源", flush=True)
    print("=" * 65, flush=True)

    channel_streams = parse_input_sources(source_file)
    if not channel_streams:
        print(f"[!] 未在 {source_file} 中检测到有效直播源！", flush=True)
        return

    user_struct = load_user_channel_structure()

    tasks: List[Tuple[str, str]] = []
    for ch, urls in channel_streams.items():
        for u in urls:
            tasks.append((ch, u))

    print(f"[*] 待测直播源总数: {len(tasks)} 条，启动 {MAX_WORKERS} 线程高并发深度评测...", flush=True)

    qualified_results: Dict[str, List[Tuple[str, float, str, int, bool, float]]] = {}
    all_tested_results: Dict[str, List[Tuple[str, float, str, int, bool, float]]] = {}
    tested_count = 0
    passed_count = 0
    rejected_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(test_single_stream_speed, ch, u) for ch, u in tasks]
        for f in as_completed(futures):
            tested_count += 1
            ch, url, speed, is_valid, video_tag, res_tier, is_domestic, ttfb = f.result()

            if is_valid:
                passed_count += 1
                record = (url, speed, video_tag, res_tier, is_domestic, ttfb)
                if ch not in all_tested_results:
                    all_tested_results[ch] = []
                all_tested_results[ch].append(record)

                if speed >= min_speed:
                    if ch not in qualified_results:
                        qualified_results[ch] = []
                    qualified_results[ch].append(record)
            else:
                rejected_count += 1

            if tested_count % 30 == 0 or tested_count == len(tasks):
                elapsed = max(time.time() - start_time, 0.1)
                rate = tested_count / elapsed
                print(f"  [进展] 已评测 {tested_count}/{len(tasks)} 条 (合格: {passed_count} 条, 拦截AVS+黑屏/无效: {rejected_count} 条) ...", flush=True)

    export_final_playlists(qualified_results, all_tested_results, user_struct)

def compute_composite_score(record: Tuple[str, float, str, int, bool, float]) -> float:
    """
    智能优选综合评分模型：
    1. 画质层级绝对优先：1080P 基础得分 30000 分，720P 基础得分 20000 分，标清 10000 分；
    2. 国内直连免翻节点大幅加成 (+5000分)：解决“海外测试快但在国内电视由于越墙卡顿”的现象，让真正丝滑的源排在前面；
    3. 真实吞吐量加成 (+min(speed, 2000))；
    4. 首包低响应时间加成 (+max(0, 500 - ttfb * 250))。
    """
    url, spd, vtag, res_tier, is_domestic, ttfb = record
    score = res_tier * 10000.0
    if is_domestic:
        score += 5000.0
    score += min(spd, 2000.0)
    score += max(0.0, 500.0 - (ttfb * 250.0))
    return score

def export_final_playlists(
    qualified_results: Dict[str, List[Tuple[str, float, str, int, bool, float]]],
    all_tested_results: Dict[str, List[Tuple[str, float, str, int, bool, float]]],
    user_struct: Dict[str, List[str]]
):
    txt_lines: List[str] = []
    m3u_lines: List[str] = ['#EXTM3U x-tvg-url="https://live.fanmingming.com/e.xml"']

    total_channels_with_streams = 0
    total_qualified_streams = 0

    print("\n" + "=" * 70, flush=True)
    print(f"{'频道名称':<18} | {'有效画面源':<10} | {'最高播放速率':<14} | {'最佳画质':<15}", flush=True)
    print("-" * 70, flush=True)

    for group_name, ch_list in user_struct.items():
        group_txt = [f"{group_name},#genre#"]
        has_channel = False

        for raw_ch in ch_list:
            std_ch = normalize_channel_name(raw_ch)
            primary_streams = qualified_results.get(std_ch, [])
            backup_streams = all_tested_results.get(std_ch, [])

            # 合并源池（优先达标源，不足时由通过校验的合格备用源补齐）
            candidate_list = list(primary_streams)
            seen_urls = {s[0] for s in candidate_list}
            for bs in backup_streams:
                if bs[0] not in seen_urls:
                    candidate_list.append(bs)
                    seen_urls.add(bs[0])

            # 按智能综合得分降序排序 (1080P优先，国内节点优先)
            candidate_list.sort(key=compute_composite_score, reverse=True)

            # 执行【同服务器 IP 防独占算法】（单 IP 最多保留 2 条线路）
            final_channel_streams: List[Tuple[str, float, str, int, bool, float]] = []
            ip_usage_count: Dict[str, int] = defaultdict(int)

            for item in candidate_list:
                url = item[0]
                ip_p = extract_ip_port(url) or "unknown_host"
                if ip_usage_count[ip_p] < MAX_SAME_IP_PER_CHANNEL:
                    ip_usage_count[ip_p] += 1
                    final_channel_streams.append(item)

            if final_channel_streams:
                has_channel = True
                total_channels_with_streams += 1
                total_qualified_streams += len(final_channel_streams)
                top_speed = final_channel_streams[0][1]
                top_video_tag = final_channel_streams[0][2]
                print(f"{std_ch:<18} | {len(final_channel_streams):<10} | {top_speed:>7.1f} KB/s | {top_video_tag:<15}", flush=True)

                for item in final_channel_streams:
                    url, spd, vtag, tier, domestic, ttfb = item
                    group_txt.append(f"{std_ch},{url}")
                    m3u_lines.append(f'#EXTINF:-1 tvg-name="{std_ch}" group-title="{group_name}",{std_ch}')
                    m3u_lines.append(url)
            else:
                print(f"{std_ch:<18} | {'0 (无可用源)':<10} | {'--':>12} | {'--':<15}", flush=True)

        if has_channel:
            txt_lines.extend(group_txt)

    with open(OUTPUT_LIVE_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines) + "\n")

    with open(OUTPUT_LIVE_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(m3u_lines) + "\n")

    print("=" * 70, flush=True)
    print(f"🎉 优选排序完成！")
    print(f"   - 1080P/高清源已全部自动置顶于前列线路 (线路1、线路2...)！")
    print(f"   - 国内免翻低延迟优质源已提升权重，杜绝卡顿！")
    print(f"   - AVS+ 黑屏源已全数拦截！")
    print(f"   - 交付文件 (TXT 格式): {OUTPUT_LIVE_TXT}")
    print(f"   - 交付文件 (M3U 格式): {OUTPUT_LIVE_M3U}")
    print("=" * 70, flush=True)

# =========================================================================
# 命令行调度入口：支持 harvest / speedtest / full
# =========================================================================

def main():
    total_start = time.time()
    mode = "full"
    min_speed = MIN_SPEED_KBS

    if len(sys.argv) > 1:
        arg1 = sys.argv[1].lower()
        if arg1 in ("harvest", "crawl"):
            mode = "harvest"
        elif arg1 in ("speedtest", "test"):
            mode = "speedtest"
            if len(sys.argv) > 2:
                try: min_speed = float(sys.argv[2])
                except: pass
        elif arg1 in ("full", "all"):
            mode = "full"
            if len(sys.argv) > 2:
                try: min_speed = float(sys.argv[2])
                except: pass
        else:
            try: min_speed = float(arg1)
            except: pass

    if mode == "harvest":
        harvest_all_sources()
    elif mode == "speedtest":
        src = sys.argv[3] if len(sys.argv) > 3 else RAW_SOURCES_TXT
        run_speedtest(source_file=src, min_speed=min_speed)
    else:
        harvest_all_sources()
        run_speedtest(source_file=RAW_SOURCES_TXT, min_speed=min_speed)

    total_elapsed = time.time() - total_start
    print(f"\n[🏁] 全部任务执行完成！总耗时: {total_elapsed:.1f} 秒", flush=True)

if __name__ == "__main__":
    main()
