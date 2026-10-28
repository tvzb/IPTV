# -*- coding: utf-8 -*-
"""
IPTV 自动化核心引擎（真实画面优先 + 高覆盖率优化版）

核心目标：
1. 测速结果尽可能接近国内真实网络 + TVBox 实际播放体验
2. 显著扩大候选源数量（更多搜索页、更多查询、更多裂变）
3. 最终列表按「清晰度（分辨率高度）优先，持续速度次之」严格排序

解决痛点：
- 源太少甚至为空
- 很多源“能下一点数据但实际卡顿/黑屏/假活”
- 同 IP 独占导致整组失效
- 跨洋测速与国内真实速度差异大
- 广播源混入
- 只有开头快、后续断流的伪优质源
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

from grok_config import (
    DEFAULT_CCTV_CHANNELS,
    DEFAULT_SATELLITE_CHANNELS,
    ALL_DEFAULT_TARGETS,
    load_user_channel_structure,
    clean_stream_url,
    normalize_channel_name,
    extract_ip_port,
    extrapolate_channels_from_seed,
    is_radio_source,
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
    MAX_SAME_IP_PER_CHANNEL,
    MIN_VIDEO_HEIGHT,
    SUSTAINED_SPEED_RATIO,
    GITHUB_SEARCH_MAX_ITEMS,
    GITHUB_SEARCH_PAGES,
    MAX_REVERSE_IPS,
    KEY_SEED_LIMIT,
)

HEADERS = {
    "User-Agent": TVBOX_USER_AGENT,
    "Accept": "*/*",
    "Connection": "keep-alive",
}

# 全网精选高星、每日同步、编码兼容性好的公共源矩阵（扩大）
FALLBACK_GITHUB_MATRIX = [
    "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv4.m3u",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.txt",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/ipv4/result.txt",
    "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/hd.txt",
    "https://raw.githubusercontent.com/ssili126/tv/main/itvlist.txt",
    "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
    "https://raw.githubusercontent.com/moevk/ChinaIPTV/main/live.m3u",
    "https://raw.githubusercontent.com/joevess/IPTV/main/sources/iptv_sources.m3u8",
    "https://raw.githubusercontent.com/BurningC4/Chinese-IPTV/master/TV-IPV4.m3u",
    "https://raw.githubusercontent.com/cymz6/AutoIPTV-Hotel/main/lives.txt",
    "https://raw.githubusercontent.com/MemoryCollection/IPTV/main/itvlist.txt",
    "https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/output/result.txt",
    "https://raw.githubusercontent.com/iptvjs/iptv-api/main/output/user_result.txt",
    "https://raw.githubusercontent.com/best-fan/iptv-sources/main/cn_cctv.m3u8",
    "https://raw.githubusercontent.com/best-fan/iptv-sources/main/cn_province.m3u8",
    "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
    "https://raw.githubusercontent.com/suxuang/myIPTV/main/ipv4.m3u",
    "https://raw.githubusercontent.com/qist/tvbox/master/tv.txt",
    "https://raw.githubusercontent.com/wwb521/live/main/tv.txt",
    "https://raw.githubusercontent.com/Kimentanm/aptv/master/m3u/iptv.m3u",
    "https://raw.githubusercontent.com/Meroser/IPTV/main/tv.m3u",
    "https://raw.githubusercontent.com/kimwang1978/collect-tv-txt/main/merged_output.txt",
    "https://raw.githubusercontent.com/pxiptv/live/main/iptv.txt",
    "https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/yylive.m3u",
]


# =========================================================================
# 模块一：动态月份计算与 GitHub 代码检索（限流保护 + 扩大页数）
# =========================================================================

_GITHUB_SEARCH_CIRCUIT_OPEN = False


def get_dynamic_search_months() -> Tuple[str, str, str]:
    now = datetime.datetime.now()
    curr_month = now.strftime("%Y-%m")
    first_day = now.replace(day=1)
    prev_month_date = first_day - datetime.timedelta(days=1)
    prev_month = prev_month_date.strftime("%Y-%m")
    # 再往前一个月，覆盖更多历史更新源
    first_day_prev = prev_month_date.replace(day=1)
    prev2_month_date = first_day_prev - datetime.timedelta(days=1)
    prev2_month = prev2_month_date.strftime("%Y-%m")
    return curr_month, prev_month, prev2_month


def get_github_auth_headers() -> Dict[str, str]:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "IPTV-Harvester-Bot/2.0",
    }
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


def search_github_code_files(query: str, max_items: int = None, pages: int = None) -> List[str]:
    """搜索 GitHub Code，支持多页，带熔断与退避，防止 403 爆炸。"""
    global _GITHUB_SEARCH_CIRCUIT_OPEN
    if _GITHUB_SEARCH_CIRCUIT_OPEN:
        return []

    max_items = max_items or GITHUB_SEARCH_MAX_ITEMS
    pages = pages or GITHUB_SEARCH_PAGES

    headers = get_github_auth_headers()
    raw_urls: List[str] = []
    per_page = min(max_items, 30)

    for page in range(1, pages + 1):
        api_url = (
            f"https://api.github.com/search/code?"
            f"q={urllib.parse.quote(query)}&per_page={per_page}&page={page}&sort=indexed&order=desc"
        )
        try:
            resp = requests.get(api_url, headers=headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                items = data.get("items", [])
                for item in items:
                    repo_full_name = item.get("repository", {}).get("full_name")
                    file_path = item.get("path")
                    if repo_full_name and file_path:
                        # 优先 main，后续 fetch 时会自动 fallback master
                        raw_url = f"https://raw.githubusercontent.com/{repo_full_name}/main/{file_path}"
                        raw_urls.append(raw_url)
            elif resp.status_code in (403, 429):
                print(f"[!] GitHub API 限流 ({resp.status_code})，等待 50 秒后重试一次…", flush=True)
                time.sleep(50)
                resp2 = requests.get(api_url, headers=headers, timeout=15)
                if resp2.status_code in (403, 429):
                    print(f"[!] GitHub API 仍被限流，开启熔断，后续搜索全部跳过，改用备用源池。", flush=True)
                    _GITHUB_SEARCH_CIRCUIT_OPEN = True
                    break
                else:
                    data = resp2.json()
                    items = data.get("items", [])
                    for item in items:
                        repo_full_name = item.get("repository", {}).get("full_name")
                        file_path = item.get("path")
                        if repo_full_name and file_path:
                            raw_url = f"https://raw.githubusercontent.com/{repo_full_name}/main/{file_path}"
                            raw_urls.append(raw_url)
            else:
                break
        except Exception as e:
            print(f"[!] 搜索异常: {str(e)[:60]}", flush=True)
            break
        time.sleep(1.5)

    return list(dict.fromkeys(raw_urls))


def fetch_file_content(url: str) -> str:
    try:
        r = requests.get(url, headers=HEADERS, timeout=12)
        if r.status_code == 200:
            return r.text
        if r.status_code == 404 and "/main/" in url:
            master_url = url.replace("/main/", "/master/")
            r2 = requests.get(master_url, headers=HEADERS, timeout=12)
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

    # M3U 格式
    if any(line.startswith("#EXTM3U") for line in lines[:5]):
        current_title = ""
        for line in lines:
            if line.startswith("#EXTINF"):
                name_match = re.search(r',([^,]+)$', line)
                if name_match:
                    current_title = name_match.group(1).strip()
            elif line.startswith("http://") or line.startswith("https://") or line.startswith("rtmp://"):
                if current_title:
                    clean_url = clean_stream_url(line)
                    norm_ch = normalize_channel_name(current_title)
                    if norm_ch and clean_url and not is_radio_source(norm_ch, clean_url):
                        if not re.search(r'http://\[[0-9a-fA-F:]+\]', clean_url):
                            streams.append((norm_ch, clean_url))
                current_title = ""
        return streams

    # TXT / 逗号分隔格式
    for line in lines:
        if line.startswith("#") or ",#genre#" in line or "更新时间" in line:
            continue
        if "," in line and ("http://" in line or "https://" in line or "rtmp://" in line):
            parts = line.split(",", 1)
            raw_ch = parts[0].strip()
            raw_url = parts[1].strip()
            clean_url = clean_stream_url(raw_url)
            norm_ch = normalize_channel_name(raw_ch)
            if norm_ch and clean_url and not is_radio_source(norm_ch, clean_url):
                if not re.search(r'http://\[[0-9a-fA-F:]+\]', clean_url):
                    streams.append((norm_ch, clean_url))

    return streams


def probe_stream_lightweight(url: str) -> bool:
    try:
        r = requests.get(url, headers=HEADERS, timeout=4.0, stream=True)
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
    global _GITHUB_SEARCH_CIRCUIT_OPEN
    if _GITHUB_SEARCH_CIRCUIT_OPEN or not ip_port:
        return []

    discovered: List[Tuple[str, str]] = []
    raw_urls = search_github_code_files(f'"{ip_port}"', max_items=10, pages=1)
    for u in raw_urls:
        content = fetch_file_content(u)
        new_streams = parse_streams_from_text(content)
        for ch, url in new_streams:
            if ip_port in url:
                discovered.append((ch, url))
    return discovered


# =========================================================================
# 模块二：全网多源采集与种子裂变引擎（扩大搜索与裂变）
# =========================================================================

def harvest_all_sources() -> Dict[str, List[str]]:
    global _GITHUB_SEARCH_CIRCUIT_OPEN
    _GITHUB_SEARCH_CIRCUIT_OPEN = False

    curr_month, prev_month, prev2_month = get_dynamic_search_months()
    print("=" * 70, flush=True)
    print(f"🚀 [阶段一] 全网直播源采集与智能裂变 (时段: {curr_month} / {prev_month} / {prev2_month})", flush=True)
    print("=" * 70, flush=True)

    user_struct = load_user_channel_structure()
    target_channels: Set[str] = set()
    for cat, ch_list in user_struct.items():
        for ch in ch_list:
            norm = normalize_channel_name(ch)
            if norm:
                target_channels.add(norm)

    print(f"[*] 监控目标频道总数: {len(target_channels)} 个", flush=True)

    # 扩大搜索查询矩阵（覆盖更多月份、更多关键词、更多格式）
    search_queries = [
        f"更新 {curr_month} CCTV-2,http",
        f"更新 {prev_month} CCTV-2,http",
        f"更新 {prev2_month} CCTV-2,http",
        f"更新 {curr_month} CCTV-6,http",
        f"更新 {curr_month} 卫视频道,http",
        f"更新 {prev_month} 卫视频道,http",
        f"CCTV-1 {curr_month} m3u8",
        f"CCTV-6 {curr_month} m3u8",
        f"CCTV-1 {prev_month} m3u8",
        f"topic:iptv {curr_month}",
        f"topic:iptv {prev_month}",
        f"user_result.txt {curr_month}",
        f"live.txt {curr_month}",
        f"itvlist.txt {curr_month}",
        f"CCTV-13 新闻 {curr_month}",
        f"湖南卫视 {curr_month} m3u8",
        f"东方卫视 {curr_month}",
        f"ipv4.m3u {curr_month}",
        f"result.txt CCTV",
        f"extension:m3u8 CCTV-1",
        f"extension:txt 更新时间 {curr_month}",
    ]

    all_file_urls: Set[str] = set(FALLBACK_GITHUB_MATRIX)

    print(f"[*] 正在检索 GitHub Code（每查询最多 {GITHUB_SEARCH_PAGES} 页 × {GITHUB_SEARCH_MAX_ITEMS} 条）与高星直播源池...", flush=True)
    for q in search_queries:
        if _GITHUB_SEARCH_CIRCUIT_OPEN:
            print("[!] 已触发熔断，跳过剩余搜索查询。", flush=True)
            break
        found_urls = search_github_code_files(q, max_items=GITHUB_SEARCH_MAX_ITEMS, pages=GITHUB_SEARCH_PAGES)
        for u in found_urls:
            all_file_urls.add(u)
        time.sleep(1.2)

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

    print("[*] 正在执行优质种子源反向检索与序号推导（裂变，限流保护）...", flush=True)
    fissioned_count = 0
    checked_ips: Set[str] = set()

    key_seed_channels = [
        "CCTV-1 综合", "CCTV-2 财经", "CCTV-5 体育", "CCTV-6 电影", "CCTV-13 新闻",
        "江西卫视", "东方卫视", "湖南卫视", "浙江卫视", "江苏卫视", "北京卫视"
    ]
    candidate_seeds: List[str] = []
    for kch in key_seed_channels:
        if kch in candidate_pool:
            candidate_seeds.extend(candidate_pool[kch][:KEY_SEED_LIMIT])

    reverse_done = 0

    for seed_url in candidate_seeds:
        if reverse_done >= MAX_REVERSE_IPS or _GITHUB_SEARCH_CIRCUIT_OPEN:
            break

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
            reverse_done += 1
            for ch, url in reverse_streams:
                if ch in target_channels and url not in seen_urls_per_channel[ch]:
                    seen_urls_per_channel[ch].add(url)
                    candidate_pool[ch].append(url)
                    fissioned_count += 1

            time.sleep(1.8)

    print(f"[✓] 智能裂变与反向搜索增补完成，新增优质源: {fissioned_count} 条（反向检索 IP 数: {reverse_done}）", flush=True)

    # 对每个频道去重后截断到 TARGET_PER_CHANNEL_RAW
    for ch in candidate_pool:
        # 保持顺序去重
        seen = set()
        unique = []
        for u in candidate_pool[ch]:
            if u not in seen:
                seen.add(u)
                unique.append(u)
        candidate_pool[ch] = unique[:TARGET_PER_CHANNEL_RAW]

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
# 模块三：真实画面优先的深度测速探针（接近 TVBox 实际观看）
# =========================================================================

def check_hls_stream_security(m3u8_text: str, base_url: str) -> Tuple[bool, str]:
    """排查无法解密的 AES-128 内网密钥死锁源"""
    key_match = re.search(r'#EXT-X-KEY:METHOD=([A-Z0-9\-]+).*?URI=\"([^\"]+)\"', m3u8_text)
    if key_match:
        method = key_match.group(1).upper()
        if method != "NONE":
            key_uri = key_match.group(2)
            full_key_url = urllib.parse.urljoin(base_url, key_uri)
            if re.search(r'(10\.\d+|192\.168\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+|127\.0\.0\.1|localhost)', full_key_url):
                return False, "内网私有加密密钥不可达"
            try:
                kr = requests.get(full_key_url, headers=HEADERS, timeout=3.0)
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
                    if norm_ch and clean_url and not is_radio_source(norm_ch, clean_url):
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
            if norm_ch and clean_url and not is_radio_source(norm_ch, clean_url):
                if norm_ch not in channel_streams:
                    channel_streams[norm_ch] = []
                if clean_url not in channel_streams[norm_ch]:
                    channel_streams[norm_ch].append(clean_url)

    return channel_streams


def _ffprobe_video_info(raw_bytes: bytes) -> Tuple[str, int, int]:
    """
    返回 (codec, width, height)。失败返回 ("", 0, 0)
    强制要求有视频轨，拒绝纯音频或空流。
    """
    try:
        cmd = [
            "ffprobe",
            "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=codec_name,width,height",
            "-of", "csv=s=x:p=0",
            "-i", "pipe:0"
        ]
        proc = subprocess.run(
            cmd, input=raw_bytes[:160 * 1024],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=2.5
        )
        out = proc.stdout.decode("utf-8", errors="ignore").strip()
        if out:
            parts = out.split("x")
            if len(parts) >= 3:
                codec = parts[0].strip()
                w = int(parts[1]) if parts[1].isdigit() else 0
                h = int(parts[2]) if parts[2].isdigit() else 0
                if w >= 320 and h >= 240 and codec:
                    return codec.upper(), w, h
    except Exception:
        pass
    return "", 0, 0


def test_single_stream_speed(channel_name: str, stream_url: str) -> Tuple[str, str, float, bool, str, int]:
    """
    真实画面优先的深度探针。
    返回: channel, url, speed_kbs, is_valid, video_tag, height

    核心改进：
    1. 持续下载更长时间/更大体积，计算平均速度
    2. HLS 尽量连续取 2~3 个 TS 片段验证连续性
    3. ffprobe 强制确认有视频轨 + 有效分辨率
    4. 拒绝开头快、后面断流或无真实画面的源
    """
    clean_url = clean_stream_url(stream_url)

    try:
        # ---------- 第一步：快速头检查 ----------
        r = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
        if r.status_code != 200:
            return channel_name, clean_url, 0.0, False, "HTTP失效", 0

        first_chunk = next(r.iter_content(chunk_size=2048), b"")
        r.close()

        if b"<html" in first_chunk.lower() or b"access denied" in first_chunk.lower() or b"<!doctype" in first_chunk.lower():
            return channel_name, clean_url, 0.0, False, "拦截页面/假活", 0

        # ---------- 第二步：处理 HLS Master / Media Playlist ----------
        ts_urls: List[str] = []
        if b"#EXTM3U" in first_chunk:
            try:
                full_m3u8 = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT).text

                sec_ok, sec_msg = check_hls_stream_security(full_m3u8, clean_url)
                if not sec_ok:
                    return channel_name, clean_url, 0.0, False, sec_msg, 0

                # 多码率 Master Playlist → 选最高分辨率/带宽
                if "#EXT-X-STREAM-INF" in full_m3u8:
                    candidates = []
                    lines = full_m3u8.splitlines()
                    for i, line in enumerate(lines):
                        line = line.strip()
                        if line.startswith("#EXT-X-STREAM-INF"):
                            bw_match = re.search(r'BANDWIDTH=(\d+)', line)
                            res_match = re.search(r'RESOLUTION=(\d+)x(\d+)', line)
                            bw = int(bw_match.group(1)) if bw_match else 0
                            h = int(res_match.group(2)) if res_match else 0
                            if i + 1 < len(lines):
                                next_line = lines[i + 1].strip()
                                if next_line and not next_line.startswith("#"):
                                    candidates.append((bw, h, next_line))
                    if candidates:
                        candidates.sort(key=lambda x: (x[1], x[0]), reverse=True)
                        clean_url = urllib.parse.urljoin(clean_url, candidates[0][2])
                        full_m3u8 = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT).text

                # 收集连续多个 TS 片段（验证连续性）
                for line in full_m3u8.splitlines():
                    line = line.strip()
                    if line and not line.startswith("#"):
                        full_ts = urllib.parse.urljoin(clean_url, line)
                        if not re.search(r'(10\.\d+|192\.168\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+|127\.0\.0\.1|localhost)', full_ts):
                            ts_urls.append(full_ts)
                        if len(ts_urls) >= 3:  # 最多取 3 个连续片段
                            break
            except Exception:
                pass

        # 如果不是 HLS 或没解析出 TS，就直接测原始 URL
        if not ts_urls:
            if re.search(r'(10\.\d+|192\.168\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+|127\.0\.0\.1|localhost)', clean_url):
                return channel_name, clean_url, 0.0, False, "内网地址不可达", 0
            ts_urls = [clean_url]

        # ---------- 第三步：持续下载探针（真实画面核心） ----------
        download_start = time.time()
        bytes_received = 0
        media_buffer = bytearray()
        successful_segments = 0

        for idx, target_url in enumerate(ts_urls):
            try:
                r_ts = requests.get(target_url, headers=HEADERS, timeout=5.0, stream=True)
                if r_ts.status_code != 200:
                    continue

                seg_bytes = 0
                for chunk in r_ts.iter_content(chunk_size=32 * 1024):
                    if not chunk:
                        break
                    seg_bytes += len(chunk)
                    bytes_received += len(chunk)
                    if len(media_buffer) < 224 * 1024:  # 保留足够数据给 ffprobe
                        media_buffer.extend(chunk)
                    # 单片段或总超时控制
                    if time.time() - download_start >= CHUNK_DOWNLOAD_TIMEOUT:
                        break
                    if bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                        break

                r_ts.close()

                # 单个片段至少要有一定数据量，否则视为断流
                if seg_bytes >= 8 * 1024:
                    successful_segments += 1
                else:
                    # 片段太小，可能是假活或断流
                    if idx == 0:
                        return channel_name, clean_url, 0.0, False, "首片段数据不足/假活", 0

                if time.time() - download_start >= CHUNK_DOWNLOAD_TIMEOUT or bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                    break
            except Exception:
                if idx == 0:
                    return channel_name, clean_url, 0.0, False, "下载异常", 0
                continue

        if successful_segments == 0 or bytes_received < 16 * 1024:
            return channel_name, clean_url, 0.0, False, "无有效数据/断流", 0

        # 对 HLS，至少成功拿到 1 个完整可用片段
        if len(ts_urls) > 1 and successful_segments < 1:
            return channel_name, clean_url, 0.0, False, "HLS片段不连续", 0

        duration = max(time.time() - download_start, 0.08)
        speed_kbs = (bytes_received / 1024.0) / duration

        # ---------- 第四步：真实视频轨 + 分辨率检测 ----------
        raw_bytes = bytes(media_buffer)
        is_ts = len(raw_bytes) > 188 and (raw_bytes[0] == 0x47 or raw_bytes[188] == 0x47 or 0x47 in raw_bytes[:400])
        is_mp4 = (b"ftyp" in raw_bytes[:64] or b"moov" in raw_bytes[:64] or b"mdat" in raw_bytes[:64])

        if not (is_ts or is_mp4):
            return channel_name, clean_url, 0.0, False, "非视频数据流", 0

        codec, width, height = _ffprobe_video_info(raw_bytes)

        # 强制要求有真实视频画面
        if height < MIN_VIDEO_HEIGHT or width < 320 or not codec:
            return channel_name, clean_url, 0.0, False, f"无有效视频画面({width}x{height})", 0

        # 构建画质标签
        if height >= 1080:
            tag = "1080P"
        elif height >= 720:
            tag = "720P"
        elif height >= 480:
            tag = "480P"
        else:
            tag = f"{width}x{height}"
        video_tag = f"{codec} {tag}"

        # 速度必须达到门槛（海外测速会偏低，但持续速度更有参考价值）
        if speed_kbs < MIN_SPEED_KBS:
            return channel_name, clean_url, speed_kbs, False, f"速度不足({speed_kbs:.0f}<{MIN_SPEED_KBS})", height

        return channel_name, clean_url, speed_kbs, True, video_tag, height

    except Exception as e:
        return channel_name, clean_url, 0.0, False, f"异常: {str(e)[:40]}", 0


def run_speedtest(source_file: str = RAW_SOURCES_TXT, min_speed: float = MIN_SPEED_KBS):
    print("=" * 70, flush=True)
    print(f"🚀 [阶段二] 真实画面优先深度测速（接近国内 TVBox 观看体验）", flush=True)
    print(f"    - 最低持续速度门槛: >= {min_speed:.1f} KB/s", flush=True)
    print(f"    - 最低有效画面高度: >= {MIN_VIDEO_HEIGHT}p（强制有视频轨）", flush=True)
    print(f"    - 安全规则: 拦截内网密钥/黑屏加密/广播源/断流假活", flush=True)
    print(f"    - 单频道同 IP 最多保留 {MAX_SAME_IP_PER_CHANNEL} 条", flush=True)
    print(f"    - 最终排序: 清晰度（分辨率高度）优先 → 持续速度次之", flush=True)
    print("=" * 70, flush=True)

    channel_streams = parse_input_sources(source_file)
    if not channel_streams:
        print(f"[!] 未在 {source_file} 中检测到有效直播源！", flush=True)
        return

    user_struct = load_user_channel_structure()

    tasks: List[Tuple[str, str]] = []
    for ch, urls in channel_streams.items():
        for u in urls:
            tasks.append((ch, u))

    print(f"[*] 待测直播源总数: {len(tasks)} 条，启动 {MAX_WORKERS} 线程高并发真实画面评测...", flush=True)

    qualified_results: Dict[str, List[Tuple[str, float, str, int]]] = {}
    all_tested_results: Dict[str, List[Tuple[str, float, str, int]]] = {}
    tested_count = 0
    passed_count = 0
    rejected_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(test_single_stream_speed, ch, u) for ch, u in tasks]
        for f in as_completed(futures):
            tested_count += 1
            ch, url, speed, is_valid, video_tag, height = f.result()

            if is_valid:
                passed_count += 1
                if ch not in all_tested_results:
                    all_tested_results[ch] = []
                all_tested_results[ch].append((url, speed, video_tag, height))

                if speed >= min_speed:
                    if ch not in qualified_results:
                        qualified_results[ch] = []
                    qualified_results[ch].append((url, speed, video_tag, height))
            else:
                rejected_count += 1

            if tested_count % 25 == 0 or tested_count == len(tasks):
                elapsed = max(time.time() - start_time, 0.1)
                print(f"  [进展] 已评测 {tested_count}/{len(tasks)} 条 (有真实画面: {passed_count} 条, 拦截: {rejected_count} 条) ...", flush=True)

    # 排序：分辨率（高度）优先，再按持续速度（清晰度越高越靠前）
    for ch in qualified_results:
        qualified_results[ch].sort(key=lambda x: (x[3], x[1]), reverse=True)
    for ch in all_tested_results:
        all_tested_results[ch].sort(key=lambda x: (x[3], x[1]), reverse=True)

    export_final_playlists(qualified_results, all_tested_results, user_struct)


def export_final_playlists(
    qualified_results: Dict[str, List[Tuple[str, float, str, int]]],
    all_tested_results: Dict[str, List[Tuple[str, float, str, int]]],
    user_struct: Dict[str, List[str]]
):
    """
    装配输出 live.txt 与 live.m3u：
    1. 同服务器 IP 防独占；
    2. 取消硬上限，达标全部保留；
    3. 保底：达标不足时回退已通过真实画面检验的源；
    4. 最终按分辨率 + 持续速度综合排序（清晰度优先）。
    """
    txt_lines: List[str] = []
    m3u_lines: List[str] = ['#EXTM3U x-tvg-url="https://live.fanmingming.com/e.xml"']

    total_channels_with_streams = 0
    total_qualified_streams = 0

    print("\n" + "=" * 78, flush=True)
    print(f"{'频道名称':<18} | {'真实画面源':<10} | {'最高持续速率':<14} | {'最佳画质':<16}", flush=True)
    print("-" * 78, flush=True)

    for group_name, ch_list in user_struct.items():
        group_txt = [f"{group_name},#genre#"]
        has_channel = False

        for raw_ch in ch_list:
            std_ch = normalize_channel_name(raw_ch)
            primary_streams = qualified_results.get(std_ch, [])
            backup_streams = all_tested_results.get(std_ch, [])

            candidate_list = list(primary_streams)
            seen_urls = {s[0] for s in candidate_list}
            for bs in backup_streams:
                if bs[0] not in seen_urls:
                    candidate_list.append(bs)
                    seen_urls.add(bs[0])

            # 同服务器 IP 防独占
            final_channel_streams: List[Tuple[str, float, str, int]] = []
            ip_usage_count: Dict[str, int] = defaultdict(int)

            for url, spd, vtag, h in candidate_list:
                ip_p = extract_ip_port(url) or "unknown_host"
                if ip_usage_count[ip_p] < MAX_SAME_IP_PER_CHANNEL:
                    ip_usage_count[ip_p] += 1
                    final_channel_streams.append((url, spd, vtag, h))

            if final_channel_streams:
                has_channel = True
                total_channels_with_streams += 1
                total_qualified_streams += len(final_channel_streams)
                top_speed = final_channel_streams[0][1]
                top_video_tag = final_channel_streams[0][2]
                print(f"{std_ch:<18} | {len(final_channel_streams):<10} | {top_speed:>7.1f} KB/s | {top_video_tag:<16}", flush=True)

                for url, spd, vtag, h in final_channel_streams:
                    group_txt.append(f"{std_ch},{url}")
                    m3u_lines.append(f'#EXTINF:-1 tvg-name="{std_ch}" group-title="{group_name}",{std_ch}')
                    m3u_lines.append(url)
            else:
                print(f"{std_ch:<18} | {'0 (无可用源)':<10} | {'--':>12} | {'--':<16}", flush=True)

        if has_channel:
            txt_lines.extend(group_txt)

    with open(OUTPUT_LIVE_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines) + "\n")

    with open(OUTPUT_LIVE_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(m3u_lines) + "\n")

    print("=" * 78, flush=True)
    print(f"🎉 真实画面测速与多元化装配完成！")
    print(f"   - 有真实画面频道数: {total_channels_with_streams} 个")
    print(f"   - 保留可流畅播放源: {total_qualified_streams} 条 (已按分辨率+持续速度排序)")
    print(f"   - 交付文件 (TXT 格式): {OUTPUT_LIVE_TXT}")
    print(f"   - 交付文件 (M3U 格式): {OUTPUT_LIVE_M3U}")
    print("=" * 78, flush=True)
    print("⚠️  注意：GitHub Actions 跑在海外，测速数值会低于国内真实盒子。")
    print("   最终仍建议在国内网络用 TVBox 抽测确认。")
    print("=" * 78, flush=True)


# =========================================================================
# 命令行调度入口
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
                try:
                    min_speed = float(sys.argv[2])
                except Exception:
                    pass
        elif arg1 in ("full", "all"):
            mode = "full"
            if len(sys.argv) > 2:
                try:
                    min_speed = float(sys.argv[2])
                except Exception:
                    pass
        else:
            try:
                min_speed = float(arg1)
            except Exception:
                pass

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
