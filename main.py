# -*- coding: utf-8 -*-
"""
IPTV 自动化五步工作流引擎 & 独立在线测速脚本 (TVBox 国内 IP 仿真 + 清晰度优先排序版)
"""
import argparse
import datetime
import os
import re
import subprocess
import sys
import time
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set, Tuple

import requests

from config import (
    CHUNK_DOWNLOAD_MAX_BYTES,
    CHUNK_DOWNLOAD_TIMEOUT,
    MAX_CANDIDATES_PER_CHANNEL,
    MAX_SAME_IP_PER_CHANNEL,
    MAX_WORKERS,
    MIN_SPEED_KBS,
    OUTPUT_LIVE_M3U,
    OUTPUT_LIVE_TXT,
    RAW_SOURCES_M3U,
    RAW_SOURCES_TXT,
    REQUEST_TIMEOUT,
    build_channel_Lookup_map,
    clean_stream_url,
    extract_ip_port,
    extrapolate_channels_from_seed,
    get_domestic_headers,
    is_blacklisted_url,
    load_user_channel_structure,
    normalize_channel_name,
)

HEADERS = get_domestic_headers()

FALLBACK_GITHUB_MATRIX = [
    "[https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.txt](https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.txt)",
    "[https://raw.githubusercontent.com/iptvjs/iptv-api/main/output/user_result.txt](https://raw.githubusercontent.com/iptvjs/iptv-api/main/output/user_result.txt)",
    "[https://raw.githubusercontent.com/cymz6/AutoIPTV-Hotel/main/lives.txt](https://raw.githubusercontent.com/cymz6/AutoIPTV-Hotel/main/lives.txt)",
    "[https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/output/result.txt](https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/output/result.txt)",
    "[https://raw.githubusercontent.com/vbskycn/iptv/master/tv/hd.txt](https://raw.githubusercontent.com/vbskycn/iptv/master/tv/hd.txt)",
    "[https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv4.m3u](https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv4.m3u)",
]


def get_resolution_tier(width: int, height: int) -> Tuple[int, int, str]:
    if width <= 0 or height <= 0:
        return 0, 0, "未知分辨率"
    res_label = f"{width}x{height}"
    pixels = width * height
    if height >= 2000 or width >= 3800:
        return 4, pixels, res_label
    if height >= 1000 or width >= 1900:
        return 3, pixels, res_label
    if height >= 700 or width >= 1200:
        return 2, pixels, res_label
    if height >= 360 and width >= 640:
        return 1, pixels, res_label
    return 0, pixels, res_label


def stream_sort_key(item: Tuple[str, float, float, int, int, str]) -> Tuple[int, int, float]:
    url, speed_kbs, ttfb_ms, width, height, vtag = item
    tier, pixels, _ = get_resolution_tier(width, height)
    return (tier, pixels, round(speed_kbs, 2))


def get_current_month_str() -> str:
    return datetime.datetime.now().strftime("%Y-%m")


def get_github_search_headers() -> Dict[str, str]:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    headers = {
        "Accept": "application/vnd.github.v3.text-match+json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


def convert_github_item_to_raw_url(item: dict) -> str:
    html_url = item.get("html_url", "")
    if html_url and "[github.com/](https://github.com/)" in html_url and "/blob/" in html_url:
        return html_url.replace("[https://github.com/](https://github.com/)", "[https://raw.githubusercontent.com/](https://raw.githubusercontent.com/)").replace("/blob/", "/")
    repo_full_name = item.get("repository", {}).get("full_name", "")
    file_path = item.get("path", "")
    if repo_full_name and file_path:
        return f"[https://raw.githubusercontent.com/](https://raw.githubusercontent.com/){repo_full_name}/main/{file_path}"
    return ""


def search_github_first_page_with_preview(
    query: str,
    per_page: int = 20
) -> List[Tuple[str, str, str]]:
    headers = get_github_search_headers()
    valid_results: List[Tuple[str, str, str]] = []
    seen_raws: Set[str] = set()

    encoded_q = urllib.parse.quote(query)
    api_url = f"[https://api.github.com/search/code?q=](https://api.github.com/search/code?q=){encoded_q}&per_page={per_page}&page=1"

    try:
        resp = requests.get(api_url, headers=headers, timeout=12)
        if resp.status_code == 200:
            items = resp.json().get("items", [])
            print(f"  [*] GitHub 首页返回结果数: {len(items)} 条 (全部直接提取文件链接，不打开仓库主页)", flush=True)
            for idx, item in enumerate(items, 1):
                repo_name = item.get("repository", {}).get("full_name", "")
                file_path = item.get("path", "")
                display_name = f"{file_path} ({repo_name})"

                text_matches = item.get("text_matches", [])
                fragments = [tm.get("fragment", "").strip() for tm in text_matches if tm.get("fragment", "").strip()]
                combined_preview = "\n".join(fragments)

                if not combined_preview:
                    print(f"  [跳过 {idx}/{len(items)}] {display_name} -> 无展开代码预览片段", flush=True)
                    continue

                raw_url = convert_github_item_to_raw_url(item)
                if raw_url and raw_url not in seen_raws:
                    seen_raws.add(raw_url)
                    preview_first_line = combined_preview.splitlines()[0][:65] if combined_preview else ""
                    valid_results.append((display_name, raw_url, preview_first_line))
        elif resp.status_code in (403, 429):
            print(f"  [!] GitHub Search API 触发限流 ({resp.status_code})，请确保已配置有效的 GITHUB_TOKEN。", flush=True)
        else:
            print(f"  [!] GitHub 搜索返回状态码: {resp.status_code}", flush=True)
    except Exception as e:
        print(f"  [!] 搜索请求异常 ({query}): {e}", flush=True)

    return valid_results


def fetch_file_content(url: str) -> str:
    try:
        r = requests.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            return r.text
        if r.status_code == 404 and "/main/" in url:
            r2 = requests.get(url.replace("/main/", "/master/"), headers=HEADERS, timeout=10)
            if r2.status_code == 200:
                return r2.text
    except Exception:
        pass
    return ""


def extract_raw_entries_from_text(content: str) -> List[Tuple[str, str]]:
    entries: List[Tuple[str, str]] = []
    if not content:
        return entries

    lines = [line.strip() for line in content.splitlines() if line.strip()]

    if any(line.startswith("#EXTM3U") for line in lines[:5]) or any(line.startswith("#EXTINF") for line in lines[:10]):
        current_title = ""
        for line in lines:
            if line.startswith("#EXTINF"):
                m = re.search(r',([^,]+)$', line)
                if m:
                    current_title = m.group(1).strip()
            elif line.startswith(("http://", "https://")):
                if current_title:
                    entries.append((current_title, line))
                current_title = ""
        return entries

    for line in lines:
        if line.startswith("#") or ",#genre#" in line or "更新时间" in line or "更新日期" in line:
            continue
        if "," in line and ("http://" in line or "https://" in line):
            parts = line.split(",", 1)
            raw_ch = parts[0].strip()
            raw_url_part = parts[1].strip()
            for sub_url in raw_url_part.split("#"):
                sub_url = sub_url.strip()
                if sub_url.startswith(("http://", "https://")):
                    entries.append((raw_ch, sub_url))

    return entries


def step1_search_and_extract(target_channels: Set[str]) -> List[Tuple[str, str]]:
    curr_month = get_current_month_str()
    print("=" * 76, flush=True)
    print(f"🚀 【第一步】精准搜索 GitHub 首页直播源 (目标月份: {curr_month}, 20条有预览文件链接全部打开提取)", flush=True)
    print("=" * 76, flush=True)

    exact_queries = [
        f"更新 {curr_month} CCTV-6,http",
        f"更新 {curr_month} CCTV6,http",
    ]

    raw_extracted_records: List[Tuple[str, str]] = []
    visited_raw_files: Set[str] = set()

    for q in exact_queries:
        print(f"\n[*] 正在搜索首页关键词: 「{q}」 (第 1 页 20 条有预览的文件链接全部打开提取)...", flush=True)
        search_items = search_github_first_page_with_preview(q, per_page=20)
        if not search_items:
            print(f"  [-] 未获取到「{q}」的带预览搜索结果。", flush=True)
            continue

        for idx, (display_name, raw_url, preview_snippet) in enumerate(search_items, 1):
            if raw_url in visited_raw_files:
                print(f"  -> [{idx}/{len(search_items)}] 文件链接已在上一轮提取过，跳过重复下载: {display_name}", flush=True)
                continue
            visited_raw_files.add(raw_url)
            print(f"  -> [{idx}/{len(search_items)}] 直接打开文件链接: {display_name}", flush=True)
            if preview_snippet:
                print(f"     [预览片段] {preview_snippet}...", flush=True)

            content = fetch_file_content(raw_url)
            if not content:
                continue

            entries = extract_raw_entries_from_text(content)
            matched_in_file = 0
            for raw_ch, raw_url_str in entries:
                norm_ch = normalize_channel_name(raw_ch, target_channels)
                if norm_ch in target_channels:
                    raw_extracted_records.append((norm_ch, raw_url_str))
                    matched_in_file += 1
            print(f"     [✓] 匹配网页目标频道记录: {matched_in_file} 条", flush=True)

        time.sleep(1.0)

    print("\n[*] 正在同步国内高活性直播源补充矩阵...", flush=True)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(fetch_file_content, u): u for u in FALLBACK_GITHUB_MATRIX if u not in visited_raw_files}
        for fut in as_completed(future_map):
            content = fut.result()
            if content:
                for raw_ch, raw_url_str in extract_raw_entries_from_text(content):
                    norm_ch = normalize_channel_name(raw_ch, target_channels)
                    if norm_ch in target_channels:
                        raw_extracted_records.append((norm_ch, raw_url_str))

    print(f"\n[✓] 第一步完成：共打开 {len(visited_raw_files)} 个搜索结果文件，提取到符合目标频道的原始记录 {len(raw_extracted_records)} 条。", flush=True)
    return raw_extracted_records


def step2_clean_and_deduplicate(
    raw_records: List[Tuple[str, str]],
    target_channels: Set[str]
) -> Tuple[Dict[str, List[str]], Set[str]]:
    print("\n" + "=" * 76, flush=True)
    print("🧹 【第二步】清洗提取结果 ($尾缀剔除、拦截/rtp/与/udp/组播假源、HTTP严格去重与配额控制)", flush=True)
    print("=" * 76, flush=True)

    cleaned_pool: Dict[str, List[str]] = {ch: [] for ch in target_channels}
    seen_urls_global: Set[str] = set()
    channel_ip_count: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    total_cleaned = 0
    removed_suffix_count = 0
    blocked_junk_count = 0
    blocked_udpxy_count = 0
    channel_overflow_count = 0

    for raw_ch, raw_url in raw_records:
        if "$" in raw_url:
            removed_suffix_count += 1

        raw_head = raw_url.split("$", 1)[0].strip()
        if is_blacklisted_url(raw_head):
            blocked_junk_count += 1
            continue

        clean_url = clean_stream_url(raw_url)
        if not clean_url or not clean_url.startswith(("http://", "https://")):
            continue

        if "/rtp/" in clean_url.lower() or "/udp/" in clean_url.lower():
            blocked_udpxy_count += 1
            continue

        if re.search(r'http://\[[0-9a-fA-F:]+\]', clean_url):
            continue

        std_ch = normalize_channel_name(raw_ch, target_channels)
        if std_ch not in target_channels:
            continue

        norm_u = clean_url.rstrip("/")
        if norm_u in seen_urls_global:
            continue

        if len(cleaned_pool[std_ch]) >= MAX_CANDIDATES_PER_CHANNEL:
            channel_overflow_count += 1
            continue

        ip_port = extract_ip_port(clean_url)
        if ip_port and channel_ip_count[std_ch][ip_port] >= MAX_SAME_IP_PER_CHANNEL:
            continue

        seen_urls_global.add(norm_u)
        cleaned_pool[std_ch].append(clean_url)
        if ip_port:
            channel_ip_count[std_ch][ip_port] += 1
        total_cleaned += 1

    print(f"[✓] 第二步完成：", flush=True)
    print(f"    - 已清除含 '$' 尾缀标记的地址: {removed_suffix_count} 处", flush=True)
    print(f"    - 已彻底拦截两IP相连的 /rtp/ 与 /udp/ 组播无图像假源: {blocked_udpxy_count} 条", flush=True)
    print(f"    - 已拦截纯音频广播/.swf/一次性Token假源: {blocked_junk_count} 条", flush=True)
    print(f"    - 每个频道限额前 {MAX_CANDIDATES_PER_CHANNEL} 条 (同IP限{MAX_SAME_IP_PER_CHANNEL}条)，总待测候选源: {total_cleaned} 条", flush=True)
    return cleaned_pool, seen_urls_global


def check_hls_stream_security(m3u8_text: str, base_url: str) -> Tuple[bool, str]:
    key_match = re.search(r'#EXT-X-KEY:METHOD=([A-Z0-9\-]+).*?URI="([^"]+)"', m3u8_text)
    if key_match:
        method = key_match.group(1).upper()
        if method != "NONE":
            key_uri = key_match.group(2)
            full_key_url = urllib.parse.urljoin(base_url, key_uri)
            if re.search(r'(10\.\d+|192\.168\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+|127\.0\.0\.1|localhost)', full_key_url):
                return False, "内网私有密钥不可达"
            try:
                kr = requests.get(full_key_url, headers=HEADERS, timeout=2.5)
                if kr.status_code != 200 or len(kr.content) not in (16, 32):
                    return False, "密钥失效无法解密"
            except Exception:
                return False, "密钥请求超时"
    return True, "OK"


def verify_video_stream_with_ffprobe(raw_bytes: bytes) -> Tuple[bool, int, int, str]:
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-show_entries", "stream=codec_name,width,height",
            "-of", "csv=s=x:p=0",
            "-i", "pipe:0"
        ]
        proc = subprocess.run(
            cmd,
            input=raw_bytes[:256 * 1024],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=2.2
        )
        out = proc.stdout.decode("utf-8", errors="ignore").strip()
        if not out:
            return False, 0, 0, "无视频画面轨(纯音频广播或损坏流)"

        first_line = out.splitlines()[0].strip()
        parts = first_line.split("x")
        if len(parts) < 3:
            return False, 0, 0, "视频参数缺失"

        codec = parts[0].upper()
        width = int(parts[1])
        height = int(parts[2])

        if width < 640 or height < 360:
            return False, width, height, f"分辨率过低({width}x{height})"

        return True, width, height, f"{width}x{height} ({codec})"
    except FileNotFoundError:
        return True, 1920, 1080, "1920x1080 (TS校验)"
    except Exception:
        return False, 0, 0, "视频解码探测超时/假流"


def evaluate_domestic_ip_affinity(url: str, ttfb_ms: float) -> float:
    lower_u = url.lower()
    if any(proxy_kw in lower_u for proxy_kw in ("cors.isteed.cc", "ghproxy.", "workers.dev", "vercel.app")):
        return 0.0

    weight = 1.0
    if "/tsfile/live/" in lower_u or "chinamobile" in lower_u or "unicom" in lower_u or "telecom" in lower_u:
        weight *= 1.18

    if ttfb_ms < 350:
        weight *= 1.12
    elif ttfb_ms > 1600:
        weight *= 0.82

    return weight


def test_single_stream_tvbox(channel_name: str, stream_url: str) -> Tuple[str, str, float, float, int, int, bool, str]:
    clean_url = clean_stream_url(stream_url)
    if not clean_url:
        return channel_name, stream_url, 0.0, 9999.0, 0, 0, False, "黑名单/组播拦截"

    req_start = time.time()

    try:
        r = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
        if r.status_code != 200:
            r.close()
            return channel_name, clean_url, 0.0, 9999.0, 0, 0, False, f"HTTP {r.status_code}"

        content_type = r.headers.get("Content-Type", "").lower()
        if any(bad_ct in content_type for bad_ct in ("audio/", "shockwave-flash", "text/html", "image/", "application/json")):
            r.close()
            return channel_name, clean_url, 0.0, 9999.0, 0, 0, False, f"非视频类型({content_type})"

        first_chunk = next(r.iter_content(chunk_size=1024), b"")
        ttfb_ms = (time.time() - req_start) * 1000.0
        r.close()

        if not first_chunk:
            return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "空响应"

        if first_chunk[:3] in (b"CWS", b"FWS", b"ZWS") or b"<html" in first_chunk.lower() or b"<!doctype" in first_chunk.lower():
            return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "Flash插件或网页"

        affinity_weight = evaluate_domestic_ip_affinity(clean_url, ttfb_ms)
        if affinity_weight <= 0.0:
            return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "境外代理死源拦截"

        ts_urls: List[str] = []
        current_m3u8_url = clean_url
        is_hls = b"#EXTM3U" in first_chunk
        m3u8_res_hint: Tuple[int, int] = (0, 0)

        if is_hls:
            try:
                m3u8_resp = requests.get(current_m3u8_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                full_m3u8 = m3u8_resp.text

                if "#EXT-X-STREAM-INF" in full_m3u8:
                    m_res = re.search(r'RESOLUTION=(\d+)x(\d+)', full_m3u8)
                    if m_res:
                        m3u8_res_hint = (int(m_res.group(1)), int(m_res.group(2)))
                    sub_playlists = [
                        line.strip() for line in full_m3u8.splitlines()
                        if line.strip() and not line.strip().startswith("#")
                    ]
                    if sub_playlists:
                        current_m3u8_url = urllib.parse.urljoin(current_m3u8_url, sub_playlists[-1])
                        full_m3u8 = requests.get(current_m3u8_url, headers=HEADERS, timeout=REQUEST_TIMEOUT).text

                if "#EXT-X-ENDLIST" in full_m3u8:
                    return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "录播死切片/非实时直播"

                sec_ok, sec_msg = check_hls_stream_security(full_m3u8, current_m3u8_url)
                if not sec_ok:
                    return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, sec_msg

                segments = [
                    urllib.parse.urljoin(current_m3u8_url, line.strip())
                    for line in full_m3u8.splitlines()
                    if line.strip() and not line.strip().startswith("#")
                ]
                if not segments:
                    return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "M3U8无有效切片"

                ts_urls = segments[-2:] if len(segments) >= 2 else segments[:1]
            except Exception:
                return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "M3U8解析失败"
        else:
            ts_urls = [clean_url]

        for tu in ts_urls:
            if is_blacklisted_url(tu):
                return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "切片命中音频/黑名单"
            if re.search(r'(10\.\d+|192\.168\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+|127\.0\.0\.1|localhost)', tu):
                return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "内网切片不可达"

        download_start = time.time()
        bytes_received = 0
        media_buffer = bytearray()
        segments_ok = 0

        for target_ts in ts_urls:
            try:
                r_ts = requests.get(target_ts, headers=HEADERS, timeout=CHUNK_DOWNLOAD_TIMEOUT, stream=True)
                if r_ts.status_code == 200:
                    seg_bytes = 0
                    for chunk in r_ts.iter_content(chunk_size=32 * 1024):
                        if not chunk:
                            break
                        bytes_received += len(chunk)
                        seg_bytes += len(chunk)
                        if len(media_buffer) < 256 * 1024:
                            media_buffer.extend(chunk)
                        if time.time() - download_start >= CHUNK_DOWNLOAD_TIMEOUT or bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                            break
                    if seg_bytes >= 32 * 1024:
                        segments_ok += 1
                r_ts.close()
            except Exception:
                break
            if time.time() - download_start >= CHUNK_DOWNLOAD_TIMEOUT or bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                break

        if bytes_received < 128 * 1024 or segments_ok == 0:
            return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "数据量不足/断流源"

        raw_bytes = bytes(media_buffer)

        is_valid_ts = (
            len(raw_bytes) >= 564
            and raw_bytes[0] == 0x47
            and raw_bytes[188] == 0x47
            and raw_bytes[376] == 0x47
        )
        is_mp4_or_flv = (
            b"ftyp" in raw_bytes[:64]
            or b"moov" in raw_bytes[:64]
            or raw_bytes[:3] == b"FLV"
        )

        if not (is_valid_ts or is_mp4_or_flv):
            return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "非标准TS/FLV视频封装"

        has_video, width, height, video_tag = verify_video_stream_with_ffprobe(raw_bytes)
        if not has_video:
            return channel_name, clean_url, 0.0, ttfb_ms, width, height, False, video_tag

        if (width == 0 or height == 0) and m3u8_res_hint != (0, 0):
            width, height = m3u8_res_hint
            video_tag = f"{width}x{height}"

        duration = max(time.time() - download_start, 0.08)
        raw_speed_kbs = (bytes_received / 1024.0) / duration
        effective_speed_kbs = raw_speed_kbs * affinity_weight

        return channel_name, clean_url, effective_speed_kbs, ttfb_ms, width, height, True, video_tag

    except Exception:
        return channel_name, clean_url, 0.0, 9999.0, 0, 0, False, "连接超时"


def batch_evaluate_streams(
    pool: Dict[str, List[str]],
    min_speed: float,
    stage_title: str,
    base_accumulated_count: int = 0
) -> Dict[str, List[Tuple[str, float, float, int, int, str]]]:
    tasks: List[Tuple[str, str]] = []
    for ch, urls in pool.items():
        for u in urls:
            tasks.append((ch, u))

    print("\n" + "=" * 76, flush=True)
    print(f"⚡ {stage_title} (待测总数: {len(tasks)} 条, 速度门槛: {min_speed:.1f} KB/s)", flush=True)
    print("=" * 76, flush=True)

    qualified: Dict[str, List[Tuple[str, float, float, int, int, str]]] = defaultdict(list)
    if not tasks:
        return qualified

    tested = 0
    passed = 0
    rejected_reasons: Dict[str, int] = defaultdict(int)
    start_t = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(test_single_stream_tvbox, ch, u) for ch, u in tasks]
        for fut in as_completed(futures):
            tested += 1
            ch, url, spd, ttfb, width, height, ok, vtag = fut.result()
            if ok and spd >= min_speed:
                passed += 1
                qualified[ch].append((url, spd, ttfb, width, height, vtag))
            else:
                reason_key = vtag if not ok else "低于速度门槛"
                rejected_reasons[reason_key] += 1

            if tested % 40 == 0 or tested == len(tasks):
                elapsed = max(time.time() - start_t, 0.1)
                if base_accumulated_count > 0:
                    print(
                        f"  [裂变新源二次测速] {tested}/{len(tasks)} | 本轮新增达标: {passed} 条 | 累计总达标源: {base_accumulated_count + passed} 条 | 耗时: {elapsed:.1f}s",
                        flush=True
                    )
                else:
                    print(
                        f"  [国内TVBox仿真测速] {tested}/{len(tasks)} | 达标源: {passed} 条 | 耗时: {elapsed:.1f}s",
                        flush=True
                    )

    if rejected_reasons:
        top_reasons = sorted(rejected_reasons.items(), key=lambda x: x[1], reverse=True)[:5]
        reason_str = ", ".join(f"{k}: {v}条" for k, v in top_reasons)
        print(f"  [拦截统计] 主要淘汰原因 -> {reason_str}", flush=True)

    for ch in qualified:
        qualified[ch].sort(key=stream_sort_key, reverse=True)

    return qualified


def step4_fission_high_speed_sources(
    step3_qualified: Dict[str, List[Tuple[str, float, float, int, int, str]]],
    already_tested_urls: Set[str],
    target_channels: Set[str]
) -> Dict[str, List[str]]:
    print("\n" + "=" * 76, flush=True)
    print("🧬 【第四步】央卫视智能裂变补齐 (cctv1hd -> cctv1~17hd / dfwshd -> jxwshd,hnwshd 等)", flush=True)
    print("=" * 76, flush=True)

    fission_candidates: Dict[str, List[str]] = {ch: [] for ch in target_channels}
    fission_seen: Set[str] = {clean_stream_url(u).rstrip("/") for u in already_tested_urls if u}

    all_high_speed_seeds: List[Tuple[str, str, float, int, int]] = []
    for ch, items in step3_qualified.items():
        for url, spd, ttfb, w, h, vtag in items:
            fission_seen.add(clean_stream_url(url).rstrip("/"))
            all_high_speed_seeds.append((ch, url, spd, w, h))

    all_high_speed_seeds.sort(key=lambda x: (get_resolution_tier(x[3], x[4])[0], x[2]), reverse=True)

    cctv_fission_added = 0
    sat_fission_added = 0
    skipped_already_tested = 0

    for seed_ch, seed_url, _, _, _ in all_high_speed_seeds:
        if any(k in seed_url.lower() for k in ("/rtp/", "/udp/")):
            continue
        derived_list = extrapolate_channels_from_seed(seed_url, seed_ch)
        for derived_ch, new_url in derived_list:
            if any(k in new_url.lower() for k in ("/rtp/", "/udp/")):
                continue
            clean_u = clean_stream_url(new_url)
            if not clean_u:
                continue
            norm_u = clean_u.rstrip("/")
            if norm_u in fission_seen:
                skipped_already_tested += 1
                continue

            std_ch = normalize_channel_name(derived_ch, target_channels)
            if std_ch in target_channels:
                if len(fission_candidates[std_ch]) < 20:
                    fission_seen.add(norm_u)
                    fission_candidates[std_ch].append(clean_u)
                    if std_ch.startswith("CCTV"):
                        cctv_fission_added += 1
                    else:
                        sat_fission_added += 1

    total_fission = cctv_fission_added + sat_fission_added
    print(f"[✓] 第四步裂变完成：共生成【全新未测速】的裂变候选源 {total_fission} 条", flush=True)
    print(f"    - 央视频道后缀规律裂变新增 (如 cctv1hd.m3u8 -> cctv2hd~cctv17hd.m3u8): {cctv_fission_added} 条", flush=True)
    print(f"    - 卫视频道后缀规律裂变新增 (如 dfwshd.m3u8 -> jxwshd, hnwshd.m3u8 等): {sat_fission_added} 条", flush=True)
    print(f"    - 自动跳过裂变前已测过速/已存在的直播源地址: {skipped_already_tested} 条 (不重复测速)", flush=True)
    return fission_candidates


def step5_merge_sort_and_export(
    step3_qualified: Dict[str, List[Tuple[str, float, float, int, int, str]]],
    step5_qualified: Dict[str, List[Tuple[str, float, float, int, int, str]]],
    user_struct: Dict[str, List[str]],
    norm_to_display: Dict[str, str],
    target_channels: Set[str]
):
    step3_count = sum(len(v) for v in step3_qualified.values())
    step5_count = sum(len(v) for v in step5_qualified.values())
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("\n" + "=" * 86, flush=True)
    print("🏆 【第五步(2/2)】汇总合并全部达标源，按清晰度优先 (1920x1080 且速度快在前 -> 720x576) 排序输出", flush=True)
    print(f"   - 第三步初次测速达标源 (无需二次测速): {step3_count} 条", flush=True)
    print(f"   - 第五步裂变新源二次测速新增达标源: {step5_count} 条", flush=True)
    print(f"{'网页频道名称':<18} | {'有效源数':<8} | {'首选分辨率':<14} | {'首选实测速度':<13} | {'首包延迟':<10}", flush=True)
    print("-" * 86, flush=True)

    txt_lines: List[str] = [
        "更新时间,#genre#",
        f"最新更新: {now_str},[https://tvzb.github.io/IPTV/live.txt](https://tvzb.github.io/IPTV/live.txt)",
    ]
    m3u_lines: List[str] = [
        '#EXTM3U x-tvg-url="[https://live.fanmingming.com/e.xml](https://live.fanmingming.com/e.xml)"',
        f'#EXTINF:-1 tvg-name="更新时间" group-title="更新时间",最新更新: {now_str}',
        '[https://tvzb.github.io/IPTV/live.m3u](https://tvzb.github.io/IPTV/live.m3u)',
    ]

    total_channels_ready = 0
    total_streams_ready = 0

    for group_name, ch_list in user_struct.items():
        group_txt = [f"{group_name},#genre#"]
        group_has_items = False

        for display_ch in ch_list:
            std_ch = normalize_channel_name(display_ch, target_channels) or display_ch
            combined_map: Dict[str, Tuple[str, float, float, int, int, str]] = {}
            for item in step3_qualified.get(std_ch, []) + step5_qualified.get(std_ch, []):
                url, spd, ttfb, w, h, vtag = item
                norm_u = url.rstrip("/")
                if norm_u not in combined_map or stream_sort_key(item) > stream_sort_key(combined_map[norm_u]):
                    combined_map[norm_u] = item

            sorted_streams = sorted(combined_map.values(), key=stream_sort_key, reverse=True)

            final_streams: List[Tuple[str, float, float, int, int, str]] = []
            ip_counter: Dict[str, int] = defaultdict(int)
            for item in sorted_streams:
                url, spd, ttfb, w, h, vtag = item
                host_key = extract_ip_port(url) or url
                if ip_counter[host_key] < MAX_SAME_IP_PER_CHANNEL:
                    ip_counter[host_key] += 1
                    final_streams.append(item)

            if final_streams:
                group_has_items = True
                total_channels_ready += 1
                total_streams_ready += len(final_streams)
                top_url, top_spd, top_ttfb, top_w, top_h, top_vtag = final_streams[0]
                res_str = f"{top_w}x{top_h}" if top_w > 0 else top_vtag
                print(
                    f"{display_ch:<18} | {len(final_streams):<8} | {res_str:<14} | {top_spd:>7.1f} KB/s | {top_ttfb:>6.0f} ms",
                    flush=True
                )
                for url, spd, ttfb, w, h, vtag in final_streams:
                    group_txt.append(f"{display_ch},{url}")
                    m3u_lines.append(f'#EXTINF:-1 tvg-name="{std_ch}" group-title="{group_name}",{display_ch}')
                    m3u_lines.append(url)
            else:
                print(f"{display_ch:<18} | {'0 (无达标)':<8} | {'--':<14} | {'--':>11} | {'--':>9}", flush=True)

        if group_has_items:
            txt_lines.extend(group_txt)

    with open(OUTPUT_LIVE_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines) + "\n")

    with open(OUTPUT_LIVE_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(m3u_lines) + "\n")

    print("=" * 86, flush=True)
    print(f"🎉 五步工作流全部完成！")
    print(f"   - 订阅更新时间戳: {now_str}")
    print(f"   - 第三步初次搜索达标源: {step3_count} 条")
    print(f"   - 第五步智能裂变新增达标源: {step5_count} 条 (两轮合并去重后共收录: {total_streams_ready} 条)")
    print(f"   - 覆盖网页定制频道: {total_channels_ready} 个")
    print(f"   - 最终收录真视频高速源: {total_streams_ready} 条 (已按 1920x1080 优先 -> 720x576 排序)")
    print(f"   - TVBox TXT 订阅文件: {OUTPUT_LIVE_TXT}")
    print(f"   - TVBox M3U 订阅文件: {OUTPUT_LIVE_M3U}")
    print("=" * 86, flush=True)


def save_intermediate_cleaned_pool(
    cleaned_pool: Dict[str, List[str]],
    user_struct: Dict[str, List[str]],
    target_channels: Set[str]
):
    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    txt_lines: List[str] = [
        "更新时间,#genre#",
        f"待测源归档: {now_str},[https://tvzb.github.io/IPTV/raw_sources.txt](https://tvzb.github.io/IPTV/raw_sources.txt)",
    ]
    m3u_lines: List[str] = [
        '#EXTM3U x-tvg-url="[https://live.fanmingming.com/e.xml](https://live.fanmingming.com/e.xml)"',
        f'#EXTINF:-1 tvg-name="更新时间" group-title="更新时间",待测源归档: {now_str}',
        '[https://tvzb.github.io/IPTV/raw_sources.m3u](https://tvzb.github.io/IPTV/raw_sources.m3u)',
    ]
    for group_name, ch_list in user_struct.items():
        group_txt = [f"{group_name},#genre#"]
        for display_ch in ch_list:
            std_ch = normalize_channel_name(display_ch, target_channels) or display_ch
            for u in cleaned_pool.get(std_ch, []):
                group_txt.append(f"{display_ch},{u}")
                m3u_lines.append(f'#EXTINF:-1 tvg-name="{std_ch}" group-title="{group_name}",{display_ch}')
                m3u_lines.append(u)
        if len(group_txt) > 1:
            txt_lines.extend(group_txt)

    with open(RAW_SOURCES_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines) + "\n")
    with open(RAW_SOURCES_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(m3u_lines) + "\n")


def run_standalone_speedtest_mode(input_file: str, min_speed: float = MIN_SPEED_KBS):
    print("=" * 82, flush=True)
    print(f"⚡ 【在线测速模式 speedtest.yml】读取输入文件: {input_file} (速度门槛: {min_speed:.1f} KB/s)", flush=True)
    print("   流程与 iptv.yml 测速部分 100% 相同：HTTP 去重 -> 国内 TVBox 仿真测速 -> 1920x1080 优先排序", flush=True)
    print("=" * 82, flush=True)

    if not os.path.exists(input_file):
        print(f"[!] 未找到输入文件 {input_file}，请检查输入内容。", flush=True)
        return

    with open(input_file, "r", encoding="utf-8", errors="ignore") as f:
        content = f.read()

    raw_entries = extract_raw_entries_from_text(content)
    seen_urls: Set[str] = set()
    channel_order: List[str] = []
    pool: Dict[str, List[str]] = defaultdict(list)

    for raw_ch, raw_u in raw_entries:
        clean_u = clean_stream_url(raw_u)
        if not clean_u:
            continue
        norm_u = clean_u.rstrip("/")
        if norm_u not in seen_urls:
            seen_urls.add(norm_u)
            if raw_ch not in channel_order:
                channel_order.append(raw_ch)
            pool[raw_ch].append(clean_u)

    print(f"[*] 原始提取 {len(raw_entries)} 条，HTTP 严格去重后共 {len(seen_urls)} 条唯一地址。", flush=True)

    qualified = batch_evaluate_streams(
        pool,
        min_speed=min_speed,
        stage_title="【在线去重测速】国内 TVBox 播放仿真 & ffprobe 分辨率检测"
    )

    now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    out_txt_lines = [
        "更新时间,#genre#",
        f"测速完成: {now_str},[https://tvzb.github.io/IPTV/live.txt](https://tvzb.github.io/IPTV/live.txt)",
        "测速优选频道,#genre#",
    ]
    out_m3u_lines = [
        '#EXTM3U x-tvg-url="[https://live.fanmingming.com/e.xml](https://live.fanmingming.com/e.xml)"',
        f'#EXTINF:-1 tvg-name="更新时间" group-title="更新时间",测速完成: {now_str}',
        '[https://tvzb.github.io/IPTV/live.m3u](https://tvzb.github.io/IPTV/live.m3u)',
    ]

    for ch in channel_order:
        items = qualified.get(ch, [])
        items.sort(key=stream_sort_key, reverse=True)
        for url, spd, ttfb, w, h, vtag in items:
            out_txt_lines.append(f"{ch},{url}")
            out_m3u_lines.append(f'#EXTINF:-1 tvg-name="{ch}" group-title="测速优选",{ch}')
            out_m3u_lines.append(url)

    with open(OUTPUT_LIVE_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(out_txt_lines) + "\n")
    with open(OUTPUT_LIVE_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(out_m3u_lines) + "\n")

    print(f"[✓] 在线去重测速完成！结果已写入 {OUTPUT_LIVE_TXT} 与 {OUTPUT_LIVE_M3U}", flush=True)


def run_five_step_workflow(min_speed: float = MIN_SPEED_KBS):
    total_start = time.time()

    user_struct = load_user_channel_structure()
    target_channels, norm_to_display = build_channel_Lookup_map(user_struct)

    print(f"[*] 当前目标分类数: {len(user_struct)} 个 | 合计目标频道数: {len(target_channels)} 个", flush=True)

    raw_records = step1_search_and_extract(target_channels)
    cleaned_pool, all_seen_urls = step2_clean_and_deduplicate(raw_records, target_channels)
    save_intermediate_cleaned_pool(cleaned_pool, user_struct, target_channels)

    tested_url_set: Set[str] = set(all_seen_urls)
    for _, raw_u in raw_records:
        cu = clean_stream_url(raw_u)
        if cu:
            tested_url_set.add(cu.rstrip("/"))

    step3_qualified = batch_evaluate_streams(
        cleaned_pool,
        min_speed=min_speed,
        stage_title="【第三步】初次测速与视频分辨率校验 (模拟国内 TVBox 播放)"
    )
    step3_passed_total = sum(len(v) for v in step3_qualified.values())
    print(f"[✓] 第三步完成：初次测速获得达标源 {step3_passed_total} 条", flush=True)

    fission_pool = step4_fission_high_speed_sources(
        step3_qualified,
        already_tested_urls=tested_url_set,
        target_channels=target_channels
    )

    step5_qualified = batch_evaluate_streams(
        fission_pool,
        min_speed=min_speed,
        stage_title="【第五步(1/2)】仅对第四步智能裂变【新生成】的直播源地址进行二次测速",
        base_accumulated_count=step3_passed_total
    )

    step5_merge_sort_and_export(step3_qualified, step5_qualified, user_struct, norm_to_display, target_channels)

    elapsed = time.time() - total_start
    print(f"\n[🏁] 全流程执行完毕，总耗时: {elapsed:.1f} 秒", flush=True)


def main():
    parser = argparse.ArgumentParser(description="IPTV 自动化五步搜索裂变测速引擎")
    parser.add_argument("min_speed_pos", nargs="?", type=float, default=None, help="最低测速门槛 (KB/s)")
    parser.add_argument("--mode", choices=["iptv", "speedtest"], default="iptv", help="运行模式: iptv(完整五步) 或 speedtest(仅去重测速)")
    parser.add_argument("--input", default=RAW_SOURCES_TXT, help="speedtest 模式下的输入文件路径")
    parser.add_argument("--min-speed", type=float, default=MIN_SPEED_KBS, help="最低速度阈值 (KB/s)")

    args, _ = parser.parse_known_args()
    effective_min_speed = args.min_speed_pos if args.min_speed_pos is not None else args.min_speed

    if args.mode == "speedtest":
        run_standalone_speedtest_mode(args.input, min_speed=effective_min_speed)
    else:
        run_five_step_workflow(min_speed=effective_min_speed)


if __name__ == "__main__":
    main()
