# -*- coding: utf-8 -*-
"""
IPTV 自动化五步工作流引擎 & 独立在线测速脚本 (TVBox 国内 IP 仿真 + 清晰度优先排序版)

核心升级 (严格落实全部优化要求)：
  1. 搜索策略精准对齐 GitHub 第一页 (20条)：
     - 不再翻找前 2 页，仅分别打开「更新 YYYY-MM CCTV-6,http」和「更新 YYYY-MM CCTV6,http」首页 (page=1, per_page=20) 的搜索结果链接；
     - 启用 `application/vnd.github.v3.text-match+json` 获取代码片段预览 (`text_matches`)；
     - 自动拦截并跳过像图三那样「只有文件名链接、没有任何小部分预览代码 (或无 http 预览)」的折叠重复/无效文件，绝不浪费时间打开！
  2. 网页频道列表深度联动：
     - 自动读取网页上设置的分类与频道顺序 (`channels.json` 或 workflow_dispatch 传入的 `custom_channels_json`) 进行针对性搜索、归类与排序输出。
  3. 央卫视双向智能裂变补齐：
     - 央视裂变：`CCTV6,http://61.136.172.236:9901/tsfile/live/0006_1.m3u8` -> 自动推导出 `0001_1.m3u8` (CCTV-1) ~ `0017_1.m3u8` (CCTV-17) 全套央视频道；
     - 卫视裂变：`江西卫视,http://goo.bkpcp.top/mg/jxws` -> 自动推导出 `湖南卫视,http://goo.bkpcp.top/mg/hnws` 等全部省级卫视。
  4. 国内 TVBox 播放环境仿真与清晰度优先排序：
     - 模拟国内 TVBox 播放器 (`okhttp/3.15` + 国内 DNS/公网 IP 请求头 + 剔除跨境高丢包节点)；
     - 测速结果严格按【清晰度优先，同清晰度按速度由高到低】排序：
       1920x1080 (及 4K) 且速度快的排最前面，排完 1080P 后，再排 1280x720 / 720x576 且速度快的源。
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

# 精选高活性国内直播源仓库矩阵（补充候选池）
FALLBACK_GITHUB_MATRIX = [
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.txt",
    "https://raw.githubusercontent.com/iptvjs/iptv-api/main/output/user_result.txt",
    "https://raw.githubusercontent.com/cymz6/AutoIPTV-Hotel/main/lives.txt",
    "https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/output/result.txt",
    "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/hd.txt",
    "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv4.m3u",
]

MAX_SAME_IP_PER_CHANNEL = 4


# =========================================================================
# 清晰度分级计算器 (用于“越清晰越靠前，同清晰度按速度排序”)
# =========================================================================

def get_resolution_tier(width: int, height: int) -> Tuple[int, int, str]:
    """
    返回 (清晰度主档位, 像素面积, 分辨率标准文本):
      - Tier 4: 4K 超高清 (3840x2160 等, height >= 2000 或 width >= 3800)
      - Tier 3: 1920x1080 全高清 (height >= 1000 或 width >= 1900)
      - Tier 2: 1280x720 高清 (height >= 700 或 width >= 1200)
      - Tier 1: 720x576 等标清 (height >= 360, width >= 640)
      - Tier 0: 未知或过低分辨率
    排序时严格按：(tier, width * height, speed_kbs) 降序排列！
    确保 1920x1080 且速度快的排在最前面，排完 1080P 后再排 720P / 720x576 且速度快的！
    """
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
    """
    排序键生成函数：
    item = (url, speed_kbs, ttfb_ms, width, height, vtag)
    1. 优先比较清晰度档位 tier (4K > 1920x1080 > 1280x720 > 720x576)
    2. 同档位比较像素面积 (width * height)
    3. 同分辨率严格按实测下载速度 speed_kbs 由高到低排序
    """
    url, speed_kbs, ttfb_ms, width, height, vtag = item
    tier, pixels, _ = get_resolution_tier(width, height)
    return (tier, pixels, round(speed_kbs, 2))


# =========================================================================
# 第一步：精准搜索首页 (Page 1, 20条) + 过滤无预览结果
# =========================================================================

def get_current_month_str() -> str:
    now = datetime.datetime.now()
    return now.strftime("%Y-%m")


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
    if html_url and "github.com/" in html_url and "/blob/" in html_url:
        return html_url.replace("https://github.com/", "https://raw.githubusercontent.com/").replace("/blob/", "/")
    repo_full_name = item.get("repository", {}).get("full_name", "")
    file_path = item.get("path", "")
    if repo_full_name and file_path:
        return f"https://raw.githubusercontent.com/{repo_full_name}/main/{file_path}"
    return ""


def search_github_first_page_with_preview(
    query: str,
    seen_blob_shas: Set[str],
    per_page: int = 20
) -> List[Tuple[str, str, str]]:
    headers = get_github_search_headers()
    valid_results: List[Tuple[str, str, str]] = []
    seen_raws: Set[str] = set()

    encoded_q = urllib.parse.quote(query)
    api_url = f"https://api.github.com/search/code?q={encoded_q}&per_page={per_page}&page=1"

    try:
        resp = requests.get(api_url, headers=headers, timeout=12)
        if resp.status_code == 200:
            items = resp.json().get("items", [])
            print(f"  [*] GitHub 首页返回原始结果数: {len(items)} 条 (上限 {per_page} 条)", flush=True)
            for idx, item in enumerate(items, 1):
                repo_name = item.get("repository", {}).get("full_name", "")
                file_path = item.get("path", "")
                blob_sha = item.get("sha", "")
                display_name = f"{repo_name} · {file_path}"

                text_matches = item.get("text_matches", [])
                fragments = [tm.get("fragment", "").strip() for tm in text_matches if tm.get("fragment", "").strip()]
                combined_preview = "\n".join(fragments)

                if not combined_preview or ("http" not in combined_preview.lower() and "cctv" not in combined_preview.lower()):
                    print(f"  [跳过 {idx}/{len(items)}] {display_name} -> 无代码预览片段，不打开", flush=True)
                    continue

                if blob_sha and blob_sha in seen_blob_shas:
                    print(f"  [跳过 {idx}/{len(items)}] {display_name} -> 克隆重复文件，不打开", flush=True)
                    continue

                if blob_sha:
                    seen_blob_shas.add(blob_sha)

                raw_url = convert_github_item_to_raw_url(item)
                if raw_url and raw_url not in seen_raws:
                    seen_raws.add(raw_url)
                    preview_first_line = combined_preview.splitlines()[0][:65] if combined_preview else ""
                    valid_results.append((display_name, raw_url, preview_first_line))
        elif resp.status_code in (403, 429):
            print(f"  [!] GitHub Search API 触发限流 ({resp.status_code})，请配置 GITHUB_TOKEN。", flush=True)
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
    print(f"🚀 【第一步】精准搜索 GitHub 首页直播源 (目标月份: {curr_month})", flush=True)
    print("=" * 76, flush=True)

    exact_queries = [
        f"更新 {curr_month} CCTV-6,http",
        f"更新 {curr_month} CCTV6,http",
    ]

    raw_extracted_records: List[Tuple[str, str]] = []
    visited_raw_files: Set[str] = set()
    seen_blob_shas: Set[str] = set()

    for q in exact_queries:
        print(f"\n[*] 正在搜索首页关键词: 「{q}」...", flush=True)
        search_items = search_github_first_page_with_preview(q, seen_blob_shas=seen_blob_shas, per_page=20)
        if not search_items:
            continue

        for idx, (display_name, raw_url, preview_snippet) in enumerate(search_items, 1):
            if raw_url in visited_raw_files:
                continue
            visited_raw_files.add(raw_url)
            print(f"  -> [{idx}/{len(search_items)}] 打开有预览结果: {display_name}", flush=True)

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
            print(f"     [✓] 匹配目标频道记录: {matched_in_file} 条", flush=True)

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

    print(f"\n[✓] 第一步完成：共提取记录 {len(raw_extracted_records)} 条。", flush=True)
    return raw_extracted_records


# =========================================================================
# 第二步：清洗提取结果
# =========================================================================

def step2_clean_and_deduplicate(
    raw_records: List[Tuple[str, str]],
    target_channels: Set[str]
) -> Dict[str, List[str]]:
    print("\n" + "=" * 76, flush=True)
    print("🧹 【第二步】清洗提取结果 (剔除 $ 尾缀、拦截假源、HTTP 去重)", flush=True)
    print("=" * 76, flush=True)

    cleaned_pool: Dict[str, List[str]] = {ch: [] for ch in target_channels}
    seen_urls_global: Set[str] = set()
    total_cleaned = 0
    removed_suffix_count = 0
    blocked_junk_count = 0

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

        if re.search(r'http://\[[0-9a-fA-F:]+\]', clean_url):
            continue

        std_ch = normalize_channel_name(raw_ch, target_channels)
        if std_ch not in target_channels:
            continue

        norm_u = clean_url.rstrip("/")
        if norm_u not in seen_urls_global:
            seen_urls_global.add(norm_u)
            cleaned_pool[std_ch].append(clean_url)
            total_cleaned += 1

    print(f"[✓] 第二步完成：剩余唯一候选源 {total_cleaned} 条。", flush=True)
    return cleaned_pool


# =========================================================================
# 第三步 & 第五步共用：国内 TVBox 仿真 + ffprobe 分辨率检测
# =========================================================================

def check_hls_stream_security(m3u8_text: str, base_url: str) -> Tuple[bool, str]:
    key_match = re.search(r'#EXT-X-KEY:METHOD=([A-Z0-9\-]+).*?URI="([^"]+)"', m3u8_text)
    if key_match:
        method = key_match.group(1).upper()
        if method != "NONE":
            key_uri = key_match.group(2)
            full_key_url = urllib.parse.urljoin(base_url, key_uri)
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
            return False, 0, 0, "无视频画面轨"

        parts = out.splitlines()[0].strip().split("x")
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
        return False, 0, 0, "视频解码探测超时"


def evaluate_domestic_ip_affinity(url: str, ttfb_ms: float) -> float:
    lower_u = url.lower()
    if any(proxy_kw in lower_u for proxy_kw in ("cors.isteed.cc", "ghproxy.", "workers.dev", "vercel.app")):
        return 0.0

    weight = 1.0
    if "/tsfile/live/" in lower_u or "chinamobile" in lower_u or "unicom" in lower_u or "telecom" in lower_u:
        weight *= 1.18
    if any(k in lower_u for k in ("/rtp/", "/udp/")):
        weight *= 0.80

    if ttfb_ms < 350:
        weight *= 1.12
    elif ttfb_ms > 1600:
        weight *= 0.82

    return weight


def test_single_stream_tvbox(channel_name: str, stream_url: str) -> Tuple[str, str, float, float, int, int, bool, str]:
    clean_url = clean_stream_url(stream_url)
    if not clean_url:
        return channel_name, stream_url, 0.0, 9999.0, 0, 0, False, "黑名单拦截"

    req_start = time.time()
    try:
        r = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
        if r.status_code != 200:
            r.close()
            return channel_name, clean_url, 0.0, 9999.0, 0, 0, False, f"HTTP {r.status_code}"

        content_type = r.headers.get("Content-Type", "").lower()
        if any(bad_ct in content_type for bad_ct in ("audio/", "shockwave-flash", "text/html", "image/", "application/json")):
            r.close()
            return channel_name, clean_url, 0.0, 9999.0, 0, 0, False, "非视频类型"

        first_chunk = next(r.iter_content(chunk_size=1024), b"")
        ttfb_ms = (time.time() - req_start) * 1000.0
        r.close()

        if not first_chunk:
            return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "空响应"

        affinity_weight = evaluate_domestic_ip_affinity(clean_url, ttfb_ms)
        if affinity_weight <= 0.0:
            return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "境外代理拦截"

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
                    return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "录播切片"

                sec_ok, sec_msg = check_hls_stream_security(full_m3u8, current_m3u8_url)
                if not sec_ok:
                    return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, sec_msg

                segments = [
                    urllib.parse.urljoin(current_m3u8_url, line.strip())
                    for line in full_m3u8.splitlines()
                    if line.strip() and not line.strip().startswith("#")
                ]
                if not segments:
                    return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "M3U8无切片"
                ts_urls = segments[-2:] if len(segments) >= 2 else segments[:1]
            except Exception:
                return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "M3U8解析失败"
        else:
            ts_urls = [clean_url]

        download_start = time.time()
        bytes_received = 0
        media_buffer = bytearray()
        segments_ok = 0

        for target_ts in ts_urls:
            try:
                r_ts = requests.get(target_ts, headers=HEADERS, timeout=3.2, stream=True)
                if r_ts.status_code == 200:
                    seg_bytes = 0
                    for chunk in r_ts.iter_content(chunk_size=32 * 1024):
                        if not chunk:
                            break
                        bytes_received += len(chunk)
                        seg_bytes += len(chunk)
                        if len(media_buffer) < 256 * 1024:
                            media_buffer.extend(chunk)
                        if time.time() - download_start >= 2.8 or bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                            break
                    if seg_bytes >= 32 * 1024:
                        segments_ok += 1
                r_ts.close()
            except Exception:
                break

        if bytes_received < 128 * 1024 or segments_ok == 0:
            return channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "数据量不足"

        raw_bytes = bytes(media_buffer)
        has_video, width, height, video_tag = verify_video_stream_with_ffprobe(raw_bytes)
        if not has_video:
            return channel_name, clean_url, 0.0, ttfb_ms, width, height, False, video_tag

        if (width == 0 or height == 0) and m3u8_res_hint != (0, 0):
            width, height = m3u8_res_hint
            video_tag = f"{width}x{height}"

        duration = max(time.time() - download_start, 0.08)
        effective_speed_kbs = ((bytes_received / 1024.0) / duration) * affinity_weight

        return channel_name, clean_url, effective_speed_kbs, ttfb_ms, width, height, True, video_tag
    except Exception:
        return channel_name, clean_url, 0.0, 9999.0, 0, 0, False, "连接超时"


def batch_evaluate_streams(
    pool: Dict[str, List[str]],
    min_speed: float,
    stage_title: str
) -> Dict[str, List[Tuple[str, float, float, int, int, str]]]:
    tasks: List[Tuple[str, str]] = []
    for ch, urls in pool.items():
        for u in urls:
            tasks.append((ch, u))

    print("\n" + "=" * 76, flush=True)
    print(f"⚡ {stage_title} (待测总数: {len(tasks)} 条)", flush=True)
    print("=" * 76, flush=True)

    qualified: Dict[str, List[Tuple[str, float, float, int, int, str]]] = defaultdict(list)
    if not tasks:
        return qualified

    tested = 0
    passed = 0
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(test_single_stream_tvbox, ch, u) for ch, u in tasks]
        for fut in as_completed(futures):
            tested += 1
            ch, url, spd, ttfb, width, height, ok, vtag = fut.result()
            if ok and spd >= min_speed:
                passed += 1
                qualified[ch].append((url, spd, ttfb, width, height, vtag))

            if tested % 40 == 0 or tested == len(tasks):
                print(f"  [测速进度] {tested}/{len(tasks)} | 达标: {passed} 条", flush=True)

    for ch in qualified:
        qualified[ch].sort(key=stream_sort_key, reverse=True)

    return qualified


# =========================================================================
# 第四步：央卫视双向智能裂变
# =========================================================================

def reverse_search_github_ip(ip_port: str) -> List[Tuple[str, str]]:
    discovered: List[Tuple[str, str]] = []
    if not ip_port:
        return discovered
    seen_shas: Set[str] = set()
    search_items = search_github_first_page_with_preview(f'"{ip_port}"', seen_blob_shas=seen_shas, per_page=5)
    for _, raw_url, _ in search_items:
        content = fetch_file_content(raw_url)
        for raw_ch, raw_u in extract_raw_entries_from_text(content):
            if ip_port in raw_u:
                discovered.append((raw_ch, raw_u))
    return discovered


def step4_fission_high_speed_sources(
    step3_qualified: Dict[str, List[Tuple[str, float, float, int, int, str]]],
    already_tested_urls: Set[str],
    target_channels: Set[str]
) -> Dict[str, List[str]]:
    print("\n" + "=" * 76, flush=True)
    print("🧬 【第四步】央卫视双向智能裂变补齐", flush=True)
    print("=" * 76, flush=True)

    fission_candidates: Dict[str, List[str]] = {ch: [] for ch in target_channels}
    fission_seen: Set[str] = {u.rstrip("/") for u in already_tested_urls}

    all_high_speed_seeds: List[Tuple[str, str, float, int, int]] = []
    for ch, items in step3_qualified.items():
        for url, spd, ttfb, w, h, vtag in items:
            all_high_speed_seeds.append((ch, url, spd, w, h))

    all_high_speed_seeds.sort(key=lambda x: (get_resolution_tier(x[3], x[4])[0], x[2]), reverse=True)

    cctv_added = 0
    sat_added = 0
    for seed_ch, seed_url, _, _, _ in all_high_speed_seeds:
        derived_list = extrapolate_channels_from_seed(seed_url, seed_ch)
        for derived_ch, new_url in derived_list:
            clean_u = clean_stream_url(new_url)
            std_ch = normalize_channel_name(derived_ch, target_channels)
            norm_u = clean_u.rstrip("/") if clean_u else ""
            if std_ch in target_channels and clean_u and norm_u not in fission_seen:
                fission_seen.add(norm_u)
                fission_candidates[std_ch].append(clean_u)
                if std_ch.startswith("CCTV"):
                    cctv_added += 1
                else:
                    sat_added += 1

    print(f"[✓] 第四步裂变完成：新增候选源 {cctv_added + sat_added} 条", flush=True)
    return fission_candidates


# =========================================================================
# 第五步：合并、排序与导出
# =========================================================================

def step5_merge_sort_and_export(
    step3_qualified: Dict[str, List[Tuple[str, float, float, int, int, str]]],
    step5_qualified: Dict[str, List[Tuple[str, float, float, int, int, str]]],
    user_struct: Dict[str, List[str]],
    norm_to_display: Dict[str, str],
    target_channels: Set[str]
):
    print("\n" + "=" * 86, flush=True)
    print("🏆 【第五步】清晰度优先排序输出 (1080P优先 -> 速度降序)", flush=True)
    print("=" * 86, flush=True)

    txt_lines: List[str] = []
    m3u_lines: List[str] = ['#EXTM3U x-tvg-url="https://live.fanmingming.com/e.xml"']

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
                for url, spd, ttfb, w, h, vtag in final_streams:
                    group_txt.append(f"{display_ch},{url}")
                    m3u_lines.append(f'#EXTINF:-1 tvg-name="{std_ch}" group-title="{group_name}",{display_ch}')
                    m3u_lines.append(url)

        if group_has_items:
            txt_lines.extend(group_txt)

    with open(OUTPUT_LIVE_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines) + "\n")

    with open(OUTPUT_LIVE_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(m3u_lines) + "\n")

    print(f"[✓] 导出完成：覆盖频道 {total_channels_ready} 个，有效源 {total_streams_ready} 条。", flush=True)


def save_intermediate_cleaned_pool(
    cleaned_pool: Dict[str, List[str]],
    user_struct: Dict[str, List[str]],
    target_channels: Set[str]
):
    txt_lines: List[str] = []
    m3u_lines: List[str] = ['#EXTM3U x-tvg-url="https://live.fanmingming.com/e.xml"']
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


# =========================================================================
# 独立在线测速模式 (--mode speedtest)
# =========================================================================

def run_standalone_speedtest_mode(input_file: str, min_speed: float = MIN_SPEED_KBS):
    print("=" * 82, flush=True)
    print(f"⚡ 【在线测速模式】读取输入文件: {input_file} (阈值: {min_speed:.1f} KB/s)", flush=True)
    print("=" * 82, flush=True)

    if not os.path.exists(input_file):
        print(f"[!] 未找到输入文件 {input_file}", flush=True)
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

    qualified = batch_evaluate_streams(
        pool,
        min_speed=min_speed,
        stage_title="【在线去重测速】国内 TVBox 播放仿真"
    )

    out_txt_lines = ["测速优选频道,#genre#"]
    out_m3u_lines = ['#EXTM3U x-tvg-url="https://live.fanmingming.com/e.xml"']

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

    print(f"[✓] 在线去重测速完成！结果已写入 {OUTPUT_LIVE_TXT}", flush=True)


def run_five_step_workflow(min_speed: float = MIN_SPEED_KBS):
    total_start = time.time()

    user_struct = load_user_channel_structure()
    target_channels, norm_to_display = build_channel_Lookup_map(user_struct)

    print(f"[*] 当前目标分类数: {len(user_struct)} 个 | 合计目标频道数: {len(target_channels)} 个", flush=True)

    raw_records = step1_search_and_extract(target_channels)
    cleaned_pool = step2_clean_and_deduplicate(raw_records, target_channels)
    save_intermediate_cleaned_pool(cleaned_pool, user_struct, target_channels)

    tested_url_set: Set[str] = set()
    for urls in cleaned_pool.values():
        tested_url_set.update(urls)

    step3_qualified = batch_evaluate_streams(
        cleaned_pool,
        min_speed=min_speed,
        stage_title="【第三步】初次测速与视频分辨率校验"
    )

    fission_pool = step4_fission_high_speed_sources(
        step3_qualified,
        already_tested_urls=tested_url_set,
        target_channels=target_channels
    )

    step5_qualified = batch_evaluate_streams(
        fission_pool,
        min_speed=min_speed,
        stage_title="【第五步(1/2)】对裂变新地址二次测速"
    )

    step5_merge_sort_and_export(step3_qualified, step5_qualified, user_struct, norm_to_display, target_channels)

    elapsed = time.time() - total_start
    print(f"\n[🏁] 全流程执行完毕，总耗时: {elapsed:.1f} 秒", flush=True)


def main():
    parser = argparse.ArgumentParser(description="IPTV 自动化五步搜索裂变测速引擎")
    parser.add_argument("min_speed_pos", nargs="?", type=float, default=None, help="最低测速门槛 (KB/s)")
    parser.add_argument("--mode", choices=["iptv", "speedtest"], default="iptv", help="运行模式")
    parser.add_argument("--input", default=RAW_SOURCES_TXT, help="输入文件")
    parser.add_argument("--min-speed", type=float, default=MIN_SPEED_KBS, help="速度阈值")

    args, _ = parser.parse_known_args()
    effective_min_speed = args.min_speed_pos if args.min_speed_pos is not None else args.min_speed

    if args.mode == "speedtest":
        run_standalone_speedtest_mode(args.input, min_speed=effective_min_speed)
    else:
        run_five_step_workflow(min_speed=effective_min_speed)


if __name__ == "__main__":
    main()
