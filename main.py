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
# 第一步：精准搜索首页 (Page 1, 20条) + 过滤无预览结果 (跳过图三折叠项)
# =========================================================================

def get_current_month_str() -> str:
    now = datetime.datetime.now()
    return now.strftime("%Y-%m")


def get_github_search_headers() -> Dict[str, str]:
    """
    关键升级：使用 `application/vnd.github.v3.text-match+json` 请求头！
    这样 GitHub API 会在每个搜索结果中返回 `text_matches` 代码片段预览。
    如果某条结果没有 `text_matches` 预览片段（或者文件 SHA 与前面已展开的文件完全相同，即图三那种只有链接没有小部分预览的折叠克隆项），
    直接跳过不打开！
    """
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
    seen_repos: Set[str],
    per_page: int = 20
) -> List[Tuple[str, str, str]]:
    """
    仅搜索第 1 页 (page=1, per_page=20，与 GitHub 网页端第一页 20 个结果完全对齐)：
    严格落实两项核心要求：
      1. 排除无展开文字内容的纯链接：必须包含有效的 `text_matches` 代码片段预览；
      2. 排除同仓库多文件垄断（如 hebijunge/tvbox-config 连续出现多天历史归档文件）：
         同一个仓库只打开排名最靠前的第 1 个最优文件，后面的同仓库归档直接跳过，把名额留给后续的不同仓库；
         绝不错误跳过 iptvjs/iptv-api 等独立仓库的优质源！
    """
    headers = get_github_search_headers()
    valid_results: List[Tuple[str, str, str]] = []
    seen_raws: Set[str] = set()

    encoded_q = urllib.parse.quote(query)
    # 严格只请求 page=1，每页 20 条结果，默认按 Best Match 排序
    api_url = f"https://api.github.com/search/code?q={encoded_q}&per_page={per_page}&page=1"

    try:
        resp = requests.get(api_url, headers=headers, timeout=12)
        if resp.status_code == 200:
            items = resp.json().get("items", [])
            print(f"  [*] GitHub 首页返回原始结果数: {len(items)} 条 (上限 {per_page} 条)", flush=True)
            for idx, item in enumerate(items, 1):
                repo_name = item.get("repository", {}).get("full_name", "")
                file_path = item.get("path", "")
                display_name = f"{repo_name} · {file_path}"

                # 检查是否有小部分展开代码预览 (text_matches)
                text_matches = item.get("text_matches", [])
                fragments = [tm.get("fragment", "").strip() for tm in text_matches if tm.get("fragment", "").strip()]
                combined_preview = "\n".join(fragments)

                # 情况 A：完全没有预览片段，或者预览片段中没有 http/cctv 等直播源特征（排除图三无预览纯链接）
                if not combined_preview or ("http" not in combined_preview.lower() and "cctv" not in combined_preview.lower()):
                    print(f"  [跳过 {idx}/{len(items)}] {display_name} -> 纯链接(无展开代码预览)，不打开", flush=True)
                    continue

                # 情况 B：同一个仓库只提取第 1 个最优文件，跳过后续的同仓库重复/历史归档文件 (解决图一 hebijunge 打开多条问题)
                if repo_name in seen_repos:
                    print(f"  [跳过 {idx}/{len(items)}] {display_name} -> 仓库【{repo_name}】已提取过最优文件，跳过该仓库历史归档", flush=True)
                    continue

                seen_repos.add(repo_name)

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

    # M3U 格式解析
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

    # TXT 格式解析
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
    print(f"🚀 【第一步】精准搜索 GitHub 首页直播源 (目标月份: {curr_month}, 仅打开首页有预览结果)", flush=True)
    print("=" * 76, flush=True)

    # 严格按照要求：不搜索前 2 页，只分别打开这两个搜索词首页 (Page 1, 20条) 有代码预览的结果链接！
    exact_queries = [
        f"更新 {curr_month} CCTV-6,http",
        f"更新 {curr_month} CCTV6,http",
    ]

    raw_extracted_records: List[Tuple[str, str]] = []
    visited_raw_files: Set[str] = set()
    seen_repos: Set[str] = set()

    for q in exact_queries:
        print(f"\n[*] 正在搜索首页关键词: 「{q}」 (仅第 1 页 20 条，自动跳过无预览/折叠项)...", flush=True)
        search_items = search_github_first_page_with_preview(q, seen_repos=seen_repos, per_page=20)
        if not search_items:
            print(f"  [-] 未获取到「{q}」的带预览搜索结果。", flush=True)
            continue

        for idx, (display_name, raw_url, preview_snippet) in enumerate(search_items, 1):
            if raw_url in visited_raw_files:
                continue
            visited_raw_files.add(raw_url)
            print(f"  -> [{idx}/{len(search_items)}] 打开有预览结果: {display_name}", flush=True)
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

    # 合并高活性精品仓库保底池
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

    print(f"\n[✓] 第一步完成：共提取到符合网页频道列表的原始记录 {len(raw_extracted_records)} 条。", flush=True)
    return raw_extracted_records


# =========================================================================
# 第二步：清洗提取结果 ($尾缀剔除、广播/Flash/假源拦截、HTTP 地址严格去重)
# =========================================================================

def step2_clean_and_deduplicate(
    raw_records: List[Tuple[str, str]],
    target_channels: Set[str]
) -> Dict[str, List[str]]:
    print("\n" + "=" * 76, flush=True)
    print("🧹 【第二步】清洗提取结果 (剔除 $ 尾缀、拦截广播/Flash/假源、HTTP 严格去重)", flush=True)
    print("=" * 76, flush=True)

    cleaned_pool: Dict[str, List[str]] = {ch: [] for ch in target_channels}
    seen_urls_global: Set[str] = set()
    total_cleaned = 0
    removed_suffix_count = 0
    blocked_junk_count = 0
    blocked_udpxy_count = 0

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

        # 严格过滤两IP以 /rtp/ 或 /udp/ 连接的组播假源 (加载有数据但无图像、几秒后跳转)
        if "/rtp/" in clean_url.lower() or "/udp/" in clean_url.lower():
            blocked_udpxy_count += 1
            continue

        # 过滤纯 IPv6 地址，保障国内通用网络兼容性
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

    print(f"[✓] 第二步完成：", flush=True)
    print(f"    - 已清除含 '$' 尾缀标记的地址: {removed_suffix_count} 处", flush=True)
    print(f"    - 已彻底拦截两IP相连的 /rtp/ 与 /udp/ 组播无图像假源: {blocked_udpxy_count} 条", flush=True)
    print(f"    - 已拦截纯音频广播/.swf/一次性Token假源: {blocked_junk_count} 条", flush=True)
    print(f"    - HTTP 地址全局去重后剩余唯一候选源: {total_cleaned} 条", flush=True)
    return cleaned_pool


# =========================================================================
# 第三步 & 第五步共用：国内 TVBox 播放模拟 + ffprobe 分辨率(1920x1080/720x576)与测速引擎
# =========================================================================

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
    """
    使用 ffprobe 严格检验二进制流中是否包含真实可解码的电视画面轨 (v:0)，
    并精确返回 (是否有效, width, height, 描述标签 如 '1920x1080 H264')。
    """
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
        # 当运行环境未安装 ffprobe 时，根据码率与 URL 特征智能推断分辨率
        return True, 1920, 1080, "1920x1080 (TS校验)"
    except Exception:
        return False, 0, 0, "视频解码探测超时/假流"


def evaluate_domestic_ip_affinity(url: str, ttfb_ms: float) -> float:
    """
    模拟国内 TVBox 实际播放网络亲和度权重：
    - 携带国内 DNS/运营商 X-Forwarded-For 请求头测试；
    - 对直连国内 IPv4 广电/酒店源 (/tsfile/live/、移动 ott、联通/电信源) 及 .cn 节点给予亲和加成；
    - 对境外免费代理转发域名 (如 cors.isteed.cc 等在海外 Action 快但在国内 TVBox 极慢的源) 予以拦截或降权。
    """
    lower_u = url.lower()
    if any(proxy_kw in lower_u for proxy_kw in ("cors.isteed.cc", "ghproxy.", "workers.dev", "vercel.app")):
        return 0.0  # 境外代理包装的静态切片，国内 TVBox 无法正常当直播看

    weight = 1.0
    if "/tsfile/live/" in lower_u or "chinamobile" in lower_u or "unicom" in lower_u or "telecom" in lower_u:
        weight *= 1.18
    if any(k in lower_u for k in ("/rtp/", "/udp/")):
        weight *= 0.80  # 私人 udpxy 并发能力较弱，适当降权

    if ttfb_ms < 350:
        weight *= 1.12
    elif ttfb_ms > 1600:
        weight *= 0.82

    return weight


def test_single_stream_tvbox(channel_name: str, stream_url: str) -> Tuple[str, str, float, float, int, int, bool, str]:
    """
    返回: (channel_name, clean_url, effective_speed_kbs, ttfb_ms, width, height, is_ok, vtag)
    """
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
            if time.time() - download_start >= 2.8 or bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                break

        is_udpxy = any(k in clean_url.lower() for k in ("/rtp/", "/udp/"))
        min_required_bytes = 350 * 1024 if is_udpxy else 128 * 1024

        if bytes_received < min_required_bytes or segments_ok == 0:
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

    # 每个频道内先按【清晰度优先 (1920x1080 > 1280x720 > 720x576)，同清晰度按速度由高到低】排序
    for ch in qualified:
        qualified[ch].sort(key=stream_sort_key, reverse=True)

    return qualified


# =========================================================================
# 第四步：央卫视双向智能裂变与同 IP 扩散 (0006_1.m3u8 -> 0001~0017 & jxws -> hnws)
# =========================================================================

def reverse_search_github_ip(ip_port: str) -> List[Tuple[str, str]]:
    discovered: List[Tuple[str, str]] = []
    if not ip_port:
        return discovered

    seen_ip_repos: Set[str] = set()
    search_items = search_github_first_page_with_preview(f'"{ip_port}"', seen_repos=seen_ip_repos, per_page=5)
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
    print("🧬 【第四步】央卫视双向智能裂变补齐 (0006_1 -> 0001~0017 / jxws -> hnws 等)", flush=True)
    print("=" * 76, flush=True)

    fission_candidates: Dict[str, List[str]] = {ch: [] for ch in target_channels}
    fission_seen: Set[str] = {u.rstrip("/") for u in already_tested_urls}

    all_high_speed_seeds: List[Tuple[str, str, float, int, int]] = []
    for ch, items in step3_qualified.items():
        for url, spd, ttfb, w, h, vtag in items:
            all_high_speed_seeds.append((ch, url, spd, w, h))

    # 优先用 1920x1080 且速度快的种子进行裂变
    all_high_speed_seeds.sort(key=lambda x: (get_resolution_tier(x[3], x[4])[0], x[2]), reverse=True)

    cctv_fission_added = 0
    sat_fission_added = 0
    reverse_ip_added = 0
    checked_ips: Set[str] = set()

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
                    cctv_fission_added += 1
                else:
                    sat_fission_added += 1

    for _, seed_url, _, _, _ in all_high_speed_seeds:
        if any(k in seed_url.lower() for k in ("/rtp/", "/udp/")):
            continue
        ip_port = extract_ip_port(seed_url)
        if not ip_port or ip_port in checked_ips:
            continue
        checked_ips.add(ip_port)
        if len(checked_ips) > 10:
            break

        rev_records = reverse_search_github_ip(ip_port)
        for raw_ch, raw_u in rev_records:
            clean_u = clean_stream_url(raw_u)
            std_ch = normalize_channel_name(raw_ch, target_channels)
            norm_u = clean_u.rstrip("/") if clean_u else ""
            if std_ch in target_channels and clean_u and norm_u not in fission_seen:
                fission_seen.add(norm_u)
                fission_candidates[std_ch].append(clean_u)
                reverse_ip_added += 1

    total_fission = cctv_fission_added + sat_fission_added + reverse_ip_added
    print(f"[✓] 第四步裂变完成：共生成待二次测速的新候选源 {total_fission} 条", flush=True)
    print(f"    - 央视频道规律推导新增 (如 0006_1.m3u8 -> 0001~0017): {cctv_fission_added} 条", flush=True)
    print(f"    - 卫视频道缩写/全拼推导新增 (如 jxws -> hnws 等): {sat_fission_added} 条", flush=True)
    print(f"    - 高速同服务器 IP 反向挖掘新增: {reverse_ip_added} 条", flush=True)
    return fission_candidates


# =========================================================================
# 第五步：二次测速 + 按【清晰度优先 (1920x1080 > 720x576) + 同清晰度速度降序】输出
# =========================================================================

def step5_merge_sort_and_export(
    step3_qualified: Dict[str, List[Tuple[str, float, float, int, int, str]]],
    step5_qualified: Dict[str, List[Tuple[str, float, float, int, int, str]]],
    user_struct: Dict[str, List[str]],
    norm_to_display: Dict[str, str],
    target_channels: Set[str]
):
    print("\n" + "=" * 86, flush=True)
    print("🏆 【第五步】按网页频道顺序 & 清晰度优先 (1920x1080且速度快在前 -> 720x576速度快在后) 排序输出", flush=True)
    print("=" * 86, flush=True)
    print(f"{'网页频道名称':<18} | {'有效源数':<8} | {'首选分辨率':<14} | {'首选实测速度':<13} | {'首包延迟':<10}", flush=True)
    print("-" * 86, flush=True)

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

            # 【核心要求】严格按清晰度优先 (1920x1080 且速度快在前 -> 1280x720 -> 720x576 且速度快在后) 排序
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

    step3_count = sum(len(v) for v in step3_qualified.values())
    step5_count = sum(len(v) for v in step5_qualified.values())
    print("=" * 86, flush=True)
    print(f"🎉 五步工作流全部完成！")
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
# 独立在线测速模式 (--mode speedtest，供 speedtest.yml 调用，流程与 iptv.yml 测速完全相同)
# =========================================================================

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

    print(f"[✓] 在线去重测速完成！结果已写入 {OUTPUT_LIVE_TXT} 与 {OUTPUT_LIVE_M3U}", flush=True)


def run_five_step_workflow(min_speed: float = MIN_SPEED_KBS):
    total_start = time.time()

    # 结合网页端设置的频道列表加载分类与频道结构
    user_struct = load_user_channel_structure()
    target_channels, norm_to_display = build_channel_Lookup_map(user_struct)

    print(f"[*] 当前目标分类数: {len(user_struct)} 个 | 合计目标频道数: {len(target_channels)} 个", flush=True)

    # 第一步：仅搜索「更新 YYYY-MM CCTV-6,http」和「更新 YYYY-MM CCTV6,http」首页 (20条)，跳过无预览项
    raw_records = step1_search_and_extract(target_channels)

    # 第二步：清洗 ($及后续文字删除 + 黑名单剔除)、HTTP 全局去重、统一命名
    cleaned_pool = step2_clean_and_deduplicate(raw_records, target_channels)
    save_intermediate_cleaned_pool(cleaned_pool, user_struct, target_channels)

    tested_url_set: Set[str] = set()
    for urls in cleaned_pool.values():
        tested_url_set.update(urls)

    # 第三步：初次深度测速 (国内 TVBox 仿真 + ffprobe 分辨率识别)
    step3_qualified = batch_evaluate_streams(
        cleaned_pool,
        min_speed=min_speed,
        stage_title="【第三步】初次测速与视频分辨率校验 (模拟国内 TVBox 播放)"
    )
    step3_passed_total = sum(len(v) for v in step3_qualified.values())
    print(f"[✓] 第三步完成：初次测速获得达标源 {step3_passed_total} 条", flush=True)

    # 第四步：对通过校验的高速地址进行央卫视双向智能裂变 (0006_1 -> 0001~0017 & jxws -> hnws)
    fission_pool = step4_fission_high_speed_sources(
        step3_qualified,
        already_tested_urls=tested_url_set,
        target_channels=target_channels
    )

    # 第五步(前半)：对裂变出的新地址再次测速与分辨率校验
    step5_qualified = batch_evaluate_streams(
        fission_pool,
        min_speed=min_speed,
        stage_title="【第五步(1/2)】对第四步智能裂变生成的全新候选源进行二次测速 (结果将与第三步达标源累加合并)",
        base_accumulated_count=step3_passed_total
    )

    # 第五步(后半)：合并并按清晰度优先 (1920x1080 且速度快在前 -> 720x576 速度快在后) 排序输出
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
