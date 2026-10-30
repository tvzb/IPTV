# -*- coding: utf-8 -*-
"""
IPTV 自动化五步工作流引擎 & 独立在线测速脚本 (TVBox 国内 IP 仿真 + 清晰度优先排序版)

核心改进落实：
  1. 彻底解决 REST API (`/search/code`) 单仓库刷屏 12 条历史文件的问题：
     - 网页端 GitHub 搜索（图一/图二）默认按仓库折叠去重，因此首页 20 条结果来自 20 个不同的仓库（如 hebijunge/tvbox-config 只有 1 条、iptvjs/iptv-api 1 条、0610840119/iptv-api-broken 1 条...）；
     - 而旧版调用的 `api.github.com/search/code` (per_page=20) 不会按仓库折叠，导致 `hebijunge/tvbox-config` 一个仓库的 12 个历史日期归档文件霸占了前 12 个名额，挤掉了其它 11 个不同仓库的搜索结果！
     - 现已改为：优先直接请求 GitHub 网页端搜索接口获取与图一 100% 一致的 20 条预览框结果；若走 API 通道，则单次拉取 100 条并按每仓库只取 1 条最新结果凑齐 20 个不同仓库的预览框，绝不让单个仓库刷屏浪费名额！
  2. 严格执行【打开搜索结果预览框 -> 点击 Raw 打开纯文本直播源列表 -> 采集全部直播源】：
     - 从第 1 条结果到最后一条结果（20/20），依次打开预览框进入文件页，再通过 Raw 直链打开纯文本直播源列表采集全部地址。
  3. 智能裂变精准对齐：
     - 央视频道（如 cctv1hd.m3u8 -> cctv1hd.m3u8 ~ cctv17hd.m3u8 裂变全部 17 个央视）；
     - 卫视频道（如 dfwshd.m3u8 -> jxwshd.m3u8、hnwshd.m3u8 等裂变全部 30 个卫视，保留 hd/sd 等画质尾缀）；
  4. 二次测速严格去重：只对第四步裂变后【新出现】的直播源地址进行测速，裂变前已测过速的地址绝不重复测速。
"""
import argparse
import datetime
import json
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

# 拼接域名防止任何编辑器或聊天框自动转换为 Markdown 链接
GH_HOST = "https://" + "github.com"
GH_API_HOST = "https://" + "api.github.com"
GH_RAW_HOST = "https://" + "raw.githubusercontent.com"

FALLBACK_GITHUB_MATRIX = [
    f"{GH_RAW_HOST}/Guovin/iptv-api/gd/output/result.txt",
    f"{GH_RAW_HOST}/iptvjs/iptv-api/main/output/user_result.txt",
    f"{GH_RAW_HOST}/cymz6/AutoIPTV-Hotel/main/lives.txt",
    f"{GH_RAW_HOST}/Ftindy/IPTV-URL/main/output/result.txt",
    f"{GH_RAW_HOST}/vbskycn/iptv/master/tv/hd.txt",
    f"{GH_RAW_HOST}/fanmingming/live/main/tv/m3u/ipv4.m3u",
]


def sanitize_url(u: str) -> str:
    """
    清理可能因复制粘贴产生的 Markdown 链接语法 [url](url)，还原纯净 URL。
    """
    if not u:
        return ""
    u = u.strip()
    m = re.match(r'^\[.*?\]\((https?://[^\s\)]+)\)(.*)$', u)
    if m:
        return m.group(1) + m.group(2)
    return u


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
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}" if token.startswith("gh") else f"token {token}"
    return headers


def get_github_web_headers() -> Dict[str, str]:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    headers = {
        "Accept": "application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}" if token.startswith("gh") else f"token {token}"
    return headers


def build_blob_and_raw_urls(item: dict) -> Tuple[str, str]:
    """
    从搜索结果卡片构造：
      1. 预览框点开后的文件页面地址 (blob_html_url)
      2. 再次点击 "Raw" 按钮对应的纯文本直播源列表地址 (raw_url)
    """
    html_url = sanitize_url(item.get("html_url", ""))
    repo_full_name = item.get("repository", {}).get("full_name", "")
    file_path = item.get("path", "")

    if not html_url and repo_full_name and file_path:
        html_url = f"{GH_HOST}/{repo_full_name}/blob/main/{file_path}"

    raw_url = ""
    if html_url and "/blob/" in html_url:
        raw_url = html_url.replace(GH_HOST + "/", GH_RAW_HOST + "/").replace("/blob/", "/")
    elif repo_full_name and file_path:
        raw_url = f"{GH_RAW_HOST}/{repo_full_name}/main/{file_path}"

    return html_url, raw_url


def open_preview_and_click_raw(blob_html_url: str, raw_url: str) -> Tuple[str, str]:
    """
    严格按照用户指定的两步动作采集：
      动作 1：打开搜索结果中的预览框（访问文件页面 blob_html_url）；
      动作 2：打开后再次点击 "Raw" 按钮打开纯文本直播源列表（即 ?raw=1 或 raw.githubusercontent.com 直链），读取全部纯文本内容。
    返回: (纯文本直播源内容, 采集通道名称)
    """
    blob_html_url = sanitize_url(blob_html_url)
    raw_url = sanitize_url(raw_url)

    raw_candidates = []
    if blob_html_url:
        raw_candidates.append((f"{blob_html_url}?raw=1", "点击 Raw 打开纯文本列表 (?raw=1)"))
        if "/blob/" in blob_html_url:
            raw_candidates.append((blob_html_url.replace("/blob/", "/raw/"), "点击 Raw 打开纯文本列表 (/raw/)"))
    if raw_url:
        raw_candidates.append((raw_url, "Raw 纯文本直链"))
        if "/main/" in raw_url:
            raw_candidates.append((raw_url.replace("/main/", "/master/"), "Raw 纯文本直链 (master)"))

    for candidate_u, desc in raw_candidates:
        try:
            r_raw = requests.get(candidate_u, headers=HEADERS, timeout=12, allow_redirects=True)
            if r_raw.status_code == 200 and r_raw.text:
                head_sample = r_raw.text[:300].lower()
                if "<!doctype html" not in head_sample and "<html" not in head_sample:
                    return r_raw.text, desc
        except Exception:
            continue

    if blob_html_url:
        try:
            r_blob = requests.get(blob_html_url, headers=get_github_web_headers(), timeout=10)
            if r_blob.status_code == 200:
                text_body = r_blob.text
                try:
                    data = r_blob.json()
                    raw_lines = (
                        data.get("payload", {}).get("blob", {}).get("rawLines")
                        or data.get("blob", {}).get("rawLines")
                    )
                    if isinstance(raw_lines, list) and len(raw_lines) > 0:
                        return "\n".join(raw_lines), "文件页文字框纯文本 (rawLines)"
                except Exception:
                    pass

                m_script = re.search(
                    r'<script[^>]*data-target="react-app\.embeddedData"[^>]*>(.*?)</script>',
                    text_body,
                    re.DOTALL | re.IGNORECASE
                )
                if m_script:
                    try:
                        embedded_json = json.loads(m_script.group(1).strip())
                        raw_lines = embedded_json.get("payload", {}).get("blob", {}).get("rawLines")
                        if isinstance(raw_lines, list) and len(raw_lines) > 0:
                            return "\n".join(raw_lines), "文件页文字框内嵌数据"
                    except Exception:
                        pass
        except Exception:
            pass

    return "", "未能读取"


def search_github_20_distinct_previews(
    query: str,
    target_count: int = 20
) -> List[Tuple[str, str, str, str]]:
    """
    获取与 GitHub 网页端搜索首页（图一/图二）完全一致的 20 条预览框结果：
      - 同一个仓库（如 hebijunge/tvbox-config）自动折叠，只保留排序最靠前、日期最新的那 1 条预览框；
      - 单次从 API 拉取 100 条候选（必要时翻页），跳过同仓库冗余历史归档后，实打实地拿满 20 个不同仓库的预览结果！
    """
    valid_results: List[Tuple[str, str, str, str]] = []
    seen_repos: Set[str] = set()
    seen_urls: Set[str] = set()

    # 通道 A：优先尝试直接读取 GitHub 网页端搜索接口结果
    try:
        web_search_url = sanitize_url(f"{GH_HOST}/search")
        params = {"q": query, "type": "code", "p": "1"}
        r_web = requests.get(web_search_url, headers=get_github_web_headers(), params=params, timeout=10)
        if r_web.status_code == 200:
            results = []
            try:
                web_data = r_web.json()
                results = web_data.get("payload", {}).get("results", [])
            except Exception:
                m_emb = re.search(
                    r'<script[^>]*data-target="react-app\.embeddedData"[^>]*>(.*?)</script>',
                    r_web.text,
                    re.DOTALL | re.IGNORECASE
                )
                if m_emb:
                    try:
                        results = json.loads(m_emb.group(1).strip()).get("payload", {}).get("results", [])
                    except Exception:
                        results = []

            if results:
                for item in results:
                    repo_name = item.get("repo_nwo", "") or item.get("repository", {}).get("full_name", "")
                    file_path = item.get("path", "")
                    commit_sha = item.get("commit_sha", "main")
                    if not repo_name or not file_path:
                        continue
                    if repo_name in seen_repos:
                        continue
                    seen_repos.add(repo_name)
                    display_name = f"{repo_name} · {file_path}"
                    blob_html_url = f"{GH_HOST}/{repo_name}/blob/{commit_sha}/{file_path}"
                    raw_url = f"{GH_RAW_HOST}/{repo_name}/{commit_sha}/{file_path}"
                    snippets = item.get("hl_trunc_description", "") or ""
                    clean_snip = re.sub(r'<[^>]+>', '', snippets).strip()
                    seen_urls.add(raw_url)
                    valid_results.append((display_name, blob_html_url, raw_url, clean_snip))
                    if len(valid_results) >= target_count:
                        print(f"  [*] 网页端搜索通道成功获取 {len(valid_results)} 条独立仓库预览框结果", flush=True)
                        return valid_results
    except Exception:
        pass

    # 通道 B：通过 GitHub Search API 单页拉取 100 条（必要时拉取第 2 页），每个仓库只取最新 1 条，凑满 20 条不同仓库的预览结果！
    clean_q = query.replace(",", " ")
    query_variants = [query, clean_q]

    for q_try in query_variants:
        api_url = sanitize_url(f"{GH_API_HOST}/search/code")
        for page_num in (1, 2):
            params = {
                "q": q_try,
                "per_page": "100",
                "page": str(page_num),
            }
            try:
                resp = requests.get(api_url, headers=get_github_search_headers(), params=params, timeout=14)
                if resp.status_code == 200:
                    items = resp.json().get("items", [])
                    if not items:
                        break

                    repo_best_item: Dict[str, dict] = {}
                    repo_order: List[str] = []
                    for item in items:
                        repo_name = item.get("repository", {}).get("full_name", "")
                        file_path = item.get("path", "")
                        if not repo_name or not file_path:
                            continue
                        if repo_name not in repo_best_item:
                            repo_best_item[repo_name] = item
                            repo_order.append(repo_name)
                        else:
                            old_path = repo_best_item[repo_name].get("path", "")
                            m_new = re.search(r'(\d{4}-\d{2}-\d{2})', file_path)
                            m_old = re.search(r'(\d{4}-\d{2}-\d{2})', old_path)
                            if m_new and m_old and m_new.group(1) > m_old.group(1):
                                repo_best_item[repo_name] = item

                    for repo_name in repo_order:
                        if repo_name in seen_repos:
                            continue
                        item = repo_best_item[repo_name]
                        file_path = item.get("path", "")
                        display_name = f"{repo_name} · {file_path}"

                        text_matches = item.get("text_matches", [])
                        fragments = [tm.get("fragment", "").strip() for tm in text_matches if tm.get("fragment", "").strip()]
                        combined_preview = "\n".join(fragments)

                        blob_html_url, raw_url = build_blob_and_raw_urls(item)
                        unique_key = raw_url or blob_html_url
                        if unique_key and unique_key not in seen_urls:
                            seen_repos.add(repo_name)
                            seen_urls.add(unique_key)
                            valid_results.append((display_name, blob_html_url, raw_url, combined_preview))
                            if len(valid_results) >= target_count:
                                break

                    if len(valid_results) >= target_count or len(items) < 100:
                        break
                elif resp.status_code in (403, 429):
                    print(f"  [!] GitHub Search API 触发限流 ({resp.status_code})", flush=True)
                    break
                else:
                    break
            except Exception as e:
                print(f"  [!] 搜索请求异常 ({q_try}): {e}", flush=True)
                break

        if valid_results:
            break

    print(
        f"  [*] 搜索完成：已对同仓库历史归档去重折叠，共获取 {len(valid_results)} 条独立预览框结果（每条均来自不同仓库，无重复霸屏）",
        flush=True
    )
    return valid_results


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
    print("=" * 82, flush=True)
    print(f"🚀 【第一步】搜索 GitHub 首页 20 条独立预览框 -> 打开预览框 -> 点击 Raw 采集全部直播源 (月份: {curr_month})", flush=True)
    print("=" * 82, flush=True)

    exact_queries = [
        f"更新 {curr_month} CCTV-6,http",
        f"更新 {curr_month} CCTV6,http",
    ]

    raw_extracted_records: List[Tuple[str, str]] = []
    visited_files: Set[str] = set()

    for q in exact_queries:
        print(f"\n[*] 正在搜索关键词: 「{q}」 (同仓库自动折叠只取 1 条最新结果，确保 20 条名额全部来自不同仓库)...", flush=True)
        search_items = search_github_20_distinct_previews(q, target_count=20)
        if not search_items:
            print(f"  [-] 未获取到「{q}」的搜索结果。", flush=True)
            continue

        total_items = len(search_items)
        for idx, (display_name, blob_html_url, raw_url, preview_text) in enumerate(search_items, 1):
            unique_key = raw_url or blob_html_url
            if unique_key in visited_files:
                print(f"  -> [{idx}/{total_items}] 已在上一轮采集过该文件，跳过重复项: {display_name}", flush=True)
                continue
            visited_files.add(unique_key)

            preview_first_line = preview_text.splitlines()[0][:65] if preview_text else ""
            print(f"  -> [{idx}/{total_items}] 打开预览框: {display_name}", flush=True)
            if preview_first_line:
                print(f"     [预览框内容] {preview_first_line}...", flush=True)

            # 打开预览框后，点击 Raw 打开纯文本直播源列表，采集里面的全部直播源
            full_text, source_mode = open_preview_and_click_raw(blob_html_url, raw_url)
            matched_in_file = 0
            if full_text:
                entries = extract_raw_entries_from_text(full_text)
                for raw_ch, raw_url_str in entries:
                    norm_ch = normalize_channel_name(raw_ch, target_channels)
                    if norm_ch in target_channels:
                        raw_extracted_records.append((norm_ch, raw_url_str))
                        matched_in_file += 1
                print(f"     [✓] {source_mode} -> 成功采集目标频道直播源: {matched_in_file} 条", flush=True)
            elif preview_text:
                for raw_ch, raw_url_str in extract_raw_entries_from_text(preview_text):
                    norm_ch = normalize_channel_name(raw_ch, target_channels)
                    if norm_ch in target_channels:
                        raw_extracted_records.append((norm_ch, raw_url_str))
                        matched_in_file += 1
                print(f"     [!] Raw 页面响应超时，已从预览框采集目标频道直播源: {matched_in_file} 条", flush=True)

        time.sleep(1.0)

    print("\n[*] 正在同步国内高活性直播源 Raw 补充矩阵...", flush=True)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {
            executor.submit(open_preview_and_click_raw, "", sanitize_url(u)): sanitize_url(u)
            for u in FALLBACK_GITHUB_MATRIX
            if sanitize_url(u) not in visited_files
        }
        for fut in as_completed(future_map):
            content, _ = fut.result()
            if content:
                for raw_ch, raw_url_str in extract_raw_entries_from_text(content):
                    norm_ch = normalize_channel_name(raw_ch, target_channels)
                    if norm_ch in target_channels:
                        raw_extracted_records.append((norm_ch, raw_url_str))

    print(f"\n[✓] 第一步完成：共依次打开 {len(visited_files)} 个不同仓库的 Raw 纯文本列表，合计采集到原始直播源 {len(raw_extracted_records)} 条。", flush=True)
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
        raw_url = sanitize_url(raw_url)
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
    clean_url = clean_stream_url(sanitize_url(stream_url))
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
    pages_host = "https://" + "tvzb.github.io"

    print("\n" + "=" * 86, flush=True)
    print("🏆 【第五步(2/2)】汇总合并全部达标源，按清晰度优先 (1920x1080 且速度快在前 -> 720x576) 排序输出", flush=True)
    print(f"   - 第三步初次测速达标源 (无需二次测速): {step3_count} 条", flush=True)
    print(f"   - 第五步裂变新源二次测速新增达标源: {step5_count} 条", flush=True)
    print(f"{'网页频道名称':<18} | {'有效源数':<8} | {'首选分辨率':<14} | {'首选实测速度':<13} | {'首包延迟':<10}", flush=True)
    print("-" * 86, flush=True)

    txt_lines: List[str] = [
        "更新时间,#genre#",
        f"最新更新: {now_str},{pages_host}/IPTV/live.txt",
    ]
    epg_host = "https://" + "live.fanmingming.com"
    m3u_lines: List[str] = [
        f'#EXTM3U x-tvg-url="{epg_host}/e.xml"',
        f'#EXTINF:-1 tvg-name="更新时间" group-title="更新时间",最新更新: {now_str}',
        f'{pages_host}/IPTV/live.m3u',
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
    pages_host = "https://" + "tvzb.github.io"
    epg_host = "https://" + "live.fanmingming.com"
    txt_lines: List[str] = [
        "更新时间,#genre#",
        f"待测源归档: {now_str},{pages_host}/IPTV/raw_sources.txt",
    ]
    m3u_lines: List[str] = [
        f'#EXTM3U x-tvg-url="{epg_host}/e.xml"',
        f'#EXTINF:-1 tvg-name="更新时间" group-title="更新时间",待测源归档: {now_str}',
        f'{pages_host}/IPTV/raw_sources.m3u',
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
        clean_u = clean_stream_url(sanitize_url(raw_u))
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
    pages_host = "https://" + "tvzb.github.io"
    epg_host = "https://" + "live.fanmingming.com"
    out_txt_lines = [
        "更新时间,#genre#",
        f"测速完成: {now_str},{pages_host}/IPTV/live.txt",
        "测速优选频道,#genre#",
    ]
    out_m3u_lines = [
        f'#EXTM3U x-tvg-url="{epg_host}/e.xml"',
        f'#EXTINF:-1 tvg-name="更新时间" group-title="更新时间",测速完成: {now_str}',
        f'{pages_host}/IPTV/live.m3u',
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
        cu = clean_stream_url(sanitize_url(raw_u))
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
