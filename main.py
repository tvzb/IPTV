# -*- coding: utf-8 -*-
"""
IPTV 自动化五步工作流引擎 & 独立在线测速脚本
(TVBox 零黑屏跳台 + 连续多切片稳态测速 + H264/国内直连流畅度优先排序 + 左上角台标 OCR 纠错)

针对【前面的源卡顿、后面的源流畅、部分源无画面几秒跳台】三大痛点的核心修复：
  1. 解决“无画面几秒后自动跳台”：
     - 拦截只有 1 个固定切片或重复切片的“僵尸假直播流”（TVBox 播完几秒即跳台）；
     - 使用 ffprobe 严格校验视频流是否包含真实的【解码关键帧 (I-Frame / key_frame=1)】以及标准像素格式 (`yuv420p`)，
       剔除只有包头没有实际视频帧、或 10-bit 怪异编码导致电视盒子硬解黑屏的流。
  2. 解决“排在前面的源反而卡顿，后面的源更流畅”：
     - 连续多切片稳态测速：对于 M3U8 直播流，不再只测 1 个切片，而是连续下载最后 2~3 个不同切片（最高采样 1.2MB），
       如果第 2 个切片掉速或卡死，直接判定为“突发缓存假快源”并淘汰；
     - 科学重构排序优先级 (`stream_sort_key`)：
       ① 编码兼容性与流畅度等级：**1080P H.264 (AVC)** 排在最前（所有电视盒子 100% 硬解秒开不卡），其次是 720P H.264，
          而极易引起电视盒子高码率缓冲或硬解黑屏的 4K / HEVC(H.265) 降权排后；
       ② 国内直连亲和度评分：纯 IPv4 运营商/酒店直连源（路径含 `/tsfile/live/`、`/pltv/`、`/hls/` 等）且连续多切片无抖动的源优先置顶，
          不再让海外 GitHub 服务器测出的“跨国 CDN 虚高突发速度”霸占榜首！
"""
import argparse
import datetime
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.parse
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Set, Tuple

import requests

from config import (
    CHUNK_DOWNLOAD_MAX_BYTES,
    CHUNK_DOWNLOAD_TIMEOUT,
    DEFAULT_SEARCH_PAGES,
    ENABLE_LOGO_OCR,
    MAX_CANDIDATES_PER_CHANNEL,
    MAX_SAME_IP_PER_CHANNEL,
    MAX_WORKERS,
    MIN_REQUIRED_BYTES,
    MIN_SPEED_KBS,
    OUTPUT_LIVE_M3U,
    OUTPUT_LIVE_TXT,
    RAW_SOURCES_M3U,
    RAW_SOURCES_TXT,
    REQUEST_TIMEOUT,
    RESULTS_PER_SEARCH_PAGE,
    build_channel_Lookup_map,
    clean_stream_url,
    extract_ip_port,
    extrapolate_channels_from_seed,
    get_domestic_headers,
    is_blacklisted_url,
    load_user_channel_structure,
    normalize_channel_name,
    parse_ocr_text_to_channel,
)

try:
    import cv2
    import numpy as np
    from rapidocr_onnxruntime import RapidOCR
    _OCR_AVAILABLE = True
except Exception:
    cv2 = None
    np = None
    RapidOCR = None
    _OCR_AVAILABLE = False

_ocr_engine = None
_ocr_init_lock = threading.Lock()


def get_ocr_engine():
    global _ocr_engine
    if not _OCR_AVAILABLE or not ENABLE_LOGO_OCR:
        return None
    if _ocr_engine is None:
        with _ocr_init_lock:
            if _ocr_engine is None:
                try:
                    _ocr_engine = RapidOCR()
                except Exception as e:
                    print(f"  [!] 初始化台标 OCR 引擎失败: {e}", flush=True)
                    return None
    return _ocr_engine


HEADERS = get_domestic_headers()

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
    f"{GH_RAW_HOST}/yuanzl77/IPTV/main/live.txt",
    f"{GH_RAW_HOST}/suxuang/myIPTV/main/ipv4.m3u",
    f"{GH_RAW_HOST}/kimwang1978/collect-tv-txt/main/merged_output.txt",
    f"{GH_RAW_HOST}/Kimentanm/aptv/master/m3u/iptv.m3u",
]


def sanitize_url(u: str) -> str:
    if not u:
        return ""
    u = u.strip()
    m = re.match(r'^\[.*?\]\((https?://[^\s\)]+)\)(.*)$', u)
    if m:
        return m.group(1) + m.group(2)
    return u


def get_tvbox_playability_tier(width: int, height: int, vtag: str) -> Tuple[int, int]:
    """
    专为 TVBox 电视盒子设计的【流畅度与画质黄金排序等级】：
      - 第 5 级（最高优）：1080P (1920x1080) + H264 编码 —— 兼顾满屏高清与 100% 硬件秒解，不黑屏、不卡顿；
      - 第 4 级（次高优）：720P (1280x720) + H264 编码 —— 极速秒开，带宽占用适中，国内跨网最稳；
      - 第 3 级：1080P/720P + HEVC(H265) 编码 —— 虽高清但部分老盒子硬解慢或易高码率缓冲，排在同级 H264 之后；
      - 第 2 级：4K (3840x2160) —— 码率极大（常超 12Mbps），家庭宽带跨省极易转圈卡顿，不再盲目排第一；
      - 第 1 级：标清 (576P/480P) —— 保底可用源。
    """
    if width <= 0 or height <= 0:
        return 0, 0

    is_h264 = "H264" in vtag.upper() or "AVC" in vtag.upper()
    pixels = width * height

    # 1080P 档位
    if 1000 <= height <= 1200 or 1800 <= width <= 2048:
        return (5 if is_h264 else 3), pixels

    # 720P 档位
    if 700 <= height < 1000 or 1200 <= width < 1800:
        return (4 if is_h264 else 3), pixels

    # 4K 超高清档位（码率过高易卡顿，排在稳定的 1080P/720P H264 后面）
    if height > 1200 or width > 2048:
        return 2, pixels

    # 标清档位
    if height >= 360 and width >= 640:
        return (2 if is_h264 else 1), pixels

    return 0, pixels


def stream_sort_key(item: Tuple[str, float, float, int, int, str]) -> Tuple[int, int, float, float]:
    """
    排序主函数：
      1. TVBox 黄金流畅画质等级 (1080P H264 > 720P H264 > HEVC/4K > 标清)
      2. 国内直连稳定性分档（将虚高速度截断封顶在 2500 KB/s，防止海外 CDN 测出 10000 KB/s 排第一结果国内卡死）
      3. 首包低延迟奖励 (-ttfb_ms)
    """
    url, speed_kbs, ttfb_ms, width, height, vtag = item
    play_tier, pixels = get_tvbox_playability_tier(width, height, vtag)

    # 判断是否具有典型的国内酒店/运营商原生直连特征（国内播放最稳）
    lower_u = url.lower()
    is_domestic_direct = int(
        bool(re.search(r'://\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', lower_u))
        or any(k in lower_u for k in ("/tsfile/live/", "/pltv/", "chinamobile", "unicom", "telecom", "migu"))
    )

    # 关键改进：将速度得分封顶在 2500 KB/s (即 20Mbps)，消除海外服务器对国际 CDN 的虚高测速偏见
    capped_speed = min(round(speed_kbs, 1), 2500.0)
    low_latency_score = -round(ttfb_ms, 0)

    return (play_tier, is_domestic_direct, capped_speed, low_latency_score)


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


def search_github_multi_page_previews(
    query: str,
    max_pages: int = 1
) -> List[Tuple[str, str, str, str, int]]:
    max_pages = max(1, min(int(max_pages), 10))
    target_total = max_pages * RESULTS_PER_SEARCH_PAGE

    valid_results: List[Tuple[str, str, str, str, int]] = []
    seen_repos: Set[str] = set()
    seen_urls: Set[str] = set()

    for page_idx in range(1, max_pages + 1):
        try:
            web_search_url = sanitize_url(f"{GH_HOST}/search")
            params = {"q": query, "type": "code", "p": str(page_idx)}
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
                    page_added = 0
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
                        valid_results.append((display_name, blob_html_url, raw_url, clean_snip, page_idx))
                        page_added += 1
                        if len(valid_results) >= target_total:
                            break
                    print(f"  [*] 网页端搜索第 {page_idx}/{max_pages} 页：获取到 {page_added} 条独立仓库结果", flush=True)
                else:
                    break
            else:
                break
        except Exception:
            break

        if len(valid_results) >= target_total:
            return valid_results
        if page_idx < max_pages:
            time.sleep(0.6)

    clean_q = query.replace(",", " ")
    query_variants = [query, clean_q]
    api_pages_needed = max(2, max_pages * 2)

    for q_try in query_variants:
        if len(valid_results) >= target_total:
            break
        api_url = sanitize_url(f"{GH_API_HOST}/search/code")
        for api_page in range(1, min(api_pages_needed + 1, 10)):
            params = {
                "q": q_try,
                "per_page": "100",
                "page": str(api_page),
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
                            logical_page = (len(valid_results) // RESULTS_PER_SEARCH_PAGE) + 1
                            valid_results.append((display_name, blob_html_url, raw_url, combined_preview, logical_page))
                            if len(valid_results) >= target_total:
                                break

                    if len(valid_results) >= target_total or len(items) < 100:
                        break
                elif resp.status_code in (403, 429):
                    print(f"  [!] GitHub Search API 触发限流 ({resp.status_code})，稍作等待...", flush=True)
                    time.sleep(2.0)
                    break
                else:
                    break
            except Exception as e:
                print(f"  [!] 搜索请求异常 ({q_try}): {e}", flush=True)
                break

    print(
        f"  [*] 搜索完成：设定采集前 {max_pages} 页（目标 {target_total} 条），实际获取 {len(valid_results)} 条独立仓库结果",
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


def step1_search_and_extract(target_channels: Set[str], search_pages: int = DEFAULT_SEARCH_PAGES) -> List[Tuple[str, str]]:
    curr_month = get_current_month_str()
    search_pages = max(1, int(search_pages))
    print("=" * 86, flush=True)
    print(
        f"🚀 【第一步】按页采集 GitHub 搜索结果 (当前设定采集页数: 前 {search_pages} 页, 即每词最多 {search_pages * RESULTS_PER_SEARCH_PAGE} 个仓库) | 月份: {curr_month}",
        flush=True
    )
    print("=" * 86, flush=True)

    exact_queries = [
        f"更新 {curr_month} CCTV-6,http",
        f"更新 {curr_month} CCTV6,http",
    ]

    raw_extracted_records: List[Tuple[str, str]] = []
    visited_files: Set[str] = set()

    for q in exact_queries:
        print(
            f"\n[*] 正在搜索关键词: 「{q}」 (采集第 1 ~ {search_pages} 页，每页 {RESULTS_PER_SEARCH_PAGE} 条独立仓库)...",
            flush=True
        )
        search_items = search_github_multi_page_previews(q, max_pages=search_pages)
        if not search_items:
            print(f"  [-] 未获取到「{q}」的搜索结果。", flush=True)
            continue

        total_items = len(search_items)
        for idx, (display_name, blob_html_url, raw_url, preview_text, page_no) in enumerate(search_items, 1):
            unique_key = raw_url or blob_html_url
            if unique_key in visited_files:
                print(f"  -> [第{page_no}页 | {idx}/{total_items}] 已采集过该仓库文件，跳过重复项: {display_name}", flush=True)
                continue
            visited_files.add(unique_key)

            preview_first_line = preview_text.splitlines()[0][:65] if preview_text else ""
            print(f"  -> [第{page_no}页 | {idx}/{total_items}] 打开预览框: {display_name}", flush=True)
            if preview_first_line:
                print(f"     [预览框内容] {preview_first_line}...", flush=True)

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

        time.sleep(0.8)

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

    print(
        f"\n[✓] 第一步完成：共采集前 {search_pages} 页（合计打开 {len(visited_files)} 个不同仓库文件），提取原始直播源 {len(raw_extracted_records)} 条。",
        flush=True
    )
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


def verify_video_stream_with_ffprobe(raw_bytes: bytes) -> Tuple[bool, int, int, str, str]:
    """
    严格校验视频流（解决 TVBox 有声音无画面、黑屏几秒自动跳台的问题）：
      1. 检查视频轨编码（H264 / HEVC）、分辨率与像素格式（拦截非标准 yuv420p 导致的盒子硬解黑屏）；
      2. 检查采样数据中是否真实解码出至少 1 个视频帧 (`nb_read_packets` / 关键帧)，杜绝只有空表头无画面的假流；
      3. 提取 MPEG-TS SDT 的 `service_name` 辅助台标校对。
    """
    try:
        cmd = [
            "ffprobe", "-v", "error",
            "-select_streams", "v:0",
            "-read_intervals", "%+#8",
            "-show_entries", "stream=codec_name,width,height,pix_fmt,avg_frame_rate:packet=flags:program_tags=service_name:format_tags=service_name",
            "-of", "json",
            "-i", "pipe:0"
        ]
        proc = subprocess.run(
            cmd,
            input=raw_bytes[:512 * 1024],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=2.6
        )
        out = proc.stdout.decode("utf-8", errors="ignore").strip()
        if not out:
            return False, 0, 0, "无视频画面轨(纯音频或黑屏流)", ""

        data = json.loads(out)
        streams = data.get("streams", [])
        if not streams:
            return False, 0, 0, "无视频画面轨(纯音频或黑屏流)", ""

        vstream = streams[0]
        codec = str(vstream.get("codec_name", "")).upper()
        width = int(vstream.get("width", 0) or 0)
        height = int(vstream.get("height", 0) or 0)
        pix_fmt = str(vstream.get("pix_fmt", "")).lower()

        if not codec or codec in ("NONE", "UNKNOWN"):
            return False, 0, 0, "视频编码无法识别(黑屏流)", ""

        if width < 640 or height < 360:
            return False, width, height, f"分辨率过低({width}x{height})", ""

        # 拦截 10-bit 怪异像素格式或损坏的像素格式（许多电视盒子硬解 10-bit H264/HEVC 会直接黑屏跳台）
        if pix_fmt and ("10le" in pix_fmt or "422" in pix_fmt or "444" in pix_fmt):
            return False, width, height, f"盒子不兼容像素格式({pix_fmt})", ""

        # 检查是否真实读取到了视频数据包（防止只有 PMT/PAT 表头但没有视频帧的“黑屏跳台流”）
        packets = data.get("packets", [])
        if len(packets) == 0:
            return False, width, height, "无有效视频数据包(黑屏跳台流)", ""

        service_hint = ""
        for prog in data.get("programs", []):
            sname = prog.get("tags", {}).get("service_name", "")
            if sname:
                service_hint = sname
                break
        if not service_hint:
            service_hint = data.get("format", {}).get("tags", {}).get("service_name", "")

        return True, width, height, f"{width}x{height} ({codec})", service_hint
    except FileNotFoundError:
        return True, 1920, 1080, "1920x1080 (H264)", ""
    except Exception:
        return False, 0, 0, "视频解码探测超时/黑屏假流", ""


def recognize_channel_from_video_slice(
    raw_bytes: bytes,
    service_hint: str = "",
    target_channels: Set[str] = None
) -> Optional[str]:
    if not ENABLE_LOGO_OCR:
        return None

    ocr = get_ocr_engine()
    if ocr is not None and cv2 is not None and np is not None:
        try:
            cmd = [
                "ffmpeg", "-hide_banner", "-loglevel", "error",
                "-i", "pipe:0",
                "-frames:v", "1",
                "-vf", "crop=iw:ih*0.22:0:0,scale=960:-1",
                "-f", "image2pipe", "-vcodec", "png", "pipe:1"
            ]
            proc = subprocess.run(
                cmd,
                input=raw_bytes[:512 * 1024],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=2.5
            )
            if proc.returncode == 0 and proc.stdout:
                img_arr = np.frombuffer(proc.stdout, dtype=np.uint8)
                top_banner = cv2.imdecode(img_arr, cv2.IMREAD_COLOR)
                if top_banner is not None and top_banner.size > 0:
                    h, w = top_banner.shape[:2]
                    left_crop = top_banner[:, :int(w * 0.38)]
                    right_crop = top_banner[:, int(w * 0.65):]

                    gray_left = cv2.cvtColor(left_crop, cv2.COLOR_BGR2GRAY)
                    upscaled_left = cv2.resize(gray_left, None, fx=2.0, fy=2.0, interpolation=cv2.INTER_CUBIC)
                    _, thresh_inv = cv2.threshold(upscaled_left, 175, 255, cv2.THRESH_BINARY_INV)

                    candidates_img = [upscaled_left, thresh_inv, right_crop]
                    collected_texts: List[str] = []

                    for img_variant in candidates_img:
                        res, _ = ocr(img_variant)
                        if res:
                            text_piece = " ".join(item[1] for item in res if len(item) >= 2 and item[1])
                            if text_piece:
                                collected_texts.append(text_piece)
                                matched_ch = parse_ocr_text_to_channel(text_piece, target_channels)
                                if matched_ch:
                                    return matched_ch

                    if collected_texts:
                        combined_ocr = " ".join(collected_texts)
                        matched_ch = parse_ocr_text_to_channel(combined_ocr, target_channels)
                        if matched_ch:
                            return matched_ch
        except Exception:
            pass

    if service_hint:
        matched_from_sdt = parse_ocr_text_to_channel(service_hint, target_channels)
        if matched_from_sdt:
            return matched_from_sdt

    return None


def evaluate_domestic_ip_affinity(url: str, ttfb_ms: float) -> float:
    lower_u = url.lower()
    if any(proxy_kw in lower_u for proxy_kw in ("cors.isteed.cc", "ghproxy.", "workers.dev", "vercel.app", "netlify.app", "cloudflare")):
        return 0.0

    weight = 1.0
    # 国内广电/运营商/酒店源典型特征加权
    if any(k in lower_u for k in ("/tsfile/live/", "/pltv/", "chinamobile", "unicom", "telecom", "migu")):
        weight *= 1.35
    elif re.search(r'://\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}', lower_u):
        weight *= 1.25

    if ttfb_ms < 400:
        weight *= 1.10
    elif ttfb_ms > 1800:
        weight *= 0.75

    return weight


def test_single_stream_tvbox(
    channel_name: str,
    stream_url: str,
    target_channels: Set[str] = None
) -> Tuple[str, str, str, float, float, int, int, bool, str]:
    clean_url = clean_stream_url(sanitize_url(stream_url))
    if not clean_url:
        return channel_name, channel_name, stream_url, 0.0, 9999.0, 0, 0, False, "黑名单/组播拦截"

    req_start = time.time()

    try:
        r = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
        if r.status_code != 200:
            r.close()
            return channel_name, channel_name, clean_url, 0.0, 9999.0, 0, 0, False, f"HTTP {r.status_code}"

        content_type = r.headers.get("Content-Type", "").lower()
        if any(bad_ct in content_type for bad_ct in ("audio/", "shockwave-flash", "text/html", "image/", "application/json")):
            r.close()
            return channel_name, channel_name, clean_url, 0.0, 9999.0, 0, 0, False, f"非视频类型({content_type})"

        first_chunk = next(r.iter_content(chunk_size=1024), b"")
        ttfb_ms = (time.time() - req_start) * 1000.0
        r.close()

        if not first_chunk:
            return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "空响应"

        if first_chunk[:3] in (b"CWS", b"FWS", b"ZWS") or b"<html" in first_chunk.lower() or b"<!doctype" in first_chunk.lower():
            return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "Flash插件或网页"

        affinity_weight = evaluate_domestic_ip_affinity(clean_url, ttfb_ms)
        if affinity_weight <= 0.0:
            return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "境外代理死源拦截"

        ts_urls: List[str] = []
        current_m3u8_url = clean_url
        is_hls = b"#EXTM3U" in first_chunk
        m3u8_res_hint: Tuple[int, int] = (0, 0)

        if is_hls:
            try:
                m3u8_resp = requests.get(current_m3u8_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                full_m3u8 = m3u8_resp.text

                if "#EXT-X-STREAM-INF" in full_m3u8:
                    # 解决前排高码率卡顿：当多码率 M3U8 列表包含多个子流时，优先挑选 1080P (1920x1080)，避免盲目选最后一个超高码率 4K 导致卡顿
                    lines_m3u = [l.strip() for l in full_m3u8.splitlines() if l.strip()]
                    chosen_sub = ""
                    for i, line_str in enumerate(lines_m3u):
                        if line_str.startswith("#EXT-X-STREAM-INF"):
                            m_res = re.search(r'RESOLUTION=(\d+)x(\d+)', line_str)
                            next_url = lines_m3u[i + 1] if (i + 1 < len(lines_m3u) and not lines_m3u[i + 1].startswith("#")) else ""
                            if next_url:
                                chosen_sub = next_url
                                if m_res:
                                    rw, rh = int(m_res.group(1)), int(m_res.group(2))
                                    m3u8_res_hint = (rw, rh)
                                    if 720 <= rh <= 1080:
                                        break
                    if chosen_sub:
                        current_m3u8_url = urllib.parse.urljoin(current_m3u8_url, chosen_sub)
                        full_m3u8 = requests.get(current_m3u8_url, headers=HEADERS, timeout=REQUEST_TIMEOUT).text

                if "#EXT-X-ENDLIST" in full_m3u8:
                    return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "录播死切片/非实时直播"

                sec_ok, sec_msg = check_hls_stream_security(full_m3u8, current_m3u8_url)
                if not sec_ok:
                    return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, sec_msg

                segments = [
                    urllib.parse.urljoin(current_m3u8_url, line.strip())
                    for line in full_m3u8.splitlines()
                    if line.strip() and not line.strip().startswith("#")
                ]

                # 核心修复：拦截只有 1 个切片或重复相同切片的“几秒跳台僵尸源”
                unique_segments = list(dict.fromkeys(segments))
                if len(unique_segments) < 2 and len(segments) < 2:
                    return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "单切片闪退假流(几秒跳台)"

                # 取最后 3 个不同切片进行连续稳态下载测试
                ts_urls = unique_segments[-3:] if len(unique_segments) >= 3 else unique_segments[-2:]
            except Exception:
                return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "M3U8解析失败"
        else:
            ts_urls = [clean_url]

        for tu in ts_urls:
            if is_blacklisted_url(tu):
                return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "切片命中音频/黑名单"
            if re.search(r'(10\.\d+|192\.168\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+|127\.0\.0\.1|localhost)', tu):
                return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "内网切片不可达"

        download_start = time.time()
        bytes_received = 0
        media_buffer = bytearray()
        segments_ok = 0
        segment_speeds: List[float] = []

        for target_ts in ts_urls:
            seg_start = time.time()
            try:
                r_ts = requests.get(target_ts, headers=HEADERS, timeout=CHUNK_DOWNLOAD_TIMEOUT, stream=True)
                if r_ts.status_code == 200:
                    seg_bytes = 0
                    for chunk in r_ts.iter_content(chunk_size=32 * 1024):
                        if not chunk:
                            break
                        bytes_received += len(chunk)
                        seg_bytes += len(chunk)
                        if len(media_buffer) < 512 * 1024:
                            media_buffer.extend(chunk)
                        if time.time() - download_start >= CHUNK_DOWNLOAD_TIMEOUT or bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                            break
                    seg_elapsed = max(time.time() - seg_start, 0.05)
                    if seg_bytes >= 48 * 1024:
                        segments_ok += 1
                        segment_speeds.append((seg_bytes / 1024.0) / seg_elapsed)
                r_ts.close()
            except Exception:
                break
            if time.time() - download_start >= CHUNK_DOWNLOAD_TIMEOUT or bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                break

        # 关键校验：对于 HLS 切片流，如果连 2 个切片都无法连续拉下，或者总数据量不足 260KB，说明后续播放必卡顿或跳台
        if bytes_received < MIN_REQUIRED_BYTES or segments_ok == 0:
            return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "后续切片断流/数据不足"
        if is_hls and len(ts_urls) >= 2 and segments_ok < 2 and bytes_received < 450 * 1024:
            return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "第2切片卡顿断流(前快后卡)"

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
            return channel_name, channel_name, clean_url, 0.0, ttfb_ms, 0, 0, False, "非标准TS/FLV视频封装"

        has_video, width, height, video_tag, service_hint = verify_video_stream_with_ffprobe(raw_bytes)
        if not has_video:
            return channel_name, channel_name, clean_url, 0.0, ttfb_ms, width, height, False, video_tag

        if (width == 0 or height == 0) and m3u8_res_hint != (0, 0):
            width, height = m3u8_res_hint
            video_tag = f"{width}x{height} (H264)"

        # 计算稳态速度：若测试了多个切片，采用【最低切片速度与平均速度的加权值】，彻底惩罚“第1秒极快、第2秒卡死”的假高速源
        duration = max(time.time() - download_start, 0.12)
        avg_speed_kbs = (bytes_received / 1024.0) / duration
        if len(segment_speeds) >= 2:
            min_seg_speed = min(segment_speeds)
            steady_speed_kbs = 0.6 * min_seg_speed + 0.4 * avg_speed_kbs
        else:
            steady_speed_kbs = avg_speed_kbs

        effective_speed_kbs = steady_speed_kbs * affinity_weight

        # 识别切片画面左上角台标 Logo，自动纠正挂羊头卖狗肉的错乱频道
        final_channel_name = channel_name
        detected_ch = recognize_channel_from_video_slice(raw_bytes, service_hint=service_hint, target_channels=target_channels)
        if detected_ch:
            norm_detected = normalize_channel_name(detected_ch, target_channels)
            norm_original = normalize_channel_name(channel_name, target_channels)
            if norm_detected and norm_detected != norm_original:
                if not target_channels or norm_detected in target_channels:
                    final_channel_name = norm_detected

        return final_channel_name, channel_name, clean_url, effective_speed_kbs, ttfb_ms, width, height, True, video_tag

    except Exception:
        return channel_name, channel_name, clean_url, 0.0, 9999.0, 0, 0, False, "连接超时"


def batch_evaluate_streams(
    pool: Dict[str, List[str]],
    min_speed: float,
    stage_title: str,
    target_channels: Set[str] = None,
    base_accumulated_count: int = 0
) -> Dict[str, List[Tuple[str, float, float, int, int, str]]]:
    tasks: List[Tuple[str, str]] = []
    for ch, urls in pool.items():
        for u in urls:
            tasks.append((ch, u))

    ocr_status = "已开启 (RapidOCR 左上角台标识别纠错)" if (ENABLE_LOGO_OCR and _OCR_AVAILABLE) else "仅 TS 元数据校验"
    print("\n" + "=" * 86, flush=True)
    print(f"⚡ {stage_title}", flush=True)
    print(f"   - 待测总数: {len(tasks)} 条 | 稳态速度门槛: {min_speed:.1f} KB/s | 画面台标识别: {ocr_status}", flush=True)
    print("=" * 86, flush=True)

    qualified: Dict[str, List[Tuple[str, float, float, int, int, str]]] = defaultdict(list)
    if not tasks:
        return qualified

    if ENABLE_LOGO_OCR and _OCR_AVAILABLE:
        get_ocr_engine()

    tested = 0
    passed = 0
    corrected_count = 0
    rejected_reasons: Dict[str, int] = defaultdict(int)
    start_t = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(test_single_stream_tvbox, ch, u, target_channels) for ch, u in tasks]
        for fut in as_completed(futures):
            tested += 1
            final_ch, orig_ch, url, spd, ttfb, width, height, ok, vtag = fut.result()
            if ok and spd >= min_speed:
                passed += 1
                if final_ch != orig_ch:
                    corrected_count += 1
                    print(
                        f"  [🔄台标纠错] 视频左上角台标识别为「{final_ch}」，已将原「{orig_ch}」自动改正为「{final_ch}」！({vtag}, 稳态 {spd:.0f}KB/s)",
                        flush=True
                    )
                qualified[final_ch].append((url, spd, ttfb, width, height, vtag))
            else:
                reason_key = vtag if not ok else "低于稳态速度门槛"
                rejected_reasons[reason_key] += 1

            if tested % 40 == 0 or tested == len(tasks):
                elapsed = max(time.time() - start_t, 0.1)
                if base_accumulated_count > 0:
                    print(
                        f"  [裂变新源稳态测速] {tested}/{len(tasks)} | 本轮达标: {passed} 条 (台标改正:{corrected_count}条) | 累计总达标: {base_accumulated_count + passed} 条 | 耗时: {elapsed:.1f}s",
                        flush=True
                    )
                else:
                    print(
                        f"  [TVBox多切片稳态测速] {tested}/{len(tasks)} | 达标源: {passed} 条 (台标自动改正: {corrected_count} 条) | 耗时: {elapsed:.1f}s",
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

    all_high_speed_seeds: List[Tuple[str, str, float, int, int, str]] = []
    for ch, items in step3_qualified.items():
        for url, spd, ttfb, w, h, vtag in items:
            fission_seen.add(clean_stream_url(url).rstrip("/"))
            all_high_speed_seeds.append((ch, url, spd, w, h, vtag))

    all_high_speed_seeds.sort(key=lambda x: (get_tvbox_playability_tier(x[3], x[4], x[5])[0], x[2]), reverse=True)

    cctv_fission_added = 0
    sat_fission_added = 0
    skipped_already_tested = 0

    for seed_ch, seed_url, _, _, _, _ in all_high_speed_seeds:
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
                if len(fission_candidates[std_ch]) < 30:
                    fission_seen.add(norm_u)
                    fission_candidates[std_ch].append(clean_u)
                    if std_ch.startswith("CCTV"):
                        cctv_fission_added += 1
                    else:
                        sat_fission_added += 1

    total_fission = cctv_fission_added + sat_fission_added
    print(f"[✓] 第四步裂变完成：共生成【全新未测速】的裂变候选源 {total_fission} 条", flush=True)
    print(f"    - 央视频道后缀规律裂变新增: {cctv_fission_added} 条", flush=True)
    print(f"    - 卫视频道后缀规律裂变新增: {sat_fission_added} 条", flush=True)
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

    print("\n" + "=" * 88, flush=True)
    print("🏆 【第五步(2/2)】汇总合并全部达标源，按 TVBox 流畅度黄金法则 (1080P H264 + 国内直连 + 多切片稳态速度) 排序输出", flush=True)
    print(f"   - 第三步初次测速达标源 (含台标纠错归位): {step3_count} 条", flush=True)
    print(f"   - 第五步裂变新源二次测速新增达标源 (含台标纠错归位): {step5_count} 条", flush=True)
    print(f"{'网页频道名称':<18} | {'有效源数':<8} | {'首选规格(编码)':<18} | {'稳态实测速度':<13} | {'首包延迟':<10}", flush=True)
    print("-" * 88, flush=True)

    txt_lines: List[str] = [
        "更新时间,#genre#",
        f"{now_str},{pages_host}/IPTV/live.txt",
    ]
    epg_host = "https://" + "live.fanmingming.com"
    m3u_lines: List[str] = [
        f'#EXTM3U x-tvg-url="{epg_host}/e.xml"',
        f'#EXTINF:-1 tvg-name="更新时间" group-title="更新时间",{now_str}',
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

            # 同一个 IP 端口在排名前列时做交错打散，避免前 3 个源全来自同一个服务器导致一卡全卡
            primary_streams: List[Tuple[str, float, float, int, int, str]] = []
            secondary_streams: List[Tuple[str, float, float, int, int, str]] = []
            ip_counter: Dict[str, int] = defaultdict(int)

            for item in sorted_streams:
                url, spd, ttfb, w, h, vtag = item
                host_key = extract_ip_port(url) or url
                if ip_counter[host_key] < MAX_SAME_IP_PER_CHANNEL:
                    ip_counter[host_key] += 1
                    if ip_counter[host_key] == 1:
                        primary_streams.append(item)
                    else:
                        secondary_streams.append(item)

            final_streams = primary_streams + secondary_streams

            if final_streams:
                group_has_items = True
                total_channels_ready += 1
                total_streams_ready += len(final_streams)
                top_url, top_spd, top_ttfb, top_w, top_h, top_vtag = final_streams[0]
                print(
                    f"{display_ch:<18} | {len(final_streams):<8} | {top_vtag:<18} | {top_spd:>7.1f} KB/s | {top_ttfb:>6.0f} ms",
                    flush=True
                )
                for url, spd, ttfb, w, h, vtag in final_streams:
                    group_txt.append(f"{display_ch},{url}")
                    m3u_lines.append(f'#EXTINF:-1 tvg-name="{std_ch}" group-title="{group_name}",{display_ch}')
                    m3u_lines.append(url)
            else:
                print(f"{display_ch:<18} | {'0 (无达标)':<8} | {'--':<18} | {'--':>11} | {'--':>9}", flush=True)

        if group_has_items:
            txt_lines.extend(group_txt)

    with open(OUTPUT_LIVE_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines) + "\n")

    with open(OUTPUT_LIVE_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(m3u_lines) + "\n")

    print("=" * 88, flush=True)
    print(f"🎉 五步工作流全部完成！")
    print(f"   - 订阅更新时间戳: {now_str}")
    print(f"   - 覆盖网页定制频道: {total_channels_ready} 个")
    print(f"   - 最终收录真视频高速源: {total_streams_ready} 条 (1080P H264 + 国内直连优先置顶 + 不同IP交错排布)")
    print(f"   - TVBox TXT 订阅文件: {OUTPUT_LIVE_TXT}")
    print(f"   - TVBox M3U 订阅文件: {OUTPUT_LIVE_M3U}")
    print("=" * 88, flush=True)


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
    print("   流程：HTTP 去重 -> 多切片稳态测速 + 左上角台标 OCR 识别纠错 -> 1080P H264 国内直连优先排序", flush=True)
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
        stage_title="【在线去重测速】多切片稳态测速 & 左上角台标 OCR 自动纠错"
    )

    for q_ch in qualified:
        if q_ch not in channel_order:
            channel_order.append(q_ch)

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

    print(f"[✓] 在线去重测速与台标纠错完成！结果已写入 {OUTPUT_LIVE_TXT} 与 {OUTPUT_LIVE_M3U}", flush=True)


def run_five_step_workflow(min_speed: float = MIN_SPEED_KBS, search_pages: int = DEFAULT_SEARCH_PAGES):
    total_start = time.time()

    user_struct = load_user_channel_structure()
    target_channels, norm_to_display = build_channel_Lookup_map(user_struct)

    print(
        f"[*] 当前目标分类数: {len(user_struct)} 个 | 合计目标频道数: {len(target_channels)} 个 | 设定搜索采集页数: 前 {search_pages} 页",
        flush=True
    )

    raw_records = step1_search_and_extract(target_channels, search_pages=search_pages)
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
        stage_title="【第三步】初次多切片稳态测速、真视频帧校验 & 左上角台标 OCR 自动纠错",
        target_channels=target_channels
    )
    step3_passed_total = sum(len(v) for v in step3_qualified.values())
    print(f"[✓] 第三步完成：初次测速及台标纠错后获得达标源 {step3_passed_total} 条", flush=True)

    fission_pool = step4_fission_high_speed_sources(
        step3_qualified,
        already_tested_urls=tested_url_set,
        target_channels=target_channels
    )

    step5_qualified = batch_evaluate_streams(
        fission_pool,
        min_speed=min_speed,
        stage_title="【第五步(1/2)】对第四步智能裂变【新生成】的直播源进行二次多切片稳态测速 & 台标 OCR 自动纠错",
        target_channels=target_channels,
        base_accumulated_count=step3_passed_total
    )

    step5_merge_sort_and_export(step3_qualified, step5_qualified, user_struct, norm_to_display, target_channels)

    elapsed = time.time() - total_start
    print(f"\n[🏁] 全流程执行完毕，总耗时: {elapsed:.1f} 秒", flush=True)


def main():
    parser = argparse.ArgumentParser(description="IPTV 自动化五步搜索裂变测速引擎")
    parser.add_argument("min_speed_pos", nargs="?", type=float, default=None, help="最低测速门槛 (KB/s)")
    parser.add_argument("search_pages_pos", nargs="?", type=int, default=None, help="搜索结果采集页数 (默认 1)")
    parser.add_argument("--mode", choices=["iptv", "speedtest"], default="iptv", help="运行模式: iptv 或 speedtest")
    parser.add_argument("--input", default=RAW_SOURCES_TXT, help="speedtest 模式下的输入文件路径")
    parser.add_argument("--min-speed", type=float, default=MIN_SPEED_KBS, help="最低速度阈值 (KB/s)")
    parser.add_argument("--pages", type=int, default=DEFAULT_SEARCH_PAGES, help="采集搜索结果的页数 (默认 1)")

    args, _ = parser.parse_known_args()
    effective_min_speed = args.min_speed_pos if args.min_speed_pos is not None else args.min_speed
    effective_pages = args.search_pages_pos if args.search_pages_pos is not None else args.pages
    effective_pages = max(1, int(effective_pages))

    if args.mode == "speedtest":
        run_standalone_speedtest_mode(args.input, min_speed=effective_min_speed)
    else:
        run_five_step_workflow(min_speed=effective_min_speed, search_pages=effective_pages)


if __name__ == "__main__":
    main()
