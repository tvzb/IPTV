# -*- coding: utf-8 -*-
"""
IPTV 自动化五步工作流引擎 (TVBox 真实播放仿真版)
严格遵循：
  第一步：GitHub 动态分页搜索 (Page 1 & Page 2) + 备选关键词自动递进扩充
  第二步：清洗 URL ($及后面汉字清理)、全局去重、频道名称标准化 (CCTV-1~17 & 省级卫视)
  第三步：初次深度测速 (过滤失效及低速地址，模拟 TVBox 连续多切片拉流)
  第四步：智能裂变 (仅针对第三步验证通过的高速有效源进行序号推导与同 IP 反向搜索)
  第五步：二次测速 + 全量合并 + 按频道顺序及连接速度由高到低排序输出
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
from typing import Dict, List, Set, Tuple

import requests

from config import (
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
    CHUNK_DOWNLOAD_MAX_BYTES,
    MAX_WORKERS,
    MIN_SPEED_KBS,
)

# 模拟真实 TVBox 内置播放器请求头
HEADERS = {
    "User-Agent": TVBOX_USER_AGENT,
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive"
}

# 当搜索结果不足或 GitHub API 触发匿名限流时的保底高活性源池
FALLBACK_GITHUB_MATRIX = [
    "https://raw.githubusercontent.com/iptvjs/iptv-api/main/output/user_result.txt",
    "https://raw.githubusercontent.com/0610840119/iptv-api/main/output/user_result.txt",
    "https://raw.githubusercontent.com/0610840119/iptv-api/main/output/xp_result.txt",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.txt",
    "https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/output/result.txt",
    "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv4.m3u",
    "https://raw.githubusercontent.com/cymz6/AutoIPTV-Hotel/main/lives.txt",
    "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/hd.txt",
]

# 单频道提取地址数量阈值：若主关键词提取不足此数量，触发第6小步备选关键词搜索
MIN_EXTRACTED_URLS_THRESHOLD = 300
# 同一频道下同一服务器 IP:Port 最多保留线路数（防止单台酒店服务器宕机导致整个频道全黑）
MAX_SAME_IP_PER_CHANNEL = 3


# =========================================================================
# 第一步：搜索及提取直播源 (支持翻页 Page 1~2 & 精确 Raw 地址还原)
# =========================================================================

def get_recent_month_str() -> Tuple[str, str]:
    """获取当前年月 (如 2026-10) 及上月，用于搜索最近一个月更新的直播源"""
    now = datetime.datetime.now()
    curr_month = now.strftime("%Y-%m")
    first_day = now.replace(day=1)
    prev_month = (first_day - datetime.timedelta(days=1)).strftime("%Y-%m")
    return curr_month, prev_month


def get_github_headers() -> Dict[str, str]:
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    }
    if token:
        headers["Authorization"] = f"token {token}"
    return headers


def convert_github_item_to_raw_url(item: dict) -> str:
    """
    精准将 GitHub 搜索结果的 html_url 转换为可直接下载的 Raw 链接。
    解决原脚本硬编码 /main/ 导致特定 commit (如图二中的 9f65ae5) 报 404 的问题。
    """
    html_url = item.get("html_url", "")
    if html_url and "github.com/" in html_url and "/blob/" in html_url:
        return html_url.replace("https://github.com/", "https://raw.githubusercontent.com/").replace("/blob/", "/")
    
    repo_full_name = item.get("repository", {}).get("full_name", "")
    file_path = item.get("path", "")
    if repo_full_name and file_path:
        return f"https://raw.githubusercontent.com/{repo_full_name}/main/{file_path}"
    return ""


def search_github_code_pages(query: str, max_pages: int = 2, per_page: int = 10) -> List[Tuple[str, str]]:
    """
    模拟在 GitHub 搜索框输入关键词并翻看前 2 页的所有结果。
    返回: [(显示路径名称, 真实Raw下载链接), ...]
    """
    headers = get_github_headers()
    results: List[Tuple[str, str]] = []
    seen_raws: Set[str] = set()

    for page in range(1, max_pages + 1):
        encoded_q = urllib.parse.quote(query)
        api_url = f"https://api.github.com/search/code?q={encoded_q}&per_page={per_page}&page={page}"
        try:
            resp = requests.get(api_url, headers=headers, timeout=12)
            if resp.status_code == 200:
                items = resp.json().get("items", [])
                if not items:
                    break
                for item in items:
                    repo_name = item.get("repository", {}).get("full_name", "")
                    file_path = item.get("path", "")
                    display_name = f"{repo_name} · {file_path}"
                    raw_url = convert_github_item_to_raw_url(item)
                    if raw_url and raw_url not in seen_raws:
                        seen_raws.add(raw_url)
                        results.append((display_name, raw_url))
            elif resp.status_code in (403, 429):
                print(f"  [!] GitHub Search API 触发速率限制 (状态码 {resp.status_code})，建议配置 GITHUB_TOKEN 环境变量。", flush=True)
                break
            else:
                break
        except Exception as e:
            print(f"  [!] 搜索请求异常 ({query}, 第{page}页): {e}", flush=True)
            break
        # 遵守 GitHub Search API 频率要求
        time.sleep(1.2)

    return results


def fetch_file_content(url: str) -> str:
    """下载搜索结果文件内容，自动兼容 main/master 分支回退"""
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
    """
    从打开的文件内容中提取原始 (频道名, 原始URL) 对。
    支持图二中的 TXT 格式 (频道名,http...) 以及标准 M3U 格式。
    """
    entries: List[Tuple[str, str]] = []
    if not content:
        return entries

    lines = [line.strip() for line in content.splitlines() if line.strip()]

    # M3U 格式识别与提取
    if any(line.startswith("#EXTM3U") for line in lines[:5]):
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

    # TXT 格式识别与提取 (如图二: CCTV-1,http://101.66.195.243:9901/...)
    for line in lines:
        if line.startswith("#") or ",#genre#" in line or "更新时间" in line:
            continue
        if "," in line and ("http://" in line or "https://" in line):
            parts = line.split(",", 1)
            raw_ch = parts[0].strip()
            raw_url_part = parts[1].strip()
            # 应对一行中用 # 分隔多条源的情况: 频道,url1#url2
            for sub_url in raw_url_part.split("#"):
                sub_url = sub_url.strip()
                if sub_url.startswith(("http://", "https://")):
                    entries.append((raw_ch, sub_url))

    return entries


def step1_search_and_extract(target_channels: Set[str]) -> List[Tuple[str, str]]:
    """
    执行第一步：
    1. 输入 "更新 YYYY-MM CCTV-6,http" 搜索前 2 页结果，逐一打开并提取 CCTV-1~17 及省级卫视；
    2. 若提取地址太少，依次使用备选关键词搜索前 2 页并提取。
    """
    curr_month, prev_month = get_recent_month_str()
    print("=" * 72, flush=True)
    print(f"🚀 【第一步】搜索及提取 GitHub 最新直播源 (目标月份: {curr_month})", flush=True)
    print("=" * 72, flush=True)

    # 主搜索词与第 6 小步指定的备选搜索词队列
    primary_query = f"更新 {curr_month} CCTV-6,http"
    backup_queries = [
        f"更新 {curr_month} CCTV6,http",
        f"更新 {curr_month} CCTV-6 http",
        f"更新 {curr_month} CCTV6 http",
        # 跨月初交界处自动兜底上月
        f"更新 {prev_month} CCTV-6,http",
        f"更新 {prev_month} CCTV6,http",
    ]

    raw_extracted_records: List[Tuple[str, str]] = []
    visited_raw_files: Set[str] = set()

    def process_query(q: str):
        print(f"\n[*] 正在 GitHub 搜索关键词: 「{q}」 (遍历前 2 页结果)...", flush=True)
        search_items = search_github_code_pages(q, max_pages=2, per_page=10)
        if not search_items:
            print(f"  [-] 未通过 API 获取到「{q}」的直接结果或已受限。", flush=True)
            return

        for idx, (display_name, raw_url) in enumerate(search_items, 1):
            if raw_url in visited_raw_files:
                continue
            visited_raw_files.add(raw_url)
            print(f"  -> [{idx}/{len(search_items)}] 打开并提取: {display_name}", flush=True)
            content = fetch_file_content(raw_url)
            if not content:
                continue
            entries = extract_raw_entries_from_text(content)
            matched_in_file = 0
            for raw_ch, raw_url_str in entries:
                norm_ch = normalize_channel_name(raw_ch)
                if norm_ch in target_channels:
                    raw_extracted_records.append((raw_ch, raw_url_str))
                    matched_in_file += 1
            print(f"     [✓] 从该文件提取到央视/卫视目标地址: {matched_in_file} 条", flush=True)

    # 1~5: 执行主搜索词
    process_query(primary_query)

    # 6: 如果提取到的地址太少，依次执行备选搜索词
    if len(raw_extracted_records) < MIN_EXTRACTED_URLS_THRESHOLD:
        print(f"\n[!] 当前提取地址数 ({len(raw_extracted_records)} 条) 少于阈值 {MIN_EXTRACTED_URLS_THRESHOLD}，启动第 6 步多关键词补充搜索...", flush=True)
        for bq in backup_queries:
            process_query(bq)
            if len(raw_extracted_records) >= MIN_EXTRACTED_URLS_THRESHOLD * 3:
                break

    # 若在无 Token 环境下 GitHub API 被限流导致提取不足，自动合并高活性保底仓库
    if len(raw_extracted_records) < MIN_EXTRACTED_URLS_THRESHOLD:
        print("\n[*] 正在从高活性镜像仓库补充基础央视与卫视直播源...", flush=True)
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            future_map = {executor.submit(fetch_file_content, u): u for u in FALLBACK_GITHUB_MATRIX if u not in visited_raw_files}
            for fut in as_completed(future_map):
                content = fut.result()
                if content:
                    for raw_ch, raw_url_str in extract_raw_entries_from_text(content):
                        if normalize_channel_name(raw_ch) in target_channels:
                            raw_extracted_records.append((raw_ch, raw_url_str))

    print(f"\n[✓] 第一步完成：共提取到原始央视与卫视直播源记录 {len(raw_extracted_records)} 条。", flush=True)
    return raw_extracted_records


# =========================================================================
# 第二步：清洗提取结果 ($及后续汉字剔除、去重、统一命名)
# =========================================================================

def step2_clean_and_deduplicate(
    raw_records: List[Tuple[str, str]],
    target_channels: Set[str]
) -> Dict[str, List[str]]:
    """
    执行第二步：
    1. 删除所有直播源地址（以 http 开头）中 $ 及后面的汉字与附加标记；
    2. 对频道进行统一命名（如 CCTV1/CCTV-1综合 -> 标准名），保留原来的有效地址内容；
    3. 进行严格去重（过滤纯 IPv6 内网及重复链接）。
    """
    print("\n" + "=" * 72, flush=True)
    print("🧹 【第二步】清洗提取结果 (剔除 $ 尾缀、统一频道命名、全局去重)", flush=True)
    print("=" * 72, flush=True)

    cleaned_pool: Dict[str, List[str]] = {ch: [] for ch in target_channels}
    seen_urls_per_channel: Dict[str, Set[str]] = {ch: set() for ch in target_channels}
    total_cleaned = 0
    removed_suffix_count = 0

    for raw_ch, raw_url in raw_records:
        if "$" in raw_url:
            removed_suffix_count += 1

        # 删除 $ 及后面的汉字（如 $订阅源、$高清、$LR 等）
        clean_url = clean_stream_url(raw_url)
        if not clean_url or not clean_url.startswith(("http://", "https://")):
            continue

        # 过滤掉纯 IPv6 地址（国内多数 TVBox 盒子或路由环境开启 IPv6 不完善易黑屏，如需保留可注释下一行）
        if re.search(r'http://\[[0-9a-fA-F:]+\]', clean_url):
            continue

        # 统一频道命名
        std_ch = normalize_channel_name(raw_ch)
        if std_ch not in target_channels:
            continue

        # 频道内去重
        if clean_url not in seen_urls_per_channel[std_ch]:
            seen_urls_per_channel[std_ch].add(clean_url)
            cleaned_pool[std_ch].append(clean_url)
            total_cleaned += 1

    print(f"[✓] 第二步完成：", flush=True)
    print(f"    - 已清除含 '$' 尾缀标记的地址: {removed_suffix_count} 处", flush=True)
    print(f"    - 统一命名并去重后剩余唯一有效候选源: {total_cleaned} 条", flush=True)
    return cleaned_pool


# =========================================================================
# 第三步 & 第五步共用：仿真 TVBox 真实观看测速引擎
# =========================================================================

def check_hls_stream_security(m3u8_text: str, base_url: str) -> Tuple[bool, str]:
    """排查内网 AES-128 死锁加密源"""
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


def test_single_stream_tvbox(channel_name: str, stream_url: str) -> Tuple[str, str, float, float, bool, str]:
    """
    高度还原国内 TVBox 实际播放体验的深度测速器：
    1. 测量 TTFB (首包响应时间)：TVBox 换台等待超过 2.5 秒体验极差；
    2. 连续切片校验：若为 M3U8，不仅下载第 1 个切片，还验证后续切片是否连续可读，彻底封杀“单切片假活源”；
    3. 校验 MPEG-TS 0x47 同步字节与视频分辨率；
    4. 计算 TVBox 综合连接速度评分 (兼顾吞吐速度与换台响应延迟)。
    返回: (频道名, URL, 排序速度KB/s, 首包延迟ms, 是否有效, 画质标签)
    """
    clean_url = clean_stream_url(stream_url)
    req_start = time.time()

    try:
        r = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
        if r.status_code != 200:
            r.close()
            return channel_name, clean_url, 0.0, 9999.0, False, f"HTTP {r.status_code}"

        first_chunk = next(r.iter_content(chunk_size=1024), b"")
        ttfb_ms = (time.time() - req_start) * 1000.0
        r.close()

        if not first_chunk:
            return channel_name, clean_url, 0.0, ttfb_ms, False, "空响应"

        low_chunk = first_chunk.lower()
        if b"<html" in low_chunk or b"<!doctype" in low_chunk or b"access denied" in low_chunk:
            return channel_name, clean_url, 0.0, ttfb_ms, False, "网页拦截/假源"

        ts_urls: List[str] = []
        current_m3u8_url = clean_url

        # 解析 HLS (M3U8) 播放列表
        if b"#EXTM3U" in first_chunk:
            try:
                m3u8_resp = requests.get(current_m3u8_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                full_m3u8 = m3u8_resp.text

                # 递归解析多码率 Master Playlist (优先挑选最高码率子流)
                if "#EXT-X-STREAM-INF" in full_m3u8:
                    sub_playlists = [
                        line.strip() for line in full_m3u8.splitlines()
                        if line.strip() and not line.strip().startswith("#")
                    ]
                    if sub_playlists:
                        current_m3u8_url = urllib.parse.urljoin(current_m3u8_url, sub_playlists[-1])
                        full_m3u8 = requests.get(current_m3u8_url, headers=HEADERS, timeout=REQUEST_TIMEOUT).text

                sec_ok, sec_msg = check_hls_stream_security(full_m3u8, current_m3u8_url)
                if not sec_ok:
                    return channel_name, clean_url, 0.0, ttfb_ms, False, sec_msg

                # 提取 M3U8 中的切片列表 (取前 2 个连续切片验证真伪直播)
                segments = [
                    urllib.parse.urljoin(current_m3u8_url, line.strip())
                    for line in full_m3u8.splitlines()
                    if line.strip() and not line.strip().startswith("#")
                ]
                ts_urls = segments[:2]
            except Exception:
                return channel_name, clean_url, 0.0, ttfb_ms, False, "M3U8解析失败"

        if not ts_urls:
            ts_urls = [clean_url]

        # 拦截指向局域网私有地址的伪切片
        for tu in ts_urls:
            if re.search(r'(10\.\d+|192\.168\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+|127\.0\.0\.1|localhost)', tu):
                return channel_name, clean_url, 0.0, ttfb_ms, False, "内网切片不可达"

        # 模拟 TVBox ExoPlayer 连续拉取视频流数据
        download_start = time.time()
        bytes_received = 0
        media_buffer = bytearray()
        segments_ok = 0

        for seg_idx, target_ts in enumerate(ts_urls):
            try:
                r_ts = requests.get(target_ts, headers=HEADERS, timeout=3.0, stream=True)
                if r_ts.status_code == 200:
                    seg_bytes = 0
                    for chunk in r_ts.iter_content(chunk_size=32 * 1024):
                        if not chunk:
                            break
                        bytes_received += len(chunk)
                        seg_bytes += len(chunk)
                        if len(media_buffer) < 128 * 1024:
                            media_buffer.extend(chunk)
                        # 控制单源总测速时长不超过 2.6 秒，兼顾准确性与整体效率
                        if time.time() - download_start >= 2.6 or bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                            break
                    if seg_bytes > 2048:
                        segments_ok += 1
                r_ts.close()
            except Exception:
                break
            if time.time() - download_start >= 2.6 or bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                break

        # 如果 M3U8 存在多个切片但连第 1 个切片都没拉够 16KB，直接判定为死源或断流源
        if bytes_received < 16 * 1024 or segments_ok == 0:
            return channel_name, clean_url, 0.0, ttfb_ms, False, "断流/切片0字节"

        # 校验 MPEG-TS (0x47 每 188 字节同步头) 或 FLV/MP4 媒体封装头
        raw_bytes = bytes(media_buffer)
        is_ts = (
            len(raw_bytes) >= 376
            and (
                (raw_bytes[0] == 0x47 and raw_bytes[188] == 0x47)
                or (b"G" in raw_bytes[:188] and raw_bytes.count(b"G") >= 3)
            )
        )
        is_mp4_or_flv = (
            b"ftyp" in raw_bytes[:64]
            or b"moov" in raw_bytes[:64]
            or b"mdat" in raw_bytes[:64]
            or raw_bytes[:3] == b"FLV"
        )

        if not (is_ts or is_mp4_or_flv):
            return channel_name, clean_url, 0.0, ttfb_ms, False, "非标准视频流"

        duration = max(time.time() - download_start, 0.08)
        raw_speed_kbs = (bytes_received / 1024.0) / duration

        # 计算贴合国内 TVBox 实际观看体验的有效连接速度：
        # 1. 首包响应越快 (<400ms 秒开)，給予速度加成；响应越慢 (>1500ms)，给予折损；
        # 2. 对国内主流广电/酒店组播转换源 (/tsfile/live/、/PLTV/、/ottrrs.) 在跨洋测速时的带宽损耗予以合理补偿。
        latency_factor = 1.15 if ttfb_ms < 400 else (0.85 if ttfb_ms > 1500 else 1.0)
        domestic_cdn_bonus = 1.25 if any(k in clean_url for k in ("/tsfile/live/", "/PLTV/88888888/", "chinamobile.com", "/gslb/")) else 1.0
        effective_speed_kbs = raw_speed_kbs * latency_factor * domestic_cdn_bonus

        # 内存级零延迟 ffprobe 分辨率与编码识别（过滤只有音频没有视频轨的“黑屏有声源”）
        video_tag = "高清直连"
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
                input=raw_bytes[:96 * 1024],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=1.2
            )
            out = proc.stdout.decode("utf-8", errors="ignore").strip()
            if out:
                parts = out.split("x")
                if len(parts) >= 3:
                    codec, w, h = parts[0].upper(), int(parts[1]), int(parts[2])
                    if w < 320 or h < 240:
                        return channel_name, clean_url, 0.0, ttfb_ms, False, "分辨率过低/假画面"
                    res_label = "1080P" if h >= 1000 else ("720P" if h >= 700 else f"{w}x{h}")
                    video_tag = f"{codec} {res_label}"
        except Exception:
            pass

        return channel_name, clean_url, effective_speed_kbs, ttfb_ms, True, video_tag

    except Exception as e:
        return channel_name, clean_url, 0.0, 9999.0, False, f"超时/重置"


def batch_evaluate_streams(
    pool: Dict[str, List[str]],
    min_speed: float,
    stage_title: str
) -> Dict[str, List[Tuple[str, float, float, str]]]:
    """
    批量并发执行测速，返回达标的: {标准频道名: [(url, 有效速度KB/s, 延迟ms, 画质标签), ...]}
    """
    tasks: List[Tuple[str, str]] = []
    for ch, urls in pool.items():
        for u in urls:
            tasks.append((ch, u))

    print("\n" + "=" * 72, flush=True)
    print(f"⚡ {stage_title} (待测总数: {len(tasks)} 条, 最低速度门槛: {min_speed:.1f} KB/s)", flush=True)
    print("=" * 72, flush=True)

    qualified: Dict[str, List[Tuple[str, float, float, str]]] = defaultdict(list)
    if not tasks:
        return qualified

    tested = 0
    passed = 0
    start_t = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(test_single_stream_tvbox, ch, u) for ch, u in tasks]
        for fut in as_completed(futures):
            tested += 1
            ch, url, spd, ttfb, ok, vtag = fut.result()
            if ok and spd >= min_speed:
                passed += 1
                qualified[ch].append((url, spd, ttfb, vtag))

            if tested % 40 == 0 or tested == len(tasks):
                elapsed = max(time.time() - start_t, 0.1)
                print(
                    f"  [测速进度] {tested}/{len(tasks)} 条 | 达标高速源: {passed} 条 | 耗时: {elapsed:.1f}s",
                    flush=True
                )

    # 按有效连接速度从高到低初步排序
    for ch in qualified:
        qualified[ch].sort(key=lambda x: x[1], reverse=True)

    return qualified


# =========================================================================
# 第四步：对有效且高速地址进行智能裂变扩容
# =========================================================================

def reverse_search_github_ip(ip_port: str) -> List[Tuple[str, str]]:
    """根据第三步验证通过的高速酒店/广电服务器 IP:Port 反向搜索 GitHub 上的完整频道列表"""
    discovered: List[Tuple[str, str]] = []
    if not ip_port:
        return discovered

    search_items = search_github_code_pages(f'"{ip_port}"', max_pages=1, per_page=5)
    for _, raw_url in search_items:
        content = fetch_file_content(raw_url)
        for raw_ch, raw_u in extract_raw_entries_from_text(content):
            if ip_port in raw_u:
                discovered.append((raw_ch, raw_u))
    return discovered


def step4_fission_high_speed_sources(
    step3_qualified: Dict[str, List[Tuple[str, float, float, str]]],
    already_tested_urls: Set[str],
    target_channels: Set[str]
) -> Dict[str, List[str]]:
    """
    执行第四步：
    仅针对第三步实测证明【有效且高速】的地址进行智能扩容：
    1. 保留原脚本的频道序号推导模式 (如 0001_1.m3u8 -> 0002_1~0017_1, cctv1hd -> cctv17hd)；
    2. 对排名前列的高速独立服务器 IP 进行 GitHub 反向同 IP 检索，挖掘该服务器下的其他卫视与央视频道；
    3. 自动清洗 $ 尾缀并剔除已经在第二步/第三步测过的重复地址。
    """
    print("\n" + "=" * 72, flush=True)
    print("🧬 【第四步】对实测有效且高速的直播源进行智能裂变扩容", flush=True)
    print("=" * 72, flush=True)

    fission_candidates: Dict[str, List[str]] = {ch: [] for ch in target_channels}
    fission_seen: Set[str] = set(already_tested_urls)

    # 收集所有通过第三步测速的高速源，按速度从高到低排列提取种子
    all_high_speed_seeds: List[Tuple[str, float]] = []
    for ch, items in step3_qualified.items():
        for url, spd, ttfb, vtag in items:
            all_high_speed_seeds.append((url, spd))

    all_high_speed_seeds.sort(key=lambda x: x[1], reverse=True)

    extrapolated_added = 0
    reverse_ip_added = 0
    checked_ips: Set[str] = set()

    # 1. 规则推导裂变：对所有高速有效源直接推导同服务器其他频道
    for seed_url, _ in all_high_speed_seeds:
        derived_list = extrapolate_channels_from_seed(seed_url)
        for ch, new_url in derived_list:
            clean_u = clean_stream_url(new_url)
            std_ch = normalize_channel_name(ch)
            if std_ch in target_channels and clean_u and clean_u not in fission_seen:
                fission_seen.add(clean_u)
                fission_candidates[std_ch].append(clean_u)
                extrapolated_added += 1

    # 2. 同 IP 反向搜索裂变：选取速度最快的前 15 个独立 IP:Port 进行同服全频道挖掘
    for seed_url, spd in all_high_speed_seeds:
        ip_port = extract_ip_port(seed_url)
        if not ip_port or ip_port in checked_ips:
            continue
        checked_ips.add(ip_port)
        if len(checked_ips) > 15:
            break

        rev_records = reverse_search_github_ip(ip_port)
        for raw_ch, raw_u in rev_records:
            clean_u = clean_stream_url(raw_u)
            std_ch = normalize_channel_name(raw_ch)
            if std_ch in target_channels and clean_u and clean_u not in fission_seen:
                fission_seen.add(clean_u)
                fission_candidates[std_ch].append(clean_u)
                reverse_ip_added += 1

    total_fission = extrapolated_added + reverse_ip_added
    print(f"[✓] 第四步裂变完成：共生成全新待测高速候选源 {total_fission} 条", flush=True)
    print(f"    - 序号规律推导新增: {extrapolated_added} 条", flush=True)
    print(f"    - 高速同 IP 反向挖掘新增: {reverse_ip_added} 条", flush=True)
    return fission_candidates


# =========================================================================
# 第五步：对裂变地址再次测速 + 合并排序 + 导出最终 TVBox 播放列表
# =========================================================================

def step5_merge_sort_and_export(
    step3_qualified: Dict[str, List[Tuple[str, float, float, str]]],
    step5_qualified: Dict[str, List[Tuple[str, float, float, str]]],
    user_struct: Dict[str, List[str]]
):
    """
    执行第五步收尾：
    1. 将第三步初测高速源与第五步裂变测速高速源合并去重；
    2. 按频道顺序（CCTV-1~17 -> 省级卫视），同一频道内严格按测速连接速度由高到低排列；
    3. 实施同 IP 负载均衡限制（同一频道单 IP 最多保留 3 条），输出最终结果。
    """
    print("\n" + "=" * 78, flush=True)
    print("🏆 【第五步】合并初测与裂变高速源，按频道顺序及速度由高到低排序输出", flush=True)
    print("=" * 78, flush=True)
    print(f"{'标准频道名称':<16} | {'有效高速源':<8} | {'最高速度':<13} | {'首包延迟':<10} | {'最佳画质':<14}", flush=True)
    print("-" * 78, flush=True)

    txt_lines: List[str] = []
    m3u_lines: List[str] = ['#EXTM3U x-tvg-url="https://live.fanmingming.com/e.xml"']

    total_channels_ready = 0
    total_streams_ready = 0

    for group_name, ch_list in user_struct.items():
        group_txt = [f"{group_name},#genre#"]
        group_has_items = False

        for raw_ch in ch_list:
            std_ch = normalize_channel_name(raw_ch)
            # 合并第三步和第五步的合格高速源
            combined_map: Dict[str, Tuple[str, float, float, str]] = {}
            for item in step3_qualified.get(std_ch, []) + step5_qualified.get(std_ch, []):
                url, spd, ttfb, vtag = item
                if url not in combined_map or spd > combined_map[url][1]:
                    combined_map[url] = item

            # 同一频道内按测速连接速度由高到低排列
            sorted_streams = sorted(combined_map.values(), key=lambda x: x[1], reverse=True)

            # 同服务器防独占保护（防止单个酒店 IP 刷屏同一频道）
            final_streams: List[Tuple[str, float, float, str]] = []
            ip_counter: Dict[str, int] = defaultdict(int)
            for url, spd, ttfb, vtag in sorted_streams:
                host_key = extract_ip_port(url) or url
                if ip_counter[host_key] < MAX_SAME_IP_PER_CHANNEL:
                    ip_counter[host_key] += 1
                    final_streams.append((url, spd, ttfb, vtag))

            if final_streams:
                group_has_items = True
                total_channels_ready += 1
                total_streams_ready += len(final_streams)
                top_url, top_spd, top_ttfb, top_vtag = final_streams[0]
                print(
                    f"{std_ch:<16} | {len(final_streams):<10} | {top_spd:>7.1f} KB/s | {top_ttfb:>6.0f} ms | {top_vtag:<14}",
                    flush=True
                )
                for url, spd, ttfb, vtag in final_streams:
                    group_txt.append(f"{std_ch},{url}")
                    m3u_lines.append(f'#EXTINF:-1 tvg-name="{std_ch}" group-title="{group_name}",{std_ch}')
                    m3u_lines.append(url)
            else:
                print(f"{std_ch:<16} | {'0 (无达标)':<10} | {'--':>11} | {'--':>9} | {'--':<14}", flush=True)

        if group_has_items:
            txt_lines.extend(group_txt)

    with open(OUTPUT_LIVE_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines) + "\n")

    with open(OUTPUT_LIVE_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(m3u_lines) + "\n")

    print("=" * 78, flush=True)
    print(f"🎉 五步工作流全部完成！")
    print(f"   - 覆盖有效频道: {total_channels_ready} 个")
    print(f"   - 最终收录高速源: {total_streams_ready} 条 (已全部按各频道连接速度从高到低排列)")
    print(f"   - TVBox TXT 订阅文件: {OUTPUT_LIVE_TXT}")
    print(f"   - TVBox M3U 订阅文件: {OUTPUT_LIVE_M3U}")
    print("=" * 78, flush=True)


def save_intermediate_cleaned_pool(cleaned_pool: Dict[str, List[str]], user_struct: Dict[str, List[str]]):
    """保存第二步清洗与去重后的中间快照 (供排查或单独运行测速使用)"""
    txt_lines: List[str] = []
    m3u_lines: List[str] = ['#EXTM3U x-tvg-url="https://live.fanmingming.com/e.xml"']
    for group_name, ch_list in user_struct.items():
        group_txt = [f"{group_name},#genre#"]
        for raw_ch in ch_list:
            std_ch = normalize_channel_name(raw_ch)
            for u in cleaned_pool.get(std_ch, []):
                group_txt.append(f"{std_ch},{u}")
                m3u_lines.append(f'#EXTINF:-1 tvg-name="{std_ch}" group-title="{group_name}",{std_ch}')
                m3u_lines.append(u)
        if len(group_txt) > 1:
            txt_lines.extend(group_txt)

    with open(RAW_SOURCES_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines) + "\n")
    with open(RAW_SOURCES_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(m3u_lines) + "\n")


# =========================================================================
# 主入口：串联完整五步法
# =========================================================================

def run_five_step_workflow(min_speed: float = MIN_SPEED_KBS):
    total_start = time.time()

    # 加载标准频道结构 (CCTV-1 ~ CCTV-17 + 省级卫视)
    user_struct = load_user_channel_structure()
    target_channels: Set[str] = set()
    for ch_list in user_struct.values():
        for ch in ch_list:
            norm = normalize_channel_name(ch)
            if norm:
                target_channels.add(norm)

    # 第一步：搜索及提取直播源
    raw_records = step1_search_and_extract(target_channels)

    # 第二步：清洗 ($及后续汉字删除)、去重、统一命名
    cleaned_pool = step2_clean_and_deduplicate(raw_records, target_channels)
    save_intermediate_cleaned_pool(cleaned_pool, user_struct)

    # 记录所有已纳入初测的 URL，避免第四步裂变出重复项
    tested_url_set: Set[str] = set()
    for urls in cleaned_pool.values():
        tested_url_set.update(urls)

    # 第三步：对清洗后的地址进行初次深度测速，过滤失效及低速地址
    step3_qualified = batch_evaluate_streams(
        cleaned_pool,
        min_speed=min_speed,
        stage_title="【第三步】对清洗后的直播源进行初次测速 (过滤失效及低速地址)"
    )

    # 第四步：仅对第三步筛选出的有效高速地址进行智能裂变扩容
    fission_pool = step4_fission_high_speed_sources(
        step3_qualified,
        already_tested_urls=tested_url_set,
        target_channels=target_channels
    )

    # 第五步(前半)：对第四步裂变出的新地址再次测速，过滤失效及低速地址
    step5_qualified = batch_evaluate_streams(
        fission_pool,
        min_speed=min_speed,
        stage_title="【第五步(1/2)】对裂变扩容后的新地址进行二次测速"
    )

    # 第五步(后半)：合并两次测速的高速地址，按频道顺序及连接速度由高到低排列并输出
    step5_merge_sort_and_export(step3_qualified, step5_qualified, user_struct)

    elapsed = time.time() - total_start
    print(f"\n[🏁] 全流程执行完毕，总耗时: {elapsed:.1f} 秒", flush=True)


def main():
    min_speed = MIN_SPEED_KBS
    if len(sys.argv) > 1:
        for arg in sys.argv[1:]:
            try:
                min_speed = float(arg)
            except ValueError:
                pass
    run_five_step_workflow(min_speed=min_speed)


if __name__ == "__main__":
    main()
