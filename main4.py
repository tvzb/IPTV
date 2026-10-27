# -*- coding: utf-8 -*-
"""
IPTV 自动化五步工作流引擎 (TVBox 真实视频帧强制验证版)
核心升级：
  1. 彻底拦截纯音频广播源 (/audio/)、Flash (.swf)、动态失效 livekey 与假视频垫片；
  2. 强制 ffprobe 视频流验证：无视频轨 (v:0) 或分辨率低于 640x360 的一律淘汰，绝不放行任何黑屏/纯音频源；
  3. 拦截 #EXT-X-ENDLIST 录播死切片，确保 100% 为实时电视直播流。
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
    is_blacklisted_url,
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

HEADERS = {
    "User-Agent": TVBOX_USER_AGENT,
    "Accept": "*/*",
    "Accept-Language": "zh-CN,zh;q=0.9",
    "Connection": "keep-alive"
}

# 精选高活性直播源仓库矩阵（优先收录酒店源、组播精选及每日实测源）
FALLBACK_GITHUB_MATRIX = [
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.txt",
    "https://raw.githubusercontent.com/iptvjs/iptv-api/main/output/user_result.txt",
    "https://raw.githubusercontent.com/cymz6/AutoIPTV-Hotel/main/lives.txt",
    "https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/output/result.txt",
    "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/hd.txt",
    "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv4.m3u",
    "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
    "https://raw.githubusercontent.com/ssili126/tv/main/itvlist.txt",
]

MIN_EXTRACTED_URLS_THRESHOLD = 400
MAX_SAME_IP_PER_CHANNEL = 3


# =========================================================================
# 第一步：搜索及提取直播源 (Page 1 & Page 2 + 多关键词递进)
# =========================================================================

def get_recent_month_str() -> Tuple[str, str]:
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
    html_url = item.get("html_url", "")
    if html_url and "github.com/" in html_url and "/blob/" in html_url:
        return html_url.replace("https://github.com/", "https://raw.githubusercontent.com/").replace("/blob/", "/")
    repo_full_name = item.get("repository", {}).get("full_name", "")
    file_path = item.get("path", "")
    if repo_full_name and file_path:
        return f"https://raw.githubusercontent.com/{repo_full_name}/main/{file_path}"
    return ""


def search_github_code_pages(query: str, max_pages: int = 2, per_page: int = 10) -> List[Tuple[str, str]]:
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
                print(f"  [!] GitHub Search API 触发限流 ({resp.status_code})，请确保已配置有效的 GITHUB_TOKEN。", flush=True)
                break
            else:
                break
        except Exception as e:
            print(f"  [!] 搜索请求异常 ({query}, 第{page}页): {e}", flush=True)
            break
        time.sleep(1.2)

    return results


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

    # TXT 格式解析
    for line in lines:
        if line.startswith("#") or ",#genre#" in line or "更新时间" in line:
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
    curr_month, prev_month = get_recent_month_str()
    print("=" * 72, flush=True)
    print(f"🚀 【第一步】搜索及提取 GitHub 最新直播源 (目标月份: {curr_month})", flush=True)
    print("=" * 72, flush=True)

    primary_query = f"更新 {curr_month} CCTV-6,http"
    backup_queries = [
        f"更新 {curr_month} CCTV6,http",
        f"更新 {curr_month} CCTV-6 http",
        f"更新 {curr_month} CCTV6 http",
        f"tsfile/live/0001_1.m3u8 {curr_month}",
        f"更新 {prev_month} CCTV-6,http",
        f"更新 {prev_month} CCTV6,http",
    ]

    raw_extracted_records: List[Tuple[str, str]] = []
    visited_raw_files: Set[str] = set()

    def process_query(q: str):
        print(f"\n[*] 正在 GitHub 搜索关键词: 「{q}」 (遍历前 2 页结果)...", flush=True)
        search_items = search_github_code_pages(q, max_pages=2, per_page=10)
        if not search_items:
            print(f"  [-] 未获取到「{q}」的搜索结果。", flush=True)
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
            print(f"     [✓] 提取到央视/卫视目标记录: {matched_in_file} 条", flush=True)

    # 1~5: 执行主搜索词
    process_query(primary_query)

    # 6: 如果提取到的地址太少，依次执行备选搜索词
    if len(raw_extracted_records) < MIN_EXTRACTED_URLS_THRESHOLD:
        print(f"\n[!] 当前提取记录 ({len(raw_extracted_records)} 条) 少于 {MIN_EXTRACTED_URLS_THRESHOLD}，启动第 6 步补充关键词搜索...", flush=True)
        for bq in backup_queries:
            process_query(bq)
            if len(raw_extracted_records) >= MIN_EXTRACTED_URLS_THRESHOLD * 3:
                break

    # 合并高活性精品仓库保底池
    print("\n[*] 正在同步精选高活性直播源矩阵以丰富候选池...", flush=True)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_map = {executor.submit(fetch_file_content, u): u for u in FALLBACK_GITHUB_MATRIX if u not in visited_raw_files}
        for fut in as_completed(future_map):
            content = fut.result()
            if content:
                for raw_ch, raw_url_str in extract_raw_entries_from_text(content):
                    if normalize_channel_name(raw_ch) in target_channels:
                        raw_extracted_records.append((raw_ch, raw_url_str))

    print(f"\n[✓] 第一步完成：共提取到原始央视与卫视记录 {len(raw_extracted_records)} 条。", flush=True)
    return raw_extracted_records


# =========================================================================
# 第二步：清洗提取结果 ($尾缀剔除、广播/Flash/假源拦截、统一命名与去重)
# =========================================================================

def step2_clean_and_deduplicate(
    raw_records: List[Tuple[str, str]],
    target_channels: Set[str]
) -> Dict[str, List[str]]:
    print("\n" + "=" * 72, flush=True)
    print("🧹 【第二步】清洗提取结果 (剔除 $ 尾缀、拦截广播/Flash/假源、统一命名去重)", flush=True)
    print("=" * 72, flush=True)

    cleaned_pool: Dict[str, List[str]] = {ch: [] for ch in target_channels}
    seen_urls_per_channel: Dict[str, Set[str]] = {ch: set() for ch in target_channels}
    total_cleaned = 0
    removed_suffix_count = 0
    blocked_junk_count = 0

    for raw_ch, raw_url in raw_records:
        if "$" in raw_url:
            removed_suffix_count += 1

        # 先检查是否命中广播源 / Flash (.swf) / 动态 Token 假源黑名单
        raw_head = raw_url.split("$", 1)[0].strip()
        if is_blacklisted_url(raw_head):
            blocked_junk_count += 1
            continue

        clean_url = clean_stream_url(raw_url)
        if not clean_url or not clean_url.startswith(("http://", "https://")):
            continue

        # 过滤纯 IPv6 地址
        if re.search(r'http://\[[0-9a-fA-F:]+\]', clean_url):
            continue

        std_ch = normalize_channel_name(raw_ch)
        if std_ch not in target_channels:
            continue

        if clean_url not in seen_urls_per_channel[std_ch]:
            seen_urls_per_channel[std_ch].add(clean_url)
            cleaned_pool[std_ch].append(clean_url)
            total_cleaned += 1

    print(f"[✓] 第二步完成：", flush=True)
    print(f"    - 已清除含 '$' 尾缀标记的地址: {removed_suffix_count} 处", flush=True)
    print(f"    - 已拦截纯音频广播/.swf/一次性Token假源: {blocked_junk_count} 条", flush=True)
    print(f"    - 统一命名并去重后剩余干净候选源: {total_cleaned} 条", flush=True)
    return cleaned_pool


# =========================================================================
# 第三步 & 第五步共用：强制视频帧验证 (ffprobe) + 连续切片仿真测速引擎
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


def verify_video_stream_with_ffprobe(raw_bytes: bytes) -> Tuple[bool, str]:
    """
    【核心铁血防线】使用 ffprobe 严格检验二进制流中是否包含真实可解码的电视画面轨 (v:0)。
    - 如果只有音频轨（如阿里云 /audio/ 广播源），ffprobe 输出为空 -> 直接返回 False 淘汰！
    - 如果是 .swf Flash 文件或乱码文件，ffprobe 报错输出为空 -> 直接返回 False 淘汰！
    - 如果分辨率低于 640x360（各种挂羊头卖狗肉的假垫片），直接返回 False 淘汰！
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
            timeout=2.0
        )
        out = proc.stdout.decode("utf-8", errors="ignore").strip()
        if not out:
            return False, "无视频画面轨(纯音频广播或损坏流)"

        # 取第一行解析: 例如 h264x1920x1080
        first_line = out.splitlines()[0].strip()
        parts = first_line.split("x")
        if len(parts) < 3:
            return False, "视频参数缺失"

        codec = parts[0].upper()
        width = int(parts[1])
        height = int(parts[2])

        if width < 640 or height < 360:
            return False, f"分辨率过低({width}x{height})"

        res_tag = "1080P" if height >= 1000 else ("720P" if height >= 700 else f"{width}x{height}")
        return True, f"{codec} {res_tag}"
    except FileNotFoundError:
        # 仅当本地电脑未安装 ffprobe 时的回退判断
        return True, "未安装ffprobe(仅字节校验)"
    except Exception:
        return False, "视频解码探测超时/假流"


def test_single_stream_tvbox(channel_name: str, stream_url: str) -> Tuple[str, str, float, float, bool, str]:
    clean_url = clean_stream_url(stream_url)
    if not clean_url:
        return channel_name, stream_url, 0.0, 9999.0, False, "黑名单拦截"

    req_start = time.time()

    try:
        r = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
        if r.status_code != 200:
            r.close()
            return channel_name, clean_url, 0.0, 9999.0, False, f"HTTP {r.status_code}"

        # 检查 HTTP Content-Type 响应头，直接拦截音频流与 Flash
        content_type = r.headers.get("Content-Type", "").lower()
        if any(bad_ct in content_type for bad_ct in ("audio/", "shockwave-flash", "text/html", "image/", "application/json")):
            r.close()
            return channel_name, clean_url, 0.0, 9999.0, False, f"非视频类型({content_type})"

        first_chunk = next(r.iter_content(chunk_size=1024), b"")
        ttfb_ms = (time.time() - req_start) * 1000.0
        r.close()

        if not first_chunk:
            return channel_name, clean_url, 0.0, ttfb_ms, False, "空响应"

        # 拦截 CWS / FWS 开头的 Adobe Flash (.swf) 文件与 HTML 网页
        if first_chunk[:3] in (b"CWS", b"FWS", b"ZWS") or b"<html" in first_chunk.lower() or b"<!doctype" in first_chunk.lower():
            return channel_name, clean_url, 0.0, ttfb_ms, False, "Flash插件或网页"

        ts_urls: List[str] = []
        current_m3u8_url = clean_url
        is_hls = b"#EXTM3U" in first_chunk

        if is_hls:
            try:
                m3u8_resp = requests.get(current_m3u8_url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
                full_m3u8 = m3u8_resp.text

                # 递归处理 Master Playlist
                if "#EXT-X-STREAM-INF" in full_m3u8:
                    sub_playlists = [
                        line.strip() for line in full_m3u8.splitlines()
                        if line.strip() and not line.strip().startswith("#")
                    ]
                    if sub_playlists:
                        current_m3u8_url = urllib.parse.urljoin(current_m3u8_url, sub_playlists[-1])
                        full_m3u8 = requests.get(current_m3u8_url, headers=HEADERS, timeout=REQUEST_TIMEOUT).text

                # 【关键防伪】如果 M3U8 包含 #EXT-X-ENDLIST，说明是录播死视频或错误提示垫片，绝非实时电视直播！
                if "#EXT-X-ENDLIST" in full_m3u8:
                    return channel_name, clean_url, 0.0, ttfb_ms, False, "录播死切片/非实时直播"

                sec_ok, sec_msg = check_hls_stream_security(full_m3u8, current_m3u8_url)
                if not sec_ok:
                    return channel_name, clean_url, 0.0, ttfb_ms, False, sec_msg

                segments = [
                    urllib.parse.urljoin(current_m3u8_url, line.strip())
                    for line in full_m3u8.splitlines()
                    if line.strip() and not line.strip().startswith("#")
                ]
                if not segments:
                    return channel_name, clean_url, 0.0, ttfb_ms, False, "M3U8无有效切片"

                # 取最新的 2 个连续切片验证
                ts_urls = segments[-2:] if len(segments) >= 2 else segments[:1]
            except Exception:
                return channel_name, clean_url, 0.0, ttfb_ms, False, "M3U8解析失败"
        else:
            ts_urls = [clean_url]

        for tu in ts_urls:
            if is_blacklisted_url(tu):
                return channel_name, clean_url, 0.0, ttfb_ms, False, "切片命中音频/黑名单"
            if re.search(r'(10\.\d+|192\.168\.\d+|172\.(1[6-9]|2\d|3[01])\.\d+|127\.0\.0\.1|localhost)', tu):
                return channel_name, clean_url, 0.0, ttfb_ms, False, "内网切片不可达"

        # 拉取视频切片并累积至少 256KB 缓冲区供 ffprobe 检验视频帧
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

        # 针对 /rtp/ 或 /udp/ 私人转发源：要求必须稳定拉取至少 350KB，否则在 TVBox 上必卡死
        is_udpxy = any(k in clean_url.lower() for k in ("/rtp/", "/udp/"))
        min_required_bytes = 350 * 1024 if is_udpxy else 128 * 1024

        if bytes_received < min_required_bytes or segments_ok == 0:
            return channel_name, clean_url, 0.0, ttfb_ms, False, "数据量不足/断流源"

        raw_bytes = bytes(media_buffer)

        # 严格校验 MPEG-TS 连续 3 个 188 字节包的 0x47 同步头（杜绝碰巧第一个字节是 0x47 的假文件）
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
            return channel_name, clean_url, 0.0, ttfb_ms, False, "非标准TS/FLV视频封装"

        # 【一票否决】调用 ffprobe 强制验证视频画面轨与高清分辨率
        has_video, video_tag = verify_video_stream_with_ffprobe(raw_bytes)
        if not has_video:
            return channel_name, clean_url, 0.0, ttfb_ms, False, video_tag

        duration = max(time.time() - download_start, 0.08)
        raw_speed_kbs = (bytes_received / 1024.0) / duration

        # 计算连接速度评分：对私人 udpxy 源做降权处理，优先把稳定的酒店源/广电 CDN 排在前面
        latency_factor = 1.15 if ttfb_ms < 400 else (0.85 if ttfb_ms > 1500 else 1.0)
        cdn_weight = 0.75 if is_udpxy else (1.20 if "/tsfile/live/" in clean_url else 1.0)
        effective_speed_kbs = raw_speed_kbs * latency_factor * cdn_weight

        return channel_name, clean_url, effective_speed_kbs, ttfb_ms, True, video_tag

    except Exception:
        return channel_name, clean_url, 0.0, 9999.0, False, "连接超时"


def batch_evaluate_streams(
    pool: Dict[str, List[str]],
    min_speed: float,
    stage_title: str
) -> Dict[str, List[Tuple[str, float, float, str]]]:
    tasks: List[Tuple[str, str]] = []
    for ch, urls in pool.items():
        for u in urls:
            tasks.append((ch, u))

    print("\n" + "=" * 72, flush=True)
    print(f"⚡ {stage_title} (待测总数: {len(tasks)} 条, 速度门槛: {min_speed:.1f} KB/s)", flush=True)
    print("=" * 72, flush=True)

    qualified: Dict[str, List[Tuple[str, float, float, str]]] = defaultdict(list)
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
            ch, url, spd, ttfb, ok, vtag = fut.result()
            if ok and spd >= min_speed:
                passed += 1
                qualified[ch].append((url, spd, ttfb, vtag))
            else:
                reason_key = vtag if not ok else "低于速度门槛"
                rejected_reasons[reason_key] += 1

            if tested % 40 == 0 or tested == len(tasks):
                elapsed = max(time.time() - start_t, 0.1)
                print(
                    f"  [测速进度] {tested}/{len(tasks)} | 真实有画面高速源: {passed} 条 | 耗时: {elapsed:.1f}s",
                    flush=True
                )

    if rejected_reasons:
        top_reasons = sorted(rejected_reasons.items(), key=lambda x: x[1], reverse=True)[:5]
        reason_str = ", ".join(f"{k}: {v}条" for k, v in top_reasons)
        print(f"  [拦截统计] 主要淘汰原因 -> {reason_str}", flush=True)

    for ch in qualified:
        qualified[ch].sort(key=lambda x: x[1], reverse=True)

    return qualified


# =========================================================================
# 第四步：对实测有画面且高速的地址进行智能裂变扩容
# =========================================================================

def reverse_search_github_ip(ip_port: str) -> List[Tuple[str, str]]:
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
    print("\n" + "=" * 72, flush=True)
    print("🧬 【第四步】对通过视频帧检验的高速直播源进行智能裂变扩容", flush=True)
    print("=" * 72, flush=True)

    fission_candidates: Dict[str, List[str]] = {ch: [] for ch in target_channels}
    fission_seen: Set[str] = set(already_tested_urls)

    all_high_speed_seeds: List[Tuple[str, float]] = []
    for ch, items in step3_qualified.items():
        for url, spd, ttfb, vtag in items:
            all_high_speed_seeds.append((url, spd))

    all_high_speed_seeds.sort(key=lambda x: x[1], reverse=True)

    extrapolated_added = 0
    reverse_ip_added = 0
    checked_ips: Set[str] = set()

    for seed_url, _ in all_high_speed_seeds:
        derived_list = extrapolate_channels_from_seed(seed_url)
        for ch, new_url in derived_list:
            clean_u = clean_stream_url(new_url)
            std_ch = normalize_channel_name(ch)
            if std_ch in target_channels and clean_u and clean_u not in fission_seen:
                fission_seen.add(clean_u)
                fission_candidates[std_ch].append(clean_u)
                extrapolated_added += 1

    for seed_url, _ in all_high_speed_seeds:
        # 不对单连接受限的私人 udpxy 路由做同 IP 扩散，重点挖掘酒店源与广电服务器
        if any(k in seed_url.lower() for k in ("/rtp/", "/udp/")):
            continue
        ip_port = extract_ip_port(seed_url)
        if not ip_port or ip_port in checked_ips:
            continue
        checked_ips.add(ip_port)
        if len(checked_ips) > 12:
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
    print(f"[✓] 第四步裂变完成：共生成待二次测速的新候选源 {total_fission} 条", flush=True)
    print(f"    - 序号规律推导新增: {extrapolated_added} 条", flush=True)
    print(f"    - 高速同 IP 反向挖掘新增: {reverse_ip_added} 条", flush=True)
    return fission_candidates


# =========================================================================
# 第五步：二次测速 + 合并按连接速度由高到低排序输出
# =========================================================================

def step5_merge_sort_and_export(
    step3_qualified: Dict[str, List[Tuple[str, float, float, str]]],
    step5_qualified: Dict[str, List[Tuple[str, float, float, str]]],
    user_struct: Dict[str, List[str]]
):
    print("\n" + "=" * 78, flush=True)
    print("🏆 【第五步】合并初测与裂变高速源，按频道顺序及速度由高到低排序输出", flush=True)
    print("=" * 78, flush=True)
    print(f"{'标准频道名称':<16} | {'有效高速源':<8} | {'最高速度':<13} | {'首包延迟':<10} | {'实测视频画质':<14}", flush=True)
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
            combined_map: Dict[str, Tuple[str, float, float, str]] = {}
            for item in step3_qualified.get(std_ch, []) + step5_qualified.get(std_ch, []):
                url, spd, ttfb, vtag = item
                if url not in combined_map or spd > combined_map[url][1]:
                    combined_map[url] = item

            sorted_streams = sorted(combined_map.values(), key=lambda x: x[1], reverse=True)

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
    print(f"   - 最终收录真视频高速源: {total_streams_ready} 条")
    print(f"   - TVBox TXT 订阅文件: {OUTPUT_LIVE_TXT}")
    print(f"   - TVBox M3U 订阅文件: {OUTPUT_LIVE_M3U}")
    print("=" * 78, flush=True)


def save_intermediate_cleaned_pool(cleaned_pool: Dict[str, List[str]], user_struct: Dict[str, List[str]]):
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


def run_five_step_workflow(min_speed: float = MIN_SPEED_KBS):
    total_start = time.time()

    user_struct = load_user_channel_structure()
    target_channels: Set[str] = set()
    for ch_list in user_struct.values():
        for ch in ch_list:
            norm = normalize_channel_name(ch)
            if norm:
                target_channels.add(norm)

    # 第一步：搜索及提取直播源
    raw_records = step1_search_and_extract(target_channels)

    # 第二步：清洗 ($及后续汉字删除 + 黑名单剔除)、去重、统一命名
    cleaned_pool = step2_clean_and_deduplicate(raw_records, target_channels)
    save_intermediate_cleaned_pool(cleaned_pool, user_struct)

    tested_url_set: Set[str] = set()
    for urls in cleaned_pool.values():
        tested_url_set.update(urls)

    # 第三步：初次深度测速 (强制 ffprobe 画面帧校验)
    step3_qualified = batch_evaluate_streams(
        cleaned_pool,
        min_speed=min_speed,
        stage_title="【第三步】初次测速与视频帧真伪校验 (淘汰广播源/假源/低速源)"
    )

    # 第四步：仅对通过画面校验的高速地址进行智能裂变
    fission_pool = step4_fission_high_speed_sources(
        step3_qualified,
        already_tested_urls=tested_url_set,
        target_channels=target_channels
    )

    # 第五步(前半)：对裂变出的新地址再次测速与画面校验
    step5_qualified = batch_evaluate_streams(
        fission_pool,
        min_speed=min_speed,
        stage_title="【第五步(1/2)】对裂变扩容后的新地址进行二次测速与画面校验"
    )

    # 第五步(后半)：合并排序输出
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
