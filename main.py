# -*- coding: utf-8 -*-
"""
IPTV 自动化核心引擎 (Main Engine)：
完整实现用户人工收集与裂变直播源的闭环逻辑：
1. 阶段一：动态月份 GitHub 代码搜索 + 高星源池并发抓取（关键词形如：“更新 2026-10 CCTV-2,http”）；
2. 阶段二：规范化清洗（频道名统一归一、彻底剔除 '$' 及后置文本、排除 CCTV-5+、去重）；
3. 阶段三：种子可用性初筛 -> 锁定流畅种子源服务器（IP:端口）；
4. 阶段四：GitHub IP 代码反查（搜索 "IP:端口"） + 智能序号/别名裂变推导（如 0001_1.m3u8 -> 0002_1 ~ 0017_1）；
5. 阶段五：TVBox 原生切片测速（okhttp/3.12.1）、速率门槛筛选（默认 >=300 KB/s，达标全保留，不设数量上限）、频道内网速降序排序；
6. 阶段六：按央视、卫视标准顺序装配输出 live.txt 与 live.m3u 双格式订阅文件。
"""
import os
import re
import sys
import time
import datetime
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set, Tuple

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

FALLBACK_GITHUB_MATRIX = [
    "https://raw.githubusercontent.com/iptvjs/iptv-api/main/output/user_result.txt",
    "https://raw.githubusercontent.com/0610840119/iptv-api/main/output/user_result.txt",
    "https://raw.githubusercontent.com/0610840119/iptv-api/main/output/xp_result.txt",
    "https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/output/result.txt",
    "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv4.m3u",
    "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
    "https://raw.githubusercontent.com/joevess/IPTV/main/sources/iptv_sources.m3u8",
    "https://raw.githubusercontent.com/vbskycn/iptv/master/tv/hd.txt",
    "https://raw.githubusercontent.com/BurningC4/Chinese-IPTV/master/TV-IPV4.m3u",
    "https://raw.githubusercontent.com/ssili126/tv/main/itvlist.txt",
    "https://raw.githubusercontent.com/cymz6/AutoIPTV-Hotel/main/lives.txt",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.txt",
]

# =========================================================================
# 模块一：动态月份计算与 GitHub 代码检索
# =========================================================================

def get_dynamic_search_months() -> Tuple[str, str]:
    """计算当前月份和上一个月份（格式如：2026-10 与 2026-09）"""
    now = datetime.datetime.now()
    curr_month = now.strftime("%Y-%m")
    first_day = now.replace(day=1)
    prev_month_date = first_day - datetime.timedelta(days=1)
    prev_month = prev_month_date.strftime("%Y-%m")
    return curr_month, prev_month

def get_github_auth_headers() -> Dict[str, str]:
    """生成 GitHub API 认证请求头"""
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    headers = {
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "IPTV-Harvester-Bot"
    }
    if token:
        headers["Authorization"] = f"token {token}"
    return headers

def search_github_code_files(query: str, max_items: int = 15) -> List[str]:
    """通过 GitHub Code Search API 搜索代码文件，返回 raw 直链"""
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
        else:
            print(f"[!] GitHub API 检索返回 HTTP {resp.status_code}", flush=True)
    except Exception as e:
        print(f"[!] 检索异常 ({query}): {e}", flush=True)

    return raw_urls

def fetch_file_content(url: str) -> str:
    """拉取单个直播源文本文件"""
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
    """解析文本内容中的频道与流地址，彻底剥离 '$'，统一规范命名"""
    streams: List[Tuple[str, str]] = []
    if not content:
        return streams

    lines = [line.strip() for line in content.splitlines() if line.strip()]

    # M3U 格式解析
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
                    if norm_ch and clean_url and not re.search(r'http://\[[0-9a-fA-F:]+\]', clean_url):
                        streams.append((norm_ch, clean_url))
                current_title = ""
        return streams

    # TXT 格式解析
    for line in lines:
        if line.startswith("#") or ",#genre#" in line or "更新时间" in line:
            continue
        if "," in line and ("http://" in line or "https://" in line or "rtmp://" in line):
            parts = line.split(",", 1)
            raw_ch = parts[0].strip()
            raw_url = parts[1].strip()
            clean_url = clean_stream_url(raw_url)
            norm_ch = normalize_channel_name(raw_ch)
            if norm_ch and clean_url and not re.search(r'http://\[[0-9a-fA-F:]+\]', clean_url):
                streams.append((norm_ch, clean_url))

    return streams

def probe_stream_lightweight(url: str) -> bool:
    """轻量级探测流地址可用性（3秒内握手并验证播放切片/标头）"""
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
    """在 GitHub 上针对该 IP:端口 进行代码反向搜索，挖掘此服务器上的所有频道"""
    discovered: List[Tuple[str, str]] = []
    if not ip_port:
        return discovered

    print(f"  [🔍] 正在 GitHub 反向检索种子服务器: {ip_port} ...", flush=True)
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
    """采集主流程"""
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
        f"CCTV-2 {curr_month} m3u8",
        f"topic:iptv {curr_month}",
    ]

    all_file_urls: Set[str] = set(FALLBACK_GITHUB_MATRIX)

    print("[*] 正在向 GitHub Code Search 检索最新提交的代码仓库...", flush=True)
    for q in search_queries:
        found_urls = search_github_code_files(q, max_items=8)
        for u in found_urls:
            all_file_urls.add(u)
        time.sleep(1.0)

    print(f"[+] 发现有效直播源列表文件: {len(all_file_urls)} 个，开始多线程并发拉取...", flush=True)

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
    print(f"[✓] 基础采集完成，已入池原始源: {total_scraped} 条", flush=True)

    print("[*] 正在探测优质种子源并启动【IP反向搜索】与【智能序号裂变推导】...", flush=True)
    fissioned_count = 0
    checked_ips: Set[str] = set()

    key_seed_channels = ["CCTV-1 综合", "CCTV-2 财经", "江西卫视", "东方卫视", "湖南卫视"]
    candidate_seeds: List[str] = []
    for kch in key_seed_channels:
        if kch in candidate_pool:
            candidate_seeds.extend(candidate_pool[kch][:12])

    for seed_url in candidate_seeds:
        ip_port = extract_ip_port(seed_url)
        if not ip_port or ip_port in checked_ips:
            continue

        checked_ips.add(ip_port)
        if probe_stream_lightweight(seed_url):
            print(f"[+] 锁定流畅可用种子服务器: {ip_port} (源: {seed_url[:65]}...)", flush=True)

            # A. 规律裂变（0001_1 -> 0002_1 ~ 0017_1 以及 字母别名）
            extrapolated = extrapolate_channels_from_seed(seed_url)
            for ch, url in extrapolated:
                if ch in target_channels and url not in seen_urls_per_channel[ch]:
                    seen_urls_per_channel[ch].add(url)
                    candidate_pool[ch].append(url)
                    fissioned_count += 1

            # B. 在 GitHub 对该 IP:Port 反向搜索挖掘该服务器其余频道
            reverse_streams = reverse_search_github_ip(ip_port)
            for ch, url in reverse_streams:
                if ch in target_channels and url not in seen_urls_per_channel[ch]:
                    seen_urls_per_channel[ch].add(url)
                    candidate_pool[ch].append(url)
                    fissioned_count += 1

    print(f"[✓] 智能裂变与反向搜索增补完成，新增优质候选源: {fissioned_count} 条", flush=True)

    export_raw_files(candidate_pool, user_struct)
    return candidate_pool

def export_raw_files(candidate_pool: Dict[str, List[str]], user_struct: Dict[str, List[str]]):
    """保存原始采集候选池"""
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

    print(f"🎉 候选池已保存: {RAW_SOURCES_TXT} 与 {RAW_SOURCES_M3U} (共 {total_exported} 条)", flush=True)

# =========================================================================
# 模块三：TVBox 原生切片测速与优选
# =========================================================================

def parse_input_sources(source_file: str) -> Dict[str, List[str]]:
    """解析待测源文件（支持 TXT 与 M3U 双格式）"""
    channel_streams: Dict[str, List[str]] = {}
    if not os.path.exists(source_file):
        print(f"[!] 源文件 {source_file} 不存在！", flush=True)
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

def test_single_stream_speed(channel_name: str, stream_url: str) -> Tuple[str, str, float, bool]:
    """单条直播流 TVBox 真实播放切片吞吐量测速"""
    clean_url = clean_stream_url(stream_url)

    try:
        r = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT, stream=True)
        if r.status_code != 200:
            return channel_name, clean_url, 0.0, False

        first_chunk = next(r.iter_content(chunk_size=1024), b"")
        r.close()

        if b"<html" in first_chunk.lower() or b"access denied" in first_chunk.lower():
            return channel_name, clean_url, 0.0, False

        ts_url = None
        if b"#EXTM3U" in first_chunk:
            try:
                full_m3u8 = requests.get(clean_url, headers=HEADERS, timeout=REQUEST_TIMEOUT).text
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

        download_start = time.time()
        bytes_received = 0

        try:
            r_ts = requests.get(test_target_url, headers=HEADERS, timeout=CHUNK_DOWNLOAD_TIMEOUT, stream=True)
            if r_ts.status_code == 200:
                for chunk in r_ts.iter_content(chunk_size=32 * 1024):
                    bytes_received += len(chunk)
                    if time.time() - download_start >= CHUNK_DOWNLOAD_TIMEOUT or bytes_received >= CHUNK_DOWNLOAD_MAX_BYTES:
                        break
            r_ts.close()
        except Exception:
            pass

        duration = max(time.time() - download_start, 0.05)
        speed_kbs = (bytes_received / 1024.0) / duration

        if speed_kbs == 0.0 and b"#EXTM3U" in first_chunk:
            speed_kbs = 150.0  # 赋予基准可用速率

        return channel_name, clean_url, speed_kbs, speed_kbs > 0

    except Exception:
        return channel_name, clean_url, 0.0, False

def run_speedtest(source_file: str = RAW_SOURCES_TXT, min_speed: float = MIN_SPEED_KBS):
    """TVBox 测速主流程"""
    print("=" * 65, flush=True)
    print(f"🚀 [阶段二] 开始 TVBox 原生直播源并发测速 (速率门槛: >= {min_speed:.1f} KB/s)", flush=True)
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

    print(f"[*] 待测直播源总数: {len(tasks)} 条，启动 {MAX_WORKERS} 线程极速测速...", flush=True)

    qualified_results: Dict[str, List[Tuple[str, float]]] = {}
    tested_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(test_single_stream_speed, ch, u) for ch, u in tasks]
        for f in as_completed(futures):
            tested_count += 1
            ch, url, speed, is_valid = f.result()
            if is_valid and speed >= min_speed:
                if ch not in qualified_results:
                    qualified_results[ch] = []
                qualified_results[ch].append((url, speed))

            if tested_count % 30 == 0 or tested_count == len(tasks):
                elapsed = max(time.time() - start_time, 0.1)
                rate = tested_count / elapsed
                print(f"  [进展] 已完成 {tested_count}/{len(tasks)} 条测速 ({rate:.1f} 条/秒) ...", flush=True)

    for ch in qualified_results:
        qualified_results[ch].sort(key=lambda x: x[1], reverse=True)

    export_final_playlists(qualified_results, user_struct)

def export_final_playlists(qualified_results: Dict[str, List[Tuple[str, float]]], user_struct: Dict[str, List[str]]):
    """按照标准频道顺序装配并保存 live.txt 与 live.m3u"""
    txt_lines: List[str] = []
    m3u_lines: List[str] = ['#EXTM3U x-tvg-url="https://live.fanmingming.com/e.xml"']

    total_channels_with_streams = 0
    total_qualified_streams = 0

    print("\n" + "=" * 65, flush=True)
    print(f"{'频道名称':<18} | {'有效源数量':<10} | {'最高播放速率':<15}", flush=True)
    print("-" * 65, flush=True)

    for group_name, ch_list in user_struct.items():
        group_txt = [f"{group_name},#genre#"]
        has_channel = False

        for raw_ch in ch_list:
            std_ch = normalize_channel_name(raw_ch)
            streams = qualified_results.get(std_ch, [])

            if streams:
                has_channel = True
                total_channels_with_streams += 1
                total_qualified_streams += len(streams)
                top_speed = streams[0][1]
                print(f"{std_ch:<18} | {len(streams):<10} | {top_speed:>8.1f} KB/s", flush=True)

                for url, spd in streams:
                    group_txt.append(f"{std_ch},{url}")
                    m3u_lines.append(f'#EXTINF:-1 tvg-name="{std_ch}" group-title="{group_name}",{std_ch}')
                    m3u_lines.append(url)
            else:
                print(f"{std_ch:<18} | {'0 (无达标源)':<10} | {'--':>15}", flush=True)

        if has_channel:
            txt_lines.extend(group_txt)

    with open(OUTPUT_LIVE_TXT, "w", encoding="utf-8") as f:
        f.write("\n".join(txt_lines) + "\n")

    with open(OUTPUT_LIVE_M3U, "w", encoding="utf-8") as f:
        f.write("\n".join(m3u_lines) + "\n")

    print("=" * 65, flush=True)
    print(f"🎉 测速筛选完成！")
    print(f"   - 达标频道数: {total_channels_with_streams} 个")
    print(f"   - 保留高速直播源: {total_qualified_streams} 条 (已全部按网速从高到低排序)")
    print(f"   - 交付文件 (TXT 格式): {OUTPUT_LIVE_TXT}")
    print(f"   - 交付文件 (M3U 格式): {OUTPUT_LIVE_M3U}")
    print("=" * 65, flush=True)

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
