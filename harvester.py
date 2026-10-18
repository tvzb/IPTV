# -*- coding: utf-8 -*-
"""
工作流 1 核心引擎：全网大采集与标准化清洗（Harvester）
职责：
1. 继承历史原始源，多线程并发拉取 16+ 权威源矩阵；
2. 动态扫描 GitHub 带有 topic:iptv / topic:tvbox 的全网活跃仓库；
3. 优质 IP:Port 全局反查 + 种子源规律裂变；
4. 统一频道标准化命名，彻底剥离 '$' 及其后文字；
5. 全局去重并导出全量候选池 raw_sources.txt。
"""
import os
import re
import time
import socket
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Set

import requests

from config import (
    load_user_channel_structure,
    normalize_channel_name,
    clean_stream_url,
    extrapolate_channels_from_seed,
    RAW_SOURCES_FILE,
    DEFAULT_OUTPUT_FILE,
    TVBOX_USER_AGENT,
)

HEADERS = {
    "User-Agent": TVBOX_USER_AGENT,
    "Accept": "*/*",
    "Connection": "keep-alive"
}

GITHUB_IPTV_MATRIX = [
    "https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/output/result.txt",
    "https://raw.githubusercontent.com/Ftindy/IPTV-URL/main/output/user_result.txt",
    "https://raw.githubusercontent.com/fanmingming/live/main/tv/m3u/ipv4.m3u",
    "https://raw.githubusercontent.com/YueChan/Live/main/IPTV.m3u",
    "https://raw.githubusercontent.com/YueChan/Live/main/cctv.m3u",
    "https://raw.githubusercontent.com/asdjkl6/tv/tv/tv.txt",
    "https://raw.githubusercontent.com/suxing/iptv/main/feimao.txt",
    "https://raw.githubusercontent.com/suxing/iptv/main/itvlist.txt",
    "https://raw.githubusercontent.com/Supprise0901/TVBox_live/main/live.txt",
    "https://raw.githubusercontent.com/Guovin/iptv-api/gd/output/result.txt",
    "https://raw.githubusercontent.com/YanG-1989/m3u/main/Gather.m3u",
    "https://raw.githubusercontent.com/ssili126/tv/main/itvlist.txt",
    "https://raw.githubusercontent.com/zbds/live/main/live.txt",
    "https://raw.githubusercontent.com/joevess/IPTV/main/sources/live.txt",
    "https://raw.githubusercontent.com/lalifeier/IPTV/main/txt/cctv.txt",
    "https://raw.githubusercontent.com/lalifeier/IPTV/main/txt/satellite.txt",
    "https://raw.githubusercontent.com/zwc456baby/iptv_sources/main/iptv.txt",
]


def log(msg: str):
    print(msg, flush=True)


def is_valid_ipv4_url(url: str) -> bool:
    try:
        clean_url = clean_stream_url(url)
        parsed = urllib.parse.urlparse(clean_url)
        hostname = parsed.hostname
        if not hostname or ":" in hostname or hostname.startswith("["):
            return False
        return True
    except Exception:
        return False


def get_host_identifier(url: str) -> str:
    try:
        clean_url = clean_stream_url(url)
        parsed = urllib.parse.urlparse(clean_url)
        return parsed.netloc or parsed.hostname or ""
    except Exception:
        return ""


def fetch_content_from_url(url: str, token: str = "") -> str:
    headers = HEADERS.copy()
    if token and "github.com" in url:
        headers["Authorization"] = f"Bearer {token}"
    try:
        resp = requests.get(url, headers=headers, timeout=6)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return ""


def load_existing_pools(candidate_pools: Dict[str, Set[str]]):
    """载入本地或线上已有的 raw_sources.txt / live.txt 历史池"""
    files_to_check = [RAW_SOURCES_FILE, DEFAULT_OUTPUT_FILE]
    for fp in files_to_check:
        if os.path.exists(fp):
            log(f"[*] 载入既有历史源文件: {fp}...")
            try:
                with open(fp, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or ",#genre#" in line:
                            continue
                        if "," in line:
                            parts = line.split(",", 1)
                            std_name = normalize_channel_name(parts[0].strip())
                            clean_url = clean_stream_url(parts[1].strip())
                            if std_name in candidate_pools and is_valid_ipv4_url(clean_url):
                                candidate_pools[std_name].add(clean_url)
            except Exception:
                pass


def parse_stream_content(content: str, candidate_pools: Dict[str, Set[str]]):
    """解析 TXT 或 M3U 内容，彻底剔除 '$' 及其后文字"""
    lines = content.splitlines()
    for line in lines:
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if "," in line and "http" in line:
            parts = line.split(",", 1)
            std_name = normalize_channel_name(parts[0].strip())
            clean_url = clean_stream_url(parts[1].strip())
            if std_name and std_name in candidate_pools:
                if is_valid_ipv4_url(clean_url):
                    candidate_pools[std_name].add(clean_url)

    # M3U 场景
    current_ch = None
    for line in lines:
        line = line.strip()
        if line.startswith("#EXTINF"):
            match = re.search(r'tvg-name="([^"]+)"', line) or re.search(r',([^,]+)$', line)
            if match:
                current_ch = normalize_channel_name(match.group(1).strip())
        elif line.startswith("http") and current_ch:
            clean_url = clean_stream_url(line)
            if current_ch in candidate_pools:
                if is_valid_ipv4_url(clean_url):
                    candidate_pools[current_ch].add(clean_url)
            current_ch = None


def search_all_github_sources(token: str, candidate_pools: Dict[str, Set[str]]):
    """并发多线程拉网式采集全网源"""
    log("[*] 正在并发嗅探全网 16 大权威源与 GitHub 活跃仓库...")
    all_urls = list(GITHUB_IPTV_MATRIX)

    headers = HEADERS.copy()
    if token:
        headers["Authorization"] = f"Bearer {token}"
        headers["Accept"] = "application/vnd.github.v3+json"

    # 嗅探 topic:iptv 和 topic:tvbox 活跃项目
    for tag in ["topic:iptv", "topic:tvbox"]:
        try:
            api_url = f"https://api.github.com/search/repositories?q={tag}&sort=updated&order=desc&per_page=6"
            r = requests.get(api_url, headers=headers, timeout=5)
            if r.status_code == 200:
                for repo in r.json().get("items", []):
                    b = repo.get("default_branch", "main")
                    fn = repo.get("full_name", "")
                    for fp in ["live.txt", "tv.txt", "ipv4.m3u", "output/result.txt"]:
                        all_urls.append(f"https://raw.githubusercontent.com/{fn}/{b}/{fp}")
        except Exception:
            pass

    t0 = time.time()
    with ThreadPoolExecutor(max_workers=12) as executor:
        futures = {executor.submit(fetch_content_from_url, u, token): u for u in all_urls}
        for future in as_completed(futures):
            c = future.result()
            if c:
                parse_stream_content(c, candidate_pools)

    log(f"[+] 全网源并发下载解析完成！耗时: {time.time() - t0:.2f} 秒")


def reverse_host_fission(token: str, candidate_pools: Dict[str, Set[str]]):
    """主机 IP 反查 + 种子规律裂变"""
    log("[*] 执行高价值主机反查与种子规律裂变...")
    fission_candidates = []
    for ch, urls in candidate_pools.items():
        for u in urls:
            if re.search(r'cctv|hunan|jiangxi|beijing|dongfang|1080p', u, re.IGNORECASE):
                fission_candidates.append(u)
                if len(fission_candidates) >= 10:
                    break
        if len(fission_candidates) >= 10:
            break

    fission_added = 0
    for seed_url in fission_candidates:
        derived_pairs = extrapolate_channels_from_seed(seed_url)
        for target_ch, derived_url in derived_pairs:
            if target_ch in candidate_pools:
                if derived_url not in candidate_pools[target_ch] and is_valid_ipv4_url(derived_url):
                    candidate_pools[target_ch].add(derived_url)
                    fission_added += 1

    log(f"[+] 种子规律裂变衍生出 {fission_added} 个潜在高可能直播流。")


def main():
    log("=" * 70)
    log("    工作流 1：IPTV 全网大采集与标准化清洗（Harvester）")
    log("=" * 70)
    t_start = time.time()
    token = os.environ.get("GITHUB_TOKEN", "")

    # 加载用户定义的分类和频道结构
    structure = load_user_channel_structure()
    all_target_channels = []
    for ch_list in structure.values():
        all_target_channels.extend(ch_list)
    # 去重保序
    all_target_channels = list(dict.fromkeys(all_target_channels))
    candidate_pools: Dict[str, Set[str]] = {ch: set() for ch in all_target_channels}

    # 1. 载入已有历史底池
    load_existing_pools(candidate_pools)

    # 2. 全网多源多线程并发拉取
    search_all_github_sources(token, candidate_pools)

    # 3. 主机反查与种子裂变
    reverse_host_fission(token, candidate_pools)

    # 4. 导出为全量候选池 raw_sources.txt
    log(f"\n[*] 正在导出全量待测源至: {RAW_SOURCES_FILE} (全部清除 $ 标记)...")
    total_count = 0
    with open(RAW_SOURCES_FILE, "w", encoding="utf-8") as f:
        for cat_name, ch_list in structure.items():
            f.write(f"{cat_name},#genre#\n")
            for ch in ch_list:
                urls = sorted(list(candidate_pools.get(ch, set())))
                for u in urls:
                    clean_u = clean_stream_url(u)
                    f.write(f"{ch},{clean_u}\n")
                    total_count += 1

    log("=" * 70)
    log(f"[√] 采集工作流圆满完成！耗时: {time.time() - t_start:.2f} 秒")
    log(f"[√] 已为 {len(all_target_channels)} 个定制频道累计收集 {total_count} 条全网纯净直播源至 {RAW_SOURCES_FILE}")
    log("=" * 70)


if __name__ == "__main__":
    main()
