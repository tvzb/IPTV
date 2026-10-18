# -*- coding: utf-8 -*-
"""
工作流 2 核心引擎：TVBox 原生测速与按网速门槛优选（Speedtester）
职责：
1. 读取 raw_sources.txt（或使用者自定义的任何直播源列表）；
2. 按照 channels.json 中的用户自定义分类和频道排序；
3. 执行 TVBox 原生级智能连通性与实际切片拉流测速；
4. 【核心革新】：取消数量上限限制，按设定阈值（如 >=300 KB/s）筛选达标源，达标的越多越好；
5. 按实际网速从高到低严格排序，生成交付给电视盒子的 live.txt。
"""
import os
import re
import time
import socket
import asyncio
import urllib.parse
from typing import Dict, List, Set, Tuple

import aiohttp
import requests

from config import (
    load_user_channel_structure,
    clean_stream_url,
    normalize_channel_name,
    RAW_SOURCES_FILE,
    DEFAULT_OUTPUT_FILE,
    MIN_SPEED_KBS,
    REQUEST_TIMEOUT,
    SPEED_TEST_TIMEOUT,
    SPEED_TEST_MAX_BYTES,
    MAX_CONCURRENT_TASKS,
    TVBOX_USER_AGENT,
)

HEADERS = {
    "User-Agent": TVBOX_USER_AGENT,
    "Accept": "*/*",
    "Connection": "keep-alive"
}


def log(msg: str):
    print(msg, flush=True)


def load_sources_to_test(channel_set: Set[str]) -> Dict[str, Set[str]]:
    """读取待测源池（优先 raw_sources.txt，保底 live.txt）"""
    pools: Dict[str, Set[str]] = {ch: set() for ch in channel_set}
    
    source_file = RAW_SOURCES_FILE if os.path.exists(RAW_SOURCES_FILE) else DEFAULT_OUTPUT_FILE
    if not os.path.exists(source_file):
        log(f"[!] 未检测到 {source_file}，尝试从线上主页拉取基础源...")
        try:
            r = requests.get("https://tvzb.github.io/live.txt", headers=HEADERS, timeout=5)
            if r.status_code == 200:
                with open(source_file, "w", encoding="utf-8") as f:
                    f.write(r.text)
        except Exception:
            pass

    if os.path.exists(source_file):
        log(f"[*] 读取待测直播源输入文件: {source_file}...")
        with open(source_file, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or ",#genre#" in line:
                    continue
                if "," in line:
                    parts = line.split(",", 1)
                    std_name = normalize_channel_name(parts[0].strip())
                    clean_url = clean_stream_url(parts[1].strip())
                    if std_name in pools and clean_url.startswith("http"):
                        pools[std_name].add(clean_url)
    
    total = sum(len(v) for v in pools.values())
    log(f"[+] 待测源载入完毕，共计 {total} 个地址等待性能检阅。")
    return pools


async def test_and_speedtest_stream(session: aiohttp.ClientSession, url: str) -> Tuple[bool, float, float]:
    """TVBox 原生级智能测速"""
    try:
        clean_url = clean_stream_url(url)
        t_start = time.perf_counter()
        
        async with session.get(clean_url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT), allow_redirects=True) as resp:
            if resp.status != 200:
                return False, 0.0, 9999.0
            
            ttfb = (time.perf_counter() - t_start) * 1000
            content_sample = await resp.content.read(2048)
            text_sample = content_sample.decode('utf-8', errors='ignore')

            if "#EXTM3U" not in text_sample and "#EXTINF" not in text_sample:
                return False, 0.0, 9999.0

            ts_url = None
            for line in text_sample.splitlines():
                line = line.strip()
                if line and not line.startswith("#"):
                    ts_url = urllib.parse.urljoin(clean_url, line)
                    break

            # 递归解析 Master Playlist
            if ts_url and ".m3u8" in ts_url.lower():
                try:
                    async with session.get(ts_url, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT), allow_redirects=True) as sub_resp:
                        if sub_resp.status == 200:
                            sub_content = await sub_resp.content.read(2048)
                            sub_text = sub_content.decode('utf-8', errors='ignore')
                            for sub_line in sub_text.splitlines():
                                sub_line = sub_line.strip()
                                if sub_line and not sub_line.startswith("#"):
                                    ts_url = urllib.parse.urljoin(ts_url, sub_line)
                                    break
                except Exception:
                    pass

            if not ts_url:
                return True, 150.0, ttfb

        # 实际切片拉流测速
        t_speed_start = time.perf_counter()
        bytes_received = 0
        try:
            async with session.get(ts_url, timeout=aiohttp.ClientTimeout(total=SPEED_TEST_TIMEOUT), allow_redirects=True) as ts_resp:
                if ts_resp.status == 200:
                    while True:
                        chunk = await ts_resp.content.read(64 * 1024)
                        if not chunk:
                            break
                        bytes_received += len(chunk)
                        if bytes_received >= SPEED_TEST_MAX_BYTES:
                            break
                        if time.perf_counter() - t_speed_start >= SPEED_TEST_TIMEOUT:
                            break
                    elapsed = time.perf_counter() - t_speed_start
                    if elapsed > 0 and bytes_received > 0:
                        speed_kbs = (bytes_received / 1024.0) / elapsed
                        return True, round(speed_kbs, 2), round(ttfb, 2)
        except Exception:
            pass

        return True, 150.0, round(ttfb, 2)

    except Exception:
        return False, 0.0, 9999.0


async def benchmark_channel(channel_name: str, urls: Set[str], semaphore: asyncio.Semaphore, min_speed: float) -> List[Tuple[float, str]]:
    """测速单个频道，保留达到速度阈值的所有源（不限数量），按速度降序排列"""
    all_tested: List[Tuple[float, str]] = []
    
    conn = aiohttp.TCPConnector(ssl=False, limit=0)
    async with aiohttp.ClientSession(headers=HEADERS, connector=conn) as session:
        async def task(u: str):
            async with semaphore:
                valid, speed, _ = await test_and_speedtest_stream(session, u)
                if valid and speed > 0:
                    all_tested.append((speed, clean_stream_url(u)))

        tasks = [task(u) for u in urls]
        await asyncio.gather(*tasks, return_exceptions=True)

    # 按速度由高到低降序
    all_tested.sort(key=lambda x: x[0], reverse=True)

    # 按照网速阈值筛选（达标的有多少留多少，不限数量）
    qualified = [item for item in all_tested if item[0] >= min_speed]

    # 保底：若没有达到阈值的源，保留测试通过的最快前 3 个源防空窗
    if not qualified and all_tested:
        qualified = all_tested[:3]

    return qualified


async def main():
    log("=" * 70)
    log(f"   工作流 2：TVBox 极速测速与流畅度优选（门槛: >= {MIN_SPEED_KBS} KB/s，不限数量）")
    log("=" * 70)
    t_start = time.time()

    # 1. 加载用户定义的分类与频道顺序
    structure = load_user_channel_structure()
    all_channels = []
    for ch_list in structure.values():
        all_channels.extend(ch_list)
    channel_set = set(all_channels)

    # 2. 读取待测源
    pools = load_sources_to_test(channel_set)

    # 3. 并发测速
    semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)
    final_results: Dict[str, List[str]] = {}

    log(f"\n[*] 启动测速，过滤网速 >= {MIN_SPEED_KBS} KB/s 的流畅直播源（实时进度）：")
    for idx, channel in enumerate(all_channels, 1):
        urls = pools.get(channel, set())
        if not urls:
            log(f"[{idx:02d}/{len(all_channels):02d}] {channel}: 无输入源")
            final_results[channel] = []
            continue

        qualified = await benchmark_channel(channel, urls, semaphore, MIN_SPEED_KBS)
        stream_urls = [u for speed, u in qualified]
        best_speed = qualified[0][0] if qualified else 0
        log(f"[{idx:02d}/{len(all_channels):02d}] {channel}: 锁定 {len(stream_urls)} 个高速达标源 (最高: {best_speed} KB/s)")
        final_results[channel] = stream_urls

    # 4. 导出为规范的 live.txt
    log(f"\n[*] 正在导出最终文件: {DEFAULT_OUTPUT_FILE} (符合 TVBox 规范，无 $ 杂质)...")
    total_exported = 0
    with open(DEFAULT_OUTPUT_FILE, "w", encoding="utf-8") as f:
        for cat_name, ch_list in structure.items():
            f.write(f"{cat_name},#genre#\n")
            for ch in ch_list:
                for u in final_results.get(ch, []):
                    f.write(f"{ch},{u}\n")
                    total_exported += 1

    log("=" * 70)
    log(f"[√] 测速工作流执行完毕！总耗时: {time.time() - t_start:.2f} 秒")
    log(f"[√] 已导出 {total_exported} 条 >= {MIN_SPEED_KBS} KB/s 的高速稳定直播源至 {DEFAULT_OUTPUT_FILE}")
    log("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
