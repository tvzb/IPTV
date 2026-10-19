import json
import time
import requests
import concurrent.futures

TEST_KEYWORD = "功夫女足"
TIMEOUT = 5  # 超过 5 秒直接判定为慢源/超时

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def test_and_speed_site(site):
    """测试单个源站的连通性，并计算响应耗时"""
    # 爬虫/推送等系统内置聚合源，强制保留在最前
    if site.get("type") != 1:
        return site, 0.0, True

    api_url = site.get("api", "")
    if not api_url:
        return site, 999.0, False

    test_url = f"{api_url}?ac=detail&wd={requests.utils.quote(TEST_KEYWORD)}" if "?" not in api_url else f"{api_url}&ac=detail&wd={requests.utils.quote(TEST_KEYWORD)}"

    start_time = time.time()
    try:
        resp = requests.get(test_url, headers=HEADERS, timeout=TIMEOUT)
        latency = round(time.time() - start_time, 2)
        if resp.status_code == 200:
            data = resp.json()
            if "list" in data:
                return site, latency, True
    except Exception:
        pass
    
    return site, 999.0, False

def main():
    with open("tv.txt", "r", encoding="utf-8") as f:
        config = json.load(f)

    original_sites = config.get("sites", [])
    valid_results = []
    failed_sites = []

    print(f"开始对 {len(original_sites)} 个源站进行并发测速与有效性验证...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_site = {executor.submit(test_and_speed_site, site): site for site in original_sites}
        for future in concurrent.futures.as_completed(future_to_site):
            site, latency, is_alive = future.result()
            if is_alive:
                valid_results.append((site, latency))
                print(f"[可用] {site.get('name')} - 响应延迟: {latency}s")
            else:
                failed_sites.append(site.get('name'))
                print(f"[失效/超时] {site.get('name')}")

    # 核心步骤：按响应延迟从小到大排序（系统源 latency 为 0 始终排最前）
    valid_results.sort(key=lambda x: x[1])

    # 提取排序后的 site 列表并去重
    sorted_sites = []
    seen_keys = set()
    for site, latency in valid_results:
        key = site.get("key")
        if key not in seen_keys:
            seen_keys.add(key)
            sorted_sites.append(site)

    config["sites"] = sorted_sites

    with open("tv.txt", "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    print(f"\n优化完成：共保留 {len(sorted_sites)} 个源，已按响应速度由快到慢重排！")

if __name__ == "__main__":
    main()
