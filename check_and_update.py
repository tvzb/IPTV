import json
import requests
import concurrent.futures

# 测试关键词
TEST_KEYWORD = "功夫女足"
TIMEOUT = 6  # 超时时间(秒)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

def test_site(site):
    """测试单个采集源的连通性和有效性"""
    if site.get("type") != 1:
        # 非 CMS 采集类（如爬虫/推送）保留
        return site, True

    api_url = site.get("api", "")
    if not api_url:
        return site, False

    test_url = f"{api_url}?ac=detail&wd={requests.utils.quote(TEST_KEYWORD)}" if "?" not in api_url else f"{api_url}&ac=detail&wd={requests.utils.quote(TEST_KEYWORD)}"

    try:
        resp = requests.get(test_url, headers=HEADERS, timeout=TIMEOUT)
        if resp.status_code == 200:
            data = resp.json()
            # 只要能正常解析返回列表即认为接口存活
            if "list" in data:
                return site, True
    except Exception:
        pass
    
    return site, False

def main():
    with open("tv.txt", "r", encoding="utf-8") as f:
        config = json.load(f)

    original_sites = config.get("sites", [])
    valid_sites = []
    failed_sites = []

    print(f"开始测试 {len(original_sites)} 个源站...")

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        future_to_site = {executor.submit(test_site, site): site for site in original_sites}
        for future in concurrent.futures.as_completed(future_to_site):
            site, is_alive = future.result()
            if is_alive:
                valid_sites.append(site)
                print(f"[可用] {site.get('name')}")
            else:
                failed_sites.append(site.get('name'))
                print(f"[失效] {site.get('name')}")

    # 保持原顺序或去重
    seen_keys = set()
    deduped_sites = []
    for s in valid_sites:
        key = s.get("key")
        if key not in seen_keys:
            seen_keys.add(key)
            deduped_sites.append(s)

    config["sites"] = deduped_sites

    with open("tv.txt", "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=2)

    print(f"\n检测完成：共保留 {len(deduped_sites)} 个有效源，剔除 {len(failed_sites)} 个失效源。")

if __name__ == "__main__":
    main()
