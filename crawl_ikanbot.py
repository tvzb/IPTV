import json
import re
import urllib.parse
from playwright.sync_api import sync_playwright

TARGET_URL = "https://www1.ikanbot.com/play/995772"

# 常见切片与资源站特征域名映射表（持续扩展）
KNOWN_API_MAP = {
    "lz": {"name": "量子资源", "api": "https://cj.lziapi.com/api.php/provide/vod/", "playUrl": "https://lziplayer.com/?url="},
    "ff": {"name": "非凡资源", "api": "http://api.ffzyapi.com/api.php/provide/vod/", "playUrl": "https://svip.ffzyplay.com/?url="},
    "bf": {"name": "暴风资源", "api": "https://bfzyapi.com/api.php/provide/vod/", "playUrl": "https://vip.vipuuvip.com/?url="},
    "sn": {"name": "索尼资源", "api": "https://suoniapi.com/api.php/provide/vod/", "playUrl": "https://dxfbk.com/?url="},
    "wl": {"name": "卧龙资源", "api": "https://collect.wolongzy.cc/api.php/provide/vod/", "playUrl": "https://jx.wolongm3u8.com:65/m3u8.php?url="},
    "hn": {"name": "红牛资源", "api": "https://www.hongniuzy2.com/api.php/provide/vod/", "playUrl": "https://www.hnjiexi.com/m3u8/?url="},
    "db": {"name": "豆瓣资源", "api": "https://caiji.dbzy5.com/api.php/provide/vod/", "playUrl": "https://dbzy99.com:699/?url="},
    "dytt": {"name": "电影天堂", "api": "http://caiji.dyttzyapi.com/api.php/provide/vod", "playUrl": "https://vip.dyttzyplay.com/?url="},
    "gs": {"name": "光速资源", "api": "https://api.guangsuapi.com/api.php/provide/vod/", "playUrl": "https://www.gszyv.com/m3u8/?url="},
    "js": {"name": "极速资源", "api": "https://jszyapi.com/api.php/provide/vod/", "playUrl": "https://jisuzyjx.com/play/?url="},
    "wujin": {"name": "无尽资源", "api": "https://api.wujinapi.me/api.php/provide/vod/", "playUrl": "https://jx.xllmcms.com/dplayer/?url="},
    "subo": {"name": "速播资源", "api": "https://subocj.com/api.php/provide/vod/", "playUrl": "https://subojiexi.com/play/?url="},
    "jy": {"name": "金鹰资源", "api": "https://jyzyapi.com/provide/vod/from/jinyingm3u8/at/json", "playUrl": "https://hd.iapijy.com/play?url="},
    "zd": {"name": "最大资源", "api": "https://api.zuidapi.com/api.php/provide/vod/", "playUrl": "https://jx.zdplay.cc/m3u8Player/?url="}
}

captured_urls = set()

def handle_request(request):
    url = request.url
    # 捕获携带 m3u8、解析器 jx、或者播放特征的请求
    if ".m3u8" in url or "jx.php" in url or "jiexi" in url or "player" in url:
        captured_urls.add(url)

def extract_domain(url):
    parsed = urllib.parse.urlparse(url)
    return f"{parsed.scheme}://{parsed.netloc}"

def run():
    print(f"正在启动嗅探引擎，抓取目标: {TARGET_URL} ...")
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        )
        page = context.new_page()
        page.on("request", handle_request)

        try:
            page.goto(TARGET_URL, wait_until="networkidle", timeout=30000)
        except Exception:
            page.goto(TARGET_URL, wait_until="domcontentloaded", timeout=30000)

        # 匹配页面中的所有线路标签（根据 ikanbot 的前端结构通常是 tab 或列表项）
        # 查找所有包含“线路”或播放选集的按钮
        buttons = page.locator("button, a, li").filter(has_text=re.compile(r"线路\d+|高清|正片|HD")).all()
        print(f"共发现 {len(buttons)} 个可交互线路/选集节点，开始自动并发触发...")

        for idx, btn in enumerate(buttons):
            try:
                if btn.is_visible():
                    btn.click(timeout=1500)
                    page.wait_for_timeout(800)  # 等待数据请求发出
            except Exception:
                continue

        browser.close()

    print(f"\n嗅探完成，共捕获 {len(captured_urls)} 个网络流请求。")

    # 解析捕获的请求，生成站点模板
    new_found_sites = []
    registered_domains = set()

    for url in captured_urls:
        domain = extract_domain(url)
        if domain in registered_domains:
            continue

        matched_site = None
        for key, info in KNOWN_API_MAP.items():
            if key in domain or any(part in url for part in [info["name"]]):
                matched_site = {
                    "key": f"{info['name']}_嗅探",
                    "name": info["name"],
                    "type": 1,
                    "api": info["api"],
                    "playUrl": info["playUrl"],
                    "searchable": 1,
                    "quickSearch": 1,
                    "categories": ["动作片", "喜剧片", "科幻片", "欧美剧", "国产剧", "日韩剧", "短剧"]
                }
                break

        if matched_site:
            new_found_sites.append(matched_site)
            registered_domains.add(domain)
        elif ".m3u8" in url:
            # 未在映射表内的未知新源：提取其解析地址作为备用源
            site_key = f"发现源_{parsed_hostname(url)}"
            new_found_sites.append({
                "key": site_key,
                "name": site_key,
                "type": 1,
                "api": f"{domain}/api.php/provide/vod/",
                "playUrl": "",
                "searchable": 1,
                "quickSearch": 1
            })
            registered_domains.add(domain)

    merge_to_tv_txt(new_found_sites)

def parsed_hostname(url):
    try:
        return urllib.parse.urlparse(url).netloc.split(".")[0]
    except:
        return "未知"

def merge_to_tv_txt(new_sites):
    with open("tv.txt", "r", encoding="utf-8") as f:
        tv_data = json.load(f)

    existing_sites = tv_data.get("sites", [])
    existing_apis = {s.get("api") for s in existing_sites if "api" in s}

    added_count = 0
    for s in new_sites:
        if s.get("api") not in existing_apis:
            existing_sites.append(s)
            existing_apis.add(s.get("api"))
            added_count += 1
            print(f"[新增源] 成功加入: {s.get('name')} -> {s.get('api')}")

    tv_data["sites"] = existing_sites

    with open("tv.txt", "w", encoding="utf-8") as f:
        json.dump(tv_data, f, ensure_ascii=False, indent=2)

    print(f"\n合并完成：新增 {added_count} 个接口，tv.txt 总站点数达 {len(existing_sites)} 个。")

if __name__ == "__main__":
    run()
