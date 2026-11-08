#!/usr/bin/env python3
"""Convert the existing collector's live.txt into normalized catalogue + free test playlist."""
import json, os, re, datetime
from pathlib import Path

SOURCE = Path(os.getenv('SOURCE_PLAYLIST', 'live.txt'))
OUT = Path(os.getenv('CATALOG_PATH', 'channels.json'))
TEST = Path(os.getenv('TEST_PATH', 'test.txt'))
TEST_CCTV = int(os.getenv('TEST_CCTV_COUNT', '5'))
TEST_SAT = int(os.getenv('TEST_SAT_COUNT', '5'))

def parse(path):
    groups, current = [], None
    if not path.exists(): return groups
    for raw in path.read_text('utf-8', errors='ignore').splitlines():
        line = raw.strip().lstrip('\ufeff')
        if not line or line.startswith('#'): continue
        if line.endswith(',#genre#'):
            name = line[:-8].rstrip(',')
            if name in ('更新时间', '频道定制'): current = None; continue
            current = {'name': name, 'channels': []}; groups.append(current); continue
        if current and ',' in line:
            name, url = line.split(',', 1)
            if url.strip().lower().startswith(('http://','https://')):
                current['channels'].append({'name': name.strip(), 'streams':[{'url':url.strip(),'quality':'unknown','speed_kbs':None}]})
    return groups

groups = parse(SOURCE)
now = datetime.datetime.now(datetime.timezone.utc).isoformat()
for g in groups:
    seen=set(); clean=[]
    for ch in g['channels']:
        key=ch['name'].casefold()
        if key in seen: continue
        seen.add(key); clean.append(ch)
    g['channels']=clean
catalog={'schema_version':1,'generated_at':now,'source':'main-collector','groups':groups}
OUT.write_text(json.dumps(catalog,ensure_ascii=False,indent=2),encoding='utf-8')

lines=[]
for g in groups:
    limit = TEST_CCTV if ('央' in g['name'] or 'CCTV' in g['name'].upper()) else TEST_SAT if ('卫视' in g['name'] or '卫' in g['name']) else 0
    if not limit: continue
    chosen=g['channels'][:limit]
    if chosen:
        lines.append(f"{g['name']},#genre#")
        for ch in chosen:
            for s in ch['streams']:
                lines.append(f"{ch['name']},{s['url']}")
TEST.write_text('\n'.join(lines)+'\n',encoding='utf-8')
print(f"catalog groups={len(groups)} channels={sum(len(g['channels']) for g in groups)}; test bytes={TEST.stat().st_size}")
