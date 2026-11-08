import json,sys
from pathlib import Path
p=json.loads(Path(sys.argv[1] if len(sys.argv)>1 else 'custom_payload.json').read_text('utf-8'))
out={}
for g in p.get('groups',[]):
 name=str(g.get('name','频道'))[:60]
 vals=[]
 for c in g.get('channels',[]):
  v=str(c.get('name') or c.get('channel_id') or '').strip()
  if v and v not in vals: vals.append(v)
 if vals: out[name]=vals
if not out: raise SystemExit('No selected channels')
Path('custom_channels.json').write_text(json.dumps(out,ensure_ascii=False,indent=2),encoding='utf-8')
print('prepared',sum(map(len,out.values())),'channels')
