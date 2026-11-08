import json,re,sys
from pathlib import Path
p=json.loads(Path('custom_payload.json').read_text('utf-8'))
source=Path('live.txt').read_text('utf-8',errors='ignore').splitlines()
groups={};current=None
for line in source:
 line=line.strip()
 if line.endswith(',#genre#'):
  name=line[:-8].rstrip(',')
  if name in ('更新时间','频道定制'):current=None;continue
  current={'name':name,'lines':[]};groups[name]=current;continue
 if current and ',' in line:
  n,u=line.split(',',1)
  if u.startswith(('http://','https://')):current['lines'].append((n,u))
lines=[];missing=[]
for g in p.get('groups',[]):
 found=[]
 for c in g.get('channels',[]):
  wanted=str(c.get('name') or c.get('channel_id') or '').casefold()
  pair=None
  for block in groups.values():
   for n,u in block['lines']:
    if n.casefold()==wanted or n.casefold()==str(c.get('display_name','')).casefold():pair=(c.get('display_name') or n,u);break
   if pair:break
  if pair:found.append(pair)
  else:missing.append(c.get('name') or c.get('channel_id'))
 if found:
  lines.append(f"{g.get('name','频道')},#genre#")
  lines.extend(f'{n},{u}' for n,u in found)
if not lines:raise SystemExit('No channels passed validation; no custom playlist published')
slug=re.sub(r'[^A-Za-z0-9_-]','',p.get('slug','user'))[:32] or 'user'
Path(f'{slug}.txt').write_text('\n'.join(lines)+'\n',encoding='utf-8')
merged=False
if p.get('include_standard') and Path('standard_live.txt').exists():
 base=[];active=False;seen=set()
 for line in Path('standard_live.txt').read_text('utf-8',errors='ignore').splitlines():
  if line in ('更新时间,#genre#','频道定制,#genre#'):active=False;continue
  if line.endswith(',#genre#'):active=True
  if not active:continue
  if line.endswith(',#genre#'):
   base.append(line);continue
  if ',' in line:
   name,url=line.split(',',1);key=name.casefold()
   if key in seen:continue
   seen.add(key);base.append(line)
 custom=[];active=False
 for line in lines:
  if line.endswith(',#genre#'):
   active=True;custom.append(line);continue
  if active and ',' in line:
   name,url=line.split(',',1);key=name.casefold()
   if key not in seen:seen.add(key);custom.append(line)
 Path(f'{slug}-merged.txt').write_text('\n'.join(base+custom)+'\n',encoding='utf-8');merged=True
Path('custom_result.json').write_text(json.dumps({'order_id':p.get('order_id'),'slug':slug,'missing':missing,'channel_count':sum(1 for x in lines if ',http' in x),'merged':merged},ensure_ascii=False,indent=2),encoding='utf-8')
print('custom playlist generated; missing=',len(missing))
if missing:print('Unavailable channels:',','.join(map(str,missing)))
