import json,glob,re,subprocess,random,time,sys
calls={json.loads(l)['call_id']:json.loads(l) for l in open('calls.ndjson')}
V={}
for f in sorted(glob.glob('verdicts/v*.json')):
    for v in json.load(open(f)): V[v['call_id']]=v
found=[c for cid,c in calls.items() if V.get(cid,{}).get('outcome')=='FOUND' and c['mode']!='"fts"' and c['timestamp']>='2026-07-03']
random.seed(7); sample=random.sample(found,120)
out=[]
for i,c in enumerate(sample):
    p=json.loads(c['params'])
    orig=re.findall(r'--- session \[\d+\][^\n]*\| ([0-9a-f-]{36}(?:/[^ |]+)?) ---', c['result_head'])
    if not orig: continue
    args=['pond','search','--format','json','--limit','10']
    for k,fl in (('project','--project'),('from_date','--from-date'),('to_date','--to-date'),('source_agent',None),('session_id','--session-id')):
        if p.get(k) and fl: args+=[fl,str(p[k])]
    def run(mode):
        # check=True: a refused or failing search must abort the replay, not
        # score as "zero hits in 0.2s" and quietly measure nothing.
        t=time.time(); r=subprocess.run(args+['--mode',mode,p['query']],capture_output=True,text=True,timeout=120,check=True); dt=time.time()-t
        return [s['session_id'] for s in json.loads(r.stdout)['sessions']],dt
    fts,tf=run('fts'); vec,tv=run('vector')
    out.append(dict(call_id=c['call_id'],query=p['query'],orig_top=orig[:3],fts_top=fts,vec_top=vec,tf=tf,tv=tv,
        fts_has_top1=orig[0] in fts, fts_has_any3=any(o in fts for o in orig[:3]), vec_has_top1=orig[0] in vec))
    print(i,'fts_top1' if orig[0] in fts else '-', 'fts_any3' if any(o in fts for o in orig[:3]) else '-', f'{tf:.1f}s/{tv:.1f}s', p['query'][:60], file=sys.stderr)
json.dump(out,open('replay.json','w'),indent=1)
n=len(out); print('n',n,'fts has orig top1:',sum(o['fts_has_top1'] for o in out),'fts has any of orig top3:',sum(o['fts_has_any3'] for o in out),'vector(today) has orig top1:',sum(o['vec_has_top1'] for o in out))
print('mean latency fts %.2fs vector %.2fs'%(sum(o['tf'] for o in out)/n,sum(o['tv'] for o in out)/n))
