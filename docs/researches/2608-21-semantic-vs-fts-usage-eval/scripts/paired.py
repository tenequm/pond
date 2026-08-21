import json,random,subprocess,sys
w=json.load(open('windows.json')); random.seed(23)
vec=[x for x in w if x['mode']!='fts' and x['ts']>='2026-07-03']; fts=[x for x in w if x['mode']=='fts']
sample=random.sample(vec,50)+random.sample(fts,40)
out=[]
for i,x in enumerate(sample):
    p=json.loads(x['params']); args=['pond','search','--limit','6']
    for k,fl in (('project','--project'),('from_date','--from-date'),('to_date','--to-date'),('session_id','--session-id')):
        if p.get(k): args+=[fl,str(p[k])]
    def run(m):
        r=subprocess.run(args+['--mode',m,p['query']],capture_output=True,text=True,timeout=120); return r.stdout[:3500]
    a,b=run('vector'),run('fts'); flip=random.random()<0.5
    out.append(dict(n=i+1,call_id=x['call_id'],orig_mode=x['mode'],query=p['query'],filters={k:p[k] for k in ('project','from_date','to_date','session_id') if p.get(k)},
        context_before=x['context_before'],A=b if flip else a,B=a if flip else b,key={'A':'fts' if flip else 'vector','B':'vector' if flip else 'fts'}))
    print(i,file=sys.stderr)
json.dump(out,open('paired_full.json','w'))
blind=[{k:v for k,v in o.items() if k not in('key','orig_mode')} for o in out]
json.dump(blind[:45],open('batches/p0.json','w')); json.dump(blind[45:],open('batches/p1.json','w'))
print(len(out))
