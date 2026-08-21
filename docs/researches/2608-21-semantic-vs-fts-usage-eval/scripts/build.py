import json, collections, os
calls = [json.loads(l) for l in open('calls.ndjson')]
stream = collections.defaultdict(list)
for l in open('stream.ndjson'):
    r = json.loads(l); stream[r['session_id']].append(r)
# collapse to events per session in order
def events(rows):
    ev = []
    for r in rows:
        if r.get('ptype') == 'tool_call':
            ev.append(('call', r.get('tool_name'), r.get('body'), r.get('call_id'), r['timestamp'], r['message_id']))
        elif r.get('ptype') == 'tool_result':
            ev.append(('result', r.get('tool_name'), ('FAIL ' if r.get('is_failure') else '') + (r.get('body') or ''), r.get('call_id'), r['timestamp'], r['message_id']))
        elif r.get('text'):
            ev.append((r['role'], None, r.get('text'), None, r['timestamp'], r['message_id']))
    # dedupe plain text rows duplicated by join (same message_id, no part)
    out=[]; seen=set()
    for e in ev:
        k=(e[0],e[5],e[2][:50])
        if e[0] in ('user','assistant') and k in seen: continue
        seen.add(k); out.append(e)
    return out
ev_by_s = {s: events(r) for s, r in stream.items()}
windows = []
for i, c in enumerate(calls):
    ev = ev_by_s[c['session_id']]
    idx = next((k for k, e in enumerate(ev) if e[0]=='call' and e[3]==c['call_id']), None)
    before = [e for e in ev[max(0,(idx or 0)-6):idx or 0] if e[0] in ('user','assistant')][-3:]
    after = []
    if idx is not None:
        for e in ev[idx+1:]:
            if e[0]=='result' and e[3]==c['call_id']: continue
            after.append(e)
            if len(after) >= 8: break
    def fmt(e):
        kind, tool, body, *_ = e
        body = (body or '')
        if kind=='call': return f"[TOOL CALL {tool}] {body[:400]}"
        if kind=='result': return f"[TOOL RESULT {tool}] {body[:300]}"
        return f"[{kind.upper()}] {body[:800]}"
    windows.append({
        'n': i+1, 'call_id': c['call_id'], 'session_id': c['session_id'], 'ts': c['timestamp'], 'mode': c['mode'].strip('"'),
        'agent': c['agent'], 'project': c['project'],
        'params': c['params'],
        'context_before': [fmt(e) for e in before],
        'result_head': (c['result_head'] or '')[:1800],
        'after': [fmt(e) for e in after],
        'idx_found': idx is not None,
    })
json.dump(windows, open('windows.json','w'))
print(len(windows), sum(1 for w in windows if not w['idx_found']), 'missing idx')
os.makedirs('batches', exist_ok=True)
B=40
for b in range(0, len(windows), B):
    json.dump(windows[b:b+B], open(f'batches/b{b//B:02d}.json','w'), indent=0)
print('batches', (len(windows)+B-1)//B)
