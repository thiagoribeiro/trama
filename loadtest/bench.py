#!/usr/bin/env python3
"""Throughput summary of scenario result dirs: wf/s, Trama CPU ms per workflow (busy window),
Postgres commits/s peak, P50 latency. Usage: loadtest/bench.py results/<set>/<scenario> ..."""
import sys,csv,collections,json,os,re
for f in sys.argv[1:]:
    rows=[r for r in csv.reader(open(f+'/metrics.csv')) if len(r)==4 and r[0]!='epochMs']
    cpu=collections.defaultdict(float); proc=collections.defaultdict(float); commits=[]
    for t,src,m,v in rows:
        t=int(t)
        if m=='cpu_pct' and src.startswith('trama'): cpu[t]+=float(v)
        if m=='processed_total': proc[t]+=float(v)
        if src=='postgres' and m=='xact_commit_total': commits.append((t,float(v)))
    pts=sorted(proc.items()); act=[t for (t,v),(t0,v0) in zip(pts[1:],pts[:-1]) if v>v0]
    lo,hi=min(act),max(act)
    cs=sum(v*5/100 for t,v in cpu.items() if lo<=t<=hi)
    d=json.load(open(f+'/summary.json')); n=sum(d['status'].values())
    commits.sort(); cr=[(b[1]-a[1])/((b[0]-a[0])/1000) for a,b in zip(commits,commits[1:]) if b[0]>a[0]]
    print('%-55s wf/s=%6.1f cpu_ms/wf=%5.1f pg_commits_peak=%5.0f p50=%s stuck=%s' % (f[-50:], d.get('throughputPerSec'), cs*1000/n, max(cr), d['latencyMs']['p50'], d['stuck']))
