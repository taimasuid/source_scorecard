
import sys
import json
import ujson

d = {}

N = 10000

for line in sys.stdin:

    try:

        k, v = line.strip().partition('\t')[::2]

        v = ujson.loads(v)

    except:
        continue

    sources = set([smdt.get('sourceAlias','') for smdt in v.get('assertedRelationship',{}).get('sourceMetadata',[]) if smdt.get('sourceAlias',None)])

    if len(sources)==0:
        continue

    l = len(sources)
    d.setdefault(l,0)

    if d[l]<N:
        print line.strip()

        d[l] += 1

print >> sys.stderr, json.dumps(d,indent=4,sort_keys=True)



