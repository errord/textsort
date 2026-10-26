#!/bin/zsh
# test_all.sh - full correctness suite for the v3 engine (old + new operators)
set -u
T=/Users/biluochun/work/error.d/project/bigtext/bigtext
D=/tmp/tsb
cd $D
FAIL=0
ok()   { echo "OK   $1"; }
bad()  { echo "FAIL $1"; FAIL=1; }
chk()  { if cmp -s "$2" "$3"; then ok "$1"; else bad "$1"; fi; }

# ---------------- regression: the pre-existing suite ----------------
$T -q --freq 20 small.txt t_freq.txt && cmp -s g_freq.txt t_freq.txt && ok freq || bad freq
$T -q --mem-budget-mb 8 --tmpdir $D small.txt t_ext.txt && chk "external sort" t_ext.txt g_plain.txt
$T -q -u --mem-budget-mb 8 --tmpdir $D small.txt t_extu.txt && chk "external uniq" t_extu.txt g_uniq.txt
$T -q -c --mem-budget-mb 8 --tmpdir $D small.txt t_extc.txt && chk "external count" t_extc.txt g_count.txt
$T -q --no-sort --grep service small.txt t_stream.txt && chk "stream grep" t_stream.txt g_stream.txt
cat small.txt | $T -q - - | cmp -s - g_plain.txt && ok stdin/stdout || bad stdin/stdout
$T -q --shuffle small.txt t_shuf.txt && LC_ALL=C gsort t_shuf.txt | cmp -s - g_plain.txt && ok shuffle || bad shuffle

# ---------------- fixtures for the new operators ----------------
python3 - <<'EOF'
import random
random.seed(7)
# fields.tsv: 3 tab-separated columns; col2 numeric incl. negatives/decimals/junk
words = ["alpha","beta","gamma","delta","eps","zeta","eta","theta"]
with open("fields.tsv","w") as f:
    for i in range(5000):
        c1 = random.choice(words) + str(random.randint(1,50))
        c2 = random.choice(["%d"%random.randint(-9999,9999),
                            "%.3f"%(random.random()*100-50),
                            "n/a","%d"%random.randint(0,10**12)])
        c3 = " ".join(random.choices(words,k=3))
        f.write(f"{c1}\t{c2}\t{c3}\n")
# jsonl.txt: strings, escapes, unicode, nested, missing keys
import json
recs = [
    {"id":1,"text":"hello world","tags":["a","b"],"meta":{"k":1}},
    {"id":2,"text":"quote \" backslash \\ tab\tend","ok":True},
    {"id":3,"text":"unicode: 你好 \u00e9\u00e8","n":None},
    {"id":4,"other":"no text field here"},
    {"id":5,"text":"nested { brace } chars","num":3.14},
    {"id":6,"text":"tail"},
]
with open("jsonl.txt","w") as f:
    for r in recs: f.write(json.dumps(r)+"\n")
# pii.txt: emails and IPs in messy text
with open("pii.txt","w") as f:
    f.write("contact john.doe+news@ex-ample.co.uk about the invoice\n")
    f.write("from root@[IP] not an ip host\n")
    f.write("server 192.168.1.24 and 8.8.8.8 blocked 999.999.1.1 (not ip)\n")
    f.write("version 1.2.3.4.5 release and code v1.2.3 not ip either x11.22.33.44\n")
    f.write("no pii on this line at all\n")
    f.write("edge a@b an@x.org alice@example.com\n")
# lens.txt: varied lengths incl. empty-ish
with open("lens.txt","w") as f:
    for i in range(3000):
        n = random.choice([0,1,2,5,17,40,80,120])
        f.write("x"*n + "\n")
# nums.txt: numeric-first lines
with open("nums.txt","w") as f:
    for i in range(3000):
        v = random.choice([random.randint(-5000,5000), random.uniform(-99,99)])
        f.write(("%f"%v if isinstance(v,float) else str(v))+" rest\n")
EOF

# ---------------- --key / --num / --by-len ----------------
LC_ALL=C gsort -t$'\t' -k1,1 fields.tsv > g_key1.tsv
$T -q --key 1 fields.tsv t_key1.tsv && chk "key 1 == gsort -k1,1" t_key1.tsv g_key1.tsv
$T -q --key 1 --mem-budget-mb 8 --tmpdir $D fields.tsv t_key1e.tsv && chk "key 1 external" t_key1e.tsv g_key1.tsv

LC_ALL=C gsort -t$'\t' -k2,2n fields.tsv > g_key2n.tsv
$T -q --key 2 --num fields.tsv t_key2n.tsv && chk "key 2 numeric == gsort -k2,2n" t_key2n.tsv g_key2n.tsv

LC_ALL=C gsort -n nums.txt > g_num.txt
$T -q --num nums.txt t_num.txt && chk "numeric whole line == gsort -n" t_num.txt g_num.txt

perl -e 'print sort { length($a) <=> length($b) || $a cmp $b } <>' lens.txt > g_len.txt
$T -q --by-len lens.txt t_len.txt && chk "by-len == perl length sort" t_len.txt g_len.txt
$T -q --by-len -r lens.txt t_lenr.txt && perl -e 'print reverse sort { length($a) <=> length($b) || $a cmp $b } <>' lens.txt > g_lenr.txt && chk "by-len reverse" t_lenr.txt g_lenr.txt

# ---------------- --dedup-key (the bounded-memory uniq-by-field) ----------------
# NOTE: GNU sort -k1,1 -u skips the last-resort compare under -u (sort.c:
# "if (unique || stable) return 0"), so WHICH representative row survives is
# an implementation detail. Verify PROPERTIES instead of bytes:
#   (1) one row per distinct key  (2) keys strictly ascending
#   (3) every row exists in the input  (4) idempotent under itself
LC_ALL=C gsort -t$'\t' -k1,1 -u fields.tsv > g_dk.tsv
$T -q --dedup-key 1 fields.tsv t_dk.tsv
$T -q --dedup-key 1 --mem-budget-mb 8 --tmpdir $D fields.tsv t_dke.tsv
want=$(wc -l < g_dk.tsv); got=$(wc -l < t_dk.tsv)
[ "$got" = "$want" ] && ok "dedup-key 1 row count == distinct keys" || bad "dedup-key count: $got vs $want"
cmp -s t_dk.tsv t_dke.tsv && ok "dedup-key external == in-mem" || bad "dedup-key external != in-mem"
python3 - <<'EOF'
import sys
rows = set(open("fields.tsv").read().splitlines())
prev = None
good = True
for line in open("t_dk.tsv"):
    line = line.rstrip("\n")
    k = line.split("\t")[0]
    if prev is not None and k <= prev: good = False; break
    if line not in rows: good = False; break
    prev = k
print("OK   dedup-key properties" if good else "FAIL dedup-key properties")
sys.exit(0 if good else 1)
EOF
[ $? -ne 0 ] && FAIL=1
$T -q --key 1 -c fields.tsv t_kc.tsv && LC_ALL=C gsort -t$'\t' -k1,1 fields.tsv | awk -F'\t' '{c[$1]++} END{for(k in c) printf "%7d %s\n", c[k], k}' | LC_ALL=C gsort -k2 > g_kc.raw
# tsort3 -c emits count + whole first line; compare key multisets instead
awk '{print $1, $2}' t_kc.tsv | LC_ALL=C gsort > t_kc.keys
awk '{print $1, $2}' g_kc.raw | LC_ALL=C gsort > g_kc.keys
chk "key count (-c by field) counts match" t_kc.keys g_kc.keys

# ---------------- --min-len / --max-len ----------------
awk 'length($0)>=40' lens.txt > g_minl.txt
$T -q --no-sort --min-len 40 lens.txt t_minl.txt && chk "min-len 40 (stream order)" t_minl.txt g_minl.txt
awk 'length($0)<=5' lens.txt > g_maxl.txt
$T -q --no-sort --max-len 5 lens.txt t_maxl.txt && chk "max-len 5" t_maxl.txt g_maxl.txt

# ---------------- --jsonl ----------------
python3 -c '
import json,sys
for line in open("jsonl.txt"):
    r=json.loads(line)
    t=r.get("text")
    if t is None: continue
    print(t)' > g_jsonl.txt
$T -q --no-sort --jsonl text jsonl.txt t_jsonl.txt && chk "jsonl text field" t_jsonl.txt g_jsonl.txt

# ---------------- --mask-email / --mask-ip ----------------
cat > g_mask_email.txt <<'EOF'
contact [EMAIL] about the invoice
from root@[IP] not an ip host
server 192.168.1.24 and 8.8.8.8 blocked 999.999.1.1 (not ip)
version 1.2.3.4.5 release and code v1.2.3 not ip either x11.22.33.44
no pii on this line at all
edge a@b [EMAIL] [EMAIL]
EOF
$T -q --no-sort --mask-email pii.txt t_me.txt && chk "mask-email" t_me.txt g_mask_email.txt
cat > g_mask_ip.txt <<'EOF'
contact john.doe+news@ex-ample.co.uk about the invoice
from root@[IP] not an ip host
server [IP] and [IP] blocked 999.999.1.1 (not ip)
version 1.2.3.4.5 release and code v1.2.3 not ip either x11.22.33.44
no pii on this line at all
edge a@b an@x.org alice@example.com
EOF
$T -q --no-sort --mask-ip pii.txt t_mi.txt && chk "mask-ip" t_mi.txt g_mask_ip.txt
$T -q --no-sort --mask-email --mask-ip pii.txt t_memi.txt && cat > g_memi.txt <<'EOF'
contact [EMAIL] about the invoice
from root@[IP] not an ip host
server [IP] and [IP] blocked 999.999.1.1 (not ip)
version 1.2.3.4.5 release and code v1.2.3 not ip either x11.22.33.44
no pii on this line at all
edge a@b [EMAIL] [EMAIL]
EOF
chk "mask email+ip chained" t_memi.txt g_memi.txt

# ---------------- --wc / --nl ----------------
$T -q --no-sort --wc small.txt t_wc.txt
want=$(awk '{l+=1; w+=NF; b+=length($0)+1} END{printf "%d %d %d", l, w, b}' small.txt)
got=$(awk '{print $1, $2, $3}' t_wc.txt)
[ "$got" = "$want" ] && ok "wc counts == awk" || bad "wc counts: got=$got want=$want"
$T -q --no-sort --nl small.txt t_nl.txt && cat -n small.txt > g_nl.txt && chk "nl == cat -n" t_nl.txt g_nl.txt
$T -q --nl small.txt t_nls.txt && LC_ALL=C gsort small.txt | cat -n > g_nls.txt && chk "nl after sort" t_nls.txt g_nls.txt

# ---------------- freq with a key ----------------
$T -q --key 1 --freq 5 fields.tsv t_fk.tsv
python3 - <<'EOF'
import subprocess, collections
out = subprocess.run(["gsort","-t\t","-k1,1","fields.tsv"],capture_output=True,text=True).stdout
c = collections.Counter()
first = {}
for line in out.splitlines():
    k = line.split("\t")[0]
    c[k] += 1
    first.setdefault(k, line)
top = sorted(c.items(), key=lambda kv: (-kv[1], first[kv[0]]))[:5]
with open("g_fk.tsv","w") as f:
    for k,n in top:
        f.write("%7d %s\n" % (n, first[k]))
EOF
chk "freq by key (top5)" t_fk.tsv g_fk.tsv

# ---------------- combined pipelines ----------------
$T -q --grep service --min-len 30 small.txt t_combo.txt
grep service small.txt | awk 'length($0)>=30' | LC_ALL=C gsort > g_combo.txt
chk "grep+min-len+sort combo" t_combo.txt g_combo.txt
$T -q --mask-ip --key 2 --num --head 10 fields.tsv t_c2.tsv
perl -ne 's/\b(\d{1,3})(?:\.(?!\d))/(?1)./g; ' fields.tsv >/dev/null 2>&1  # (perl mask not used; do python)
python3 - <<'EOF'
import re, subprocess
lines = open("fields.tsv").read().splitlines()
def mask_ip(s):
    return re.sub(r'(?<![\w.])(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})(?![\w.])',
                  lambda m: "[IP]" if all(int(g)<=255 for g in m.groups()) else m.group(0), s)
def keyf(l):
    f = l.split("\t")
    try: return float(f[1].strip() or 0)
    except ValueError:
        d = ""
        for ch in f[1].lstrip():
            if ch in "0123456789+-.": d += ch
            else: break
        try: return float(d) if d else 0.0
        except ValueError: return 0.0
masked = [mask_ip(l) for l in lines]
masked.sort(key=lambda l: (keyf(l), l))
with open("g_c2.tsv","w") as f:
    for l in masked[:10]: f.write(l+"\n")
EOF
chk "mask-ip+num key+head combo" t_c2.tsv g_c2.tsv

echo "----"
[ $FAIL -eq 0 ] && echo "ALL PASS" || echo "SOME FAILED"
exit $FAIL
