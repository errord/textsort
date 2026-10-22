#!/bin/bash
# bench20b.sh - fair re-run: warm the page cache before EVERY contestant,
# pure timing (no verification inside the timed window).
set -u
D=/data/labs/bc
TS=$D/textsort/textsort3
GS=$D/bin/gsort
DATA=$D/data_20g.txt
O=$D/bench20.o

timed() {  # timed NAME -- cmd...
  local name=$1; shift
  [ "$1" = "--" ] && shift
  cat $DATA > /dev/null     # equalize the page cache
  /usr/bin/time -v bash -c "$*" >$O.out 2>$O.tv
  local rc=$?
  local el=$(grep "Elapsed (wall clock)" $O.tv | sed 's/.*): *//' | tail -1)
  local rss=$(grep "Maximum resident set size" $O.tv | grep -oE "[0-9]+")
  printf "%-10s %-10s rss=%6dMB rc=%d\n" "$name" "$el" "$(( ${rss:-0} / 1024 ))" "$rc"
}

echo "=== sort (20G) ==="
timed gsort    "LC_ALL=C $GS $DATA -o $O.g1 && rm -f $O.g1"
timed tsort3   "$TS -q $DATA $O.t1 && rm -f $O.t1"
timed ts-ch512 "$TS -q --chunk-mb 512 $DATA $O.t1 && rm -f $O.t1"
timed polars   "python3 $D/pl_bench.py sort $DATA $O.p1 >/dev/null 2>&1 && rm -f $O.p1"

echo "=== sort -u (20G) ==="
timed gsort    "LC_ALL=C $GS -u $DATA -o $O.g1 && rm -f $O.g1"
timed tsort3   "$TS -q -u $DATA $O.t1 && rm -f $O.t1"
timed polars   "python3 $D/pl_bench.py uniq $DATA $O.p1 >/dev/null 2>&1 && rm -f $O.p1"

echo "=== grep->sort (20G, lvl=W) ==="
timed tsort3   "$TS -q --grep 'lvl=W' $DATA $O.t1 && rm -f $O.t1"
timed unix     "LC_ALL=C grep 'lvl=W' $DATA | $GS -o $O.g1 && rm -f $O.g1"
timed polars   "python3 $D/pl_bench.py grep_sort $DATA $O.p1 >/dev/null 2>&1 && rm -f $O.p1"

echo "=== correctness spot checks (untimed) ==="
$TS -q $DATA $O.ts
LC_ALL=C $GS $DATA -o $O.gs
cmp -s $O.ts $O.gs && echo "sort bytes == gsort" || echo "sort MISMATCH"
$TS -q -u $DATA $O.tsu
LC_ALL=C $GS -u $DATA -o $O.gsu
cmp -s $O.tsu $O.gsu && echo "sort -u bytes == gsort -u" || echo "sort -u MISMATCH"
rm -f $O.ts $O.gs $O.tsu $O.gsu
echo "DONE"
