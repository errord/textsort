#!/bin/bash
# bench20.sh - 20GB benchmark: tsort3 vs GNU coreutils-9.7 sort vs polars vs awk
# One timed round per competitor per suite; wall time + peak RSS via GNU time -v.
set -u
D=/data/labs/bc
TS=$D/textsort/textsort3
GS=$D/bin/gsort
GU=$D/bin/guniq
MLR=$D/bin/mlr
DATA=${DATA:-$D/data_20g.txt}
O=$D/bench20.o
TAB="$(printf '\t')"

timed() {  # timed NAME -- cmd...
  local name=$1; shift
  [ "$1" = "--" ] && shift
  /usr/bin/time -v bash -c "$*" >$O.out 2>$O.tv
  local rc=$?
  local el=$(grep "Elapsed (wall clock)" $O.tv | sed 's/.*): *//;s/h:mm:ss or m:ss: *//' | tail -1)
  local rss=$(grep "Maximum resident set size" $O.tv | grep -oE "[0-9]+")
  printf "%-8s %-10s rss=%6dMB rc=%d\n" "$name" "$el" "$(( ${rss:-0} / 1024 ))" "$rc"
}

run_suite() {
  echo "=== $1 ==="; shift
  for pair in "$@"; do
    timed "${pair%%|*}" "${pair#*|}"
  done
}

run_suite "sort (20G)" \
  "gsort9.7|LC_ALL=C $GS $DATA -o $O.ref_sort" \
  "tsort3|$TS -q $DATA $O.ts && cmp -s $O.ts $O.ref_sort && rm -f $O.ts" \
  "polars|python3 $D/pl_bench.py sort $DATA $O.pl >/dev/null 2>&1 && rm -f $O.pl"

run_suite "sort -u (20G)" \
  "tsort3|$TS -q -u $DATA $O.ts && LC_ALL=C $GS -u $DATA -o $O.gs && cmp -s $O.ts $O.gs && rm -f $O.ts $O.gs" \
  "polars|python3 $D/pl_bench.py uniq $DATA $O.pl >/dev/null 2>&1 && rm -f $O.pl"

run_suite "uniq -c (20G)" \
  "tsort3|$TS -q -c $DATA $O.ts && rm -f $O.ts" \
  "unix|LC_ALL=C $GS $DATA | $GU -c > $O.gs && rm -f $O.gs" \
  "polars|python3 $D/pl_bench.py count $DATA $O.pl >/dev/null 2>&1 && rm -f $O.pl"

run_suite "grep->sort (20G, lvl=W)" \
  "tsort3|$TS -q --grep 'lvl=W' $DATA $O.ts && rm -f $O.ts" \
  "unix|LC_ALL=C grep 'lvl=W' $DATA | $GS -o $O.gs && rm -f $O.gs" \
  "polars|python3 $D/pl_bench.py grep_sort $DATA $O.pl >/dev/null 2>&1 && rm -f $O.pl"

run_suite "stream grep (20G, transformer, no sort)" \
  "tsort3|$TS -q --no-sort --grep 'transformer' $DATA $O.ts && rm -f $O.ts" \
  "grep|LC_ALL=C grep 'transformer' $DATA > $O.gs && rm -f $O.gs" \
  "polars|python3 $D/pl_bench.py grep_only $DATA $O.pl >/dev/null 2>&1 && rm -f $O.pl"

run_suite "top-20 freq (20G)" \
  "unix|LC_ALL=C $GS $DATA | $GU -c | $GS -k1,1rn -k2 | head -20 > $O.gs" \
  "tsort3|$TS -q --freq 20 $DATA $O.ts && cmp -s $O.ts $O.gs && rm -f $O.ts" \
  "polars|python3 $D/pl_bench.py freq $DATA $O.pl >/dev/null 2>&1 && rm -f $O.pl"

run_suite "dedup by first word (20G)" \
  "tsort3|$TS -q --delim ' ' --dedup-key 1 $DATA $O.ts && wc -l < $O.ts > $O.tsc && rm -f $O.ts" \
  "awk|LC_ALL=C awk '!seen[\$1]++' $DATA > $O.gs && wc -l < $O.gs > $O.awkc && cmp -s $O.tsc $O.awkc && rm -f $O.gs"

run_suite "wc summary (20G)" \
  "tsort3|$TS -q --no-sort --wc $DATA $O.ts && cat $O.ts && rm -f $O.ts" \
  "wc|LC_ALL=C wc $DATA"
echo "ALL DONE"
