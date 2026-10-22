#!/bin/zsh
# bench.sh - tsort v3 vs GNU sort vs BSD sort vs polars vs datamash
# Suites: sort / sort -u / grep->sort / uniq -c / stream-grep
# Hot cache, best of ROUNDS (default 3). Peak RSS via /usr/bin/time -l.
set -u
TS=/Users/biluochun/work/error.d/project/textsort/textsort3
V=/Users/biluochun/work/error.d/project/textsort/.venv-bench/bin/python
D=/tmp/tsb
DATA=$D/data_2g.txt
ROUNDS=${ROUNDS:-3}

timed_rss() {  # timed_rss NAME "CMD"
  local name=$1; shift
  /usr/bin/time -l sh -c "$*" >$D/.bench_out 2>$D/.time_l
  local rc=$?
  # macOS time -l: "  1.23 real  0.45 user  0.12 sys" (col2==real);
  # "cycles elapsed" is a DIFFERENT line - never match on /elapsed/ alone.
  local el=$(awk '$2=="real" {print $1}' $D/.time_l | tail -1)
  # max RSS on macOS is in BYTES -> MB needs /1048576 (not /1024).
  local rss=$(awk '/maximum resident set size/ {print $1}' $D/.time_l | tail -1)
  [ -z "$el" ] && el=999
  echo "$name ${el}s rss=$(( ${rss:-0} / 1048576 ))MB rc=$rc"
}

suite() {  # suite TITLE pairs...
  local title=$1; shift
  echo "=== $title ==="
  for round in $(seq 1 $ROUNDS); do
    for pair in "$@"; do
      timed_rss "${pair%%|*}" "${pair#*|}"
    done
  done
}

echo "=== warmup ==="
cat $DATA > /dev/null

suite "sort" \
  "tsort3 |$TS -q $DATA $D/o.txt && rm -f $D/o.txt" \
  "gsort  |LC_ALL=C gsort $DATA -o $D/o.txt && rm -f $D/o.txt" \
  "bsdsort|LC_ALL=C /usr/bin/sort $DATA -o $D/o.txt && rm -f $D/o.txt" \
  "polars |$V $D/pl_sort.py $DATA $D/o.txt && rm -f $D/o.txt"

suite "sort -u" \
  "tsort3 |$TS -q -u $DATA $D/o.txt && rm -f $D/o.txt" \
  "gsort  |LC_ALL=C gsort -u $DATA -o $D/o.txt && rm -f $D/o.txt" \
  "polars |$V $D/pl_uniq.py $DATA $D/o.txt && rm -f $D/o.txt"

suite "grep->sort" \
  "tsort3 |$TS -q --grep 'lvl=W' $DATA $D/o.txt && rm -f $D/o.txt" \
  "unix   |LC_ALL=C grep 'lvl=W' $DATA | gsort -o $D/o.txt && rm -f $D/o.txt" \
  "polars |$V $D/pl_grep_sort.py $DATA $D/o.txt 'lvl=W' && rm -f $D/o.txt"

suite "uniq -c" \
  "tsort3 |$TS -q -c $DATA $D/o.txt && rm -f $D/o.txt" \
  "unix   |LC_ALL=C gsort $DATA | guniq -c > $D/o.txt && rm -f $D/o.txt" \
  "polars |$V $D/pl_count.py $DATA $D/o.txt && rm -f $D/o.txt"

suite "stream grep (no sort)" \
  "tsort3 |$TS -q --no-sort --grep 'transformer' $DATA $D/o.txt && rm -f $D/o.txt" \
  "grep   |LC_ALL=C grep 'transformer' $DATA > $D/o.txt && rm -f $D/o.txt" \
  "polars |$V $D/pl_grep_only.py $DATA $D/o.txt 'transformer' && rm -f $D/o.txt"
