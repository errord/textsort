# bigtext

**bigtext** — a composable, parallel large-text processing engine for the LLM
era. It processes unstructured line-oriented text (logs, corpora, JSONL, TSV)
with **unbounded input size**: every execution path streams, and intermediate
results spill to disk under a memory budget.

```
make bigtext          # the engine
make tools/gen_data   # LLM-ish corpus generator for testing
```

## Why

LLM data pipelines grind through hundreds of GB of messy text. The classic
toolbox (`grep | sort | uniq | awk`) is sequential and memory-bound; dataframes
(polars/duckdb) load everything into RAM. bigtext fuses the same Unix-operator
semantics into one parallel engine whose memory stays bounded no matter how
big the input is — 20GB, 200GB, or a pipe that never ends.

## The operator framework

A pipeline is described as composable operators and executed by one of three
physical plans (chosen automatically):

```
input ─▶ pre LineOps ─▶ [ sort barrier ] ─▶ inline dedup ─▶ post PostOps ─▶ output
        (parallel,       chunk-sort → RAM    (unique /      (head / freq /
         fused, 0-copy)  runs + spill runs    count, by      nl / wc, on the
                        + pre-merges +       sort KEY)       ordered stream)
                        parallel slice merge
```

* **pure streaming** (`--no-sort`): parallel chunked map/filter, ordered
  output, ~RAM = chunk size × threads
* **sort barrier**: in-RAM runs while the budget allows, spilled run files +
  fan-in-limited pre-merges + sample-anchor **slice-parallel final merge**
  when it doesn't — memory bounded by `--mem-budget-mb`, never by input size
* **shuffle**: hash-shard to disk, shuffle each shard in RAM, stream out —
  unbounded `shuf`

### Adding a new operator

* streaming op: subclass `LineOp` in `engine/operators.h`, implement
  `Process(LineView*, std::vector<char>*) → Drop/Inplace/Materialized` —
  stateless and thread-safe by contract; register one flag in `tsort.cc`
* ordered-stream op: subclass `PostOp` in `engine/postsort.h`
  (`Feed` per line, optional `Flush` at end-of-stream)
* new sort key: extend `SortKey` in `engine/core.h` (`View`, `Head`,
  `Cmp3Lines`, `Cmp3Keys`) — chunk sort, run files, sparse indexes, the
  merge tree and slice boundaries all follow automatically

## Operators

Order (need or refine the sort barrier):

| flag | meaning | Unix equivalent |
|---|---|---|
| (default) | whole-line byte sort | `LC_ALL=C sort` |
| `--key N` `--delim D` | sort by Nth field | `sort -t D -kN,N` |
| `--num` | numeric order (line or `--key` field) | `sort -n` / `-kN,Nn` |
| `--by-len` | order by line length | `awk+sort` combo |
| `-u` / `--dedup-key N` | drop dups (by sort KEY) | `sort -u` / `-k1,1 -u`, bounded-memory `awk '!seen[$1]++'` |
| `-c` | count per key | `sort \| uniq -c` |
| `--freq K` | top-K most frequent keys | `sort \| uniq -c \| sort -rn \| head` |
| `-r` / `--head N` / `--tail N` | descending / first-N / last-N | `sort -r` / `head` / `tail` |

Streaming (fused before the barrier, parallel, zero-copy when possible):

`--grep S` `--grepv S` `--prefix S` `--suffix S` `--regex P` `--replace A B`
`--replace-first A B` `--cut LIST` `--delim D` `--lower` `--upper` `--trim`
`--squeeze` `--sample RATE` `--min-len N` `--max-len N` `--jsonl KEY`
`--mask-email` `--mask-ip` `--shuffle` `--seed N`

Post (on the ordered stream): `--nl` (cat -n style numbering), `--wc`
(one `lines words bytes` summary line, works with `--no-sort` too).

## Engine options

```
--threads N        worker threads (default: all cores)
--chunk-mb M       read/sort chunk (default 64)
--mem-budget-mb M  memory budget; bigger inputs spill automatically
--tmpdir DIR       spill directory (default /tmp)
--external         force the external path (sorted runs on disk)
```

Examples:

```
tsort corpus.txt out.txt                       # parallel LC_ALL=C sort
tsort -u --mem-budget-mb 8000 huge.txt out.txt # bounded-RAM sort -u
tsort --dedup-key 1 --delim ' ' corpus.txt o   # dedupe by first word
tsort --jsonl text shards/*.jsonl o            # pull one field from JSONL
tsort --mask-ip --mask-email logs.txt o        # PII scrubbing, parallel
tsort --no-sort --grep ERROR logs.txt o        # parallel grep
tsort --freq 20 corpus.txt o                   # top-20 lines
```

## Correctness

Cross-checked byte-for-byte against GNU coreutils (sort/uniq), awk, perl,
cat -n and wc on mixed corpora, including forced-external runs
(`--mem-budget-mb 8` + multi-pass pre-merges) — see `bench/` and the test
suite in the project history. One deliberate divergence: under `--key N -u`
GNU sort picks an arbitrary representative row (its comparator short-circuits
on `-u`); tsort keeps the smallest whole line deterministically.

## Architecture notes

* comparisons ride an 8-byte big-endian line-key prefix (`MakeKey`) with
  full-compare fallback — the same trick GNU sort uses, but carried through
  the chunk sorter, the on-disk run format, its RAM sparse index, and a
  tournament merge tree (one compare per tree level)
* on-disk runs are `[u32 len][bytes]` records with a 1/4096 sparse key index;
  slice merges seek each source to its anchor via binary search + short scan
* slice boundaries use **key-level** comparison so one key group never
  straddles two parallel slices (keeps `-u/-c/--freq` exact)
* the reader, chunk queue, ordered hub, and slice queues are all
  byte-capped; a line may span chunks (up to 3GB)
