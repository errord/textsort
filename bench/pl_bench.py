import sys
import polars as pl

mode, src, dst = sys.argv[1], sys.argv[2], sys.argv[3]

def read():
    return pl.read_csv(src, has_header=False, new_columns=["l"], quote_char=None)

def write(df):
    try:
        df.write_csv(dst, include_header=False)
    except TypeError:
        df.write_csv(dst, has_header=False)

if mode == "sort":
    df = read()
    write(df.sort("l"))
elif mode == "uniq":
    df = read()
    write(df.unique(subset=["l"], keep="first").sort("l"))
elif mode == "count":
    df = read()
    g = df.group_by("l").agg(pl.len().alias("c")).sort("l").select(["c", "l"])
    write(g)
elif mode == "grep_sort":
    df = read()
    write(df.filter(pl.col("l").str.contains("lvl=W")).sort("l"))
elif mode == "grep_only":
    df = read()
    write(df.filter(pl.col("l").str.contains("transformer")))
elif mode == "freq":
    df = read()
    vc = df["l"].value_counts(sort=True).head(20)
    vc.write_csv(dst)
else:
    raise SystemExit("unknown mode " + mode)
