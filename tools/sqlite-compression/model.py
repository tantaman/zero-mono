#!/usr/bin/env python3
"""Project measured SQLite compression ratios out to a 100GB replica and cost
the resulting S3 base-image uploads.

Reads the measured matrix (matrix.jsonl) and emits:
  * ratio vs. size, per schema, to show whether the ratio drifts with scale
  * projected compressed size at 100GB
  * compress + upload wall time
  * S3 PUT and storage cost
  * how many bases/day are affordable in time and in dollars
"""

import argparse
import json
import sys
from collections import defaultdict

# S3 Standard, us-east-1, list price. Data transfer INTO S3 is free; the
# request charge is per multipart part.
PUT_PER_1K = 0.005
STORAGE_PER_GB_MONTH = 0.023
LITESTREAM_PART_BYTES = 16 * 1024 * 1024   # --litestream-multipart-size default
LITESTREAM_CONCURRENCY = 48                # --litestream-multipart-concurrency default


def load(path):
    by = defaultdict(dict)
    for line in open(path):
        line = line.strip()
        if not line.startswith("{"):
            continue
        r = json.loads(line)
        by[(r["schema"], r["codec"])][r["label"]] = r
    return by


def size_trend(by, codec):
    """Ratio as a function of DB size, per schema."""
    out = defaultdict(dict)
    for (schema, c), labels in by.items():
        if c != codec:
            continue
        for label, r in labels.items():
            out[schema][label] = r
    return out


def fit_pct(points, raw_gb):
    """Project the compressed percentage out to raw_gb.

    The measured ratio is not always flat: a schema whose compressibility comes
    from long-range repetition loses ground as the file grows past the codec's
    match window, so the percentage creeps up with size. Fit pct against
    log10(size) over the measured points and extrapolate along that line;
    with a flat measurement the slope is ~0 and this reduces to holding the
    last value. Returns (pct, slope_pp_per_decade).
    """
    import math
    pts = sorted(points)
    if len(pts) < 2:
        return pts[-1][1], 0.0
    xs = [math.log10(g) for g, _ in pts]
    ys = [p for _, p in pts]
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / denom if denom else 0.0
    intercept = my - slope * mx
    pct = intercept + slope * math.log10(raw_gb)
    # Never project outside what the codec can actually do.
    return max(1.0, min(100.0, pct)), slope


def measured_points(labels):
    return [(float(l.replace("GB", "")), r["pct"]) for l, r in labels.items()]


def project(labels, raw_gb):
    pct, _ = fit_pct(measured_points(labels), raw_gb)
    return raw_gb * pct / 100.0


def fmt_time(secs):
    if secs < 90:
        return "%.0fs" % secs
    if secs < 5400:
        return "%.1f min" % (secs / 60)
    return "%.1f hr" % (secs / 3600)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--results", default="matrix.jsonl")
    ap.add_argument("--target-gb", type=float, default=100.0)
    ap.add_argument("--bandwidth-gbps", type=float, nargs="+",
                    default=[1.0, 2.5, 5.0, 10.0])
    ap.add_argument("--retention-days", type=float, default=1.0,
                    help="how long each minted base is retained")
    args = ap.parse_args()

    by = load(args.results)
    if not by:
        sys.exit("no results in %s" % args.results)

    schemas = sorted({s for s, _ in by})
    codecs = []
    for _, c in by:
        if c not in codecs:
            codecs.append(c)

    # ---- 1. does the ratio drift with size? ------------------------------
    print("=" * 78)
    print("COMPRESSED SIZE AS %% OF RAW, BY DB SIZE  (target %.0fGB)" % args.target_gb)
    print("=" * 78)
    labels_seen = []
    for labels in by.values():
        for l in labels:
            if l not in labels_seen:
                labels_seen.append(l)
    labels_seen.sort(key=lambda s: float(s.replace("GB", "")))

    for schema in schemas:
        print("\n%s" % schema)
        hdr = "  %-18s" % "codec" + "".join("%>9s" % "" for _ in ())
        hdr = "  %-18s" % "codec" + "".join("%9s" % l for l in labels_seen) + "%12s" % "drift"
        print(hdr)
        for codec in codecs:
            labels = by.get((schema, codec))
            if not labels:
                continue
            cells = []
            vals = []
            for l in labels_seen:
                r = labels.get(l)
                if r:
                    cells.append("%8.2f%%" % r["pct"])
                    vals.append((float(l.replace("GB", "")), r["pct"]))
                else:
                    cells.append("%9s" % "-")
            drift = ""
            if len(vals) >= 2:
                vals.sort()
                drift = "%+.2f pp" % (vals[-1][1] - vals[0][1])
            print("  %-18s" % codec + "".join(cells) + "%12s" % drift)

    # ---- 2. throughput ---------------------------------------------------
    print("\n" + "=" * 78)
    print("COMPRESSION THROUGHPUT (largest measured DB, 4 vCPU)")
    print("=" * 78)
    print("  %-18s %-12s %10s %10s %14s" % ("codec", "schema", "MB/s", "% of raw",
                                            "100GB compress"))
    for codec in codecs:
        for schema in schemas:
            labels = by.get((schema, codec))
            if not labels:
                continue
            biggest = max(labels.values(), key=lambda r: r["raw_bytes"])
            secs = args.target_gb * 1000 / biggest["mb_per_s"]
            print("  %-18s %-12s %10.1f %9.2f%% %14s"
                  % (codec, schema, biggest["mb_per_s"], biggest["pct"], fmt_time(secs)))

    # ---- 3. the 100GB base image -----------------------------------------
    print("\n" + "=" * 78)
    print("PROJECTED %.0fGB BASE IMAGE" % args.target_gb)
    print("=" * 78)
    for schema in schemas:
        print("\n%s" % schema)
        print("  %-18s %12s %12s %10s %16s"
              % ("codec", "uploaded", "vs raw", "parts", "size trend"))
        for codec in codecs:
            labels = by.get((schema, codec))
            if not labels:
                continue
            biggest = max(labels.values(), key=lambda r: r["raw_bytes"])
            gb = project(labels, args.target_gb)
            _, slope = fit_pct(measured_points(labels), args.target_gb)
            parts = int(gb * 1e9 / LITESTREAM_PART_BYTES) + 1
            print("  %-18s %9.1f GB %11.2fx %10d %+8.2f pp/decade"
                  % (codec, gb, args.target_gb / gb, parts, slope))

    # ---- 4. upload time + cost -------------------------------------------
    print("\n" + "=" * 78)
    print("UPLOAD TIME AND COST PER BASE  (S3 Standard, us-east-1, transfer IN free)")
    print("=" * 78)
    print("  Litestream defaults: %d concurrent parts x %d MiB = %d MiB in flight"
          % (LITESTREAM_CONCURRENCY, LITESTREAM_PART_BYTES // 2**20,
             LITESTREAM_CONCURRENCY * LITESTREAM_PART_BYTES // 2**20))
    for schema in schemas:
        for codec in ("zstd-3", "zstd-9", "lz4", "gzip-6"):
            labels = by.get((schema, codec))
            if not labels:
                continue
            biggest = max(labels.values(), key=lambda r: r["raw_bytes"])
            gb = project(labels, args.target_gb)
            parts = int(gb * 1e9 / LITESTREAM_PART_BYTES) + 1
            put_cost = parts / 1000 * PUT_PER_1K
            store = gb * STORAGE_PER_GB_MONTH * args.retention_days / 30
            comp_secs = args.target_gb * 1000 / biggest["mb_per_s"]
            print("\n  %s / %s -> %.1f GB uploaded (compress %s at %.0f MB/s)"
                  % (schema, codec, gb, fmt_time(comp_secs), biggest["mb_per_s"]))
            print("    %-14s %14s %14s %14s" % ("link", "upload", "total wall",
                                                "$/base"))
            for gbps in args.bandwidth_gbps:
                up = gb * 8 / gbps          # GB*8 = Gbit; / Gbit/s = s
                total = max(comp_secs, up) if True else comp_secs + up
                cost = put_cost + store
                print("    %-14s %14s %14s %14s"
                      % ("%.1f Gbps" % gbps, fmt_time(up), fmt_time(total),
                         "$%.4f" % cost))
            print("    (streamed: compress and upload overlap, so wall time is the"
                  " slower of the two)")
            print("    PUT requests: %d parts = $%.4f;  storage @ %.0fd retention:"
                  " $%.4f/base" % (parts, put_cost, args.retention_days, store))

    # ---- 5. what cadence is affordable -----------------------------------
    print("\n" + "=" * 78)
    print("BASE CADENCE: WHAT IS AFFORDABLE")
    print("=" * 78)
    print("  Each base is a full copy. Cost is storage x retention plus one PUT")
    print("  per %d MiB part; transfer into S3 is free. Wall time assumes the"
          % (LITESTREAM_PART_BYTES // 2**20))
    print("  compressor and the uploader are streamed together, so the slower of")
    print("  the two sets the pace.\n")

    for schema in schemas:
        # Use the fastest measured variant of each codec family: nobody
        # compresses 100GB single-threaded when -T is available.
        best = {}
        for codec in codecs:
            labels = by.get((schema, codec))
            if not labels:
                continue
            fam = codec.split("-T")[0]
            biggest = max(labels.values(), key=lambda r: r["raw_bytes"])
            gb = project(labels, args.target_gb)
            cand = (gb, biggest["mb_per_s"], codec)
            if fam not in best or cand[1] > best[fam][1]:
                best[fam] = cand

        for fam in ("zstd-3", "zstd-9", "lz4"):
            if fam not in best:
                continue
            gb, mbps, codec = best[fam]
            comp_secs = args.target_gb * 1000 / mbps
            parts = int(gb * 1e9 / LITESTREAM_PART_BYTES) + 1
            put_each = parts / 1000 * PUT_PER_1K
            print("  %s / %s -- %.1f GB per base, compress %s (%.0f MB/s)"
                  % (schema, codec, gb, fmt_time(comp_secs), mbps))
            print("    %-14s %11s %11s %10s %11s %11s"
                  % ("cadence", "wall/base", "duty cycle", "resident",
                     "$/mo store", "$/mo PUT"))
            for per_day, name in ((1, "daily"), (6, "every 4h"), (24, "hourly")):
                # retention: hold each base for retention_days
                resident_gb = gb * per_day * args.retention_days
                store_mo = resident_gb * STORAGE_PER_GB_MONTH
                put_mo = put_each * per_day * 30
                # wall time at the most constrained link we were asked about
                slowest = min(args.bandwidth_gbps)
                up = gb * 8 / slowest
                wall = max(comp_secs, up)
                duty = 100 * wall * per_day / 86400
                print("    %-14s %11s %10.1f%% %8.0f GB %11s %11s"
                      % (name, fmt_time(wall), duty, resident_gb,
                         "$%.2f" % store_mo, "$%.2f" % put_mo))
            print("    (duty cycle at %.1f Gbps, the slowest link modelled;"
                  " retention %.0f day(s))\n" % (min(args.bandwidth_gbps),
                                                 args.retention_days))


if __name__ == "__main__":
    main()
