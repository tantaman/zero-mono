#!/usr/bin/env bash
# Compress $1 with a matrix of codecs; emit one JSON line per run.
# Output is piped to `wc -c` so no disk is needed for the compressed copy.
#
#   bench.sh <db> <schema-label> <size-label> <matrix: full|large>
set -u
DB="$1"; SCHEMA="$2"; LABEL="$3"; MATRIX="${4:-full}"
RAW=$(stat -c%s "$DB")
NP=$(nproc)

run() {
  local name="$1"; shift
  local t0 t1 out
  t0=$(date +%s.%N)
  out=$("$@" < "$DB" | wc -c)
  t1=$(date +%s.%N)
  python3 -c "
import json
raw=$RAW; comp=$out; secs=$t1-$t0
print(json.dumps({'schema':'$SCHEMA','label':'$LABEL','codec':'$name',
  'raw_bytes':raw,'comp_bytes':comp,'ratio':round(raw/comp,3),
  'pct':round(100*comp/raw,2),'seconds':round(secs,2),
  'mb_per_s':round(raw/1e6/secs,1)}))"
}

# The practical set: what you would actually consider running on a 100GB file.
run "lz4"        zstd --format=lz4 -c
run "zstd-1"     zstd -1 -c -T1
run "zstd-3"     zstd -3 -c -T1
run "zstd-9"     zstd -9 -c -T1
run "zstd-3-T$NP" zstd -3 -c -T"$NP"
run "zstd-9-T$NP" zstd -9 -c -T"$NP"
run "gzip-6"     gzip -6 -c

# Reference points: too slow for a 100GB base, useful as an upper bound.
if [ "$MATRIX" = "full" ]; then
  run "zstd-12"           zstd -12 -c -T1
  run "zstd-19-long27"    zstd -19 -c -T1 --long=27
  run "zstd-19-T$NP-long27" zstd -19 -c -T"$NP" --long=27
  run "xz-6"              xz -6 -c -T"$NP"
fi
