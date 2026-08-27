#!/usr/bin/env bash
# Supplementary measurements that the main matrix does not cover:
#   * the zbugs 4GB point (its first run died on a generator bug -- githubID was
#     drawn at random and collided with its UNIQUE index once the user count
#     passed the birthday bound; fixed in gen_db.py)
#   * lz4 throughput from the real lz4 binary. bench.sh reaches lz4 through
#     `zstd --format=lz4`, whose encoder is several times slower than upstream
#     lz4 and understates the codec litestream v0.3 actually ships.
#   * raw disk read rate, which on a 100GB file can bind before any codec does.
set -u
TOOLS=/home/user/zero-mono/tools/sqlite-compression
WORK="${WORK:-/tmp/claude-0/-home-user-zero-mono/f0f83565-b8f0-5787-8e7b-38e13c75494a/scratchpad}"
export SQLITE_TMPDIR="$WORK" TMPDIR="$WORK"
DB="$WORK/makeup.db"
for gb in 4; do
  rm -f "$DB"
  echo "=== makeup zbugs @ ${gb}GB ($(date -u +%H:%M:%S)) ===" >&2
  python3 "$TOOLS/gen_db.py" --schema zbugs --gb "$gb" --out "$DB" \
      --text-entropy template --version-mode mixed >> "$WORK/dbmeta.jsonl" || continue
  bash "$TOOLS/bench.sh" "$DB" zbugs "${gb}GB" large >> "$WORK/matrix.jsonl"

  # `zstd --format=lz4` is a slow lz4 encoder and understates the codec that
  # litestream v0.3 actually ships with. Re-measure with the real lz4 binary,
  # and measure raw disk read separately -- on a 100GB file the read can be the
  # bottleneck before any codec is.
  RAW=$(stat -c%s "$DB")
  for spec in "lz4-real:lz4 -1 -c" "lz4-9-real:lz4 -9 -c"; do
    name="${spec%%:*}"; cmd="${spec#*:}"
    t0=$(date +%s.%N); C=$($cmd < "$DB" | wc -c); t1=$(date +%s.%N)
    python3 -c "
import json
print(json.dumps({'schema':'zbugs','label':'${gb}GB','codec':'$name',
  'raw_bytes':$RAW,'comp_bytes':$C,'ratio':round($RAW/$C,3),
  'pct':round(100*$C/$RAW,2),'seconds':round($t1-$t0,2),
  'mb_per_s':round($RAW/1e6/($t1-$t0),1)}))" >> "$WORK/matrix.jsonl"
  done
  t0=$(date +%s.%N); cat "$DB" > /dev/null; t1=$(date +%s.%N)
  python3 -c "
import json
print(json.dumps({'measurement':'disk-read','bytes':$RAW,
  'seconds':round($t1-$t0,2),'mb_per_s':round($RAW/1e6/($t1-$t0),1)}))" >> "$WORK/dbmeta.jsonl"
done
rm -f "$DB"
echo "MAKEUP-DONE" >&2
