#!/usr/bin/env bash
# zbugs' ratio degrades ~2.4pp per 10x of DB size while chinook's is flat.
# Hypothesis: zbugs' compressibility comes from long-range repetition (the same
# issue/comment templates recurring megabytes apart, separated by random ids),
# so it falls outside zstd's default match window as the file grows. chinook's
# comes from local structure (small ints, short strings) and is window-immune.
#
# If that is right, --long=27 (128MB window) should flatten the zbugs curve --
# which matters, because it decides whether a 100GB base is ~38% or ~43%.
set -u
TOOLS=/home/user/zero-mono/tools/sqlite-compression
WORK="${WORK:-/tmp/claude-0/-home-user-zero-mono/f0f83565-b8f0-5787-8e7b-38e13c75494a/scratchpad}"
OUT="$WORK/window.jsonl"
export SQLITE_TMPDIR="$WORK" TMPDIR="$WORK"
DB="$WORK/win.db"
NP=$(nproc)

for schema in zbugs chinook; do
  for gb in 1 10; do
    rm -f "$DB"
    echo ">>> $schema ${gb}GB ($(date -u +%H:%M:%S))" >&2
    python3 "$TOOLS/gen_db.py" --schema "$schema" --gb "$gb" --out "$DB" \
        --version-mode mixed >/dev/null 2>&1 || continue
    RAW=$(stat -c%s "$DB")
    for spec in "zstd-3:-3" "zstd-3-long27:-3 --long=27" \
                "zstd-9:-9" "zstd-9-long27:-9 --long=27" \
                "zstd-9-long31:-9 --long=31"; do
      name="${spec%%:*}"; flags="${spec#*:}"
      t0=$(date +%s.%N)
      C=$(zstd $flags -c -T"$NP" < "$DB" | wc -c)
      t1=$(date +%s.%N)
      python3 -c "
import json
print(json.dumps({'schema':'$schema','gb':$gb,'codec':'$name','raw':$RAW,'comp':$C,
  'pct':round(100*$C/$RAW,2),'ratio':round($RAW/$C,3),
  'mb_per_s':round($RAW/1e6/($t1-$t0),1)}))" >> "$OUT"
    done
  done
done
rm -f "$DB"
echo "WINDOW-DONE" >&2
