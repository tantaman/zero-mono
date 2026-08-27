#!/usr/bin/env bash
# Two variables that turned out to move the ratio more than DB size does:
#   1. _0_version cardinality (constant / mixed / random)
#   2. text entropy (templated seed corpus vs. per-row-unique content)
# Held at a fixed 1GB so only the variable moves.
set -u
TOOLS=/home/user/zero-mono/tools/sqlite-compression
WORK="${WORK:-/tmp/claude-0/-home-user-zero-mono/f0f83565-b8f0-5787-8e7b-38e13c75494a/scratchpad}"
OUT="$WORK/sensitivity.jsonl"
export SQLITE_TMPDIR="$WORK" TMPDIR="$WORK"
DB="$WORK/sens.db"

for schema in zbugs chinook; do
  for vm in constant mixed random; do
    for te in template high; do
      [ "$schema" = "chinook" ] && [ "$te" = "high" ] && continue
      rm -f "$DB"
      echo ">>> $schema version=$vm text=$te" >&2
      python3 "$TOOLS/gen_db.py" --schema "$schema" --gb 1 --out "$DB" \
          --version-mode "$vm" --text-entropy "$te" >/dev/null 2>&1
      RAW=$(stat -c%s "$DB")
      for lvl in 1 3 9; do
        C=$(zstd -$lvl -c -T1 < "$DB" | wc -c)
        python3 -c "
import json
print(json.dumps({'schema':'$schema','version_mode':'$vm','text_entropy':'$te',
  'codec':'zstd-$lvl','raw':$RAW,'comp':$C,'pct':round(100*$C/$RAW,2),
  'ratio':round($RAW/$C,3)}))" >> "$OUT"
      done
    done
  done
done
rm -f "$DB"
echo "SENSITIVITY-DONE" >&2
