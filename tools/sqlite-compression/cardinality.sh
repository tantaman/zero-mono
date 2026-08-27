#!/usr/bin/env bash
# zbugs loses 2.44pp of ratio per 10x of file size; chinook loses none. It is
# not the codec window (--long=27 and --long=31 both fail to recover it) and it
# is not a shift in table/index composition (that holds at ~41% index either way).
#
# Remaining candidate: the id-reference columns. A 10GB zbugs has 10x the users
# of a 1GB zbugs, so creatorID / assigneeID / viewState.userID point into a 10x
# larger set of distinct nanoids, and each reference costs correspondingly more
# bits to code. chinook is immune because its foreign keys are integers, which
# grow in the raw file at the same rate as their entropy.
#
# Test: hold the file at 1GB and vary only the entity count. If cardinality is
# the cause, the 10x-users build should lose about the same 2.4pp -- with no
# change in file size at all.
set -u
TOOLS=/home/user/zero-mono/tools/sqlite-compression
WORK="${WORK:-/tmp/claude-0/-home-user-zero-mono/f0f83565-b8f0-5787-8e7b-38e13c75494a/scratchpad}"
OUT="$WORK/cardinality.jsonl"
export SQLITE_TMPDIR="$WORK" TMPDIR="$WORK"
DB="$WORK/card.db"

for ipu in 500 50 5; do
  rm -f "$DB"
  echo ">>> 1GB issues_per_user=$ipu ($(date -u +%H:%M:%S))" >&2
  python3 "$TOOLS/gen_db.py" --schema zbugs --gb 1 --out "$DB" \
      --version-mode mixed --issues-per-user "$ipu" >/dev/null 2>&1 || continue
  RAW=$(stat -c%s "$DB")
  USERS=$(python3 -c "
import sqlite3;print(sqlite3.connect('$DB').execute('SELECT count(*) FROM \"user\"').fetchone()[0])")
  for lvl in 3 9; do
    C=$(zstd -$lvl -c -T"$(nproc)" < "$DB" | wc -c)
    python3 -c "
import json
print(json.dumps({'issues_per_user':$ipu,'users':$USERS,'codec':'zstd-$lvl',
  'raw':$RAW,'comp':$C,'pct':round(100*$C/$RAW,2),'ratio':round($RAW/$C,3)}))" >> "$OUT"
  done
done
rm -f "$DB"
echo "CARDINALITY-DONE" >&2
