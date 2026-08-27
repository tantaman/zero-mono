#!/usr/bin/env bash
# Build -> benchmark -> delete, one DB at a time, so peak disk stays ~1 DB.
set -u
TOOLS=/home/user/zero-mono/tools/sqlite-compression
WORK="${WORK:-/tmp/claude-0/-home-user-zero-mono/f0f83565-b8f0-5787-8e7b-38e13c75494a/scratchpad}"
OUT="$WORK/matrix.jsonl"
META="$WORK/dbmeta.jsonl"
export SQLITE_TMPDIR="$WORK"
export TMPDIR="$WORK"
mkdir -p "$WORK"

bench_one() { # schema, gb, extra-gen-args..., variant-label
  local schema="$1"; local gb="$2"; local variant="$3"; shift 3
  local db="$WORK/bench.db"
  local matrix=full
  awk "BEGIN{exit !($gb > 1.5)}" && matrix=large
  echo "=== $variant @ ${gb}GB ($(date -u +%H:%M:%S)) ===" >&2
  python3 "$TOOLS/gen_db.py" --schema "$schema" --gb "$gb" --out "$db" "$@" >> "$META" || return 1
  python3 - "$db" "$variant" "$gb" <<'PY' >> "$META"
import sqlite3, sys, json
db = sqlite3.connect(sys.argv[1])
rows = db.execute("""SELECT m.type, sum(s.pgsize) FROM dbstat s
    JOIN sqlite_master m ON m.name=s.name GROUP BY m.type""").fetchall()
print(json.dumps({"composition": True, "variant": sys.argv[2], "gb": float(sys.argv[3]),
                  **{t: b for t, b in rows}}))
PY
  bash "$TOOLS/bench.sh" "$db" "$variant" "${gb}GB" "$matrix" >> "$OUT"
  rm -f "$db"
}

for gb in 1 4 10; do
  bench_one zbugs   "$gb" "zbugs"       --text-entropy template --version-mode mixed
  bench_one chinook "$gb" "chinook"     --version-mode mixed
done
for gb in 1 4; do
  bench_one zbugs   "$gb" "zbugs-highentropy" --text-entropy high --version-mode mixed
done
echo "MATRIX-DONE" >&2
