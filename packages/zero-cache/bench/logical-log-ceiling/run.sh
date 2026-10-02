#!/usr/bin/env bash
#
# Runs the whole logical-log replay comparison on this machine and prints one
# table. Everything is derived from a single base replica, so the variants are
# directly comparable.
#
#   ./run.sh                 # 2M-row replica, 24 MB log  (~5 min, ~3 GB disk)
#   ./run.sh 200000 12       # smaller and faster, for a first look
#   ROWS=4000000 ./run.sh    # or set them as env vars
#
# Requires: a C compiler, node, and `pnpm install` already run at the repo root.
set -euo pipefail

ROWS="${1:-${ROWS:-2000000}}"
LOG_MB="${2:-${LOG_MB:-24}}"
WORKLOADS="${WORKLOADS:-insert-heavy mixed update-heavy}"
DIR="${CEILING_DIR:-${TMPDIR:-/tmp}/zero-logical-log-ceiling}"
export CEILING_DIR="$DIR"

cd "$(dirname "$0")"

echo "==> building (same SQLite amalgamation and defines as zero-sqlite3)"
make check
make -s

CHECKSUMS=""

# One row of the table. Runs against a throwaway copy of the base every time.
run_variant() {
  local label="$1"; shift
  cp "$DIR/base.db" "$DIR/work.db"
  local out
  out=$(env "$@" ./apply "$DIR/work.db" "$DIR/log.bin")
  rm -f "$DIR/work.db"

  # Where a variant does work outside the apply loop (rebuilding deferred
  # indexes), the honest figure is the total, not the apply alone.
  local ms
  ms=$(sed -n 's/.*=> total \([0-9.]*\) ms.*/\1/p' <<<"$out" | head -1)
  [ -n "$ms" ] || ms=$(sed -n 's/applied=[0-9]*  \([0-9.]*\) ms.*/\1/p' <<<"$out" | head -1)
  local n
  n=$(sed -n 's/applied=\([0-9]*\) .*/\1/p' <<<"$out" | head -1)

  awk -v l="$label" -v mb="$LOG_LOGICAL_MB" -v ms="$ms" -v n="$n" \
    'BEGIN{ printf "  %-34s %8.2f us/change %8.1f MB/s\n", l, ms*1000/n, mb*1000/ms }'
  sed -n 's/^    \(insert\|update\|delete\) /      \1 /p' <<<"$out" || true
  sed -n 's/^    \(deferred .*\)/      \1/p' <<<"$out" || true
  CHECKSUMS="$CHECKSUMS$(sed -n 's/.*checksum=\(.*\)/\1/p' <<<"$out")\n"
}

for workload in $WORKLOADS; do
  echo
  echo "==> generating: $workload, ${ROWS} row base, ${LOG_MB} MB log"
  WORKLOAD="$workload" ROWS="$ROWS" LOG_MB="$LOG_MB" node export.ts >/dev/null
  LOG_LOGICAL_MB=$(node -e "console.log(require('$DIR/meta.json').logMB.toFixed(3))")
  echo "    log: $(node -e "
    const m=require('$DIR/meta.json');
    console.log(\`\${m.logMB.toFixed(1)} MB JSON / \${(m.binBytes/1048576).toFixed(1)} MB binary, \${m.changes} changes\`)")"
  echo
  echo "  --- $workload ---"
  run_variant "C, zero-parse (the ceiling)"        NOOP=1
  run_variant "C, deferred index rebuild"          ZLOG_DEFER_INDEXES=1
  run_variant "C, secondary indexes dropped"       ZLOG_DROP_SECONDARY=1
  echo
  echo "  TypeScript, same log, same base:"
  local_js=$(WORKLOAD="$workload" ROWS="$ROWS" LOG_MB="$LOG_MB" node jsref.ts 2>/dev/null | tail -2)
  sed 's/^/    /;s/checksum=.*//' <<<"$local_js"
  CHECKSUMS="$CHECKSUMS$(sed -n 's/.*checksum=\(.*\)/\1/p' <<<"$local_js")\n"

  # Every variant must have left the replica in the same state.
  distinct=$(printf '%b' "$CHECKSUMS" | sed '/^$/d' | sort -u | wc -l)
  if [ "$distinct" -eq 1 ]; then
    echo "  checksums: all variants agree"
  else
    echo "  CHECKSUM MISMATCH -- the variants did different work, timings are meaningless:"
    printf '%b' "$CHECKSUMS" | sed '/^$/d' | sort -u | sed 's/^/    /'
    exit 1
  fi
  CHECKSUMS=""
done

echo
echo "==> key scheme and table structure (independent of the log)"
./keys "$ROWS" 50000

cat <<'NOTE'

All variants print a checksum over the resulting replica; they must match.
If they do not, the runs did different work and the timings mean nothing.

If you suspect disk: this workload is CPU-bound here (0 bytes read from storage
during apply, ~17 MB written for a 24 MB log, and tmpfs is worth ~10%). To check
on your machine, put the base somewhere in RAM and compare:

    CEILING_DIR=/dev/shm/zllc ./run.sh     # Linux
    CEILING_DIR=/Volumes/RAM/zllc ./run.sh # macOS, after diskutil erasevolume
NOTE
