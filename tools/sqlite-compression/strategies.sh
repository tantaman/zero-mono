#!/usr/bin/env bash
# Compare base-image *strategies* at a fixed DB size, not just codecs:
#   1. raw page image + zstd      (what litestream ships today)
#   2. VACUUMed page image + zstd (does defragmenting pay?)
#   3. logical .dump + zstd       (no index data, no B-tree overhead --
#                                  but the restore has to rebuild everything)
#   4. page_size 4K / 16K / 64K   (does a bigger page compress better?)
# Also times the restore, because the point of a base image is recovery speed.
set -u
TOOLS=/home/user/zero-mono/tools/sqlite-compression
WORK="${WORK:-/tmp/claude-0/-home-user-zero-mono/f0f83565-b8f0-5787-8e7b-38e13c75494a/scratchpad}"
GB="${GB:-2}"
SCHEMA="${SCHEMA:-zbugs}"
OUT="$WORK/strategies.jsonl"
export SQLITE_TMPDIR="$WORK" TMPDIR="$WORK"

emit() { python3 -c "
import json,sys
print(json.dumps(dict(zip(sys.argv[1::2], sys.argv[2::2]))))" "$@"; }

DB="$WORK/strat.db"

for PS in 4096 16384 65536; do
  echo ">>> $SCHEMA ${GB}GB page_size=$PS" >&2
  rm -f "$DB"
  python3 "$TOOLS/gen_db.py" --schema "$SCHEMA" --gb "$GB" --out "$DB" \
      --page-size "$PS" --version-mode mixed >/dev/null 2>&1
  RAW=$(stat -c%s "$DB")

  C=$(zstd -3 -c -T1 < "$DB" | wc -c)
  emit strategy "page-image" schema "$SCHEMA" gb "$GB" page_size "$PS" \
       raw "$RAW" comp "$C" >> "$OUT"

  if [ "$PS" = "4096" ]; then
    # --- VACUUM ---
    t0=$(date +%s.%N)
    sqlite3 "$DB" "VACUUM INTO '$WORK/vac.db'"
    t1=$(date +%s.%N)
    VRAW=$(stat -c%s "$WORK/vac.db")
    VC=$(zstd -3 -c -T1 < "$WORK/vac.db" | wc -c)
    emit strategy "vacuumed-page-image" schema "$SCHEMA" gb "$GB" page_size "$PS" \
         raw "$VRAW" comp "$VC" vacuum_secs "$(python3 -c "print(round($t1-$t0,1))")" >> "$OUT"
    rm -f "$WORK/vac.db"

    # --- logical dump ---
    t0=$(date +%s.%N)
    sqlite3 "$DB" .dump | zstd -3 -c -T1 > "$WORK/dump.zst"
    t1=$(date +%s.%N)
    DC=$(stat -c%s "$WORK/dump.zst")
    DUMP_SECS=$(python3 -c "print(round($t1-$t0,1))")

    # restore the dump: decompress -> ingest -> rebuild every index
    t0=$(date +%s.%N)
    rm -f "$WORK/restored.db"
    zstd -dc "$WORK/dump.zst" | sqlite3 "$WORK/restored.db"
    t1=$(date +%s.%N)
    RESTORE_SECS=$(python3 -c "print(round($t1-$t0,1))")
    RESTORED=$(stat -c%s "$WORK/restored.db")
    rm -f "$WORK/restored.db"

    # restore a page image for comparison: decompress -> write
    zstd -3 -c -T1 < "$DB" > "$WORK/img.zst"
    t0=$(date +%s.%N)
    zstd -dc "$WORK/img.zst" > "$WORK/restored.db"
    t1=$(date +%s.%N)
    IMG_RESTORE=$(python3 -c "print(round($t1-$t0,1))")
    rm -f "$WORK/restored.db" "$WORK/img.zst"

    emit strategy "logical-dump" schema "$SCHEMA" gb "$GB" page_size "$PS" \
         raw "$RAW" comp "$DC" dump_secs "$DUMP_SECS" \
         restore_secs "$RESTORE_SECS" restored_bytes "$RESTORED" >> "$OUT"
    emit strategy "page-image-restore" schema "$SCHEMA" gb "$GB" page_size "$PS" \
         raw "$RAW" comp "$C" restore_secs "$IMG_RESTORE" >> "$OUT"
  fi
done
rm -f "$DB"
echo "STRATEGIES-DONE" >&2
