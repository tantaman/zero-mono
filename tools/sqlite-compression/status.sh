#!/usr/bin/env bash
W="${WORK:-/tmp/claude-0/-home-user-zero-mono/f0f83565-b8f0-5787-8e7b-38e13c75494a/scratchpad}"
for j in matrix strategies sensitivity makeup window; do
  log="$W/$j.log"; out="$W/$j.jsonl"
  n=$( [ -f "$out" ] && wc -l < "$out" || echo 0 )
  last=$( [ -f "$log" ] && grep -E "^(===|>>>)" "$log" | tail -1 || echo "not started" )
  done_marker=$( [ -f "$log" ] && grep -oE "[A-Z]+-DONE|COMPLETE|FINISHED" "$log" | tail -1 || echo "" )
  printf "%-12s rows=%-4s %-10s %s\n" "$j" "$n" "${done_marker:-running}" "$last"
done
echo "--- disk ---"; df -h / | tail -1
