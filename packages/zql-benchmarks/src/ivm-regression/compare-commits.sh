#!/bin/bash
# Runs ivm-regression.ts against several commits and writes one JSONL file per
# commit and mode. The harness from this checkout is copied into a worktree of
# each commit, so every commit is measured with the same workload.
#
# usage: compare-commits.sh <out-dir> <label>=<commit>... [-- <mode>...]
#   e.g. compare-commits.sh /tmp/ivm base=1d9e880 head=HEAD -- zqlite driver
#
# Env: PARALLEL (default 1) runs that many jobs at a time. Other
# IVM_REGRESSION_* variables are passed through to the harness.
set -euo pipefail

HERE=$(cd "$(dirname "$0")" && pwd)
ROOT=$(git -C "$HERE" rev-parse --show-toplevel)
OUT=$(mkdir -p "$1" && cd "$1" && pwd)
shift
WT_ROOT=${WT_ROOT:-$ROOT/../ivm-regression-worktrees}
PARALLEL=${PARALLEL:-1}

commits=()
while [ $# -gt 0 ] && [ "$1" != "--" ]; do
  commits+=("$1")
  shift
done
[ "${1:-}" = "--" ] && shift
modes=("${@:-memory zqlite driver}")
read -r -a modes <<<"${modes[*]}"

jobs=()
for spec in "${commits[@]}"; do
  label=${spec%%=*}
  rev=${spec#*=}
  wt=$WT_ROOT/$label
  if [ ! -d "$wt" ]; then
    git -C "$ROOT" worktree add --detach "$wt" "$rev" >/dev/null
  else
    git -C "$wt" checkout --detach "$rev" >/dev/null 2>&1
  fi
  # Dependencies are shared with this checkout (the lockfile must match).
  for nm in "$ROOT"/node_modules "$ROOT"/{packages,apps,tools}/*/node_modules; do
    [ -e "$nm" ] || continue
    rel=${nm#"$ROOT"/}
    [ -d "$wt/$(dirname "$rel")" ] && ln -sfn "$nm" "$wt/$rel"
  done
  mkdir -p "$wt/packages/zql-benchmarks/src/ivm-regression"
  cp "$HERE/ivm-regression.ts" "$wt/packages/zql-benchmarks/src/ivm-regression/"
  for mode in "${modes[@]}"; do
    jobs+=("$label $mode $wt")
  done
done

run_job() {
  read -r label mode wt <<<"$1"
  (cd "$wt/packages/zql-benchmarks/src/ivm-regression" &&
    IVM_REGRESSION_LABEL=$label node --expose-gc --no-warnings \
      --max-old-space-size=4096 ivm-regression.ts "$mode" \
      "$OUT/$label-$mode.jsonl" >"$OUT/$label-$mode.log" 2>&1) &&
    echo "done $label $mode" || echo "FAILED $label $mode (see $OUT/$label-$mode.log)"
}
export -f run_job
export OUT
printf '%s\n' "${jobs[@]}" | xargs -P "$PARALLEL" -I{} bash -c 'run_job "$@"' _ {}

echo "Summarize with: node $HERE/analyze.ts $OUT"
