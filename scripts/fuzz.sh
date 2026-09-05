#!/usr/bin/env bash
# Run every root-module fuzz target, retaining Go's generated regression seeds
# even when another target fails. FUZZTIME accepts Go durations or Nx counts.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export GOWORK=off

fuzz_time="${FUZZTIME:-20s}"
if [[ ! "$fuzz_time" =~ ^([0-9]+x|([0-9]+([.][0-9]+)?(ns|us|ms|s|m|h))+)$ ]] || [[ ! "$fuzz_time" =~ [1-9] ]]; then
  printf 'FUZZTIME must be a positive Go duration or iteration count, got %s\n' "$fuzz_time" >&2
  exit 2
fi

failed=0
packages=$(go list ./...)
for package in $packages; do
  # Do not hide discovery/build failures behind grep's no-match exit status.
  targets=$(go test -list '^Fuzz' "$package")
  while IFS= read -r target; do
    [[ "$target" =~ ^Fuzz[[:alnum:]_]+$ ]] || continue
    printf '\nFuzzing %s %s (%s)\n' "$package" "$target" "$fuzz_time"
    if ! go test -run '^$' -fuzz "^${target}\$" -fuzztime "$fuzz_time" "$package"; then
      failed=1
    fi
  done <<< "$targets"
done
exit "$failed"
