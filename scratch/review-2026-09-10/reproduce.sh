#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/../.."
review_dir=scratch/review-2026-09-10
for target in supervised/review_20260910_test.go queued/review_20260910_test.go; do
  if [ -e "$target" ]; then
    printf 'Refusing to overwrite existing file: %s\n' "$target" >&2
    exit 2
  fi
done
trap 'rm -f supervised/review_20260910_test.go queued/review_20260910_test.go' EXIT
cp "$review_dir/supervised_repro_test.go.txt" supervised/review_20260910_test.go
cp "$review_dir/queued_repro_test.go.txt" queued/review_20260910_test.go
GOTOOLCHAIN="${GOTOOLCHAIN:-go1.26.8}" go test -race ./supervised ./queued -run '^TestReview' -count=1 -v
