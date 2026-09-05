#!/usr/bin/env bash
# Retain repeated raw measurements for benchstat comparisons on like hardware.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export GOWORK=off

benchmark_output="${BENCHMARK_OUTPUT:-benchmark.out}"
{
  go version
  go env GOOS GOARCH
  go test -run '^$' -bench . -benchmem -benchtime=100ms -count=3 ./...
} | tee "$benchmark_output"
