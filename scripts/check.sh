#!/usr/bin/env bash
# Shared local and CI verification. The root module stays dependency-free;
# SQLite integration has its own module and is checked separately.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
export GOWORK=off

check_suite="${1:-all}"
case "$check_suite" in
  all|test) ;;
  *) printf 'Usage: %s [all|test]\n' "$0" >&2; exit 2 ;;
esac

go version
go vet ./...
if [ "$check_suite" = test ]; then
  go test ./...
  exit 0
fi

format_errors=$(gofmt -l .)
if [ -n "$format_errors" ]; then
  printf 'Run gofmt on:\n%s\n' "$format_errors" >&2
  exit 1
fi

root_module=$(go list -m)
module_graph=$(go list -m all)
if [ "$module_graph" != "$root_module" ] || [ -f go.sum ]; then
  printf 'The root module must have no dependency modules or go.sum. Module graph:\n%s\n' "$module_graph" >&2
  exit 1
fi

# CI supplies tools from the versioned open-ships/ci workflow. Local checks
# remain self-contained; do not download an independent toolset inside CI.
run_tool() {
  local tool="$1" module="$2"
  shift 2
  if [ "${OPEN_SHIPS_CI:-}" = true ]; then
    "$tool" "$@"
  else
    go run "$module" "$@"
  fi
}

run_tool errcheck github.com/kisielk/errcheck@v1.20.0 ./...
run_tool staticcheck honnef.co/go/tools/cmd/staticcheck@v0.7.0 ./...
run_tool actionlint github.com/rhysd/actionlint/cmd/actionlint@v1.7.12

coverage_output="${COVERAGE_OUTPUT:-coverage.out}"
coverage_floor="${COVERAGE_FLOOR:-90}"
go test -race -coverprofile="$coverage_output" ./...
coverage_percent=$(go tool cover -func="$coverage_output" | awk '/^total:/ {gsub("%", "", $3); print $3}')
if ! awk -v coverage="$coverage_percent" -v floor="$coverage_floor" 'BEGIN {
  if (coverage !~ /^[0-9]+([.][0-9]+)?$/ || floor !~ /^[0-9]+([.][0-9]+)?$/ || coverage < floor) exit 1
}'; then
  printf 'Coverage %s%% is below the required %s%% (or the profile is invalid).\n' "$coverage_percent" "$coverage_floor" >&2
  exit 1
fi
printf 'Statement coverage: %s%% (minimum %s%%)\n' "$coverage_percent" "$coverage_floor"

run_tool govulncheck golang.org/x/vuln/cmd/govulncheck@v1.6.0 ./...
go -C integration/sqlite test -race ./...
