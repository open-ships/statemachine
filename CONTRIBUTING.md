# Contributing

Run the same source checks used by CI before submitting a change:

```sh
gofmt -w .
GOTOOLCHAIN=go1.26.8 ./scripts/check.sh
```

The script runs vet, pinned errcheck and staticcheck, workflow lint, formatting,
a root module-graph check, race tests with a 90% statement-coverage floor,
pinned govulncheck for the root and separate SQLite modules, and SQLite race
tests. The SQLite scan includes tests because its driver is a test dependency.
The library's root module must remain free of third-party dependencies;
`integration/sqlite` owns its test-only database dependencies. `coverage.out`
is the retained coverage profile. The tools run at explicit versions without
adding them to the library's dependency graph. In CI, the versioned
`open-ships/ci` workflow supplies these binaries and sets `OPEN_SHIPS_CI=true`;
the script then uses the supplied toolset. Local runs retain explicit tool
versions as a self-contained fallback.

Use `./scripts/check.sh test` for a shorter vet-and-test pass. CI uses Go 1.26.8
on Linux, macOS, and Windows, runs the full source checks on Linux, and tests the
root module separately with Go 1.27.1. The Go 1.26.0 job verifies the language
minimum declared in `go.mod`; releases and routine checks use the maintained
1.26.8 patch release. Pin `GOTOOLCHAIN` when reproducing a particular run.

Run live fuzzing and repeated benchmarks with:

```sh
FUZZTIME=20s GOTOOLCHAIN=go1.26.8 ./scripts/fuzz.sh
GOTOOLCHAIN=go1.26.8 ./scripts/benchmark.sh
```

Fuzz discovery visits every root-module target and continues after target
failures. CI uploads generated `testdata/fuzz` inputs from both per-change and
nightly campaigns. Commit each failing input as a permanent regression seed
alongside the fix. The local Go build cache can also retain nonfailing discoveries;
that cache is an optimization, not durable regression evidence.

Benchmarks run three 100 ms samples per benchmark with allocation reporting.
CI retains `coverage.out` and the raw `benchmark.out` together in the
`source-assurance-<sha>` artifact, including toolchain and platform, for
comparison with a previous run using `benchstat`. Compare the same toolchain,
hardware, and benchmark units; hosted-runner timing alone is not a regression
gate. No performance budget is inferred from a single smoke measurement.

Behavioral changes must update `CONTEXT.md`, applicable ADRs, package
documentation, `SAFETY.md`, and `docs/assurance/requirements.md`. Add a regression
test that names the invariant being protected. Changes to Supervisor identity,
commit points, timing, Journal/Recorder behavior, Observer failure semantics,
or release provenance require an explicit changelog entry and independent review.

## Stable release policy

`1.4.0` is the first stable release. All earlier tags are development
prereleases, including tags that used ordinary version numbers. Establish this
baseline directly; do not add compatibility shims or an interface gate against
those prereleases. The module and import path remain
`github.com/open-ships/statemachine`.

After 1.4.0, maintainers review exported-interface and documented-behavior changes
for compatibility. Compatible additions require a minor baseline bump;
compatible fixes can use patch releases. A future incompatible stable interface
requires a new major version and Go module path. Set `VERSION` to the intended
next baseline and document its migration before merging such a change. The
shared release workflow uses that baseline when it exceeds existing tags and
otherwise increments the latest patch; it cannot decide compatibility for reviewers.

Successful CI on the current `main` commit triggers the pinned shared release
workflow. It creates an annotated tag and source release with checksums, an SBOM,
toolchain and coverage evidence, and separate build-provenance and SBOM
attestations. Tags are annotated, not cryptographically signed. Updates to the
shared workflow require reviewing its exact commit before changing the pin.
