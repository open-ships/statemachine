These probes reproduce defects observed in commit `9538650` (`v1.3.2`). They intentionally assert the defective behavior. A passing probe means the defect was reproduced. After repairing the implementation, replace these assertions with the intended invariant before adopting them as regression tests.

The `.go.txt` extension keeps review evidence out of the normal package test suite. The Supervisor probes reuse existing test fixtures from this revision. They call public execution methods; the counter probe also uses an existing internal restoration helper to supply the explicit volatile configuration.

From the repository root, create an isolated copy and run all probes:

```sh
review_dir=$(mktemp -d)
git archive 9538650 | tar -x -C "$review_dir"
cp scratch/review-2026-09-05/supervised_repro_test.go.txt "$review_dir/supervised/review_repro_test.go"
cp scratch/review-2026-09-05/edge_repro_test.go.txt "$review_dir/review_edge_repro_test.go"
(cd "$review_dir" && go test -race . ./supervised -run TestReview -count=1 -v)
```

`supervised_repro_test.go.txt` covers late Journal overwrites, clean-restart Record identity reuse, refused Attempt identity reuse, external self-transition adoption, original Change metadata, early Operation release, Recorder reentry, Trip backlog, causal record eviction, and record-counter wraparound.

`edge_repro_test.go.txt` covers unselectable NaN keys, an unreadable NaN MemoryStore key, the zero Statechart Instance panic, and nil lifecycle actions counted as completed.

The timing/backlog probe uses several short real deadlines. The causal-retention probe briefly sets GOMAXPROCS to 1, restores it afterward, and uses an injected nonblocking Clock to expose a particular valid scheduling order. None of these probes should run in parallel with other tests in the same package.

The main review's normal test, race, static-analysis, coverage, fuzz, and benchmark results refer to the unmodified production code and original test suite. [reproduction-output.txt](reproduction-output.txt) records a separate race-enabled execution of this evidence bundle.
