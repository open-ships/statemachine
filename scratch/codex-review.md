# Maritime Safety, CE Readiness, and Go Architecture Review

Review date: 2026-08-09

Scope: the repository at main/70a7308, reviewed as a maritime robotics execution library where human safety, restart behavior, incident evidence, and product liability matter.

## Executive verdict

This codebase is a strong safety-adjacent application-orchestration library. It is not sufficient as a safety-rated control element, the sole controller for hazardous motion, or evidence of CE conformity.

The correct deployment judgment is:

- Go for coordinating application logic inside a larger, independently protected vessel architecture.
- No-go for directly owning the only path to thrusters, winches, brakes, gangways, steering, launch/recovery machinery, or mobile robotic motion around people.

The repository itself states this limitation accurately in [SAFETY.md](../SAFETY.md#safety-adjacent-use). Logical state is not physical truth, and Supervisor.Trip is not an emergency stop.

Direct answers:

| Question | Assessment |
|---|---|
| Does it do everything needed? | No. Independent protective functions, durable command closure, recovery adjudication, authority/fencing, evidence integrity, and the product safety case remain external. |
| Are its patterns abnormal? | Mostly sound. Record creation after operation completion, post-commit Observer errors, prepare-only journaling, and goroutine-based callback containment have important sharp edges. |
| Is it immutable enough? | Structurally yes for ordinary Go; no for forensic evidence. States, events, errors, callbacks, Records, Snapshots, and application data are shallow. |
| Is it observable enough? | Supervisor.Status is strong, but lifecycle order, persistence, retention, and ordinary Observers are not liability-grade. |
| Does it follow strong Go practice? | Generally yes, with one release-blocking module-version defect and several medium concurrency/interface issues. |

## Release and deployment blockers

### 1. This is not an independent safety function

Supervisor.Trip and operation timeout cancel a Go context and latch a software Fault. They cannot stop a callback that ignores cancellation and cannot prove that physical work stopped. See [Trip](../supervised/execution.go#L668), [callback timeout selection](../supervised/execution.go#L920), and [fault latching](../supervised/execution.go#L1148).

Concrete scenario: a thruster or winch command reaches its controller, the callback blocks, and the operation times out. The Supervisor becomes Faulted and reports CallbackRunning, but thrust or haul-in may continue.

A deployed system still requires independent emergency stop, protective stop, safe torque off or brake application, overspeed protection, collision avoidance, human-presence separation, watchdogs, command expiry, and measured worst-case physical response.

### 2. The declared v2 release is invalid under Go module versioning

[VERSION](../VERSION) and [CHANGELOG.md](../CHANGELOG.md#200) declare 2.0.0, while [go.mod](../go.mod) still declares:

    module github.com/open-ships/statemachine

A v2 Go module must use github.com/open-ships/statemachine/v2, including its internal imports and public examples. Otherwise a v2.0.0 tag is not a valid consumable v2 at the advertised import path.

Before release, either migrate the module and imports to /v2 or keep the release on v1. Add a clean external-consumer test that resolves the exact proposed tag.

Official Go rule: https://go.dev/ref/mod#major-version-suffixes

## High-severity implementation findings

### 3. Lifecycle Record ordering is not causally authoritative

Record.Seq is documented as authoritative ordering, but public operations create their Record only after the internal operation has released the Supervisor. See [Issue](../supervised/execution.go#L381) and [recordResult](../supervised/record.go#L86).

Record.Mode and Record.At are sampled when recordResult eventually obtains its locks, rather than at the operation's state-linearization point. A concurrent Issue, Trip, Recover, or verification expiry can therefore:

- receive an earlier Record sequence despite occurring later;
- cause an earlier Record to describe a later Mode; or
- make Record.At represent recorder serialization delay rather than outcome time.

Restoration creates another identity defect. Snapshot preserves ExecutionID but not the lifecycle Record high-water mark. A restored execution begins again at Record.Seq 1, duplicating the durable identity pair (ExecutionID, Seq).

For incident reconstruction, Record identity, time, Mode, and sequence must be assigned atomically with the state decision. Delivery can occur later, but identity cannot.

### 4. Persistence is prepare-only and cannot durably close an issued Change

The Journal receives an in-doubt Snapshot before external Issue in [prepareIssue](../supervised/execution.go#L544). Successful commit updates only in-memory state and Revision in [commit](../supervised/execution.go#L1015). The Journal is not updated with a committed, rejected, faulted, or reconciled closure.

Concrete scenario:

1. Engage propulsion is issued.
2. Physical Verification succeeds.
3. Logical state commits to Underway.
4. The process loses power before an application-owned clean Snapshot is persisted.
5. The last durable Journal entry still says in doubt from Ready.

Restoration conservatively faults, which is correct, but Recover can only run predicate-like Reconcilers. A successful Recover clears the Fault while keeping the old state at [recovery completion](../supervised/execution.go#L772). It cannot explicitly adopt a controller-proven Destination, retain Source, or select and record a minimum-risk state.

The durable execution protocol needs explicit closure and a typed, auditable reconciliation decision for every power-loss point.

### 5. Observation and recording can exhaust or freeze execution

Observer isolation launches a goroutine and waits indefinitely for it at [internal/observer.Call](../internal/observer/observer.go#L17). There is no timeout or cancellation select.

A blocked network logger can therefore:

- leave an Instance or Statechart in flight after state already committed;
- freeze a queued Runtime's drain loop and every later root; and
- prevent the caller from learning the committed outcome.

Supervisor Records also grow without limit at [record append](../supervised/record.go#L108). Recorder timeout bounds recordResult's wait, but it cannot terminate a Recorder that ignores cancellation. One permanently blocked Recorder goroutine can be leaked per operation.

Vessel-lifetime execution needs bounded history, bounded outstanding deliveries, and an explicit drop, retry, degrade, or fault policy.

### 6. Post-commit Observer failure shares the transition error channel

Instance commits state and then joins an Observer failure into Fire's ordinary error at [Instance.Fire](../instance.go#L122). Statechart commits and can return only an Observer error at [Statechart.Fire](../statechart/statechart.go#L687). queued.Runtime behaves similarly after one or more committed events.

This is documented through ErrObserverFailed, but it violates the common operational assumption that err != nil means retry is safe. A retry can occur after effects and state changes already committed.

Transition outcome and observation-health outcome should be structurally distinct, or observation health should be reported through an independent seam.

### 7. Safety-critical durability remains opt-in

The ordinary Supervisor constructors accept a Machine containing external Issue callbacks without a Journal or Recorder. [prepareIssue](../supervised/execution.go#L560) silently proceeds if Journal is nil. RequireJournal is optional.

For a safety-focused use profile, external Issue should default to requiring durable preparation and durable lifecycle recording. A weaker mode should be explicitly named for tests or non-hazardous application work.

### 8. Snapshot rollback protection and definition provenance are external

Restore validates DefinitionID, declared state, counters, and pending-transition structure at [Restore](../supervised/execution.go#L183), which is useful. It does not supply:

- an anti-rollback generation;
- a signature or MAC;
- an independent durable Attempt high-water authority;
- a Snapshot schema version;
- controller-side rejection of stale fencing tokens; or
- a cryptographic digest tying DefinitionID to callbacks, source, and build artifact.

A valid older Snapshot can therefore be restored unless an external durable authority detects rollback. The documentation correctly assigns fencing, replay prevention, and provenance to the integration, but they remain mandatory.

### 9. Verification evidence is conventional rather than enforceable

Verify, Invariants, Postconditions, and Reconcilers receive arbitrary shallow T. The Module cannot require or validate:

- sensor and controller source identity;
- acquisition time and maximum evidence age;
- coherent sampling across subsystems;
- controller boot or lease identity;
- calibration and health state;
- plausibility and independence;
- operator authority; or
- an immutable evidence snapshot.

This is acceptable for a generic Go library, but a CE-oriented integration profile needs typed evidence with those invariants before any Check runs.

## Immutability assessment

### Strong

- Machine and Chart compilation defensively copies tables and action slices.
- State-owning executions copy compiled definition values, preventing caller whole-value overwrite.
- Internal compiled maps and slices are private.
- Fault snapshots copy public Fault structs rather than returning Supervisor storage.
- Construction acts as the restoration seam; unrestricted state setters are absent.

### Insufficient for safety evidence

- comparable permits pointer-bearing state and event types.
- Function values and mutable closure captures remain shared.
- T is caller-owned and may be concurrently mutated.
- Snapshot, Record, Observation, state, and event values are shallow.
- Fault.Cause retains an arbitrary possibly mutable error object.
- Records returns a new outer slice but does not deep-copy referenced values.

The existing [known limitations](../docs/assurance/known-limitations.md) acknowledge most of this. A safety profile should restrict states and events to small defined scalar values, forbid mutable closure captures, serialize evidence at durable seams, and bind definitions to reviewed artifacts.

## Visibility and observability assessment

Supervisor.Status is strong. It exposes Mode, logical Snapshot, active Operation, Phase, deadlines, pending Change, Fault, callback liveness, and Recorder failure.

Supervisor Records also distinguish starts, issues, verification, trips, recovery, expiry, and secondary causes.

Remaining gaps:

- causal and restart-unique Record sequencing is broken;
- Record cause is primarily text and loses structured callback/check identity;
- durable recording is optional and fail-open;
- Recorder failure is only a volatile string, with no lost-record range, sequence, count, or replay queue;
- ordinary Observations omit attempts, refusals, action execution, self-transitions, and internal transitions by design;
- ordinary Observers cannot return an I/O error;
- Observation sequence restarts on reconstructed executions and has no built-in ExecutionID;
- non-supervised observation timestamps use time.Now directly; and
- all in-memory histories are process-local.

Ordinary Observations are useful application deltas, not a safety audit trail.

## Go design assessment

### Strong and idiomatic

- Clear package separation by state ownership and failure semantics.
- Typed generic states, events, and application data.
- Context is consistently the first callback argument.
- Errors support errors.Is, errors.As, and errors.Join appropriately.
- Constructors return concrete values; Store, Clock, Recorder, and Journal are narrow seams.
- Function adapters such as FuncStore are idiomatic.
- User callbacks generally run without the execution mutex held.
- Zero-value behavior is intentional where it is safe; Supervisor construction is strict.
- Compile and MustCompile follow familiar Go conventions.
- iter.Seq inspection methods keep immutable definitions read-only.
- No runtime dependencies beyond the standard library.

### Abnormal or risky

- Observer functions cannot return errors, so composition uses panic containment as an error-transport mechanism.
- Result plus a duplicate ordinary error is unusual, though justified by errcheck visibility.
- Goroutine-per-callback containment is sophisticated but can leak when application code ignores cancellation.
- Clock.Now, Clock.AfterFunc, and Timer.Stop are called while the Supervisor mutex is held. The Clock contract does not explicitly prohibit synchronous callbacks or lock reentrancy, permitting a custom Clock deadlock.
- Store callback-contract enforcement cannot prevent a broken Store from invoking its first callback after Update has already returned.
- Operation, Record, and Observation counters can wrap even though their contracts imply non-reuse or authoritative ordering.

The large supervised/execution.go file is not itself a defect. It preserves locality for difficult synchronization invariants. Splitting it mechanically would likely make the Module shallower and harder to audit.

## Assurance and test evidence

Local verification with Go 1.26.0:

- go test ./...
- go test -race ./...
- go vet ./...
- gofmt
- errcheck
- staticcheck
- govulncheck

All passed. Race-run statement coverage was 94.3%, and govulncheck reported no called vulnerability.

The repository also has strong multi-platform CI, a coverage floor, vulnerability scanning, signed release intent, SBOM generation, checksums, and provenance attestation.

Important limitations:

- the sole fuzz model exercises synchronous Supervisor lifecycle commands;
- CI runs fuzz seeds, not a sustained fuzz campaign;
- there is no durable-record/restoration model;
- no adversarial Clock model;
- no concurrent Trip/Verify/expiry state model;
- no Observer liveness or resource-exhaustion property test;
- no Store contract fuzzing;
- no power-loss matrix at every durable seam; and
- statement coverage is not evidence of hazard coverage or independence.

No library test establishes a required PLr, SIL, systematic capability, diagnostic coverage, or physical response time.

## CE and maritime context

At the review date, the EU Machinery Regulation is scheduled to apply from 20 January 2027. It explicitly recognizes software as a possible safety component, requires risk assessment, and requires control-system logic faults not to produce hazardous situations. Autonomous software-based safety systems may also have safety-decision recording obligations.

Official text: https://eur-lex.europa.eu/eli/reg/2023/1230/en

The non-mandatory IMO MASS Code has been effective since 1 July 2026. It emphasizes risk assessment, robust system design, cybersecurity, alert management, remote operations, and continued human responsibility. This library supplies only a small execution primitive within that system.

IMO overview: https://www.imo.org/en/mediacentre/hottopics/pages/autonomous-shipping.aspx

For products placed on the market after 9 December 2026, the revised Product Liability Directive treats software as a product and provides for evidence disclosure and legal presumptions in specified circumstances. Durable, causal records therefore matter to liability as well as operations.

Official text: https://eur-lex.europa.eu/eli/dir/2024/2853/oj

A real conformity route still needs a product-specific hazard analysis and selection of applicable standards, potentially including:

- ISO 12100 for machinery risk assessment and risk reduction;
- ISO 13849-1:2023 or IEC 62061:2021 with current amendments for safety-related control systems;
- IEC 61508 where its lifecycle is applicable;
- applicable marine equipment, flag-state, class, SOLAS, and MASS requirements; and
- cybersecurity obligations and standards appropriate to the product and deployment.

This review is an engineering assessment, not a conformity certificate or legal opinion.

## Architectural deepening priorities

### 1. Durable Supervisor lifecycle Module

Files: supervised/execution.go, supervised/record.go, supervised/result.go.

Problem: state decisions, Record identity, Journal state, and Recorder delivery do not share one causal point.

Solution: concentrate lifecycle identity and durable closure behind one deep Module, while allowing delivery to occur after the decision.

Benefits: stronger locality, restart-safe identity, causal audit order, and power-loss testing through one interface.

### 2. Recovery adjudication Module

Files: supervised/execution.go and supervised/definition.go.

Problem: Recover can validate only the old logical state and cannot record a decision to retain Source, adopt Destination, or select a minimum-risk state.

Solution: make reconciliation produce an explicit, durable adjudication before the Supervisor returns to Ready.

Benefits: leverage across controller-restart cases, better locality for recovery rules, and direct tests for every in-doubt physical outcome.

### 3. Bounded observation-delivery Module

Files: observe.go, internal/observer, queued/observe.go, and statechart/observe.go.

Problem: synchronous delivery is duplicated, can block forever, and conflates observation failure with transition failure.

Solution: concentrate delivery deadlines, backpressure, lineage, and failure classification at one seam.

Benefits: deterministic resource bounds, clearer caller behavior, and reusable liveness tests.

### 4. Deterministic Supervisor model Module

Files: principally supervised/execution.go.

Problem: state decisions, Clock behavior, callback ownership, persistence, and delivery are interwoven.

Solution: isolate a deterministic execution model behind the existing Supervisor interface while keeping callbacks and Adapters in the implementation.

Benefits: model checking, replay, fault injection, and improved locality without expanding the public interface.

## Bottom line

The repository is substantially better than a typical Go state-machine library. Its domain language, failure semantics, documentation, tests, and explicit safety limitations are excellent.

It should be used as application orchestration behind independent protection, not represented as CE-ready or safety-rated. Before v2 release, the module path must be corrected. Before any safety-adjacent production claim, causal durable lifecycle recording, closed-loop restart adjudication, bounded observation resources, and a hazard-derived system assurance case are the most important next steps.
