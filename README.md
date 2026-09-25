# Substrates Technology Compatibility Kit

Executable compatibility tests for Java providers of the Substrates API.

The TCK does not build or install the API or provider. Those artifacts must already be available to
Maven before the tests are started.

## Preconditions

Verify all of the following before running the TCK:

1. **Java 26 is active.** `JAVA_HOME` and `java --version` must select JDK 26.
2. **The published Humainary artifacts are Maven-resolvable.** The defaults are:
    - `io.humainary.substrates:humainary-substrates-api:3.6.0` — the Substrates API
    - `io.humainary.specs:humainary-specs-api:3.6.0` — the `@SpecDoc` / `@SpecRef` traceability
      annotations the test sources reference. Scope `provided`: needed to compile the tests, and
      `SOURCE`-retained, so none of it reaches the run
3. **The provider artifact is Maven-resolvable.** You must know its Maven `groupId`, `artifactId`,
   and `version`. Configure any remote repositories and credentials in Maven settings, or install
   unpublished artifacts into the local Maven repository.
4. **The provider is discoverable at runtime.** Its class must extend
   `io.humainary.substrates.spi.CortexProvider`, have a public no-argument constructor, and be
   selected
   using one of these mechanisms:
    - Preferably, the provider artifact contains
      `META-INF/services/io.humainary.substrates.spi.CortexProvider` with exactly one provider
      class.
    - Alternatively, pass `-Dio.humainary.substrates.spi.provider=<provider-class>` when running
      Maven.
5. **Only the intended provider is selected.** If multiple provider artifacts are visible through
   `ServiceLoader`, select one explicitly with the system property above.

The Maven wrapper is included; a system Maven installation is not required. On first use, the
wrapper
must be able to download Maven 3.9.16 unless that distribution is already cached. On Windows,
replace
`./mvnw` with `mvnw.cmd`.

## Run the portable TCK

Run from the repository root and supply all three provider coordinates:

```sh
./mvnw clean test \
  -Dsubstrates.spi.groupId=com.example \
  -Dsubstrates.spi.artifactId=example-substrates-provider \
  -Dsubstrates.spi.version=1.0.0
```

Supplying `substrates.spi.artifactId` activates the provider dependency. The corresponding
`substrates.spi.groupId` and `substrates.spi.version` properties are therefore also required.

If the provider does not use `ServiceLoader`, select its provider class explicitly:

```sh
./mvnw clean test \
  -Dsubstrates.spi.groupId=com.example \
  -Dsubstrates.spi.artifactId=example-substrates-provider \
  -Dsubstrates.spi.version=1.0.0 \
  -Dio.humainary.substrates.spi.provider=com.example.ExampleCortexProvider
```

This TCK defaults to `3.6.0` for every Humainary artifact it resolves. Intentional overrides can be
supplied per artifact:

| Property                 | Artifact                   |
|--------------------------|----------------------------|
| `substrates.api.version` | `humainary-substrates-api` |
| `specs.api.version`      | `humainary-specs-api`      |

A result then applies to that combination of API version and provider.

## Interpret the result

The default run executes 1,067 conformance tests in this revision:

- `BUILD SUCCESS`, with zero failures and zero errors, means the API/provider combination passed.
- Any failed or errored test means it did not pass.
- Successful compilation alone is not a TCK pass.

Some tests deliberately provoke and isolate callback exceptions, so expected exception text may be
printed during a successful run. Use the final Maven/JUnit summary as the result.

Concurrent tests use `TestTasks` to own submitted futures and executor cleanup. Fixed pools retain
their worker counts and queueing with daemon platform threads; virtual tasks retain virtual threads.
Tests check completion explicitly through `TestSupport.get`. Cleanup cancels tracked work and
requests
shutdown without waiting for interruption-resistant provider calls. Such calls can outlive a failed
test, but their worker threads cannot prevent JVM exit. Tests still own their gates and circuits.

Coordination waits default to 10 seconds (`tck.coordination.timeout.seconds`). The JUnit test
timeout
defaults to `2 m` (`tck.test.timeout`), but interruption cannot terminate an uninterruptible
provider
call on the test thread. Surefire's fork timeout is the process-level backstop: 1800 seconds by
default,
configurable with `-Dtck.fork.timeout.seconds=<seconds>`.

The two `TestTasksTest` harness checks are tagged `harness-check` and excluded from conformance
runs,
including the scheduler diagnostic profile. They verify the test infrastructure, not the provider.
One launches child JVMs and requires permission to create processes. Maintainers can run both
explicitly,
without configuring a provider:

```sh
./mvnw test -Dtest=TestTasksTest -Dgroups=harness-check \
  -Dtck.excludedGroups=scheduler-performance
```

## Optional scheduler diagnostic

The scheduler-performance test is excluded from portable conformance. Include it separately with:

```sh
./mvnw -Pscheduler-performance clean test \
  -Dsubstrates.spi.groupId=com.example \
  -Dsubstrates.spi.artifactId=example-substrates-provider \
  -Dsubstrates.spi.version=1.0.0
```

Tests use source-retained `@SpecRef` annotations to document relevant specification sections. The
annotations do not affect execution or determine the result. Executable suites are named
`<SurfaceOrConcern>ContractTest`.

## What a conformance test may not assert

A TCK test composes guarantees the specification makes. It must not create stronger ones, however
reasonable they look on a given provider. Each row below is an assertion that passes against one
plausible implementation and fails against another that is equally conformant.

| Tempting assertion                                                     | Correct boundary                                                                            |
|------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| Awaiting one circuit means its whole distributed cascade has completed | `await` is local; a network needs a terminal acknowledgement                                |
| A fan-out finishes its first-listed target before its second           | List order is the order targets are *enqueued*, not the order they complete (§16.3, §5.3)   |
| Concurrent callers produce one global sequence                         | Only per-caller order, causal order, and consistent local observation are fixed             |
| Closing a source revokes work already admitted to a remote endpoint    | Communicating with a circuit confers neither ownership of it nor distributed cancellation   |
| Every pre-close emission completes                                     | §§9.1/9.3 permit documented provider choices; quiesce before asserting exact delivery       |
| Every `onClose` callback runs even after its circuit has terminated    | §9.1 explicitly permits cleanup never to drain in that situation                            |
| A retained `Window` may be forwarded to another circuit                | It is callback-scoped; copy its values inside its lifetime                                  |
| One receptor registered on two circuits is globally serialized         | §6.3 permits concurrent invocation; synchronize its shared test observations                |
| Mixed receptor and pipe registrations have one total callback order    | The Java projection leaves mixed-kind invocation order unspecified                          |
| An idle circuit proves another circuit runs within some latency        | There is no fairness or scheduling bound to assert; keep such checks as bounded diagnostics |

The converse also holds: an assertion weak enough to hold under every implementation, conformant or
not, records no obligation. `assertTrue(count >= 0)` on a counter is the degenerate case.
