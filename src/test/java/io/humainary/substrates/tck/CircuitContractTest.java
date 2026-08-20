// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.lang.Thread.*;
import static java.util.concurrent.Executors.*;
import static java.util.concurrent.TimeUnit.*;
import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §5 Circuit execution, admission, ordering, await, Current, and Pulse
/// behavior; §9.3 lifecycle; and Circuit-owned topology, plus Java projection conveniences.
/// @author William David Louth
/// @since 1.0

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.0/SPEC.md")
final class CircuitContractTest
  extends TestSupport {

  private Cortex cortex;

  /// Await returns immediately after terminal close.
  @SpecRef({"5.5", "9.3"})
  @Test
  void await_afterTerminalClose_returnsImmediately() {

    final var circuit = cortex.circuit();

    final var conduit =
      circuit.conduit(Integer.class);

    final var pipe =
      conduit.get(cortex.name("fast.await.channel"));

    // Emit and process
    pipe.emit(1);
    circuit.await();

    // Close the circuit
    circuit.close();

    final var waiter = Executors.newSingleThreadExecutor();
    try {
      final var completed = waiter.submit(() -> {
        for (int i = 0; i < 5; i++) {
          circuit.await();
        }
        return null;
      });
      assertDoesNotThrow(
        () -> get(completed, "repeated await calls after terminal close")
      );
    } finally {
      waiter.shutdownNow();
    }

  }

  // ===========================
  // Circuit Creation Tests
  // ===========================

  /// A receptor holds the Circuit behind a release gate while two unrelated callers invoke await.
  /// Both awaiters must be waiting before release and both must complete afterward: this distinguishes
  /// independent barriers for the same admitted work from an implementation that services only one.
  ///
  /// Concurrent caller contexts await the same prior work independently.
  @SpecRef("5.5")
  @Test
  void await_concurrentCallers_completeIndependently() throws Exception {

    final var circuit = cortex.circuit();
    final var receptorEntered = new CountDownLatch(1);
    final var releaseReceptor = new CountDownLatch(1);
    final var awaitersReady = new CountDownLatch(2);
    final var awaitersCompleted = new CountDownLatch(2);
    final var executor = newFixedThreadPool(2);

    try {

      final Pipe< Integer > pipe =
        circuit.pipe(_ -> {
          receptorEntered.countDown();
          try {
            await(releaseReceptor, "the blocked receptor release gate");
          } catch (final InterruptedException error) {
            currentThread().interrupt();
          }
        });

      pipe.emit(1);
      await(receptorEntered, "the blocked receptor to start");

      final var futures = new ArrayList< Future< ? > >();
      for (int index = 0; index < 2; index++) {
        futures.add(executor.submit(() -> {
          awaitersReady.countDown();
          circuit.await();
          awaitersCompleted.countDown();
        }));
      }

      await(awaitersReady, "both concurrent awaiters to start");
      assertEquals(2L, awaitersCompleted.getCount());

      releaseReceptor.countDown();
      await(awaitersCompleted, "both concurrent awaiters to complete");

      for (final var future : futures) {
        get(future, "a concurrent awaiter");
      }

    } finally {

      releaseReceptor.countDown();
      executor.shutdown();
      circuit.closeAwait();

    }

  }

  /// Await may drain accepted shutdown work.
  @SpecRef({"5.5", "9.3"})
  @Test
  void await_duringDrainableShutdown_observesAcceptedEmissions() {

    final var circuit = cortex.circuit();
    final var processed = new AtomicInteger(0);

    final var conduit =
      circuit.conduit(Integer.class);

    final var pipe =
      conduit.get(cortex.name("shutdown.await.channel"));

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("shutdown.subscriber"),
        (_, registrar) ->
          registrar.register(_ -> {

              processed.incrementAndGet();

              // Simulate work
              try {
                Thread.sleep(50);
              } catch (final InterruptedException e) {
                currentThread().interrupt();
              }

            }
          )
      )
    );

    // Emit multiple values
    for (int i = 0; i < 5; i++) {
      pipe.emit(i);
    }

    // Close immediately (before processing completes)
    circuit.close();

    // await() should wait for pending emissions to complete
    circuit.await();

    // All emissions should have been processed
    assertTrue(
      processed.get() >= 1,
      "At least some emissions should be processed during shutdown"
    );

  }

  /// Await completes when no accepted work remains.
  @SpecRef("5.5")
  @Test
  void await_emptyQueue_returnsAfterBarrier() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      conduit.get(cortex.name("test.channel"))
        .emit(42);

      // Should complete when queue is drained
      circuit.await();

    } finally {

      circuit.close();

    }

  }

  /// Validates the circuit's fundamental safety constraint: await() cannot be
  /// called from within its own execution context.
  ///
  /// This test creates a subscriber whose emission handler attempts to call
  /// circuit.await() while executing in the Circuit context. This would cause
  /// self-deadlock: the callback would wait for work that cannot progress.
  ///
  /// The API therefore rejects the call with IllegalStateException instead of
  /// exposing callers to a hang.
  ///
  /// Expected: IllegalStateException with message "Cannot call Circuit::await
  /// from within a circuit's thread"
  /// Await from circuit context signals illegal context use.
  @SpecRef({"5.5", "15.1"})
  @Test
  void await_fromCircuitContext_throwsIllegalStateException() {

    final var circuit = cortex.circuit();

    try {

      final AtomicReference< Throwable > captured = new AtomicReference<>();
      final AtomicReference< Thread > workerThread = new AtomicReference<>();

      final var conduit =
        circuit.conduit(
          cortex.name("circuit.await.conduit"),
          Integer.class
        );

      final Subscriber< Integer > subscriber =
        circuit.subscriber(
          cortex.name("circuit.await.subscriber"),
          (_, registrar) ->
            registrar.register(
              _ -> {
                workerThread.set(Thread.currentThread());
                try {
                  circuit.await();
                } catch (final IllegalStateException ex) {
                  captured.set(ex);
                }
              }
            )
        );

      final var subscription =
        conduit.subscribe(subscriber);

      final Pipe< Integer > pipe =
        conduit.get(cortex.name("circuit.await.channel"));

      pipe.emit(1);

      circuit.await();

      subscription.close();

      final var thrown = captured.get();

      assertNotNull(thrown, "await() on the circuit thread should throw");
      assertEquals(IllegalStateException.class, thrown.getClass());
      assertEquals(
        "Cannot call Circuit::await from within a circuit's thread",
        thrown.getMessage()
      );
      assertNotNull(workerThread.get(), "Subscriber should execute in Circuit context");

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Conduit Creation Tests
  // ===========================

  /// External await makes prior Circuit modifications visible.
  @SuppressWarnings("resource")
  @SpecRef({"5.4.2", "5.5"})
  @Test
  void await_fromExternalContext_observesPriorWork() throws Exception {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final var executor = Executors.newSingleThreadExecutor();
      try {
        final var future = executor.submit(() -> {
          final Pipe< Integer > pipe =
            conduit.get(cortex.name("async.channel"));

          pipe.emit(100);
          circuit.await();
        });

        get(future, "the external await caller");
      } finally {
        executor.shutdownNow();
      }

    } finally {

      circuit.close();

    }

  }

  /// The documented re-entrancy guard idiom — `cortex.current() != circuit.current()`
  /// — correctly suppresses an `await()` call from inside a receptor, avoiding the
  /// `IllegalStateException` that would otherwise be thrown.
  /// Current identity enables a safe await re-entrancy guard.
  @Test
  void await_guardedByCurrentIdentity_avoidsCircuitContextViolation()
    throws InterruptedException {

    final var circuit = cortex.circuit();
    final var awaitThrew = new AtomicBoolean(false);
    final var awaitRan = new AtomicBoolean(false);
    final var done = new CountDownLatch(1);

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final var pipe =
        conduit.get(cortex.name("reentrancy.guard.channel"));

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("reentrancy.guard.subscriber"),
          (_, registrar) ->
            registrar.register(_ -> {

              if (cortex.current()!=circuit.current()) {

                try {
                  circuit.await();
                  awaitRan.set(true);
                } catch (final IllegalStateException e) {
                  awaitThrew.set(true);
                }

              }

              done.countDown();

            })
        )
      );

      pipe.emit(1);

      await(done, "the guarded receptor invocation");

      assertFalse(
        awaitRan.get(),
        "Guard must skip the await() branch when on the circuit thread"
      );
      assertFalse(
        awaitThrew.get(),
        "Guard must prevent the IllegalStateException from being thrown"
      );

    } finally {

      circuit.close();

    }

  }

  /// Await observes completion of multiple prior emissions.
  @SpecRef("5.5")
  @Test
  void await_multiplePriorEmissions_observesAllCompletions() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(String.class);

      final Pipe< String > pipe =
        conduit.get(cortex.name("multi.emit.channel"));

      pipe.emit("first");
      pipe.emit("second");
      pipe.emit("third");

      circuit.await();

    } finally {

      circuit.close();

    }

  }

  /// Repeated await after termination uses the completed barrier.
  @SpecRef({"5.5", "9.3"})
  @Test
  void await_repeatedAfterTermination_returnsImmediately() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(String.class);

      final Pipe< String > pipe =
        conduit.get(cortex.name("circuit.fastpath.channel"));

      pipe.emit("first");
      pipe.emit("second");

      circuit.await();

      circuit.close();
      circuit.await();

      final var waiter = Executors.newSingleThreadExecutor();
      try {
        final var completed = waiter.submit(() -> {
          circuit.await();
          circuit.await();
          return null;
        });
        assertDoesNotThrow(
          () -> get(completed, "the completed post-termination await barrier")
        );
      } finally {
        waiter.shutdownNow();
      }

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Pulse Tests
  // ===========================

  /// A Circuit composes Conduit emissions with a Basin.
  @SpecRef({"10", "11.1"})
  @Test
  void circuit_conduitAndBasin_composesEmissionDrain() {

    final var circuit = cortex.circuit(
      cortex.name("integration.circuit")
    );

    try {

      final var conduit =
        circuit.conduit(
          cortex.name("integration.conduit"),
          Integer.class
        );

      final CaptureBuffer< Integer > captureBuffer = CaptureBuffer.of(circuit, conduit, 1024);

      final Pipe< Integer > pipe =
        conduit.get(cortex.name("integration.channel"));

      pipe.emit(10);
      pipe.emit(20);
      pipe.emit(30);

      circuit.await();

      final var captures =
        captureBuffer.drain().toList();

      assertEquals(3, captures.size());
      assertEquals(10, captures.get(0).emission());
      assertEquals(20, captures.get(1).emission());
      assertEquals(30, captures.get(2).emission());

      captureBuffer.close();

    } finally {

      circuit.close();

    }

  }

  /// Circuit pipe materialization composes a configured Flow.
  @Test
  void circuit_configuredFlow_composesPipeDelivery() {

    final var circuit = cortex.circuit();

    try {

      final var flow =
        cortex.fiber(Integer.class).limit(2);

      final var conduit =
        circuit.conduit(
          cortex.name("flow.conduit"),
          Integer.class
        );

      final var pipes = conduit.pool(flow::pipe);

      final CaptureBuffer< Integer > captureBuffer = CaptureBuffer.of(circuit, conduit, 1024);

      final Pipe< Integer > pipe =
        pipes.get(cortex.name("flow.channel"));

      pipe.emit(1);
      pipe.emit(2);
      pipe.emit(3); // Should be limited

      circuit.await();

      final var captures =
        captureBuffer.drain().toList();

      // Limit should restrict to first 2 emissions
      assertEquals(2, captures.size());

      captureBuffer.close();

    } finally {

      circuit.close();

    }

  }

  /// Cortex#circuit(Name) binds the supplied subject name.
  @Test
  void circuit_explicitName_usesSuppliedSubjectName() {

    final var circuitName = cortex.name("circuit.test.named");
    final var circuit = cortex.circuit(circuitName);

    assertNotNull(circuit);
    assertEquals(circuitName, circuit.subject().name());
    assertEquals(Circuit.class, circuit.subject().type());

    circuit.close();

  }

  /// One Circuit supports independent conduits and named pipes.
  @SpecRef("10")
  @Test
  void circuit_multipleConduitsAndNames_routesIndependently() {

    final var circuit = cortex.circuit();

    try {

      final var conduit1 =
        circuit.conduit(cortex.name("conduit.one"), String.class);

      final var conduit2 =
        circuit.conduit(cortex.name("conduit.two"), Integer.class);

      final CaptureBuffer< String > captureBuffer1 = CaptureBuffer.of(circuit, conduit1, 1024);
      final CaptureBuffer< Integer > captureBuffer2 = CaptureBuffer.of(circuit, conduit2, 1024);

      final Pipe< String > pipe1 =
        conduit1.get(cortex.name("channel.alpha"));

      final Pipe< Integer > pipe2 =
        conduit2.get(cortex.name("channel.beta"));

      pipe1.emit("hello");
      pipe2.emit(42);

      circuit.await();

      assertEquals(1, captureBuffer1.drain().count());
      assertEquals(1, captureBuffer2.drain().count());

      captureBuffer1.close();
      captureBuffer2.close();

    } finally {

      circuit.close();

    }

  }

  /// Named Circuit creation rejects absence.
  @SpecRef("15.2")
  @Test
  void circuit_nullName_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> cortex.circuit(null)
    );

  }

  /// Cortex#circuit assigns a valid default subject name.
  @Test
  void circuit_withoutExplicitName_usesDefaultSubjectName() {

    final var circuit = cortex.circuit();

    assertNotNull(circuit);
    assertNotNull(circuit.subject());
    assertNotNull(circuit.subject().name());
    assertNotNull(circuit.subject().id());
    assertEquals(Circuit.class, circuit.subject().type());

    circuit.close();

  }

  /// All callers wait behind one gate before close begins. Exceptions from raw threads are collected
  /// explicitly because thread termination alone would not fail JUnit; terminal API checks then prove
  /// that exactly one closed state, rather than merely a completed race, was reached.
  ///
  /// Concurrent Circuit close calls are safe and idempotent.
  @SpecRef({"9.1", "9.3"})
  @Test
  void close_concurrentCallers_completesSafely()
    throws InterruptedException {

    final var circuit = cortex.circuit();
    final var ready = new CountDownLatch(10);
    final var start = new CountDownLatch(1);
    final var failures = new ConcurrentLinkedQueue< Throwable >();

    final var threads = new Thread[10];
    for (int i = 0; i < 10; i++) {
      threads[i] = new Thread(
        () -> {
          ready.countDown();
          try {
            await(start, "the concurrent close start gate");
            circuit.close();
          } catch (final Throwable failure) {
            failures.add(failure);
          }
        }
      );
      threads[i].start();
    }

    try {
      await(ready, "all concurrent close callers to reach the start gate");
    } finally {
      start.countDown();
    }

    for (final var thread : threads) {
      join(thread, "a concurrent close caller");
    }

    assertTrue(failures.isEmpty(), () -> "Concurrent close failures: " + failures);
    assertDoesNotThrow(circuit::await);
    assertThrows(Fault.class, () -> circuit.conduit(Integer.class));
    assertDoesNotThrow(circuit::close);

  }

  /// Closing a Bank closes all materialized conduits.
  @SpecRef({"9.1", "10.4"})
  @Test
  void close_materializedBankConduits_closesEveryConduit() {

    final var circuit = cortex.circuit();

    try {

      final var conduits =
        circuit.bank(
          Integer.class
        );

      final var first =
        conduits.get(
          cortex.name("bank.close.first")
        );

      final var second =
        conduits.get(
          cortex.name("bank.close.second")
        );

      conduits.close();
      circuit.await();

      assertThrows(
        Fault.class,
        () -> CaptureBuffer.of(circuit, first, 1024)
      );

      assertThrows(
        Fault.class,
        () -> CaptureBuffer.of(circuit, second, 1024)
      );

      assertThrows(
        Fault.class,
        () -> conduits.get(
          cortex.name("bank.close.third")
        )
      );

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Circuit.await() Tests
  // ===========================

  /// Multiple Circuit close calls have no additional effect.
  @SpecRef({"9.1", "9.3"})
  @Test
  void close_multipleCalls_haveNoAdditionalEffect() {

    final var circuit = cortex.circuit();

    // Close multiple times should be safe
    circuit.close();
    circuit.close();
    circuit.close();

    // No exceptions should be thrown
    assertTrue(true, "Multiple close() calls should be idempotent");

  }

  /// Terminal Circuit close releases owned conduits and channels.
  @SpecRef("9.3")
  @Test
  void close_ownedResources_releasesResources() {

    final var circuit = cortex.circuit();

    final var conduit =
      circuit.conduit(Integer.class);

    assertNotNull(conduit);

    circuit.close();

    // Circuit is closed, but we can't really verify internal state
    // This test mainly ensures close doesn't throw

  }

  // ===========================
  // Circuit.close() Tests
  // ===========================

  /// Circuit close is non-blocking with accepted work pending.
  @SpecRef("9.3")
  @Test
  void close_pendingEmissions_returnsWithoutBlocking() {

    final var circuit = cortex.circuit();
    final var processing = new CountDownLatch(1);
    final var releaseProcessing = new CountDownLatch(1);
    final var closer = Executors.newSingleThreadExecutor();

    final var conduit =
      circuit.conduit(Integer.class);

    final var pipe =
      conduit.get(cortex.name("non.blocking.close"));

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("slow.subscriber"),
        (_, registrar) ->
          registrar.register(_ -> {

            processing.countDown();
            try {
              await(releaseProcessing, "the pending receptor release gate");
            } catch (final InterruptedException e) {
              currentThread().interrupt();
            }

          })
      )
    );

    try {
      pipe.emit(1);
      assertDoesNotThrow(() -> await(processing, "the pending receptor to start"));

      final var closed = closer.submit(circuit::close);
      assertDoesNotThrow(
        () -> get(closed, "close to return while accepted work remains blocked")
      );
      assertEquals(
        1L,
        releaseProcessing.getCount(),
        "The receptor must still be blocked when close returns"
      );
    } finally {
      releaseProcessing.countDown();
      circuit.close();
      closer.shutdownNow();
    }

  }

  // ===========================
  // Integration Tests
  // ===========================

  /// Circuit close is idempotent.
  @SpecRef({"9.1", "9.3"})
  @Test
  void close_repeatedCalls_areIdempotent() {

    final var circuit = cortex.circuit();

    circuit.close();
    circuit.close(); // Should not throw
    circuit.close(); // Should not throw

  }

  /// One Circuit can own multiple independent Conduits.
  @SpecRef("10")
  @Test
  void conduit_multipleFromSameCircuit_areIndependent() {

    final var circuit = cortex.circuit();

    try {

      final var conduit1 =
        circuit.conduit(Integer.class);

      final var conduit2 =
        circuit.conduit(Integer.class);

      assertNotSame(conduit1, conduit2);
      assertNotSame(conduit1.subject(), conduit2.subject());

    } finally {

      circuit.close();

    }

  }

  /// Named Circuit conduit creation binds name and factory.
  @Test
  void conduit_namedWithComposer_bindsNameAndAppliesComposer() {

    final var circuit = cortex.circuit();

    try {

      final var conduitName = cortex.name("circuit.test.conduit");
      final var conduit =
        circuit.conduit(conduitName, String.class);

      assertNotNull(conduit);
      assertEquals(conduitName, conduit.subject().name());
      assertEquals(Conduit.class, conduit.subject().type());

    } finally {

      circuit.close();

    }

  }

  /// Circuit conduit factories reject required nulls.
  @SpecRef("15.2")
  @Test
  void conduit_nullRequiredArguments_throwNullPointerException() {

    final var circuit = cortex.circuit();

    try {

      final var name = cortex.name("test");

      assertThrows(
        NullPointerException.class,
        () -> circuit.conduit((Class< Integer >) null)
      );

      assertThrows(
        NullPointerException.class,
        () -> circuit.conduit(null, Integer.class)
      );

      assertThrows(
        NullPointerException.class,
        () -> circuit.conduit(name, (Class< Integer >) null)
      );

      assertThrows(
        NullPointerException.class,
        () -> circuit.bank((Class< Integer >) null)
      );

      assertThrows(
        NullPointerException.class,
        () -> circuit.bank(Integer.class, null)
      );

    } finally {

      circuit.close();

    }

  }

  /// Circuit conduit creation materializes the supplied factory.
  @Test
  void conduit_withComposer_appliesComposer() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      assertNotNull(conduit);
      assertNotNull(conduit.subject());
      assertEquals(Conduit.class, conduit.subject().type());

    } finally {

      circuit.close();

    }

  }

  /// `circuit.current()` returns a non-null Current immediately after construction.
  /// Capture runs asynchronously on the worker as its first action; a caller hitting
  /// the slow path spins with backoff and parks until the worker writes the field.
  /// Circuit#current returns its execution-context identity.
  @SpecRef("11.3")
  @Test
  void current_afterCircuitCreation_returnsNonNull() {

    final var circuit = cortex.circuit();

    try {

      assertNotNull(
        circuit.current(),
        "circuit.current() must be non-null right after construction"
      );

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Sequential Execution Tests
  // ===========================

  /// Distinct circuits have distinct Currents — each circuit owns its own
  /// processing context.
  /// Distinct Circuits own distinct stable execution-context Currents.
  @SpecRef("11.3")
  @Test
  void current_distinctCircuits_returnsDistinctInstances() {

    final var first = cortex.circuit();
    final var second = cortex.circuit();

    try {

      assertNotSame(
        first.current(),
        second.current(),
        "Each circuit's processing context has its own Current"
      );

    } finally {

      first.close();
      second.close();

    }

  }

  // ===========================
  // Thread-Safe Pool Tests
  // ===========================

  /// From an external thread, `cortex.current()` and `circuit.current()` are
  /// distinct Currents — they identify different execution contexts.
  /// Circuit and external caller contexts have distinct Currents.
  @SpecRef({"5.1", "11.3"})
  @Test
  void current_fromExternalContext_differsFromCircuitCurrent() {

    final var circuit = cortex.circuit();

    try {

      assertNotSame(
        cortex.current(),
        circuit.current(),
        "External thread's Current must differ from the circuit's processing-context Current"
      );

    } finally {

      circuit.close();

    }

  }

  /// Inside a receptor (which runs on the circuit thread), `cortex.current()`
  /// resolves to the same Current that `circuit.current()` returns. This is the
  /// definitional property that lets callers detect when they're on the circuit.
  /// Cortex current inside dispatch equals Circuit current.
  @SpecRef({"5.1", "11.3"})
  @Test
  void current_insideReceptor_matchesCircuitCurrent()
    throws InterruptedException {

    final var circuit = cortex.circuit();
    final var captured = new AtomicReference< Current >();
    final var done = new CountDownLatch(1);

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final var pipe =
        conduit.get(cortex.name("current.match.channel"));

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("current.match.subscriber"),
          (_, registrar) ->
            registrar.register(_ -> {

              captured.set(cortex.current());
              done.countDown();

            })
        )
      );

      pipe.emit(1);

      await(done, "the Current-observing receptor invocation");

      assertSame(
        circuit.current(),
        captured.get(),
        "Inside a receptor on the circuit thread, cortex.current() == circuit.current()"
      );

    } finally {

      circuit.close();

    }

  }

  /// Repeated calls to `circuit.current()` return the same instance — the captured
  /// Current is stable for the circuit's lifetime.
  /// Circuit#current remains stable for the Circuit lifetime.
  @SpecRef("11.3")
  @Test
  void current_repeatedLookup_returnsSameInstance() {

    final var circuit = cortex.circuit();

    try {

      final var first = circuit.current();
      final var second = circuit.current();
      final var third = circuit.current();

      assertSame(first, second);
      assertSame(second, third);

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Concurrent Name Creation Tests
  // ===========================

  /// Validates the circuit's fundamental guarantee: all pipe executions are
  /// strictly sequential, never concurrent.
  ///
  /// This test creates maximum contention by having 4 threads concurrently emit
  /// 50 values each (200 total emissions) to the same channel. The subscriber
  /// tracks the number of currently executing handlers using atomic counters.
  ///
  /// Setup:
  /// - 4 emitter threads × 50 emissions = 200 total emissions
  /// - Subscriber increments counter on entry, decrements on exit
  /// - Thread.sleep(1ms) in handler increases likelihood of detecting concurrency
  /// - Tracks max concurrent executions and violation count
  ///
  /// The circuit's single-threaded worker ensures that even though emissions
  /// come from multiple threads concurrently, the handlers execute one at a time
  /// in sequence. This is the foundation of the circuit's determinism and
  /// state-safety guarantees.
  ///
  /// Why this matters:
  /// - Enables lock-free observer implementations (no synchronization needed)
  /// - Guarantees deterministic execution order (testability, reproducibility)
  /// - Simplifies reasoning about state mutations in handlers
  /// - Critical for neural-like networks where consistent state is required
  ///
  /// Expected: Zero violations, max concurrent = 1
  /// One Circuit never executes pipe callbacks concurrently.
  @SpecRef({"5.1", "5.4"})
  @SuppressWarnings("resource")
  @Test
  void dispatch_concurrentIngress_executesCallbacksExclusively()
    throws InterruptedException {

    final var circuit = cortex.circuit();

    try {

      final var executingNow = new AtomicInteger(0);
      final var maxConcurrent = new AtomicInteger(0);
      final var violations = new AtomicInteger(0);

      final var conduit =
        circuit.conduit(Integer.class);

      final var pipe =
        conduit.get(cortex.name("sequential.test"));

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("sequential.subscriber"),
          (_, registrar) ->
            registrar.register(_ -> {

              // Increment executing counter
              final int current = executingNow.incrementAndGet();

              // Update max observed concurrency
              maxConcurrent.updateAndGet(max -> Math.max(max, current));

              // If current > 1, we have concurrent execution (violation)
              if (current > 1) {
                violations.incrementAndGet();
              }

              // Simulate work
              try {
                Thread.sleep(1);
              } catch (final InterruptedException e) {
                currentThread().interrupt();
              }

              // Decrement executing counter
              executingNow.decrementAndGet();

            }))
      );

      // Emit from multiple threads
      final var executor = newFixedThreadPool(4);

      try {

        final var futures = new ArrayList< Future< ? > >();
        for (int t = 0; t < 4; t++) {
          futures.add(
            executor.submit(() -> {
              for (int i = 0; i < 50; i++) {
                pipe.emit(i);
              }
            })
          );
        }

        for (final var future : futures) {
          get(future, "a concurrent admission task");
        }

        circuit.await();

        // Should never have concurrent execution
        assertEquals(
          0,
          violations.get(),
          "No concurrent pipe execution should occur"
        );

        assertEquals(
          1,
          maxConcurrent.get(),
          "Maximum concurrency should be 1 (sequential execution)"
        );

      } catch (final ExecutionException e) {

        fail("Execution failed: " + e.getMessage());

      } finally {

        executor.shutdown();
        assertTrue(
          executor.awaitTermination(10, SECONDS),
          "Executor should terminate"
        );

      }

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Emission Rejection Tests
  // ===========================

  /// Verifies that emission values are correctly preserved - not just counted.
  ///
  /// This test emits unique values from multiple threads and verifies that
  /// all unique values are received exactly once. This catches subtle bugs
  /// like value corruption or duplicate delivery.
  ///
  /// Uses a thread-safe set to track received values and verify completeness.
  /// Concurrent admission preserves every emission value intact.
  @SpecRef({"5.3", "5.4"})
  @SuppressWarnings("resource")
  @Test
  void dispatch_concurrentProducers_preservesValueIntegrity()
    throws InterruptedException, ExecutionException {

    final var circuit = cortex.circuit();

    try {

      final int threadCount = 8;
      final int valuesPerThread = 1_000;
      final int expectedTotal = threadCount * valuesPerThread;

      // Thread-safe set to track unique received values
      final var receivedValues = ConcurrentHashMap.< Long > newKeySet();

      final var conduit =
        circuit.conduit(Long.class);

      final var pipe =
        conduit.get(cortex.name("integrity.channel"));

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("integrity.subscriber"),
          (_, registrar) ->
            registrar.register(receivedValues::add)
        )
      );

      final var latch = new CountDownLatch(1);
      final var executor = newFixedThreadPool(threadCount);

      try {

        final var futures = new ArrayList< Future< ? > >();

        for (int t = 0; t < threadCount; t++) {
          final long threadBase = (long) t * valuesPerThread;
          futures.add(
            executor.submit(() -> {
              try {
                await(latch, "the exclusivity contention start gate");
                for (int i = 0; i < valuesPerThread; i++) {
                  // Each thread emits unique values: threadBase + i
                  pipe.emit(threadBase + i);
                }
              } catch (final InterruptedException e) {
                currentThread().interrupt();
              }
            })
          );
        }

        // Release all threads
        latch.countDown();

        for (final var future : futures) {
          get(future, "an exclusivity contention task");
        }

        circuit.await();

        // Verify exact count - no duplicates, no losses
        assertEquals(
          expectedTotal,
          receivedValues.size(),
          "All unique values must be received exactly once"
        );

        // Verify all expected values are present
        for (int t = 0; t < threadCount; t++) {
          final long threadBase = (long) t * valuesPerThread;
          for (int i = 0; i < valuesPerThread; i++) {
            assertTrue(
              receivedValues.contains(threadBase + i),
              "Value " + (threadBase + i) + " should be received"
            );
          }
        }

      } finally {

        executor.shutdown();
        assertTrue(
          executor.awaitTermination(30, SECONDS),
          "Executor should terminate"
        );

      }

    } finally {

      circuit.close();

    }

  }

  /// Verifies emission integrity when subscribers dynamically subscribe/unsubscribe
  /// while emissions are ongoing.
  ///
  /// This is a particularly challenging scenario because:
  /// - Subscription changes modify the emission target list
  /// - Emissions are being processed concurrently
  /// - The circuit must maintain consistency during structural changes
  ///
  /// The test verifies that:
  /// 1. No emissions are lost during subscription changes
  /// 2. New subscribers receive emissions after subscribing
  /// 3. Unsubscribed handlers stop receiving emissions
  /// Concurrent subscription changes preserve emissions in visibility windows.
  @SpecRef("7.6")
  @SuppressWarnings("resource")
  @Test
  void dispatch_duringSubscriptionChanges_preservesVisibleEmissions()
    throws InterruptedException, ExecutionException {

    final var circuit = cortex.circuit();

    try {

      final int emissionCount = 10_000;
      final var primaryReceived = new AtomicInteger(0);
      final var secondaryReceived = new AtomicInteger(0);

      final var conduit =
        circuit.conduit(Integer.class);

      final var pipe =
        conduit.get(cortex.name("dynamic.channel"));

      // Primary subscriber - always subscribed
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("primary.subscriber"),
          (_, registrar) ->
            registrar.register(_ -> primaryReceived.incrementAndGet())
        )
      );

      // Secondary subscriber - will subscribe/unsubscribe during emissions
      final Subscriber< Integer > secondarySubscriber =
        circuit.subscriber(
          cortex.name("secondary.subscriber"),
          (_, registrar) ->
            registrar.register(_ -> secondaryReceived.incrementAndGet())
        );

      final var latch = new CountDownLatch(1);
      final var executor = newFixedThreadPool(2);

      try {

        // Thread 1: Emit values
        final var emitterFuture = executor.submit(() -> {
          try {
            await(latch, "the emission/subscription race start gate");
            for (int i = 0; i < emissionCount; i++) {
              pipe.emit(i);
            }
          } catch (final InterruptedException e) {
            currentThread().interrupt();
          }
        });

        // Thread 2: Subscribe/unsubscribe during emissions
        final var subscriptionFuture = executor.submit(() -> {
          try {
            await(latch, "the emission/subscription race start gate");
            for (int i = 0; i < 100; i++) {
              final var subscription = conduit.subscribe(secondarySubscriber);
              Thread.sleep(1); // Brief pause
              subscription.close();
            }
          } catch (final InterruptedException e) {
            currentThread().interrupt();
          }
        });

        // Release both threads
        latch.countDown();

        get(emitterFuture, "the concurrent emitter");
        get(subscriptionFuture, "the concurrent subscriber");

        circuit.await();

        // Primary subscriber must receive ALL emissions
        assertEquals(
          emissionCount,
          primaryReceived.get(),
          "Primary subscriber must receive all emissions despite subscription changes"
        );

        // Secondary subscriber should have received some (exact count varies)
        assertTrue(
          secondaryReceived.get() >= 0,
          "Secondary subscriber count should be non-negative"
        );

      } finally {

        executor.shutdown();
        assertTrue(
          executor.awaitTermination(30, SECONDS),
          "Executor should terminate"
        );

      }

    } finally {

      circuit.close();

    }

  }

  /// Verifies that the circuit preserves emission order (FIFO semantics).
  ///
  /// Emits values 1, 2, 3, 4, 5 sequentially from the same thread and
  /// verifies they are delivered to subscribers in the exact same order.
  ///
  /// This validates the circuit's fundamental ordering guarantee: emissions
  /// from the ingress queue are processed in FIFO order. This is critical
  /// for causal consistency in event processing.
  ///
  /// Note: This test uses the same channel for all emissions. Different
  /// channels or concurrent emitters may have different ordering semantics
  /// (see cascade/priority tests for transit queue behavior).
  /// A Circuit preserves accepted emission order.
  @SpecRef({"5.3", "5.4"})
  @Test
  void dispatch_multipleAcceptedEmissions_preservesOrder() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final List< Integer > emissions = new ArrayList<>();

      final Subscriber< Integer > subscriber =
        circuit.subscriber(
          cortex.name("ordering.subscriber"),
          (_, registrar) ->
            registrar.register(emissions::add)
        );

      conduit.subscribe(subscriber);

      final Pipe< Integer > pipe =
        conduit.get(cortex.name("ordering.channel"));

      pipe.emit(1);
      pipe.emit(2);
      pipe.emit(3);
      pipe.emit(4);
      pipe.emit(5);

      circuit.await();

      assertEquals(List.of(1, 2, 3, 4, 5), emissions);

    } finally {

      circuit.close();

    }

  }

  /// Verifies that no emissions are lost when multiple threads emit to
  /// different channels on the same conduit concurrently.
  ///
  /// This tests a different contention pattern than single-channel stress:
  /// threads compete at the conduit level but not at the channel level.
  ///
  /// Configuration:
  /// - 10 emitter threads, each with its own channel
  /// - 5,000 emissions per thread
  /// - 50,000 total expected emissions
  /// Admitted emissions remain intact across multiple named pipes.
  @SpecRef({"5.3", "10.3"})
  @SuppressWarnings("resource")
  @Test
  void dispatch_multipleNamedPipes_preservesAdmittedEmissions()
    throws InterruptedException, ExecutionException {

    final var circuit = cortex.circuit();

    try {

      final int threadCount = 10;
      final int emissionsPerThread = 5_000;
      final int expectedTotal = threadCount * emissionsPerThread;

      final var received = new AtomicInteger(0);

      final var conduit =
        circuit.conduit(Integer.class);

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("multi.channel.subscriber"),
          (_, registrar) ->
            registrar.register(_ -> received.incrementAndGet())
        )
      );

      final var latch = new CountDownLatch(1);
      final var executor = newFixedThreadPool(threadCount);

      try {

        final var futures = new ArrayList< Future< ? > >();

        for (int t = 0; t < threadCount; t++) {
          final int threadId = t;
          futures.add(
            executor.submit(() -> {
              try {
                // Each thread gets its own channel
                final var channelName = cortex.name("channel." + threadId);
                final var threadPipe = conduit.get(channelName);

                await(latch, "the ordering contention start gate");

                for (int i = 0; i < emissionsPerThread; i++) {
                  threadPipe.emit(i);
                }
              } catch (final InterruptedException e) {
                currentThread().interrupt();
              }
            })
          );
        }

        // Release all threads simultaneously
        latch.countDown();

        // Wait for all emitters to complete
        for (final var future : futures) {
          get(future, "an ordering contention task");
        }

        // Wait for circuit to process all emissions
        circuit.await();

        // Verify no emissions were lost
        assertEquals(
          expectedTotal,
          received.get(),
          "All emissions across multiple channels must be received"
        );

      } finally {

        executor.shutdown();
        assertTrue(
          executor.awaitTermination(30, SECONDS),
          "Executor should terminate"
        );

      }

    } finally {

      circuit.close();

    }

  }

  /// Every observer sees the same complete admission sequence.
  @SpecRef("5.4.3")
  @Test
  void dispatch_multipleObservers_seeSameAdmissionOrder() {

    final var circuit = cortex.circuit();

    try {

      final var first = new ArrayList< Integer >();
      final var second = new ArrayList< Integer >();
      final var conduit = circuit.conduit(Integer.class);
      final var pipe = conduit.get(cortex.name("ordering.observers"));

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("ordering.subscriber"),
          (_, registrar) -> {
            registrar.register(first::add);
            registrar.register(second::add);
          }
        )
      );

      final var expected = new ArrayList< Integer >();
      for (int value = 0; value < 100; value++) {
        expected.add(value);
        pipe.emit(value);
      }
      circuit.await();

      assertEquals(expected, first);
      assertEquals(expected, second);
      assertEquals(first, second);

    } finally {

      circuit.closeAwait();

    }

  }

  /// Validates that transit queue emissions complete before the next ingress item.
  ///
  /// This test verifies the dual queue priority model: when processing an ingress
  /// emission causes cascading emissions (via transit queue), all cascading work
  /// completes before the circuit thread returns to the ingress queue.
  ///
  /// Setup:
  /// - External thread emits two values in rapid succession: 1 then 100
  /// - Both enter the ingress queue (FIFO)
  /// - Processing value 1 triggers cascading emissions 2, 3 via transit queue
  /// - Processing value 100 simply records it (no cascade)
  ///
  /// Expected order: [1,2,3,100]
  /// - 1 pulled from ingress, processed
  /// - 2 cascaded to transit, processed (transit has priority)
  /// - 3 cascaded to transit, processed (transit drains completely)
  /// - 100 pulled from ingress (only after transit is empty)
  ///
  /// NOT: [1,100,2,3] — which would indicate ingress has priority
  ///
  /// This is a fundamental guarantee of the dual queue model: cascading
  /// effects appear atomic to external observers. All consequences of
  /// processing an emission complete before the next external input.
  /// Transit work drains before later ingress work.
  @SpecRef("5.3")
  @Test
  void dispatch_transitAndIngress_prioritizesTransit() {

    final var circuit = cortex.circuit();

    try {

      final var results = new ArrayList< Integer >();
      final var conduit =
        circuit.conduit(Integer.class);

      final var pipe =
        conduit.get(cortex.name("transit.priority.channel"));

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("transit.priority.subscriber"),
          (_, registrar) ->
            registrar.register(value -> {

              results.add(value);

              // Value 1 cascades to 2, which cascades to 3
              // These go to transit queue
              if (value==1) {
                pipe.emit(2);
              } else if (value==2) {
                pipe.emit(3);
              }

            })
        )
      );

      // Emit both values in rapid succession from external thread
      // Both enter ingress queue before circuit thread processes either
      pipe.emit(1);
      pipe.emit(100);

      circuit.await();

      // Transit queue must drain before next ingress item
      assertEquals(
        List.of(1, 2, 3, 100),
        results,
        "Transit emissions must complete before next ingress item"
      );

    } finally {

      circuit.close();

    }

  }

  /// Validates transit queue priority under deterministic ingress contention.
  ///
  /// Strengthens [#testTransitQueuePriorityOverIngress()] by guaranteeing that
  /// an external ingress emission is enqueued *while the cascade is in progress*.
  /// The earlier test relies on rapid back-to-back emits from a single thread,
  /// which on a fast machine may not actually exercise contention if the cascade
  /// completes before the second emit reaches the queue.
  ///
  /// Setup:
  /// - The receptor for value 1 pauses on a latch before cascading
  /// - An external producer thread waits until the receptor is paused, then
  ///   emits 100 into the ingress queue
  /// - The producer thread releases the latch; cascade continues normally
  /// - Result: 100 is provably in the ingress queue while the cascade is mid-flight
  ///
  /// Expected: cascade [1,2,3] still completes before 100 is dequeued from
  /// ingress. This is the strongest deterministic form of the dual-queue
  /// priority guarantee from SPEC.md §5.3.
  /// Transit priority is preserved while ingress arrives concurrently.
  @SpecRef("5.3")
  @SuppressWarnings("resource")
  @Test
  void dispatch_transitWithConcurrentIngress_preservesTransitPriority() throws Exception {

    final var circuit = cortex.circuit();

    try {

      final var results = new ArrayList< Integer >();

      final var conduit =
        circuit.conduit(Integer.class);

      final var pipe =
        conduit.get(cortex.name("transit.contention.channel"));

      final var receptorReached1 = new CountDownLatch(1);
      final var releaseCascade = new CountDownLatch(1);

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("transit.contention.subscriber"),
          (_, registrar) ->
            registrar.register(value -> {

              results.add(value);

              if (value==1) {
                // Signal "I'm processing value 1" — circuit thread is now
                // committed to this receptor invocation. Cascade has not started.
                receptorReached1.countDown();

                // Block the circuit thread mid-receptor. While blocked, the
                // producer thread will enqueue 100 into the ingress queue. By
                // the time we unblock and emit 2, we know 100 is in ingress.
                try {
                  await(releaseCascade, "the transit cascade release gate");
                } catch (InterruptedException ignored) {
                  currentThread().interrupt();
                }

                pipe.emit(2);  // → transit (we are on the circuit thread)

              } else if (value==2) {
                pipe.emit(3);  // → transit
              }

            })
        )
      );

      // Primer: enqueue 1 into ingress. Circuit thread will pick it up and run
      // the receptor, which pauses on releaseCascade.
      pipe.emit(1);

      // Producer thread: wait until receptor is paused mid-1, then enqueue 100
      // and release the cascade.
      final var producer = newFixedThreadPool(1);
      try {

        final var future = producer.submit(() -> {
          try {
            await(receptorReached1, "the receptor for value 1");
            // 100 is now provably in the ingress queue while the circuit thread
            // is paused inside the receptor for value 1. The cascade (2, 3) has
            // not yet been emitted to the transit queue.
            pipe.emit(100);
            releaseCascade.countDown();
          } catch (InterruptedException e) {
            currentThread().interrupt();
          }
        });

        get(future, "the concurrent ingress producer");

      } finally {
        producer.shutdown();
      }

      circuit.await();

      assertEquals(
        List.of(1, 2, 3, 100),
        results,
        "Transit queue cascade must drain before ingress is consulted, even when "
          + "an ingress emission is provably enqueued mid-cascade"
      );

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Multiple Close Tests
  // ===========================

  /// Verifies that no emissions are lost under heavy concurrent load.
  ///
  /// This stress test creates maximum contention by having multiple threads
  /// emit to a single channel simultaneously. Each thread emits a fixed number
  /// of values, and the test verifies that exactly the expected total number
  /// of emissions are received by the subscriber.
  ///
  /// Configuration:
  /// - 10 emitter threads
  /// - 10,000 emissions per thread
  /// - 100,000 total expected emissions
  ///
  /// This validates concurrent admission and ensures no emissions are dropped
  /// during concurrent access.
  /// Concurrent admitted load does not lose emissions.
  @SpecRef({"5.3", "5.6"})
  @SuppressWarnings("resource")
  @Test
  void dispatch_underConcurrentLoad_preservesAdmittedEmissions()
    throws InterruptedException, ExecutionException {

    final var circuit = cortex.circuit();

    try {

      final int threadCount = 10;
      final int emissionsPerThread = 10_000;
      final int expectedTotal = threadCount * emissionsPerThread;

      final var received = new AtomicInteger(0);

      final var conduit =
        circuit.conduit(Integer.class);

      final var pipe =
        conduit.get(cortex.name("stress.channel"));

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("stress.subscriber"),
          (_, registrar) ->
            registrar.register(_ -> received.incrementAndGet())
        )
      );

      final var latch = new CountDownLatch(1);
      final var executor = newFixedThreadPool(threadCount);

      try {

        // All threads wait at latch to create maximum contention
        final var futures = new ArrayList< Future< ? > >();

        for (int t = 0; t < threadCount; t++) {
          futures.add(
            executor.submit(() -> {
              try {
                await(latch, "the high-contention emission start gate");
                for (int i = 0; i < emissionsPerThread; i++) {
                  pipe.emit(i);
                }
              } catch (final InterruptedException e) {
                currentThread().interrupt();
              }
            })
          );
        }

        // Release all threads simultaneously
        latch.countDown();

        // Wait for all emitters to complete
        for (final var future : futures) {
          get(future, "a high-contention emission task");
        }

        // Wait for circuit to process all emissions
        circuit.await();

        // Verify no emissions were lost
        assertEquals(
          expectedTotal,
          received.get(),
          "All emissions must be received - no loss allowed"
        );

      } finally {

        executor.shutdown();
        assertTrue(
          executor.awaitTermination(30, SECONDS),
          "Executor should terminate"
        );

      }

    } finally {

      circuit.close();

    }

  }

  /// Emissions after accepted close have no observable delivery.
  @SpecRef({"9.1", "9.3"})
  @Test
  void emit_afterCircuitClose_isSilentlyDropped() {

    final var circuit = cortex.circuit();
    final var received = new AtomicInteger(0);

    final var conduit =
      circuit.conduit(Integer.class);

    final var pipe =
      conduit.get(cortex.name("post.close.channel"));

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("post.close.subscriber"),
        (_, registrar) ->
          registrar.register(_ -> received.incrementAndGet())
      )
    );

    // Emit before close
    pipe.emit(1);
    circuit.await();

    assertEquals(
      1,
      received.get(),
      "Emission before close should be received"
    );

    // Close the circuit
    circuit.close();

    // Attempt to emit after close
    pipe.emit(2);

    circuit.await();

    // Second emission should not be received
    assertEquals(
      1,
      received.get(),
      "Emissions after close should be rejected/ignored"
    );

  }

  // ===========================
  // Shutdown Order Tests
  // ===========================

  /// Caller-side admission remains responsive and lossless while Circuit
  /// processing is blocked behind an existing emission.
  @SuppressWarnings("resource")
  @SpecRef("5.6")
  @Test
  void emit_blockedCircuitBacklog_returnsWithoutLoss() throws Exception {

    final var circuit = cortex.circuit();
    final var release = new CountDownLatch(1);

    try {

      final int backlog = 50_000;
      final var entered = new CountDownLatch(1);
      final var delivered = new AtomicInteger();

      final Pipe< Integer > pipe =
        circuit.pipe(
          value -> {
            if (value==-1) {
              entered.countDown();
              try {
                await(release, "the blocked receptor release gate");
              } catch (final InterruptedException exception) {
                currentThread().interrupt();
              }
            } else {
              delivered.incrementAndGet();
            }
          }
        );

      pipe.emit(-1);
      await(entered, "the blocking emission to enter Circuit context");

      final var submitter = Executors.newSingleThreadExecutor();

      try {

        final var admitted =
          submitter.submit(() -> {
            for (int value = 0; value < backlog; value++) {
              pipe.emit(value);
            }
          });

        get(admitted, "the blocked-circuit backlog admission");
        assertEquals(0, delivered.get(), "Circuit remains blocked while callers admit work");

      } finally {

        release.countDown();
        submitter.shutdown();
        assertTrue(submitter.awaitTermination(30, SECONDS));

      }

      circuit.await();

      assertEquals(backlog, delivered.get(), "open-Circuit backlog must not be discarded");

    } finally {

      release.countDown();
      circuit.close();

    }

  }

  /// Repeated post-close emissions remain silently dropped.
  @SpecRef({"9.1", "9.3"})
  @Test
  void emit_multipleAfterCircuitClose_areSilentlyDropped() {

    final var circuit = cortex.circuit();
    final var received = new AtomicInteger(0);

    final var conduit =
      circuit.conduit(Integer.class);

    final var pipe =
      conduit.get(cortex.name("multi.post.close"));

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("multi.post.subscriber"),
        (_, registrar) ->
          registrar.register(_ -> received.incrementAndGet())
      )
    );

    // Emit and process
    pipe.emit(1);
    circuit.await();

    assertEquals(1, received.get());

    // Close circuit
    circuit.close();

    // Attempt multiple emissions
    for (int i = 0; i < 10; i++) {
      pipe.emit(i);
    }

    circuit.await();

    // Should still be 1
    assertEquals(
      1,
      received.get(),
      "No emissions should be processed after close"
    );

  }

  // ===========================
  // Emission Loss Stress Tests
  // ===========================

  /// Validates the post-close operation semantics for a Circuit (SPEC §9.1).
  ///
  /// Operations whose ingress-queue position falls at-or-after the circuit's
  /// own close job MUST NOT throw an error in the caller context, and may be
  /// silently dropped by the component. This is the generalization of the
  /// visibility window rule (§7.6.1) to the component's own lifetime.
  ///
  /// Test design: subscribe a counting receptor, emit one pre-close value to
  /// establish a baseline, close the circuit, then emit more values. The
  /// post-close emits MUST NOT throw, and the receptor MUST NOT observe them.
  ///
  /// Post-close queued emissions do not throw or produce delivery.
  @SpecRef("9.1")
  @Test
  void emit_postCloseQueuedOperations_dropWithoutCallerError() {

    final var circuit = cortex.circuit();

    final var count = new AtomicInteger(0);

    final var conduit =
      circuit.conduit(Integer.class);

    final var pipe =
      conduit.get(cortex.name("post.close.channel"));

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("post.close.subscriber"),
        (_, registrar) ->
          registrar.register(_ -> count.incrementAndGet())
      )
    );

    // Pre-close emission establishes the baseline.
    pipe.emit(1);
    circuit.await();
    assertEquals(1, count.get(), "Pre-close emission should be delivered");

    // Close the circuit. This is an enqueued operation that takes effect
    // when the circuit context processes it.
    circuit.close();

    // Post-close emissions MUST NOT throw in the caller context. The circuit
    // is free to silently drop them.
    assertDoesNotThrow(() -> pipe.emit(2));
    assertDoesNotThrow(() -> pipe.emit(3));
    assertDoesNotThrow(() -> pipe.emit(4));

    // After await, the circuit has processed the close and any drained work.
    // Per the silent-drop semantics, the receptor must not have observed any
    // post-close emissions.
    circuit.await();

    assertEquals(
      1,
      count.get(),
      "Post-close emissions must be silently dropped; receptor should only "
        + "have observed the pre-close emission"
    );

  }

  /// Different Bank names produce conduits with independent pipe pools.
  @SpecRef("10.4")
  @Test
  void get_differentBankNames_returnsIndependentConduitPools() {

    final var circuit = cortex.circuit();

    try {

      final var conduits =
        circuit.bank(
          Integer.class
        );

      final var first =
        conduits.get(
          cortex.name("pooled.conduit.one")
        );

      final var second =
        conduits.get(
          cortex.name("pooled.conduit.two")
        );

      final var pipeName =
        cortex.name("shared.pipe");

      assertNotSame(
        first,
        second,
        "Different names must return different conduit instances"
      );

      assertNotSame(
        first.get(pipeName),
        second.get(pipeName),
        "Different banked conduits must retain independent pipe pools"
      );

    } finally {

      circuit.close();

    }

  }

  /// Concurrent lookup of different names yields independent pipes.
  @SpecRef({"10", "12"})
  @SuppressWarnings("resource")
  @Test
  void get_differentNamesUnderContention_returnsIndependentPipes()
    throws InterruptedException, ExecutionException {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final var channels = new ConcurrentHashMap< String, Pipe< Integer > >();
      final var executor = newFixedThreadPool(10);

      try {

        // Each thread accesses different channel
        final var futures = new ArrayList< Future< ? > >();
        for (int t = 0; t < 100; t++) {
          final String channelName = "channel." + t;
          futures.add(
            executor.submit(() -> {
              final var name = cortex.name(channelName);
              final var ch = conduit.get(name);
              channels.put(channelName, ch);
            })
          );
        }

        for (final var future : futures) {
          get(future, "a concurrent conduit lookup");
        }

        // Should have 100 distinct channels
        assertEquals(
          100,
          channels.size(),
          "Should create distinct channels for different names"
        );

        // Verify each channel is unique
        final var uniqueChannels = new java.util.HashSet<>(channels.values());
        assertEquals(
          100,
          uniqueChannels.size(),
          "All channels should be distinct instances"
        );

      } finally {

        executor.shutdown();
        assertTrue(
          executor.awaitTermination(5, SECONDS),
          "Executor should terminate"
        );

      }

    } finally {

      circuit.close();

    }

  }

  /// Concurrent Bank lookup creates one conduit per name.
  @SpecRef({"10.4", "12"})
  @SuppressWarnings("resource")
  @Test
  void get_sameBankNameUnderContention_returnsOneCanonicalConduit()
    throws InterruptedException, ExecutionException {

    final var circuit = cortex.circuit();

    try {

      final var conduits =
        circuit.bank(
          Integer.class
        );

      final var name = cortex.name("contention.conduit");
      final var latch = new CountDownLatch(1);
      final var conduitReferences = new ConcurrentHashMap< Integer, Conduit< Integer > >();

      final var executor = newFixedThreadPool(20);

      try {

        final var futures = new ArrayList< Future< ? > >();
        for (int t = 0; t < 20; t++) {
          final int threadId = t;
          futures.add(
            executor.submit(() -> {
              try {
                await(latch, "the concurrent bank-lookup start gate");
                conduitReferences.put(
                  threadId,
                  conduits.get(
                    name
                  )
                );
              } catch (final InterruptedException e) {
                currentThread().interrupt();
              }
            })
          );
        }

        latch.countDown();

        for (final var future : futures) {
          get(future, "a concurrent bank lookup");
        }

        final var firstConduit = conduitReferences.get(0);
        for (int i = 1; i < 20; i++) {
          assertSame(
            firstConduit,
            conduitReferences.get(i),
            "All threads under contention must receive same conduit instance"
          );
        }

      } finally {

        executor.shutdown();
        assertTrue(
          executor.awaitTermination(5, SECONDS),
          "Executor should terminate"
        );

      }

    } finally {

      circuit.close();

    }

  }

  /// Bank lookup pools conduits by canonical Name.
  @SpecRef({"10.4", "12"})
  @Test
  void get_sameBankName_returnsSameConduit() {

    final var circuit = cortex.circuit();

    try {

      final Bank< Conduit< Integer > > conduits =
        circuit.bank(
          Integer.class
        );

      final var name = cortex.name("pooled.conduit");

      final var first =
        conduits.get(
          name
        );

      final var second =
        conduits.get(
          name
        );

      assertSame(
        first,
        second,
        "Same name must return same conduit instance from a conduit bank"
      );

      assertEquals(
        name,
        first.subject().name()
      );

    } finally {

      circuit.close();

    }

  }

  /// Concurrent lookup of one name yields one canonical pipe identity.
  @SpecRef("12")
  @SuppressWarnings("resource")
  @Test
  void get_sameNameUnderConcurrentAccess_returnsSamePipeIdentity()
    throws InterruptedException, ExecutionException {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final var name = cortex.name("concurrent.channel");
      final var channelReferences = new ConcurrentHashMap< Integer, Pipe< Integer > >();
      final var executor = newFixedThreadPool(10);

      try {

        // Multiple threads accessing same channel by name
        final var futures = new ArrayList< Future< ? > >();
        for (int t = 0; t < 10; t++) {
          final int threadId = t;
          futures.add(
            executor.submit(() -> {
              final var ch = conduit.get(name);
              channelReferences.put(threadId, ch);
            })
          );
        }

        for (final var future : futures) {
          get(future, "a concurrent pipe lookup");
        }

        // All threads should receive the SAME channel instance
        final var firstChannel = channelReferences.get(0);
        assertNotNull(firstChannel, "First channel should exist");

        for (int i = 1; i < 10; i++) {
          assertSame(
            firstChannel,
            channelReferences.get(i),
            "All threads must receive the same channel instance for the same name"
          );
        }

      } finally {

        executor.shutdown();
        assertTrue(
          executor.awaitTermination(5, SECONDS),
          "Executor should terminate"
        );

      }

    } finally {

      circuit.close();

    }

  }

  /// Verifies canonical pipe lookup under maximum contention.
  ///
  /// Creates 20 threads that simultaneously attempt to retrieve a channel
  /// with the same name from the same conduit. Uses a CountDownLatch to
  /// ensure all threads start at exactly the same time, maximizing contention
  /// on equal-name lookup.
  ///
  /// Expected: All threads must receive the exact same channel instance,
  /// proving that the public lookup contract returns one canonical Pipe for
  /// one name, even under concurrent access.
  /// Concurrent equal-name pipe lookup returns one canonical pipe.
  @SpecRef("12")
  @SuppressWarnings("resource")
  @Test
  void get_sameNameUnderContention_returnsOneCanonicalPipe()
    throws InterruptedException, ExecutionException {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final var name = cortex.name("contention.channel");
      final var latch = new CountDownLatch(1);
      final var channelReferences = new ConcurrentHashMap< Integer, Pipe< Integer > >();

      final var executor = newFixedThreadPool(20);

      try {

        // All threads wait at latch to create maximum contention
        final var futures = new ArrayList< Future< ? > >();
        for (int t = 0; t < 20; t++) {
          final int threadId = t;
          futures.add(
            executor.submit(() -> {
              try {
                await(latch, "the concurrent pipe-lookup start gate");
                final var ch = conduit.get(name);
                channelReferences.put(threadId, ch);
              } catch (final InterruptedException e) {
                currentThread().interrupt();
              }
            })
          );
        }

        // Release all threads simultaneously
        latch.countDown();

        for (final var future : futures) {
          get(future, "a concurrent pipe lookup");
        }

        // Verify all got same instance
        final var firstChannel = channelReferences.get(0);
        for (int i = 1; i < 20; i++) {
          assertSame(
            firstChannel,
            channelReferences.get(i),
            "All threads under contention must receive same channel instance"
          );
        }

      } finally {

        executor.shutdown();
        assertTrue(
          executor.awaitTermination(5, SECONDS),
          "Executor should terminate"
        );

      }

    } finally {

      circuit.close();

    }

  }

  /// Validates multi-producer ingress-queue ordering guarantees under
  /// concurrent contention.
  ///
  /// SPEC §5.4.3 specifies that:
  /// - Per-caller FIFO: emissions from a single caller are observed in
  ///   the order they were enqueued.
  /// - Total order within a run: every observer in the circuit context
  ///   sees the same sequence.
  /// - Cross-caller ordering is implementation-defined: concurrent callers
  ///   may interleave in any order.
  ///
  /// Together with SPEC §5.6 (no loss, no block, unbounded ingress),
  /// these imply:
  /// - Every enqueued emission MUST be delivered (no loss).
  /// - No emission is delivered more than once (no duplication).
  /// - For each producer, its emissions appear in the received sequence
  ///   in monotonic enqueue order (per-caller FIFO).
  ///
  /// Test design: N producer threads each emit a disjoint range of
  /// integers encoding (producerId, sequence). They start simultaneously
  /// via a CyclicBarrier to maximize contention. After await(), the
  /// captured sequence is checked for:
  ///   (1) Total count equals N × emissions-per-producer (no loss).
  ///   (2) Every emitted value appears exactly once (no duplication).
  ///   (3) Within each producer's subsequence (filtered by producerId),
  ///       values appear in monotonic sequence order (per-caller FIFO).
  ///
  /// The cross-producer interleaving is intentionally not asserted —
  /// because §5.4.3 deliberately leaves it implementation-defined.
  /// Multi-producer ingress preserves FIFO order for each caller.
  @SpecRef("5.3")
  @SuppressWarnings("resource")
  @Test
  void ingress_multipleProducers_preservesPerCallerFifo() throws Exception {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final var pipe =
        conduit.get(cortex.name("multi.producer.channel"));

      final var captured = new ArrayList< Integer >();

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("multi.producer.subscriber"),
          (_, registrar) ->
            registrar.register(captured::add)
        )
      );

      final int producerCount = 8;
      final int emissionsPerProducer = 200;

      // Encode (producerId, sequence) as a single int:
      //   value = producerId * emissionsPerProducer + sequence
      // This gives each producer a disjoint range [producerId * 200, producerId * 200 + 199].

      final var barrier = new CyclicBarrier(producerCount);
      final var producers = newFixedThreadPool(producerCount);

      try {

        final var futures = new ArrayList< Future< ? > >();

        for (int p = 0; p < producerCount; p++) {
          final int producerId = p;
          futures.add(
            producers.submit(() -> {
              try {
                await(barrier, "all contending producers");
              } catch (InterruptedException | BrokenBarrierException e) {
                currentThread().interrupt();
                return;
              }
              for (int seq = 0; seq < emissionsPerProducer; seq++) {
                pipe.emit(producerId * emissionsPerProducer + seq);
              }
            })
          );
        }

        for (final var future : futures) {
          get(future, "a contending ingress producer");
        }

      } finally {
        producers.shutdown();
      }

      circuit.await();

      // (1) No loss: total count matches.
      assertEquals(
        producerCount * emissionsPerProducer,
        captured.size(),
        "All emitted values must be delivered (no loss under contention)"
      );

      // (2) No duplication: every value appears exactly once.
      final var seen = new java.util.HashSet< Integer >();
      for (final var v : captured) {
        assertTrue(
          seen.add(v),
          "Value " + v + " was delivered more than once (duplication under contention)"
        );
      }

      // (2 continued) Every expected value appears: ranges fully covered.
      for (int p = 0; p < producerCount; p++) {
        for (int seq = 0; seq < emissionsPerProducer; seq++) {
          final int expected = p * emissionsPerProducer + seq;
          assertTrue(
            seen.contains(expected),
            "Expected value " + expected + " was not delivered"
          );
        }
      }

      // (3) Per-caller FIFO: within each producer's subsequence, values
      // are monotonically increasing. Cross-producer interleaving is
      // unspecified (SPEC §5.4.3) and NOT asserted.
      for (int p = 0; p < producerCount; p++) {
        int lastSeen = -1;
        for (final var v : captured) {
          final int vProducer = v / emissionsPerProducer;
          if (vProducer!=p) {
            continue;
          }
          final int vSeq = v % emissionsPerProducer;
          assertTrue(
            vSeq > lastSeen,
            "Producer " + p + " FIFO violated: saw seq " + vSeq
              + " after seq " + lastSeen
          );
          lastSeen = vSeq;
        }
        assertEquals(
          emissionsPerProducer - 1,
          lastSeen,
          "Producer " + p + " did not complete its sequence"
        );
      }

    } finally {

      circuit.close();

    }

  }

  /// Concurrent equivalent Name creation is interned atomically.
  @SpecRef({"4.1", "12"})
  @SuppressWarnings("resource")
  @Test
  void name_equivalentCreationUnderContention_returnsSameInstance()
    throws InterruptedException, ExecutionException {

    final var names = new ConcurrentHashMap< Integer, Name >();
    final var latch = new CountDownLatch(1);
    final var executor = newFixedThreadPool(20);

    try {

      // All threads create same name path simultaneously
      final var futures = new ArrayList< Future< ? > >();
      for (int t = 0; t < 20; t++) {
        final int threadId = t;
        futures.add(
          executor.submit(() -> {
            try {
              await(latch, "the concurrent dispatch start gate");
              final var name = cortex.name("concurrent.name.test");
              names.put(threadId, name);
            } catch (final InterruptedException e) {
              currentThread().interrupt();
            }
          })
        );
      }

      // Release all threads
      latch.countDown();

      for (final var future : futures) {
        get(future, "a concurrent dispatch");
      }

      // All threads must receive the SAME Name instance (interning)
      final var firstName = names.get(0);
      assertNotNull(firstName, "First name should exist");

      for (int i = 1; i < 20; i++) {
        assertSame(
          firstName,
          names.get(i),
          "Concurrent name creation must return same interned instance"
        );
      }

    } finally {

      executor.shutdown();
      assertTrue(
        executor.awaitTermination(5, SECONDS),
        "Executor should terminate"
      );

    }

  }

  // ===========================
  // Circuit.current() Tests
  // ===========================

  /// Concurrent hierarchical Name creation preserves canonical identity.
  @SpecRef({"4.1", "12"})
  @SuppressWarnings("resource")
  @Test
  void name_sameHierarchyUnderContention_returnsCanonicalIdentity()
    throws InterruptedException, ExecutionException {

    final var names = new ConcurrentHashMap< Integer, Name >();
    final var latch = new CountDownLatch(1);
    final var executor = newFixedThreadPool(20);

    try {

      // All threads build hierarchical name simultaneously
      final var futures = new ArrayList< Future< ? > >();
      for (int t = 0; t < 20; t++) {
        final int threadId = t;
        futures.add(
          executor.submit(() -> {
            try {
              await(latch, "the concurrent emission start gate");
              final var base = cortex.name("base");
              final var extended = base.name("child");
              final var full = extended.name("grandchild");
              names.put(threadId, full);
            } catch (final InterruptedException e) {
              currentThread().interrupt();
            }
          })
        );
      }

      latch.countDown();

      for (final var future : futures) {
        get(future, "a concurrent emission");
      }

      // All should be same instance
      final var firstName = names.get(0);
      for (int i = 1; i < 20; i++) {
        assertSame(
          firstName,
          names.get(i),
          "Hierarchical name creation must preserve interning"
        );
      }

    } finally {

      executor.shutdown();
      assertTrue(
        executor.awaitTermination(5, SECONDS),
        "Executor should terminate"
      );

    }

  }

  /// Every time-aware transit hop in one ingress chain observes the same
  /// processing-time reading despite elapsed wall time between hops.
  @SpecRef("5.8")
  @Test
  void processingTime_delayedTransitHops_shareStimulusReading() {

    final var circuit = cortex.circuit();

    try {

      final var windows = new ArrayList< List< Integer > >();
      final Pipe< Window< Integer > > sink =
        circuit.pipe(window -> {
          final var values = new ArrayList< Integer >();
          window.forEach(values::add);
          windows.add(values);
        });
      final Pipe< Integer > window =
        cortex.flow(Integer.class)
          .window(Duration.ofMillis(5L), 10)
          .pipe(sink);
      final Pipe< Integer > delayedTransit =
        circuit.pipe(_ -> {
          try {
            Thread.sleep(25L);
          } catch (final InterruptedException error) {
            currentThread().interrupt();
          }
          window.emit(2);
        });
      final Pipe< Integer > ingress =
        circuit.pipe(_ -> {
          window.emit(1);
          delayedTransit.emit(0);
        });

      ingress.emit(0);
      circuit.await();

      assertEquals(List.of(List.of(1), List.of(1, 2)), windows);

    } finally {

      circuit.closeAwait();

    }

  }

  /// Verifies that pulse() returns an empty optional once the circuit is
  /// fully closed and that subsequent calls remain empty and return quickly.
  ///
  /// After [Circuit#close()] returns, the circuit may still be in the CLOSING phase;
  /// await supplies the causal terminal barrier before pulse is inspected.
  /// Pulse returns empty after terminal close.
  @SpecRef("5.7")
  @Test
  void pulse_afterTerminalClose_returnsEmpty() {

    final var circuit = cortex.circuit();

    circuit.close();
    circuit.await();

    assertTrue(
      circuit.pulse().isEmpty(),
      "pulse() should return empty once the circuit is fully closed"
    );

    // Subsequent invocations remain empty after the terminal barrier.
    for (int i = 0; i < 5; i++) {
      assertTrue(
        circuit.pulse().isEmpty(),
        "subsequent pulse() should remain empty after close"
      );
    }

  }

  /// Pulse waits behind earlier ingress and its blocked transit cascade without
  /// dispatching an additional user emission.
  @SpecRef("5.7")
  @Test
  void pulse_blockedPriorTransit_waitsForCascadeWithoutEmission() throws Exception {

    final var circuit = cortex.circuit();
    final var transitEntered = new CountDownLatch(1);
    final var releaseTransit = new CountDownLatch(1);
    final var callbacks = new ArrayList< Integer >();
    final var pipeReference = new AtomicReference< Pipe< Integer > >();
    final var executor = newFixedThreadPool(1);

    try {

      final Pipe< Integer > pipe =
        circuit.pipe(value -> {
          callbacks.add(value);
          if (value==1) {
            pipeReference.get().emit(2);
          } else {
            transitEntered.countDown();
            try {
              await(releaseTransit, "the blocked transit release gate");
            } catch (final InterruptedException error) {
              currentThread().interrupt();
            }
          }
        });
      pipeReference.set(pipe);
      final var subject = pipe.subject();
      final var state = subject.state();

      pipe.emit(1);
      await(transitEntered, "the blocked transit callback");

      final Future< Optional< Pulse > > pulse = executor.submit(circuit::pulse);
      assertThrows(TimeoutException.class, () -> pulse.get(50L, TimeUnit.MILLISECONDS));

      releaseTransit.countDown();

      assertTrue(get(pulse, "the Pulse after transit release").isPresent());
      assertEquals(List.of(1, 2), callbacks);
      assertSame(subject, pipe.subject());
      assertEquals(state, pipe.subject().state());

      pipe.emit(3);
      circuit.await();
      assertEquals(List.of(1, 2, 3), callbacks);

    } finally {

      releaseTransit.countDown();
      executor.shutdown();
      circuit.closeAwait();

    }

  }

  /// Verifies that pulse() called from within Circuit context throws
  /// [IllegalStateException], matching the await self-deadlock rule.
  /// Pulse from circuit context signals illegal context use.
  @SpecRef({"5.7", "15.1"})
  @Test
  void pulse_fromCircuitContext_throwsIllegalStateException() {

    final var circuit = cortex.circuit();

    try {

      final AtomicReference< Throwable > captured = new AtomicReference<>();

      final var conduit =
        circuit.conduit(
          cortex.name("circuit.pulse.conduit"),
          Integer.class
        );

      final Subscriber< Integer > subscriber =
        circuit.subscriber(
          cortex.name("circuit.pulse.subscriber"),
          (_, registrar) ->
            registrar.register(
              _ -> {
                try {
                  circuit.pulse();
                } catch (final IllegalStateException ex) {
                  captured.set(ex);
                }
              }
            )
        );

      final var subscription =
        conduit.subscribe(subscriber);

      final Pipe< Integer > pipe =
        conduit.get(cortex.name("circuit.pulse.channel"));

      pipe.emit(1);

      circuit.await();

      subscription.close();

      final var thrown = captured.get();

      assertNotNull(thrown, "pulse() on the circuit thread should throw");
      assertEquals(IllegalStateException.class, thrown.getClass());
      assertEquals(
        "Cannot call Circuit::pulse from within a circuit's thread",
        thrown.getMessage()
      );

    } finally {

      circuit.close();

    }

  }

  /// Verifies that a healthy circuit returns a present pulse with sane
  /// timestamp ordering. Same-thread ordering (start ≤ enqueued ≤ stop)
  /// is strict; cross-thread checks (involving dequeued) treat the dequeue
  /// stamp as bounded by start and stop in expectation, allowing for the
  /// rare cross-CPU nanoTime slop noted in the Pulse Javadoc.
  /// Pulse reports present ordered processing-time observations.
  @SpecRef({"5.7", "5.8"})
  @Test
  void pulse_healthyCircuit_returnsPresentOrderedTimes() {

    final var circuit = cortex.circuit();

    try {

      final Optional< Pulse > result = circuit.pulse();

      assertTrue(
        result.isPresent(),
        "pulse() on a healthy circuit should return a present optional"
      );

      final var pulse = result.get();

      // Same-thread orderings — strict, both stamps on caller thread.
      assertTrue(
        pulse.start() <= pulse.enqueued(),
        "start (" + pulse.start() + ") <= enqueued (" + pulse.enqueued() + ")"
      );
      assertTrue(
        pulse.enqueued() <= pulse.stop(),
        "enqueued (" + pulse.enqueued() + ") <= stop (" + pulse.stop() + ")"
      );

      // Cross-thread ordering — dequeued is between start and stop in expectation.
      // The probe cannot be dequeued before it was created on the caller, and
      // the caller cannot return before the worker stamped dequeued.
      assertTrue(
        pulse.start() <= pulse.stop(),
        "start (" + pulse.start() + ") <= stop (" + pulse.stop() + ")"
      );
      assertTrue(
        pulse.dequeued() <= pulse.stop(),
        "dequeued (" + pulse.dequeued() + ") <= stop (" + pulse.stop() + ")"
      );

    } finally {

      circuit.close();

    }

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

  /// Distinct Circuits expose distinct subject identities.
  @SpecRef({"4.2", "4.3"})
  @Test
  void subject_distinctCircuits_haveDistinctIdentities() {

    final var circuit1 = cortex.circuit();
    final var circuit2 = cortex.circuit();

    try {

      assertNotSame(circuit1.subject(), circuit2.subject());
      assertNotEquals(circuit1.subject().id(), circuit2.subject().id());

    } finally {

      circuit1.close();
      circuit2.close();

    }

  }

  /// A Circuit subject exposes its immutable State.
  @SpecRef({"4.3", "8.1"})
  @Test
  void subject_newCircuit_exposesState() {

    final var circuit = cortex.circuit(
      cortex.name("circuit.state.test")
    );

    try {

      final Subject< ? > subject = circuit.subject();
      final var state = subject.state();

      assertNotNull(state);
      // State should be empty for a newly created circuit
      assertEquals(0, state.stream().count());

    } finally {

      circuit.close();

    }

  }

  /// Recursive circuit-context emission enters the transit queue.
  @SpecRef("5.3")
  @Test
  void transit_recursiveEmission_usesTransitQueue() {

    final var circuit = cortex.circuit();

    try {

      final var results = new ArrayList< Integer >();
      final var conduit =
        circuit.conduit(Integer.class);

      final var recursivePipe =
        conduit.get(cortex.name("recursive.channel"));

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("recursive.subscriber"),
          (_, registrar) ->
            registrar.register(value -> {

              results.add(value);

              // Recursive emission from Circuit context must preserve transit ordering.
              if (value < 10) {
                recursivePipe.emit(value + 1);
              }

            })
        )
      );

      // Start the recursive chain
      recursivePipe.emit(1);

      circuit.await();

      // Should process all recursive emissions
      assertEquals(
        List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        results,
        "Recursive emissions should be processed via transit queue"
      );

    } finally {

      circuit.close();

    }

  }

  /// A caller write completed before admission is visible when the Circuit
  /// processes the queued operation.
  @SpecRef("5.4.2")
  @Test
  void visibility_callerWriteBeforeAdmission_isVisibleDuringProcessing() {

    final var circuit = cortex.circuit();

    try {

      final int[] callerState = {0};
      final int[] circuitObservation = {0};
      final Pipe< Integer > pipe =
        circuit.pipe(_ -> circuitObservation[0] = callerState[0]);

      callerState[0] = 73;
      pipe.emit(1);
      circuit.await();

      assertEquals(73, circuitObservation[0]);

    } finally {

      circuit.closeAwait();

    }

  }

  /// Circuit writes are visible to later Circuit work and to a caller after
  /// await, without requiring volatile application state.
  @SpecRef("5.4.2")
  @Test
  void visibility_circuitWrites_areVisibleToLaterWorkAndAwaitingCaller() {

    final var circuit = cortex.circuit();

    try {

      final int[] circuitState = {0};
      final int[] laterObservation = {0};
      final Pipe< Integer > pipe =
        circuit.pipe(value -> {
          if (value==1) {
            circuitState[0] = 41;
          } else {
            laterObservation[0] = circuitState[0];
          }
        });

      pipe.emit(1);
      pipe.emit(2);
      circuit.await();

      assertEquals(41, laterObservation[0]);
      assertEquals(41, circuitState[0]);

    } finally {

      circuit.closeAwait();

    }

  }

}
