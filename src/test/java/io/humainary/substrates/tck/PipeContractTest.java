// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.lang.Thread.*;
import static java.util.concurrent.Executors.*;
import static java.util.concurrent.TimeUnit.*;
import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §§4.3, 5.3, 6.1–6.3, and 15.2 Pipe identity, admission, ordering,
/// context, composition, non-recursive dispatch, validation, and failure isolation.
/// @author William David Louth
/// @since 1.0

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.0/SPEC.md")
final class PipeContractTest
  extends TestSupport {

  private Cortex cortex;

  /// Verifies that multi-node feedback cycles work correctly with proper ordering.
  ///
  /// Creates a 3-pipe cycle: A → B → C → A, where each pipe forwards to the
  /// next in sequence. Pipe C closes the loop by emitting back to A, with an
  /// iteration counter to prevent infinite loops.
  ///
  /// Topology:
  /// ```
  ///   ┌─────┐
  ///   │  A  │──→ B ──→ C
  ///   └──▲──┘           │
  ///      └──────────────┘
  /// ```
  ///
  /// Execution flow demonstrates transit queue priority:
  /// 1. External emit to A (ingress queue)
  /// 2. A emits to B (transit queue - takes priority)
  /// 3. B emits to C (transit queue - cascading)
  /// 4. C emits back to A (transit queue - completes cycle)
  /// 5. Process repeats until iteration limit
  ///
  /// The trace shows execution order: A→B→C→A→B→C→... proving that the entire
  /// cycle completes before any new external emissions would be processed.
  /// This transit queue priority ensures that feedback loops run to completion
  /// atomically from an external observer's perspective.
  ///
  /// Critical for neural networks:
  /// - Enables recurrent connections across multiple nodes
  /// - Ensures signal propagates through entire cycle before new inputs
  /// - Provides deterministic execution in cyclic topologies
  /// - No deadlock despite circular dependencies
  ///
  /// Expected: 5 iterations × 3 nodes = 15 trace entries in strict A→B→C order
  /// A bounded multi-node cycle uses queued non-recursive transit.
  @SpecRef({"5.3", "6.1"})
  @Test
  @SuppressWarnings("unchecked")
  void dispatch_boundedMultiNodeCycle_usesQueuedTransit() {

    final var circuit = cortex.circuit();

    try {

      final List< String > trace = new ArrayList<>();
      final int maxIterations = 5;
      final AtomicInteger iterations = new AtomicInteger(0);

      // Create A -> B -> C -> A cycle

      final Pipe< String >[] pipes = new Pipe[3];

      pipes[0] = circuit.pipe(
        value -> {
          trace.add("A:" + value);
          pipes[1].emit(value);
        }
      );

      pipes[1] = circuit.pipe(
        value -> {
          trace.add("B:" + value);
          pipes[2].emit(value);
        }
      );

      pipes[2] = circuit.pipe(
        value -> {
          trace.add("C:" + value);
          if (iterations.incrementAndGet() < maxIterations) {
            pipes[0].emit(value);
          }
        }
      );

      pipes[0].emit("start");
      circuit.await();

      assertEquals(maxIterations * 3, trace.size());
      assertTrue(trace.get(0).startsWith("A:"));
      assertTrue(trace.get(1).startsWith("B:"));
      assertTrue(trace.get(2).startsWith("C:"));

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Basic Creation and Emission
  // ===========================

  /// Verifies that cyclic pipe connections enable feedback loops without deadlock.
  ///
  /// Creates a pipe that emits back to itself, forming a feedback loop. Each
  /// emission increments the value and re-emits until reaching a threshold (10).
  /// This tests the circuit's ability to handle self-referential connections.
  ///
  /// This is critical for neural-like network topologies that require recurrent
  /// connections and feedback dynamics. The async nature of circuit.pipe() ensures
  /// that feedback emissions are queued rather than recursively invoked, preventing
  /// both stack overflow and deadlock.
  ///
  /// Expected behavior follows the transit queue priority model: cascading emissions
  /// (from the circuit thread itself) are processed before new ingress emissions.
  /// This ensures the feedback loop completes (1→2→3...→10) before any external
  /// emissions would be processed.
  /// A bounded cyclic Pipe topology uses queued non-recursive dispatch.
  @SpecRef({"5.3", "6.1"})
  @SuppressWarnings("unchecked")
  @Test
  void dispatch_boundedPipeCycle_usesQueuedNonRecursiveTransit() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > emissions = new ArrayList<>();
      final int maxCount = 10;

      // Create a cyclic pipe that feeds back to itself
      // but terminates after maxCount iterations

      final Pipe< Integer >[] cycle = new Pipe[1];

      cycle[0] = circuit.pipe(
        value -> {
          emissions.add(value);
          if (value < maxCount) {
            cycle[0].emit(value + 1);
          }
        }
      );

      cycle[0].emit(1);
      circuit.await();

      assertEquals(
        List.of(1, 2, 3, 4, 5, 6, 7, 8, 9, 10),
        emissions
      );

    } finally {

      circuit.close();

    }

  }

  /// A Circuit Pipe composes delivery into a Conduit Pipe.
  @SpecRef({"6.1", "10.1"})
  @Test
  void dispatch_circuitPipeToConduitPipe_deliversEmission() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final List< Integer > emissions = new ArrayList<>();

      final Pipe< Integer > async =
        circuit.pipe(emissions::add);

      final Subscriber< Integer > subscriber =
        circuit.subscriber(
          cortex.name("pipe.test.subscriber"),
          (_, registrar) -> registrar.register(async)
        );

      conduit.subscribe(subscriber);

      final Pipe< Integer > pipe =
        conduit.get(cortex.name("pipe.test.channel"));

      pipe.emit(10);
      pipe.emit(20);
      pipe.emit(30);

      circuit.await();

      assertEquals(List.of(10, 20, 30), emissions);

    } finally {

      circuit.close();

    }

  }

  /// A Circuit Pipe target executes in Circuit context.
  @SpecRef({"5.1", "6.1"})
  @Test
  void dispatch_circuitPipe_executesInOwningCircuitContext() {

    final var circuit = cortex.circuit();

    try {

      final List< Thread > threads = new ArrayList<>();
      final var callerThread = currentThread();

      final Pipe< Integer > target =
        circuit.pipe(_ -> threads.add(currentThread()));

      final Pipe< Integer > async = circuit.pipe(target);

      async.emit(1);
      circuit.await();

      assertEquals(1, threads.size());
      assertNotSame(callerThread, threads.getFirst());

    } finally {

      circuit.close();

    }

  }

  /// Verifies that when pipes from different circuits are chained,
  /// each pipe's receptor executes on its own circuit's thread.
  ///
  /// Creates two circuits with their own pipes. Circuit1's pipe
  /// forwards emissions to circuit2's pipe. This tests that the
  /// async boundary is correctly maintained: circuit1's receptor
  /// runs on circuit1's thread, and circuit2's receptor runs on
  /// circuit2's thread.
  ///
  /// Flow:
  /// 1. The caller submits through pipe1.
  /// 2. Circuit1 executes pipe1's receptor, which emits through pipe2.
  /// 3. Circuit2 executes pipe2's receptor.
  ///
  /// This is critical for neural-like networks where circuits represent
  /// independent processing nodes with isolated execution contexts.
  /// Each stage in a cross-Circuit Pipe chain executes in its owning context.
  @SpecRef("6.3")
  @Test
  void dispatch_crossCircuitChain_usesEachOwningContext() {

    final var circuit1 = cortex.circuit();
    final var circuit2 = cortex.circuit();

    try {

      final var callerThread = currentThread();
      final List< Thread > circuit1Threads = new ArrayList<>();
      final List< Thread > circuit2Threads = new ArrayList<>();

      // Circuit2's pipe captures its executing thread
      final Pipe< Integer > pipe2 =
        circuit2.pipe(_ -> circuit2Threads.add(currentThread()));

      // Circuit1's pipe forwards to pipe2, capturing its own thread
      final Pipe< Integer > pipe1 =
        circuit1.pipe(value -> {
          circuit1Threads.add(currentThread());
          pipe2.emit(value);
        });

      // Emit from caller thread
      pipe1.emit(1);

      // Wait for both circuits to complete
      circuit1.await();
      circuit2.await();

      // Verify circuit1's receptor ran on circuit1's thread (not caller)
      assertEquals(1, circuit1Threads.size());
      assertNotSame(callerThread, circuit1Threads.getFirst());

      // Verify circuit2's receptor ran on circuit2's thread
      assertEquals(1, circuit2Threads.size());
      assertNotSame(callerThread, circuit2Threads.getFirst());

      // Verify the two circuits use DIFFERENT threads
      assertNotSame(circuit1Threads.getFirst(), circuit2Threads.getFirst());

    } finally {

      circuit1.close();
      circuit2.close();

    }

  }

  // ===========================
  // Deep Chain Stack Safety
  // ===========================

  /// Verifies that multiple emissions through cross-circuit pipe chains
  /// maintain correct thread affinity for each circuit.
  ///
  /// Extends the basic cross-circuit test to verify that thread affinity
  /// is consistent across multiple emissions, not just a single one.
  /// Cross-Circuit context affinity persists across multiple emissions.
  @SpecRef("6.3")
  @Test
  void dispatch_crossCircuitMultipleEmissions_preservesContextAffinity() {

    final var circuit1 = cortex.circuit();
    final var circuit2 = cortex.circuit();

    try {

      final List< Thread > circuit1Threads = new ArrayList<>();
      final List< Thread > circuit2Threads = new ArrayList<>();
      final int emissionCount = 10;

      // Circuit2's pipe captures its executing thread
      final Pipe< Integer > pipe2 =
        circuit2.pipe(_ -> circuit2Threads.add(currentThread()));

      // Circuit1's pipe forwards to pipe2, capturing its own thread
      final Pipe< Integer > pipe1 =
        circuit1.pipe(value -> {
          circuit1Threads.add(currentThread());
          pipe2.emit(value);
        });

      // Emit multiple values
      for (int i = 0; i < emissionCount; i++) {
        pipe1.emit(i);
      }

      // Wait for both circuits to complete
      circuit1.await();
      circuit2.await();

      // Verify all emissions processed by both circuits
      assertEquals(emissionCount, circuit1Threads.size());
      assertEquals(emissionCount, circuit2Threads.size());

      // Verify each circuit uses a single consistent thread
      final Thread circuit1Thread = circuit1Threads.getFirst();
      final Thread circuit2Thread = circuit2Threads.getFirst();

      for (final Thread t : circuit1Threads) {
        assertSame(circuit1Thread, t);
      }

      for (final Thread t : circuit2Threads) {
        assertSame(circuit2Thread, t);
      }

      // Verify the two circuits use DIFFERENT threads
      assertNotSame(circuit1Thread, circuit2Thread);

    } finally {

      circuit1.close();
      circuit2.close();

    }

  }

  /// Verifies that async pipes enable arbitrarily deep chains without stack overflow.
  ///
  /// Builds a chain of 1000 async pipes, each forwarding to the next, ending
  /// with a counter. If pipes used recursive invocation (synchronous emit),
  /// this would overflow the call stack. Instead, async pipes enqueue emissions
  /// to the circuit's queue, making deep chains stack-safe.
  ///
  /// This is critical for neural-like network topologies where signals may
  /// propagate through many layers. The queue-based model prevents stack
  /// overflow regardless of chain depth.
  ///
  /// Expected: A single emission at the head reaches the tail through all
  /// 1000 intermediate pipes without any stack overflow.
  /// Deep Pipe chains dispatch without recursive caller-stack growth.
  @SpecRef("6.1")
  @Test
  void dispatch_deepPipeChain_deliversWithoutCallerRecursion() {

    final var circuit = cortex.circuit();

    try {

      final AtomicInteger counter = new AtomicInteger(0);

      // Build a deep chain: pipe0 -> pipe1 -> ... -> pipe999 -> counter
      // Without async, this would risk stack overflow

      Pipe< Integer > tail =
        circuit.pipe(_ -> counter.incrementAndGet());

      for (int i = 0; i < 1000; i++) {
        final Pipe< Integer > next = tail;
        tail = circuit.pipe(next);
      }

      final Pipe< Integer > head = tail;

      head.emit(42);
      circuit.await();

      assertEquals(1, counter.get());

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Cyclic Pipe Connections
  // ===========================

  /// A deep transformed Pipe chain preserves composition and delivery.
  @SpecRef("6.2.6")
  @Test
  void dispatch_deepTransformedChain_deliversFinalValue() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > result = new ArrayList<>();

      // Build chain with transformations
      Pipe< Integer > tail = circuit.pipe(result::add);

      for (int i = 0; i < 100; i++) {
        final Pipe< Integer > next = tail;
        final int increment = i;

        tail = circuit.pipe(
          value -> next.emit(value + increment)
        );
      }

      final Pipe< Integer > head = tail;

      head.emit(0);
      circuit.await();

      // Sum of 0..99 = 4950
      assertEquals(List.of(4950), result);

    } finally {

      circuit.close();

    }

  }

  /// A diff Fiber materialized to a Pipe suppresses duplicates.
  @SpecRef("6.2.3")
  @Test
  void dispatch_diffFiber_suppressesDuplicates() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > results = new ArrayList<>();
      final Pipe< Integer > target = circuit.pipe(results::add);

      final Pipe< Integer > diffed =
        cortex.fiber(Integer.class)
          .diff()
          .pipe(circuit.pipe(target));

      diffed.emit(1);
      diffed.emit(1);
      diffed.emit(2);
      diffed.emit(2);
      diffed.emit(3);

      circuit.await();

      assertEquals(List.of(1, 2, 3), results);

    } finally {

      circuit.close();

    }

  }

  /// Verifies emissions to the empty pipe go through the queue — a later
  /// emission on an observable pipe drains only after await(), proving the
  /// sink's emissions are queued ahead of it rather than short-circuited.
  /// Empty-Pipe emissions retain their position in Circuit ordering.
  @SpecRef("5.3")
  @Test
  void dispatch_emptyPipeBetweenEmissions_preservesQueueOrder() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > observed = new ArrayList<>();
      final Pipe< Integer > sink = circuit.pipe();
      final Pipe< Integer > observable = circuit.pipe(observed::add);

      for (int i = 0; i < 10; i++) {
        sink.emit(i);
      }
      observable.emit(-1);

      circuit.await();

      assertEquals(List.of(-1), observed);

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Integration Tests
  // ===========================

  /// An every Fiber materialized to a Pipe samples deterministically.
  @SpecRef("6.2.3")
  @Test
  void dispatch_everyFiber_emitsSelectedOrdinals() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > results = new ArrayList<>();
      final Pipe< Integer > target = circuit.pipe(results::add);

      final Pipe< Integer > sampled =
        cortex.fiber(Integer.class)
          .every(3)
          .pipe(circuit.pipe(target));

      for (int i = 0; i < 10; i++) {
        sampled.emit(i);
      }

      circuit.await();

      // Every 3rd: indices 2, 5, 8 (0-based, skips first 2)
      assertEquals(List.of(2, 5, 8), results);

    } finally {

      circuit.close();

    }

  }

  /// A guard Fiber materialized to a Pipe filters rejected values.
  @SpecRef("6.2.2")
  @Test
  void dispatch_guardFiber_filtersRejectedValues() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > results = new ArrayList<>();
      final Pipe< Integer > target = circuit.pipe(results::add);

      final Pipe< Integer > filtered =
        cortex.fiber(Integer.class)
          .guard(x -> x > 0)
          .pipe(circuit.pipe(target));

      filtered.emit(-1);
      filtered.emit(0);
      filtered.emit(1);
      filtered.emit(5);

      circuit.await();

      assertEquals(List.of(1, 5), results);

    } finally {

      circuit.close();

    }

  }

  /// A limit Fiber materialized to a Pipe bounds delivery count.
  @SpecRef("6.2.3")
  @Test
  void dispatch_limitFiber_boundsDeliveredEmissions() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > results = new ArrayList<>();
      final Pipe< Integer > target = circuit.pipe(results::add);

      final Pipe< Integer > limited =
        cortex.fiber(Integer.class)
          .limit(3)
          .pipe(circuit.pipe(target));

      for (int i = 0; i < 10; i++) {
        limited.emit(i);
      }

      circuit.await();

      assertEquals(List.of(0, 1, 2), results);

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Null Guards
  // ===========================

  /// Validates that async pipes preserve FIFO ordering across the async boundary.
  ///
  /// Emits 100 sequential values (0..99) to an async pipe from a single thread,
  /// then verifies they arrive at the target in the exact same order. This tests
  /// that the circuit's queueing mechanism maintains order when crossing the
  /// thread boundary from caller to circuit worker.
  ///
  /// Flow:
  /// 1. Caller thread: emit(0), emit(1), ..., emit(99) → ingress queue
  /// 2. Circuit worker: process queue in FIFO order
  /// 3. Target receives: 0, 1, 2, ..., 99 (same order)
  ///
  /// While this test uses a single emitter thread, the ordering guarantee extends
  /// to the ingress queue: emissions enqueued first are processed first, regardless
  /// of which thread enqueued them. For concurrent emitters, the order depends on
  /// which thread's emit() completes first (arrival order at the queue).
  ///
  /// This FIFO guarantee is fundamental to:
  /// - Causal consistency (if A happens-before B in caller, A processed before B)
  /// - Predictable behavior in single-threaded emission scenarios
  /// - Testability (reproducible execution order)
  ///
  /// Note: Different from transit queue (see testCyclicPipeConnection) which has
  /// priority over ingress for cascading emissions.
  ///
  /// Expected: All 100 values arrive in order 0, 1, 2, ..., 99
  /// A Pipe preserves accepted emission order.
  @SpecRef({"5.3", "6.1"})
  @Test
  void dispatch_multipleEmissions_preservesAcceptedOrder() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > emissions = new ArrayList<>();

      final Pipe< Integer > async =
        circuit.pipe(emissions::add);

      for (int i = 0; i < 100; i++) {
        async.emit(i);
      }

      circuit.await();

      assertEquals(100, emissions.size());

      for (int i = 0; i < 100; i++) {
        assertEquals(i, emissions.get(i));
      }

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Multiple Circuits
  // ===========================

  /// Multiple Fiber operators execute in declaration order.
  @SpecRef("6.2.5")
  @Test
  void dispatch_multipleFiberOperators_executeInOrder() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > results = new ArrayList<>();
      final Pipe< Integer > target = circuit.pipe(results::add);

      final Pipe< Integer > pipeline =
        cortex.fiber(Integer.class)
          .guard(x -> x > 0)
          .diff()
          .limit(3)
          .pipe(circuit.pipe(target));

      pipeline.emit(-1);  // filtered by guard
      pipeline.emit(1);   // passes (diff)
      pipeline.emit(1);   // filtered by diff
      pipeline.emit(2);   // passes (diff)
      pipeline.emit(3);   // passes (diff)
      pipeline.emit(4);   // filtered by limit
      pipeline.emit(5);   // filtered by limit

      circuit.await();

      assertEquals(List.of(1, 2, 3), results);

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Transformation Pipes
  // ===========================

  /// Registered downstream targets all receive an emission.
  @SpecRef("6.3")
  @Test
  void dispatch_multipleRegisteredTargets_broadcastsEmission() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > target1 = new ArrayList<>();
      final List< Integer > target2 = new ArrayList<>();
      final List< Integer > target3 = new ArrayList<>();

      final Pipe< Integer > async =
        circuit.pipe(
          value -> {
            target1.add(value);
            target2.add(value);
            target3.add(value);
          }
        );

      async.emit(42);
      circuit.await();

      assertEquals(List.of(42), target1);
      assertEquals(List.of(42), target2);
      assertEquals(List.of(42), target3);

    } finally {

      circuit.close();

    }

  }

  /// Pipes from different Circuits deliver independently in their contexts.
  @SpecRef("6.3")
  @Test
  void dispatch_pipesFromDifferentCircuits_deliverIndependently() {

    final var circuit1 = cortex.circuit();
    final var circuit2 = cortex.circuit();

    try {

      final List< Integer > emissions1 = new ArrayList<>();
      final List< Integer > emissions2 = new ArrayList<>();

      final Pipe< Integer > async1 =
        circuit1.pipe(emissions1::add);

      final Pipe< Integer > async2 =
        circuit2.pipe(emissions2::add);

      async1.emit(1);
      async2.emit(2);

      circuit1.await();
      circuit2.await();

      assertEquals(List.of(1), emissions1);
      assertEquals(List.of(2), emissions2);

    } finally {

      circuit1.close();
      circuit2.close();

    }

  }

  /// A reduce Fiber materialized to a Pipe emits accumulated values.
  @SpecRef("6.2.3")
  @Test
  void dispatch_reduceFiber_emitsAccumulations() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > results = new ArrayList<>();
      final Pipe< Integer > target = circuit.pipe(results::add);

      final Pipe< Integer > accumulator =
        cortex.fiber(Integer.class)
          .reduce(0, Integer::sum)
          .pipe(circuit.pipe(target));

      accumulator.emit(1);
      accumulator.emit(2);
      accumulator.emit(3);

      circuit.await();

      assertEquals(List.of(1, 3, 6), results);

    } finally {

      circuit.close();

    }

  }

  /// A sift Fiber materialized to a Pipe filters by predicate.
  @SpecRef("6.2.2")
  @Test
  void dispatch_siftFiber_filtersByPredicate() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > results = new ArrayList<>();
      final Pipe< Integer > target = circuit.pipe(results::add);

      final Pipe< Integer > filtered =
        cortex.fiber(Integer.class)
          .above(Integer::compareTo, 5)
          .below(Integer::compareTo, 15)
          .pipe(circuit.pipe(target));

      for (int i = 0; i < 20; i++) {
        filtered.emit(i);
      }

      circuit.await();

      // Values > 5 and < 15: 6, 7, 8, 9, 10, 11, 12, 13, 14
      assertEquals(List.of(6, 7, 8, 9, 10, 11, 12, 13, 14), results);

    } finally {

      circuit.close();

    }

  }

  /// Verifies that a three-circuit chain maintains correct thread
  /// affinity at each hop.
  ///
  /// Creates a chain: circuit1 → circuit2 → circuit3
  /// Each circuit should execute its receptor on its own thread.
  /// A three-Circuit Pipe chain preserves each stage's context affinity.
  @SpecRef("6.3")
  @Test
  void dispatch_threeCircuitChain_usesEachOwningContext() {

    final var circuit1 = cortex.circuit();
    final var circuit2 = cortex.circuit();
    final var circuit3 = cortex.circuit();

    try {

      final List< Thread > threads1 = new ArrayList<>();
      final List< Thread > threads2 = new ArrayList<>();
      final List< Thread > threads3 = new ArrayList<>();

      // Circuit3's pipe (final destination)
      final Pipe< Integer > pipe3 =
        circuit3.pipe(_ -> threads3.add(currentThread()));

      // Circuit2's pipe (intermediate)
      final Pipe< Integer > pipe2 =
        circuit2.pipe(value -> {
          threads2.add(currentThread());
          pipe3.emit(value);
        });

      // Circuit1's pipe (entry point)
      final Pipe< Integer > pipe1 =
        circuit1.pipe(value -> {
          threads1.add(currentThread());
          pipe2.emit(value);
        });

      pipe1.emit(1);

      circuit1.await();
      circuit2.await();
      circuit3.await();

      // All three circuits should have processed exactly one emission
      assertEquals(1, threads1.size());
      assertEquals(1, threads2.size());
      assertEquals(1, threads3.size());

      // All three should use different threads
      final Thread t1 = threads1.getFirst();
      final Thread t2 = threads2.getFirst();
      final Thread t3 = threads3.getFirst();

      assertNotSame(t1, t2);
      assertNotSame(t2, t3);
      assertNotSame(t1, t3);

    } finally {

      circuit1.close();
      circuit2.close();
      circuit3.close();

    }

  }

  /// Pipe chains compose transformation before delivery.
  @SpecRef("6.2.6")
  @Test
  void dispatch_transformedPipeChain_deliversTransformedValue() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > results = new ArrayList<>();

      // Create async chain: source -> async1 -> async2 -> results
      // Transformation: (value + 1) * 2

      final Pipe< Integer > async2 =
        circuit.pipe(
          value -> results.add(value * 2)
        );

      final Pipe< Integer > async1 =
        circuit.pipe(
          value -> async2.emit(value + 1)
        );

      async1.emit(5);  // (5 + 1) * 2 = 12
      async1.emit(10); // (10 + 1) * 2 = 22

      circuit.await();

      assertEquals(2, results.size());
      assertEquals(12, results.get(0));
      assertEquals(22, results.get(1));

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Pipe Utilities
  // ===========================

  /// The receptor announces that delivery has begun, then waits behind a release gate. The emitting
  /// caller runs separately, so completing it while the receptor is still blocked proves admission is
  /// non-blocking without relying on scheduler speed or a wall-clock threshold.
  ///
  /// Pipe emit queues delivery and returns without blocking for the receptor.
  @SpecRef("6.1")
  @Test
  void emit_blockingReceptor_returnsBeforeDeliveryCompletes() throws Exception {

    final var circuit = cortex.circuit();
    final var receptorEntered = new CountDownLatch(1);
    final var releaseReceptor = new CountDownLatch(1);
    final var emitter = newSingleThreadExecutor();

    try {

      final AtomicInteger counter = new AtomicInteger(0);

      final Pipe< Integer > slowPipe =
        circuit.pipe(
          _ -> {
            receptorEntered.countDown();
            try {
              await(releaseReceptor, "the blocked receptor release gate");
            } catch (final InterruptedException exception) {
              currentThread().interrupt();
            }
            counter.incrementAndGet();
          }
        );

      final var emitted = emitter.submit(() -> slowPipe.emit(1));

      await(receptorEntered, "the blocked receptor to receive the queued value");

      assertDoesNotThrow(
        () -> get(emitted, "emit to return while delivery remains blocked"),
        "emit() must return while delivery remains blocked"
      );

      assertEquals(0, counter.get(), "Delivery must remain incomplete behind the release gate");

      releaseReceptor.countDown();

      circuit.await();

      assertEquals(1, counter.get(), "Target executed after await");

    } finally {

      releaseReceptor.countDown();
      emitter.shutdown();
      assertTrue(emitter.awaitTermination(5, SECONDS), "Emitter thread should terminate");
      circuit.close();

    }

  }

  /// Verifies that emissions to an empty pipe are queued and drained on the
  /// circuit thread. After `await()` returns, every emitted value has been
  /// dequeued and discarded without surfacing any exception.
  /// An empty Circuit Pipe discards emissions after normal admission.
  @Test
  void emit_emptyPipe_discardsEmission() {

    final var circuit = cortex.circuit();

    try {

      final Pipe< Integer > sink = circuit.pipe();

      for (int i = 0; i < 100; i++) {
        sink.emit(i);
      }

      circuit.await();

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Flow-Configured Pipes
  // ===========================

  /// Verifies the empty pipe still enforces the non-null emit contract.
  /// An empty Pipe still rejects absence.
  @SpecRef({"6.1", "15.2"})
  @Test
  void emit_nullIntoEmptyPipe_throwsNullPointerException() {

    final var circuit = cortex.circuit();

    try {

      final Pipe< Integer > sink = circuit.pipe();

      assertThrows(
        NullPointerException.class,
        () -> sink.emit(null)
      );

    } finally {

      circuit.close();

    }

  }

  /// Validates that emitting null to a pipe throws NullPointerException.
  ///
  /// Pipes enforce a non-null contract on emissions. The @NotNull annotation
  /// on Pipe.emit() is backed by a requireNonNull check in the SPI. This
  /// prevents null values from propagating through the pipeline where they
  /// could cause silent failures or NPEs in downstream operators.
  ///
  /// Expected: NullPointerException thrown synchronously on the caller's thread
  /// Pipe emit synchronously rejects absence.
  @SpecRef({"6.1", "15.2"})
  @Test
  void emit_nullValue_throwsNullPointerException() {

    final var circuit = cortex.circuit();

    try {

      final Pipe< Integer > pipe =
        circuit.pipe(
          _ -> {
          }
        );

      assertThrows(
        NullPointerException.class,
        () -> pipe.emit(null)
      );

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Pipe Subject Tests
  // ===========================

  /// A Circuit Pipe delivers an emitted value to its target.
  @SpecRef("6.1")
  @Test
  void emit_validValue_deliversToTarget() {

    final var circuit = cortex.circuit();

    try {

      final List< String > emissions = new ArrayList<>();

      final Pipe< String > target = circuit.pipe(emissions::add);
      final Pipe< String > async = circuit.pipe(target);

      async.emit("first");
      async.emit("second");
      async.emit("third");

      circuit.await();

      assertEquals(List.of("first", "second", "third"), emissions);

    } finally {

      circuit.close();

    }

  }

  /// Circuit Pipe creation rejects an absent target.
  @SpecRef("15.2")
  @Test
  void pipe_nullTarget_throwsNullPointerException() {

    final var circuit = cortex.circuit();

    try {

      assertThrows(
        NullPointerException.class,
        () -> circuit.pipe((Receptor< Integer >) null)
      );

      assertThrows(
        NullPointerException.class,
        () -> circuit.pipe(
          null,
          Receptor.of(
            Integer.class
          )
        )
      );

      assertThrows(
        NullPointerException.class,
        () -> circuit.pipe(
          cortex.name(
            "named.receptor"
          ),
          (Receptor< Integer >) null
        )
      );

    } finally {

      circuit.close();

    }

  }

  /// Receptor.of creates a typed consumer usable by Circuit#pipe.
  @Test
  void pipe_receptorFactory_deliversToConsumer() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > results = new ArrayList<>();

      // Using method reference
      final Pipe< Integer > pipe =
        circuit.pipe(results::add);

      pipe.emit(1);
      pipe.emit(2);
      pipe.emit(3);

      circuit.await();

      assertEquals(List.of(1, 2, 3), results);

    } finally {

      circuit.close();

    }

  }

  /// Receptor.of preserves stateful consumer behavior.
  @Test
  void pipe_statefulReceptorFactory_preservesConsumerState() {

    final var circuit = cortex.circuit();

    try {

      final AtomicInteger counter = new AtomicInteger(0);

      // Explicit lambda shows intent: ignoring value, counting emissions
      final Pipe< String > pipe =
        circuit.pipe(
          _ -> counter.incrementAndGet()
        );

      pipe.emit("a");
      pipe.emit("b");
      pipe.emit("c");

      circuit.await();

      assertEquals(3, counter.get());

    } finally {

      circuit.close();

    }

  }

  /// Circuit creates a non-null identity-bearing Pipe.
  @SpecRef("6.1")
  @Test
  void pipe_validReceptor_returnsIdentityBearingPipe() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > emissions = new ArrayList<>();

      final Pipe< Integer > target = circuit.pipe(emissions::add);
      final Pipe< Integer > async = circuit.pipe(target);

      assertNotNull(async);

      async.emit(42);
      circuit.await();

      assertEquals(List.of(42), emissions);

    } finally {

      circuit.close();

    }

  }

  /// Verifies `circuit.pipe()` returns a non-null pipe with a subject.
  /// Circuit#pipe() returns a valid no-op Pipe.
  @Test
  void pipe_withoutTarget_returnsValidNoOp() {

    final var circuit = cortex.circuit();

    try {

      final Pipe< Integer > pipe = circuit.pipe();

      assertNotNull(pipe);
      assertNotNull(pipe.subject());

    } finally {

      circuit.close();

    }

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

  /// Verifies that a conduit-created pipe (via channel) has the channel as parent.
  /// An anonymous Conduit Pipe enclosure uses the Conduit's subject name.
  @Test
  void subject_anonymousConduitPipe_enclosureUsesConduitName() {

    final var circuit = cortex.circuit();

    try {

      final var pipeName = cortex.name("test.pipe");

      final var conduit =
        circuit.conduit(Integer.class);

      final Pipe< Integer > pipe =
        conduit.get(pipeName);

      final var subject = pipe.subject();

      assertNotNull(subject);

      // Subject's enclosure should be the conduit's subject
      assertTrue(subject.enclosure().isPresent());

      // The pipe's parent should be the conduit
      subject.enclosure(
        parent -> assertEquals(
          circuit.subject().name().toString(),
          parent.name().toString()
        )
      );

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Cross-Circuit Threading Tests
  // ===========================

  /// Verifies that a circuit-created pipe has a subject with the circuit as parent.
  /// A Circuit-created Pipe subject is enclosed by the Circuit subject.
  @SpecRef("4.3")
  @Test
  void subject_circuitPipe_hasCircuitEnclosure() {

    final var circuit = cortex.circuit(cortex.name("test.circuit"));

    try {

      final Pipe< Integer > pipe =
        circuit.pipe(Receptor.of(Integer.class));

      final var subject = pipe.subject();

      assertNotNull(subject);
      assertNotNull(subject.id());

      // Subject's enclosure should be the circuit's subject
      assertTrue(subject.enclosure().isPresent());

      subject.enclosure(
        parent -> assertEquals(
          circuit.subject().id(),
          parent.id()
        )
      );

    } finally {

      circuit.close();

    }

  }

  /// Verifies the subject hierarchy for a conduit pipe: circuit → conduit → pipe
  /// A Conduit Pipe subject is enclosed by Conduit, Circuit, and Cortex subjects.
  @SpecRef("4.3")
  @Test
  void subject_conduitPipe_exposesOwnershipHierarchy() {

    final var circuitName = cortex.name("root.circuit");
    final var conduitName = cortex.name("events.conduit");
    final var pipeName = cortex.name("orders.pipe");

    final var circuit = cortex.circuit(circuitName);

    try {

      final var conduit =
        circuit.conduit(
          conduitName,
          Integer.class
        );

      final Pipe< Integer > pipe =
        conduit.get(pipeName);

      final var pipeSubject = pipe.subject();

      // Pipe name is the pooled lookup name.
      assertEquals(pipeName.toString(), pipeSubject.name().toString());

      // Walk up the hierarchy: pipe → conduit → circuit
      pipeSubject.enclosure(conduitSubject -> {
        assertEquals(conduitName.toString(), conduitSubject.name().toString());

        conduitSubject.enclosure(circuitSubject ->
          assertEquals(circuitName.toString(), circuitSubject.name().toString())
        );
      });

    } finally {

      circuit.close();

    }

  }

  /// Verifies the empty pipe is circuit-owned — passing it back to
  /// `circuit.pipe(target)` returns it unchanged (same-circuit optimization).
  /// Circuit#pipe returns an already-owned target Pipe unchanged.
  @SpecRef("14")
  @Test
  void subject_emptyPipe_hasCircuitOwnership() {

    final var circuit = cortex.circuit();

    try {

      final Pipe< Integer > sink = circuit.pipe();
      final Pipe< Integer > wrapped = circuit.pipe(sink);

      assertSame(sink, wrapped);

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Empty Pipe (circuit.pipe())
  // ===========================

  /// Verifies that pipes with flow configuration still have proper subjects.
  /// A materialized Fiber Pipe exposes a subject.
  @Test
  void subject_materializedFiberPipe_isPresent() {

    final var circuit = cortex.circuit(cortex.name("flow.circuit"));

    try {

      final Pipe< Integer > pipe =
        cortex.fiber(Integer.class)
          .guard(x -> x > 0)
          .diff()
          .pipe(circuit.pipe());

      final var subject = pipe.subject();

      assertNotNull(subject);
      assertNotNull(subject.id());

      // Parent is the circuit
      assertTrue(subject.enclosure().isPresent());

    } finally {

      circuit.close();

    }

  }

  /// Verifies that multiple pipes from same circuit have distinct subjects.
  /// Separately created Pipes have distinct subject identities.
  @SpecRef({"4.2", "4.3"})
  @Test
  void subject_multipleCreatedPipes_haveDistinctIdentities() {

    final var circuit = cortex.circuit();

    try {

      final Pipe< Integer > pipe1 =
        circuit.pipe();

      final Pipe< Integer > pipe2 =
        circuit.pipe();

      // Different subject instances
      assertNotSame(pipe1.subject(), pipe2.subject());

      // Different IDs
      assertNotEquals(
        pipe1.subject().id(),
        pipe2.subject().id()
      );

    } finally {

      circuit.close();

    }

  }

  /// Verifies that a conduit-created pipe inherits the channel's name.
  /// A Conduit Pipe subject uses its lookup Name.
  @SpecRef("10.1")
  @Test
  void subject_namedConduitPipe_usesLookupName() {

    final var circuit = cortex.circuit();

    try {

      final var channelName = cortex.name("orders");

      final var conduit =
        circuit.conduit(Integer.class);

      final Pipe< Integer > pipe =
        conduit.get(channelName);

      assertEquals(
        channelName.toString(),
        pipe.subject().name().toString()
      );

    } finally {

      circuit.close();

    }

  }

  /// Verifies `circuit.pipe(name, receptor)` assigns the route name while preserving dispatch.
  /// Named receptor Pipe creation binds the supplied route name.
  @Test
  void subject_namedReceptorPipe_usesRouteName() {

    final var circuit = cortex.circuit();

    try {

      final var name =
        cortex.name(
          "named.receptor.pipe"
        );

      final List< Integer > results =
        new ArrayList<>();

      final Pipe< Integer > pipe =
        circuit.pipe(
          name,
          results::add
        );

      assertSame(
        name,
        pipe.subject().name()
      );

      pipe.emit(
        42
      );

      circuit.await();

      assertEquals(
        List.of(
          42
        ),
        results
      );

    } finally {

      circuit.close();

    }

  }

  /// Verifies that a circuit-created pipe inherits the circuit's name by default.
  /// An unnamed Circuit Pipe uses its Circuit's name.
  @Test
  void subject_unnamedCircuitPipe_usesCircuitName() {

    final var circuitName = cortex.name("my.circuit");
    final var circuit = cortex.circuit(circuitName);

    try {

      final Pipe< Integer > pipe =
        circuit.pipe(Receptor.of(Integer.class));

      assertEquals(
        circuitName.toString(),
        pipe.subject().name().toString()
      );

    } finally {

      circuit.close();

    }

  }

  /// Verifies the empty pipe's subject is parented by the circuit and inherits its name.
  /// An unnamed empty Pipe uses its Circuit subject name.
  @Test
  void subject_unnamedEmptyPipe_usesCircuitName() {

    final var circuitName = cortex.name("empty.pipe.circuit");
    final var circuit = cortex.circuit(circuitName);

    try {

      final Pipe< Integer > sink = circuit.pipe();
      final var subject = sink.subject();

      assertEquals(
        circuitName.toString(),
        subject.name().toString()
      );

      assertTrue(subject.enclosure().isPresent());

      subject.enclosure(
        parent -> assertEquals(
          circuit.subject().id(),
          parent.id()
        )
      );

    } finally {

      circuit.close();

    }

  }

}
