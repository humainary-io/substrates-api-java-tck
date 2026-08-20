// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.*;

import static io.humainary.substrates.api.Substrates.Routing.*;
import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §§10.1–10.3 Conduit pooling, Source lifecycle, and routing, plus the
/// Java projection's Fiber, Flow, and hierarchical-routing extensions.
/// @author William David Louth
/// @since 1.0
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.1/SPEC.md")
final class ConduitContractTest
  extends TestSupport {

  private Cortex cortex;

  /// Verifies that closing a conduit releases its downstream subscription state.
  ///
  /// Scenario:
  /// 1. Subscribe to conduit, emit values, verify receipt
  /// 2. Close the conduit
  /// 3. Emit more values
  /// 4. Verify no further emissions reach subscribers
  /// 5. Verify sub.close() and conduit.close() are idempotent
  /// Closing a Conduit releases downstream subscriptions.
  @SpecRef({"9.1", "10.2"})
  @Test
  void close_activeSubscriptions_stopsSubsequentDelivery() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(
          Integer.class
        );

      final List< Integer > results = new ArrayList<>();

      final var sub =
        conduit.subscribe(
          circuit.subscriber(
            cortex.name("collector"),
            (_, registrar) ->
              registrar.register(results::add)
          )
        );

      final var pipe =
        conduit.get(
          cortex.name("test")
        );

      pipe.emit(1);
      pipe.emit(2);

      circuit.await();

      assertEquals(2, results.size());

      conduit.close();
      circuit.await();

      // Emissions after close do not reach downstream
      pipe.emit(3);
      pipe.emit(4);
      circuit.await();

      assertEquals(2, results.size());

      // Idempotent: sub.close() and conduit.close() are safe after close
      sub.close();
      conduit.close();
      circuit.await();

      assertEquals(2, results.size());

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Channel Pooling Identity Tests
  // ===========================

  /// The default Conduit routing mode delivers only to the target named Pipe.
  @SpecRef("10.3")
  @Test
  void dispatch_defaultRouting_deliversOnlyTargetPipe() {

    final var circuit = cortex.circuit();

    try {

      final var conduit = circuit.conduit(Integer.class);
      final var deliveries = new ArrayList< Name >();
      final var subscriber =
        circuit.< Integer > subscriber(
          cortex.name("routing.default.subscriber"),
          (subject, registrar) ->
            registrar.register(_ -> deliveries.add(subject.name()))
        );
      conduit.subscribe(subscriber);

      final var parentName = cortex.name("routing.default.parent");
      final var leafName = cortex.name("routing.default.parent.leaf");

      conduit.get(parentName).emit(0);
      circuit.await();
      deliveries.clear();

      conduit.get(leafName).emit(1);
      circuit.await();

      assertEquals(List.of(leafName), deliveries);

    } finally {

      circuit.closeAwait();

    }

  }

  /// Verifies that closing a conduit leaves user-held Pipe references callable.
  ///
  /// Since conduits do not clear their internal pools on close, Pipe references
  /// returned by earlier `get()` calls remain usable. Emissions
  /// through them are accepted for processing but dispatch to no
  /// subscribers after close.
  /// A Pipe reference obtained before Conduit close remains callable.
  @Test
  void emit_pipeRetainedAfterConduitClose_doesNotThrow() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(
          Integer.class
        );

      final var name = cortex.name("test");

      final var before = conduit.get(name);

      conduit.close();
      circuit.await();

      // A Pipe obtained before close remains callable.
      assertDoesNotThrow(() -> before.emit(1));
      circuit.await();

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Conduit lifecycle
  // ===========================

  /// A custom wrapper derived Pool preserves canonical per-name identity.
  @SpecRef("10.1")
  @Test
  void get_customWrapperPool_returnsSameWrapperPerName() {

    final var circuit = cortex.circuit();

    try {

      // Custom Pipe wrapper.
      record Sensor( Pipe< Double > pipe ) {

      }

      final var conduit =
        circuit.conduit(Double.class);

      final var sensors = conduit.pool(Sensor::new);

      final var name = cortex.name("temperature.sensor");

      // Resolve the custom wrapper multiple times.
      final var sensor1 = sensors.get(name);
      final var sensor2 = sensors.get(name);
      final var sensor3 = sensors.get(name);

      // All lookups return the same wrapper instance.
      assertSame(
        sensor1,
        sensor2,
        "Same name must return same wrapper instance (1st vs 2nd)"
      );

      assertSame(
        sensor2,
        sensor3,
        "Same name must return same wrapper instance (2nd vs 3rd)"
      );

      // Underlying pipes should also be same
      assertSame(
        sensor1.pipe(),
        sensor2.pipe(),
        "Same wrapper should expose the same Pipe"
      );

    } finally {

      circuit.close();

    }

  }

  /// A derived Pool produces distinct cached results for different names.
  @SpecRef("10.1")
  @Test
  void get_differentNamesFromDerivedPool_returnsDistinctResults() {

    final var circuit = cortex.circuit();

    try {

      record Probe( Pipe< Integer > pipe ) {
      }

      final var conduit =
        circuit.conduit(Integer.class);

      final var probes = conduit.pool(Probe::new);

      final var name1 = cortex.name("probe.alpha");
      final var name2 = cortex.name("probe.beta");

      final var probe1 = probes.get(name1);
      final var probe2 = probes.get(name2);

      // Different names produce different derived results.
      assertNotSame(
        probe1,
        probe2,
        "Different names must return different derived results"
      );

      // With different underlying pipes
      assertNotSame(
        probe1.pipe(),
        probe2.pipe(),
        "Different derived results should have different pipes"
      );

    } finally {

      circuit.close();

    }

  }

  /// Different names within one Conduit produce distinct Pipes.
  @SpecRef("10.1")
  @Test
  void get_differentNames_returnsDistinctPipes() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final var name1 = cortex.name("channel.one");
      final var name2 = cortex.name("channel.two");

      final var channel1 = conduit.get(name1);
      final var channel2 = conduit.get(name2);

      // Different names should produce different channels
      assertNotSame(
        channel1,
        channel2,
        "Different names must return different channel instances"
      );

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Pool.get() by Subject/Substrate Tests
  // ===========================

  /// Repeated derived-Pool lookup returns one transformed result per name.
  @SpecRef("10.1")
  @Test
  void get_repeatedNameFromDerivedPool_returnsSameResult() {

    final var circuit = cortex.circuit();

    try {

      record Monitor( Pipe< String > pipe ) {
      }

      final var conduit =
        circuit.conduit(String.class);

      final var monitors = conduit.pool(Monitor::new);

      final var name = cortex.name("system.monitor");

      // Resolve the same pooled result repeatedly.
      final var firstMonitor = monitors.get(name);

      for (int i = 0; i < 100; i++) {
        final var monitor = monitors.get(name);
        assertSame(
          firstMonitor,
          monitor,
          "Repeated get() must return the same result (iteration " + i + ")"
        );
      }

    } finally {

      circuit.close();

    }

  }


  // ===========================
  // Resource Lifecycle Tests
  // ===========================

  /// Distinct Conduits maintain independent Pipe pools.
  @SpecRef("10.1")
  @Test
  void get_sameNameFromDifferentConduits_returnsDistinctPipes() {

    final var circuit = cortex.circuit();

    try {

      final var conduit1 =
        circuit.conduit(Integer.class);

      final var conduit2 =
        circuit.conduit(Integer.class);

      final var name = cortex.name("shared.name");

      // Get channels from different conduits with same name
      final var channel1 = conduit1.get(name);
      final var channel2 = conduit2.get(name);

      // Should be different instances (separate pools)
      assertNotSame(
        channel1,
        channel2,
        "Different conduits should have separate channel pools"
      );

    } finally {

      circuit.close();

    }

  }

  /// Repeated same-name Conduit lookup returns one canonical Pipe.
  @SpecRef({"10.1", "12"})
  @Test
  void get_sameName_returnsSamePipe() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final var name = cortex.name("pooled.channel");

      // Get channel multiple times with same name
      final var channel1 = conduit.get(name);
      final var channel2 = conduit.get(name);
      final var channel3 = conduit.get(name);

      // All should be the SAME instance
      assertSame(
        channel1,
        channel2,
        "Same name must return same channel instance (1st vs 2nd)"
      );

      assertSame(
        channel2,
        channel3,
        "Same name must return same channel instance (2nd vs 3rd)"
      );

      assertSame(
        channel1,
        channel3,
        "Same name must return same channel instance (1st vs 3rd)"
      );

    } finally {

      circuit.close();

    }

  }


  // ===========================
  // STEM Routing Tests
  // ===========================

  /// Verifies that `pool(Flow)` materializes a composed flow (via
  /// `flow.flow(next)`) correctly per channel. Exercises the
  /// Composed receptor path through the pool's name-keyed materialization.
  /// Conduit#pool(Flow) materializes composed Flow operations.
  @Test
  void pool_composedFlow_materializesCompositionPerPipe() {

    final var circuit = cortex.circuit();

    try {

      final var conduit = circuit.conduit(Integer.class);

      // composed: String → (parseInt) → (+1) → Integer
      final var composed =
        cortex.flow(String.class).map(Integer::parseInt)
          .flow(cortex.flow(Integer.class).map(i -> i + 1));

      final var inputs = conduit.pool(composed);

      final var captureBuffer = CaptureBuffer.of(circuit, conduit, 1024);

      final var pipe = inputs.get(cortex.name("ch"));

      pipe.emit("10");
      pipe.emit("41");

      circuit.await();

      final var captures = captureBuffer.drain().toList();

      assertEquals(2, captures.size());
      assertEquals(11, captures.get(0).emission());
      assertEquals(42, captures.get(1).emission());

      captureBuffer.close();

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // pool(Flow) / pool(Fiber) overloads
  // ===========================

  /// Verifies `conduit.pool(Fiber)` prepends a type-preserving fiber —
  /// filtering/stateful operators apply before emissions reach the conduit.
  /// Conduit#pool(Fiber) applies the Fiber to each named Pipe.
  @Test
  void pool_fiberOverload_appliesFiberPerPipe() {

    final var circuit = cortex.circuit();

    try {

      final var conduit = circuit.conduit(Integer.class);

      final var deduped =
        conduit.pool(
          cortex.fiber(Integer.class).diff()
        );

      final var captureBuffer = CaptureBuffer.of(circuit, conduit, 1024);

      final var pipe = deduped.get(cortex.name("ch"));

      pipe.emit(1);
      pipe.emit(1);
      pipe.emit(2);
      pipe.emit(2);
      pipe.emit(3);

      circuit.await();

      final var captures = captureBuffer.drain().toList();

      assertEquals(3, captures.size());
      assertEquals(1, captures.get(0).emission());
      assertEquals(2, captures.get(1).emission());
      assertEquals(3, captures.get(2).emission());

      captureBuffer.close();

    } finally {

      circuit.close();

    }

  }

  /// Verifies `conduit.pool(Flow)` prepends the flow to each named pipe —
  /// emissions of type T flow through operators and land in the conduit
  /// as type E after transformation.
  /// Conduit#pool(Flow) applies the Flow to each named Pipe.
  @Test
  void pool_flowOverload_appliesFlowPerPipe() {

    final var circuit = cortex.circuit();

    try {

      final var conduit = circuit.conduit(String.class);

      final var inputs =
        conduit.pool(
          cortex.flow(Integer.class).map(i -> "n=" + i)
        );

      final var captureBuffer = CaptureBuffer.of(circuit, conduit, 1024);

      final var pipe = inputs.get(cortex.name("ch"));

      pipe.emit(1);
      pipe.emit(42);

      circuit.await();

      final var captures = captureBuffer.drain().toList();

      assertEquals(2, captures.size());
      assertEquals("n=1", captures.get(0).emission());
      assertEquals("n=42", captures.get(1).emission());

      captureBuffer.close();

    } finally {

      circuit.close();

    }

  }

  /// Verifies null guards on the new pool overloads.
  /// Conduit Pool overloads reject null recipes.
  @SpecRef("15.2")
  @Test
  void pool_nullRecipe_throwsNullPointerException() {

    final var circuit = cortex.circuit();

    try {

      final var conduit = circuit.conduit(Integer.class);

      assertThrows(
        NullPointerException.class,
        () -> conduit.pool((Flow< Integer, Integer >) null)
      );

      assertThrows(
        NullPointerException.class,
        () -> conduit.pool((Fiber< Integer >) null)
      );

    } finally {

      circuit.close();

    }

  }

  /// Verifies that `pool(Fiber)` materializes fresh operator state per channel.
  /// Two channels sharing the same `diff()` fiber must each dedupe their own
  /// emissions — no state bleed across channels. Regression guard for the
  /// name-caching / per-channel materialization invariant.
  /// Conduit#pool(Fiber) materializes independent state per named Pipe.
  @Test
  void pool_statefulFiber_materializesIndependentStatePerPipe() {

    final var circuit = cortex.circuit();

    try {

      final var conduit = circuit.conduit(Integer.class);

      final var deduped =
        conduit.pool(
          cortex.fiber(Integer.class).diff()
        );

      final var captureBuffer = CaptureBuffer.of(circuit, conduit, 1024);

      final var a = cortex.name("ch.a");
      final var b = cortex.name("ch.b");

      final var pipeA = deduped.get(a);
      final var pipeB = deduped.get(b);

      // A: 1, 1, 2 → expect 1, 2 after diff
      // B: 1, 1, 2 → expect 1, 2 after diff (independent state)
      pipeA.emit(1);
      pipeA.emit(1);
      pipeB.emit(1);
      pipeB.emit(1);
      pipeA.emit(2);
      pipeB.emit(2);

      circuit.await();

      final var captures = captureBuffer.drain().toList();

      // Group by channel subject
      final List< Integer > aEmissions = new ArrayList<>();
      final List< Integer > bEmissions = new ArrayList<>();

      for (final var c : captures) {
        if (c.subject().name().equals(a)) {
          aEmissions.add(c.emission());
        } else if (c.subject().name().equals(b)) {
          bEmissions.add(c.emission());
        }
      }

      assertEquals(List.of(1, 2), aEmissions, "channel A diff state isolated");
      assertEquals(List.of(1, 2), bEmissions, "channel B diff state isolated");

      captureBuffer.close();

    } finally {

      circuit.close();

    }

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

  @Nested
  final class StemRouting {

    /// Leaf-first ordering: leaf subscribers see emission before parent subscribers.
    /// Hierarchical routing delivers leaf before ancestors.
    @SpecRef("10.3")
    @Test
    void dispatch_leafEmission_deliversLeafBeforeAncestors() {

      final var circuit = cortex.circuit();

      try {

        final var conduit =
          circuit.conduit(
            Integer.class,
            STEM
          );

        final List< String > order = new ArrayList<>();

        conduit.subscribe(
          circuit.subscriber(
            cortex.name("observer"),
            (subject, registrar) -> {

              final var name = subject.name().toString();

              if (name.equals("app")) {
                registrar.register(_ -> order.add("root"));
              } else if (name.equals("app.leaf")) {
                registrar.register(_ -> order.add("leaf"));
              }

            }
          )
        );

        conduit.get(
          cortex.name("app.leaf")
        ).emit(1);

        circuit.await();

        assertEquals(2, order.size());
        assertEquals("leaf", order.get(0), "Leaf should be dispatched first");
        assertEquals("root", order.get(1), "Root should be dispatched second");

      } finally {

        circuit.close();

      }

    }

    /// Emitting on a leaf pipe propagates through multiple ancestor levels.
    /// Hierarchical routing reaches every ancestor.
    @SpecRef("10.3")
    @Test
    void dispatch_leafEmission_reachesAllAncestors() {

      final var circuit = cortex.circuit();

      try {

        final var conduit =
          circuit.conduit(
            String.class,
            STEM
          );

        final List< String > rootResults = new ArrayList<>();
        final List< String > midResults = new ArrayList<>();
        final List< String > leafResults = new ArrayList<>();

        conduit.subscribe(
          circuit.subscriber(
            cortex.name("observer"),
            (subject, registrar) -> {

              final var name = subject.name().toString();

              switch (name) {
                case "app" -> registrar.register(rootResults::add);
                case "app.service" -> registrar.register(midResults::add);
                case "app.service.endpoint" -> registrar.register(leafResults::add);
              }

            }
          )
        );

        final var leaf =
          conduit.get(
            cortex.name("app.service.endpoint")
          );

        leaf.emit("hello");

        circuit.await();

        assertEquals(1, leafResults.size(), "Leaf should receive emission");
        assertEquals(1, midResults.size(), "Middle ancestor should receive emission");
        assertEquals(1, rootResults.size(), "Root ancestor should receive emission");

        assertEquals("hello", leafResults.getFirst());
        assertEquals("hello", midResults.getFirst());
        assertEquals("hello", rootResults.getFirst());

      } finally {

        circuit.close();

      }

    }

    /// Emitting on a leaf pipe propagates to a subscriber on the parent name.
    /// Hierarchical routing reaches the direct parent.
    @SpecRef("10.3")
    @Test
    void dispatch_leafEmission_reachesDirectParent() {

      final var circuit = cortex.circuit();

      try {

        final var conduit =
          circuit.conduit(
            Integer.class,
            STEM
          );

        final List< Integer > parentResults = new ArrayList<>();

        conduit.subscribe(
          circuit.subscriber(
            cortex.name("observer"),
            (subject, registrar) -> {

              if (subject.name().equals(cortex.name("app"))) {
                registrar.register(parentResults::add);
              }

            }
          )
        );

        final var leaf =
          conduit.get(
            cortex.name("app.service")
          );

        leaf.emit(1);
        leaf.emit(2);
        leaf.emit(3);

        circuit.await();

        assertEquals(
          3,
          parentResults.size(),
          "Parent should receive all leaf emissions via STEM propagation"
        );

        assertEquals(1, parentResults.get(0));
        assertEquals(2, parentResults.get(1));
        assertEquals(3, parentResults.get(2));

      } finally {

        circuit.close();

      }

    }

    /// A previously empty STEM chain is invalidated when a parent subscriber is added.
    /// A parent subscription observes future leaf emissions.
    @SpecRef("10.3")
    @Test
    void dispatch_parentSubscribedBeforeLeaf_receivesFutureLeafEmission() {

      final var circuit = cortex.circuit();

      try {

        final var conduit =
          circuit.conduit(
            Integer.class,
            STEM
          );

        final var leaf =
          conduit.get(
            cortex.name("app.service")
          );

        leaf.emit(0);

        circuit.await();

        final List< Integer > parentResults = new ArrayList<>();

        conduit.subscribe(
          circuit.subscriber(
            cortex.name("observer"),
            (subject, registrar) -> {

              if (subject.name().equals(cortex.name("app"))) {
                registrar.register(parentResults::add);
              }

            }
          )
        );

        leaf.emit(1);

        circuit.await();

        assertEquals(1, parentResults.size());
        assertEquals(1, parentResults.getFirst());

      } finally {

        circuit.close();

      }

    }

    /// Root name (no parent) behaves like PIPE routing — no propagation.
    /// Root emission has no ancestor propagation.
    @SpecRef("10.3")
    @Test
    void dispatch_rootEmission_deliversOnlyAtRoot() {

      final var circuit = cortex.circuit();

      try {

        final var conduit =
          circuit.conduit(
            Integer.class,
            STEM
          );

        final List< Integer > results = new ArrayList<>();

        conduit.subscribe(
          circuit.subscriber(
            cortex.name("observer"),
            (_, registrar) ->
              registrar.register(results::add)
          )
        );

        conduit.get(
          cortex.name("root")
        ).emit(42);

        circuit.await();

        assertEquals(1, results.size());
        assertEquals(42, results.getFirst());

      } finally {

        circuit.close();

      }

    }


    /// Sibling leaves share ancestor pipes — both propagate to the same parent.
    /// Sibling emissions both propagate to a shared ancestor.
    @SpecRef("10.3")
    @Test
    void dispatch_siblingEmissions_reachSharedAncestor() {

      final var circuit = cortex.circuit();

      try {

        final var conduit =
          circuit.conduit(
            String.class,
            STEM
          );

        final List< String > parentResults = new ArrayList<>();

        conduit.subscribe(
          circuit.subscriber(
            cortex.name("observer"),
            (subject, registrar) -> {

              if (subject.name().equals(cortex.name("app"))) {
                registrar.register(parentResults::add);
              }

            }
          )
        );

        conduit.get(cortex.name("app.alice")).emit("from-alice");
        conduit.get(cortex.name("app.bob")).emit("from-bob");

        circuit.await();

        assertEquals(2, parentResults.size(), "Parent should see emissions from both siblings");
        assertEquals("from-alice", parentResults.get(0));
        assertEquals("from-bob", parentResults.get(1));

      } finally {

        circuit.close();

      }

    }

  }

}
