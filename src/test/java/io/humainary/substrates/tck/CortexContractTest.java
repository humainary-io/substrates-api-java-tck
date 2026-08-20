// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance and Java-projection tests for Cortex identity and its Circuit, Scope, Subscriber,
/// and capture-composition factory surface.
/// @author William David Louth
/// @since 1.0

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.1/SPEC.md")
final class CortexContractTest
  extends TestSupport {

  private Cortex cortex;

  /// A Cortex-created Circuit await completes with an empty queue.
  @SpecRef("5.5")
  @Test
  void await_newCircuit_returnsAfterBarrier() {

    final var circuit = cortex.circuit();

    try {

      // Create a simple conduit and emit a value
      final var conduit =
        circuit.conduit(Integer.class);

      conduit.get(cortex.name("test.channel"))
        .emit(42);

      // Await should complete when queue is drained
      circuit.await();

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Cortex Tests
  // ===========================

  /// Validates that subscribers receive notifications for each channel created.
  ///
  /// This test verifies the dynamic subscription mechanism: when a subscriber
  /// is registered with a conduit, it receives a callback for EVERY channel
  /// that is subsequently created. This enables observability patterns where
  /// subscribers can attach monitoring pipes to all channels without knowing
  /// their names ahead of time.
  ///
  /// Test Scenario:
  /// 1. Subscribe to conduit (before any channels exist)
  /// 2. Create channel "one" → subscriber notified with channel subject
  /// 3. Create channel "two" → subscriber notified with channel subject
  /// 4. Create channel "three" → subscriber notified with channel subject
  /// 5. Verify: subscriber received 3 distinct channel subjects
  ///
  /// Dynamic Subscription Flow:
  /// ```
  /// conduit.subscribe(subscriber)
  ///   ↓
  /// [subscriber registered in conduit's subscriber list]
  ///   ↓
  /// conduit.get("channel.one")
  ///   ↓
  /// [channel created]
  ///   ↓
  /// subscriber.callback(channelSubject, registrar)
  ///   ↓ [subscriber registers pipes]
  /// registrar.register(observerPipe)
  ///   ↓
  /// [observerPipe added to channel's emission list]
  /// ```
  ///
  /// Why this pattern matters:
  /// - **Universal observability**: Monitor all channels without hardcoding names
  /// - **Runtime discovery**: No need to know channel names at subscriber creation
  /// - **Automatic attachment**: New channels automatically get monitoring pipes
  /// - **Separation of concerns**: Observability separate from business logic
  ///
  /// Real-world use cases:
  /// - **Metrics collection**: Attach counter/timer to every channel
  /// - **Distributed tracing**: Inject trace context propagation on all channels
  /// - **Logging**: Log all emissions across all channels
  /// - **Debugging**: Attach inspector to all channels in a circuit
  ///
  /// Subscriber Callback Contract:
  /// ```java
  /// BiConsumer<Subject<Channel<T>>, Registrar<T>> subscriber = (subject, registrar) -> {
  ///   // subject: The channel that was just created
  ///   // registrar: Register pipes to receive channel's emissions
  ///
  ///   Pipe<T> observerPipe = circuit.pipe(value -> {
  ///     // This runs on circuit thread for each emission
  ///     recordMetric(subject.name(), value);
  ///   });
  ///
  ///   registrar.register(observerPipe);
  /// };
  /// ```
  ///
  /// Critical behaviors verified:
  /// - Subscriber callback invoked for each channel creation
  /// - Subject parameter contains channel identity information
  /// - Registrar enables pipe registration per channel
  /// - Emissions trigger registered pipes (verified by test structure)
  ///
  /// Timing considerations:
  /// - Subscribe BEFORE channel creation (forward subscription)
  /// - Subscribe AFTER channel creation triggers rebuild (retrospective)
  /// - This test validates forward case (most common)
  ///
  /// Expected: 3 channel subjects received by subscriber (one per channel)
  /// Subscriber callback receives the active named Pipe subject.
  @SpecRef("7.3")
  @Test
  void callback_namedPipeEmission_receivesPipeSubject() {

    final var circuit = cortex.circuit();

    try {

      final Conduit< String > conduit =
        circuit.conduit(String.class);

      final List< Subject< ? > > receivedSubjects = new ArrayList<>();

      final Subscriber< String > subscriber =
        circuit.subscriber(
          cortex.name("test.subscriber"),
          (subject, registrar) -> {
            receivedSubjects.add(subject);
            registrar.register(Receptor.of());
          }
        );

      // Subscribe before creating channels
      conduit.subscribe(subscriber);

      // Create channels which should trigger subscriber
      final Pipe< String > pipe1 = conduit.get(cortex.name("channel.one"));
      final Pipe< String > pipe2 = conduit.get(cortex.name("channel.two"));
      final Pipe< String > pipe3 = conduit.get(cortex.name("channel.three"));

      // Emit values to ensure channels are actually created and subscribed
      pipe1.emit("test1");
      pipe2.emit("test2");
      pipe3.emit("test3");

      circuit.await();

      assertEquals(3, receivedSubjects.size());

    } finally {

      circuit.close();

    }

  }

  /// The Basin-backed capture helper can be created from Circuit context.
  @Test
  void capture_helperCreatedInCircuitContext_returnsUsableBuffer() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      // Conduit is a Context
      final CaptureBuffer< Integer > captureBuffer = CaptureBuffer.of(circuit, conduit, 1024);

      assertNotNull(captureBuffer);

      captureBuffer.close();

    } finally {

      circuit.close();

    }

  }

  /// Validates incremental drain behavior: each drain returns only new emissions.
  ///
  /// This test verifies that draining a source-fed capture buffer operates
  /// incrementally: each drain returns only the emissions retained since the last
  /// drain (the buffer is consumed and cleared), so previously drained emissions
  /// are NOT returned again, enabling efficient polling without duplicate handling.
  ///
  /// Test Scenario:
  /// 1. Emit "first", wait for processing
  /// 2. First drain → returns "first" (1 emission)
  /// 3. Emit "second", wait for processing
  /// 4. Second drain → returns "second" (1 emission, not 2!)
  ///
  /// Drain Semantics (consume-and-clear):
  /// ```
  /// The buffer retains values until drained; each drain forwards then clears them:
  ///   buffer: [________________]
  ///
  /// After first emission:
  ///   buffer: [first___________]
  ///
  /// After first drain:
  ///   buffer: [________________]  (forwarded "first", then cleared)
  ///
  /// After second emission:
  ///   buffer: [second__________]
  ///
  /// After second drain:
  ///   buffer: [________________]  (forwarded "second", then cleared)
  /// ```
  ///
  /// Why incremental matters:
  /// - **Polling loops**: Can periodically drain without tracking what was seen
  /// - **Memory efficiency**: Drained values are released after each drain
  /// - **Batch processing**: Process chunks incrementally (e.g., flush every 100)
  /// - **No duplication**: Each value is forwarded exactly once
  ///
  /// Usage Pattern:
  /// ```java
  /// // Polling loop with incremental drain
  /// while (running) {
  ///   Thread.sleep(100);
  ///   basin.drain(circuit.pipe(capture -> process(capture.emission())));
  ///   circuit.await();
  /// }
  /// // No need to track "last processed" — each drain consumes and clears
  /// ```
  ///
  /// Critical behaviors verified:
  /// - First drain returns emission count = 1 (only "first")
  /// - Second drain returns emission count = 1 (only "second", not 2)
  /// - Drains are independent (second doesn't include first)
  /// - Each drain consumes and clears the buffer (no re-delivery)
  ///
  /// Real-world applications:
  /// - Log aggregation (drain logs every second)
  /// - Metrics collection (drain counters periodically)
  /// - Event streaming (batch events for bulk processing)
  /// - Testing/debugging (inspect emissions without affecting system)
  ///
  /// Expected: First `drain=[first]`, second `drain=[second]` (incremental, not cumulative)
  /// Repeated Basin-backed capture drains contain only new emissions.
  @Test
  void capture_repeatedBasinDrains_returnNewEmissionsOnly() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(String.class);

      final CaptureBuffer< String > captureBuffer = CaptureBuffer.of(circuit, conduit, 1024);

      final Pipe< String > pipe =
        conduit.get(cortex.name("test.channel"));

      pipe.emit("first");

      circuit.await();

      final var firstDrain =
        captureBuffer.drain().toList();

      assertEquals(1, firstDrain.size());
      assertEquals("first", firstDrain.getFirst().emission());

      // Emit another value after the first drain
      pipe.emit("second");

      circuit.await();

      // Second drain should have only new emissions since last drain
      final var secondDrain =
        captureBuffer.drain().toList();

      assertEquals(1, secondDrain.size());
      assertEquals("second", secondDrain.getFirst().emission());

      captureBuffer.close();

    } finally {

      circuit.close();

    }

  }

  /// The Basin-backed capture helper retains Source emissions.
  @Test
  void capture_sourceIntoBasin_retainsEmissions() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(Integer.class);

      final CaptureBuffer< Integer > captureBuffer = CaptureBuffer.of(circuit, conduit, 1024);

      final Pipe< Integer > pipe =
        conduit.get(cortex.name("test.channel"));

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

  // ===========================
  // Circuit Tests
  // ===========================

  /// Cortex creates a concrete Circuit entry point.
  @SpecRef("3")
  @Test
  void circuit_cortexFactory_returnsCircuit() {

    final var circuit = cortex.circuit();

    assertNotNull(circuit);
    assertNotNull(circuit.subject());

  }

  /// Cortex#circuit(Name) binds the supplied subject name.
  @Test
  void circuit_explicitName_bindsSubjectName() {

    final var circuitName = cortex.name("cortex.test.circuit");
    final var circuit = cortex.circuit(circuitName);

    assertNotNull(circuit);
    assertEquals(circuitName, circuit.subject().name());

  }

  /// Named Circuit factory rejects absence.
  @SpecRef("15.2")
  @Test
  void circuit_nullName_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> cortex.circuit(null)
    );

  }

  /// Nested Cortex-created Scopes cascade resource closure.
  @SpecRef("9.2")
  @Test
  void close_nestedScopes_closesRegisteredResources() {

    final var root = cortex.scope(
      cortex.name("nested.root")
    );

    final var child = root.scope(
      cortex.name("nested.child")
    );

    final var rootCircuit = root.register(cortex.circuit());
    final var childCircuit = child.register(cortex.circuit());

    assertNotNull(rootCircuit);
    assertNotNull(childCircuit);

    child.close();
    root.close();

  }

  /// A Cortex-created Circuit closes safely.
  @SpecRef("9.3")
  @Test
  void close_newCircuit_completesSafely() {

    final var circuit = cortex.circuit();

    // Should not throw
    circuit.close();

    // Multiple closes should be idempotent
    circuit.close();
    circuit.close();

  }

  /// Scope manages and closes multiple resources.
  @SpecRef("9.2")
  @Test
  void close_scopeWithMultipleResources_closesEveryResource() {

    final var scope = cortex.scope(
      cortex.name("multi.resource.scope")
    );

    final var circuit1 = scope.register(cortex.circuit());
    final var circuit2 = scope.register(cortex.circuit());
    final var circuit3 = scope.register(cortex.circuit());

    assertNotNull(circuit1);
    assertNotNull(circuit2);
    assertNotNull(circuit3);

    // All should be closed when scope closes
    scope.close();

  }

  /// A Cortex-created Scope closes its registered resources.
  @SpecRef("9.2")
  @Test
  void close_scopeWithRegisteredResource_closesResource() {

    final var scope = cortex.scope();

    final var circuit = cortex.circuit();
    scope.register(circuit);

    // Closing scope should close registered resources
    scope.close();

    // Multiple closes should be idempotent
    scope.close();

  }

  // ===========================
  // ===========================

  /// Scope closure manages one resource through single-use cleanup.
  @SpecRef("9.2")
  @Test
  void closure_sameResource_consumesAndCleansUp() {

    final var scope = cortex.scope();

    final var circuit = cortex.circuit();

    try {

      final Closure< Circuit > first = scope.closure(circuit);
      final Closure< Circuit > second = scope.closure(circuit);

      assertSame(first, second);

      final var consumed = new AtomicBoolean(false);

      first.consume(_ ->
        consumed.set(true)
      );

      assertTrue(consumed.get());

      final Closure< Circuit > third = scope.closure(circuit);

      assertNotSame(first, third);

      third.consume(ignored -> {
      });

    } finally {

      scope.close();
      circuit.close();

    }

  }

  /// Cortex factories compose Circuit, Conduit, Sink, and Basin capture.
  @Test
  void cortex_conduitSinkBasin_composesCapturePipeline() {

    final var circuit = cortex.circuit(
      cortex.name("integration.circuit")
    );

    try {

      final var conduit =
        circuit.conduit(
          cortex.name("integration.conduit"),
          String.class
        );

      final CaptureBuffer< String > captureBuffer = CaptureBuffer.of(circuit, conduit, 1024);

      final Pipe< String > pipe =
        conduit.get(cortex.name("integration.channel"));

      pipe.emit("integration-test");

      circuit.await();

      final var captures =
        captureBuffer.drain().toList();

      assertEquals(1, captures.size());
      assertEquals("integration-test", captures.getFirst().emission());
      assertEquals(
        Pipe.class,
        captures.getFirst().subject().type()
      );

      captureBuffer.close();

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Scope Tests
  // ===========================

  /// Child Scope enclosure accessors expose the parent.
  @SpecRef({"4.4", "9.2"})
  @Test
  void enclosure_childScope_returnsParent() {

    final var parent = cortex.scope(
      cortex.name("scope.enclosure.parent")
    );

    final var child = parent.scope();
    final var grandchild = child.scope();

    final Scope[] captured = new Scope[1];
    final var rootCalled = new AtomicBoolean(false);

    assertSame(parent, child.enclosure().orElseThrow());
    assertSame(child, grandchild.enclosure().orElseThrow());

    grandchild.enclosure(scope ->
      captured[0] = scope
    );

    parent.enclosure(ignored ->
      rootCalled.set(true)
    );

    assertSame(child, captured[0]);
    assertFalse(parent.enclosure().isPresent());
    assertFalse(rootCalled.get());

    grandchild.close();
    child.close();
    parent.close();

  }

  /// Scope#register returns and manages the supplied resource.
  @SpecRef("9.2")
  @Test
  void register_openScope_returnsSuppliedResource() {

    final var scope = cortex.scope();

    final var circuit = cortex.circuit();
    final var registered = scope.register(circuit);

    assertSame(circuit, registered);

    scope.close();

  }

  /// Terminal Scope rejects new management operations.
  @SpecRef("9.2")
  @Test
  void scope_afterClose_rejectsManagementOperations() {

    final var scope = cortex.scope();

    scope.close();

    assertThrows(
      Fault.class,
      scope::scope
    );

    final var circuit = cortex.circuit();

    try {

      assertThrows(
        Fault.class,
        () -> scope.register(circuit)
      );

      assertThrows(
        Fault.class,
        () -> scope.closure(circuit)
      );

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Basin capture tests (source → sink → basin)
  // ===========================

  /// Scope#scope creates an enclosed child Scope.
  @SpecRef("9.2")
  @Test
  void scope_anonymousChild_hasParentEnclosure() {

    final var parent = cortex.scope();

    final var child = parent.scope();

    assertNotNull(child);
    assertNotNull(child.subject());

    child.close();
    parent.close();

  }

  /// Cortex creates a concrete root Scope.
  @SpecRef("9.2")
  @Test
  void scope_cortexFactory_returnsScope() {

    final var scope = cortex.scope();

    assertNotNull(scope);
    assertNotNull(scope.subject());

    scope.close();

  }

  /// Cortex#scope(Name) binds the supplied subject name.
  @Test
  void scope_explicitName_bindsSubjectName() {

    final var scopeName = cortex.name("cortex.test.scope");
    final var scope = cortex.scope(scopeName);

    assertNotNull(scope);
    assertEquals(scopeName, scope.subject().name());

    scope.close();

  }

  /// Scope#scope(Name) creates a named enclosed child.
  @SpecRef("9.2")
  @Test
  void scope_namedChild_bindsNameAndParent() {

    final var parent = cortex.scope();

    final var childName = cortex.name("cortex.test.child");
    final var child = parent.scope(childName);

    assertEquals(childName, child.subject().name());

    child.close();
    parent.close();

  }

  /// Nested Scope hierarchy preserves enclosure and closure structure.
  @SpecRef("9.2")
  @Test
  void scope_nestedHierarchy_preservesEnclosures() {

    final var root = cortex.scope();
    final var child = root.scope();
    final var grandchild = child.scope();

    assertTrue(child.within(root));
    assertTrue(grandchild.within(child));
    assertTrue(grandchild.within(root));

    grandchild.close();
    child.close();
    root.close();

  }

  /// Named Scope creation rejects absence.
  @SpecRef("15.2")
  @SuppressWarnings({"resource"})
  @Test
  void scope_nullName_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> cortex.scope(null)
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

  /// A Circuit subject exposes the Circuit classifier.
  @SpecRef({"4.3", "4.5"})
  @Test
  void subject_circuit_hasCircuitType() {

    final var circuit = cortex.circuit();

    try {

      assertEquals(Circuit.class, circuit.subject().type());

    } finally {

      circuit.close();

    }

  }

  /// A Cortex subject exposes the Cortex classifier.
  @SpecRef({"4.3", "4.5"})
  @Test
  void subject_cortex_hasCortexType() {

    assertEquals(Cortex.class, cortex.subject().type());

  }

  /// A Cortex subject carries a valid Name.
  @SpecRef("4.3")
  @Test
  void subject_cortex_hasName() {

    assertNotNull(cortex.subject().name());

  }

  // ===========================
  // Subscriber Tests
  // ===========================

  /// Cortex is identity-bearing and exposes a subject.
  @SpecRef({"3", "4.3"})
  @Test
  void subject_cortex_isPresent() {

    assertNotNull(cortex.subject());

  }

  /// Multiple Cortex-created Circuits have distinct subject identities.
  @SpecRef({"4.2", "4.3"})
  @Test
  void subject_multipleCircuits_haveDistinctIdentities() {

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

  /// The interned Cortex exposes one stable subject identity.
  @SpecRef("13")
  @Test
  void subject_repeatedCortexAccess_returnsSameInstance() {

    final var cortex1 = cortex();
    final var cortex2 = cortex();

    assertSame(cortex1.subject(), cortex2.subject());
    assertEquals(cortex1.subject().id(), cortex2.subject().id());

  }

  /// A Scope subject exposes the Scope classifier.
  @SpecRef({"4.3", "4.5"})
  @Test
  void subject_scope_hasScopeType() {

    final var scope = cortex.scope();

    assertEquals(Scope.class, scope.subject().type());

    scope.close();

  }

  /// A Subscriber subject exposes the Subscriber classifier.
  @SpecRef({"4.3", "4.5"})
  @Test
  void subject_subscriber_hasSubscriberType() {

    final var circuit = cortex.circuit();

    try {

      final Subscriber< String > subscriber =
        circuit.subscriber(
          cortex.name("subscriber.type.test"),
          (_, _) -> {
          }
        );

      assertEquals(Subscriber.class, subscriber.subject().type());

    } finally {

      circuit.close();

    }

  }

  /// Subscriber creation rejects an absent callback.
  @SpecRef("15.2")
  @Test
  void subscriber_nullCallback_throwsNullPointerException() {

    final var circuit = cortex.circuit();

    try {

      assertThrows(
        NullPointerException.class,
        () -> circuit.subscriber(
          cortex.name("test"),
          (BiConsumer< Subject< Pipe< String > >, Registrar< String > >) null
        )
      );

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Integration Tests
  // ===========================

  /// Subscriber creation rejects an absent Name.
  @SpecRef("15.2")
  @Test
  void subscriber_nullName_throwsNullPointerException() {

    final var circuit = cortex.circuit();

    try {

      assertThrows(
        NullPointerException.class,
        () -> circuit.subscriber(
          null,
          (_, _) -> {
          }
        )
      );

    } finally {

      circuit.close();

    }

  }

  /// Circuit creates a callback-backed Subscriber.
  @SpecRef("7.2")
  @Test
  void subscriber_validCallback_returnsSubscriber() {

    final var circuit = cortex.circuit();

    try {

      final var subscriberName = cortex.name("cortex.test.subscriber");

      final Subscriber< String > subscriber =
        circuit.subscriber(
          subscriberName,
          (_, _) -> {
            // Subscriber behavior
          }
        );

      assertNotNull(subscriber);
      assertEquals(subscriberName, subscriber.subject().name());

    } finally {

      circuit.close();

    }

  }

}
