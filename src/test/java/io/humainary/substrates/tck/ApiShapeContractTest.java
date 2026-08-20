// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import io.humainary.substrates.api.*;
import org.junit.jupiter.api.*;

import java.lang.reflect.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for the SPEC §§6.2 and 16.2 API-shape requirements as projected into the
/// public Java API.
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.0/SPEC.md")
final class ApiShapeContractTest
  extends TestSupport {

  private static final Set< Class< ? > > REQUIRED_TYPES =
    Set.of(
      Bank.class,
      Basin.class,
      Capture.class,
      Cell.class,
      Change.class,
      Circuit.class,
      Closure.class,
      Conduit.class,
      Cortex.class,
      Current.class,
      Extent.class,
      Fiber.class,
      Flow.class,
      Id.class,
      Lookup.class,
      Name.class,
      Pin.class,
      Pipe.class,
      Pool.class,
      Port.class,
      Pulse.class,
      Receptor.class,
      Registrar.class,
      Resource.class,
      Routing.class,
      Run.class,
      Scope.class,
      Sink.class,
      Slot.class,
      Source.class,
      State.class,
      Subject.class,
      Subscriber.class,
      Subscription.class,
      Ticker.class,
      Window.class
    );

  private static void recordRealization(
    final Set< Class< ? > > realized,
    final Class< ? > contract,
    final Object value
  ) {

    assertInstanceOf(contract, value, contract.getSimpleName());
    realized.add(contract);

  }

  /// Subject-aware Flow configuration is exposed through the canonical
  /// `fiber(factory)` and `flow(factory)` composition primitives, not per-operator overloads.
  @SpecRef("6.2")
  @Test
  void flow_subjectAwareConfiguration_exposesCompositionFactoriesOnly() {

    for (final var method : Flow.class.getDeclaredMethods()) {
      if (method.getName().equals("fiber") || method.getName().equals("flow")) {
        continue;
      }

      for (final var parameter : method.getGenericParameterTypes()) {
        assertFalse(
          parameter.getTypeName().contains("Substrates$Subject"),
          method.toGenericString()
        );
      }
    }

  }

  /// Receptor consumption is exposed as `receive`, and
  /// abstract State iteration is exposed through Java Iterable.
  @SpecRef("16.3")
  @Test
  void requiredOperations_javaProjection_exposesCallbackAndIterationMappings() {

    final var observed = new AtomicInteger();
    final Receptor< Integer > receptor = observed::set;

    receptor.receive(7);

    final var state =
      cortex().state().state(
        cortex().name("value"),
        1
      );

    assertEquals(7, observed.get());
    assertTrue(state.iterator().hasNext());

  }

  /// Each required type is obtained through public API rather than implementation classes.
  /// Callback-scoped and emitted values are included because they are easy to omit from
  /// a factory-only audit.
  ///
  /// The configured provider supplies concrete realizations reachable through
  /// the public API for the complete required-type inventory.
  @SpecRef("16.2")
  @Test
  void requiredTypes_configuredProvider_realizesCompleteInventory() {

    final Set< Class< ? > > realized = new HashSet<>();
    final var cortex = cortex();
    final var circuit = cortex.circuit();

    try (var scope = cortex.scope()) {

      final var name = cortex.name("api.shape.realization");
      final var state = cortex.state();
      final var slot = cortex.slot(name, 1);
      final var subject = cortex.subject();
      final var conduit = circuit.conduit(Integer.class);
      final var bank = circuit.bank(Integer.class);
      final var basin = circuit.< Integer > basin(2);
      final var cell = circuit.cell(0);
      final var port = circuit.port(0);
      final var pin = circuit.pin(0);
      final Pipe< Integer > pipe = circuit.pipe();
      final var ticker =
        circuit.ticker(
          Duration.ofDays(1L),
          circuit.pipe()
        );
      final Pool< Name > pool = cortex.pool(candidate -> candidate);
      final Receptor< Integer > receptor = Receptor.of();
      final Fiber< Integer > fiber = cortex.fiber(Integer.class);
      final Flow< Integer, Integer > flow = cortex.flow(Integer.class);

      recordRealization(realized, Cortex.class, cortex);
      recordRealization(realized, Circuit.class, circuit);
      recordRealization(realized, Conduit.class, conduit);
      recordRealization(realized, Source.class, conduit);
      recordRealization(realized, Bank.class, bank);
      recordRealization(realized, Basin.class, basin);
      recordRealization(realized, Cell.class, cell);
      recordRealization(realized, Port.class, port);
      recordRealization(realized, Pin.class, pin);
      recordRealization(realized, Pipe.class, pipe);
      recordRealization(realized, Ticker.class, ticker);
      recordRealization(realized, Lookup.class, pool);
      recordRealization(realized, Pool.class, pool);
      recordRealization(realized, Receptor.class, receptor);
      recordRealization(realized, Fiber.class, fiber);
      recordRealization(realized, Flow.class, flow);
      recordRealization(realized, Routing.class, Routing.PIPE);
      recordRealization(realized, Current.class, cortex.current());
      recordRealization(realized, Name.class, name);
      recordRealization(realized, State.class, state);
      recordRealization(realized, Slot.class, slot);
      recordRealization(realized, Subject.class, subject);
      recordRealization(realized, Id.class, subject.id());
      recordRealization(realized, Extent.class, subject);
      recordRealization(realized, Scope.class, scope);
      recordRealization(realized, Resource.class, circuit);
      recordRealization(realized, Pulse.class, circuit.pulse().orElseThrow());

      final var subscriber =
        circuit.< Integer > subscriber(
          cortex.name("api.shape.subscriber"),
          (_, registrar) -> {
            recordRealization(realized, Registrar.class, registrar);
            registrar.register(receptor);
          }
        );
      final var subscription = conduit.subscribe(subscriber);
      final var closure = scope.closure(subscription);

      recordRealization(realized, Subscriber.class, subscriber);
      recordRealization(realized, Subscription.class, subscription);
      recordRealization(realized, Closure.class, closure);

      final Pipe< Capture< Integer > > captureEndpoint =
        circuit.pipe(capture -> recordRealization(realized, Capture.class, capture));
      final var sink = circuit.sink(captureEndpoint);
      recordRealization(realized, Sink.class, sink);

      final Pipe< Integer > runHead =
        flow.run().pipe(
          circuit.pipe(run -> recordRealization(realized, Run.class, run))
        );
      final Pipe< Integer > changeHead =
        flow.change().pipe(
          circuit.pipe(change -> recordRealization(realized, Change.class, change))
        );
      final Pipe< Integer > windowHead =
        flow.window(2).pipe(
          circuit.pipe(window -> recordRealization(realized, Window.class, window))
        );

      sink.get(name).emit(1);
      conduit.get(name).emit(1);
      runHead.emit(1);
      changeHead.emit(1);
      changeHead.emit(2);
      windowHead.emit(1);
      circuit.await();

      assertEquals(REQUIRED_TYPES, realized);

    } finally {

      circuit.closeAwait();

    }

  }

  /// All required classifiers are distinct
  /// public members of the Substrates API.
  @SpecRef({"4.5", "16.2"})
  @Test
  void requiredTypes_javaApi_exposesPublicInventory() {

    assertEquals(36, REQUIRED_TYPES.size());

    for (final var type : REQUIRED_TYPES) {
      assertSame(Substrates.class, type.getEnclosingClass(), type.getSimpleName());
      assertTrue(Modifier.isPublic(type.getModifiers()), type.getSimpleName());
      assertTrue(Modifier.isStatic(type.getModifiers()), type.getSimpleName());
    }

  }

  /// Required Resource types satisfy the Java Resource role.
  @SpecRef("16.2")
  @Test
  void resource_capabilityMatrix_assignsRequiredTypes() {

    final Set< Class< ? > > resources =
      Set.of(
        Bank.class,
        Circuit.class,
        Conduit.class,
        Source.class,
        Subscriber.class,
        Subscription.class,
        Ticker.class
      );

    for (final var type : resources) {
      assertTrue(Resource.class.isAssignableFrom(type), type.getSimpleName());
    }

  }

  /// Non-Resource components do not acquire the
  /// Resource role, while Scope binds closeability directly to AutoCloseable.
  @SpecRef("16.2")
  @Test
  void resource_nonResourceComponents_preserveCapabilityBoundaries() {

    final Set< Class< ? > > nonResources =
      Set.of(
        Basin.class,
        Cell.class,
        Current.class,
        Pin.class,
        Pipe.class,
        Port.class,
        Scope.class,
        Sink.class
      );

    for (final var type : nonResources) {
      assertFalse(Resource.class.isAssignableFrom(type), type.getSimpleName());
    }

    assertTrue(AutoCloseable.class.isAssignableFrom(Scope.class));

  }

  /// Source capability belongs to Source and Conduit but not Bank or Sink.
  @SpecRef("16.2")
  @Test
  void source_capabilityMatrix_matchesRequiredTypes() {

    assertTrue(Source.class.isAssignableFrom(Conduit.class));
    assertFalse(Source.class.isAssignableFrom(Bank.class));
    assertFalse(Source.class.isAssignableFrom(Sink.class));

  }

  /// Every type marked Substrate satisfies the Java Substrate role.
  @SpecRef("16.2")
  @Test
  void substrate_capabilityMatrix_assignsRequiredTypes() {

    final Set< Class< ? > > substrates =
      Set.of(
        Bank.class,
        Basin.class,
        Cell.class,
        Circuit.class,
        Conduit.class,
        Cortex.class,
        Current.class,
        Pin.class,
        Pipe.class,
        Port.class,
        Scope.class,
        Sink.class,
        Source.class,
        Subscriber.class,
        Subscription.class,
        Ticker.class
      );

    for (final var type : substrates) {
      assertTrue(Substrate.class.isAssignableFrom(type), type.getSimpleName());
    }

  }

  /// Equivalent classifiers obtained independently compare as equal through
  /// the Java projection's ordinary value-equality mechanism.
  @SpecRef("4.5")
  @Test
  void type_equivalentClassifiers_compareEqual() {

    final var first = cortex().circuit();
    final var second = cortex().circuit();

    try {

      assertEquals(Circuit.class, first.subject().type());
      assertEquals(first.subject().type(), second.subject().type());

      final var firstSlot = cortex().slot(cortex().name("type.first"), 1);
      final var secondSlot = cortex().slot(cortex().name("type.second"), 2);

      assertEquals(int.class, firstSlot.type());
      assertEquals(firstSlot.type(), secondSlot.type());

    } finally {

      first.closeAwait();
      second.closeAwait();

    }

  }

  /// Every time-bounded Window factory also requires an explicit capacity;
  /// the public Flow API exposes no unbounded duration-only overload.
  @SpecRef("6.2.3")
  @Test
  void window_timeBoundedFactory_requiresExplicitCapacity() {

    for (final var method : Flow.class.getDeclaredMethods()) {
      if (!method.getName().equals("window")) {
        continue;
      }

      final var parameters = method.getParameterTypes();
      assertFalse(
        parameters.length==1 && parameters[0]==Duration.class,
        method.toGenericString()
      );
    }

  }

}
