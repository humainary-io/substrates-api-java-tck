// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §11.5 Port queued mutation, emission, ordering, non-immediacy,
/// failure isolation, identity, naming, validation, and capability boundaries.

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.2/SPEC.md")
final class PortContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;
  // ----- factory: subject and initialization -----

  /// Port emission composes with a Fiber operator pipeline.
  @Test
  void emit_fiberTarget_appliesFiberOperator() {

    final Port< Integer > port =
      circuit.port(0);

    final Cell< Integer > sink =
      circuit.cell(-1);

    final Pipe< Integer > target =
      cortex.fiber(Integer.class)
        .reduce(0, Integer::sum)
        .pipe(sink.pipe());

    port.replace(1);
    port.emit(target);
    port.replace(2);
    port.emit(target);
    port.replace(3);
    port.emit(target);

    circuit.await();

    // reduce sums: 1, 1+2=3, 3+3=6 → last published is 6
    assertEquals(6, sink.get());

  }

  /// Port emission composes with a type-changing Flow map.
  @Test
  void emit_flowTarget_appliesFlowMap() {

    final Port< Integer > port =
      circuit.port(5);

    final Cell< String > sink =
      circuit.cell("<empty>");

    final Pipe< Integer > target =
      cortex.flow(Integer.class)
        .map(n -> "value=" + n)
        .pipe(sink.pipe());

    port.emit(target);
    circuit.await();

    assertEquals("value=5", sink.get());

  }

  /// Port emit rejects a target from a foreign provider.
  @SpecRef({"11.5", "15.1"})
  @Test
  void emit_foreignProviderTarget_throwsFault() {

    final var port = circuit.port(0);
    final var subject = circuit.< Integer > pipe().subject();

    final Pipe< Integer > foreign = new Pipe<>() {
      @Override
      public void emit(
        final Integer emission
      ) {
      }

      @Override
      public Subject< Pipe< Integer > > subject() {
        return subject;
      }
    };

    assertThrows(Fault.class, () -> port.emit(foreign));

  }

  /// Port emit queues its current present value to the target pipe.
  @SpecRef("11.5")
  @Test
  void emit_initializedPort_forwardsCurrentValue() {

    final Port< Integer > port =
      circuit.port(13);

    final Cell< Integer > sink =
      circuit.cell(-1);

    port.emit(sink.pipe());
    circuit.await();

    assertEquals(13, sink.get());

  }

  /// Port emit synchronously rejects an absent target.
  @SpecRef({"11.5", "15.2"})
  @Test
  void emit_nullTarget_throwsNullPointerException() {

    final Port< Integer > port =
      circuit.port(0);

    assertThrows(
      NullPointerException.class,
      () -> port.emit(null)
    );

  }

  // ----- replace -----

  /// Port operations after Circuit close do not throw or take effect.
  @SpecRef({"9.3", "11.5"})
  @Test
  void operations_afterCircuitClose_areSilentlyDropped() {

    final var foreignCircuit = cortex.circuit();

    try {

      final var deliveries = new AtomicInteger();
      final var target = foreignCircuit.< Integer > pipe(_ -> deliveries.incrementAndGet());
      final var port = circuit.port(0);

      circuit.close();

      assertDoesNotThrow(() -> port.replace(1));
      assertDoesNotThrow(() -> port.update(value -> value + 1));
      assertDoesNotThrow(() -> port.update(1, Integer::sum));
      assertDoesNotThrow(() -> port.emit(target));

      foreignCircuit.await();
      assertEquals(0, deliveries.get());

    } finally {

      foreignCircuit.close();

    }

  }

  /// Port creation after Circuit close signals closed resource.
  @SpecRef({"9.1", "11.5"})
  @Test
  void port_afterCircuitClose_throwsFault() {

    final var owner = cortex.circuit();
    owner.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> owner.port(0)
      );

    assertSame(
      owner.subject(),
      fault.subject()
    );

  }

  /// Port is a Substrate but not a Pipe, Source, or Resource.
  @SpecRef({"11.0", "11.5"})
  @Test
  void port_capabilitySurface_excludesPipeSourceAndResource() {

    final Object port =
      circuit.port(0);

    assertInstanceOf(Substrate.class, port);
    assertFalse(port instanceof Pipe< ? >);
    assertFalse(port instanceof Receptor< ? >);

  }

  /// Named Port creation after Circuit close signals closed resource.
  @SpecRef({"9.1", "11.5"})
  @Test
  void port_namedAfterCircuitClose_throwsFault() {

    final var owner = cortex.circuit();
    final Name name = cortex.name("p");
    owner.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> owner.port(name, 0)
      );

    assertSame(
      owner.subject(),
      fault.subject()
    );

  }

  // ----- update(fn) -----

  /// Port factories reject required null arguments.
  @SpecRef({"11.5", "15.2"})
  @Test
  void port_nullRequiredArguments_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> circuit.port((Integer) null)
    );

    final Name name = cortex.name("p");

    assertThrows(
      NullPointerException.class,
      () -> circuit.port(null, 0)
    );

    assertThrows(
      NullPointerException.class,
      () -> circuit.port(name, null)
    );

  }

  /// Equal-name Port factories return distinct handles and identities.
  @SpecRef("11.5")
  @Test
  void port_repeatedNamedFactory_returnsDistinctHandles() {

    final Name name =
      cortex.name("ledger");

    final Port< Integer > a =
      circuit.port(name, 0);

    final Port< Integer > b =
      circuit.port(name, 0);

    assertNotSame(a, b);
    assertNotEquals(
      a.subject().id(),
      b.subject().id()
    );

  }

  /// Named Port creation binds the supplied subject name.
  @SpecRef("11.5")
  @Test
  void port_withExplicitName_bindsSuppliedSubjectName() {

    final Name name =
      cortex.name("ledger");

    final Port< Integer > port =
      circuit.port(name, 0);

    assertEquals(name, port.subject().name());

  }

  /// An unnamed Port uses its owning Circuit's name.
  @SpecRef("11.5")
  @Test
  void port_withoutExplicitName_usesCircuitName() {

    final Port< Integer > port =
      circuit.port(0);

    assertEquals(
      Port.class,
      port.subject().type()
    );

    assertEquals(
      circuit.subject().name(),
      port.subject().name()
    );

    assertTrue(
      port.subject().enclosure().isPresent()
    );

    port.subject().enclosure(
      parent -> assertEquals(
        circuit.subject().id(),
        parent.id()
      )
    );

  }

  /// Port replacement is queued and not synchronously visible to its caller.
  @SpecRef("11.5")
  @Test
  void replace_fromCallerContext_isNotAppliedInline() {

    final Port< Integer > port =
      circuit.port(0);

    final Cell< Integer > sink =
      circuit.cell(-1);

    // Replace without awaiting.
    port.replace(7);

    // Without await, downstream emission of the new value is not guaranteed.
    // Now flush.
    port.emit(sink.pipe());
    circuit.await();

    assertEquals(7, sink.get());

  }

  // ----- update(arg, fn) -----

  /// Port replacements are processed in accepted order.
  @SpecRef("11.5")
  @Test
  void replace_multipleAcceptedValues_appliesInOrder() {

    final Port< Integer > port =
      circuit.port(0);

    final Cell< Integer > sink =
      circuit.cell(-1);

    port.replace(1);
    port.replace(2);
    port.replace(3);

    port.emit(sink.pipe());
    circuit.await();

    assertEquals(3, sink.get());

  }

  /// Port replacement synchronously rejects absence.
  @SpecRef({"11.5", "15.2"})
  @Test
  void replace_nullValue_throwsNullPointerException() {

    final Port< Integer > port =
      circuit.port(0);

    assertThrows(
      NullPointerException.class,
      () -> port.replace(null)
    );

  }

  // ----- emit(pipe) -----

  @BeforeEach
  void setUp() {

    cortex = cortex();
    circuit = cortex.circuit();

  }

  @AfterEach
  void tearDown() {

    circuit.closeAwait();

  }

  /// The first Port transformation receives its seed value.
  @SpecRef("11.5")
  @Test
  void update_firstTransformation_receivesSeed() {

    final Port< Integer > port =
      circuit.port(42);

    final Cell< Integer > sink =
      circuit.cell(0);

    port.emit(sink.pipe());

    circuit.await();

    assertEquals(42, sink.get());

  }

  // ----- owner-context queueing (non-immediate) -----

  /// Owner-context Port transformation enters transit and is not inline.
  @SpecRef("11.5")
  @Test
  void update_fromOwnerContext_isNotAppliedInline() {

    final Port< Integer > port =
      circuit.port(0);

    final Cell< Integer > sink =
      circuit.cell(-1);

    final AtomicInteger ran =
      new AtomicInteger();

    final AtomicInteger observedInline =
      new AtomicInteger(-1);

    final Pipe< Integer > trigger =
      circuit.pipe((Integer next) -> {
        port.update(_ -> {
          ran.incrementAndGet();
          return next;
        });
        observedInline.set(ran.get());
      });

    trigger.emit(99);
    circuit.await();

    assertEquals(0, observedInline.get());
    assertEquals(1, ran.get());

    port.emit(sink.pipe());
    circuit.await();

    assertEquals(99, sink.get());

  }

  // ----- null arguments (caller-thread synchronous rejection) -----

  /// Argument transformations are processed in accepted order.
  @SpecRef("11.5")
  @Test
  void update_multipleArgumentTransforms_appliesInAcceptedOrder() {

    final Port< String > port =
      circuit.port("");

    final Cell< String > sink =
      circuit.cell("<empty>");

    port.update("a", String::concat);
    port.update("b", String::concat);
    port.update("c", String::concat);

    port.emit(sink.pipe());
    circuit.await();

    assertEquals("abc", sink.get());

  }

  /// Port transformations are processed in accepted order.
  @SpecRef("11.5")
  @Test
  void update_multipleTransforms_appliesInAcceptedOrder() {

    final Port< Integer > port =
      circuit.port(0);

    final Cell< Integer > sink =
      circuit.cell(-1);

    port.update(n -> n + 1);
    port.update(n -> n * 10);
    port.update(n -> n + 5);

    port.emit(sink.pipe());
    circuit.await();

    // ((0 + 1) * 10) + 5 = 15
    assertEquals(15, sink.get());

  }

  /// Port update synchronously rejects required null arguments.
  @SpecRef({"11.5", "15.2"})
  @Test
  void update_nullRequiredArguments_throwsNullPointerException() {

    final Port< Integer > port =
      circuit.port(0);

    assertThrows(
      NullPointerException.class,
      () -> port.update(null)
    );

    assertThrows(
      NullPointerException.class,
      () -> port.update(null, Integer::sum)
    );

    assertThrows(
      NullPointerException.class,
      () -> port.update(1, null)
    );

  }

  /// A failing transform does not stop subsequent circuit work.
  @SpecRef({"11.5", "15.4"})
  @Test
  void update_transformFails_preservesCircuitLiveness() {

    final Port< Integer > port =
      circuit.port(1);

    final Cell< Integer > sink =
      circuit.cell(-1);

    // Failure in the middle of a sequence.
    port.update(n -> n + 1);        // -> 2
    port.update(_ -> null);         // failure: stays 2
    port.update(_ -> {
      throw new RuntimeException();
    });                               // failure: stays 2
    port.update(n -> n * 10);       // -> 20

    port.emit(sink.pipe());
    circuit.await();

    assertEquals(20, sink.get());

  }

  // ----- closed-circuit Fault -----

  /// An absent transform result preserves the previous value.
  @SpecRef({"11.5", "15.4"})
  @Test
  void update_transformReturnsNull_preservesPreviousValue() {

    final Port< Integer > port =
      circuit.port(7);

    final Cell< Integer > sink =
      circuit.cell(-1);

    port.update(_ -> null);

    port.emit(sink.pipe());
    circuit.await();

    assertEquals(7, sink.get());
  }

  /// A throwing transform preserves the previous Port value.
  @SpecRef({"11.5", "15.4"})
  @Test
  void update_transformThrows_preservesPreviousValue() {

    final Port< Integer > port =
      circuit.port(7);

    final Cell< Integer > sink =
      circuit.cell(-1);

    port.update(_ -> {
      throw new RuntimeException("intentional");
    });

    port.emit(sink.pipe());
    circuit.await();

    assertEquals(7, sink.get());

  }

  /// Every Port transformation receives a present current value.
  @SpecRef("11.5")
  @Test
  void update_validTransform_receivesNonNullCurrentValue() {

    final Port< Integer > port =
      circuit.port(42);

    final AtomicInteger observed =
      new AtomicInteger();

    port.update(current -> {
      observed.set(current);
      return current;
    });

    circuit.await();

    assertEquals(42, observed.get());

  }

  /// Argument transformation receives current value and supplied argument.
  @SpecRef("11.5")
  @Test
  void update_withArgument_appliesFunctionToCurrentValue() {

    final Port< Integer > port =
      circuit.port(0);

    final Cell< Integer > sink =
      circuit.cell(-1);

    port.update(5, Integer::sum);
    port.update(3, Integer::sum);

    port.emit(sink.pipe());
    circuit.await();

    assertEquals(8, sink.get());

  }

}
