// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// SPEC §6.2.6 and Java-projection tests for contravariant Pipe attachment: the single-target
/// [Circuit#pipe(Pipe)], [Fiber#pipe(Pipe)], [Fiber#pipe(Cell)],
/// [Flow#pipe(Pipe)], [Flow#pipe(Cell)], [Fiber#tee(Pipe)], [Fiber#route(java.util.function.Predicate, Pipe)]
/// and [Circuit#sink(Pipe)] accept consumers of a supertype, matching [Registrar#register(Pipe)],
/// and [Conduit#pool(Flow)] accepts a flow producing a subtype.

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.15.1/SPEC.md")
final class PipeVarianceContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;
  private List< Number > seen;
  private Pipe< Number > numbers;

  /// Fiber#pipe accepts a Cell Pipe of a supertype.
  @SpecRef("6.2.6")
  @Test
  void pipe_fiberToSupertypeCell_compilesAndDelivers() {

    final Cell< Number > cell =
      circuit.cell(
        0
      );

    final Pipe< Integer > ints =
      cortex.fiber(Integer.class)
        .guard(v -> v > 0)
        .pipe(cell);

    ints.emit(-1);
    ints.emit(7);

    circuit.await();

    assertEquals(
      7,
      cell.get()
    );

  }

  /// Fiber#pipe accepts a supertype consumer.
  @SpecRef("6.2.6")
  @Test
  void pipe_fiberToSupertypeConsumer_compilesAndDelivers() {

    final Pipe< Integer > ints =
      cortex.fiber(Integer.class)
        .guard(v -> v > 0)
        .pipe(numbers);

    ints.emit(-1);
    ints.emit(2);

    circuit.await();

    assertEquals(
      List.of(2),
      seen
    );

  }

  /// Flow#pipe accepts a Cell Pipe of its output
  /// supertype.
  @SpecRef("6.2.6")
  @Test
  void pipe_flowToSupertypeCell_compilesAndDelivers() {

    final Cell< Number > cell =
      circuit.cell(
        0
      );

    final Pipe< String > strings =
      cortex.flow(String.class)
        .map(Integer::parseInt)
        .pipe(cell);

    strings.emit("42");

    circuit.await();

    assertEquals(
      42,
      cell.get()
    );

  }

  /// Flow#pipe accepts a supertype consumer.
  @SpecRef("6.2.6")
  @Test
  void pipe_flowToSupertypeConsumer_compilesAndDelivers() {

    final Pipe< String > strings =
      cortex.flow(String.class)
        .map(Integer::parseInt)
        .pipe(numbers);

    strings.emit("3");

    circuit.await();

    assertEquals(
      List.of(3),
      seen
    );

  }

  /// Fiber#tee accepts a supertype consumer for its side pipe.
  @Test
  void tee_supertypeConsumer_compilesAndDelivers() {

    final Pipe< Integer > ints =
      cortex.fiber(Integer.class)
        .tee(numbers)
        .pipe(circuit.pipe(_ -> { }));

    ints.emit(3);

    circuit.await();

    assertEquals(
      List.of(3),
      seen
    );

  }

  /// Fiber#route accepts a supertype consumer for its side pipe.
  @Test
  void route_supertypeConsumer_compilesAndDelivers() {

    final Pipe< Integer > ints =
      cortex.fiber(Integer.class)
        .route(v -> v > 5, numbers)
        .pipe(circuit.pipe(_ -> { }));

    ints.emit(3);
    ints.emit(9);

    circuit.await();

    assertEquals(
      List.of(9),
      seen
    );

  }

  /// Circuit#sink accepts an endpoint consuming a supertype of its captures.
  @Test
  void sink_supertypeEndpoint_compilesAndDelivers() {

    final List< Object > captured = new ArrayList<>();
    final Pipe< Object > objects = circuit.pipe(captured::add);
    final Sink< Integer > sink = circuit.sink(objects);

    sink.get(cortex.name("variance.sink")).emit(4);

    circuit.await();

    assertEquals(1, captured.size());
    assertEquals(4, ((Capture< ? >) captured.getFirst()).emission());

  }

  /// Conduit#pool accepts a flow whose output is a subtype of the conduit's type.
  @Test
  void pool_flowProducingSubtype_compilesAndDelivers() {

    final Conduit< Number > conduit = circuit.conduit(Number.class);
    conduit.subscribe(
      circuit.subscriber(
        cortex.name("variance.collector"),
        (_, registrar) -> registrar.register(numbers)
      )
    );

    final Pool< Pipe< String > > parsed =
      conduit.pool(cortex.flow(String.class).map(Integer::parseInt));

    parsed.get(cortex.name("variance.parsed")).emit("5");

    circuit.await();

    assertEquals(
      List.of(5),
      seen
    );

  }

  /// Generic narrowing preserves same-Circuit Pipe dispatch.
  @Test
  void pipe_narrowedSameCircuitTarget_preservesDispatch() {

    final Pipe< Integer > ints =
      circuit.pipe(
        numbers
      );

    assertSame(
      numbers,
      ints
    );

  }

  /// Circuit Pipe receptor overload accepts a supertype consumer.
  @Test
  void pipe_supertypeConsumer_compilesAndReceivesEmission() {

    final Pipe< Integer > ints =
      circuit.pipe(
        numbers
      );

    ints.emit(1);

    circuit.await();

    assertEquals(
      List.of(1),
      seen
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    circuit = cortex.circuit();
    seen = new ArrayList<>();

    numbers =
      circuit.pipe(
        seen::add
      );

  }

  @AfterEach
  void tearDown() {

    circuit.closeAwait();

  }

}
