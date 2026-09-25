// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for graphs of conduits on **one** circuit: SPEC §§5.3,
/// 6.3, 7.4 and 10.1.
///
/// The suite has same-circuit tests that mix pipe kinds, and a deep loop
/// through a single channel. Neither establishes an order for a graph: several
/// named conduits, paths that branch and rejoin, and different mechanisms at
/// the edges. That order is not a new guarantee — it falls out of FIFO transit
/// (§5.3) — but it falls out of it in a way no single-channel test exercises,
/// because a branch is the first place where two registrations each append to
/// transit and the appended work then has to interleave correctly with work
/// already there.
///
/// Values are immutable and tagged with the path they travelled, so each case
/// compares a complete expected trace rather than a count. A trace that agrees
/// only on its length cannot distinguish reconvergence from duplication.
///
/// Where the Java projection leaves an order unspecified, none is asserted. In
/// particular no case here demands an order between raw receptor callbacks and
/// pipe deliveries on the same channel: registrations within a case are all of
/// one kind, and the traces are defined at comparable endpoints.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class CrossConduitContractTest
  extends TestSupport {

  /// Every value a conduit's channels delivered, tagged with its path.
  private final List< String > trace = new CopyOnWriteArrayList<>();
  private Cortex cortex;
  private Circuit circuit;

  /// The same branch, with transit already pending ahead of it and an ingress
  /// marker admitted behind it.
  ///
  /// The whole graph must drain before the marker, and the work that was
  /// already pending when the branch was raised must drain before the branch.
  /// This is where a graph differs from a chain: a provider that gave freshly
  /// branched work any priority over work already accepted would reorder here
  /// and nowhere in a single-channel test.
  @SpecRef({"5.3", "6.3", "7.4"})
  @Test
  void branch_behindPendingTransit_keepsAdmissionOrder() {

    final var a = circuit.conduit(cortex.name("a"), String.class);
    final var b = circuit.conduit(cortex.name("b"), String.class);
    final var c = circuit.conduit(cortex.name("c"), String.class);

    record(b, "B");
    record(c, "C");

    final Pipe< String > pending =
      circuit.pipe((String value) -> trace.add("pending:" + value));

    final Pipe< String > marker =
      circuit.pipe((String value) -> trace.add("marker:" + value));

    a.subscribe(
      circuit.subscriber(
        cortex.name("a.branch"),
        (_, registrar) -> {
          registrar.register(circuit.pipe((String value) -> channel(b).emit(value)));
          registrar.register(circuit.pipe((String value) -> channel(c).emit(value)));
        }
      )
    );

    // One ingress item raises the pending work first, then the branch.
    circuit.pipe(
      (String value) -> {
        pending.emit(value);
        channel(a).emit(value);
      }
    ).emit("x");

    marker.emit("x");       // ingress, behind the whole cascade

    circuit.await();

    assertEquals(
      List.of("pending:x", "B:x", "C:x", "marker:x"),
      trace,
      "transit drains in admission order, and all of it before the ingress behind it"
    );

  }

  /// The same pipe registered twice on one channel is two registrations, and
  /// an emission travels both. Reconvergence must not collapse them.
  @SpecRef({"6.3", "7.4"})
  @Test
  void branch_duplicateRegistration_deliversOncePerOccurrence() {

    final var a = circuit.conduit(cortex.name("a"), String.class);
    final var d = circuit.conduit(cortex.name("d"), String.class);

    record(d, "D");

    final Pipe< String > toD =
      circuit.pipe((String value) -> channel(d).emit(value + ">D"));

    a.subscribe(
      circuit.subscriber(
        cortex.name("a.duplicate"),
        (_, registrar) -> {
          registrar.register(toD);
          registrar.register(toD);
        }
      )
    );

    channel(a).emit("x");
    circuit.await();

    assertEquals(
      List.of("D:x>D", "D:x>D"),
      trace,
      "the same pipe registered twice is reached twice"
    );

  }

  /// A branch whose edge filters everything delivers nothing, and its sibling
  /// is unaffected. A non-identity Fiber sits on both edges, so the case also
  /// covers a composed attachment rather than a bare pipe.
  @SpecRef({"6.2", "6.3", "7.4"})
  @Test
  void branch_filteredEdge_deliversNothingAndLeavesSiblingIntact() {

    final var a = circuit.conduit(cortex.name("a"), String.class);
    final var b = circuit.conduit(cortex.name("b"), String.class);
    final var c = circuit.conduit(cortex.name("c"), String.class);

    record(b, "B");
    record(c, "C");

    a.subscribe(
      circuit.subscriber(
        cortex.name("a.branch"),
        (_, registrar) -> {

          registrar.register(
            cortex.fiber(String.class)
              .guard(value -> value.startsWith("keep"))
              .pipe(circuit.pipe((String value) -> channel(b).emit(value)))
          );

          registrar.register(
            cortex.fiber(String.class)
              .guard(_ -> false)
              .pipe(circuit.pipe((String value) -> channel(c).emit(value)))
          );

        }
      )
    );

    channel(a).emit("keep.one");
    channel(a).emit("drop.this");
    channel(a).emit("keep.two");

    circuit.await();

    assertEquals(
      List.of("B:keep.one", "B:keep.two"),
      trace,
      "the surviving branch keeps exactly its matches, and the closed one yields nothing"
    );

  }

  /// `A → {B,C} → D`: both branches deliver, both reconverge, and every path
  /// delivers exactly once.
  ///
  /// The order is FIFO transit and nothing more. Processing A's channel runs
  /// its two registrations in registration order, appending B's emission and
  /// then C's; each of those appends its own emission to D behind the other's.
  /// So D receives B's before C's, and the branches do not interleave.
  @SpecRef({"5.3", "6.3", "7.4", "10.1"})
  @Test
  void branch_reconvergentPaths_deliverEveryPathOnce() {

    final var a = circuit.conduit(cortex.name("a"), String.class);
    final var b = circuit.conduit(cortex.name("b"), String.class);
    final var c = circuit.conduit(cortex.name("c"), String.class);
    final var d = circuit.conduit(cortex.name("d"), String.class);

    record(d, "D");

    forward(b, "b.to.d", circuit.pipe((String value) -> channel(d).emit(value + ">D")));
    forward(c, "c.to.d", circuit.pipe((String value) -> channel(d).emit(value + ">D")));

    a.subscribe(
      circuit.subscriber(
        cortex.name("a.branch"),
        (_, registrar) -> {
          registrar.register(circuit.pipe((String value) -> channel(b).emit(value + ">B")));
          registrar.register(circuit.pipe((String value) -> channel(c).emit(value + ">C")));
        }
      )
    );

    channel(a).emit("x");
    circuit.await();

    assertEquals(
      List.of("D:x>B>D", "D:x>C>D"),
      trace,
      "both branches reconverge, each exactly once, in the order they were registered"
    );

  }

  /// The one channel name every conduit in these cases uses, so that a
  /// provider keying anything by name alone has to collide.
  private Pipe< String > channel(
    final Conduit< String > conduit
  ) {

    return
      conduit.get(cortex.name("channel"));

  }

  /// Four conduits, one channel name, four separate channels. A provider that
  /// keyed a channel or its dispatch list by name rather than by conduit would
  /// deliver one emission to all four recorders.
  @SpecRef({"7.4", "10.1"})
  @Test
  void conduits_sharingOneChannelName_stayDistinct() {

    final var a = circuit.conduit(cortex.name("a"), String.class);
    final var b = circuit.conduit(cortex.name("b"), String.class);
    final var c = circuit.conduit(cortex.name("c"), String.class);
    final var d = circuit.conduit(cortex.name("d"), String.class);

    record(a, "A");
    record(b, "B");
    record(c, "C");
    record(d, "D");

    channel(b).emit("only");
    circuit.await();

    assertEquals(
      List.of("B:only"),
      trace,
      "a channel name is scoped to its conduit"
    );

  }

  /// Registers `pipe` on every channel of `conduit`, under a named subscriber.
  private void forward(
    final Conduit< String > conduit,
    final String name,
    final Pipe< String > pipe
  ) {

    conduit.subscribe(
      circuit.subscriber(
        cortex.name(name),
        (_, registrar) -> registrar.register(pipe)
      )
    );

  }

  /// A bounded loop through three conduits. The cycle is queued transit rather
  /// than recursion, so it terminates on its own counter and the ingress behind
  /// it waits for the whole thing.
  @SpecRef({"5.3", "6.1", "10.1"})
  @Test
  void loop_boundedAcrossConduits_runsAsQueuedTransit() {

    final var a = circuit.conduit(cortex.name("a"), String.class);
    final var b = circuit.conduit(cortex.name("b"), String.class);
    final var c = circuit.conduit(cortex.name("c"), String.class);

    final int laps = 3;

    forward(a, "a.to.b", circuit.pipe((String value) -> {
      trace.add("A:" + value);
      channel(b).emit(value);
    }));

    forward(b, "b.to.c", circuit.pipe((String value) -> {
      trace.add("B:" + value);
      channel(c).emit(value);
    }));

    forward(c, "c.to.a", circuit.pipe((String value) -> {

      trace.add("C:" + value);

      final var lap = Integer.parseInt(value);

      if (lap < laps) {
        channel(a).emit(String.valueOf(lap + 1));
      }

    }));

    final Pipe< String > marker =
      circuit.pipe((String value) -> trace.add("marker:" + value));

    channel(a).emit("1");
    marker.emit("after");

    circuit.await();

    assertEquals(
      List.of(
        "A:1", "B:1", "C:1",
        "A:2", "B:2", "C:2",
        "A:3", "B:3", "C:3",
        "marker:after"
      ),
      trace,
      "the loop completes every lap through every conduit before the ingress behind it"
    );

  }

  /// Records every emission reaching `conduit`, tagged with `label`.
  private void record(
    final Conduit< String > conduit,
    final String label
  ) {

    conduit.subscribe(
      circuit.subscriber(
        cortex.name(label + ".recorder"),
        (_, registrar) ->
          registrar.register((String value) -> trace.add(label + ":" + value))
      )
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    circuit = cortex.circuit(cortex.name("graph"));

  }

  @AfterEach
  void tearDown() {

    circuit.closeAwait();

  }

}
