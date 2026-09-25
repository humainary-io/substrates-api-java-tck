// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §6.3 and the Java [Circuit#pipe(List)] static fan-out projection.
///
/// Unlike conduit/subscriber fan-out (dynamic, wired via subscriptions), this fan-out
/// is fixed at creation: the targets are snapshotted, resolved once against the owning
/// circuit, and dispatched to in list order on every emission. This is the wiring
/// primitive for static network topologies (e.g. boolean networks).
///
/// Covers: ordering, duplicate handling, empty/single short-circuits, snapshot
/// isolation, null guards, cross-circuit dispatch, sibling isolation, and subject
/// inheritance.

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
@SuppressWarnings("DataFlowIssue")  // deliberate null-rejection coverage
final class PipeFanoutContractTest
  extends TestSupport {

  private Cortex cortex;

  /// A fan-out owned by one circuit dispatches to both same-circuit and
  /// cross-circuit targets, each receptor running on its own circuit.
  /// A fan-out Pipe dispatches to targets owned by different Circuits.
  @SpecRef("6.3")
  @Test
  void dispatch_crossCircuitTargets_deliversToEveryCircuit() {

    final var circuit1 = cortex.circuit();
    final var circuit2 = cortex.circuit();

    try {

      final List< Integer > received1 = new ArrayList<>();
      final List< Integer > received2 = new ArrayList<>();

      final Pipe< Integer > t1 = circuit1.pipe(received1::add);
      final Pipe< Integer > t2 = circuit2.pipe(received2::add);

      // Owned by circuit1: t1 is same-circuit, t2 is cross-circuit.
      final Pipe< Integer > fan =
        circuit1.pipe(
          List.of(t1, t2)
        );

      fan.emit(5);

      circuit1.await();
      circuit2.await();

      assertEquals(List.of(5), received1);
      assertEquals(List.of(5), received2);

    } finally {

      circuit1.close();
      circuit2.close();

    }

  }

  // ===========================
  // Fan-out Dispatch
  // ===========================

  /// A duplicate target entry receives the emission once per entry.
  /// Duplicate fan-out target entries create independent deliveries.
  @SpecRef("6.3")
  @Test
  void dispatch_duplicateTargets_deliversOncePerEntry() {

    final var circuit = cortex.circuit();

    try {

      final AtomicInteger counter = new AtomicInteger();

      final Pipe< Integer > target =
        circuit.pipe(_ -> counter.incrementAndGet());

      final Pipe< Integer > fan =
        circuit.pipe(
          List.of(target, target, target)
        );

      fan.emit(1);
      circuit.await();

      assertEquals(3, counter.get());

    } finally {

      circuit.close();

    }

  }

  /// A deep chain of fan-outs stays iterative, as §5.3 requires.
  ///
  /// A fan-out's targets must exist before it does, so fan-outs cannot form a
  /// cycle — but acyclic is not the same as shallow, and §5.3's "deeply
  /// cascading chains do not overflow the call stack" is about depth. A provider
  /// that reaches a fan-out's targets by invoking them on the dispatching stack
  /// rather than by queueing them recurses once per level here.
  ///
  /// The list has two elements deliberately: a single-element list is equivalent
  /// to `pipe(target)`, which SHOULD return a same-circuit target as-is (§14),
  /// so a chain built from one-element lists collapses to a single pipe and
  /// tests nothing.
  ///
  /// Asserted on the delivery count rather than on a thrown error, for the same
  /// reason as the other stack-safety tests here: a provider that isolates
  /// callback failures (§15.4) catches its own `StackOverflowError`, and the
  /// emission simply never arrives.
  @SpecRef({"5.3", "6.3"})
  @Test
  void dispatch_deeplyNestedFanouts_staysIterative() {

    final var circuit = cortex.circuit();

    try {

      final var delivered = new AtomicInteger();
      final int depth = 50_000;

      Pipe< Integer > chain = circuit.pipe(_ -> delivered.incrementAndGet());

      final Pipe< Integer > idle = circuit.pipe();

      for (int i = 0; i < depth; i++) {
        chain = circuit.pipe(List.of(chain, idle));
      }

      chain.emit(1);
      circuit.await();

      assertEquals(
        1,
        delivered.get(),
        "a deep fan-out chain must deliver without exhausting the stack"
      );

    } finally {

      circuit.close();

    }

  }

  /// List order holds when the targets are not all of one kind.
  ///
  /// §6.3 requires same-circuit targets to observe list order, and says nothing
  /// about how a provider represents each one. A conduit channel and a circuit
  /// pipe are the two kinds a provider is most likely to represent differently —
  /// a channel dispatches onward to its own subscribers, a circuit pipe runs a
  /// terminal receptor — so a provider that delivers one during the dispatch
  /// walk and the other after it passes every uniform ordering test here and
  /// still reorders this one.
  ///
  /// Both orderings are asserted: a list is not in order if only one arrangement
  /// of it happens to be.
  @SpecRef("6.3")
  @Test
  void dispatch_mixedTargetKinds_deliversInListOrder() {

    final var circuit = cortex.circuit();

    try {

      final List< String > trace = new ArrayList<>();

      final var conduit =
        circuit.conduit(
          cortex.name("channel"),
          Integer.class
        );

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("observer"),
          (_, registrar) ->
            registrar.register(
              v -> trace.add("channel:" + v)
            )
        )
      );

      final Pipe< Integer > channel = conduit.get(cortex.name("a"));
      final Pipe< Integer > direct = circuit.pipe(v -> trace.add("direct:" + v));

      circuit.pipe(List.of(channel, direct)).emit(1);
      circuit.await();

      assertEquals(
        List.of("channel:1", "direct:1"),
        trace,
        "channel listed first must be delivered first"
      );

      trace.clear();

      circuit.pipe(List.of(direct, channel)).emit(2);
      circuit.await();

      assertEquals(
        List.of("direct:2", "channel:2"),
        trace,
        "circuit pipe listed first must be delivered first"
      );

    } finally {

      circuit.close();

    }

  }

  /// Fan-out targets are delivered behind transit work accepted before the dispatch.
  ///
  /// §5.3 sequences what a circuit-context callback raises behind transit work
  /// already accepted for the current cascading chain, and the fan-out's own
  /// contract is that an emission enters the owning circuit's queue once and then
  /// dispatches to the targets. A receptor that emits to the fan-out and then to a
  /// circuit pipe has accepted the second before the fan-out dispatches, so the
  /// targets are delivered behind it.
  ///
  /// The consequence is asserted where it decides a result as well as an order:
  /// with a cell and a basin as targets, a direct update accepted before the
  /// dispatch must win the cell and lead the basin.
  @SpecRef({"5.3", "6.3"})
  @Test
  void dispatch_pendingTransitWork_precedesTargetDelivery() {

    final var circuit = cortex.circuit();

    try {

      final var trace = new ArrayList< String >();

      final Pipe< Integer > first = circuit.pipe(value -> trace.add("first:" + value));
      final Pipe< Integer > second = circuit.pipe(value -> trace.add("second:" + value));
      final Pipe< Integer > pending = circuit.pipe(value -> trace.add("pending:" + value));

      final var fanout = circuit.pipe(List.of(first, second));

      final Pipe< Integer > start =
        circuit.pipe(
          value -> {
            fanout.emit(value);
            pending.emit(value);
          }
        );

      start.emit(1);
      circuit.await();

      assertEquals(
        List.of("pending:1", "first:1", "second:1"),
        trace,
        "fan-out targets must be delivered behind work accepted before the dispatch"
      );

      final Cell< Integer > cell = circuit.cell(0);
      final Basin< Integer > basin = circuit.basin(8);

      final var holders = circuit.pipe(List.of(cell.pipe(), basin.pipe()));

      final Pipe< Integer > updates =
        circuit.pipe(
          _ -> {
            holders.emit(1);
            cell.pipe().emit(2);
            basin.pipe().emit(2);
          }
        );

      updates.emit(0);
      circuit.await();

      final var retained = new ArrayList< Integer >();
      basin.drain(circuit.pipe(retained::add));
      circuit.await();

      assertEquals(
        1,
        cell.get(),
        "the fanned-out update is applied after the update accepted before the dispatch"
      );

      assertEquals(
        List.of(2, 1),
        retained,
        "the basin retains the accepted update ahead of the fanned-out one"
      );

    } finally {

      circuit.close();

    }

  }

  /// Multiple emissions preserve both per-emission target order and emission order.
  /// Fan-out preserves target order across multiple emissions.
  @SpecRef({"5.3", "6.3"})
  @Test
  void dispatch_multipleEmissions_preservesTargetOrder() {

    final var circuit = cortex.circuit();

    try {

      final List< String > trace = new ArrayList<>();

      final Pipe< Integer > t1 = circuit.pipe(v -> trace.add("A:" + v));
      final Pipe< Integer > t2 = circuit.pipe(v -> trace.add("B:" + v));

      final Pipe< Integer > fan =
        circuit.pipe(
          List.of(t1, t2)
        );

      fan.emit(1);
      fan.emit(2);
      circuit.await();

      assertEquals(
        List.of("A:1", "B:1", "A:2", "B:2"),
        trace
      );

    } finally {

      circuit.close();

    }

  }

  /// Each emission reaches every target, in list order.
  /// Fan-out invokes all targets sequentially in list order.
  @SpecRef("6.3")
  @Test
  void dispatch_multipleTargets_deliversInListOrder() {

    final var circuit = cortex.circuit();

    try {

      final List< String > trace = new ArrayList<>();

      final Pipe< Integer > t1 = circuit.pipe(v -> trace.add("A:" + v));
      final Pipe< Integer > t2 = circuit.pipe(v -> trace.add("B:" + v));
      final Pipe< Integer > t3 = circuit.pipe(v -> trace.add("C:" + v));

      final Pipe< Integer > fan =
        circuit.pipe(
          List.of(t1, t2, t3)
        );

      fan.emit(7);
      circuit.await();

      assertEquals(
        List.of("A:7", "B:7", "C:7"),
        trace
      );

    } finally {

      circuit.close();

    }

  }

  /// `pipe(name, targets)` binds the name to the fan-out pipe's subject and
  /// dispatches to every target in list order.
  /// Named fan-out binds its name and dispatches targets in order.
  @Test
  void dispatch_namedFanout_bindsNameAndDeliversInOrder() {

    final var name = cortex.name("named.fanout");
    final var circuit = cortex.circuit();

    try {

      final List< String > trace = new ArrayList<>();

      final Pipe< Integer > t1 = circuit.pipe(v -> trace.add("A:" + v));
      final Pipe< Integer > t2 = circuit.pipe(v -> trace.add("B:" + v));

      final Pipe< Integer > fan = circuit.pipe(name, List.of(t1, t2));

      assertEquals(
        name.toString(),
        fan.subject().name().toString()
      );

      fan.emit(3);
      circuit.await();

      assertEquals(List.of("A:3", "B:3"), trace);

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Short-Circuits
  // ===========================

  /// The target list is snapshotted at creation; later mutation of the caller's
  /// list has no effect on the fan-out pipe.
  /// Fan-out snapshots its target list at Pipe creation.
  @Test
  void dispatch_targetListMutatedAfterCreation_usesOriginalSnapshot() {

    final var circuit = cortex.circuit();

    try {

      final List< String > trace = new ArrayList<>();

      final Pipe< Integer > t1 = circuit.pipe(v -> trace.add("A:" + v));
      final Pipe< Integer > t2 = circuit.pipe(v -> trace.add("B:" + v));
      final Pipe< Integer > t3 = circuit.pipe(v -> trace.add("C:" + v));

      final List< Pipe< Integer > > targets = new ArrayList<>();
      targets.add(t1);
      targets.add(t2);

      final Pipe< Integer > fan =
        circuit.pipe(targets);

      // Mutating the source list after creation must not rewire the pipe.
      targets.add(t3);
      targets.clear();

      fan.emit(9);
      circuit.await();

      assertEquals(
        List.of("A:9", "B:9"),
        trace
      );

    } finally {

      circuit.close();

    }

  }

  /// Work a fan-out target raises is delivered behind the remaining targets.
  ///
  /// §5.3 sequences what a callback raises behind transit work already
  /// accepted, and every target of one fan-out dispatch is accepted before any
  /// of them runs. So when the first target emits, that emission joins the back
  /// of transit, behind the second target's delivery, however a provider groups
  /// the targets internally.
  @SpecRef({"5.3", "6.3"})
  @Test
  void dispatch_targetEmission_followsRemainingTargets() {

    final var circuit = cortex.circuit();

    try {

      final List< String > trace = new ArrayList<>();

      final Pipe< Integer > raised =
        circuit.pipe(value -> trace.add("raised:" + value));

      final Pipe< Integer > first =
        circuit.pipe(
          value -> {
            trace.add("first:" + value);
            raised.emit(value);
          }
        );

      final Pipe< Integer > second =
        circuit.pipe(value -> trace.add("second:" + value));

      circuit.pipe(List.of(first, second)).emit(1);
      circuit.await();

      assertEquals(
        List.of("first:1", "second:1", "raised:1"),
        trace,
        "what a target raises must follow the targets accepted with it"
      );

    } finally {

      circuit.close();

    }

  }

  /// A target that throws when its emission is processed does not prevent
  /// delivery to its sibling targets.
  /// A failing fan-out target does not block sibling targets.
  @SpecRef({"6.3", "15.4"})
  @Test
  void dispatch_targetThrows_preservesSiblingDelivery() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > received = new ArrayList<>();

      final Pipe< Integer > good1 = circuit.pipe(v -> received.add(v * 10));
      final Pipe< Integer > bad = circuit.pipe(_ -> {
        throw new RuntimeException("boom");
      });
      final Pipe< Integer > good2 = circuit.pipe(v -> received.add(v * 100));

      final Pipe< Integer > fan =
        circuit.pipe(
          List.of(good1, bad, good2)
        );

      fan.emit(1);
      circuit.await();

      assertEquals(
        List.of(10, 100),
        received
      );

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Snapshot Semantics
  // ===========================

  /// An empty target list yields a working no-op pipe — emissions are queued and
  /// discarded without surfacing any exception (consistent with `circuit.pipe()`).
  /// An empty target list creates a no-op Pipe.
  @Test
  void pipe_emptyTargetList_returnsNoOpPipe() {

    final var circuit = cortex.circuit();

    try {

      final List< Pipe< Integer > > targets = List.of();

      final Pipe< Integer > fan =
        circuit.pipe(targets);

      assertNotNull(fan);
      assertNotNull(fan.subject());

      for (int i = 0; i < 10; i++) {
        fan.emit(i);
      }

      circuit.await();

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Null Guards
  // ===========================

  /// Fan-out rejects a foreign-provider target synchronously.
  @SpecRef({"15.1", "16.3"})
  @Test
  void pipe_foreignProviderTarget_throwsFault() {

    final var circuit = cortex.circuit();

    try {

      final var subject = circuit.< Integer > pipe().subject();
      final Pipe< Integer > foreign = new Pipe<>() {
        @Override
        public void emit(@NotNull final Integer emission) {
        }

        @NotNull
        @Override
        public Subject< Pipe< Integer > > subject() {

          return subject;

        }
      };

      assertThrows(Fault.class, () -> circuit.pipe(List.of(foreign)));

    } finally {

      circuit.close();

    }

  }

  /// A named empty-list fan-out mints a named no-op pipe rather than collapsing
  /// to the anonymous `pipe()` form.
  /// Named empty fan-out creates a named no-op Pipe.
  @Test
  void pipe_namedEmptyFanout_returnsNamedNoOp() {

    final var name = cortex.name("named.empty");
    final var circuit = cortex.circuit();

    try {

      final List< Pipe< Integer > > targets = List.of();

      final Pipe< Integer > fan = circuit.pipe(name, targets);

      assertEquals(
        name.toString(),
        fan.subject().name().toString()
      );

      // No targets: emissions are discarded without surfacing any exception.
      fan.emit(1);
      circuit.await();

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Cross-Circuit Dispatch
  // ===========================

  /// A null list / null element is rejected synchronously by the named fan-out
  /// overload.
  /// Named fan-out rejects an absent target list.
  @SpecRef("15.2")
  @Test
  void pipe_namedFanoutWithNullTargets_throwsNullPointerException() {

    final var name = cortex.name("named.null");
    final var circuit = cortex.circuit();

    try {

      assertThrows(
        NullPointerException.class,
        () -> circuit.pipe(name, (List< Pipe< Integer > >) null)
      );

      final List< Pipe< Integer > > targets = new ArrayList<>();
      targets.add(circuit.pipe(Receptor.of(Integer.class)));
      targets.add(null);

      assertThrows(
        NullPointerException.class,
        () -> circuit.pipe(name, targets)
      );

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Subject
  // ===========================

  /// A named single-target fan-out mints a named forwarder — it does not return
  /// the target itself the way the anonymous single-element `pipe(List)` does.
  /// Named single-target fan-out creates a named forwarding Pipe.
  @Test
  void pipe_namedSingleTarget_returnsNamedForwarder() {

    final var name = cortex.name("named.single");
    final var circuit = cortex.circuit();

    try {

      final List< Integer > received = new ArrayList<>();

      final Pipe< Integer > target = circuit.pipe(received::add);

      final Pipe< Integer > fan = circuit.pipe(name, List.of(target));

      assertNotSame(target, fan);
      assertEquals(
        name.toString(),
        fan.subject().name().toString()
      );

      fan.emit(8);
      circuit.await();

      assertEquals(List.of(8), received);

    } finally {

      circuit.close();

    }

  }

  // ===========================
  // Named Variants
  // ===========================

  /// A null name is rejected synchronously by the named fan-out overload.
  /// Named fan-out rejects an absent name.
  @SpecRef("15.2")
  @Test
  void pipe_nullFanoutName_throwsNullPointerException() {

    final var circuit = cortex.circuit();

    try {

      final Pipe< Integer > target = circuit.pipe(Receptor.of(Integer.class));

      assertThrows(
        NullPointerException.class,
        () -> circuit.pipe(null, List.of(target))
      );

    } finally {

      circuit.close();

    }

  }

  /// A null target list is rejected synchronously on the caller thread.
  /// Fan-out creation rejects an absent target list.
  @SpecRef("15.2")
  @Test
  void pipe_nullTargetList_throwsNullPointerException() {

    final var circuit = cortex.circuit();

    try {

      assertThrows(
        NullPointerException.class,
        () -> circuit.pipe((List< Pipe< Integer > >) null)
      );

    } finally {

      circuit.close();

    }

  }

  /// A single same-circuit target short-circuits to `pipe(target)`, which returns
  /// the target itself (same-circuit optimization) — no wrapping.
  /// Anonymous fan-out with one same-Circuit target reuses that target.
  @Test
  void pipe_singleSameCircuitTarget_returnsTargetItself() {

    final var circuit = cortex.circuit();

    try {

      final Pipe< Integer > target =
        circuit.pipe(Receptor.of(Integer.class));

      final Pipe< Integer > fan =
        circuit.pipe(
          List.of(target)
        );

      assertSame(target, fan);

    } finally {

      circuit.close();

    }

  }

  /// A null element in the target list is rejected synchronously on the caller thread.
  /// Fan-out creation rejects an absent target element.
  @SpecRef("15.2")
  @Test
  void pipe_targetListContainingNull_throwsNullPointerException() {

    final var circuit = cortex.circuit();

    try {

      final List< Pipe< Integer > > targets = new ArrayList<>();
      targets.add(circuit.pipe(Receptor.of(Integer.class)));
      targets.add(null);

      assertThrows(
        NullPointerException.class,
        () -> circuit.pipe(targets)
      );

    } finally {

      circuit.close();

    }

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

  /// The fan-out pipe's subject inherits the owning circuit's name and is
  /// parented by the circuit.
  /// A fan-out Pipe subject is enclosed by its creating Circuit.
  @SpecRef("4.3")
  @Test
  void subject_fanoutPipe_hasCircuitEnclosure() {

    final var circuitName = cortex.name("fanout.circuit");
    final var circuit = cortex.circuit(circuitName);

    try {

      final Pipe< Integer > t1 = circuit.pipe(Receptor.of(Integer.class));
      final Pipe< Integer > t2 = circuit.pipe(Receptor.of(Integer.class));

      final Pipe< Integer > fan =
        circuit.pipe(
          List.of(t1, t2)
        );

      final var subject = fan.subject();

      assertNotNull(subject);
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
