// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
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

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.2/SPEC.md")
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
        public void emit(final Integer emission) {
        }

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
