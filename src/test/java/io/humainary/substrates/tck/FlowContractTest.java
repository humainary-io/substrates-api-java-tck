// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §§6.2–6.2.6 and 6.4 Flow transformation,
/// composition, stateful operators, Window views, attachment isolation, validation, and temporal
/// enforcement.
///
/// Flow holds only the operators that cross type boundaries:
/// - `map(Function)` — appends a type-changing stage
/// - `fiber(Fiber)` — attaches a same-type [Fiber] at the output side
/// - `flow(Flow)` — composes with another Flow (optionally type-changing)
///
/// All per-emission operators (guard, diff, peek, reduce, ...) live on [Fiber]
/// and are exercised in [FiberContractTest].
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.1/SPEC.md")
final class FlowContractTest
  extends TestSupport {

  private Cortex cortex;

  private static < E > List< E > values(
    final Window< E > window
  ) {

    final List< E > values =
      new ArrayList<>();

    window.forEach(
      values::add
    );

    return
      values;

  }


  // ============================================================
  // Cortex factories
  // ============================================================

  /// Cortex#flow(Class) accepts a type witness.
  @Test
  void flow_classWitness_returnsFlow() {

    final Flow< Integer, Integer > f = cortex.flow(Integer.class);
    assertEquals(cortex.< Integer > flow().getClass(), f.getClass());

  }

  /// Cortex#flow returns an identity Flow.
  @SpecRef("6.2")
  @Test
  void flow_cortexFactory_returnsIdentityFlow() {

    final Flow< Integer, Integer > f = cortex.flow();
    assertEquals(f.getClass(), cortex.< Integer > flow().getClass());

  }

  /// Verifies that `flow.flow(identity)` and `identity.flow(flow)` produce the
  /// same emission behavior as the base flow alone — exercises the identity
  /// short-circuits that skip `Composed` wrapper construction.
  /// Identity Flow preserves emission values.
  @SpecRef("6.2")
  @Test
  void flow_identityMaterialization_preservesValues() {

    final var circuit = cortex.circuit();

    try {

      final Flow< Integer, Integer > identity = cortex.flow(Integer.class);

      // base: maps x -> x + 1
      final Flow< Integer, Integer > base =
        cortex.flow(Integer.class).map(x -> x + 1);

      // Identity on the right: base.flow(identity) ≡ base
      final Flow< Integer, Integer > rhs = base.flow(identity);

      // Identity on the left: identity.flow(base) ≡ base
      final Flow< Integer, Integer > lhs = identity.flow(base);

      final List< Integer > baseOut = new ArrayList<>();
      final List< Integer > rhsOut = new ArrayList<>();
      final List< Integer > lhsOut = new ArrayList<>();

      base.pipe(circuit.pipe(baseOut::add)).emit(10);
      rhs.pipe(circuit.pipe(rhsOut::add)).emit(10);
      lhs.pipe(circuit.pipe(lhsOut::add)).emit(10);

      circuit.await();

      assertEquals(List.of(11), baseOut);
      assertEquals(baseOut, rhsOut, "flow.flow(identity) must match base");
      assertEquals(baseOut, lhsOut, "identity.flow(flow) must match base");

    } finally {

      circuit.close();

    }

  }


  // ============================================================
  // flow — composition of two flows
  // ============================================================

  /// One stateful Flow value may be materialized across Circuits;
  /// each materialization owns its operator state and executes in its owning Circuit context.
  @SpecRef({"5.1", "6.2"})
  @Test
  void flow_sameValueAcrossCircuits_isolatesStateAndUsesOwningContexts() {

    final var first = cortex.circuit();
    final var second = cortex.circuit();

    try {

      final var firstContext = new AtomicReference< Current >();
      final var secondContext = new AtomicReference< Current >();

      final Flow< Integer, Integer > shared =
        cortex.flow(Integer.class)
          .fiber(
            cortex.fiber(Integer.class)
              .diff()
              .peek(value -> {
                if (value < 10) {
                  firstContext.set(cortex.current());
                } else {
                  secondContext.set(cortex.current());
                }
              })
          );

      final List< Integer > firstValues = new ArrayList<>();
      final List< Integer > secondValues = new ArrayList<>();

      final var firstPipe = shared.pipe(first.pipe(firstValues::add));
      final var secondPipe = shared.pipe(second.pipe(secondValues::add));

      firstPipe.emit(1);
      firstPipe.emit(1);
      secondPipe.emit(10);
      secondPipe.emit(10);

      first.await();
      second.await();

      assertEquals(List.of(1), firstValues);
      assertEquals(List.of(10), secondValues);
      assertSame(first.current(), firstContext.get());
      assertSame(second.current(), secondContext.get());

    } finally {

      first.close();
      second.close();

    }

  }

  /// Flow wrapping a Fiber executes it before downstream delivery.
  @SpecRef("6.2.6")
  @Test
  void flow_wrappedFiber_executesBeforeDownstream() {

    final var circuit = cortex.circuit();

    try {

      final List< Integer > captured = new ArrayList<>();

      final var fiber =
        cortex.fiber(Integer.class)
          .guard(v -> v > 0)
          .peek(captured::add);

      final Flow< Integer, Integer > flow = cortex.flow(fiber);

      final Pipe< Integer > sink = circuit.pipe();
      final Pipe< Integer > target = flow.pipe(sink);

      target.emit(-1);
      target.emit(2);
      target.emit(-3);
      target.emit(4);

      circuit.await();

      assertEquals(List.of(2, 4), captured);

    } finally {

      circuit.close();

    }

  }


  // ============================================================
  // map — type-changing transformation
  // ============================================================

  /// Flow attachment rejects Pipe and Cell targets from an
  /// incompatible provider.
  @SpecRef({"6.2.6", "15.1"})
  @Test
  void pipe_foreignTargets_throwFault() {

    final Pipe< Integer > foreignPipe = foreignProviderStub(Pipe.class);
    final Cell< Integer > foreignCell = foreignProviderStub(Cell.class);
    final var flow = cortex.flow(Integer.class);

    assertThrows(Fault.class, () -> flow.pipe(foreignPipe));
    assertThrows(Fault.class, () -> flow.pipe(foreignCell));

  }

  /// Flow attachment rejects absent Pipe and Cell targets.
  @SpecRef("15.2")
  @Test
  void pipe_nullTargets_throwNullPointerException() {

    final var flow = cortex.flow(Integer.class);

    assertThrows(
      NullPointerException.class,
      () -> flow.pipe((Pipe< Integer >) null)
    );

    assertThrows(
      NullPointerException.class,
      () -> flow.pipe((Cell< Integer >) null)
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

  @Nested
  final class ChangeOperator {

    /// The terminal length a [Change] carries equals the length [Flow#run()]
    /// would have reported on the admission immediately before the boundary.
    /// Change terminal length equals the run preceding the boundary.
    @SpecRef("6.2.3")
    @Test
    void change_afterRun_reportsTerminalRunLength() {

      final var circuit = cortex.circuit();

      try {

        final List< Long > runLengths = new ArrayList<>();
        final List< Long > changeLengths = new ArrayList<>();

        final Pipe< Integer > runHead =
          cortex.flow(Integer.class)
            .run().map(Run::length)
            .pipe(circuit.pipe(runLengths::add));

        final Pipe< Integer > changeHead =
          cortex.flow(Integer.class)
            .change().map(Change::length)
            .pipe(circuit.pipe(changeLengths::add));

        for (final int v : new int[]{1, 1, 1, 2, 2, 3}) {

          runHead.emit(v);
          changeHead.emit(v);

        }

        circuit.await();

        // run reports a length on every admission; change reports only the
        // terminal length of each closed run (3 for the 1-run, 2 for the 2-run).
        assertEquals(List.of(1L, 2L, 3L, 1L, 2L, 1L), runLengths);
        assertEquals(List.of(3L, 2L), changeLengths);

      } finally {

        circuit.close();

      }

    }

    /// The first admission opens the first run and emits nothing.
    /// Change emits nothing for the first admission.
    @SpecRef("6.2.3")
    @Test
    void change_firstAdmission_emitsNothing() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .change()
            .map(c -> c.from() + "->" + c.to() + ":" + c.length())
            .pipe(circuit.pipe(captured::add));

        head.emit(1);

        circuit.await();

        assertEquals(List.of(), captured);

      } finally {

        circuit.close();

      }

    }

    /// Each emitted Change is a fresh immutable envelope: references retained
    /// across later boundaries keep their captured from/to/length.
    /// Change emits an immutable retainable snapshot.
    @SpecRef("6.2.3")
    @Test
    void change_retainedSnapshot_remainsImmutable() {

      final var circuit = cortex.circuit();

      try {

        final List< Change< Integer > > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .change()
            .pipe(circuit.pipe(captured::add));

        head.emit(1);  // open run
        head.emit(1);  // 1-run length 2
        head.emit(2);  // boundary 1->2 (len 2)
        head.emit(3);  // boundary 2->3 (len 1)

        circuit.await();

        assertEquals(2, captured.size());
        assertEquals(1, captured.getFirst().from());
        assertEquals(2, captured.get(0).to());
        assertEquals(2L, captured.get(0).length());
        assertEquals(2, captured.get(1).from());
        assertEquals(3, captured.get(1).to());
        assertEquals(1L, captured.get(1).length());

      } finally {

        circuit.close();

      }

    }

    /// Fires only at a boundary, carrying the closed value, the opening value,
    /// and the closed run's terminal length. The open final run is never
    /// reported.
    /// Change emits boundary values and terminal run length.
    @SpecRef("6.2.3")
    @Test
    void change_valueBoundary_emitsFromToAndLength() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .change()
            .map(c -> c.from() + "->" + c.to() + ":" + c.length())
            .pipe(circuit.pipe(captured::add));

        head.emit(1);  // open run 1
        head.emit(1);  // 1-run length 2
        head.emit(1);  // 1-run length 3
        head.emit(2);  // boundary: 1->2 (1 ran 3)
        head.emit(2);  // 2-run length 2
        head.emit(3);  // boundary: 2->3 (2 ran 2)

        circuit.await();   // final 3-run (length 1) never reported

        assertEquals(
          List.of("1->2:3", "2->3:2"),
          captured
        );

      } finally {

        circuit.close();

      }

    }

  }

  @Nested
  final class FiberOperator {

    /// An attached Fiber observes values before the next Flow segment.
    @SpecRef("6.2.6")
    @Test
    void fiber_beforeFlow_observesIntermediateValue() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        // Peek placed INSIDE the fiber captures post-fiber output.
        final var fiber =
          cortex.fiber(Integer.class)
            .guard(v -> v > 0)
            .diff()
            .peek(captured::add);

        final Flow< Integer, Integer > flow =
          cortex.flow(Integer.class).fiber(fiber);

        final Pipe< Integer > sink = circuit.pipe();
        final Pipe< Integer > target = flow.pipe(sink);

        target.emit(1);
        target.emit(1);   // diff dropped
        target.emit(-2);  // guard dropped
        target.emit(3);
        target.emit(3);   // diff dropped

        circuit.await();

        assertEquals(List.of(1, 3), captured);

      } finally {

        circuit.close();

      }

    }

    /// Flow Fiber composition executes before a following map.
    @SpecRef("6.2.6")
    @Test
    void fiber_beforeMap_executesInOrder() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< String > target = circuit.pipe(captured::add);

        // flow(Integer).fiber(Fiber<Integer>) runs the fiber on the Integer
        // output; the subsequent map(Integer → String) appends after the fiber.
        // Emission order: emit(I) → fiber guard → map → target.
        final Pipe< Integer > piped =
          cortex.flow(Integer.class)
            .fiber(cortex.fiber(Integer.class).guard(v -> v > 0))
            .map(i -> "v:" + i)
            .pipe(target);

        piped.emit(1);
        piped.emit(-2);  // fiber drops
        piped.emit(3);

        circuit.await();

        assertEquals(List.of("v:1", "v:3"), captured);

      } finally {

        circuit.close();

      }

    }

    /// Flow Fiber attachment rejects an incompatible provider.
    @SpecRef("15.1")
    @Test
    void fiber_foreignArgument_throwsFault() {

      final Fiber< Integer > foreign = foreignProviderStub(Fiber.class);

      assertThrows(
        Fault.class,
        () -> cortex.flow(Integer.class).fiber(foreign)
      );

    }

    /// Flow#fiber rejects an absent Fiber.
    @SpecRef("15.2")
    @Test
    void fiber_nullArgument_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).fiber((Fiber< Integer >) null)
      );

    }

  }

  @Nested
  final class FiberSubjectiveOperator {

    /// Subject-aware Fiber factory receives the downstream subject.
    @SpecRef("6.2")
    @Test
    void fiberFactory_atAttachment_receivesDownstreamSubject() {

      final var circuit = cortex.circuit();

      try {

        final var conduit = circuit.conduit(Integer.class);
        final var name = cortex.name("config.lookup.key");
        final var sink = conduit.get(name);

        final List< Name > seenNames = new ArrayList<>();

        cortex.flow(Integer.class)
          .fiber(subject -> {
            seenNames.add(subject.name());
            return cortex.fiber(Integer.class);
          })
          .pipe(sink);

        assertEquals(List.of(name), seenNames);

      } finally {

        circuit.close();

      }

    }

    /// Fiber factory is invoked once for each materialization.
    @SpecRef({"6.2", "6.2.6"})
    @Test
    void fiberFactory_eachAttachment_invokesOnce() {

      final var circuit = cortex.circuit();

      try {

        final var counter = new int[]{0};

        final Flow< Integer, Integer > flow =
          cortex.flow(Integer.class)
            .fiber(_ -> {
              counter[0]++;
              return cortex.fiber(Integer.class);
            });

        flow.pipe(circuit.pipe());
        flow.pipe(circuit.pipe());
        flow.pipe(circuit.pipe());

        assertEquals(3, counter[0]);

      } finally {

        circuit.close();

      }

    }

    /// An attachment-time Fiber factory may allocate fresh mutable state that
    /// remains private to the resulting materialization.
    @SpecRef("6.2")
    @Test
    void fiberFactory_freshMutableStatePerAttachment_remainsIsolated() {

      final var circuit = cortex.circuit();

      try {

        final List< List< Integer > > attachmentObservations = new ArrayList<>();

        final Flow< Integer, Integer > flow =
          cortex.flow(Integer.class)
            .fiber(_ -> {
              final List< Integer > local = new ArrayList<>();
              attachmentObservations.add(local);
              return cortex.fiber(Integer.class).peek(local::add);
            });

        final var first = flow.pipe(circuit.pipe());
        final var second = flow.pipe(circuit.pipe());

        first.emit(1);
        second.emit(2);
        circuit.await();

        assertEquals(
          List.of(List.of(1), List.of(2)),
          attachmentObservations
        );

      } finally {

        circuit.close();

      }

    }

    /// Subject-aware Fiber factory creates independent attachment
    /// state.
    @SpecRef({"6.2", "6.2.3"})
    @Test
    void fiberFactory_multipleAttachments_isolatesState() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > a = new ArrayList<>();
        final List< Integer > b = new ArrayList<>();

        // Same factory used for both attachments — each must produce a
        // fresh fiber recipe and each materialization must hold its own state.
        final Flow< Integer, Integer > flow =
          cortex.flow(Integer.class)
            .fiber(_ -> cortex.fiber(Integer.class).diff());

        final Pipe< Integer > pa = flow.pipe(circuit.pipe(a::add));
        final Pipe< Integer > pb = flow.pipe(circuit.pipe(b::add));

        pa.emit(1);
        pa.emit(1);   // dropped by pa's diff
        pa.emit(2);

        pb.emit(1);   // not dropped: pb has independent diff state
        pb.emit(1);   // dropped
        pb.emit(2);

        circuit.await();

        assertEquals(List.of(1, 2), a);
        assertEquals(List.of(1, 2), b);

      } finally {

        circuit.close();

      }

    }

    /// Subject-aware Fiber factory overload rejects absence.
    @SpecRef("15.2")
    @Test
    void fiberFactory_nullArgument_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).fiber(
          (java.util.function.Function< Subject< ? >, Fiber< Integer > >) null
        )
      );

    }

    /// Absent Fiber factory result fails at materialization.
    @SpecRef({"6.2", "6.2.6", "15.2"})
    @Test
    void fiberFactory_returnsNull_failsAtAttachment() {

      final var circuit = cortex.circuit();

      try {

        final Flow< Integer, Integer > flow =
          cortex.flow(Integer.class).fiber(_ -> null);

        assertThrows(
          NullPointerException.class,
          () -> flow.pipe(circuit.pipe())
        );

      } finally {

        circuit.close();

      }

    }

    /// Fiber factory configuration failure propagates at
    /// materialization.
    @SpecRef({"6.2", "15.1"})
    @Test
    void fiberFactory_throwsException_propagatesAtAttachment() {

      final var circuit = cortex.circuit();

      try {

        final Flow< Integer, Integer > flow =
          cortex.flow(Integer.class).fiber(_ -> {
            throw new IllegalStateException("no config");
          });

        assertThrows(
          IllegalStateException.class,
          () -> flow.pipe(circuit.pipe())
        );

      } finally {

        circuit.close();

      }

    }

  }

  @Nested
  final class FlowOperator {

    /// Flow composition rejects an incompatible provider.
    @SpecRef("15.1")
    @Test
    void flow_foreignNext_throwsFault() {

      final Flow< Integer, Integer > foreign = foreignProviderStub(Flow.class);

      assertThrows(
        Fault.class,
        () -> cortex.flow(Integer.class).flow(foreign)
      );

    }

    /// Flow composition bridges an intermediate output/input type.
    @SpecRef("6.2.1")
    @Test
    void flow_intermediateType_bridgesComposition() {

      final var circuit = cortex.circuit();

      try {

        final List< Boolean > captured = new ArrayList<>();

        final Flow< Integer, String > intToString =
          cortex.flow(Integer.class).map(Object::toString);

        final Flow< String, Boolean > stringNonEmpty =
          cortex.flow(String.class).map(s -> !s.isEmpty());

        final Pipe< Boolean > sink = circuit.pipe(captured::add);
        final Pipe< Integer > head =
          intToString.flow(stringNonEmpty).pipe(sink);

        head.emit(7);
        head.emit(0);

        circuit.await();

        assertEquals(List.of(true, true), captured);

      } finally {

        circuit.close();

      }

    }

    /// Stateful composed Flow state is independent per attachment.
    @SpecRef("6.2.3")
    @Test
    void flow_multipleAttachments_isolatesState() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > a = new ArrayList<>();
        final List< Integer > b = new ArrayList<>();

        // Stateful operator inside the composed flow: diff (via attached fiber).
        // Two attachments must get independent state instances.
        final Flow< Integer, Integer > left =
          cortex.flow(Integer.class);

        final Flow< Integer, Integer > right =
          cortex.flow(Integer.class)
            .fiber(cortex.fiber(Integer.class).diff());

        final Flow< Integer, Integer > composed = left.flow(right);

        final Pipe< Integer > pa = composed.pipe(circuit.pipe(a::add));
        final Pipe< Integer > pb = composed.pipe(circuit.pipe(b::add));

        pa.emit(1);
        pa.emit(1);   // dropped by pa's diff
        pa.emit(2);

        pb.emit(1);   // not dropped: pb has its own diff state
        pb.emit(1);   // dropped
        pb.emit(2);

        circuit.await();

        assertEquals(List.of(1, 2), a);
        assertEquals(List.of(1, 2), b);

      } finally {

        circuit.close();

      }

    }

    /// Flow#flow rejects an absent next Flow.
    @SpecRef("15.2")
    @Test
    void flow_nullNext_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).flow((Flow< Integer, Integer >) null)
      );

    }

    /// Composed Flows route through both segments in order.
    @SpecRef("6.2.6")
    @Test
    void flow_twoSegments_executesBothInOrder() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Flow< Integer, Integer > incr =
          cortex.flow(Integer.class).map(i -> i + 1);

        final Flow< Integer, String > stringify =
          cortex.flow(Integer.class).map(i -> "v:" + i);

        final Pipe< String > sink = circuit.pipe(captured::add);
        final Pipe< Integer > head = incr.flow(stringify).pipe(sink);

        head.emit(1);
        head.emit(2);
        head.emit(3);

        circuit.await();

        assertEquals(List.of("v:2", "v:3", "v:4"), captured);

      } finally {

        circuit.close();

      }

    }

  }

  @Nested
  final class FlowSubjectiveOperator {

    /// Subject-aware Flow factory receives the downstream subject.
    @SpecRef("6.2")
    @Test
    void flowFactory_atAttachment_receivesDownstreamSubject() {

      final var circuit = cortex.circuit();

      try {

        final var conduit = circuit.conduit(Integer.class);
        final var name = cortex.name("config.lookup.key");
        final var sink = conduit.get(name);

        final List< Name > seenNames = new ArrayList<>();

        cortex.flow(Integer.class)
          .flow(subject -> {
            seenNames.add(subject.name());
            return cortex.flow(Integer.class);
          })
          .pipe(sink);

        assertEquals(List.of(name), seenNames);

      } finally {

        circuit.close();

      }

    }

    /// Subject-aware Flow factory is independently invoked per
    /// attachment.
    @SpecRef({"6.2", "6.2.6"})
    @Test
    void flowFactory_multipleAttachments_invokesIndependently() {

      final var circuit = cortex.circuit();

      try {

        final var counter = new int[]{0};

        final Flow< Integer, Integer > flow =
          cortex.flow(Integer.class)
            .flow(_ -> {
              counter[0]++;
              return cortex.flow(Integer.class);
            });

        flow.pipe(circuit.pipe());
        flow.pipe(circuit.pipe());
        flow.pipe(circuit.pipe());

        assertEquals(3, counter[0]);

      } finally {

        circuit.close();

      }

    }

    /// Subject-aware Flow factory creates independent state per
    /// attachment.
    @SpecRef({"6.2", "6.2.3"})
    @Test
    void flowFactory_multipleAttachments_isolatesState() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > a = new ArrayList<>();
        final List< Integer > b = new ArrayList<>();

        // Each attachment must invoke the factory and get an independent
        // produced flow; downstream state (diff) must not leak across them.
        final Flow< Integer, Integer > flow =
          cortex.flow(Integer.class)
            .flow(_ -> cortex.flow(Integer.class)
              .fiber(cortex.fiber(Integer.class).diff()));

        final Pipe< Integer > pa = flow.pipe(circuit.pipe(a::add));
        final Pipe< Integer > pb = flow.pipe(circuit.pipe(b::add));

        pa.emit(1);
        pa.emit(1);   // dropped by pa's diff
        pa.emit(2);

        pb.emit(1);   // not dropped: pb has independent diff state
        pb.emit(1);   // dropped
        pb.emit(2);

        circuit.await();

        assertEquals(List.of(1, 2), a);
        assertEquals(List.of(1, 2), b);

      } finally {

        circuit.close();

      }

    }

    /// Subject-aware Flow factory overload rejects absence.
    @SpecRef("15.2")
    @Test
    void flowFactory_nullArgument_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).flow(
          (java.util.function.Function< Subject< ? >, Flow< Integer, Integer > >) null
        )
      );

    }

    /// Absent Flow factory result fails at materialization.
    @SpecRef({"6.2", "6.2.6", "15.2"})
    @Test
    void flowFactory_returnsNull_failsAtAttachment() {

      final var circuit = cortex.circuit();

      try {

        final Flow< Integer, Integer > flow =
          cortex.flow(Integer.class).flow(_ -> null);

        assertThrows(
          NullPointerException.class,
          () -> flow.pipe(circuit.pipe())
        );

      } finally {

        circuit.close();

      }

    }

    /// Flow factory configuration failure propagates at
    /// materialization.
    @SpecRef({"6.2", "15.1"})
    @Test
    void flowFactory_throwsException_propagatesAtAttachment() {

      final var circuit = cortex.circuit();

      try {

        final Flow< Integer, Integer > flow =
          cortex.flow(Integer.class).flow(_ -> {
            throw new IllegalStateException("no config");
          });

        assertThrows(
          IllegalStateException.class,
          () -> flow.pipe(circuit.pipe())
        );

      } finally {

        circuit.close();

      }

    }

    /// A Flow factory may produce a type-changing segment.
    @SpecRef({"6.2", "6.2.1"})
    @Test
    void flowFactory_typeChangingSegment_transformsType() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .flow(_ ->
              cortex.flow(Integer.class).map(i -> "v:" + i)
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(1);
        head.emit(42);

        circuit.await();

        assertEquals(List.of("v:1", "v:42"), captured);

      } finally {

        circuit.close();

      }

    }

  }

  @Nested
  final class MapOperator {

    /// Chained mapped Pipes form nested subject enclosures.
    @Test
    void map_chainedPipes_nestSubjects() {

      final var circuit = cortex.circuit();

      try {

        final Pipe< Integer > tail =
          circuit.pipe();

        final Pipe< Integer > mid =
          cortex.flow(Integer.class).map(i -> i + 1).pipe(tail);

        final Pipe< Integer > head =
          cortex.flow(Integer.class).map(i -> i + 1).pipe(mid);

        assertTrue(head.subject().within(mid.subject()));
        assertTrue(head.subject().within(tail.subject()));

      } finally {

        circuit.close();

      }

    }

    /// A map function returning absence filters the emission.
    @SpecRef("6.2.3")
    @Test
    void map_functionReturnsNull_filtersEmission() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< String > target = circuit.pipe(captured::add);

        final Pipe< Integer > mapped =
          cortex.flow(Integer.class).map(
            i -> i > 0 ? "+" + i:null
          ).pipe(target);

        mapped.emit(1);
        mapped.emit(-2);
        mapped.emit(3);
        mapped.emit(-4);

        circuit.await();

        assertEquals(List.of("+1", "+3"), captured);

      } finally {

        circuit.close();

      }

    }

    /// Map transforms input values to its output type.
    @SpecRef("6.2.1")
    @Test
    void map_inputValues_transformsOutputs() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< String > target = circuit.pipe(captured::add);

        // flow(Integer).map(i -> "v:" + i) returns Flow<Integer, String>
        final Pipe< Integer > mapped =
          cortex.flow(Integer.class).map(i -> "v:" + i).pipe(target);

        mapped.emit(1);
        mapped.emit(42);

        circuit.await();

        assertEquals(List.of("v:1", "v:42"), captured);

      } finally {

        circuit.close();

      }

    }

    /// Flow#map rejects an absent function.
    @SpecRef("15.2")
    @Test
    void map_nullFunction_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).< Integer > map(null)
      );

    }

  }

  @Nested
  final class RelateOperator {

    /// Fires on change, suppresses on repeat, and changes type (Integer → String).
    /// The first emission sees the `initial` seed as `prev`.
    /// Relate emits on change and suppresses repeated equality.
    @SpecRef("6.2.3")
    @Test
    void relate_changedInput_emitsOnlyOnChange() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .relate(
              0,
              (prev, curr) -> prev.equals(curr) ? null:prev + "->" + curr
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(0);  // (0,0) equal → suppress
        head.emit(1);  // (0,1) → "0->1"
        head.emit(1);  // (1,1) equal → suppress
        head.emit(2);  // (1,2) → "1->2"

        circuit.await();

        assertEquals(List.of("0->1", "1->2"), captured);

      } finally {

        circuit.close();

      }

    }

    /// `prev` advances on EVERY emission, including suppressed ones. If `prev`
    /// froze on suppression, the pair at the first odd value would be `(0,3)`
    /// not `(2,3)`.
    /// Relate advances previous state for every admitted input.
    @SpecRef("6.2.3")
    @Test
    void relate_everyInput_advancesPreviousState() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .relate(
              0,
              (prev, curr) -> curr % 2==1 ? prev + "," + curr:null
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(2);  // (0,2) even → suppress; prev → 2
        head.emit(3);  // (2,3) odd  → "2,3";    prev → 3
        head.emit(4);  // (3,4) even → suppress; prev → 4
        head.emit(5);  // (4,5) odd  → "4,5";    prev → 5

        circuit.await();

        assertEquals(List.of("2,3", "4,5"), captured);

      } finally {

        circuit.close();

      }

    }

    /// The first emission's `prev` is exactly the `initial` seed.
    /// Relate compares the first emission with its initial seed.
    @SpecRef("6.2.3")
    @Test
    void relate_firstEmission_comparesInitialSeed() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .relate(
              100,
              (prev, curr) -> prev + ":" + curr
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(7);
        head.emit(8);

        circuit.await();

        assertEquals(List.of("100:7", "7:8"), captured);

      } finally {

        circuit.close();

      }

    }

    /// A `null` seed is allowed; the first invocation receives `null` as `prev`.
    /// Relate permits an absent initial previous value.
    @SpecRef("6.2.3")
    @Test
    void relate_nullInitial_passesNullPreviousValue() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .relate(
              null,
              (prev, curr) -> (prev==null ? "null":prev) + "->" + curr
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(1);  // (null,1) → "null->1"
        head.emit(2);  // (1,2)    → "1->2"

        circuit.await();

        assertEquals(List.of("null->1", "1->2"), captured);

      } finally {

        circuit.close();

      }

    }

    /// Relate rejects an absent relation operation.
    @SpecRef("15.2")
    @Test
    void relate_nullOperation_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).relate(
          0,
          (java.util.function.BiFunction< Integer, Integer, Integer >) null
        )
      );

    }

    /// op throws → emission dropped, but `prev` has already advanced to
    /// the incoming value. The next emission proceeds from the advanced `prev`
    /// (proving `(2,3)`, not `(1,3)`).
    /// Relate advances previous input across operator failure.
    @SpecRef({"6.2.3", "15.4"})
    @Test
    void relate_operationThrows_advancesPreviousInput() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .relate(
              0,
              (prev, curr) -> {
                if (curr==2) throw new ArithmeticException("trigger");
                return prev + "->" + curr;
              }
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(1);  // (0,1) → "0->1"; prev → 1
        head.emit(2);  // (1,2) → throws; prev → 2
        head.emit(3);  // (2,3) → "2->3"

        circuit.await();

        assertEquals(List.of("0->1", "2->3"), captured);

      } finally {

        circuit.close();

      }

    }

  }

  @Nested
  final class RunOperator {

    /// Length is `1` on the first admission, increments while value-equal, and
    /// resets to `1` on a change. Every admission emits a [Run] view.
    /// Run length increments while equal and resets on change.
    @SpecRef("6.2.3")
    @Test
    void run_equalThenChangedValues_incrementsThenResetsLength() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .run()
            .map(r -> r.emission() + ":" + r.length())
            .pipe(circuit.pipe(captured::add));

        head.emit(7);  // 7:1
        head.emit(7);  // 7:2
        head.emit(7);  // 7:3
        head.emit(9);  // 9:1  (change resets)
        head.emit(7);  // 7:1  (non-consecutive resets)

        circuit.await();

        assertEquals(
          List.of("7:1", "7:2", "7:3", "9:1", "7:1"),
          captured
        );

      } finally {

        circuit.close();

      }

    }

    /// State is per materialization: two pipes from the same flow value keep
    /// independent run counts.
    /// Run state is independent for each materialization.
    @SpecRef("6.2.3")
    @Test
    void run_multipleMaterializations_isolatesState() {

      final var circuit = cortex.circuit();

      try {

        final List< Long > a = new ArrayList<>();
        final List< Long > b = new ArrayList<>();

        final Flow< Integer, Run< Integer > > flow =
          cortex.flow(Integer.class).run();

        final Pipe< Integer > ha =
          flow.map(Run::length).pipe(circuit.pipe(a::add));
        final Pipe< Integer > hb =
          flow.map(Run::length).pipe(circuit.pipe(b::add));

        ha.emit(5);  // a: 1
        ha.emit(5);  // a: 2
        hb.emit(9);  // b: 1   (independent of a)
        ha.emit(5);  // a: 3   (unaffected by hb)

        circuit.await();

        assertEquals(List.of(1L, 2L, 3L), a);
        assertEquals(List.of(1L), b);

      } finally {

        circuit.close();

      }

    }

    /// Each emitted Run is a fresh immutable envelope: references retained across
    /// later admissions keep their captured value and length (they would all
    /// read the latest state if the view were reused).
    /// Run emits an immutable retainable snapshot.
    @SpecRef("6.2.3")
    @Test
    void run_retainedSnapshot_remainsImmutable() {

      final var circuit = cortex.circuit();

      try {

        final List< Run< Integer > > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .run()
            .pipe(circuit.pipe(captured::add));

        head.emit(7);  // 7:1
        head.emit(7);  // 7:2
        head.emit(9);  // 9:1

        circuit.await();

        assertEquals(3, captured.size());
        assertEquals(7, captured.get(0).emission());
        assertEquals(1L, captured.get(0).length());
        assertEquals(7, captured.get(1).emission());
        assertEquals(2L, captured.get(1).length());
        assertEquals(9, captured.get(2).emission());
        assertEquals(1L, captured.get(2).length());

      } finally {

        circuit.close();

      }

    }

    /// The first admission carries the value with length `1`.
    /// The first run snapshot has length one.
    @SpecRef("6.2.3")
    @Test
    void run_singleAdmission_reportsLengthOne() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .run()
            .map(r -> r.emission() + ":" + r.length())
            .pipe(circuit.pipe(captured::add));

        head.emit(42);

        circuit.await();

        assertEquals(List.of("42:1"), captured);

      } finally {

        circuit.close();

      }

    }

  }

  @Nested
  final class ScanInputOperator {

    /// Input-aware scan filters an absent emitter result.
    @SpecRef("6.2.3")
    @Test
    void scanInput_emitReturnsNull_filtersEmission() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        // emit filters when current input is even
        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> 0,
              Integer::sum,
              (sum, current) -> current % 2==0 ? null:sum
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(1);  // sum=1, odd → emit 1
        head.emit(2);  // sum=3, even → filter
        head.emit(3);  // sum=6, odd → emit 6
        head.emit(4);  // sum=10, even → filter

        circuit.await();

        assertEquals(List.of(1, 6), captured);

      } finally {

        circuit.close();

      }

    }

    /// emit throws → emission dropped, but state has already advanced via
    /// step. Subsequent emission proceeds from the advanced state, confirming
    /// the asymmetry from the step-throw case.
    /// Input-aware scan state advances across emitter failure.
    @SpecRef({"6.2.3", "15.4"})
    @Test
    void scanInput_emitterThrows_advancesStoredState() {

      final var circuit = cortex.circuit();

      try {

        final List< Long > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> 0L,
              (count, _) -> count + 1L,
              (count, _) -> {
                if (count==2L) throw new ArithmeticException("trigger");
                return count;
              }
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(10);    // step → state=1, emit ok → 1
        head.emit(10);    // step → state=2, emit throws → state already 2
        head.emit(10);    // step → state=3, emit ok → 3 (NOT 2)

        circuit.await();

        assertEquals(List.of(1L, 3L), captured);

      } finally {

        circuit.close();

      }

    }

    /// Input-aware scan rejects an absent emitter.
    @SpecRef("15.2")
    @Test
    void scanInput_nullEmitter_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).scan(
          () -> 0,
          Integer::sum,
          (java.util.function.BiFunction< Integer, Integer, Integer >) null
        )
      );

    }

    /// Input-aware scan rejects an absent step operation.
    @SpecRef("15.2")
    @Test
    void scanInput_nullStep_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).scan(
          () -> 0,
          null,
          (s, _) -> s
        )
      );

    }

    /// Input-aware scan rejects an absent state supplier.
    @SpecRef("15.2")
    @Test
    void scanInput_nullSupplier_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).scan(
          null,
          Integer::sum,
          (s, _) -> s
        )
      );

    }

    /// step throws → emission dropped, stored state reference unchanged.
    /// Subsequent emission proceeds with the prior state, confirming the
    /// assignment-after-step ordering in the receptor.
    /// Input-aware scan retains state when the step throws.
    @SpecRef({"6.2.3", "15.4"})
    @Test
    void scanInput_stepThrows_preservesStoredState() {

      final var circuit = cortex.circuit();

      try {

        final List< Long > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> 0L,
              (count, v) -> {
                if (v < 0) throw new ArithmeticException("negative input");
                return count + 1L;
              },
              (count, _) -> count
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(10);    // step ok → state=1, emit=1
        head.emit(-1);    // step throws → state stays 1, downstream sees nothing
        head.emit(20);    // step ok with state=1 → state=2, emit=2

        circuit.await();

        assertEquals(List.of(1L, 2L), captured);

      } finally {

        circuit.close();

      }

    }

    /// Supplier throws at attachment time — exception propagates synchronously
    /// from `pipe(...)` (factory contract, not emission-time).
    /// Input-aware scan supplier failure propagates at attachment.
    @SpecRef("15.1")
    @Test
    void scanInput_supplierThrows_propagatesAtAttachment() {

      final var circuit = cortex.circuit();

      try {

        final Flow< Integer, Integer > recipe =
          cortex.flow(Integer.class)
            .scan(
              () -> {
                throw new IllegalStateException("seed failure");
              },
              Integer::sum,
              (s, _) -> s
            );

        assertThrows(
          IllegalStateException.class,
          () -> recipe.pipe(circuit.pipe())
        );

      } finally {

        circuit.close();

      }

    }

    /// Input-aware scan emitter receives current state and input.
    @SpecRef("6.2.3")
    @Test
    void scanInput_validEmission_receivesCurrentInput() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        // emit projects (state, input) pair so we can confirm the input is delivered.
        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> 0,
              Integer::sum,
              (sum, current) -> "sum=" + sum + ",cur=" + current
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(5);
        head.emit(10);
        head.emit(7);

        circuit.await();

        assertEquals(
          List.of("sum=5,cur=5", "sum=15,cur=10", "sum=22,cur=7"),
          captured
        );

      } finally {

        circuit.close();

      }

    }

    /// Input-aware scan supports distinct statistical state and output.
    @SpecRef("6.2.3")
    @Test
    void scanInput_zScoreState_emitsDerivedStatistic() {

      final var circuit = cortex.circuit();

      try {

        final List< Double > captured = new ArrayList<>();

        // Welford running mean & variance, emit z-score = (v - mean) / stddev
        // using BiFunction emit that sees both state and current input.
        final Pipe< Double > head =
          cortex.flow(Double.class)
            .scan(
              () -> new double[]{0.0, 0.0, 0.0},  // (count, mean, M2)
              (s, v) -> {
                final double n = s[0] + 1.0;
                final double delta = v - s[1];
                final double mean = s[1] + delta / n;
                final double m2 = s[2] + delta * (v - mean);
                s[0] = n;
                s[1] = mean;
                s[2] = m2;
                return s;
              },
              (s, v) -> {
                if (s[0] < 2.0) return null;
                final double variance = s[2] / (s[0] - 1.0);
                if (variance==0.0) return null;
                return (v - s[1]) / Math.sqrt(variance);
              }
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(10.0);  // n=1, no z-score
        head.emit(12.0);  // n=2, mean=11, var=2, stddev=√2, z=(12-11)/√2 ≈ 0.7071
        head.emit(14.0);  // n=3, mean=12, var=4, stddev=2, z=(14-12)/2 = 1.0

        circuit.await();

        assertEquals(2, captured.size());
        assertEquals(0.7071, captured.get(0), 0.001);
        assertEquals(1.0, captured.get(1), 0.001);

      } finally {

        circuit.close();

      }

    }

  }

  @Nested
  final class ScanOperator {

    /// Scan invokes its supplier once per attachment.
    @SpecRef("6.2.3")
    @Test
    void scan_eachAttachment_invokesSupplierOnce() {

      final var circuit = cortex.circuit();

      try {

        final var counter = new int[]{0};

        final Flow< Integer, Integer > recipe =
          cortex.flow(Integer.class)
            .scan(
              () -> {
                counter[0]++;
                return 0;
              },
              Integer::sum,
              s -> s
            );

        recipe.pipe(circuit.pipe());
        recipe.pipe(circuit.pipe());
        recipe.pipe(circuit.pipe());

        assertEquals(3, counter[0]);

      } finally {

        circuit.close();

      }

    }

    /// Scan filters an absent emitter result.
    @SpecRef("6.2.3")
    @Test
    void scan_emitterReturnsNull_filtersEmission() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        // emit returns null until 3 emissions accumulated
        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> new int[]{0, 0},
              (s, v) -> {
                s[0] += v;
                s[1] += 1;
                return s;
              },
              s -> s[1] >= 3 ? s[0]:null
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(1);
        head.emit(2);
        head.emit(3);
        head.emit(4);

        circuit.await();

        assertEquals(List.of(6, 10), captured);

      } finally {

        circuit.close();

      }

    }

    /// emit throws → emission dropped, but state has already advanced via
    /// step. Subsequent emission proceeds from the advanced state, confirming
    /// the asymmetry from the step-throw case. The two cases together pin
    /// the contract that the assignment happens between step return and emit
    /// invocation.
    /// Scan state advances across emitter failure.
    @SpecRef({"6.2.3", "15.4"})
    @Test
    void scan_emitterThrows_advancesStoredState() {

      final var circuit = cortex.circuit();

      try {

        final List< Long > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> 0L,
              (count, _) -> count + 1L,
              count -> {
                if (count==2L) throw new ArithmeticException("trigger");
                return count;
              }
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(10);    // step → state=1, emit ok → 1
        head.emit(10);    // step → state=2, emit throws → state already 2
        head.emit(10);    // step → state=3, emit ok → 3 (NOT 2)

        circuit.await();

        assertEquals(List.of(1L, 3L), captured);

      } finally {

        circuit.close();

      }

    }

    /// Scan composes with map and Fiber stages in declaration order.
    @SpecRef("6.2.6")
    @Test
    void scan_mapAndFiberComposition_executesInOrder() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        // map → scan (running sum) → map (stringify) → fiber (diff)
        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .map(i -> i * 2)
            .scan(
              () -> 0,
              Integer::sum,
              s -> s
            )
            .map(s -> "sum=" + s)
            .fiber(cortex.fiber(String.class).diff())
            .pipe(circuit.pipe(captured::add));

        head.emit(1);  // *2 = 2, sum = 2
        head.emit(2);  // *2 = 4, sum = 6
        head.emit(0);  // *2 = 0, sum = 6 (diff drops repeat)
        head.emit(3);  // *2 = 6, sum = 12

        circuit.await();

        assertEquals(List.of("sum=2", "sum=6", "sum=12"), captured);

      } finally {

        circuit.close();

      }

    }

    /// Scan state is independent for every attachment.
    @SpecRef({"6.2", "6.2.3"})
    @Test
    void scan_multipleAttachments_isolatesState() {

      final var circuit = cortex.circuit();

      try {

        final List< Long > a = new ArrayList<>();
        final List< Long > b = new ArrayList<>();

        // Recipe is reused across two attachments. Supplier guarantees fresh
        // mutable state per attachment — no aliasing.
        final Flow< Integer, Long > recipe =
          cortex.flow(Integer.class)
            .scan(
              () -> new long[]{0L},
              (s, v) -> {
                s[0] += v;
                return s;
              },
              s -> s[0]
            );

        final Pipe< Integer > pa = recipe.pipe(circuit.pipe(a::add));
        final Pipe< Integer > pb = recipe.pipe(circuit.pipe(b::add));

        pa.emit(1);
        pa.emit(2);

        pb.emit(100);
        pb.emit(200);

        circuit.await();

        assertEquals(List.of(1L, 3L), a);
        assertEquals(List.of(100L, 300L), b);

      } finally {

        circuit.close();

      }

    }

    /// Scan rejects an absent emitter.
    @SpecRef("15.2")
    @Test
    void scan_nullEmitter_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).scan(
          () -> 0,
          Integer::sum,
          (java.util.function.Function< Integer, Integer >) null
        )
      );

    }

    /// Scan rejects an absent step operation.
    @SpecRef("15.2")
    @Test
    void scan_nullStep_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).scan(
          () -> 0,
          null,
          s -> s
        )
      );

    }

    /// Scan rejects an absent state supplier.
    @SpecRef("15.2")
    @Test
    void scan_nullSupplier_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).scan(
          null,
          Integer::sum,
          s -> s
        )
      );

    }

    /// Scan supports distinct state and output types.
    @SpecRef("6.2.1")
    @Test
    void scan_runningMean_separatesStateAndOutputTypes() {

      final var circuit = cortex.circuit();

      try {

        final List< Double > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> new long[]{0L, 0L},
              (s, v) -> {
                s[0] += v;
                s[1] += 1L;
                return s;
              },
              s -> s[1]==0L ? null:(double) s[0] / s[1]
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(10);
        head.emit(20);
        head.emit(30);

        circuit.await();

        assertEquals(List.of(10.0, 15.0, 20.0), captured);

      } finally {

        circuit.close();

      }

    }

    /// State-as-output scan rejects absent required arguments.
    @SpecRef("15.2")
    @Test
    void scan_stateAsOutputNullArguments_throwNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).scan(
          null,
          Integer::sum
        )
      );

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).scan(
          () -> 0,
          null
        )
      );

    }

    /// State-as-output scan may emit the same immutable state reference.
    @SpecRef("6.2.3")
    @Test
    void scan_stateAsOutput_allowsImmutableReference() {

      record Marker(
        int value
      ) {
      }

      final var circuit = cortex.circuit();

      try {

        final Marker unchanged =
          new Marker(
            7
          );

        final List< Marker > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> unchanged,
              (state, _) -> state
            )
            .pipe(
              circuit.pipe(captured::add)
            );

        head.emit(1);
        head.emit(2);

        circuit.await();

        assertEquals(
          List.of(
            unchanged,
            unchanged
          ),
          captured
        );

        assertSame(
          unchanged,
          captured.getFirst()
        );

      } finally {

        circuit.close();

      }

    }

    /// State-as-output scan emits immutable accumulator snapshots.
    @SpecRef("6.2.3")
    @Test
    void scan_stateAsOutput_emitsImmutableAccumulator() {

      record Running(
        long count,
        long sum
      ) {
      }

      final var circuit = cortex.circuit();

      try {

        final List< Running > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> new Running(0L, 0L),
              (state, value) ->
                new Running(
                  state.count() + 1L,
                  state.sum() + value
                )
            )
            .pipe(
              circuit.pipe(captured::add)
            );

        head.emit(2);
        head.emit(3);
        head.emit(-1);

        circuit.await();

        assertEquals(
          List.of(
            new Running(1L, 2L),
            new Running(2L, 5L),
            new Running(3L, 4L)
          ),
          captured
        );

      } finally {

        circuit.close();

      }

    }

    /// A null scan step result is passed to the emitter as state.
    @SpecRef("6.2.3")
    @Test
    void scan_stepReturnsNull_propagatesNullStateToEmitter() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        // step returns null on negative inputs; emit handles null by filtering.
        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> "",
              (s, v) -> v < 0 ? null:s + v,
              s -> s
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(1);
        head.emit(-2);  // step returns null; emit returns null; filtered
        head.emit(3);   // step receives null prev; concatenates "null3"

        circuit.await();

        assertEquals(List.of("1", "null3"), captured);

      } finally {

        circuit.close();

      }

    }

    /// step throws → emission dropped, stored state reference unchanged.
    /// Subsequent emission proceeds with the prior state, confirming the
    /// assignment-after-step ordering in the receptor. Future refactors that
    /// move the assignment ahead of the call would silently flip the contract;
    /// this test pins it.
    /// Scan retains stored state when the step throws.
    @SpecRef({"6.2.3", "15.4"})
    @Test
    void scan_stepThrows_preservesStoredState() {

      final var circuit = cortex.circuit();

      try {

        final List< Long > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> 0L,
              (count, v) -> {
                if (v < 0) throw new ArithmeticException("negative input");
                return count + 1L;
              },
              count -> count
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(10);    // step ok → state=1, emit=1
        head.emit(-1);    // step throws → state stays 1, downstream sees nothing
        head.emit(20);    // step ok with state=1 → state=2, emit=2

        circuit.await();

        assertEquals(List.of(1L, 2L), captured);

      } finally {

        circuit.close();

      }

    }

    /// Subject-aware Flow factory seeds scan state per subject.
    @Test
    void scan_subjectAwareFactory_seedsPerSubject() {

      final var circuit = cortex.circuit();

      try {

        final var conduit = circuit.conduit(Long.class);
        final var nameA = cortex.name("alpha");
        final var nameB = cortex.name("beta");

        final List< Long > capturedSeeds = new ArrayList<>();

        // Per-subject seed lookup happens via the canonical flow.flow(factory)
        // hop — no scanWith overload needed. Each attachment invokes the factory
        // with the materialized subject; the factory pins a per-subject seed
        // into the produced scan recipe.
        final Flow< Integer, Long > recipe =
          cortex.flow(Integer.class)
            .flow(subject -> {
              final long seed = subject.name().equals(nameA) ? 1000L:0L;
              capturedSeeds.add(seed);
              return cortex.flow(Integer.class)
                .scan(
                  () -> new long[]{seed},
                  (s, v) -> {
                    s[0] += v;
                    return s;
                  },
                  s -> s[0]
                );
            });

        recipe.pipe(conduit.get(nameA));
        recipe.pipe(conduit.get(nameB));

        assertEquals(List.of(1000L, 0L), capturedSeeds);

      } finally {

        circuit.close();

      }

    }

    /// Scan permits an absent initial state from its supplier.
    @SpecRef("6.2.3")
    @Test
    void scan_supplierReturnsNull_propagatesNullState() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        // Supplier returns null; step receives null as prev on first call.
        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .scan(
              () -> (String) null,
              (prev, v) -> prev==null ? "first:" + v:prev + ",+" + v,
              s -> s
            )
            .pipe(circuit.pipe(captured::add));

        head.emit(1);
        head.emit(2);
        head.emit(3);

        circuit.await();

        assertEquals(List.of("first:1", "first:1,+2", "first:1,+2,+3"), captured);

      } finally {

        circuit.close();

      }

    }

    /// Supplier throws at attachment time — exception propagates synchronously
    /// from `pipe(...)` (factory contract, not emission-time).
    /// Scan supplier failure propagates during attachment.
    @SpecRef("15.1")
    @Test
    void scan_supplierThrows_propagatesAtAttachment() {

      final var circuit = cortex.circuit();

      try {

        final Flow< Integer, Integer > recipe =
          cortex.flow(Integer.class)
            .scan(
              () -> {
                throw new IllegalStateException("seed failure");
              },
              Integer::sum,
              s -> s
            );

        assertThrows(
          IllegalStateException.class,
          () -> recipe.pipe(circuit.pipe())
        );

      } finally {

        circuit.close();

      }

    }

  }

  @Nested
  final class WindowOperator {

    /// Window use after its callback returns signals temporal misuse.
    @SpecRef({"6.4", "6.4.1"})
    @Test
    void window_afterCallbackReturn_throwsFault() {

      final var circuit = cortex.circuit();

      try {

        final List< Window< Integer > > retained = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(2)
            .pipe(
              circuit.pipe(window -> {

                assertEquals(
                  1,
                  window.size()
                );

                retained.add(
                  window
                );

              })
            );

        head.emit(
          1
        );

        circuit.await();

        final var fault =
          assertThrows(
            Fault.class,
            () -> retained.getFirst().size()
          );

        assertNotNull(
          fault
        );

      } finally {

        circuit.close();

      }

    }

    /// Window observes only outputs surviving prior map and filter stages.
    @SpecRef("6.2.3")
    @Test
    void window_afterMapAndFilter_containsSurvivingOutputs() {

      final var circuit = cortex.circuit();

      try {

        final List< List< String > > captured = new ArrayList<>();

        final Pipe< Window< String > > sink =
          circuit.pipe(window -> captured.add(values(window)));

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .map(i -> i > 0 ? "v:" + i:null)
            .window(2)
            .pipe(sink);

        head.emit(1);
        head.emit(-2);
        head.emit(3);
        head.emit(4);

        circuit.await();

        assertEquals(
          List.of(
            List.of("v:1"),
            List.of("v:1", "v:3"),
            List.of("v:3", "v:4")
          ),
          captured
        );

      } finally {

        circuit.close();

      }

    }

    /// The callback retains its Window deliberately, then returns. Each public operation is invoked
    /// afterward so the test protects the full lease boundary rather than one convenient accessor.
    ///
    /// Every required Window operation detects use after its callback lease.
    @SpecRef("6.4.1")
    @Test
    void window_allOperationsAfterCallbackReturn_throwFault() {

      final var circuit = cortex.circuit();

      try {

        final var retained = new AtomicReference< Window< Integer > >();
        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(2)
            .pipe(circuit.pipe(retained::set));

        head.emit(1);
        circuit.await();

        final var window = retained.get();
        assertNotNull(window);

        final List< Runnable > operations =
          List.of(
            window::first,
            window::last,
            window::size,
            window::isEmpty,
            () -> window.prefix(1),
            () -> window.suffix(1),
            () -> window.skip(1),
            () -> window.trim(1),
            () -> window.slice(0, 1),
            window::reverse,
            () -> window.forEach(_ -> {
            }),
            () -> window.all(_ -> true),
            () -> window.any(_ -> true),
            () -> window.none(_ -> true),
            () -> window.count(_ -> true),
            () -> window.fold(0, Integer::sum),
            () -> window.reduce(0, Integer::sum)
          );

        for (final var operation : operations) {
          assertThrows(Fault.class, operation::run);
        }

      } finally {

        circuit.closeAwait();

      }

    }

    /// Capacity trims recent Window values before duration expiry.
    @SpecRef("6.2.3")
    @Test
    void window_capacityBeforeDuration_trimsOldestValues() {

      final var circuit = cortex.circuit();

      try {

        final List< List< Integer > > captured = new ArrayList<>();

        final Pipe< Window< Integer > > sink =
          circuit.pipe(window -> captured.add(values(window)));

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(
              Duration.ofDays(
                1L
              ),
              2
            )
            .pipe(sink);

        head.emit(
          1
        );

        head.emit(
          2
        );

        head.emit(
          3
        );

        circuit.await();

        assertEquals(
          List.of(
            List.of(1),
            List.of(1, 2),
            List.of(2, 3)
          ),
          captured
        );

      } finally {

        circuit.close();

      }

    }

    /// Chained first/last views restrict from the current source view.
    @SpecRef("6.2.3")
    @Test
    void window_chainedFirstAndLast_restrictCurrentView() {

      final var circuit = cortex.circuit();

      try {

        final List< List< Integer > > middle = new ArrayList<>();
        final List< List< Integer > > recentHead = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(5)
            .pipe(
              circuit.pipe(window -> {

                middle.add(
                  values(
                    window.prefix(4).suffix(2)
                  )
                );

                recentHead.add(
                  values(
                    window.suffix(4).prefix(2)
                  )
                );

              })
            );

        head.emit(1);
        head.emit(2);
        head.emit(3);
        head.emit(4);
        head.emit(5);
        head.emit(6);

        circuit.await();

        assertEquals(
          List.of(
            List.of(1),
            List.of(1, 2),
            List.of(2, 3),
            List.of(3, 4),
            List.of(3, 4),
            List.of(4, 5)
          ),
          middle
        );

        assertEquals(
          List.of(
            List.of(1),
            List.of(1, 2),
            List.of(1, 2),
            List.of(1, 2),
            List.of(2, 3),
            List.of(3, 4)
          ),
          recentHead
        );

      } finally {

        circuit.close();

      }

    }

    /// Window size reports the current derived view cardinality.
    @SpecRef("6.2.3")
    @Test
    void window_currentView_reportsVisibleSize() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > root = new ArrayList<>();
        final List< Integer > first = new ArrayList<>();
        final List< Integer > slice = new ArrayList<>();
        final List< Integer > skip = new ArrayList<>();
        final List< Integer > trim = new ArrayList<>();
        final List< Integer > reversed = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(3)
            .pipe(
              circuit.pipe(window -> {

                root.add(
                  window.size()
                );

                first.add(
                  window.prefix(
                    2
                  ).size()
                );

                slice.add(
                  window.slice(
                    2,
                    2
                  ).size()
                );

                skip.add(
                  window.skip(
                    1
                  ).size()
                );

                trim.add(
                  window.trim(
                    1
                  ).size()
                );

                reversed.add(
                  window.reverse().size()
                );

              })
            );

        head.emit(1);
        head.emit(2);
        head.emit(3);
        head.emit(4);

        circuit.await();

        assertEquals(
          List.of(
            1,
            2,
            3,
            3
          ),
          root
        );

        assertEquals(
          List.of(
            1,
            2,
            2,
            2
          ),
          first
        );

        assertEquals(
          List.of(
            0,
            0,
            1,
            1
          ),
          slice
        );

        assertEquals(
          List.of(
            0,
            1,
            2,
            2
          ),
          skip
        );

        assertEquals(
          List.of(
            0,
            1,
            2,
            2
          ),
          trim
        );

        assertEquals(
          root,
          reversed
        );

      } finally {

        circuit.close();

      }

    }

    /// Window exposes one callback-scoped snapshot per emission.
    @SpecRef({"6.2.3", "6.4"})
    @Test
    void window_eachEmission_exposesTemporalSnapshot() {

      final var circuit = cortex.circuit();

      try {

        final List< List< Integer > > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(2)
            .pipe(
              circuit.pipe(window ->
                captured.add(
                  values(
                    window
                  )
                )
              )
            );

        head.emit(1);
        head.emit(2);
        head.emit(3);

        circuit.await();

        assertEquals(
          List.of(
            List.of(1),
            List.of(1, 2),
            List.of(2, 3)
          ),
          captured
        );

      } finally {

        circuit.close();

      }

    }

    /// A zero-length prefix creates an empty view without leaving the callback lease. The assertions
    /// record the algebraic identities of every terminal operation, making empty-window behavior an
    /// explicit API contract rather than an implementation accident.
    ///
    /// Window terminal aggregators define results for empty derived views.
    @SpecRef("6.2.3")
    @Test
    void window_emptyDerivedView_returnsDefinedTerminalResults() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > sums = new ArrayList<>();
        final List< String > folds = new ArrayList<>();
        final List< Integer > counts = new ArrayList<>();
        final List< Boolean > alls = new ArrayList<>();
        final List< Boolean > anys = new ArrayList<>();
        final List< Boolean > nones = new ArrayList<>();
        final List< Boolean > emptyFlag = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(3)
            .pipe(
              circuit.pipe(window -> {

                final Window< Integer > empty =
                  window.prefix(0);

                sums.add(
                  empty.reduce(
                    7,
                    Integer::sum
                  )
                );

                folds.add(
                  empty.fold(
                    "seed",
                    (acc, v) -> acc + v
                  )
                );

                counts.add(
                  empty.count(
                    _ -> true
                  )
                );

                alls.add(
                  empty.all(
                    _ -> false
                  )
                );

                anys.add(
                  empty.any(
                    _ -> true
                  )
                );

                nones.add(
                  empty.none(
                    _ -> true
                  )
                );

                emptyFlag.add(
                  empty.isEmpty()
                );

              })
            );

        head.emit(1);
        head.emit(2);

        circuit.await();

        assertEquals(
          List.of(7, 7),
          sums
        );

        assertEquals(
          List.of("seed", "seed"),
          folds
        );

        assertEquals(
          List.of(0, 0),
          counts
        );

        assertEquals(
          List.of(true, true),
          alls
        );

        assertEquals(
          List.of(false, false),
          anys
        );

        assertEquals(
          List.of(true, true),
          nones
        );

        assertEquals(
          List.of(true, true),
          emptyFlag
        );

      } finally {

        circuit.close();

      }

    }

    /// Processing time advances across ingress items so a duration
    /// Window removes entries older than its bound.
    @SpecRef({"5.8", "6.2.3"})
    @Test
    void window_entriesOlderThanDuration_areTrimmed()
      throws InterruptedException {

      final var circuit = cortex.circuit();

      try {

        final List< List< Integer > > captured = new ArrayList<>();

        final Pipe< Window< Integer > > sink =
          circuit.pipe(window -> captured.add(values(window)));

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(
              Duration.ofMillis(
                5L
              ),
              10
            )
            .pipe(sink);

        head.emit(
          1
        );

        circuit.await();

        Thread.sleep(
          25L
        );

        head.emit(
          2
        );

        circuit.await();

        assertEquals(
          List.of(
            List.of(1),
            List.of(2)
          ),
          captured
        );

      } finally {

        circuit.close();

      }

    }

    /// Window first and last terminals follow encounter order.
    @SpecRef("6.2.3")
    @Test
    void window_firstAndLastValues_followEncounterOrder() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > first = new ArrayList<>();
        final List< Integer > last = new ArrayList<>();
        final List< Integer > reversedFirst = new ArrayList<>();
        final List< Integer > reversedLast = new ArrayList<>();
        final List< Class< ? extends Throwable > > empty = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(3)
            .pipe(
              circuit.pipe(window -> {

                first.add(
                  window.first()
                );

                last.add(
                  window.last()
                );

                final Window< Integer > reversed =
                  window.reverse();

                reversedFirst.add(
                  reversed.first()
                );

                reversedLast.add(
                  reversed.last()
                );

                try {

                  window.prefix(
                    0
                  ).first();

                } catch (final NoSuchElementException e) {

                  empty.add(
                    e.getClass()
                  );

                }

                try {

                  window.suffix(
                    0
                  ).last();

                } catch (final NoSuchElementException e) {

                  empty.add(
                    e.getClass()
                  );

                }

              })
            );

        head.emit(1);
        head.emit(2);
        head.emit(3);
        head.emit(4);

        circuit.await();

        assertEquals(
          List.of(
            1,
            1,
            1,
            2
          ),
          first
        );

        assertEquals(
          List.of(
            1,
            2,
            3,
            4
          ),
          last
        );

        assertEquals(
          last,
          reversedFirst
        );

        assertEquals(
          first,
          reversedLast
        );

        assertEquals(
          List.of(
            NoSuchElementException.class,
            NoSuchElementException.class,
            NoSuchElementException.class,
            NoSuchElementException.class,
            NoSuchElementException.class,
            NoSuchElementException.class,
            NoSuchElementException.class,
            NoSuchElementException.class
          ),
          empty
        );

      } finally {

        circuit.close();

      }

    }

    /// First and last create restricted temporal views.
    @SpecRef("6.2.3")
    @Test
    void window_firstAndLastViews_restrictVisibleValues() {

      final var circuit = cortex.circuit();

      try {

        final List< List< Integer > > first = new ArrayList<>();
        final List< List< Integer > > last = new ArrayList<>();
        final List< List< Integer > > firstZero = new ArrayList<>();
        final List< List< Integer > > lastZero = new ArrayList<>();
        final List< Boolean > full = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(4)
            .pipe(
              circuit.pipe(window -> {

                first.add(
                  values(
                    window.prefix(2)
                  )
                );

                last.add(
                  values(
                    window.suffix(2)
                  )
                );

                firstZero.add(
                  values(
                    window.prefix(0)
                  )
                );

                lastZero.add(
                  values(
                    window.suffix(0)
                  )
                );

                full.add(
                  window.prefix(10)==window &&
                    window.suffix(10)==window
                );

              })
            );

        head.emit(1);
        head.emit(2);
        head.emit(3);
        head.emit(4);
        head.emit(5);
        head.emit(6);

        circuit.await();

        assertEquals(
          List.of(
            List.of(1),
            List.of(1, 2),
            List.of(1, 2),
            List.of(1, 2),
            List.of(2, 3),
            List.of(3, 4)
          ),
          first
        );

        assertEquals(
          List.of(
            List.of(1),
            List.of(1, 2),
            List.of(2, 3),
            List.of(3, 4),
            List.of(4, 5),
            List.of(5, 6)
          ),
          last
        );

        assertEquals(
          List.of(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of()
          ),
          firstZero
        );

        assertEquals(
          List.of(
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of(),
            List.of()
          ),
          lastZero
        );

        assertEquals(
          List.of(
            true,
            true,
            true,
            true,
            true,
            true
          ),
          full
        );

      } finally {

        circuit.close();

      }

    }

    /// First and last views are based on the current emission snapshot.
    @SpecRef("6.2.3")
    @Test
    void window_firstAndLastViews_useCurrentSnapshot() {

      final var circuit = cortex.circuit();

      try {

        final List< List< Integer > > first = new ArrayList<>();
        final List< List< Integer > > last = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(4)
            .pipe(
              circuit.pipe(window -> {

                first.add(
                  values(
                    window.prefix(2)
                  )
                );

                last.add(
                  values(
                    window.suffix(2)
                  )
                );

              })
            );

        head.emit(1);
        head.emit(2);
        head.emit(3);
        head.emit(4);
        head.emit(5);

        circuit.await();

        assertEquals(
          List.of(
            List.of(1),
            List.of(1, 2),
            List.of(1, 2),
            List.of(1, 2),
            List.of(2, 3)
          ),
          first
        );

        assertEquals(
          List.of(
            List.of(1),
            List.of(1, 2),
            List.of(2, 3),
            List.of(3, 4),
            List.of(4, 5)
          ),
          last
        );

      } finally {

        circuit.close();

      }

    }

    /// Duration Window rejects invalid duration and capacity.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void window_invalidDurationArguments_throwConfigurationError() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).window(
          null,
          1
        )
      );

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.flow(Integer.class).window(
          Duration.ZERO,
          1
        )
      );

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.flow(Integer.class).window(
          Duration.ofNanos(
            -1L
          ),
          1
        )
      );

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.flow(Integer.class).window(
          Duration.ofSeconds(
            Long.MAX_VALUE
          ),
          1
        )
      );

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.flow(Integer.class).window(
          Duration.ofSeconds(
            1L
          ),
          0
        )
      );

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.flow(Integer.class).window(
          Duration.ofSeconds(
            1L
          ),
          -1
        )
      );

    }

    /// Window state is independent for each Flow attachment.
    @SpecRef({"6.2", "6.2.3"})
    @Test
    void window_multipleAttachments_isolatesState() {

      final var circuit = cortex.circuit();

      try {

        final List< List< Integer > > a = new ArrayList<>();
        final List< List< Integer > > b = new ArrayList<>();

        final Flow< Integer, Window< Integer > > flow =
          cortex.flow(Integer.class).window(2);

        final Pipe< Integer > pa =
          flow.pipe(
            circuit.pipe(window -> a.add(values(window)))
          );

        final Pipe< Integer > pb =
          flow.pipe(
            circuit.pipe(window -> b.add(values(window)))
          );

        pa.emit(1);
        pa.emit(2);
        pb.emit(10);
        pa.emit(3);
        pb.emit(20);
        pb.emit(30);

        circuit.await();

        assertEquals(
          List.of(
            List.of(1),
            List.of(1, 2),
            List.of(2, 3)
          ),
          a
        );

        assertEquals(
          List.of(
            List.of(10),
            List.of(10, 20),
            List.of(20, 30)
          ),
          b
        );

      } finally {

        circuit.close();

      }

    }

    /// Window derived views reject negative counts.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void window_negativeViewCount_throwsIllegalArgumentException() {

      final var circuit = cortex.circuit();

      try {

        final List< Class< ? extends Throwable > > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(2)
            .pipe(
              circuit.pipe(window -> {

                try {

                  window.prefix(-1);

                } catch (final IllegalArgumentException e) {

                  captured.add(
                    e.getClass()
                  );

                }

                try {

                  window.suffix(-1);

                } catch (final IllegalArgumentException e) {

                  captured.add(
                    e.getClass()
                  );

                }

                try {

                  window.skip(
                    -1
                  );

                } catch (final IllegalArgumentException e) {

                  captured.add(
                    e.getClass()
                  );

                }

                try {

                  window.trim(
                    -1
                  );

                } catch (final IllegalArgumentException e) {

                  captured.add(
                    e.getClass()
                  );

                }

                try {

                  window.slice(
                    -1,
                    1
                  );

                } catch (final IllegalArgumentException e) {

                  captured.add(
                    e.getClass()
                  );

                }

                try {

                  window.slice(
                    0,
                    -1
                  );

                } catch (final IllegalArgumentException e) {

                  captured.add(
                    e.getClass()
                  );

                }

              })
            );

        head.emit(1);

        circuit.await();

        assertEquals(
          List.of(
            IllegalArgumentException.class,
            IllegalArgumentException.class,
            IllegalArgumentException.class,
            IllegalArgumentException.class,
            IllegalArgumentException.class,
            IllegalArgumentException.class
          ),
          captured
        );

      } finally {

        circuit.close();

      }

    }

    /// Root Window count must be positive.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void window_nonPositiveCapacity_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.flow(Integer.class).window(0)
      );

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.flow(Integer.class).window(-1)
      );

    }

    /// Window terminal aggregators reject absent functions.
    @SpecRef("15.2")
    @Test
    void window_nullTerminalFunctions_throwNullPointerException() {

      final var circuit = cortex.circuit();

      try {

        final List< Class< ? extends Throwable > > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(2)
            .pipe(
              circuit.pipe(window -> {

                try {
                  window.all(null);
                } catch (final NullPointerException e) {
                  captured.add(e.getClass());
                }
                try {
                  window.any(null);
                } catch (final NullPointerException e) {
                  captured.add(e.getClass());
                }
                try {
                  window.none(null);
                } catch (final NullPointerException e) {
                  captured.add(e.getClass());
                }
                try {
                  window.count(null);
                } catch (final NullPointerException e) {
                  captured.add(e.getClass());
                }
                try {
                  window.fold(null, (a, _) -> a);
                } catch (final NullPointerException e) {
                  captured.add(e.getClass());
                }
                try {
                  window.fold("seed", null);
                } catch (final NullPointerException e) {
                  captured.add(e.getClass());
                }
                try {
                  window.reduce(null, Integer::sum);
                } catch (final NullPointerException e) {
                  captured.add(e.getClass());
                }
                try {
                  window.reduce(0, null);
                } catch (final NullPointerException e) {
                  captured.add(e.getClass());
                }

              })
            );

        head.emit(1);

        circuit.await();

        assertEquals(
          8,
          captured.size()
        );

        assertTrue(
          captured.stream().allMatch(c -> c==NullPointerException.class)
        );

      } finally {

        circuit.close();

      }

    }

    /// Reversed Window view uses the current emission snapshot.
    @SpecRef("6.2.3")
    @Test
    void window_reversedView_usesCurrentSnapshot() {

      final var circuit = cortex.circuit();

      try {

        final List< List< Integer > > captured = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(4)
            .pipe(
              circuit.pipe(window ->
                captured.add(
                  values(
                    window.reverse()
                  )
                ))
            );

        head.emit(1);
        head.emit(2);
        head.emit(3);
        head.emit(4);
        head.emit(5);

        circuit.await();

        assertEquals(
          List.of(
            List.of(1),
            List.of(2, 1),
            List.of(3, 2, 1),
            List.of(4, 3, 2, 1),
            List.of(5, 4, 3, 2)
          ),
          captured
        );

      } finally {

        circuit.close();

      }

    }

    /// Rolling Window emits values in oldest-first encounter order.
    @SpecRef("6.2.3")
    @Test
    void window_rollingEmissions_useOldestFirstOrder() {

      final var circuit = cortex.circuit();

      try {

        final List< List< Integer > > captured = new ArrayList<>();

        final Pipe< Window< Integer > > sink =
          circuit.pipe(window -> captured.add(values(window)));

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(3)
            .pipe(sink);

        head.emit(1);
        head.emit(2);
        head.emit(3);
        head.emit(4);
        head.emit(5);

        circuit.await();

        assertEquals(
          List.of(
            List.of(1),
            List.of(1, 2),
            List.of(1, 2, 3),
            List.of(2, 3, 4),
            List.of(3, 4, 5)
          ),
          captured
        );

      } finally {

        circuit.close();

      }

    }

    /// A same-Circuit emission queued inside a callback does not advance
    /// the Window observed by that callback re-entrantly.
    @SpecRef("6.4.1")
    @Test
    void window_sameCircuitQueuedEmission_preservesCallbackSnapshot() {

      final var circuit = cortex.circuit();

      try {

        final var headReference = new AtomicReference< Pipe< Integer > >();
        final List< List< Integer > > snapshots = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(2)
            .pipe(
              circuit.pipe(window -> {
                snapshots.add(values(window));

                if (snapshots.size()==1) {
                  headReference.get().emit(2);
                  snapshots.add(values(window));
                }
              })
            );

        headReference.set(head);
        head.emit(1);
        circuit.await();

        assertEquals(
          List.of(
            List.of(1),
            List.of(1),
            List.of(1, 2)
          ),
          snapshots
        );

      } finally {

        circuit.close();

      }

    }

    /// This matrix starts from a three-value window, derives views in both directions, and observes
    /// each result inside the callback lease. It protects view composition from accidentally reverting
    /// to the original window or losing the current encounter order.
    ///
    /// Window slice and reverse operate in current encounter order.
    @SpecRef("6.2.3")
    @Test
    void window_sliceAndReverse_preserveViewSemantics() {

      final var circuit = cortex.circuit();

      try {

        final List< List< Integer > > slice = new ArrayList<>();
        final List< List< Integer > > skip = new ArrayList<>();
        final List< List< Integer > > trim = new ArrayList<>();
        final List< List< Integer > > reversed = new ArrayList<>();
        final List< List< Integer > > reversedFirst = new ArrayList<>();
        final List< List< Integer > > reversedLast = new ArrayList<>();
        final List< List< Integer > > reversedSlice = new ArrayList<>();
        final List< List< Integer > > reversedSkip = new ArrayList<>();
        final List< List< Integer > > reversedTrim = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(3)
            .pipe(
              circuit.pipe(window -> {

                slice.add(
                  values(
                    window.slice(
                      1,
                      2
                    )
                  )
                );

                skip.add(
                  values(
                    window.skip(
                      1
                    )
                  )
                );

                trim.add(
                  values(
                    window.trim(
                      1
                    )
                  )
                );

                final Window< Integer > reverse =
                  window.reverse();

                reversed.add(
                  values(
                    reverse
                  )
                );

                reversedFirst.add(
                  values(
                    reverse.prefix(
                      2
                    )
                  )
                );

                reversedLast.add(
                  values(
                    reverse.suffix(
                      2
                    )
                  )
                );

                reversedSlice.add(
                  values(
                    reverse.slice(
                      1,
                      2
                    )
                  )
                );

                reversedSkip.add(
                  values(
                    reverse.skip(
                      1
                    )
                  )
                );

                reversedTrim.add(
                  values(
                    reverse.trim(
                      1
                    )
                  )
                );

              })
            );

        head.emit(1);
        head.emit(2);
        head.emit(3);
        head.emit(4);
        head.emit(5);

        circuit.await();

        assertEquals(
          List.of(
            List.of(),
            List.of(2),
            List.of(2, 3),
            List.of(3, 4),
            List.of(4, 5)
          ),
          slice
        );

        assertEquals(
          List.of(
            List.of(),
            List.of(2),
            List.of(2, 3),
            List.of(3, 4),
            List.of(4, 5)
          ),
          skip
        );

        assertEquals(
          List.of(
            List.of(),
            List.of(1),
            List.of(1, 2),
            List.of(2, 3),
            List.of(3, 4)
          ),
          trim
        );

        assertEquals(
          List.of(
            List.of(1),
            List.of(2, 1),
            List.of(3, 2, 1),
            List.of(4, 3, 2),
            List.of(5, 4, 3)
          ),
          reversed
        );

        assertEquals(
          List.of(
            List.of(1),
            List.of(2, 1),
            List.of(3, 2),
            List.of(4, 3),
            List.of(5, 4)
          ),
          reversedFirst
        );

        assertEquals(
          List.of(
            List.of(1),
            List.of(2, 1),
            List.of(2, 1),
            List.of(3, 2),
            List.of(4, 3)
          ),
          reversedLast
        );

        assertEquals(
          List.of(
            List.of(),
            List.of(1),
            List.of(2, 1),
            List.of(3, 2),
            List.of(4, 3)
          ),
          reversedSlice
        );

        assertEquals(
          List.of(
            List.of(),
            List.of(1),
            List.of(2, 1),
            List.of(3, 2),
            List.of(4, 3)
          ),
          reversedSkip
        );

        assertEquals(
          List.of(
            List.of(),
            List.of(2),
            List.of(3, 2),
            List.of(4, 3),
            List.of(5, 4)
          ),
          reversedTrim
        );

      } finally {

        circuit.close();

      }

    }

    /// Non-commutative product and string-fold results make encounter order observable while the same
    /// callback also checks predicate terminals. One scenario therefore guards the complete terminal
    /// family against inconsistent traversal of a Window.
    ///
    /// Window terminal aggregators evaluate in encounter order.
    @SpecRef("6.2.3")
    @Test
    void window_terminalAggregators_followEncounterOrder() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > sums = new ArrayList<>();
        final List< Integer > products = new ArrayList<>();
        final List< String > folded = new ArrayList<>();
        final List< Integer > evenCount = new ArrayList<>();
        final List< Boolean > anyOdd = new ArrayList<>();
        final List< Boolean > allPositive = new ArrayList<>();
        final List< Boolean > noneNegative = new ArrayList<>();
        final List< Boolean > emptyFlag = new ArrayList<>();

        final Pipe< Integer > head =
          cortex.flow(Integer.class)
            .window(3)
            .pipe(
              circuit.pipe(window -> {

                sums.add(
                  window.reduce(
                    0,
                    Integer::sum
                  )
                );

                products.add(
                  window.reduce(
                    1,
                    (a, b) -> a * b
                  )
                );

                folded.add(
                  window.fold(
                    "",
                    (acc, v) -> acc + v
                  )
                );

                evenCount.add(
                  window.count(
                    v -> v % 2==0
                  )
                );

                anyOdd.add(
                  window.any(
                    v -> v % 2!=0
                  )
                );

                allPositive.add(
                  window.all(
                    v -> v > 0
                  )
                );

                noneNegative.add(
                  window.none(
                    v -> v < 0
                  )
                );

                emptyFlag.add(
                  window.isEmpty()
                );

              })
            );

        head.emit(1);
        head.emit(2);
        head.emit(3);
        head.emit(4);

        circuit.await();

        assertEquals(
          List.of(1, 3, 6, 9),
          sums
        );

        assertEquals(
          List.of(1, 2, 6, 24),
          products
        );

        assertEquals(
          List.of("1", "12", "123", "234"),
          folded
        );

        assertEquals(
          List.of(0, 1, 1, 2),
          evenCount
        );

        assertEquals(
          List.of(true, true, true, true),
          anyOdd
        );

        assertEquals(
          List.of(true, true, true, true),
          allPositive
        );

        assertEquals(
          List.of(true, true, true, true),
          noneNegative
        );

        assertEquals(
          List.of(false, false, false, false),
          emptyFlag
        );

      } finally {

        circuit.close();

      }

    }

  }

}
