// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// Comprehensive tests for the `Fiber<E>` interface and its operators.
///
/// Fibers are same-type per-emission processing recipes. This test class covers:
/// - Every stateless and comparison operator
/// - Every stateful operator, including count- and duration-based variants
/// - Fiber/Flow composition and Pipe/Cell attachment
/// - Null-argument and illegal-argument validation
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.0/SPEC.md")
final class FiberContractTest
  extends TestSupport {

  private Cortex cortex;

  /// Creates a fiber-attached pipe on the given circuit, feeding into a list.
  private static Pipe< Integer > attach(
    final Circuit circuit,
    final Fiber< Integer > fiber,
    final List< Integer > sink
  ) {

    return fiber.pipe(circuit.pipe(sink::add));

  }

  /// Cortex#fiber(Class) accepts a type witness.
  @Test
  void fiber_classWitness_returnsFiber() {

    final var fiber = cortex.fiber(Integer.class);
    assertEquals(
      cortex.< Integer > fiber().getClass(),
      fiber.getClass()
    );

  }


  // ============================================================
  // Cortex factories
  // ============================================================

  /// Cortex#fiber returns an identity Fiber.
  @SpecRef("6.2")
  @Test
  void fiber_cortexFactory_returnsIdentityFiber() {

    final var a = cortex.< Integer > fiber();
    final var b = cortex.< Integer > fiber();

    assertEquals(a.getClass(), b.getClass());

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }


  // ============================================================
  // Stateless operators
  // ============================================================

  @Nested
  final class Composition {

    /// Chained comparison stages each apply their own bound independently.
    @SpecRef("6.2.4")
    @Test
    void comparison_chainedBounds_filtersToTheirIntersection() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();
        final var pipe =
          attach(
            circuit,
            cortex.fiber(Integer.class).above(0).below(10),
            captured
          );

        pipe.emit(-1);
        pipe.emit(0);
        pipe.emit(5);
        pipe.emit(10);
        pipe.emit(12);
        circuit.await();

        assertEquals(List.of(5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Flow#fiber attaches a Fiber at the Flow output/input boundary.
    @SpecRef("6.2.6")
    @Test
    void fiber_attachedToFlow_executesAtBoundary() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        // Peek inside the fiber — sees post-fiber output.
        final var fiber =
          cortex.fiber(Integer.class)
            .guard(v -> v > 0)
            .diff()
            .peek(captured::add);

        final var flow =
          cortex.flow(Integer.class)
            .fiber(fiber);

        final Pipe< Integer > sink = circuit.pipe();
        final Pipe< Integer > target = flow.pipe(sink);

        target.emit(1);
        target.emit(1);  // diff dropped
        target.emit(-2); // guard dropped
        target.emit(3);
        target.emit(3);  // diff dropped

        circuit.await();

        assertEquals(List.of(1, 3), captured);

      } finally {

        circuit.close();

      }

    }

    /// Fiber composition rejects a Fiber from an incompatible provider.
    @SpecRef("15.1")
    @Test
    void fiber_foreignNext_throwsFault() {

      final Fiber< Integer > foreign = foreignProviderStub(Fiber.class);

      assertThrows(
        Fault.class,
        () -> cortex.fiber(Integer.class).fiber(foreign)
      );

    }

    /// Guard, diff, and peek execute in declaration order.
    @SpecRef("6.2.5")
    @Test
    void fiber_guardDiffPeek_executesInOrder() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .guard(v -> v > 0)
            .diff()
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(1);   // diff dropped
        pipe.emit(-2);  // guard dropped
        pipe.emit(3);
        pipe.emit(3);   // diff dropped
        pipe.emit(4);

        circuit.await();

        assertEquals(List.of(1, 3, 4), captured);

      } finally {

        circuit.close();

      }

    }

    /// Composed stateful Fiber state is independent per attachment.
    @SpecRef({"6.2", "6.2.3"})
    @Test
    void fiber_multipleAttachments_isolatesState() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > a = new ArrayList<>();
        final List< Integer > b = new ArrayList<>();

        final var composed =
          cortex.fiber(Integer.class)
            .guard(v -> v > 0)
            .fiber(cortex.fiber(Integer.class).diff());

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

    /// Fiber#fiber rejects an absent next Fiber.
    @SpecRef("15.2")
    @Test
    void fiber_nullNext_throwsNullPointerException() {

      final var fiber = cortex.< Integer > fiber();

      assertThrows(
        NullPointerException.class,
        () -> fiber.fiber(null)
      );

    }

    /// Composed Fibers route through both segments in order.
    @SpecRef("6.2.6")
    @Test
    void fiber_twoComposedSegments_executeInOrder() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var first =
          cortex.fiber(Integer.class).guard(v -> v > 0);

        final var second =
          cortex.fiber(Integer.class).diff();

        final Pipe< Integer > sink = circuit.pipe(captured::add);
        final Pipe< Integer > target = first.fiber(second).pipe(sink);

        target.emit(1);
        target.emit(1);  // diff dropped
        target.emit(-2); // guard dropped
        target.emit(3);
        target.emit(3);  // diff dropped
        target.emit(4);

        circuit.await();

        assertEquals(List.of(1, 3, 4), captured);

      } finally {

        circuit.close();

      }

    }

    /// Cortex Flow wrapping executes a Fiber before downstream delivery.
    @SpecRef("6.2.6")
    @Test
    void fiber_wrappedByFlow_executesBeforeDownstream() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        // Peek inside the fiber — forward semantics, sees filtered output.
        final var fiber =
          cortex.fiber(Integer.class)
            .guard(v -> v > 0)
            .peek(captured::add);

        final Pipe< Integer > target = cortex
          .flow(fiber)
          .pipe(circuit.pipe());

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

    /// Fiber#pipe attaches operators before the target Pipe.
    @SpecRef("6.2.6")
    @Test
    void pipe_attachedFiber_executesBeforeTarget() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final Pipe< Integer > target = cortex.fiber(Integer.class)
          .guard(v -> v > 0)
          .diff()
          .peek(captured::add)
          .pipe(circuit.pipe());

        target.emit(1);
        target.emit(1);   // diff dropped
        target.emit(-2);  // guard dropped
        target.emit(3);
        target.emit(3);   // diff dropped
        target.emit(4);

        circuit.await();

        assertEquals(List.of(1, 3, 4), captured);

      } finally {

        circuit.close();

      }

    }

    /// Fiber attachment executes on the target Pipe's Circuit.
    @SpecRef({"5.1", "6.2.6"})
    @Test
    void pipe_crossCircuitTarget_executesOnTargetCircuit() {

      final var circuitSink = cortex.circuit();
      final var circuitFiber = cortex.circuit();

      try {

        final List< Integer > terminal = new ArrayList<>();
        final List< Integer > peeked = new ArrayList<>();

        // Terminal receptor lives on circuitSink.
        final Pipe< Integer > sink = circuitSink.pipe(terminal::add);

        // Hop to circuitFiber, then attach the fiber — fiber runs on circuitFiber.
        final var fiber =
          cortex.fiber(Integer.class)
            .guard(v -> v > 0)
            .diff()
            .peek(peeked::add);

        final Pipe< Integer > target = fiber.pipe(circuitFiber.pipe(sink));

        assertNotNull(target);

        for (final int v : new int[]{1, 1, -2, 3, 3, 4}) {
          target.emit(v);
        }

        // Drain circuitFiber first — it produces the cross-circuit emits,
        // then circuitSink drains the terminal receptor invocations.
        circuitFiber.await();
        circuitSink.await();

        assertEquals(List.of(1, 3, 4), peeked);
        assertEquals(List.of(1, 3, 4), terminal);

      } finally {

        circuitFiber.close();
        circuitSink.close();

      }

    }

    /// Fiber materialization preserves emission type.
    @SpecRef("6.2.1")
    @Test
    void pipe_fiberMaterialization_preservesType() {

      final var circuit = cortex.circuit();

      try {

        final List< String > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(String.class)
            .peek(captured::add);

        final Pipe< String > sink = circuit.pipe();
        final Pipe< String > target = fiber.pipe(sink);

        target.emit("a");
        target.emit("b");

        circuit.await();

        assertEquals(List.of("a", "b"), captured);

      } finally {

        circuit.close();

      }

    }

    /// Fiber#pipe rejects an absent target.
    @SpecRef("15.2")
    @Test
    void pipe_nullTarget_throwsNullPointerException() {

      final var fiber = cortex.< Integer > fiber();

      assertThrows(
        NullPointerException.class,
        () -> fiber.pipe(
          (Pipe< Integer >) null
        )
      );

    }

  }


  // ============================================================
  // Natural-order comparison operators
  // ============================================================

  @Nested
  final class NaturalOrdering {

    // ----- Positive path: same behavior as comparator-taking sibling -----

    /// Above passes only values strictly greater than its bound.
    @SpecRef("6.2.4")
    @Test
    void above_greaterValues_passesOnlyMatches() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .above(0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-1);
        pipe.emit(0);
        pipe.emit(1);
        pipe.emit(5);

        circuit.await();

        assertEquals(List.of(1, 5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Natural-order above rejects a non-Comparable bound eagerly.
    @Test
    void above_nonComparableBound_throwsClassCastException() {

      assertThrows(
        ClassCastException.class,
        () -> cortex.fiber(Object.class).above(new Object())
      );

    }

    /// Below passes only values strictly less than its bound.
    @SpecRef("6.2.4")
    @Test
    void below_lesserValues_passesOnlyMatches() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .below(5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(3);
        pipe.emit(5);
        pipe.emit(7);

        circuit.await();

        assertEquals(List.of(3), captured);

      } finally {

        circuit.close();

      }

    }

    /// Natural-order below rejects a non-Comparable bound eagerly.
    @Test
    void below_nonComparableBound_throwsClassCastException() {

      assertThrows(
        ClassCastException.class,
        () -> cortex.fiber(Object.class).below(new Object())
      );

    }

    /// Equal clamp bounds coerce every value to that bound.
    @SpecRef("6.2.4")
    @Test
    void clamp_equalBounds_coercesToConstant() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .clamp(5, 5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-1);
        pipe.emit(5);
        pipe.emit(99);

        circuit.await();

        assertEquals(List.of(5, 5, 5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Natural-order clamp rejects mutually incomparable bounds eagerly.
    @Test
    void clamp_mismatchedBoundTypes_throwsClassCastException() {

      assertThrows(
        ClassCastException.class,
        () -> cortex.fiber(Object.class).clamp(1, "x")
      );

    }

    /// Natural-order clamp rejects non-Comparable bounds eagerly.
    @Test
    void clamp_nonComparableBounds_throwsClassCastException() {

      assertThrows(
        ClassCastException.class,
        () -> cortex.fiber(Object.class).clamp(new Object(), new Object())
      );

    }

    /// Clamp coerces values into its inclusive interval.
    @SpecRef("6.2.4")
    @Test
    void clamp_outsideValues_coercesIntoRange() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .clamp(0, 10)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-5);
        pipe.emit(5);
        pipe.emit(15);

        circuit.await();

        assertEquals(List.of(0, 5, 10), captured);

      } finally {

        circuit.close();

      }

    }

    /// Equal deadband bounds drop only the bound value.
    @SpecRef("6.2.4")
    @Test
    void deadband_equalBounds_dropsOnlyBound() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .deadband(5, 5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int v : new int[]{3, 5, 7}) pipe.emit(v);

        circuit.await();

        assertEquals(List.of(3, 7), captured);

      } finally {

        circuit.close();

      }

    }

    // ----- Fail fast: chain-build CCE when E is not Comparable -----
    //
    // Object is not Comparable. The CCE must surface at the operator call
    // site (no circuit, no emit) — proving the eager probe inside
    // naturalOrder(probe) fired before the chain even materialized.

    /// Deadband drops its inclusive interval and passes outside values.
    @SpecRef("6.2.4")
    @Test
    void deadband_insideAndOutsideValues_dropsInclusiveBand() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .deadband(3, 7)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int v : new int[]{1, 3, 5, 7, 9}) pipe.emit(v);

        circuit.await();

        assertEquals(List.of(1, 9), captured);

      } finally {

        circuit.close();

      }

    }

    /// Natural-order deadband rejects mutually incomparable bounds eagerly.
    @Test
    void deadband_mismatchedBoundTypes_throwsClassCastException() {

      assertThrows(
        ClassCastException.class,
        () -> cortex.fiber(Object.class).deadband(1, "x")
      );

    }

    /// Natural-order deadband rejects non-Comparable bounds eagerly.
    @Test
    void deadband_nonComparableBounds_throwsClassCastException() {

      assertThrows(
        ClassCastException.class,
        () -> cortex.fiber(Object.class).deadband(new Object(), new Object())
      );

    }

    /// Max passes values at or below its inclusive upper bound.
    @SpecRef("6.2.4")
    @Test
    void max_belowAtAboveBound_passesAtOrBelow() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .max(5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(3);
        pipe.emit(5);
        pipe.emit(7);

        circuit.await();

        assertEquals(List.of(3, 5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Natural-order max rejects a non-Comparable bound eagerly.
    @Test
    void max_nonComparableBound_throwsClassCastException() {

      assertThrows(
        ClassCastException.class,
        () -> cortex.fiber(Object.class).max(new Object())
      );

    }

    /// Min passes values at or above its inclusive lower bound.
    @SpecRef("6.2.4")
    @Test
    void min_belowAtAboveBound_passesAtOrAbove() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .min(5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(3);
        pipe.emit(5);
        pipe.emit(7);

        circuit.await();

        assertEquals(List.of(5, 7), captured);

      } finally {

        circuit.close();

      }

    }

    /// Natural-order min rejects a non-Comparable bound eagerly.
    @Test
    void min_nonComparableBound_throwsClassCastException() {

      assertThrows(
        ClassCastException.class,
        () -> cortex.fiber(Object.class).min(new Object())
      );

    }

    /// Equal range bounds pass only the bound value.
    @SpecRef("6.2.4")
    @Test
    void range_equalBounds_passesOnlyBound() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .range(5, 5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int v : new int[]{3, 5, 7}) pipe.emit(v);

        circuit.await();

        assertEquals(List.of(5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Range passes its inclusive interval and drops outside values.
    @SpecRef("6.2.4")
    @Test
    void range_insideAndOutsideValues_passesInclusiveInterval() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .range(3, 7)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int i = 1; i <= 10; i++) pipe.emit(i);

        circuit.await();

        assertEquals(List.of(3, 4, 5, 6, 7), captured);

      } finally {

        circuit.close();

      }

    }

    /// Natural-order range rejects mutually incomparable bounds eagerly.
    @Test
    void range_mismatchedBoundTypes_throwsClassCastException() {

      assertThrows(
        ClassCastException.class,
        () -> cortex.fiber(Object.class).range(1, "x")
      );

    }

    /// Natural-order range rejects non-Comparable bounds eagerly.
    @Test
    void range_nonComparableBounds_throwsClassCastException() {

      assertThrows(
        ClassCastException.class,
        () -> cortex.fiber(Object.class).range(new Object(), new Object())
      );

    }

  }


  // ============================================================
  // Stateful operators
  // ============================================================

  @Nested
  final class Stateful {

    /// Chance with probability one passes every emission.
    @SpecRef("6.2.3")
    @Test
    void chance_probabilityOne_passesAll() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        // Probability 1.0 passes every emission.
        final var fiber =
          cortex.fiber(Integer.class)
            .chance(1.0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(3);

        circuit.await();

        assertEquals(List.of(1, 2, 3), captured);

      } finally {

        circuit.close();

      }

    }

    /// Chance with probability zero drops every emission.
    @SpecRef("6.2.3")
    @Test
    void chance_probabilityZero_dropsAll() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        // probability 0.0 drops every emission (SPEC §6.2.3)
        final var fiber =
          cortex.fiber(Integer.class)
            .chance(0.0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(3);

        circuit.await();

        assertEquals(List.of(), captured);

      } finally {

        circuit.close();

      }

    }

    /// Change emits only when the projected key changes.
    @SpecRef("6.2.3")
    @Test
    void change_projectedKeyChanges_emitsBoundaries() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .change(v -> v / 10)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(5);
        pipe.emit(7);   // same bucket 0 — dropped
        pipe.emit(13);  // bucket 1 — passes
        pipe.emit(18);  // same bucket — dropped
        pipe.emit(9);   // back to 0 — passes

        circuit.await();

        assertEquals(List.of(5, 13, 9), captured);

      } finally {

        circuit.close();

      }

    }

    /// Delay emits values displaced by its configured depth.
    @SpecRef("6.2.3")
    @Test
    void delay_positiveDepth_emitsLaggedValues() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .delay(2, 0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(10);
        pipe.emit(20);
        pipe.emit(30);
        pipe.emit(40);

        circuit.await();

        // First 2 emissions yield initial (0); then values delayed by 2.
        assertEquals(List.of(0, 0, 10, 20), captured);

      } finally {

        circuit.close();

      }

    }

    /// Diff drops consecutive value-equal emissions.
    @SpecRef("6.2.3")
    @Test
    void diff_consecutiveDuplicates_dropsRepeats() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .diff()
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(1);  // dup — dropped
        pipe.emit(2);
        pipe.emit(2);  // dup — dropped
        pipe.emit(1);

        circuit.await();

        assertEquals(List.of(1, 2, 1), captured);

      } finally {

        circuit.close();

      }

    }

    /// Diff compares ordinary value equality rather than canonical
    /// identity or Java reference identity.
    @SpecRef({"1.2", "6.2.3"})
    @Test
    void diff_equalDistinctValues_dropsSecondValue() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();
        final Integer first = 1_000;
        final Integer equalButDistinct = 1_000;

        assertNotSame(first, equalButDistinct);

        final var pipe =
          attach(
            circuit,
            cortex.fiber(Integer.class).diff().peek(captured::add),
            new ArrayList<>()
          );

        pipe.emit(first);
        pipe.emit(equalButDistinct);
        circuit.await();

        assertEquals(List.of(first), captured);

      } finally {

        circuit.close();

      }

    }

    /// Seeded diff compares its first emission with the initial value.
    @SpecRef("6.2.3")
    @Test
    void diff_seededInitial_dropsMatchingFirstValue() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .diff(5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(5);  // equals initial — dropped
        pipe.emit(7);
        pipe.emit(7);  // dup — dropped
        pipe.emit(5);

        circuit.await();

        assertEquals(List.of(7, 5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Bounded distinct permits a value after FIFO eviction.
    @SpecRef("6.2.3")
    @Test
    void distinct_boundedHistory_allowsAfterEviction() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .distinct(3)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);  // window: [1]        — passes
        pipe.emit(2);  // window: [1,2]      — passes
        pipe.emit(1);  // in window           — suppressed
        pipe.emit(3);  // window: [1,2,3]    — passes
        pipe.emit(4);  // full: evict 1, window: [2,3,4]  — passes
        pipe.emit(1);  // 1 evicted — passes again

        circuit.await();

        assertEquals(List.of(1, 2, 3, 4, 1), captured);

      } finally {

        circuit.close();

      }

    }

    /// Unbounded distinct drops every historically repeated value.
    @SpecRef("6.2.3")
    @Test
    void distinct_unboundedHistory_dropsAllRepeats() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .distinct()
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(1);  // already seen — suppressed
        pipe.emit(3);
        pipe.emit(2);  // already seen — suppressed
        pipe.emit(3);  // already seen — suppressed

        circuit.await();

        assertEquals(List.of(1, 2, 3), captured);

      } finally {

        circuit.close();

      }

    }

    /// DropWhile drops leading matches then passes permanently.
    @SpecRef("6.2.3")
    @Test
    void dropWhile_leadingMatches_dropsUntilFirstMiss() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .dropWhile(v -> v < 5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(3);
        pipe.emit(5);
        pipe.emit(2);  // latch released — passes
        pipe.emit(8);

        circuit.await();

        assertEquals(List.of(5, 2, 8), captured);

      } finally {

        circuit.close();

      }

    }

    /// Edge emits the current value on matching transitions.
    @SpecRef("6.2.3")
    @Test
    void edge_matchingTransitions_emitsCurrentValues() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .edge(0, (prev, curr) -> curr > prev)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);   // 1>0 — passes
        pipe.emit(0);   // 0>1 false — dropped (but prev advances)
        pipe.emit(3);   // 3>0 — passes
        pipe.emit(2);   // 2>3 false — dropped

        circuit.await();

        assertEquals(List.of(1, 3), captured);

      } finally {

        circuit.close();

      }

    }

    /// Duration-based every advances by whole interval slots after an
    /// overrun instead of re-anchoring at the late arrival.
    @SpecRef("6.2.3")
    @Test
    void every_durationOverrun_preservesIntervalPhase() throws InterruptedException {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();
        final var fiber =
          cortex.fiber(Integer.class)
            .every(Duration.ofMillis(250L))
            .peek(captured::add);
        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        circuit.await();

        Thread.sleep(600L);
        pipe.emit(2);
        circuit.await();

        Thread.sleep(180L);
        pipe.emit(3);
        circuit.await();

        assertEquals(List.of(2, 3), captured);

      } finally {

        circuit.close();

      }

    }

    /// Duration-based every pass after a whole interval elapses.
    @SpecRef("6.2.3")
    @Test
    void every_elapsedDuration_passesAfterInterval()
      throws InterruptedException {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .every(Duration.ofMillis(1L))
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        circuit.await();

        Thread.sleep(20L);

        pipe.emit(2);
        circuit.await();

        assertEquals(List.of(2), captured);

      } finally {

        circuit.close();

      }

    }

    /// Duration-based every drops values within its initial interval.
    @SpecRef("6.2.3")
    @Test
    void every_initialDuration_dropsWithinInterval() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .every(Duration.ofDays(1L))
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(3);

        circuit.await();

        assertEquals(List.of(), captured);

      } finally {

        circuit.close();

      }

    }

    /// Interval-based every passes each Nth emission.
    @SpecRef("6.2.3")
    @Test
    void every_positiveInterval_passesEveryNth() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .every(3)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int i = 1; i <= 9; i++) pipe.emit(i);

        circuit.await();

        // 3rd, 6th, 9th values
        assertEquals(List.of(3, 6, 9), captured);

      } finally {

        circuit.close();

      }

    }

    /// Stateful guard compares against the previous accepted value.
    @SpecRef("6.2.3")
    @Test
    void guard_previousAcceptedValue_comparesPairwise() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .guard(0, (prev, curr) -> curr > prev)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(5);  // 5>0 — pass, prev=5
        pipe.emit(3);  // 3>5 false — drop, prev stays 5
        pipe.emit(7);  // 7>5 — pass, prev=7
        pipe.emit(7);  // 7>7 false — drop
        pipe.emit(9);  // 9>7 — pass

        circuit.await();

        assertEquals(List.of(5, 7, 9), captured);

      } finally {

        circuit.close();

      }

    }

    /// A changed heartbeat value clears the duplicate-run timer.
    @SpecRef("6.2.3")
    @Test
    void heartbeat_changedValue_clearsSilenceAnchor()
      throws InterruptedException {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .heartbeat(Duration.ofMillis(1L))
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(3);  // changed — passes, unanchors
        circuit.await();

        Thread.sleep(20L);

        // Lazy semantics: silence is measured from the start of a duplicate run,
        // not from the last emit. The first duplicate after a long gap only
        // anchors the run — it does NOT heartbeat — so nothing is emitted here.
        pipe.emit(3);
        circuit.await();

        assertEquals(List.of(3), captured);

      } finally {

        circuit.close();

      }

    }

    /// Heartbeat re-emits the held value instance at keep-alive time.
    @SpecRef("6.2.3")
    @Test
    void heartbeat_duplicateTrigger_reemitsHeldInstance()
      throws InterruptedException {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .heartbeat(Duration.ofMillis(1L))
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        // Values above the Integer cache (127) box to distinct instances, so
        // these compare equal by value yet differ by reference.
        final Integer held = 1000;
        final Integer dup = 1000;
        assertNotSame(held, dup);  // premise: equal but distinct references

        pipe.emit(held);  // changed — passes, holds this instance
        circuit.await();

        pipe.emit(dup);   // first duplicate — anchors run, dropped
        circuit.await();

        Thread.sleep(20L);

        pipe.emit(1000);  // duplicate after silence — heartbeat
        circuit.await();

        // Heartbeat re-emits the HELD instance, not the equal-but-distinct
        // duplicate that triggered it.
        assertEquals(List.of(1000, 1000), captured);
        assertSame(held, captured.get(1));

      } finally {

        circuit.close();

      }

    }

    /// Heartbeat emits a duplicate after maximum silence elapses.
    @SpecRef("6.2.3")
    @Test
    void heartbeat_elapsedSilence_emitsDuplicate()
      throws InterruptedException {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .heartbeat(Duration.ofMillis(1L))
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(7);  // changed — passes, unanchors
        circuit.await();

        pipe.emit(7);  // first duplicate — anchors run, dropped
        circuit.await();

        Thread.sleep(20L);

        pipe.emit(7);  // duplicate, silence elapsed — heartbeat
        circuit.await();

        assertEquals(List.of(7, 7), captured);

      } finally {

        circuit.close();

      }

    }

    /// Heartbeat drops duplicates while maximum silence has not elapsed.
    @SpecRef("6.2.3")
    @Test
    void heartbeat_unelapsedSilence_dropsDuplicates() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        // Heartbeat window so large it never fires — reduces to plain diff.
        final var fiber =
          cortex.fiber(Integer.class)
            .heartbeat(Duration.ofDays(1L))
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);  // changed — passes
        pipe.emit(1);  // first duplicate — anchors run, dropped
        pipe.emit(1);  // duplicate within window — dropped
        pipe.emit(2);  // changed — passes, unanchors
        pipe.emit(2);  // first duplicate — anchors run, dropped
        pipe.emit(1);  // changed (differs from 2) — passes

        circuit.await();

        assertEquals(List.of(1, 2, 1), captured);

      } finally {

        circuit.close();

      }

    }

    /// High emits only new running maximum values.
    @SpecRef("6.2.4")
    @Test
    void high_newMaximum_emitsRunningRecords() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .high(Integer::compareTo)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int v : new int[]{3, 5, 2, 7, 6, 9}) pipe.emit(v);

        circuit.await();

        assertEquals(List.of(3, 5, 7, 9), captured);

      } finally {

        circuit.close();

      }

    }

    /// Hysteresis emits from entry until its exit predicate matches.
    @SpecRef("6.2.3")
    @Test
    void hysteresis_enterAndExit_latchesBetweenThresholds() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .hysteresis(v -> v >= 10, v -> v <= 2)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(5);   // inactive — drop
        pipe.emit(10);  // enter — pass
        pipe.emit(8);   // active — pass
        pipe.emit(5);   // active — pass
        pipe.emit(2);   // exit — drop (exit value suppressed)
        pipe.emit(7);   // inactive — drop
        pipe.emit(12);  // re-enter — pass

        circuit.await();

        assertEquals(List.of(10, 8, 5, 12), captured);

      } finally {

        circuit.close();

      }

    }

    /// Inhibit drops its refractory count after each passing emission.
    @SpecRef("6.2.3")
    @Test
    void inhibit_refractoryCount_suppressesAfterPass() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .inhibit(2)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int i = 1; i <= 7; i++) pipe.emit(i);

        circuit.await();

        // 1 passes, 2-3 inhibited, 4 passes, 5-6 inhibited, 7 passes.
        assertEquals(List.of(1, 4, 7), captured);

      } finally {

        circuit.close();

      }

    }

    /// Integrate accepts absent initial state and restores it after firing.
    @SpecRef("6.2.3")
    @Test
    void integrate_absentInitialState_accumulatesAndResets() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .integrate(
              null,
              (state, value) -> state==null ? value:state + value,
              sum -> sum >= 3
            )
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(4);

        circuit.await();

        assertEquals(List.of(3, 4), captured);

      } finally {

        circuit.close();

      }

    }

    /// Integrate accumulates silently, emits on fire, then resets.
    @SpecRef("6.2.3")
    @Test
    void integrate_firePredicate_emitsAndResets() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .integrate(0, Integer::sum, sum -> sum >= 10)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(3);
        pipe.emit(4);
        pipe.emit(5);   // sum=12 — fires 12, resets
        pipe.emit(6);
        pipe.emit(7);   // sum=13 — fires 13

        circuit.await();

        assertEquals(List.of(12, 13), captured);

      } finally {

        circuit.close();

      }

    }

    /// Limit drops all emissions after its configured cap.
    @SpecRef("6.2.3")
    @Test
    void limit_emissionCap_dropsAfterLimit() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .limit(3)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int i = 1; i <= 5; i++) pipe.emit(i);

        circuit.await();

        assertEquals(List.of(1, 2, 3), captured);

      } finally {

        circuit.close();

      }

    }

    /// Low emits only new running minimum values.
    @SpecRef("6.2.4")
    @Test
    void low_newMinimum_emitsRunningRecords() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .low(Integer::compareTo)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int v : new int[]{5, 3, 7, 2, 4, 1}) pipe.emit(v);

        circuit.await();

        assertEquals(List.of(5, 3, 2, 1), captured);

      } finally {

        circuit.close();

      }

    }

    /// Pulse emits only on false-to-true predicate transitions.
    @SpecRef("6.2.3")
    @Test
    void pulse_risingPredicate_emitsOnTransition() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .pulse(v -> v > 5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(3);   // F — drop
        pipe.emit(7);   // T (rising) — pass
        pipe.emit(8);   // T (still) — drop
        pipe.emit(2);   // F — drop
        pipe.emit(9);   // T (rising) — pass

        circuit.await();

        assertEquals(List.of(7, 9), captured);

      } finally {

        circuit.close();

      }

    }

    /// Reduce passes absent accumulator state through until restored.
    @SpecRef("6.2.3")
    @Test
    void reduce_absentAccumulator_dropsUntilPresentResult() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();
        final List< Integer > previous = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .reduce(
              null,
              (state, value) -> {
                previous.add(state);
                return value==1 ? null:(state==null ? value:state + value);
              }
            )
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);

        circuit.await();

        assertEquals(Arrays.asList(null, null), previous);
        assertEquals(List.of(2), captured);

      } finally {

        circuit.close();

      }

    }

    /// Reduce emits each result of its running accumulation.
    @SpecRef("6.2.3")
    @Test
    void reduce_runningAccumulator_emitsEachResult() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .reduce(0, Integer::sum)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(3);

        circuit.await();

        assertEquals(List.of(1, 3, 6), captured);

      } finally {

        circuit.close();

      }

    }

    /// Relate advances previous input when an absent result is dropped.
    @SpecRef("6.2.3")
    @Test
    void relate_absentInitialAndResult_advancesPreviousInput() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .relate(null, (previous, current) -> previous==null ? null:current - previous)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(3);
        pipe.emit(8);

        circuit.await();

        assertEquals(List.of(5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Relate derives output from previous and current inputs.
    @SpecRef("6.2.3")
    @Test
    void relate_previousAndCurrent_emitsDerivedValue() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .relate(0, (prev, curr) -> curr - prev)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(10);  // 10-0 = 10
        pipe.emit(15);  // 15-10 = 5
        pipe.emit(13);  // 13-15 = -2

        circuit.await();

        assertEquals(List.of(10, 5, -2), captured);

      } finally {

        circuit.close();

      }

    }

    /// Rolling continues folding after absence and drops an absent aggregate.
    @SpecRef("6.2.3")
    @Test
    void rolling_absentIntermediateState_continuesFoldAndFilters() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .rolling(
              2,
              (state, value) -> value==2 ? null:(state==null ? value:state + value),
              0
            )
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(3);

        circuit.await();

        assertEquals(List.of(3), captured);

      } finally {

        circuit.close();

      }

    }

    /// Rolling emits overlapping aggregates after warm-up.
    @SpecRef("6.2.3")
    @Test
    void rolling_fullWindows_emitsSlidingAggregates() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .rolling(3, Integer::sum, 0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(3);  // window filled — sum=6
        pipe.emit(4);  // 2+3+4=9
        pipe.emit(5);  // 3+4+5=12

        circuit.await();

        assertEquals(List.of(6, 9, 12), captured);

      } finally {

        circuit.close();

      }

    }

    /// Skip drops its initial count then passes permanently.
    @SpecRef("6.2.3")
    @Test
    void skip_initialCount_dropsThenPasses() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .skip(2)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int i = 1; i <= 5; i++) pipe.emit(i);

        circuit.await();

        assertEquals(List.of(3, 4, 5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Steady emits a change after N equal confirmations.
    @SpecRef("6.2.3")
    @Test
    void steady_consecutiveEqualValues_emitsConfirmedChange() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .steady(3)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(5);  // 1st
        pipe.emit(5);  // 2nd
        pipe.emit(5);  // 3rd — fires 5
        pipe.emit(5);  // suppressed (same run, already emitted)
        pipe.emit(7);  // new run — counter resets

        circuit.await();

        assertEquals(List.of(5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Generalized steady compares each value with its run candidate.
    @SpecRef("6.2.3")
    @Test
    void steady_customEquality_usesCandidateComparison() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        // Treat values within 1 as same
        final var fiber =
          cortex.fiber(Integer.class)
            .steady(2, (candidate, curr) -> Math.abs(curr - candidate) <= 1)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(10);
        pipe.emit(11);   // within 1 of 10 — 2nd confirmation — fires 11

        circuit.await();

        assertEquals(List.of(11), captured);

      } finally {

        circuit.close();

      }

    }

    /// Streak drops matching emissions below its threshold.
    @SpecRef("6.2.3")
    @Test
    void streak_belowThreshold_holds() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .streak(5, v -> v < 0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-1);
        pipe.emit(-2);
        pipe.emit(-3);
        pipe.emit(-4);   // 4 of 5 matches — still no emission

        circuit.await();

        assertTrue(captured.isEmpty());

      } finally {

        circuit.close();

      }

    }

    /// A non-match resets the streak counter.
    @SpecRef("6.2.3")
    @Test
    void streak_interveningMiss_resetsCounter() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .streak(3, v -> v < 0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-1);   // 1st match
        pipe.emit(-2);   // 2nd match
        pipe.emit(7);   // miss — resets counter to 0
        pipe.emit(-3);   // 1st match of new run
        pipe.emit(-4);   // 2nd
        pipe.emit(-5);   // 3rd — fires -5

        circuit.await();

        assertEquals(List.of(-5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Streak counters are independent per materialization.
    @SpecRef("6.2.3")
    @Test
    void streak_multipleAttachments_isolatesCounters() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > capturedA = new ArrayList<>();
        final List< Integer > capturedB = new ArrayList<>();

        final var recipe =
          cortex.fiber(Integer.class)
            .streak(2, v -> v < 0);

        final var pipeA = recipe.pipe(circuit.pipe(capturedA::add));
        final var pipeB = recipe.pipe(circuit.pipe(capturedB::add));

        pipeA.emit(-1);    // A counter: 0 → 1
        pipeB.emit(-10);   // B counter (independent): 0 → 1
        pipeA.emit(-2);    // A: 1 → 2 — fires -2
        pipeB.emit(-20);   // B: 1 → 2 — fires -20

        circuit.await();

        assertEquals(List.of(-2), capturedA);
        assertEquals(List.of(-20), capturedB);

      } finally {

        circuit.close();

      }

    }

    /// Streak emits at its threshold and resets its counter.
    @SpecRef("6.2.3")
    @Test
    void streak_reachingThreshold_emitsAndResets() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .streak(3, v -> v < 0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-1);   // 1st match
        pipe.emit(-2);   // 2nd match
        pipe.emit(-3);   // 3rd — fires -3, count resets
        pipe.emit(-4);   // 1st of next run
        pipe.emit(-5);   // 2nd of next run
        pipe.emit(-6);   // 3rd — fires -6

        circuit.await();

        assertEquals(List.of(-3, -6), captured);

      } finally {

        circuit.close();

      }

    }

    /// Streak with threshold one emits every matching value.
    @SpecRef("6.2.3")
    @Test
    void streak_requiredOne_emitsEveryMatch() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .streak(1, v -> v < 0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-1);   // match — fires
        pipe.emit(7);   // miss
        pipe.emit(-2);   // match — fires
        pipe.emit(-3);   // match — fires

        circuit.await();

        assertEquals(List.of(-1, -2, -3), captured);

      } finally {

        circuit.close();

      }

    }

    /// A throwing streak predicate drops only that emission.
    @SpecRef({"6.2.3", "15.4"})
    @Test
    void streak_throwingPredicate_dropsAndPreservesCounter() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        // The throwing emission MUST be treated as no-decision: count stays at 1,
        // so the next match takes count to 2 and fires.
        final var fiber =
          cortex.fiber(Integer.class)
            .streak(
              2,
              v -> {
                if (v==999) {
                  throw new RuntimeException("boom");
                }
                return v < 0;
              }
            )
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-1);    // 1st match — count 0 → 1
        pipe.emit(999);   // predicate throws — drop, count stays at 1
        pipe.emit(-2);    // 2nd match — count 1 → 2 — fires -2

        circuit.await();

        assertEquals(
          List.of(-2),
          captured,
          "throwing predicate must be treated as no decision: counter unchanged"
        );

      } finally {

        circuit.close();

      }

    }

    /// TakeWhile passes until its first miss then drops permanently.
    @SpecRef("6.2.3")
    @Test
    void takeWhile_firstPredicateMiss_dropsPermanently() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .takeWhile(v -> v < 5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(3);
        pipe.emit(5);   // predicate false — latch closes
        pipe.emit(2);   // still dropped

        circuit.await();

        assertEquals(List.of(1, 3), captured);

      } finally {

        circuit.close();

      }

    }

    /// Tumble drops an absent batch result and resets to its identity.
    @SpecRef("6.2.3")
    @Test
    void tumble_absentBatchAggregate_dropsAndResets() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .tumble(
              2,
              (state, value) -> value==2 ? null:(state==null ? value:state + value),
              0
            )
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(3);
        pipe.emit(4);

        circuit.await();

        assertEquals(List.of(7), captured);

      } finally {

        circuit.close();

      }

    }

    /// Tumble emits non-overlapping fixed-size batch aggregates.
    @SpecRef("6.2.3")
    @Test
    void tumble_fixedBatch_emitsNonOverlappingAggregates() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .tumble(3, Integer::sum, 0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int i = 1; i <= 9; i++) pipe.emit(i);

        circuit.await();

        // 1+2+3=6, 4+5+6=15, 7+8+9=24
        assertEquals(List.of(6, 15, 24), captured);

      } finally {

        circuit.close();

      }

    }

  }


  // ============================================================
  // Natural-ordering overloads
  // ============================================================

  @Nested
  final class Stateless {

    /// Comparator-based above passes only strictly greater values.
    @SpecRef("6.2.4")
    @Test
    void above_greaterValues_passesOnlyMatches() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .above(Integer::compareTo, 0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-1);
        pipe.emit(0);
        pipe.emit(1);
        pipe.emit(5);

        circuit.await();

        assertEquals(List.of(1, 5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Comparator-based below passes only strictly lesser values.
    @SpecRef("6.2.4")
    @Test
    void below_lesserValues_passesOnlyMatches() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .below(Integer::compareTo, 5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(3);
        pipe.emit(5);
        pipe.emit(7);

        circuit.await();

        assertEquals(List.of(3), captured);

      } finally {

        circuit.close();

      }

    }

    /// Comparator-based clamp coerces values into range.
    @SpecRef({"6.2.2", "6.2.4"})
    @Test
    void clamp_outsideValues_coercesIntoRange() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .clamp(Integer::compareTo, 0, 10)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-5);
        pipe.emit(5);
        pipe.emit(15);

        circuit.await();

        assertEquals(List.of(0, 5, 10), captured);

      } finally {

        circuit.close();

      }

    }

    /// Comparator-based deadband drops its inclusive band.
    @SpecRef("6.2.4")
    @Test
    void deadband_insideAndOutsideValues_dropsInclusiveBand() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .deadband(Integer::compareTo, 3, 7)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int v : new int[]{1, 3, 5, 7, 9}) pipe.emit(v);

        circuit.await();

        // 3 and 7 are inclusive edges (suppressed); 5 is in-band.
        assertEquals(List.of(1, 9), captured);

      } finally {

        circuit.close();

      }

    }

    /// Guard passes only values whose predicate returns true.
    @SpecRef("6.2.2")
    @Test
    void guard_predicateResult_filtersValues() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .guard(v -> v > 0)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-1);
        pipe.emit(2);
        pipe.emit(-3);
        pipe.emit(4);

        circuit.await();

        assertEquals(List.of(2, 4), captured);

      } finally {

        circuit.close();

      }

    }

    /// Comparator-based max includes its upper bound.
    @SpecRef("6.2.4")
    @Test
    void max_belowAtAboveBound_passesAtOrBelow() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .max(Integer::compareTo, 5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(3);
        pipe.emit(5);
        pipe.emit(7);

        circuit.await();

        assertEquals(List.of(3, 5), captured);

      } finally {

        circuit.close();

      }

    }

    /// Comparator-based min includes its lower bound.
    @SpecRef("6.2.4")
    @Test
    void min_belowAtAboveBound_passesAtOrAbove() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .min(Integer::compareTo, 5)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(3);
        pipe.emit(5);
        pipe.emit(7);

        circuit.await();

        assertEquals(List.of(5, 7), captured);

      } finally {

        circuit.close();

      }

    }

    /// Peek observes each reached emission and passes it unchanged.
    @SpecRef("6.2.2")
    @Test
    void peek_observedValues_passesUnchanged() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > peeked = new ArrayList<>();
        final List< Integer > downstream = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .peek(peeked::add);

        final var pipe = attach(circuit, fiber, downstream);

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(3);

        circuit.await();

        assertEquals(List.of(1, 2, 3), peeked);
        assertEquals(List.of(1, 2, 3), downstream);

      } finally {

        circuit.close();

      }

    }

    /// Comparator-based range passes its inclusive interval.
    @SpecRef("6.2.4")
    @Test
    void range_insideAndOutsideValues_passesInclusiveInterval() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .range(Integer::compareTo, 3, 7)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        for (int i = 1; i <= 10; i++) pipe.emit(i);

        circuit.await();

        assertEquals(List.of(3, 4, 5, 6, 7), captured);

      } finally {

        circuit.close();

      }

    }

    /// Replace drops an emission when its mapper returns absence.
    @SpecRef("6.2.2")
    @Test
    void replace_absentMappedValue_dropsEmission() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .replace(value -> value % 2==0 ? value * 10:null)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);

        circuit.await();

        assertEquals(List.of(20), captured);

      } finally {

        circuit.close();

      }

    }

    /// Replace maps each emission within the existing type.
    @SpecRef("6.2.2")
    @Test
    void replace_mappedValues_emitsTransformations() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .replace(v -> v * 10)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(3);

        circuit.await();

        assertEquals(List.of(10, 20, 30), captured);

      } finally {

        circuit.close();

      }

    }

    /// Route diverts matching values and drops them from the main path.
    @SpecRef("6.2.2")
    @Test
    void route_matchingValues_divertsFromMain() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > routed = new ArrayList<>();
        final List< Integer > downstream = new ArrayList<>();

        final Pipe< Integer > routePipe = circuit.pipe(routed::add);

        final var fiber =
          cortex.fiber(Integer.class)
            .route(v -> v > 0, routePipe);

        final var pipe = attach(circuit, fiber, downstream);

        pipe.emit(1);
        pipe.emit(-2);
        pipe.emit(3);
        pipe.emit(-4);

        circuit.await();

        assertEquals(List.of(1, 3), routed);
        assertEquals(List.of(-2, -4), downstream);

      } finally {

        circuit.close();

      }

    }

    /// Successive route stages partition emissions independently.
    @SpecRef("6.2.2")
    @Test
    void route_multipleStages_partitionsStream() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > positives = new ArrayList<>();
        final List< Integer > negatives = new ArrayList<>();
        final List< Integer > remaining = new ArrayList<>();

        final Pipe< Integer > posPipe = circuit.pipe(positives::add);
        final Pipe< Integer > negPipe = circuit.pipe(negatives::add);

        final var fiber =
          cortex.fiber(Integer.class)
            .route(v -> v > 0, posPipe)
            .route(v -> v < 0, negPipe);

        final var pipe = attach(circuit, fiber, remaining);

        pipe.emit(1);
        pipe.emit(-2);
        pipe.emit(0);
        pipe.emit(3);
        pipe.emit(-4);

        circuit.await();

        assertEquals(List.of(1, 3), positives);
        assertEquals(List.of(-2, -4), negatives);
        assertEquals(List.of(0), remaining);

      } finally {

        circuit.close();

      }

    }

    /// Tee forwards to its side Pipe and continues downstream.
    @SpecRef("6.2.2")
    @Test
    void tee_eachEmission_fansOutAndContinues() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > side = new ArrayList<>();
        final List< Integer > downstream = new ArrayList<>();

        final Pipe< Integer > sidePipe = circuit.pipe(side::add);

        final var fiber =
          cortex.fiber(Integer.class)
            .tee(sidePipe);

        final var pipe = attach(circuit, fiber, downstream);

        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(3);

        circuit.await();

        assertEquals(List.of(1, 2, 3), side);
        assertEquals(List.of(1, 2, 3), downstream);

      } finally {

        circuit.close();

      }

    }

    /// Successive when stages apply their sub-fibers independently.
    @SpecRef("6.2.2")
    @Test
    void when_chainedStages_applyIndependently() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var fiber =
          cortex.fiber(Integer.class)
            .when(v -> v > 10, cortex.fiber(Integer.class).diff())
            .when(v -> v < 0, cortex.fiber(Integer.class).guard(v -> v % 2==0))
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(20);  // >10, diff: first — passes
        pipe.emit(20);  // >10, diff: duplicate — drops
        pipe.emit(30);  // >10, diff: new value — passes
        pipe.emit(-3);   // <0, guard odd — drops
        pipe.emit(-4);   // <0, guard even — passes
        pipe.emit(5);  // neither predicate — passes through

        circuit.await();

        assertEquals(List.of(20, 30, -4, 5), captured);

      } finally {

        circuit.close();

      }

    }

    /// When passes misses and applies its sub-fiber to matches.
    @SpecRef("6.2.2")
    @Test
    void when_matchingAndUnmatched_appliesSubfiberOrPasses() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > captured = new ArrayList<>();

        final var subFiber =
          cortex.fiber(Integer.class)
            .guard(v -> v > 2);

        final var fiber =
          cortex.fiber(Integer.class)
            .when(v -> v > 0, subFiber)
            .peek(captured::add);

        final var pipe = attach(circuit, fiber, new ArrayList<>());

        pipe.emit(-1);  // unmatched — passes through
        pipe.emit(1);  // matched, sub-fiber drops (1 not > 2)
        pipe.emit(3);  // matched, sub-fiber passes (3 > 2)
        pipe.emit(-4);  // unmatched — passes through

        circuit.await();

        assertEquals(List.of(-1, 3, -4), captured);

      } finally {

        circuit.close();

      }

    }

  }


  // ============================================================
  // Null/illegal argument validation
  // ============================================================

  @Nested
  final class Validation {

    /// Natural-order above rejects an absent lower bound.
    @SpecRef("15.2")
    @Test
    void above_naturalNullLower_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).above(null)
      );

    }

    /// Comparator-based above rejects an absent lower bound.
    @SpecRef("15.2")
    @Test
    void above_nullBound_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).above(Integer::compareTo, null)
      );

    }

    /// Above rejects an absent comparator.
    @SpecRef("15.2")
    @Test
    void above_nullComparator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).above(null, 0)
      );

    }

    /// Natural-order below rejects an absent upper bound.
    @SpecRef("15.2")
    @Test
    void below_naturalNullUpper_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).below(null)
      );

    }

    /// Below rejects an absent comparator.
    @SpecRef("15.2")
    @Test
    void below_nullComparator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).below(null, 0)
      );

    }

    /// Comparator-based below rejects an absent upper bound.
    @SpecRef("15.2")
    @Test
    void below_nullUpper_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).below(Integer::compareTo, null)
      );

    }

    /// Chance rejects probability outside zero through one.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void chance_outOfRangeProbability_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).chance(1.5)
      );

    }

    /// Change rejects an absent key function.
    @SpecRef("15.2")
    @Test
    void change_nullKeyFunction_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).change(null)
      );

    }

    /// Comparator-based clamp rejects inverted bounds.
    @SpecRef({"6.2.4", "15.1"})
    @Test
    void clamp_invertedBounds_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).clamp(Integer::compareTo, 10, 0)
      );

    }

    /// Natural-order clamp rejects inverted bounds.
    @SpecRef({"6.2.4", "15.1"})
    @Test
    void clamp_naturalInvertedBounds_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).clamp(10, 0)
      );

    }

    /// Natural-order clamp rejects an absent lower bound.
    @SpecRef("15.2")
    @Test
    void clamp_naturalNullLower_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).clamp(null, 10)
      );

    }

    /// Natural-order clamp rejects an absent upper bound.
    @SpecRef("15.2")
    @Test
    void clamp_naturalNullUpper_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).clamp(0, null)
      );

    }

    /// Clamp rejects an absent comparator.
    @SpecRef("15.2")
    @Test
    void clamp_nullComparator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).clamp(null, 0, 10)
      );

    }

    /// Comparator-based clamp rejects an absent lower bound.
    @SpecRef("15.2")
    @Test
    void clamp_nullLower_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).clamp(Integer::compareTo, null, 10)
      );

    }

    /// Comparator-based clamp rejects an absent upper bound.
    @SpecRef("15.2")
    @Test
    void clamp_nullUpper_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).clamp(Integer::compareTo, 0, null)
      );

    }

    /// Cortex rejects an absent Fiber type witness.
    @SpecRef("15.2")
    @Test
    void cortex_nullFiberClass_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(null)
      );

    }

    /// Comparator-based deadband rejects inverted bounds.
    @SpecRef({"6.2.4", "15.1"})
    @Test
    void deadband_invertedBounds_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).deadband(Integer::compareTo, 7, 3)
      );

    }

    /// Natural-order deadband rejects inverted bounds.
    @SpecRef({"6.2.4", "15.1"})
    @Test
    void deadband_naturalInvertedBounds_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).deadband(7, 3)
      );

    }

    /// Natural-order deadband rejects an absent lower bound.
    @SpecRef("15.2")
    @Test
    void deadband_naturalNullLower_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).deadband(null, 7)
      );

    }

    /// Natural-order deadband rejects an absent upper bound.
    @SpecRef("15.2")
    @Test
    void deadband_naturalNullUpper_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).deadband(3, null)
      );

    }

    /// Deadband rejects an absent comparator.
    @SpecRef("15.2")
    @Test
    void deadband_nullComparator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).deadband(null, 1, 3)
      );

    }

    /// Comparator-based deadband rejects an absent lower bound.
    @SpecRef("15.2")
    @Test
    void deadband_nullLower_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).deadband(Integer::compareTo, null, 3)
      );

    }

    /// Comparator-based deadband rejects an absent upper bound.
    @SpecRef("15.2")
    @Test
    void deadband_nullUpper_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).deadband(Integer::compareTo, 1, null)
      );

    }

    /// Delay rejects a negative depth.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void delay_negativeDepth_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).delay(-1, 0)
      );

    }

    /// Delay rejects an absent initial value.
    @SpecRef("15.2")
    @Test
    void delay_nullInitial_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).delay(1, null)
      );

    }

    /// Delay rejects a zero depth.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void delay_zeroDepth_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).delay(0, 0)
      );

    }

    /// Seeded diff rejects an absent initial value.
    @SpecRef("15.2")
    @Test
    void diff_nullInitial_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).diff(null)
      );

    }

    /// Bounded distinct rejects negative capacity.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void distinct_negativeCapacity_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).distinct(-1)
      );

    }

    /// Bounded distinct rejects zero capacity.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void distinct_zeroCapacity_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).distinct(0)
      );

    }

    /// DropWhile rejects an absent predicate.
    @SpecRef("15.2")
    @Test
    void dropWhile_nullPredicate_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).dropWhile(null)
      );

    }

    /// Edge rejects an absent initial value.
    @SpecRef("15.2")
    @Test
    void edge_nullInitial_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).edge(null, Integer::equals)
      );

    }

    /// Edge rejects an absent transition predicate.
    @SpecRef("15.2")
    @Test
    void edge_nullTransition_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).edge(0, null)
      );

    }

    /// Duration-based every requires positive duration.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void every_nonPositiveDuration_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).every(Duration.ZERO)
      );

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).every(Duration.ofNanos(-1L))
      );

    }

    /// Interval-based every requires a positive interval.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void every_nonPositiveInterval_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).every(0)
      );

    }

    /// Duration-based every rejects an absent duration.
    @SpecRef("15.2")
    @Test
    void every_nullDuration_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).every(null)
      );

    }

    /// Flow rejects an absent Fiber attachment.
    @SpecRef("15.2")
    @Test
    void fiber_nullFlowAttachment_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow(Integer.class).fiber((Fiber< Integer >) null)
      );

    }

    /// Cortex rejects an absent Fiber when creating a Flow.
    @SpecRef("15.2")
    @Test
    void flow_nullWrappedFiber_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.flow((Fiber< Integer >) null)
      );

    }

    /// Stateful guard rejects an absent bi-predicate.
    @SpecRef("15.2")
    @Test
    void guard_nullBiPredicate_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).guard(0, null)
      );

    }

    /// Stateful guard rejects an absent initial value.
    @SpecRef("15.2")
    @Test
    void guard_nullInitial_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).guard(null, Integer::equals)
      );

    }

    /// Stateless guard rejects an absent predicate.
    @SpecRef("15.2")
    @Test
    void guard_nullPredicate_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).guard(null)
      );

    }

    /// Heartbeat requires positive maximum silence.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void heartbeat_nonPositiveDuration_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).heartbeat(Duration.ZERO)
      );

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).heartbeat(Duration.ofNanos(-1L))
      );

    }

    /// Heartbeat rejects an absent maximum-silence duration.
    @SpecRef("15.2")
    @Test
    void heartbeat_nullDuration_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).heartbeat(null)
      );

    }

    /// High rejects an absent comparator.
    @SpecRef("15.2")
    @Test
    void high_nullComparator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).high(null)
      );

    }

    /// Hysteresis rejects an absent enter predicate.
    @SpecRef("15.2")
    @Test
    void hysteresis_nullEnter_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).hysteresis(null, _ -> false)
      );

    }

    /// Hysteresis rejects an absent exit predicate.
    @SpecRef("15.2")
    @Test
    void hysteresis_nullExit_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).hysteresis(_ -> true, null)
      );

    }

    /// Inhibit rejects a negative refractory count.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void inhibit_negativeRefractory_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).inhibit(-1)
      );

    }

    /// Integrate rejects an absent accumulator function.
    @SpecRef("15.2")
    @Test
    void integrate_nullAccumulator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).integrate(0, null, _ -> true)
      );

    }

    /// Integrate rejects an absent fire predicate.
    @SpecRef("15.2")
    @Test
    void integrate_nullFire_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).integrate(0, Integer::sum, null)
      );

    }

    /// Limit rejects a negative count.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void limit_negativeCount_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).limit(-1)
      );

    }

    /// Low rejects an absent comparator.
    @SpecRef("15.2")
    @Test
    void low_nullComparator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).low(null)
      );

    }


    // ----- Natural-ordering overloads -----

    /// Natural-order max rejects an absent maximum.
    @SpecRef("15.2")
    @Test
    void max_naturalNullMaximum_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).max(null)
      );

    }

    /// Max rejects an absent comparator.
    @SpecRef("15.2")
    @Test
    void max_nullComparator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).max(null, 5)
      );

    }

    /// Comparator-based max rejects an absent maximum.
    @SpecRef("15.2")
    @Test
    void max_nullMaximum_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).max(Integer::compareTo, null)
      );

    }

    /// Natural-order min rejects an absent minimum.
    @SpecRef("15.2")
    @Test
    void min_naturalNullMinimum_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).min(null)
      );

    }

    /// Min rejects an absent comparator.
    @SpecRef("15.2")
    @Test
    void min_nullComparator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).min(null, 5)
      );

    }

    /// Comparator-based min rejects an absent minimum.
    @SpecRef("15.2")
    @Test
    void min_nullMinimum_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).min(Integer::compareTo, null)
      );

    }

    /// Peek rejects an absent receptor.
    @SpecRef("15.2")
    @Test
    void peek_nullReceptor_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).peek(null)
      );

    }

    /// Fiber attachment rejects Pipe and Cell targets from
    /// an incompatible provider.
    @SpecRef({"6.2.6", "15.1"})
    @Test
    void pipe_foreignTargets_throwFault() {

      final Pipe< Integer > foreignPipe = foreignProviderStub(Pipe.class);
      final Cell< Integer > foreignCell = foreignProviderStub(Cell.class);
      final var fiber = cortex.fiber(Integer.class);

      assertThrows(Fault.class, () -> fiber.pipe(foreignPipe));
      assertThrows(Fault.class, () -> fiber.pipe(foreignCell));

    }

    /// Fiber#pipe rejects an absent Cell target.
    @SpecRef({"6.2.6", "15.2"})
    @Test
    void pipe_nullCell_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).pipe((Cell< Integer >) null)
      );

    }

    /// Fiber#pipe rejects an absent target for every recipe shape.
    @SpecRef("15.2")
    @Test
    void pipe_nullTarget_throwsNullPointerException() {

      final var circuit = cortex.circuit();

      try {

        final Pipe< Integer > sink = circuit.pipe();

        assertThrows(
          NullPointerException.class,
          () -> cortex.fiber(Integer.class).pipe(
            (Pipe< Integer >) null
          )
        );

        assertThrows(
          NullPointerException.class,
          () -> cortex.fiber(Integer.class).diff().pipe(
            (Pipe< Integer >) null
          )
        );

        // sanity: non-null attachment works on the sink
        cortex.fiber(Integer.class).pipe(sink);

      } finally {

        circuit.close();

      }

    }

    /// Pulse rejects an absent predicate.
    @SpecRef("15.2")
    @Test
    void pulse_nullPredicate_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).pulse(null)
      );

    }

    /// Comparator-based range rejects inverted bounds.
    @SpecRef({"6.2.4", "15.1"})
    @Test
    void range_invertedBounds_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).range(Integer::compareTo, 7, 3)
      );

    }

    /// Natural-order range rejects inverted bounds.
    @SpecRef({"6.2.4", "15.1"})
    @Test
    void range_naturalInvertedBounds_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).range(7, 3)
      );

    }

    /// Natural-order range rejects an absent lower bound.
    @SpecRef("15.2")
    @Test
    void range_naturalNullLower_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).range(null, 7)
      );

    }

    /// Natural-order range rejects an absent upper bound.
    @SpecRef("15.2")
    @Test
    void range_naturalNullUpper_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).range(3, null)
      );

    }

    /// Range rejects an absent comparator.
    @SpecRef("15.2")
    @Test
    void range_nullComparator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).range(null, 1, 3)
      );

    }

    /// Comparator-based range rejects an absent lower bound.
    @SpecRef("15.2")
    @Test
    void range_nullLower_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).range(Integer::compareTo, null, 3)
      );

    }

    /// Comparator-based range rejects an absent upper bound.
    @SpecRef("15.2")
    @Test
    void range_nullUpper_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).range(Integer::compareTo, 1, null)
      );

    }

    /// Reduce rejects an absent accumulator operator.
    @SpecRef("15.2")
    @Test
    void reduce_nullOperator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).reduce(0, null)
      );

    }

    /// Relate rejects an absent binary operator.
    @SpecRef("15.2")
    @Test
    void relate_nullOperator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).relate(0, null)
      );

    }

    /// Replace rejects an absent mapping operator.
    @SpecRef("15.2")
    @Test
    void replace_nullOperator_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).replace(null)
      );

    }

    /// Rolling requires a positive window size.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void rolling_nonPositiveSize_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).rolling(0, Integer::sum, 0)
      );

    }

    /// Rolling rejects an absent combiner.
    @SpecRef("15.2")
    @Test
    void rolling_nullCombiner_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).rolling(3, null, 0)
      );

    }

    /// Rolling rejects an absent identity value.
    @SpecRef("15.2")
    @Test
    void rolling_nullIdentity_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).rolling(3, Integer::sum, null)
      );

    }

    /// Route rejects an absent side Pipe.
    @SpecRef("15.2")
    @Test
    void route_nullPipe_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).route(v -> v > 0, null)
      );

    }

    /// Route rejects an absent predicate.
    @SpecRef("15.2")
    @Test
    void route_nullPredicate_throwsNullPointerException() {

      final var circuit = cortex.circuit();

      try {

        assertThrows(
          NullPointerException.class,
          () -> cortex.fiber(Integer.class).route(null, circuit.pipe())
        );

      } finally {

        circuit.close();

      }

    }

    /// Skip rejects a negative count.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void skip_negativeCount_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).skip(-1)
      );

    }

    /// Steady requires a positive confirmation count.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void steady_nonPositiveCount_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).steady(0)
      );

    }

    /// Generalized steady rejects an absent equality predicate.
    @SpecRef("15.2")
    @Test
    void steady_nullEquality_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).steady(3, null)
      );

    }

    /// Streak requires a positive threshold.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void streak_nonPositiveThreshold_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).streak(0, v -> true)
      );

    }

    /// Streak rejects an absent matching predicate.
    @SpecRef("15.2")
    @Test
    void streak_nullPredicate_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).streak(3, null)
      );

    }

    /// TakeWhile rejects an absent predicate.
    @SpecRef("15.2")
    @Test
    void takeWhile_nullPredicate_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).takeWhile(null)
      );

    }

    /// Tee rejects an absent side Pipe.
    @SpecRef("15.2")
    @Test
    void tee_nullPipe_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).tee(null)
      );

    }

    /// Tumble requires a positive batch size.
    @SpecRef({"6.2.3", "15.1"})
    @Test
    void tumble_nonPositiveSize_throwsIllegalArgumentException() {

      assertThrows(
        IllegalArgumentException.class,
        () -> cortex.fiber(Integer.class).tumble(0, Integer::sum, 0)
      );

    }

    /// Tumble rejects an absent combiner.
    @SpecRef("15.2")
    @Test
    void tumble_nullCombiner_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).tumble(3, null, 0)
      );

    }

    /// Tumble rejects an absent identity value.
    @SpecRef("15.2")
    @Test
    void tumble_nullIdentity_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).tumble(3, Integer::sum, null)
      );

    }

    /// When rejects an absent sub-fiber.
    @SpecRef("15.2")
    @Test
    void when_nullFiber_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).when(v -> v > 0, null)
      );

    }

    /// When rejects an absent predicate.
    @SpecRef("15.2")
    @Test
    void when_nullPredicate_throwsNullPointerException() {

      assertThrows(
        NullPointerException.class,
        () -> cortex.fiber(Integer.class).when(null, cortex.fiber(Integer.class))
      );

    }

  }

}
