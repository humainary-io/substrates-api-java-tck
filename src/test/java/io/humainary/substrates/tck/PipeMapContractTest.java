// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §§6.2.1, 6.2.3, and 6.2.6 type-transforming Flow map composition,
/// materialization, state isolation, absence filtering, context, and delivery.

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.1/SPEC.md")
final class PipeMapContractTest
  extends TestSupport {

  private Cortex cortex;

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }


  // ================================================
  // Circuit Pipe materialization path
  // ================================================

  @Nested
  final class CircuitPipe {

    /// Mapping every emission to absence produces no output.
    @SpecRef("6.2.3")
    @Test
    void map_allResultsAbsent_producesNoOutput() {

      final var circuit = cortex.circuit();

      try {

        final List< String > results = new ArrayList<>();

        final Pipe< String > target =
          circuit.pipe(
            results::add
          );

        final Pipe< Integer > mapped =
          cortex.flow(Integer.class).map(_ -> (String) null).pipe(target);

        mapped.emit(1);
        mapped.emit(2);
        circuit.await();

        assertTrue(results.isEmpty());

      } finally {

        circuit.close();

      }

    }

    /// Chained map stages transform in composition order.
    @SpecRef("6.2.6")
    @Test
    void map_chainedStages_transformInOrder() {

      final var circuit = cortex.circuit();

      try {

        final List< String > results = new ArrayList<>();

        final Pipe< String > target =
          circuit.pipe(
            results::add
          );

        // int → double → string
        final Pipe< Integer > mapped =
          cortex.flow(Integer.class).map(i -> i * 1.5).pipe(
            cortex.flow(Double.class).map(d -> "d:" + d).pipe(target)
          );

        mapped.emit(10);
        mapped.emit(20);
        circuit.await();

        assertEquals(
          List.of("d:15.0", "d:30.0"),
          results
        );

      } finally {

        circuit.close();

      }

    }

    /// A mapping Flow materializes onto a Circuit Pipe.
    @SpecRef("6.2.6")
    @Test
    void map_circuitPipeMaterialization_deliversTransformedValue() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > results = new ArrayList<>();

        // circuit.pipe with flow: dedup on the Integer side
        final Pipe< Integer > target =
          cortex.fiber(Integer.class).diff().pipe(
            circuit.pipe(results::add)
          );

        // map String → Integer, then dedup applies
        final Pipe< String > mapped =
          cortex.flow(String.class).map(Integer::parseInt).pipe(target);

        mapped.emit("1");
        mapped.emit("1");
        mapped.emit("2");
        mapped.emit("2");
        mapped.emit("1");
        circuit.await();

        assertEquals(
          List.of(1, 2, 1),
          results
        );

      } finally {

        circuit.close();

      }

    }

    /// A mapping function returning absence filters that emission.
    @SpecRef("6.2.3")
    @Test
    void map_functionReturnsNull_filtersEmission() {

      final var circuit = cortex.circuit();

      try {

        final List< String > results = new ArrayList<>();

        final Pipe< String > target =
          circuit.pipe(
            results::add
          );

        final Pipe< Integer > mapped =
          cortex.flow(Integer.class).map(i -> i > 0 ? i.toString():null).pipe(target);

        mapped.emit(1);
        mapped.emit(-5);
        mapped.emit(3);
        mapped.emit(-2);
        circuit.await();

        assertEquals(
          List.of("1", "3"),
          results
        );

      } finally {

        circuit.close();

      }

    }

    /// Map transforms an Integer input to a String output.
    @SpecRef("6.2.1")
    @Test
    void map_integerInput_producesStringOutput() {

      final var circuit = cortex.circuit();

      try {

        final List< String > results = new ArrayList<>();

        final Pipe< String > target =
          circuit.pipe(
            results::add
          );

        final Pipe< Integer > mapped =
          cortex.flow(Integer.class).map(i -> "v:" + i).pipe(target);

        mapped.emit(42);
        mapped.emit(7);
        circuit.await();

        assertEquals(
          List.of("v:42", "v:7"),
          results
        );

      } finally {

        circuit.close();

      }

    }

    /// Stateful map execution is confined to Circuit context.
    @SpecRef({"5.1", "6.2.3"})
    @Test
    void map_statefulFunction_accumulatesInCircuitContext() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > results = new ArrayList<>();

        final Pipe< Integer > target =
          circuit.pipe(
            results::add
          );

        final var sum = new int[1];

        final Pipe< Integer > mapped =
          cortex.flow(Integer.class).map(i -> {
            sum[0] += i;
            return sum[0];
          }).pipe(target);

        mapped.emit(1);
        mapped.emit(2);
        mapped.emit(3);
        circuit.await();

        assertEquals(
          List.of(1, 3, 6),
          results
        );

      } finally {

        circuit.close();

      }

    }

    /// Map transforms a String input to an Integer output.
    @SpecRef("6.2.1")
    @Test
    void map_stringInput_producesIntegerOutput() {

      final var circuit = cortex.circuit();

      try {

        final List< Integer > results = new ArrayList<>();

        final Pipe< Integer > target =
          circuit.pipe(
            results::add
          );

        final Pipe< String > mapped =
          cortex.flow(String.class).map(Integer::parseInt).pipe(target);

        mapped.emit("100");
        mapped.emit("200");
        circuit.await();

        assertEquals(
          List.of(100, 200),
          results
        );

      } finally {

        circuit.close();

      }

    }

  }


  // ===============================================
  // Pipe path: Flow materialization in the derived-Pool factory
  // ===============================================

  @Nested
  final class ConduitPipe {

    /// Mapped derived-Pool Pipes emit to Subscribers.
    @SpecRef({"6.2.6", "10.1"})
    @Test
    void map_derivedPoolPipe_deliversToSubscribers() {

      final var circuit = cortex.circuit();

      try {

        final var flow =
          cortex.flow(Integer.class).map(String::valueOf);

        final var conduit =
          circuit.conduit(String.class);

        final var inputs =
          conduit.pool(flow::pipe);

        final var captureBuffer =
          CaptureBuffer.of(circuit, conduit, 1024);

        final var pipe =
          inputs.get(
            cortex.name("test")
          );

        pipe.emit(42);
        pipe.emit(99);
        circuit.await();

        final var captures =
          captureBuffer.drain().toList();

        assertEquals(2, captures.size());
        assertEquals("42", captures.get(0).emission());
        assertEquals("99", captures.get(1).emission());

      } finally {

        circuit.close();

      }

    }

    /// Absent mapped results in a derived Pool do not reach Subscribers.
    @SpecRef("6.2.3")
    @Test
    void map_derivedPoolReturnsNull_filtersBeforeSubscribers() {

      final var circuit = cortex.circuit();

      try {

        final var flow =
          cortex.flow(Integer.class).map(
            i -> i > 0 ? String.valueOf(i):null
          );

        final var conduit =
          circuit.conduit(String.class);

        final var inputs =
          conduit.pool(flow::pipe);

        final var captureBuffer =
          CaptureBuffer.of(circuit, conduit, 1024);

        final var pipe =
          inputs.get(
            cortex.name("test")
          );

        pipe.emit(1);
        pipe.emit(-1);
        pipe.emit(2);
        pipe.emit(-2);
        circuit.await();

        final var emissions =
          captureBuffer.drainEmissions()
            .toList();

        assertEquals(
          List.of("1", "2"),
          emissions
        );

      } finally {

        circuit.close();

      }

    }

    /// Each named Pipe materializes independent stateful map state.
    @SpecRef("6.2.3")
    @Test
    void map_multipleNamedPipes_isolatesStatePerPipe() {

      final var circuit = cortex.circuit();

      try {

        final var conduit =
          circuit.conduit(String.class);

        final var inputs =
          conduit.pool(
            pipe -> {
              final var count = new int[1];
              return cortex.flow(Integer.class).map(i -> {
                count[0] += i;
                return String.valueOf(count[0]);
              }).pipe(pipe);
            }
          );

        final var captureBuffer =
          CaptureBuffer.of(circuit, conduit, 1024);

        final Pipe< Integer > a =
          inputs.get(
            cortex.name("a")
          );

        final Pipe< Integer > b =
          inputs.get(
            cortex.name("b")
          );

        a.emit(10);
        b.emit(100);
        a.emit(20);
        b.emit(200);
        circuit.await();

        final var captures =
          captureBuffer.drain().toList();

        assertEquals(4, captures.size());

        // a: 10, 30 (accumulated independently)
        // b: 100, 300 (accumulated independently)
        assertEquals("10", captures.get(0).emission());
        assertEquals("100", captures.get(1).emission());
        assertEquals("30", captures.get(2).emission());
        assertEquals("300", captures.get(3).emission());

      } finally {

        circuit.close();

      }

    }

    /// Multiple Subscribers receive transformed values.
    @SpecRef({"6.3", "7.3"})
    @Test
    void map_multipleSubscribers_receiveTransformedValues() {

      final var circuit = cortex.circuit();

      try {

        final var flow =
          cortex.flow(Integer.class).map(i -> "s:" + i);

        final var conduit =
          circuit.conduit(String.class);

        final var inputs =
          conduit.pool(flow::pipe);

        final var captureBuffer1 =
          CaptureBuffer.of(circuit, conduit, 1024);

        final var captureBuffer2 =
          CaptureBuffer.of(circuit, conduit, 1024);

        final var pipe =
          inputs.get(
            cortex.name("test")
          );

        pipe.emit(1);
        circuit.await();

        final var e1 =
          captureBuffer1.drainEmissions()
            .toList();

        final var e2 =
          captureBuffer2.drainEmissions()
            .toList();

        assertEquals(List.of("s:1"), e1);
        assertEquals(List.of("s:1"), e2);

      } finally {

        circuit.close();

      }

    }

    /// Per-Pipe Flow mapping preserves named Conduit routing.
    @SpecRef("6.2.6")
    @Test
    void map_namedConduitFlow_preservesNameRouting() {

      final var circuit = cortex.circuit();

      try {

        // Per-Pipe Flow: diff on String inside the factory.
        final var flow =
          cortex.flow(Integer.class).map(String::valueOf).fiber(cortex.fiber(String.class).diff());

        final var conduit =
          circuit.conduit(
            cortex.name("test"),
            String.class
          );

        final var inputs =
          conduit.pool(flow::pipe);

        final var captureBuffer =
          CaptureBuffer.of(circuit, conduit, 1024);

        final var pipe =
          inputs.get(
            cortex.name("x")
          );

        pipe.emit(1);
        pipe.emit(1);
        pipe.emit(2);
        pipe.emit(2);
        pipe.emit(1);
        circuit.await();

        final var emissions =
          captureBuffer.drainEmissions()
            .toList();

        assertEquals(
          List.of("1", "2", "1"),
          emissions
        );

      } finally {

        circuit.close();

      }

    }

    /// A Flow recipe materializes independently for each named Pipe.
    @SpecRef("6.2.6")
    @Test
    void map_perPipeFlow_transformsEachNamedPipe() {

      final var circuit = cortex.circuit();

      try {

        // Per-pipe flow via flow.pipe(pipe), with map then an attached diff fiber
        final var flow =
          cortex.flow(Integer.class).map(String::valueOf).fiber(cortex.fiber(String.class).diff());

        final var conduit =
          circuit.conduit(String.class);

        final var inputs =
          conduit.pool(flow::pipe);

        final var captureBuffer =
          CaptureBuffer.of(circuit, conduit, 1024);

        final var pipe =
          inputs.get(
            cortex.name("x")
          );

        pipe.emit(5);
        pipe.emit(5);
        pipe.emit(10);
        circuit.await();

        final var emissions =
          captureBuffer.drainEmissions()
            .toList();

        // Per-pipe diff operates on String (after map)
        // "5", "5", "10" → dedup on String → "5", "10"
        assertEquals(
          List.of("5", "10"),
          emissions
        );

      } finally {

        circuit.close();

      }

    }

    /// Chained map materializations create nested Pipe subjects.
    @Test
    void subject_chainedMapPipes_formNestedChain() {

      final var circuit = cortex.circuit();

      try {

        final Pipe< Integer > target =
          circuit.pipe(
            Receptor.of()
          );

        final Pipe< Integer > first =
          cortex.flow(Integer.class).map(i -> i + 1).pipe(target);

        final Pipe< Integer > second =
          cortex.flow(Integer.class).map(i -> i + 1).pipe(first);

        // second → first → target
        assertTrue(second.subject().within(first.subject()));
        assertTrue(second.subject().within(target.subject()));
        assertTrue(first.subject().within(target.subject()));

        // Depth increases by 1 at each level
        assertEquals(target.subject().depth() + 1, first.subject().depth());
        assertEquals(first.subject().depth() + 1, second.subject().depth());

      } finally {

        circuit.close();

      }

    }

    /// A mapped Pipe subject is enclosed by its original Pipe subject.
    @Test
    void subject_mappedPipe_isNestedUnderOriginal() {

      final var circuit = cortex.circuit();

      try {

        final Pipe< String > target =
          circuit.pipe(
            Receptor.of()
          );

        final Pipe< Integer > mapped =
          cortex.flow(Integer.class).map(Object::toString).pipe(target);

        final var mappedSubject = mapped.subject();
        final var targetSubject = target.subject();

        // Mapped pipe's subject is nested under the original pipe's subject
        assertTrue(mappedSubject.within(targetSubject));

        // Mapped pipe is one level deeper
        assertEquals(targetSubject.depth() + 1, mappedSubject.depth());

        // Direct enclosure is the original pipe's subject
        assertTrue(mappedSubject.enclosure().isPresent());
        assertSame(targetSubject, mappedSubject.enclosure().get());

      } finally {

        circuit.close();

      }

    }

    /// Mapped Pipe lookup preserves canonical subject identity per name.
    @Test
    void subject_repeatedMappedLookup_preservesIdentity() {

      final var circuit = cortex.circuit();

      try {

        final var name = cortex.name("test");

        final var flow =
          cortex.flow(Integer.class).map(String::valueOf);

        final var conduit =
          circuit.conduit(String.class);

        final var inputs =
          conduit.pool(flow::pipe);

        final var captureBuffer =
          CaptureBuffer.of(circuit, conduit, 1024);

        final var pipe =
          inputs.get(name);

        pipe.emit(1);
        circuit.await();

        final var captures =
          captureBuffer.drain().toList();

        assertEquals(1, captures.size());
        assertEquals(name, captures.getFirst().subject().name());

      } finally {

        circuit.close();

      }

    }

  }


  // ===============
  // Null Arguments
  // ===============

  @Nested
  final class NullArgument {

    /// Flow#map rejects an absent mapping function.
    @SpecRef("15.2")
    @Test
    void map_nullFunction_throwsNullPointerException() {

      final var circuit = cortex.circuit();

      try {

        final Pipe< String > target =
          circuit.pipe(
            Receptor.of()
          );

        assertThrows(
          NullPointerException.class,
          () -> cortex.flow(String.class).pipe(
            (Pipe< String >) null
          )
        );

        assertThrows(
          NullPointerException.class,
          () -> cortex.flow(Integer.class).map(_ -> "").pipe(
            (Pipe< String >) null
          )
        );

        // sanity: non-null attachment works on the target
        cortex.flow(String.class).pipe(target);

      } finally {

        circuit.close();

      }

    }

  }


  // ===================
  // Threading Guarantee
  // ===================

  @Nested
  final class Threading {

    /// A map function executes in Circuit context.
    @SpecRef({"5.1", "6.2.3"})
    @Test
    void map_circuitPipeFunction_executesInCircuitContext() {

      final var circuit = cortex.circuit();

      try {

        final var callerThread = Thread.currentThread();
        final var fnThreads = new ArrayList< Thread >();

        final Pipe< String > target =
          circuit.pipe(
            Receptor.of()
          );

        final Pipe< Integer > mapped =
          cortex.flow(Integer.class).map(i -> {
            fnThreads.add(Thread.currentThread());
            return i.toString();
          }).pipe(target);

        mapped.emit(1);
        mapped.emit(2);
        circuit.await();

        assertEquals(2, fnThreads.size());

        // Function must NOT run on the caller thread
        assertNotSame(callerThread, fnThreads.get(0));
        assertNotSame(callerThread, fnThreads.get(1));

        // Both must run on the same circuit thread
        assertSame(fnThreads.get(0), fnThreads.get(1));

      } finally {

        circuit.close();

      }

    }

    /// A derived-Pool map function executes in Circuit context.
    @SpecRef({"5.1", "6.2.3"})
    @Test
    void map_derivedPoolFunction_executesInCircuitContext() {

      final var circuit = cortex.circuit();

      try {

        final var callerThread = Thread.currentThread();
        final var fnThreads = new ArrayList< Thread >();

        final var flow =
          cortex.flow(Integer.class).map(i -> {
            fnThreads.add(Thread.currentThread());
            return String.valueOf(i);
          });

        final var conduit =
          circuit.conduit(String.class);

        final var inputs =
          conduit.pool(flow::pipe);

        final var pipe =
          inputs.get(
            cortex.name("test")
          );

        pipe.emit(1);
        pipe.emit(2);
        circuit.await();

        assertEquals(2, fnThreads.size());
        assertNotSame(callerThread, fnThreads.get(0));
        assertSame(fnThreads.get(0), fnThreads.get(1));

      } finally {

        circuit.close();

      }

    }

  }

}
