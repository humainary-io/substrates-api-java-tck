// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import io.humainary.substrates.api.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §11.1 Capture provenance as produced by a [Sink] feeding a [Basin].
///
/// - `current()` is the emitting context. For a source-fed capture the sink mints
///   during the source's transit cascade, so `current()` is the circuit; a value
///   emitted directly into a sink channel reports its own emitter.
/// - `state()` is a per-emission measures bag, defaulting to the empty state.
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.0/SPEC.md")
final class CaptureContractTest
  extends TestSupport {

  private Cortex cortex;

  @BeforeEach
  void setUp() {
    cortex = cortex();
  }

  @Nested
  final class BasinCaptures {

    /// Source-to-Sink transit identifies Circuit context.
    @SpecRef({"5.2", "10.5", "11.1"})
    @Test
    void current_sourceFedSink_identifiesCircuitContext() {

      final var circuit = cortex.circuit();

      try {

        final var conduit = circuit.conduit(Integer.class);
        final var captureBuffer = CaptureBuffer.of(circuit, conduit, 16);
        final var pipe = conduit.get(cortex.name("capture.external"));

        pipe.emit(42);
        circuit.await();

        final var captures = captureBuffer.drain().toList();
        assertEquals(1, captures.size());

        final var capture = captures.getFirst();

        assertEquals(42, capture.emission());

        // A source-fed buffer captures through a Sink, which mints during the
        // source's transit cascade, so the emitting context is the circuit, not
        // the external caller that emitted into the source.
        assertSame(circuit.current().subject(), capture.current());
        assertNotSame(cortex.current().subject(), capture.current());

        // measures default to the empty state
        assertNotNull(capture.state());

      } finally {

        circuit.close();

      }

    }

    /// A [Substrates.Ticker] is a circuit-internal mechanism; its scheduler thread is
    /// never surfaced. Tick emissions into a same-circuit target are owner-circuit
    /// originated, so current() == the owner circuit (per the Ticker contract).
    /// Ticker Capture identifies its owning Circuit context.
    @SpecRef({"10.5", "11.1", "11.4"})
    @Test
    void current_tickerEmission_identifiesOwnerCircuitContext() throws InterruptedException {

      final var circuit = cortex.circuit();

      try {

        final var conduit = circuit.conduit(Long.class);
        final var captureBuffer = CaptureBuffer.of(circuit, conduit, 8);
        final var target = conduit.get(cortex.name("capture.tick.target"));
        final var ticked = new CountDownLatch(1);

        final var ticker =
          circuit.ticker(
            java.time.Duration.ofMillis(5L),
            circuit.pipe(
              List.of(
                target,
                circuit.pipe(_ -> ticked.countDown())
              )
            )
          );

        await(ticked, "the first ticker emission");

        ticker.closeAwait();
        circuit.await();

        final var captures = captureBuffer.drain().toList();
        assertFalse(captures.isEmpty(), "expected the ticker to have produced captures");

        final var capture = captures.getFirst();

        // owner-circuit originated: never the ticker's private scheduler thread
        assertSame(circuit.current().subject(), capture.current());

      } finally {

        circuit.close();

      }

    }

    /// A transit-cascade Capture identifies the Circuit context.
    @SpecRef({"5.3", "11.1"})
    @Test
    void current_transitCascade_identifiesCircuitContext() {

      final var circuit = cortex.circuit();

      try {

        final var conduit = circuit.conduit(Integer.class);
        final var captureBuffer = CaptureBuffer.of(circuit, conduit, 16);
        final var target = conduit.get(cortex.name("capture.cascade.target"));

        // source consumer runs on the worker and cascades into target
        final Pipe< Integer > source = circuit.pipe((Integer v) -> target.emit(v + 1));

        source.emit(10);
        circuit.await();

        final var capture = captureBuffer.drain().toList().getFirst();

        assertEquals(11, capture.emission());

        // a worker-emitted (cascade) value's emitting context is the circuit itself
        assertSame(circuit.current().subject(), capture.current());

      } finally {

        circuit.close();

      }

    }

  }

}
