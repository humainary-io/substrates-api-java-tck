// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// Verifies that synchronous operations on a closed resource raise
/// a [Fault] with the closed resource's [Subject] as the fault subject.
///
/// Per spec §9.1, queued operations on a closed resource silently drop, but
/// synchronous `@New` operations raise on the caller thread. This file
/// exercises every guarded `@New` method on [Circuit] and [Conduit],
/// plus the existing [Source#subscribe] (closed subscriber) path.

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.0/SPEC.md")
final class ClosedResourceFaultContractTest
  extends TestSupport {

  private Cortex cortex;

  /// Circuit#bank after close synchronously signals closed resource.
  @SpecRef({"9.1", "15.1"})
  @Test
  void bank_afterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.bank(
          Integer.class
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  /// Routed Circuit#bank rejects a closed receiver.
  @SpecRef({"9.1", "15.1"})
  @Test
  void bank_routedAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.bank(
          Integer.class,
          Routing.STEM
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  // ===========================
  // Circuit @New methods
  // ===========================

  /// Circuit#cell after close synchronously signals closed resource.
  @SpecRef({"9.1", "15.1"})
  @Test
  void cell_afterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.cell(0)
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  /// Named Circuit#cell after close signals closed resource.
  @SpecRef({"9.1", "15.1"})
  @Test
  void cell_namedAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.cell(
          cortex.name("x"),
          0
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  /// Circuit#conduit after close signals closed resource.
  @SpecRef({"9.1", "15.1"})
  @Test
  void conduit_afterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.conduit(Integer.class)
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  /// Named Circuit#conduit after close signals closed resource.
  @SpecRef({"9.1", "15.1"})
  @Test
  void conduit_namedAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    assertThrows(
      Fault.class,
      () -> circuit.conduit(
        cortex.name("x"),
        Integer.class
      )
    );

  }

  /// Routed Circuit#conduit rejects a closed receiver.
  @SpecRef({"9.1", "15.1"})
  @Test
  void conduit_routedAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    assertThrows(
      Fault.class,
      () -> circuit.conduit(
        cortex.name("x"),
        Integer.class,
        Routing.PIPE
      )
    );

  }

  /// A Conduit obtained from a closed Bank rejects open-required get.
  @SpecRef({"9.1", "15.1"})
  @Test
  void get_onConduitFromClosedBank_throwsFault() {

    final var circuit = cortex.circuit();
    final var bank =
      circuit.bank(
        Integer.class
      );

    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> bank.get(
          cortex.name("x")
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  /// Circuit#pipe after close signals closed resource.
  @SpecRef({"9.1", "15.1"})
  @Test
  void pipe_afterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        circuit::pipe
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

    assertEquals(
      "pipe",
      fault.operation()
    );

  }

  /// Named receptor-backed Circuit#pipe rejects a closed receiver.
  @SpecRef({"9.1", "15.1"})
  @Test
  void pipe_namedReceptorAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.< Integer > pipe(
          cortex.name("x"),
          _ -> {
          }
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  /// Named target-backed Circuit#pipe rejects a closed receiver.
  @SpecRef({"9.1", "15.1"})
  @Test
  void pipe_namedTargetsAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();

    final Pipe< Integer > target =
      circuit.pipe();

    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.pipe(
          cortex.name("x"),
          List.of(target)
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  /// Receptor-backed Circuit#pipe rejects a closed receiver.
  @SpecRef({"9.1", "15.1"})
  @Test
  void pipe_receptorAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.< Integer > pipe(
          _ -> {
          }
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  /// Target-backed Circuit#pipe rejects its closed receiver.
  @SpecRef({"9.1", "15.1"})
  @Test
  void pipe_sameCircuitTargetAfterClose_throwsFault() {

    final var circuit = cortex.circuit();

    final Pipe< Integer > target =
      circuit.pipe();

    circuit.close();

    // the same-circuit fast path must not return the target as-is after close
    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.pipe(
          target
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  /// Multi-target Circuit#pipe rejects a closed receiver.
  @SpecRef({"9.1", "15.1"})
  @Test
  void pipe_targetsAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.< Integer > pipe(
          List.of()
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

  /// Circuit#sink after close signals closed resource.
  @SpecRef({"9.1", "15.1"})
  @Test
  void sink_afterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    final Pipe< Capture< Integer > > endpoint = circuit.pipe();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.sink(endpoint)
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

    assertEquals(
      "sink",
      fault.operation()
    );

  }

  /// Named Circuit#sink after close signals closed resource.
  @SpecRef({"9.1", "15.1"})
  @Test
  void sink_namedAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    final Pipe< Capture< Integer > > endpoint = circuit.pipe();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.sink(
          cortex.name("x"),
          endpoint
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  /// Source#subscribe after Conduit close signals closed resource.
  @SpecRef({"9.1", "15.1"})
  @Test
  void subscribe_afterConduitClose_throwsFault() {

    final var circuit = cortex.circuit();
    final var conduit = circuit.conduit(Integer.class);

    conduit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> CaptureBuffer.of(circuit, conduit, 1024)
      );

    assertSame(
      conduit.subject(),
      fault.subject()
    );

    circuit.close();

  }

  /// A closed Subscriber argument is identified by the Fault.
  @SpecRef({"9.1", "15.3"})
  @Test
  void subscribe_withClosedSubscriber_throwsFaultIdentifyingArgument() {

    final var circuit = cortex.circuit();
    final var conduit = circuit.conduit(Integer.class);

    final var subscriber =
      circuit.< Integer > subscriber(
        cortex.name("s"),
        (_, _) -> {
        }
      );

    subscriber.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> conduit.subscribe(
          subscriber
        )
      );

    // subject = receiver (the conduit); offending subscriber rendered into the message
    assertSame(
      conduit.subject(),
      fault.subject()
    );

    assertEquals(
      "subscribe",
      fault.operation()
    );

    assertTrue(
      fault.getMessage().contains(subscriber.subject().toString()),
      "fault message should render the offending subscriber"
    );

    circuit.close();

  }

  /// Callback-backed Circuit#subscriber rejects a closed receiver.
  @SpecRef({"9.1", "15.1"})
  @Test
  void subscriber_callbackAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    circuit.close();

    assertThrows(
      Fault.class,
      () -> circuit.subscriber(
        cortex.name("s"),
        (_, _) -> {
        }
      )
    );

  }

  /// Pool-backed Circuit#subscriber rejects a closed receiver.
  @SpecRef({"9.1", "15.1"})
  @Test
  void subscriber_poolAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();

    final var pool =
      circuit
        .conduit(Integer.class)
        .pool(pipe -> pipe);

    circuit.close();

    assertThrows(
      Fault.class,
      () -> circuit.subscriber(
        cortex.name("s"),
        pool
      )
    );

  }

  // ===========================
  // Source.subscribe (closed subscriber)
  // ===========================

  /// Circuit#ticker rejects a closed receiver.
  @SpecRef({"9.1", "11.4"})
  @Test
  void ticker_afterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    final Pipe< Long > target = circuit.pipe();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.ticker(
          Duration.ofSeconds(1L),
          target
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

  /// Named Circuit#ticker rejects a closed receiver.
  @SpecRef({"9.1", "11.4"})
  @Test
  void ticker_namedAfterCircuitClose_throwsFault() {

    final var circuit = cortex.circuit();
    final Pipe< Long > target = circuit.pipe();
    circuit.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> circuit.ticker(
          cortex.name("x"),
          Duration.ofSeconds(1L),
          target
        )
      );

    assertSame(
      circuit.subject(),
      fault.subject()
    );

  }

}
