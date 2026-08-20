// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §11.1 Basin capacity, retention, ordering, drain, clearing, and
/// post-close behavior.
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.1/SPEC.md")
final class BasinContractTest
  extends TestSupport {

  /// Drains the basin into a list on the circuit and returns the collected values.
  private static < E > List< E > drained(
    final Circuit circuit,
    final Basin< E > basin
  ) {

    final var seen =
      new ArrayList< E >();

    basin.drain(
      circuit.pipe(seen::add)
    );

    circuit.await();

    return seen;

  }

  /// Basin creation after Circuit close signals closed resource.
  @SpecRef({"9.1", "11.1"})
  @Test
  void basin_afterCircuitClose_throwsFault() {

    final var circuit = cortex().circuit();
    circuit.close();

    assertThrows(
      Fault.class,
      () -> circuit.basin(1024)
    );

  }

  /// Basin capacity must be strictly positive.
  @SpecRef({"11.1", "15.1"})
  @Test
  void basin_nonPositiveCapacity_throwsIllegalArgumentException() {

    final var circuit = cortex().circuit();

    try {

      assertThrows(
        IllegalArgumentException.class,
        () -> circuit.basin(0)
      );

      assertThrows(
        IllegalArgumentException.class,
        () -> circuit.basin(-1)
      );

    } finally {

      circuit.close();

    }

  }

  /// Drain after Circuit close is silently dropped.
  @SpecRef({"9.1", "11.1"})
  @Test
  void drain_afterCircuitClose_isSilentlyDropped() {

    final var circuit = cortex().circuit();

    final Basin< Integer > basin = circuit.basin(16);
    final var target = circuit.pipe((Integer _) -> {
    });

    basin.pipe().emit(1);
    circuit.await();

    circuit.close();

    // A drain is a queued emission, not a lifecycle operation: enqueued after the
    // circuit has accepted close it is silently dropped — it must not throw.
    assertDoesNotThrow(() -> basin.drain(target));

  }

  /// Drain evicts every forwarded value from the Basin.
  @SpecRef("11.1")
  @Test
  void drain_retainedValues_clearsBuffer() {

    final var circuit = cortex().circuit();

    try {

      final Basin< Integer > basin = circuit.basin(16);

      basin.pipe().emit(1);
      basin.pipe().emit(2);
      circuit.await();

      assertEquals(
        List.of(1, 2),
        drained(circuit, basin)
      );

      assertTrue(
        drained(circuit, basin).isEmpty()
      );

    } finally {

      circuit.close();

    }

  }

  /// Drain forwards retained values in emission order to its target pipe.
  @SpecRef("11.1")
  @Test
  void drain_retainedValues_forwardsInEmissionOrder() {

    final var circuit = cortex().circuit();

    try {

      final Basin< Integer > basin = circuit.basin(16);

      final var forwarded = new ArrayList< Integer >();
      final var target = circuit.pipe((Integer v) -> forwarded.add(v));

      basin.pipe().emit(7);
      basin.pipe().emit(8);
      circuit.await();

      basin.drain(target);
      circuit.await();

      assertEquals(
        List.of(7, 8),
        forwarded
      );

    } finally {

      circuit.close();

    }

  }

  /// A full Basin evicts the oldest value before retaining a new one.
  @SpecRef("11.1")
  @Test
  void retention_capacityExceeded_evictsOldestValue() {

    final var circuit = cortex().circuit();

    try {

      final Basin< Integer > basin = circuit.basin(3);
      final var pipe = basin.pipe();

      for (int i = 1; i <= 5; i++) {
        pipe.emit(i);
      }

      circuit.await();

      assertEquals(
        List.of(3, 4, 5),
        drained(circuit, basin)
      );

    } finally {

      circuit.close();

    }

  }

}
