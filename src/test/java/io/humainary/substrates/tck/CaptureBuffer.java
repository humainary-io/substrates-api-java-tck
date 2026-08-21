// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import java.util.*;
import java.util.stream.*;

import static io.humainary.substrates.api.Substrates.*;
import static io.humainary.substrates.api.Substrates.Sink;

/// Test-only bounded capture collector built from the public [Basin] and [Sink]
/// primitives.
///
/// It funnels a source's emissions through a [Sink] (which mints the [Capture]s)
/// into a [Basin] of captures. [#drain] consumes the buffered captures after
/// awaiting circuit progress; [#close] closes the bridging [Subscription].
///
/// Note: because captures now flow through a [Sink], `Capture#subject()` is the
/// sink channel (mirroring the source channel's name with its own identity).
public final class CaptureBuffer < E > {

  private static final int DEFAULT_CAPACITY = 1024;
  private static final Name NAME = cortex().name("capture-buffer");

  private final Circuit circuit;
  private final Basin< Capture< E > > basin;
  private final Subscription subscription;

  private CaptureBuffer(
    final Circuit circuit,
    final Basin< Capture< E > > basin,
    final Subscription subscription
  ) {

    this.circuit = circuit;
    this.basin = basin;
    this.subscription = subscription;

  }

  public static < E > CaptureBuffer< E > of(
    final Circuit circuit,
    final Source< E, ? > source
  ) {

    return of(circuit, source, DEFAULT_CAPACITY);

  }

  public static < E > CaptureBuffer< E > of(
    final Circuit circuit,
    final Source< E, ? > source,
    final int capacity
  ) {

    final Basin< Capture< E > > basin =
      circuit.basin(capacity);

    final Subscription subscription =
      source.subscribe(
        circuit.subscriber(
          NAME,
          circuit.sink(basin.pipe())
        )
      );

    return
      new CaptureBuffer<>(
        circuit,
        basin,
        subscription
      );

  }

  /// Closes the bridging subscription, stopping further capture into the basin.
  public void close() {

    subscription.close();

  }

  public Stream< Capture< E > > drain() {

    final var seen =
      new ArrayList< Capture< E > >();

    basin.drain(
      circuit.pipe(seen::add)
    );

    circuit.await();

    return seen.stream();

  }

  /// Drains buffered captures and projects them to their emissions.
  public Stream< E > drainEmissions() {

    return drain().map(Capture::emission);

  }

}
