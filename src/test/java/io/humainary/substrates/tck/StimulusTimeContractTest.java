// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;

import static java.lang.Thread.*;
import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for where a stimulus reading begins when work arrives
/// from another circuit: SPEC §§5.8 and 6.2.3.
///
/// §5.8 advances processing time **per ingress**, not per transit hop, so
/// everything one submission cascades into is co-temporal however long the
/// cascade takes. `CircuitContractTest.processingTime_delayedTransitHops_
/// shareStimulusReading` establishes that for a local cascade. The open
/// question is what a cross-circuit arrival is: a fresh ingress on the
/// destination, or something that carries the source's reading with it.
///
/// It must be the former, and two plausible implementations answer
/// differently. One lets an arrival inherit the source's stimulus, dating work
/// by when it was sent. The other takes a single reading per drained batch,
/// making a whole backlog co-temporal. Neither shows up in a local cascade, and
/// neither changes a value — only which values are co-temporal with which.
///
/// The coordinate is **processing** time, not submission time. Two arrivals
/// submitted an hour apart and then drained back to back are co-temporal, and
/// that is correct rather than a defect. What separates two backlogged
/// arrivals is elapsed processing between them, which is how the backlog case
/// below is built.
///
/// The oracle throughout is **window membership**, never a comparison against
/// wall-clock time, and the margins are deliberately wide: the durations here
/// are chosen so that a correct provider passes by an order of magnitude and a
/// wrong one fails by one. Window contents are copied inside the callback, as
/// §6.4.1 requires.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class StimulusTimeContractTest
  extends TestSupport {

  /// Wide enough that a co-temporal cascade stays inside it however slow the
  /// machine, and short enough that a deliberately separated arrival is well
  /// outside it.
  private static final Duration RETENTION = Duration.ofMillis(100L);

  /// Comfortably longer than RETENTION, so two arrivals separated by it cannot
  /// share a window unless the provider shares their reading.
  private static final long SEPARATION = 600L;

  /// Longer than RETENTION, so a cascade that spans it is only co-temporal if
  /// time advances per ingress rather than per hop.
  private static final long DELAY = 300L;
  private final List< List< Integer > > windows = new CopyOnWriteArrayList<>();
  private Cortex cortex;
  private Circuit source;
  private Circuit destination;

  private static void pause(
    final long millis
  ) {

    try {
      Thread.sleep(millis);
    } catch (final InterruptedException _) {
      currentThread().interrupt();
    }

  }

  /// One arrival from another circuit, whose cascade on the destination spans
  /// far longer than the window retains. Everything it cascades into is still
  /// co-temporal, because the arrival is one ingress.
  @SpecRef({"5.8", "6.2.3"})
  @Test
  void arrival_withSlowLocalCascade_staysCoTemporal() {

    final var window = windowed();

    final Pipe< Integer > delayed =
      destination.pipe(
        (Integer _) -> {
          pause(DELAY);
          window.emit(2);
        }
      );

    final Pipe< Integer > arrival =
      destination.pipe(
        (Integer _) -> {
          window.emit(1);
          delayed.emit(0);
        }
      );

    source.pipe(arrival).emit(0);

    source.await();
    destination.await();

    assertEquals(
      List.of(List.of(1), List.of(1, 2)),
      windows,
      "one arrival is one stimulus, so its whole cascade shares a reading"
    );

  }

  /// Two arrivals backlogged together, with a slow hop inside the first one's
  /// cascade so that real time passes between the two stimulus readings.
  ///
  /// Both are submitted while the destination is held, so neither is processed
  /// as it arrives and the gap between their submissions is irrelevant — two
  /// items drained back to back are co-temporal whenever they were sent. What
  /// separates them is processing, and it has to be placed with care: the
  /// window operator reads the stimulus when its **transit item** runs, not
  /// when `emit` is called, so a pause in the receptor that emitted it happens
  /// after the read and separates nothing. The pause here is a further transit
  /// hop queued behind the window emit, so it falls between the first reading
  /// and the second.
  ///
  /// A provider taking one reading per drained batch keeps the first value
  /// here. Time advancing per ingress expires it.
  @SpecRef({"5.8", "6.2.3"})
  @Test
  void backloggedArrivals_separatedByProcessing_doNotShareOneReading() {

    final var entered = new CountDownLatch(1);
    final var gate = new CountDownLatch(1);

    final var window = windowed();

    final Pipe< Integer > trailing =
      destination.pipe((Integer _) -> pause(DELAY));

    final Pipe< Integer > slow =
      destination.pipe(
        (Integer value) -> {
          window.emit(value);     // transit: the reading is taken when this runs
          trailing.emit(0);       // transit: queued behind it, so the pause follows
        }
      );

    final Pipe< Integer > prompt =
      destination.pipe(window::emit);

    destination.pipe(
      (Integer _) -> {
        entered.countDown();
        try {
          if (!gate.await(10L, TimeUnit.SECONDS)) {
            windows.clear();
          }
        } catch (final InterruptedException _) {
          currentThread().interrupt();
        }
      }
    ).emit(0);

    assertDoesNotThrow(() -> await(entered, "the destination to reach its gate"));

    // Both admitted while the destination is held, so they drain together.
    source.pipe(slow).emit(1);
    source.pipe(prompt).emit(2);
    source.await();

    gate.countDown();
    destination.await();

    assertEquals(
      List.of(List.of(1), List.of(2)),
      windows,
      "a backlog is a sequence of stimuli, and processing between them advances time"
    );

  }

  /// Two separate arrivals from the same circuit, deliberately spaced beyond
  /// the retention. Each is its own ingress on the destination, so the first
  /// value has expired by the time the second is processed.
  ///
  /// This is the contrast that gives the case above its content: a provider
  /// that never advanced time on arrivals would pass that one and hold `1`
  /// here forever.
  @SpecRef({"5.8", "6.2.3"})
  @Test
  void separateArrivals_spacedBeyondRetention_expireTheOlderValue() {

    final var window = windowed();
    final var bridge = source.pipe(window);

    bridge.emit(1);
    source.await();
    destination.await();

    pause(SEPARATION);

    bridge.emit(2);
    source.await();
    destination.await();

    assertEquals(
      List.of(List.of(1), List.of(2)),
      windows,
      "each arrival advances the destination's reading, so the older value expires"
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    source = cortex.circuit(cortex.name("source"));
    destination = cortex.circuit(cortex.name("destination"));

  }

  @AfterEach
  void tearDown() {

    source.closeAwait();
    destination.closeAwait();

  }

  /// A pipe on the destination that copies each window's contents during the
  /// callback, which is the only point the view is valid.
  private Pipe< Integer > windowed() {

    return
      cortex.flow(Integer.class)
        .window(RETENTION, 10)
        .pipe(
          destination.pipe(
            (Window< Integer > window) -> {
              final var values = new ArrayList< Integer >();
              window.forEach(values::add);
              windows.add(values);
            }
          )
        );

  }

}
