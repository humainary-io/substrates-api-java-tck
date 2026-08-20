// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §11.4 Ticker scheduling, sequence, lifecycle, identity, validation,
/// and failure-isolation behavior.

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.1/SPEC.md")
final class TickerContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;

  @BeforeEach
  void setUp() {

    cortex = cortex();
    circuit = cortex.circuit();

  }

  @AfterEach
  void tearDown() {

    circuit.closeAwait();

  }

  /// Circuit close rejects future ticker submissions.
  @SpecRef({"9.3", "11.4"})
  @Test
  void ticker_afterCircuitClose_stopsFutureTicks() throws InterruptedException {

    final AtomicLong counter = new AtomicLong();

    final var owner =
      cortex.circuit();

    final Pipe< Long > sink =
      owner.pipe(
        _ -> counter.incrementAndGet()
      );

    owner.ticker(
      Duration.ofMillis(5L),
      sink
    );

    owner.ticker(
      Duration.ofMillis(5L),
      sink
    );

    Thread.sleep(30L);

    owner.close();
    owner.await();

    final long observed = counter.get();

    Thread.sleep(40L);

    assertEquals(observed, counter.get());

  }

  /// Closing a Ticker stops future emissions.
  @SpecRef({"9.1", "11.4"})
  @Test
  void ticker_afterTickerClose_stopsFutureEmissions() throws InterruptedException {

    final AtomicLong counter = new AtomicLong();

    final Pipe< Long > sink =
      circuit.pipe(
        _ -> counter.incrementAndGet()
      );

    final var ticker =
      circuit.ticker(
        Duration.ofMillis(5L),
        sink
      );

    Thread.sleep(40L);

    ticker.close();
    circuit.await();

    final long observed = counter.get();

    Thread.sleep(40L);

    circuit.await();

    assertEquals(observed, counter.get());

  }

  /// Ticker admissions accumulate without shedding while the
  /// target Circuit is blocked.
  @SpecRef({"11.4", "5.6"})
  @Test
  void ticker_blockedTarget_accumulatesGapFreeBacklog() throws InterruptedException {

    final List< Long > received = new ArrayList<>();
    final var entered = new CountDownLatch(1);
    final var release = new CountDownLatch(1);

    final Pipe< Long > target =
      circuit.pipe(
        value -> {
          synchronized (received) {
            received.add(value);
          }

          if (value==0L) {
            entered.countDown();
            try {
              await(release, "the blocked ticker target release gate");
            } catch (final InterruptedException exception) {
              Thread.currentThread().interrupt();
            }
          }
        }
      );

    final var ticker = circuit.ticker(Duration.ofMillis(2L), target);

    try {

      await(entered, "the blocked ticker target invocation");
      Thread.sleep(50L);
      ticker.close();

    } finally {

      release.countDown();

    }

    circuit.await();

    synchronized (received) {
      assertTrue(received.size() >= 3, "Ticker must retain work admitted during target backlog");
      for (int index = 0; index < received.size(); index++) {
        assertEquals(index, received.get(index));
      }
    }

  }

  /// Ticker emissions can drive a circuit-owned Cell through its pipe.
  @SpecRef("11.4")
  @Test
  void ticker_cellTarget_drivesAccumulator() throws InterruptedException {

    final var cell =
      circuit.cell(0L);

    final var ticker =
      circuit.ticker(
        Duration.ofMillis(5L),
        cell.pipe()
      );

    Thread.sleep(50L);

    ticker.close();
    circuit.await();

    assertTrue(
      cell.get() > 0L,
      "expected ticks to have advanced the cell"
    );

  }

  /// A Ticker subject is enclosed by its owning Circuit.
  @SpecRef({"4.3", "11.4"})
  @Test
  void ticker_createdByCircuit_hasCircuitSubjectEnclosure() {

    final var sink = circuit.< Long > pipe();

    final var ticker =
      circuit.ticker(
        Duration.ofMillis(50L),
        sink
      );

    assertTrue(
      ticker.subject().enclosure().isPresent()
    );

    ticker.subject().enclosure(
      parent -> assertEquals(
        circuit.subject().id(),
        parent.id()
      )
    );

    ticker.close();

  }

  /// A foreign-provider target signals provider mismatch.
  @SpecRef({"11.4", "15.1"})
  @Test
  void ticker_foreignProviderPipe_throwsFault() {

    final var subject =
      circuit.< Long > pipe().subject();

    final Pipe< Long > foreign =
      new Pipe<>() {
        @Override
        public void emit(
          final Long emission
        ) {
        }

        @Override
        public Subject< Pipe< Long > > subject() {
          return subject;
        }
      };

    assertThrows(
      Fault.class,
      () -> circuit.ticker(
        Duration.ofMillis(5L),
        foreign
      )
    );

  }

  /// Ticker emits a gap-free monotonic sequence beginning at zero.
  @SpecRef("11.4")
  @Test
  void ticker_multipleTicks_emitsGapFreeSequenceFromZero() throws InterruptedException {

    final List< Long > received = new ArrayList<>();

    final Pipe< Long > collector =
      circuit.pipe(
        value -> {
          synchronized (received) {
            received.add(value);
          }
        }
      );

    final var ticker =
      circuit.ticker(
        Duration.ofMillis(10L),
        collector
      );

    Thread.sleep(60L);

    ticker.close();
    circuit.await();

    synchronized (received) {
      assertFalse(received.isEmpty());
      assertEquals(0L, received.getFirst());

      for (int i = 1; i < received.size(); i++) {
        assertEquals(
          received.get(i - 1) + 1L,
          received.get(i)
        );
      }
    }

  }

  /// A non-positive interval signals configuration error.
  @SpecRef({"11.4", "15.1"})
  @Test
  void ticker_nonPositiveInterval_throwsIllegalArgumentException() {

    final var sink = circuit.< Long > pipe();

    assertThrows(
      IllegalArgumentException.class,
      () -> circuit.ticker(
        Duration.ZERO,
        sink
      )
    );

    assertThrows(
      IllegalArgumentException.class,
      () -> circuit.ticker(
        Duration.ofMillis(-1L),
        sink
      )
    );

  }

  /// Required Ticker arguments reject absence.
  @SpecRef({"11.4", "15.2"})
  @Test
  void ticker_nullRequiredArguments_throwNullPointerException() {

    final var sink = circuit.< Long > pipe();
    final var name = cortex.name("t");

    assertThrows(
      NullPointerException.class,
      () -> circuit.ticker(
        null,
        sink
      )
    );

    assertThrows(
      NullPointerException.class,
      () -> circuit.ticker(
        Duration.ofMillis(5L),
        null
      )
    );

    assertThrows(
      NullPointerException.class,
      () -> circuit.ticker(
        null,
        Duration.ofMillis(5L),
        sink
      )
    );

    assertThrows(
      NullPointerException.class,
      () -> circuit.ticker(
        name,
        null,
        sink
      )
    );

    assertThrows(
      NullPointerException.class,
      () -> circuit.ticker(
        name,
        Duration.ofMillis(5L),
        null
      )
    );

  }

  /// The Ticker target executes in its owning circuit context.
  @SpecRef({"5.1", "11.4"})
  @Test
  void ticker_targetInvocation_executesInOwningCircuitContext() throws InterruptedException {

    final var observed = new AtomicReference< Current >();
    final var delivered = new CountDownLatch(1);
    final var target = circuit.pipe(_ -> {
      observed.set(cortex.current());
      delivered.countDown();
    });

    final var ticker = circuit.ticker(Duration.ofMillis(5), target);

    try {

      await(delivered, "the ticker target invocation");
      assertSame(circuit.current(), observed.get());

    } finally {

      ticker.close();

    }

  }

  /// The target fails on every tick. Counting attempts across two intervals verifies that failure is
  /// isolated from the ticker's scheduling lifecycle rather than merely tolerated for one delivery.
  ///
  /// A target failure does not stop subsequent ticks.
  @SpecRef({"11.4", "15.4"})
  @Test
  void ticker_targetThrows_continuesSubsequentTicks() throws InterruptedException {

    final AtomicLong attempts =
      new AtomicLong();

    final Pipe< Long > failing =
      circuit.pipe(
        _ -> {
          attempts.incrementAndGet();
          throw new RuntimeException(
            "boom"
          );
        }
      );

    final var ticker =
      circuit.ticker(
        Duration.ofMillis(5L),
        failing
      );

    Thread.sleep(
      40L
    );

    circuit.await();

    final long before =
      attempts.get();

    Thread.sleep(
      40L
    );

    ticker.close();
    circuit.await();

    assertTrue(
      before > 0L,
      "expected failing target to receive at least one tick"
    );

    assertTrue(
      attempts.get() > before,
      "target exceptions should be isolated from ticker scheduling"
    );

  }

  /// Fixed-rate scheduling does not accumulate interval drift;
  /// this wall-clock throughput probe is excluded from portable conformance.
  @SpecRef("11.4")
  @Tag("scheduler-performance")
  @Test
  void ticker_unstalledRun_doesNotAccumulateDrift() throws InterruptedException {

    final AtomicLong counter = new AtomicLong();

    final Pipe< Long > sink =
      circuit.pipe(
        _ -> counter.incrementAndGet()
      );

    final long intervalMs = 5L;
    final long runMs = 400L;

    final var ticker =
      circuit.ticker(
        Duration.ofMillis(intervalMs),
        sink
      );

    Thread.sleep(runMs);

    ticker.close();
    circuit.await();

    final long observed = counter.get();
    final long ideal = runMs / intervalMs;

    // Fixed-rate scheduling must not accumulate drift. A regression to
    // fixed-delay would compound per-tick overhead and deliver well under
    // the ideal count; the 85% floor allows generous slack for scheduler
    // jitter and startup latency while still failing such a regression.
    assertTrue(
      observed >= ideal * 85L / 100L,
      "expected >= " + (ideal * 85L / 100L) + " ticks for non-drifting cadence, got " + observed
    );

  }

  /// Ticker creation returns a resource handle.
  @SpecRef("11.4")
  @Test
  void ticker_validArguments_returnsResourceHandle() {

    final var cell =
      circuit.cell(0L);

    final Ticker ticker =
      circuit.ticker(
        Duration.ofMillis(5L),
        cell.pipe()
      );

    assertNotNull(ticker);
    assertNotNull(ticker.subject());
    assertEquals(
      Ticker.class,
      ticker.subject().type()
    );

    ticker.close();

  }

  /// An explicitly named Ticker exposes the supplied subject name.
  @SpecRef("11.4")
  @Test
  void ticker_withExplicitName_usesSuppliedSubjectName() {

    final var sink = circuit.< Long > pipe();
    final var name = cortex.name("heartbeat");

    final var ticker =
      circuit.ticker(
        name,
        Duration.ofMillis(50L),
        sink
      );

    assertEquals(
      name,
      ticker.subject().name()
    );

    ticker.close();

  }

  /// Ticker delivery proceeds without caller-driven draining.
  @SpecRef("11.4")
  @Test
  void ticker_withoutCallerDrain_deliversTicks() throws InterruptedException {

    final var delivered = new CountDownLatch(1);
    final var target = circuit.< Long > pipe(_ -> delivered.countDown());

    final var ticker =
      circuit.ticker(
        Duration.ofMillis(5L),
        target
      );

    await(delivered, "ticker delivery without caller-driven draining");

    ticker.close();
    circuit.await();

  }

  /// An unnamed Ticker receives a valid non-empty default name.
  @SpecRef("11.4")
  @Test
  void ticker_withoutExplicitName_usesCircuitName() {

    final var sink = circuit.< Long > pipe();

    final var ticker =
      circuit.ticker(
        Duration.ofMillis(50L),
        sink
      );

    assertEquals(
      circuit.subject().name(),
      ticker.subject().name()
    );

    ticker.close();

  }


}
