// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.lang.ref.Reference;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §10.5 Sink capture production, provenance, endpoint routing,
/// pre-capture filtering, naming, multiplexing, and Subscriber integration.
///
/// - The endpoint receives a [Capture] per emission, tagged with the channel's
///   [Capture#subject()] and the emitting [Capture#current()] context.
/// - Pre-capture transformation/filtering composes via the inherited
///   [Pool#pool(java.util.function.Function)] (e.g. `fiber::pipe`).
/// - The endpoint is caller-owned: closing the sink never closes the endpoint.
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.15.1/SPEC.md")
final class SinkContractTest
  extends TestSupport {

  private Cortex cortex;

  /// A Sink channel forwards value, subject, current, and State.
  @SpecRef({"10.5", "11.1"})
  @Test
  void capture_channelEmission_forwardsProvenanceEnvelope() {

    final var circuit = cortex.circuit();

    try {

      final List< Capture< Integer > > seen = new CopyOnWriteArrayList<>();

      final Pipe< Capture< Integer > > endpoint =
        circuit.pipe(seen::add);

      final var sink = circuit.sink(endpoint);
      final var name = cortex.name("sink.basic");
      final var channel = sink.get(name);

      channel.emit(7);
      circuit.await();

      assertEquals(1, seen.size());

      final var capture = seen.getFirst();

      assertEquals(7, capture.emission());

      // subject() identifies the channel pipe the value was emitted through
      assertSame(channel.subject(), capture.subject());
      assertEquals(name, capture.subject().name());

      // measures default to the (non-null) empty state
      assertNotNull(capture.state());

    } finally {

      circuit.close();

    }

  }

  /// Multiple named Sink channels multiplex to one fixed endpoint.
  @SpecRef("10.5")
  @Test
  void capture_multipleNamedChannels_multiplexesSharedEndpoint() {

    final var circuit = cortex.circuit();

    try {

      final List< Capture< Integer > > seen = new CopyOnWriteArrayList<>();

      final Pipe< Capture< Integer > > endpoint =
        circuit.pipe(seen::add);

      // two sinks funnel into one shared, caller-owned endpoint
      final var sinkA = circuit.sink(endpoint);
      final var sinkB = circuit.sink(endpoint);

      sinkA.get(cortex.name("a")).emit(1);
      sinkB.get(cortex.name("b")).emit(2);
      circuit.await();

      assertEquals(
        List.of(1, 2),
        seen.stream().map(Capture::emission).toList()
      );

    } finally {

      circuit.close();

    }

  }

  /// Pre-capture Fiber filtering suppresses values before Capture creation.
  @SpecRef("10.5")
  @Test
  void capture_preCaptureFiber_suppressesFilteredValues() {

    final var circuit = cortex.circuit();

    try {

      final List< Capture< Integer > > seen = new CopyOnWriteArrayList<>();

      final Pipe< Capture< Integer > > endpoint =
        circuit.pipe(seen::add);

      final var sink = circuit.sink(endpoint);

      // pre-capture dedup via the inherited Pool.pool(Function): a value the fiber
      // suppresses never reaches the channel and so mints no capture
      final var deduped = sink.pool(cortex.fiber(Integer.class).diff()::pipe);
      final var input = deduped.get(cortex.name("sink.dedup"));

      input.emit(1);
      input.emit(1);
      input.emit(2);
      circuit.await();

      assertEquals(
        List.of(1, 2),
        seen.stream().map(Capture::emission).toList()
      );

      // the capture's subject is the sink channel, taken after the fiber
      assertEquals(
        cortex.name("sink.dedup"),
        seen.getFirst().subject().name()
      );

    } finally {

      circuit.close();

    }

  }

  /// A cross-Circuit endpoint retains the ingress caller context.
  @SpecRef({"5.2", "10.5"})
  @Test
  void current_crossCircuitEndpoint_preservesIngressCallerContext() {

    final var producer = cortex.circuit();
    final var consumer = cortex.circuit();

    try {

      final List< Capture< Integer > > seen = new CopyOnWriteArrayList<>();

      // the endpoint belongs to a DIFFERENT circuit than the sink
      final Pipe< Capture< Integer > > endpoint =
        consumer.pipe(seen::add);

      final var channel = producer.sink(endpoint).get(cortex.name("sink.cross"));

      channel.emit(99);
      producer.await();
      consumer.await();

      assertEquals(1, seen.size());

      final var capture = seen.getFirst();

      assertEquals(99, capture.emission());

      // provenance is frozen at the producer-side ingress: the external caller
      assertSame(cortex.current().subject(), capture.current());

    } finally {

      producer.close();
      consumer.close();

    }

  }

  /// An external Sink emission identifies the caller context.
  @SpecRef({"5.2", "10.5"})
  @Test
  void current_externalEmission_identifiesCallerContext() {

    final var circuit = cortex.circuit();

    try {

      final List< Capture< Integer > > seen = new CopyOnWriteArrayList<>();

      final Pipe< Capture< Integer > > endpoint =
        circuit.pipe(seen::add);

      final var channel = circuit.sink(endpoint).get(cortex.name("sink.external"));

      channel.emit(42);
      circuit.await();

      final var capture = seen.getFirst();

      // emitting context is THIS external caller, not the circuit worker
      assertSame(cortex.current().subject(), capture.current());
      assertNotSame(circuit.current().subject(), capture.current());

    } finally {

      circuit.close();

    }

  }

  /// Two distinct callers are two contexts even when their threads claim to be
  /// equal. Callers are told apart by identity: `Thread.equals` and
  /// `Thread.hashCode` can be overridden.
  @SpecRef({"5.2", "10.5"})
  @Test
  void current_distinctCallersThatCompareEqual_receiveDistinctContexts() throws Exception {

    final var circuit = cortex.circuit();

    try {

      final List< Capture< Integer > > seen = new CopyOnWriteArrayList<>();

      final Pipe< Capture< Integer > > endpoint =
        circuit.pipe(seen::add);

      final var channel = circuit.sink(endpoint).get(cortex.name("sink.equal"));

      final List< Object > callers = new CopyOnWriteArrayList<>();
      final List< Thread > threads = new ArrayList<>();

      // Both threads stay reachable, so the second caller's lookup meets the
      // first caller's entry if the provider compares threads by equality.
      for (var index = 0; index < 2; index++) {

        final var value = index;

        final var thread =
          new EqualThread(() -> {
            callers.add(cortex.current().subject());
            channel.emit(value);
          });

        threads.add(thread);
        thread.start();
        assertTrue(thread.join(Duration.ofSeconds(10)), "caller " + index + " finished");
        circuit.await();

      }

      assertEquals(2, seen.size(), "captures");
      assertSame(callers.get(0), seen.get(0).current(), "first caller's context");
      assertSame(callers.get(1), seen.get(1).current(), "second caller's context");
      assertNotSame(seen.get(0).current(), seen.get(1).current(), "distinct callers");

      Reference.reachabilityFence(threads);

    } finally {

      circuit.close();

    }

  }

  /// A caller that holds its own monitor while it emits and waits for the
  /// capture sees it delivered. Resolving the caller must not need that
  /// monitor, as calling a `synchronized` override of `Thread.hashCode` on it
  /// would.
  ///
  /// The wait is bounded so that a provider which does need the monitor gets
  /// it back when the wait gives up, and this test fails alone. Waiting without
  /// a bound would leave that provider's worker blocked, holding whatever it
  /// holds, for every test that runs after this one.
  @SpecRef({"5.2", "10.5"})
  @Test
  void current_callerHoldingItsOwnMonitor_receivesItsCapture() throws Exception {

    final var circuit = cortex.circuit();

    try {

      final List< Capture< Integer > > seen = new CopyOnWriteArrayList<>();
      final var delivered = new CountDownLatch(1);

      final Pipe< Capture< Integer > > endpoint =
        circuit.pipe(capture -> {
          seen.add(capture);
          delivered.countDown();
        });

      final var channel = circuit.sink(endpoint).get(cortex.name("sink.monitor"));

      final var failure = new AtomicReference< Throwable >();

      final var caller =
        new MonitorHashThread(() -> {

          synchronized (Thread.currentThread()) {

            channel.emit(1);

            try {
              await(delivered, "the capture of a caller holding its own monitor");
            } catch (final AssertionError | InterruptedException e) {
              failure.set(e);
            }

          }

        });

      caller.setDaemon(true);
      caller.start();

      join(caller, "a caller holding its own monitor to see its capture");

      assertNull(failure.get(), "the caller's wait for its capture");

      circuit.await();

      assertEquals(1, seen.size(), "captures");

    } finally {

      circuit.close();

    }

  }

  /// A thread that claims equality with every other of its kind.
  private static final class EqualThread
    extends Thread {

    EqualThread(final Runnable task) {
      super(task);
    }

    @Override
    public boolean equals(final Object other) {
      return other instanceof EqualThread;
    }

    @Override
    public int hashCode() {
      return EqualThread.class.hashCode();
    }

  }

  /// A thread whose hash code needs its own monitor.
  private static final class MonitorHashThread
    extends Thread {

    MonitorHashThread(final Runnable task) {
      super(task);
    }

    @Override
    public synchronized int hashCode() {
      return super.hashCode();
    }

  }

  /// A transit-cascade Sink Capture identifies Circuit context.
  @SpecRef({"5.3", "10.5"})
  @Test
  void current_transitCascade_identifiesCircuitContext() {

    final var circuit = cortex.circuit();

    try {

      final List< Capture< Integer > > seen = new CopyOnWriteArrayList<>();

      final Pipe< Capture< Integer > > endpoint =
        circuit.pipe(seen::add);

      final var channel = circuit.sink(endpoint).get(cortex.name("sink.cascade"));

      // a worker-side cascade emits into the sink channel
      final Pipe< Integer > source =
        circuit.pipe((Integer v) -> channel.emit(v + 1));

      source.emit(10);
      circuit.await();

      final var capture = seen.getFirst();

      assertEquals(11, capture.emission());

      // a worker-emitted (cascade) value's emitting context is the circuit itself
      assertSame(circuit.current().subject(), capture.current());

    } finally {

      circuit.close();

    }

  }

  @BeforeEach
  void setUp() {
    cortex = cortex();
  }

  /// Named Sink creation binds the supplied subject name.
  @SpecRef("10.5")
  @Test
  void sink_withExplicitName_usesSuppliedSubjectName() {

    final var circuit = cortex.circuit();

    try {

      final Pipe< Capture< Integer > > endpoint = circuit.pipe();

      final var name = cortex.name("my.sink");
      final var sink = circuit.sink(name, endpoint);

      assertEquals(name, sink.subject().name());

    } finally {

      circuit.close();

    }

  }

  /// A pool-backed Subscriber wires source names into a Sink.
  @SpecRef({"7.2", "10.5"})
  @Test
  void subscribe_sinkPool_routesMatchingSourceChannels() {

    final var circuit = cortex.circuit();

    try {

      final List< Capture< Integer > > seen = new CopyOnWriteArrayList<>();

      final Pipe< Capture< Integer > > endpoint =
        circuit.pipe(seen::add);

      final var sink = circuit.sink(endpoint);
      final var source = circuit.conduit(Integer.class);

      source.subscribe(
        circuit.subscriber(
          cortex.name("sink.subscriber"),
          sink
        )
      );

      source.get(cortex.name("source.alpha")).emit(1);
      source.get(cortex.name("source.beta")).emit(2);
      circuit.await();

      assertEquals(
        List.of(1, 2),
        seen.stream().map(Capture::emission).toList()
      );

      assertEquals(
        List.of(
          cortex.name("source.alpha"),
          cortex.name("source.beta")
        ),
        seen.stream().map(capture -> capture.subject().name()).toList()
      );

      assertSame(
        sink.get(cortex.name("source.alpha")).subject(),
        seen.getFirst().subject()
      );

      // a source delivers to its subscribers as a transit cascade, so the sink
      // mints during the transit phase — the emitting context is the circuit,
      // not the external caller that emitted into the source
      assertSame(
        circuit.current().subject(),
        seen.getFirst().current()
      );

    } finally {

      circuit.close();

    }

  }

  /// Every emission pending at once carries its own emitting context.
  ///
  /// The existing provenance cases emit one value at a time, so each is admitted
  /// to an empty queue. A provider may hold the first pending item differently
  /// from the rest — a reserved position ahead of its general storage is a common
  /// shape — and an envelope assembled correctly for the first can still be
  /// assembled incorrectly for the others. §11.1 admits no such distinction: the
  /// context belongs to the emission, whatever is pending alongside it.
  ///
  /// The circuit context is held inside a receptor so that nothing drains while
  /// the producer runs, which is what makes every one of its emissions pending
  /// together rather than one after another. The producer is a thread of its own,
  /// so the context every capture must report is neither the circuit's nor this
  /// test thread's.
  @SpecRef({"5.6", "10.5", "11.1"})
  @Test
  void capture_backlogFromOneCaller_forwardsThatCallerPerEmission() throws Exception {

    final var circuit = cortex.circuit();

    try {

      final List< Capture< Integer > > seen = new CopyOnWriteArrayList<>();

      final Pipe< Capture< Integer > > endpoint =
        circuit.pipe(seen::add);

      final var channel =
        circuit.sink(endpoint)
          .get(cortex.name("sink.backlog"));

      final var conduit = circuit.conduit(Integer.class);
      final var gate = conduit.get(cortex.name("sink.backlog.gate"));

      final var holding = new CountDownLatch(1);
      final var release = new CountDownLatch(1);

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("sink.backlog.subscriber"),
          (_, registrar) ->
            registrar.register(_ -> {

              holding.countDown();

              try {
                await(release, "the sink backlog release gate");
              } catch (final InterruptedException ignored) {
                Thread.currentThread().interrupt();
              }

            })
        )
      );

      // Occupy the circuit context. Until the gate opens, nothing is delivered.
      gate.emit(0);
      await(holding, "the circuit context to be occupied");

      final var count = 64;

      try (final var producer = TestTasks.fixed(1)) {

        final var caller =
          get(
            producer.submit(() -> {

              for (var value = 0; value < count; value++) {
                channel.emit(value);
              }

              return cortex.current().subject();

            }),
            "the backlog producer"
          );

        release.countDown();
        circuit.await();

        assertEquals(count, seen.size(), "captures");

        for (var position = 0; position < count; position++) {

          final var capture = seen.get(position);

          assertEquals(position, capture.emission(), "emission at " + position);

          assertSame(
            caller,
            capture.current(),
            "emitting context at " + position
          );

        }

      }

    } finally {

      circuit.close();

    }

  }

}
