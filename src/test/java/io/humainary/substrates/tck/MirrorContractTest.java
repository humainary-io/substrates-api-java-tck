// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Java-projection integration tests for source-to-Conduit mirroring via a pool-backed Subscriber —
/// the composition that replaced the former `Tap` type:
///
/// ```java
/// Conduit<T> mirror = circuit.conduit(type);
/// Subscription bridge =
///   source.subscribe(circuit.subscriber(name, mirror.pool(flow)));
/// ```
///
/// The mirror follows the structure of the source — named pipes appear under
/// the same names — while the pooled flow or fiber transforms emissions on
/// the way in. The mirror is an ordinary [Conduit]: a full [Source] that can
/// be subscribed, pooled, and closed independently of the bridging
/// [Subscription].
///
/// This test class covers:
/// - Transformation and named-Pipe structure mirroring
/// - Per-Pipe operator state isolation in pooled views
/// - Lifecycle: bridge close (stop the feed) vs mirror close (drop downstream)
/// - Retention of mirrored emissions through the Basin-backed capture helper
final class MirrorContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;

  /// Tests the split lifecycle of the composition: the bridge subscription
  /// stops the feed, the mirror's own close drops downstream subscriptions,
  /// and both are idempotent and safe in either order thereafter.
  ///
  /// Scenario:
  /// 1. Create source conduit and mirror; bridge them
  /// 2. Subscribe to the mirror (downstream subscription)
  /// 3. Close the bridge, then the mirror
  /// 4. Verify the returned subscription can be closed safely (idempotent)
  /// 5. Verify mirror.close() itself is idempotent
  /// 6. Verify no emissions flow after close
  /// Closing the mirror Conduit releases its downstream Subscription.
  @Test
  void close_mirrorConduit_releasesDownstreamSubscription() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var mirror =
      circuit.conduit(
        String.class
      );

    final var bridge =
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("bridge"),
          mirror.pool(
            cortex.flow(Integer.class).map(Object::toString)
          )
        )
      );

    final List< String > results = new ArrayList<>();

    final var sub =
      mirror.subscribe(
        circuit.subscriber(
          cortex.name("collector"),
          (_, registrar) ->
            registrar.register(results::add)
        )
      );

    final var pipe =
      conduit.get(
        cortex.name("test")
      );

    pipe.emit(1);
    pipe.emit(2);

    circuit.await();

    assertEquals(2, results.size());

    // Close the bridge (stops the feed), then the mirror (drops downstream)
    bridge.close();
    mirror.close();
    circuit.await();

    // Closing the returned subscription after mirror.close() is a safe no-op
    sub.close();
    circuit.await();

    // mirror.close() is idempotent
    mirror.close();
    circuit.await();

    // No further emissions reach downstream
    pipe.emit(3);
    pipe.emit(4);
    circuit.await();

    assertEquals(2, results.size());

  }

  /// Tests that closing the bridge subscription leaves other subscriptions
  /// on other sources untouched.
  ///
  /// Scenario:
  /// 1. Create two conduits; mirror from the first
  /// 2. Subscribe distinct subscribers to the mirror and the second conduit
  /// 3. Close the bridge subscription
  /// 4. Verify second conduit's subscription is still active
  /// 5. Verify subscribers can still be closed cleanly afterward
  /// Closing a mirroring bridge preserves unrelated Subscriptions.
  @Test
  void close_mirroringBridge_preservesOtherSubscriptions() {

    final var mirroredConduit =
      circuit.conduit(
        Integer.class
      );

    final var otherConduit =
      circuit.conduit(
        Integer.class
      );

    final var mirror =
      circuit.conduit(
        Integer.class
      );

    final var bridge =
      mirroredConduit.subscribe(
        circuit.subscriber(
          cortex.name("bridge"),
          mirror.pool(
            cortex.fiber(Integer.class)
          )
        )
      );

    final var mirrorCount = new AtomicInteger(0);
    final var otherCount = new AtomicInteger(0);

    final var mirrorSubscriber =
      circuit.< Integer > subscriber(
        cortex.name("mirror-subscriber"),
        (_, registrar) ->
          registrar.register(
            _ -> mirrorCount.incrementAndGet()
          )
      );

    final var otherSubscriber =
      circuit.< Integer > subscriber(
        cortex.name("other-subscriber"),
        (_, registrar) ->
          registrar.register(
            _ -> otherCount.incrementAndGet()
          )
      );

    mirror.subscribe(mirrorSubscriber);
    otherConduit.subscribe(otherSubscriber);

    final var mirroredPipe =
      mirroredConduit.get(
        cortex.name("mirrored-pipe")
      );

    final var otherPipe =
      otherConduit.get(
        cortex.name("other-pipe")
      );

    mirroredPipe.emit(1);
    mirroredPipe.emit(2);
    otherPipe.emit(10);

    circuit.await();

    assertEquals(2, mirrorCount.get());
    assertEquals(1, otherCount.get());

    // Close the bridge - the mirror stops receiving from the source
    bridge.close();
    circuit.await();

    // Other conduit still delivers
    otherPipe.emit(20);
    circuit.await();

    assertEquals(2, mirrorCount.get());
    assertEquals(2, otherCount.get());

    // Subscribers close cleanly (bridge subscription already closed)
    mirrorSubscriber.close();
    otherSubscriber.close();
    circuit.await();

    // Further emissions reach nothing
    mirroredPipe.emit(3);
    otherPipe.emit(30);
    circuit.await();

    assertEquals(2, mirrorCount.get());
    assertEquals(2, otherCount.get());

  }

  /// Tests that closing the bridge subscription stops the feed while the
  /// mirror remains a live, directly usable conduit.
  ///
  /// Scenario:
  /// 1. Create source conduit and mirror
  /// 2. Subscribe to the mirror
  /// 3. Emit some values
  /// 4. Close the bridge subscription
  /// 5. Emit more values through the source
  /// 6. Verify only pre-close emissions were relayed
  /// 7. Verify direct emission into the mirror still delivers
  /// Closing a mirroring bridge stops future mirrored emissions.
  @Test
  void close_mirroringBridge_stopsMirroredEmissions() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var mirror =
      circuit.conduit(
        String.class
      );

    final var bridge =
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("bridge"),
          mirror.pool(
            cortex.flow(Integer.class).map(Object::toString)
          )
        )
      );

    final List< String > results = new ArrayList<>();

    mirror.subscribe(
      circuit.subscriber(
        cortex.name("collector"),
        (_, registrar) ->
          registrar.register(results::add)
      )
    );

    final var name = cortex.name("test");
    final var pipe = conduit.get(name);

    pipe.emit(1);
    pipe.emit(2);

    circuit.await();

    bridge.close();
    circuit.await();

    pipe.emit(3);
    pipe.emit(4);

    circuit.await();

    assertEquals(List.of("1", "2"), results);

    // The mirror is an ordinary conduit - direct emission still delivers
    mirror.get(name).emit("direct");

    circuit.await();

    assertEquals(List.of("1", "2", "direct"), results);

  }

  /// Verifies mirrors chain: a mirror is a full source, so it can itself be
  /// bridged into a further mirror with additional processing.
  /// Pool-backed Subscriber mirroring composes through multiple stages.
  @Test
  void mirroring_chainedConduits_transformsThroughEveryStage() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var doubled =
      circuit.conduit(
        Integer.class
      );

    final var limited =
      circuit.conduit(
        Integer.class
      );

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("double-bridge"),
        doubled.pool(
          cortex.flow(Integer.class).map(i -> i * 2)
        )
      )
    );

    doubled.subscribe(
      circuit.subscriber(
        cortex.name("limit-bridge"),
        limited.pool(
          cortex.fiber(Integer.class).limit(2)
        )
      )
    );

    final List< Integer > results = new ArrayList<>();

    limited.subscribe(
      circuit.subscriber(
        cortex.name("collector"),
        (_, registrar) ->
          registrar.register(results::add)
      )
    );

    final var pipe = conduit.get(cortex.name("ch"));

    pipe.emit(1);
    pipe.emit(2);
    pipe.emit(3);
    pipe.emit(4);

    circuit.await();

    assertEquals(List.of(2, 4), results);

  }

  /// Verifies that a composed flow (`flow.flow(next)`) materializes correctly
  /// per mirrored channel through the pooled view.
  /// Mirroring applies every operation in a composed Flow.
  @Test
  void mirroring_composedFlow_appliesComposition() {

    final var conduit = circuit.conduit(Integer.class);
    final var mirror = circuit.conduit(String.class);

    // composed: Integer → (+1) → Integer → (toString) → "n=...": produces String
    conduit.subscribe(
      circuit.subscriber(
        cortex.name("bridge"),
        mirror.pool(
          cortex.flow(Integer.class).map(i -> i + 1)
            .flow(cortex.flow(Integer.class).map(i -> "n=" + i))
        )
      )
    );

    final List< String > results = new ArrayList<>();

    mirror.subscribe(
      circuit.subscriber(
        cortex.name("collector"),
        (_, registrar) ->
          registrar.register(results::add)
      )
    );

    final var pipe = conduit.get(cortex.name("ch"));

    pipe.emit(1);
    pipe.emit(41);

    circuit.await();

    assertEquals(List.of("n=2", "n=42"), results);

  }

  /// Verifies a fiber-pooled mirror applies per-emission operators without
  /// changing type. Uses `diff()` to dedupe consecutive duplicate emissions.
  /// Mirroring through Fiber#diff suppresses repeated values.
  @Test
  void mirroring_diffFiber_suppressesDuplicates() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var deduped =
      circuit.conduit(
        Integer.class
      );

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("bridge"),
        deduped.pool(
          cortex.fiber(Integer.class).diff()
        )
      )
    );

    final List< Integer > results = new ArrayList<>();

    deduped.subscribe(
      circuit.subscriber(
        cortex.name("collector"),
        (_, registrar) ->
          registrar.register(results::add)
      )
    );

    final var pipe = conduit.get(cortex.name("ch"));

    pipe.emit(1);
    pipe.emit(1);
    pipe.emit(2);
    pipe.emit(2);
    pipe.emit(3);

    circuit.await();

    assertEquals(List.of(1, 2, 3), results);

  }

  /// Verifies mirroring via the raw pipe-function form (`Pool.pool(Function)`),
  /// the general path underlying the `pool(Flow)` and `pool(Fiber)` sugar. The
  /// function receives the mirror's channel pipe and returns a source-compatible
  /// pipe, so type transformation and per-emission processing compose within a
  /// single function — the former `tap(Function)` semantics.
  /// Function-derived Pool mirroring transforms each named Pipe.
  @Test
  void mirroring_functionDerivedPool_transformsEmissions() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var mirror =
      circuit.conduit(
        String.class
      );

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("bridge"),
        mirror.pool(
          target ->
            cortex.fiber(Integer.class).diff().pipe(
              cortex.flow(Integer.class).map(i -> "fn:" + i).pipe(
                target
              )
            )
        )
      )
    );

    final List< String > results = new ArrayList<>();

    mirror.subscribe(
      circuit.subscriber(
        cortex.name("collector"),
        (_, registrar) ->
          registrar.register(results::add)
      )
    );

    final var pipe = conduit.get(cortex.name("ch"));

    pipe.emit(1);
    pipe.emit(1);  // deduped by the fiber before the map
    pipe.emit(2);

    circuit.await();

    assertEquals(List.of("fn:1", "fn:2"), results);

  }

  /// Tests that mirrored emissions can be retained through the Basin-backed capture helper.
  /// Mirrored emissions can be retained and drained through a Basin.
  @Test
  void mirroring_intoBasin_retainsTransformedEmissions() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var mirror =
      circuit.conduit(
        String.class
      );

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("bridge"),
        mirror.pool(
          cortex.flow(Integer.class).map(i -> "val:" + i)
        )
      )
    );

    final var buffer = CaptureBuffer.of(circuit, mirror, 1024);

    assertNotNull(buffer);

    final var pipe = conduit.get(cortex.name("test"));

    pipe.emit(10);
    pipe.emit(20);

    circuit.await();

    final var captures = buffer.drain().toList();

    assertEquals(2, captures.size());
    assertEquals("val:10", captures.get(0).emission());
    assertEquals("val:20", captures.get(1).emission());

    buffer.close();

  }

  /// Tests that a mapper returning null filters out the emission.
  ///
  /// Scenario:
  /// 1. Create a conduit emitting integers
  /// 2. Bridge into a mirror with a mapper that returns null for even numbers
  /// 3. Emit a mix of odd and even values
  /// 4. Verify only odd values (non-null mapped) are received
  /// A mirroring mapper returning null filters that emission.
  @Test
  void mirroring_mapperReturnsNull_filtersEmission() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var mirror =
      circuit.conduit(
        String.class
      );

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("bridge"),
        mirror.pool(
          cortex.flow(Integer.class).map(
            i -> i % 2!=0
              ? "odd:" + i
              :null
          )
        )
      )
    );

    final List< String > results = new ArrayList<>();

    mirror.subscribe(
      circuit.subscriber(
        cortex.name("collector"),
        (_, registrar) ->
          registrar.register(results::add)
      )
    );

    final var pipe = conduit.get(cortex.name("test.channel"));

    pipe.emit(1);  // odd  → "odd:1"
    pipe.emit(2);  // even → null (filtered)
    pipe.emit(3);  // odd  → "odd:3"
    pipe.emit(4);  // even → null (filtered)
    pipe.emit(5);  // odd  → "odd:5"

    circuit.await();

    assertEquals(List.of("odd:1", "odd:3", "odd:5"), results);

  }

  /// Tests that a mirror mirrors the channel structure of its source.
  ///
  /// Scenario:
  /// 1. Create a conduit with multiple channels
  /// 2. Bridge into a mirror conduit
  /// 3. Subscribe to the mirror
  /// 4. Emit through different source channels
  /// 5. Verify emissions come from correspondingly named mirror channels
  /// Mirroring preserves named Pipe structure between Conduits.
  @Test
  void mirroring_multipleNamedPipes_preservesNames() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var mirror =
      circuit.conduit(
        String.class
      );

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("bridge"),
        mirror.pool(
          cortex.flow(Integer.class).map(i -> "num:" + i)
        )
      )
    );

    final List< String > channelAResults = new ArrayList<>();
    final List< String > channelBResults = new ArrayList<>();

    final var channelA = cortex.name("channel.a");
    final var channelB = cortex.name("channel.b");

    mirror.subscribe(
      circuit.subscriber(
        cortex.name("collector"),
        (subject, registrar) -> {
          if (subject.name().equals(channelA)) {
            registrar.register(channelAResults::add);
          } else if (subject.name().equals(channelB)) {
            registrar.register(channelBResults::add);
          }
        }
      )
    );

    final var pipeA = conduit.get(channelA);
    final var pipeB = conduit.get(channelB);

    pipeA.emit(1);
    pipeB.emit(2);
    pipeA.emit(3);

    circuit.await();

    assertEquals(List.of("num:1", "num:3"), channelAResults);
    assertEquals(List.of("num:2"), channelBResults);

  }

  /// Verifies that a pooled fiber materializes fresh operator state per
  /// mirrored channel. Two channels sharing the same `diff()` fiber must each
  /// dedupe their own emissions — no state bleed across channels. Regression
  /// guard for the pooled view's once-per-name materialization invariant.
  /// Stateful mirroring Fiber instances are isolated per named Pipe.
  @Test
  void mirroring_statefulFiber_isolatesStatePerPipe() {

    final var conduit = circuit.conduit(Integer.class);
    final var deduped = circuit.conduit(Integer.class);

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("bridge"),
        deduped.pool(
          cortex.fiber(Integer.class).diff()
        )
      )
    );

    final var a = cortex.name("ch.a");
    final var b = cortex.name("ch.b");

    final List< Integer > aEmissions = new ArrayList<>();
    final List< Integer > bEmissions = new ArrayList<>();

    deduped.subscribe(
      circuit.subscriber(
        cortex.name("collector"),
        (subject, registrar) -> {
          if (subject.name().equals(a)) {
            registrar.register(aEmissions::add);
          } else if (subject.name().equals(b)) {
            registrar.register(bEmissions::add);
          }
        }
      )
    );

    final var pipeA = conduit.get(a);
    final var pipeB = conduit.get(b);

    // Interleave identical sequences on A and B; each channel must dedupe
    // independently.
    pipeA.emit(1);
    pipeA.emit(1);
    pipeB.emit(1);
    pipeB.emit(1);
    pipeA.emit(2);
    pipeB.emit(2);

    circuit.await();

    assertEquals(List.of(1, 2), aEmissions, "channel A diff state isolated");
    assertEquals(List.of(1, 2), bEmissions, "channel B diff state isolated");

  }

  /// Tests that a mirror correctly transforms emissions from one type to another.
  ///
  /// Scenario:
  /// 1. Create a conduit emitting integers
  /// 2. Create a mirror conduit of strings, bridged via a mapping flow
  /// 3. Subscribe to the mirror
  /// 4. Emit integers through the source conduit
  /// 5. Verify strings are received through the mirror
  /// Pool-backed Subscriber mirroring applies a type-changing Flow.
  @Test
  void mirroring_typeChangingFlow_transformsEmissions() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var mirror =
      circuit.conduit(
        String.class
      );

    assertNotNull(mirror);
    assertNotNull(mirror.subject());

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("bridge"),
        mirror.pool(
          cortex.flow(Integer.class).map(i -> "value:" + i)
        )
      )
    );

    final List< String > results = new ArrayList<>();

    mirror.subscribe(
      circuit.subscriber(
        cortex.name("collector"),
        (_, registrar) ->
          registrar.register(results::add)
      )
    );

    final var name = cortex.name("test.channel");
    final var pipe = conduit.get(name);

    pipe.emit(1);
    pipe.emit(2);
    pipe.emit(3);

    circuit.await();

    assertEquals(List.of("value:1", "value:2", "value:3"), results);

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    circuit = cortex.circuit();

  }

  @AfterEach
  void tearDown() {

    circuit.closeAwait();

  }

}
