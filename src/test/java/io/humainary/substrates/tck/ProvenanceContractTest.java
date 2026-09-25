// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for the provenance a [Capture] carries: SPEC §§5.2–5.3,
/// 10.5, 11.1 and 11.4.
///
/// Three different facts meet on a capture and are easy to conflate, so every
/// case here asserts them apart:
///
/// - `subject()` is the **channel** the value was emitted through.
/// - `current()` is the **immediate emitter** — the execution context that
///   performed this emission, not the ultimate root of a multi-hop chain.
/// - `cortex.current()` inside the endpoint is the **executing destination**,
///   which is the circuit that owns the endpoint and need not be either of
///   the above.
///
/// [SinkContractTest] establishes the single-origin case, and that a minted
/// capture survives being forwarded. What it cannot show is a stale origin: one
/// caller cannot reveal an implementation that mints correctly and then reuses
/// the last origin it saw for the next caller. Every case below therefore has
/// at least two origins in flight, and the backlog case deliberately has them
/// queued together.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class ProvenanceContractTest
  extends TestSupport {

  private Cortex cortex;
  /// The circuit owning the sink that mints.
  private Circuit minting;
  /// The circuit owning the endpoint that receives what was minted.
  private Circuit receiving;

  /// The origin tag carried in each payload, so a capture can be checked
  /// against the emitter that actually produced it.
  private static String tag(
    final String value
  ) {

    return
      value.substring(0, value.indexOf(':'));

  }

  /// Four origins — two external callers and two circuit workers — feeding one
  /// sink channel, with a backlog holding them together before any is minted.
  ///
  /// The backlog is the point. A provider that captured the emitter once per
  /// drain, or that held the last one it saw, gives every capture in the batch
  /// one origin and passes any single-caller test. Each payload names its own
  /// origin, so the assertion is per capture rather than per batch.
  @SpecRef({"5.2", "5.3", "10.5", "11.1"})
  @Test
  void capture_backlogFromManyOrigins_recordsEachImmediateEmitter()
    throws InterruptedException, ExecutionException {

    final var tasks = TestTasks.fixed(2);
    final var relaying = cortex.circuit(cortex.name("relaying"));

    try {

      final List< Capture< String > > captures = new CopyOnWriteArrayList<>();
      final var entered = new CountDownLatch(1);
      final var gate = new CountDownLatch(1);

      final var sink =
        minting.sink(
          receiving.pipe((Capture< String > capture) -> captures.add(capture))
        );

      final var channel = sink.get(cortex.name("channel"));

      // Holds the minting circuit so everything below queues behind it.
      minting.pipe(
        (String _) -> {
          entered.countDown();
          try {
            if (!gate.await(10, TimeUnit.SECONDS)) {
              captures.clear();
            }
          } catch (final InterruptedException _) {
            Thread.currentThread().interrupt();
          }
        }
      ).emit("hold");

      await(entered, "the minting circuit to reach its gate");

      // Two circuit workers relay into the sink; two external callers emit
      // into it directly. All four are pending together when the gate opens.
      final Pipe< String > viaRelaying = relaying.pipe(channel);
      final Pipe< String > viaReceiving = receiving.pipe(channel);

      final var first = tasks.submit(() -> {
        for (int i = 0; i < 4; i++) {
          channel.emit("callerA:" + i);
        }
      });

      final var second = tasks.submit(() -> {
        for (int i = 0; i < 4; i++) {
          channel.emit("callerB:" + i);
        }
      });

      for (int i = 0; i < 4; i++) {
        viaRelaying.emit("relaying:" + i);
        viaReceiving.emit("receiving:" + i);
      }

      get(first, "the first external caller");
      get(second, "the second external caller");
      relaying.await();
      receiving.await();

      gate.countDown();

      minting.await();
      receiving.await();

      assertEquals(16, captures.size(), "every emission mints exactly one capture");

      // Each origin tag must map to exactly one emitting context.
      final Map< String, Set< Subject< Current > > > byTag = new LinkedHashMap<>();

      for (final var capture : captures) {
        byTag.computeIfAbsent(tag(capture.emission()), _ -> new LinkedHashSet<>())
          .add(capture.current());
      }

      assertEquals(
        Set.of("callerA", "callerB", "relaying", "receiving"),
        byTag.keySet(),
        "all four origins reached the sink"
      );

      byTag.forEach(
        (origin, contexts) ->
          assertEquals(
            1,
            contexts.size(),
            "every capture tagged " + origin + " must name one emitting context"
          )
      );

      final var distinct = new LinkedHashSet< Subject< Current > >();
      byTag.values().forEach(distinct::addAll);

      assertEquals(4, distinct.size(), "and the four origins must not share one");

      assertEquals(
        Set.of(relaying.current().subject()),
        byTag.get("relaying"),
        "a relaying circuit's worker is named as that circuit's context"
      );

      assertEquals(
        Set.of(receiving.current().subject()),
        byTag.get("receiving"),
        "and so is the endpoint's own circuit when it is the emitter"
      );

      // The other two facts, asserted apart from the first.
      for (final var capture : captures) {

        assertSame(
          channel.subject(),
          capture.subject(),
          "every capture names the channel it was captured at"
        );

      }

    } finally {

      tasks.close();
      relaying.closeAwait();

    }

  }

  /// Forwarding a minted capture is not re-capturing it. The object that
  /// arrives downstream carries the provenance of where it was minted, however
  /// many circuits it is passed through afterwards.
  @SpecRef({"10.5", "11.1"})
  @Test
  void capture_forwardedThroughRelays_keepsItsOriginalProvenance() {

    final var relayed = new ArrayList< Capture< String > >();
    final var contexts = new ArrayList< Current >();

    final Pipe< Capture< String > > terminal =
      receiving.pipe(
        (Capture< String > capture) -> {
          relayed.add(capture);
          contexts.add(cortex.current());
        }
      );

    // Minted on `minting`, then relayed through `minting` again before landing.
    final var sink = minting.sink(minting.pipe(terminal));
    final var channel = sink.get(cortex.name("channel"));

    channel.emit("payload");

    minting.await();
    receiving.await();

    assertEquals(1, relayed.size(), "one emission, one capture, however many hops");

    assertSame(
      channel.subject(),
      relayed.getFirst().subject(),
      "the forwarded capture still names the channel it was captured at"
    );

    assertEquals(
      List.of(receiving.current()),
      contexts,
      "while the terminal runs in the circuit that owns it"
    );

  }

  /// Emitting a capture's **payload** into a second sink mints afresh. The new
  /// capture names the second sink's channel and the context that re-emitted
  /// it, not the first sink's channel and not the original caller.
  ///
  /// This is the case that separates carrying provenance from deriving it. A
  /// provider that propagated the incoming capture's fields would produce the
  /// first sink's subject here and look correct in every forwarding test.
  @SpecRef({"5.2", "10.5", "11.1"})
  @Test
  void capture_payloadRecapturedDownstream_mintsNewProvenance() {

    final var second = new ArrayList< Capture< String > >();

    final var outerSink =
      receiving.sink(receiving.pipe((Capture< String > capture) -> second.add(capture)));

    final var outerChannel = outerSink.get(cortex.name("outer"));

    final var innerSink =
      minting.sink(
        minting.pipe(
          (Capture< String > capture) -> outerChannel.emit(capture.emission())
        )
      );

    final var innerChannel = innerSink.get(cortex.name("inner"));

    innerChannel.emit("payload");

    minting.await();
    receiving.await();

    assertEquals(1, second.size(), "the payload is captured again, once");

    assertEquals(
      "payload",
      second.getFirst().emission(),
      "the value itself crosses unchanged"
    );

    assertSame(
      outerChannel.subject(),
      second.getFirst().subject(),
      "the new capture names the sink that captured it, not the one upstream"
    );

    assertNotSame(
      innerChannel.subject(),
      second.getFirst().subject(),
      "and must not have inherited the upstream channel"
    );

    assertEquals(
      minting.current().subject(),
      second.getFirst().current(),
      "the emitter is the circuit that re-emitted the payload, not the original caller"
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    minting = cortex.circuit(cortex.name("minting"));
    receiving = cortex.circuit(cortex.name("receiving"));

  }

  @AfterEach
  void tearDown() {

    minting.closeAwait();
    receiving.closeAwait();

  }

}
