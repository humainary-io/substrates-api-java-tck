// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for [Basin] and [Port] used as **coordination stages**
/// rather than as containers: SPEC §§5.3, 11.1 and 11.5.
///
/// [BasinContractTest] and [PortContractTest] establish retention, eviction,
/// drain order, clearing, and queued updates. Each does so an operation at a
/// time, with the circuit quiesced between steps, which is the shape that
/// answers "does this store what I put in it". These types exist to do
/// something else as well: to stage work across a queue boundary, so that what
/// is written and what is read are separated by other queued work rather than
/// by an await.
///
/// Every case below therefore interleaves operations **without** draining the
/// circuit between them, and asserts batch membership rather than eventual
/// contents. A batch boundary is only observable when two batches go to
/// different destinations, so the drains here do.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class CoordinationStageContractTest
  extends TestSupport {

  private Cortex cortex;
  /// The circuit owning the stage.
  private Circuit owner;
  /// The circuit owning the destinations it drains or publishes to.
  private Circuit destination;

  /// Writes, a drain, more writes, and a second drain, queued together with no
  /// await between them. Each drain must carry exactly the values retained at
  /// its own position, in retention order.
  ///
  /// The two drains go to different destinations, which is the only way the
  /// boundary between them is visible: a provider that drained everything at
  /// the last position, or that let the second drain re-read the first batch,
  /// produces the same total and a different split.
  @SpecRef({"5.3", "11.1"})
  @Test
  void basin_interleavedWritesAndDrains_deliverPreciseBatches() {

    final Basin< Integer > basin = owner.basin(8);
    final var feed = basin.pipe();

    final List< Integer > firstBatch = new CopyOnWriteArrayList<>();
    final List< Integer > secondBatch = new CopyOnWriteArrayList<>();

    feed.emit(1);
    feed.emit(2);
    basin.drain(destination.pipe(firstBatch::add));
    feed.emit(3);
    feed.emit(4);
    basin.drain(destination.pipe(secondBatch::add));

    settle();

    assertEquals(
      List.of(1, 2),
      firstBatch,
      "a drain carries exactly what was retained at its own position"
    );

    assertEquals(
      List.of(3, 4),
      secondBatch,
      "and the next drain carries exactly what arrived after it"
    );

  }

  /// A drain recipient that writes back into the basin is producing later
  /// retained work, not extending the batch it is being delivered from.
  ///
  /// Without this rule a drain would have no defined end: each delivery could
  /// append to the batch being delivered. The write-back must instead be
  /// visible only to a subsequent drain.
  @SpecRef({"5.3", "11.1"})
  @Test
  void basin_recipientWritingBack_belongsToLaterRetention() {

    final Basin< Integer > basin = owner.basin(8);
    final var feed = basin.pipe();

    final List< Integer > firstBatch = new CopyOnWriteArrayList<>();
    final List< Integer > secondBatch = new CopyOnWriteArrayList<>();

    feed.emit(1);
    feed.emit(2);

    basin.drain(
      owner.pipe(
        (Integer value) -> {
          firstBatch.add(value);
          if (value < 10) {
            feed.emit(value + 100);
          }
        }
      )
    );

    settle();

    assertEquals(
      List.of(1, 2),
      firstBatch,
      "the batch ends where it began; the write-back does not extend it"
    );

    basin.drain(destination.pipe(secondBatch::add));
    settle();

    assertEquals(
      List.of(101, 102),
      secondBatch,
      "and the written-back values are retained for the next drain"
    );

  }

  /// A drain empties what it delivered. Draining again delivers nothing, and
  /// draining an untouched basin delivers nothing — neither is an error, and
  /// neither replays.
  @SpecRef("11.1")
  @Test
  void basin_repeatedAndEmptyDrains_doNotRedeliver() {

    final Basin< Integer > basin = owner.basin(8);
    final var feed = basin.pipe();

    final List< Integer > delivered = new CopyOnWriteArrayList<>();
    final Pipe< Integer > recipient =
      destination.pipe(delivered::add);

    basin.drain(recipient);           // empty
    feed.emit(1);
    basin.drain(recipient);           // carries 1
    basin.drain(recipient);           // empty again
    basin.drain(recipient);

    settle();

    assertEquals(
      List.of(1),
      delivered,
      "a value is drained once; empty and repeated drains deliver nothing"
    );

  }

  /// A transformation that fails leaves the port holding its previous value,
  /// and the port keeps publishing afterwards.
  ///
  /// Both halves matter. A provider that let the failure escape would strand
  /// the queued emissions behind it; one that applied a partial result would
  /// publish a value no transformation ever returned.
  @SpecRef({"11.5", "15.4"})
  @Test
  void port_failedUpdate_keepsPreviousValueAndKeepsPublishing() {

    final List< Integer > seen = new CopyOnWriteArrayList<>();
    final Pipe< Integer > target =
      destination.pipe(seen::add);

    final Port< Integer > port = owner.port(0);

    port.replace(7);
    port.emit(target);

    port.update(_ -> {
      throw new IllegalStateException("deliberate");
    });
    port.emit(target);

    port.replace(9);
    port.emit(target);

    settle();

    assertEquals(
      List.of(7, 7, 9),
      seen,
      "a failed update retains the previous value, and later publication still works"
    );

  }

  /// A port publishing through a **stateful** channel on another circuit. The
  /// queued read happens at each `emit`'s position, so the values reaching the
  /// channel are the ones current then — and the channel's own operator state
  /// then sees a real sequence rather than a repeated final value.
  ///
  /// A provider that read the port at delivery time would publish `2` twice
  /// here, which the `diff` on the far side would collapse to a single
  /// delivery. The stateful target is what turns that mistake from a wrong
  /// value into a missing one.
  @SpecRef({"5.3", "11.5"})
  @Test
  void port_publishingThroughStatefulChannel_readsAtEachEmitPosition() {

    final var conduit = destination.conduit(Integer.class);
    final List< Integer > seen = new CopyOnWriteArrayList<>();

    conduit.subscribe(
      destination.subscriber(
        cortex.name("deduplicating"),
        (_, registrar) ->
          registrar.register(
            cortex.fiber(Integer.class)
              .diff()
              .pipe(destination.pipe(seen::add))
          )
      )
    );

    final var channel = conduit.get(cortex.name("channel"));
    final Port< Integer > port = owner.port(0);

    port.replace(1);
    port.emit(channel);
    port.replace(2);
    port.emit(channel);

    settle();

    assertEquals(
      List.of(1, 2),
      seen,
      "each publication carries the value current at its own position"
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    owner = cortex.circuit(cortex.name("owner"));
    destination = cortex.circuit(cortex.name("destination"));

  }

  private void settle() {

    owner.await();
    destination.await();

  }

  @AfterEach
  void tearDown() {

    owner.closeAwait();
    destination.closeAwait();

  }

}
