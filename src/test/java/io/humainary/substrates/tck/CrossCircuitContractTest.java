// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for work that crosses a circuit boundary: SPEC §5.1
/// circuit-context confinement, §6.3 dispatch, and §7.3's requirement that a
/// subscriber's callbacks run in the circuit context of the source they observe.
///
/// The question every case here asks is the same one: **when a target belongs to
/// another circuit, whose context does its callback observe?** The answer is
/// always the target's, however the emission reached it, and the way to ask it
/// from the API is `cortex.current()` inside the callback — the projection of
/// §11.3's Current, and the same instrument `CurrentContractTest` uses. No case
/// here looks at a thread: a circuit's context is a contract, its worker is an
/// implementation.
///
/// Both **kinds** of target appear in each shape, and that pairing is the point.
/// A plain circuit pipe forwards onward through its own circuit whatever a
/// provider does with it, so it passes under arrangements a channel does not: a
/// channel dispatches to its registrations when it receives, so a provider that
/// reaches it by invoking rather than by admitting runs another circuit's
/// subscribers — and whatever those subscribers' own routes then queue — in the
/// wrong context. A suite built only from plain pipes cannot see that, which is
/// how it went unseen.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class CrossCircuitContractTest
  extends TestSupport {

  private Cortex cortex;
  /// The circuit an emission enters.
  private Circuit entry;
  /// The circuit that owns the target, and whose context every delivery to it
  /// must observe.
  private Circuit owner;

  /// A basin on another circuit, fed through an adapter, retains the value and
  /// drains in its own context.
  @SpecRef({"5.1", "6.3", "11.1"})
  @Test
  void adapter_toForeignBasin_retainsValueAndDrainsInOwnerContext() {

    final Basin< Integer > basin = owner.basin(4);
    final var drained = new ArrayList< Integer >();
    final var contexts = new ArrayList< Current >();

    entry.pipe(basin.pipe()).emit(1);
    settle();

    basin.drain(
      owner.pipe(
        (Integer value) -> {
          drained.add(value);
          contexts.add(cortex.current());
        }
      )
    );

    owner.await();

    assertEquals(List.of(1), drained, "the basin must retain what crossed the boundary");
    assertEquals(
      List.of(owner.current()),
      contexts,
      "a basin on another circuit drains in that circuit's context"
    );

  }

  /// A cell on another circuit, fed through an adapter, publishes the value.
  @SpecRef({"5.1", "6.3", "11.2"})
  @Test
  void adapter_toForeignCell_publishesValue() {

    final Cell< Integer > cell = owner.cell(0);

    entry.pipe(cell.pipe()).emit(7);
    settle();

    assertEquals(7, cell.get(), "an update that crossed the boundary must be applied");

  }

  /// A channel on another circuit, reached through `pipe(target)`, delivers in
  /// its own circuit's context.
  ///
  /// The channel is the case a plain pipe cannot stand in for: it dispatches to
  /// its registrations the moment it receives, so a provider that calls into it
  /// rather than admitting to its circuit runs those registrations here.
  @SpecRef({"5.1", "6.3", "7.3"})
  @Test
  void adapter_toForeignChannel_deliversInOwnerContext() {

    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "adapter.observer");
    final var channel = conduit.get(cortex.name("channel"));

    entry.pipe(channel).emit(1);
    settle();

    assertEquals(List.of(1), observed.values, "the emission must reach the registration");
    assertEquals(
      List.of(owner.current()),
      observed.deliveries,
      "a registration on another circuit is delivered in that circuit's context"
    );
    assertEquals(
      List.of(owner.current()),
      observed.callbacks,
      "the subscriber callback it triggers runs in that circuit's context too"
    );

  }

  /// A plain pipe on another circuit, reached through `pipe(target)`, delivers
  /// in its own circuit's context.
  ///
  /// The companion to [#adapter_toForeignChannel_deliversInOwnerContext], and
  /// the weaker of the two: a plain pipe forwards through its own circuit
  /// however it is reached.
  @SpecRef({"5.1", "6.3"})
  @Test
  void adapter_toForeignPipe_deliversInOwnerContext() {

    final var contexts = new ArrayList< Current >();

    final Pipe< Integer > target =
      owner.pipe((Integer _) -> contexts.add(cortex.current()));

    entry.pipe(target).emit(1);
    settle();

    assertEquals(
      List.of(owner.current()),
      contexts,
      "a pipe on another circuit is delivered in that circuit's context"
    );

  }

  /// The same, with the channel's registrations already built by an earlier
  /// emission.
  ///
  /// Worth its own case because the two are different code paths in any provider
  /// that builds a channel's dispatch list lazily: the first emission both
  /// discovers the channel and delivers to it, later ones only deliver.
  @SpecRef({"5.1", "6.3", "7.3"})
  @Test
  void adapter_toWarmedForeignChannel_deliversInOwnerContext() {

    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "warm.observer");
    final var channel = conduit.get(cortex.name("channel"));

    channel.emit(1);
    owner.await();

    observed.callbacks.clear();
    observed.deliveries.clear();
    observed.values.clear();

    entry.pipe(channel).emit(2);
    settle();

    assertEquals(List.of(2), observed.values, "the second emission must reach the registration");
    assertEquals(
      List.of(owner.current()),
      observed.deliveries,
      "a warmed channel is no less confined than a cold one"
    );
    assertEquals(List.of(), observed.callbacks, "no further discovery was due");

  }

  /// A throwing registration on another circuit does not deny its sibling, and
  /// does not disturb the circuit the emission entered.
  @SpecRef({"5.1", "6.3", "15.4"})
  @Test
  void adapter_withThrowingForeignRegistration_preservesSiblingDelivery() {

    final var conduit = owner.conduit(Integer.class);
    final var sibling = new ArrayList< Current >();

    conduit.subscribe(
      owner.subscriber(
        cortex.name("isolation.observer"),
        (_, registrar) -> {

          registrar.register(
            (Integer _) -> {
              throw new IllegalStateException("deliberate");
            }
          );

          registrar.register((Integer _) -> sibling.add(cortex.current()));

        }
      )
    );

    final var channel = conduit.get(cortex.name("channel"));
    final var after = new ArrayList< Current >();

    entry.pipe(channel).emit(1);
    entry.pipe((Integer _) -> after.add(cortex.current())).emit(2);
    settle();

    assertEquals(
      List.of(owner.current()),
      sibling,
      "the sibling registration still receives, in the owner's context"
    );
    assertEquals(
      List.of(entry.current()),
      after,
      "the circuit the emission entered keeps running its own work"
    );

  }

  /// A basin owned by one circuit, drained into a channel owned by another.
  /// The drain is the emitting side here, so the values leave in retention
  /// order and each arrives as its own admission.
  @SpecRef({"5.1", "6.3", "11.1"})
  @Test
  void basin_drainedToForeignChannel_deliversInOwnerContext() {

    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "basin.observer");
    final var channel = conduit.get(cortex.name("channel"));

    final Basin< Integer > basin = entry.basin(4);
    final var feed = basin.pipe();

    feed.emit(1);
    feed.emit(2);
    entry.await();

    basin.drain(channel);
    settle();

    assertEquals(List.of(1, 2), observed.values, "the drain preserves retention order");
    assertEquals(
      List.of(owner.current(), owner.current()),
      observed.deliveries,
      "a foreign drain recipient runs in its own circuit"
    );

  }

  /// Registers a receptor on `conduit` that records the context each delivery
  /// and each subscriber callback observes.
  private Channels channels(
    final Conduit< Integer > conduit,
    final String name
  ) {

    final var channels = new Channels();

    conduit.subscribe(
      owner.subscriber(          // a subscriber belongs to the circuit whose source it observes
        cortex.name(name),
        (_, registrar) -> {

          channels.callbacks.add(cortex.current());

          registrar.register(
            (Integer value) -> {
              channels.deliveries.add(cortex.current());
              channels.values.add(value);
            }
          );

        }
      )
    );

    return channels;

  }

  /// A target listed twice receives the emission twice. The duplicate is what
  /// distinguishes dispatching the list from dispatching its distinct members,
  /// which a provider that de-duplicated foreign admissions would do instead.
  @SpecRef({"5.1", "5.3", "6.3", "16.3"})
  @Test
  void fanout_duplicateForeignTarget_deliversOncePerOccurrence() {

    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "duplicate.observer");
    final var channel = conduit.get(cortex.name("channel"));

    entry.pipe(List.of(channel, channel)).emit(1);
    settle();

    assertEquals(List.of(1, 1), observed.values, "a target listed twice receives twice");
    assertEquals(
      List.of(owner.current(), owner.current()),
      observed.deliveries,
      "both occurrences run in the destination's context"
    );

  }

  /// Two destinations interleaved in one list: `[B1, C1, B2, B1]`. Each
  /// circuit's own subsequence is asserted and the global order is not, which
  /// is the whole distinction §16.3 draws — the list fixes the order targets
  /// are *enqueued*, and their workers decide the rest.
  @SpecRef({"5.1", "5.3", "6.3", "16.3"})
  @Test
  void fanout_interleavedDestinations_preservesEachLocalSubsequence() {

    final var third = cortex.circuit(cortex.name("third"));

    try {

      final var atOwner = new ArrayList< String >();
      final var atThird = new ArrayList< String >();

      final Pipe< Integer > b1 = owner.pipe((Integer _) -> atOwner.add("B1"));
      final Pipe< Integer > b2 = owner.pipe((Integer _) -> atOwner.add("B2"));
      final Pipe< Integer > c1 = third.pipe((Integer _) -> atThird.add("C1"));

      entry.pipe(List.of(b1, c1, b2, b1)).emit(1);

      entry.await();
      owner.await();
      third.await();

      assertEquals(
        List.of("B1", "B2", "B1"),
        atOwner,
        "one destination's own arrivals keep the list's relative order"
      );
      assertEquals(
        List.of("C1"),
        atThird,
        "and the other's are unaffected by them"
      );

    } finally {

      third.closeAwait();

    }

  }

  /// Targets on two circuits in one fan-out: each delivers in its own context,
  /// and the list order is the order each is reached in.
  ///
  /// Completion order **across** circuits is not asserted, and must not be:
  /// §16.3 promises only that cross-circuit targets are *enqueued* to their own
  /// circuits in list order; they execute on those circuits' workers, so §5.3
  /// leaves the order they finish in unconstrained.
  @SpecRef({"5.1", "5.3", "6.3"})
  @Test
  void fanout_mixingCircuits_deliversEachInItsOwnContext() {

    final var local = new ArrayList< Current >();
    final var foreign = new ArrayList< Current >();

    final Pipe< Integer > here = entry.pipe((Integer _) -> local.add(cortex.current()));
    final Pipe< Integer > there = owner.pipe((Integer _) -> foreign.add(cortex.current()));

    entry.pipe(List.of(here, there)).emit(1);
    settle();

    assertEquals(List.of(entry.current()), local, "the same-circuit target keeps this context");
    assertEquals(List.of(owner.current()), foreign, "the cross-circuit target keeps its own");

  }

  /// A fan-out to a foreign channel delivers in the owner's context.
  @SpecRef({"5.1", "6.3", "7.3"})
  @Test
  void fanout_toForeignChannel_deliversInOwnerContext() {

    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "fanout.observer");
    final var channel = conduit.get(cortex.name("channel"));
    final Pipe< Integer > other = entry.pipe((Integer _) -> {
    });

    entry.pipe(List.of(channel, other)).emit(1);
    settle();

    assertEquals(List.of(1), observed.values, "the emission must reach the registration");
    assertEquals(
      List.of(owner.current()),
      observed.deliveries,
      "a fanned-out cross-circuit target is delivered in its own circuit's context"
    );

  }

  /// A fiber materialized against a pipe on another circuit runs its operators
  /// and its target in that circuit's context.
  @SpecRef({"5.1", "6.2", "6.3"})
  @Test
  void fiber_materializedAgainstForeignPipe_runsInOwnerContext() {

    final var stages = new ArrayList< Current >();
    final var deliveries = new ArrayList< Current >();

    final Pipe< Integer > target =
      owner.pipe((Integer _) -> deliveries.add(cortex.current()));

    final Pipe< Integer > composed =
      cortex.fiber(Integer.class)
        .peek(_ -> stages.add(cortex.current()))
        .pipe(target);

    composed.emit(1);
    settle();

    assertEquals(List.of(owner.current()), stages, "the operator runs in the target's context");
    assertEquals(List.of(owner.current()), deliveries, "and so does the target");

  }

  /// A Flow materialized against a foreign channel runs its type-changing
  /// stages in the destination's context, as [FiberContractTest]'s counterpart
  /// establishes for same-type operators against a plain pipe.
  @SpecRef({"5.1", "6.2", "6.3"})
  @Test
  void flow_materializedAgainstForeignChannel_runsInOwnerContext() {

    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "flow.observer");
    final var channel = conduit.get(cortex.name("channel"));

    final var stages = new ArrayList< Current >();

    cortex.flow(Integer.class)
      .map(value -> {
        stages.add(cortex.current());
        return value * 10;
      })
      .pipe(channel)
      .emit(1);

    settle();

    assertEquals(List.of(10), observed.values, "the transformed value must arrive");
    assertEquals(List.of(owner.current()), stages, "the stage runs in the target's context");
    assertEquals(List.of(owner.current()), observed.deliveries, "and so does the channel");

  }

  /// The pool form of the same bridge: the subscriber is given a foreign
  /// conduit as its pool, so the pipe it registers per name is minted by the
  /// other circuit rather than supplied by the caller.
  @SpecRef({"5.1", "6.3", "7.3", "7.4"})
  @Test
  void poolBackedSubscriber_toForeignConduit_deliversInOwnerContext() {

    final var source = entry.conduit(Integer.class);
    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "pooled.observer");

    source.subscribe(
      entry.subscriber(
        cortex.name("pooled.bridge"),
        conduit                            // Conduit is a Pool< Pipe< E > >
      )
    );

    source.get(cortex.name("channel")).emit(1);
    settle();

    assertEquals(List.of(1), observed.values, "the mirrored name must be reached");
    assertEquals(
      List.of(owner.current()),
      observed.deliveries,
      "a pipe the foreign pool minted belongs to that pool's circuit"
    );

  }


  // ===========================================================================
  // The forwarding matrix
  //
  // Every mechanism below hands a value to a pipe the emitting circuit does not
  // own, and each reaches the boundary by a different route through the
  // provider. A correct shared resolver does not prove that each of these
  // callers supplies the right owner, or that each takes it at all, so each
  // route needs its own case. The target is a conduit **channel** throughout,
  // for the reason this suite's header gives: a plain pipe forwards through its
  // own circuit however a provider reaches it, and cannot tell invoking from
  // admitting.
  // ===========================================================================

  /// A port owned by one circuit publishing into a channel owned by another.
  /// The interleaving is the assertion: `replace(1), emit, replace(2), emit`
  /// must publish 1 then 2, not 2 twice, so the queued read happens at each
  /// emit's position rather than when the value is finally delivered.
  @SpecRef({"5.1", "6.3", "11.5"})
  @Test
  void port_emitToForeignChannel_publishesEachQueuedValue() {

    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "port.observer");
    final var channel = conduit.get(cortex.name("channel"));

    final Port< Integer > port = entry.port(0);

    port.replace(1);
    port.emit(channel);
    port.replace(2);
    port.emit(channel);

    settle();

    assertEquals(List.of(1, 2), observed.values, "each emit publishes the value current at it");
    assertEquals(
      List.of(owner.current(), owner.current()),
      observed.deliveries,
      "a foreign publication target runs in its own circuit"
    );

  }

  /// A subscriber owned by the emitting circuit registers a pipe belonging to
  /// another. This is the bridge §7.2 leaves open once a foreign *subscriber*
  /// is ruled out, and the only one that puts a foreign pipe inside a local
  /// source's dispatch list.
  @SpecRef({"5.1", "6.3", "7.3", "7.4"})
  @Test
  void registeredPipe_toForeignChannel_deliversInOwnerContext() {

    final var source = entry.conduit(Integer.class);
    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "registered.observer");
    final var channel = conduit.get(cortex.name("channel"));

    source.subscribe(
      entry.subscriber(                     // the subscriber belongs to the source
        cortex.name("registered.bridge"),
        (_, registrar) -> registrar.register(channel)   // the pipe does not
      )
    );

    source.get(cortex.name("origin")).emit(1);
    settle();

    assertEquals(List.of(1), observed.values, "the registration must be reached");
    assertEquals(
      List.of(owner.current()),
      observed.deliveries,
      "a registered foreign pipe is admitted to its own circuit, not invoked here"
    );
    assertEquals(
      List.of(owner.current()),
      observed.callbacks,
      "and the discovery it triggers runs there too"
    );

  }

  /// `route` is `tee`'s exclusive twin: what matches leaves for the foreign
  /// circuit and does not continue, what does not match never crosses. Both
  /// halves are asserted, since a provider that forwarded everything would
  /// satisfy either half alone.
  @SpecRef({"5.1", "6.2.2", "6.3"})
  @Test
  void route_toForeignChannel_divertsMatchesAndKeepsTheRest() {

    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "route.observer");
    final var channel = conduit.get(cortex.name("channel"));

    final var kept = new ArrayList< Integer >();

    final var composed =
      cortex.fiber(Integer.class)
        .route(value -> value % 2==0, channel)
        .pipe(entry.pipe(kept::add));

    composed.emit(1);
    composed.emit(2);
    composed.emit(3);
    composed.emit(4);

    settle();

    assertEquals(List.of(2, 4), observed.values, "matches are diverted across the boundary");
    assertEquals(
      List.of(owner.current(), owner.current()),
      observed.deliveries,
      "each diverted value runs in the destination's context"
    );
    assertEquals(List.of(1, 3), kept, "and the rest never leave this circuit");

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    entry = cortex.circuit(cortex.name("entry"));
    owner = cortex.circuit(cortex.name("owner"));

  }

  /// Drains both circuits, in the order work flows through them.
  private void settle() {

    entry.await();
    owner.await();

  }

  /// A one-element fan-out list is equivalent to `pipe(target)` — including in
  /// which context it delivers.
  ///
  /// Asserted rather than assumed: the Java projection states the equivalence,
  /// so a provider that collapses the list must collapse it onto a correct path.
  /// Collapsing it onto a defective one satisfies the equivalence and breaks the
  /// contract, which is what made this case worth writing.
  @SpecRef({"5.1", "6.3", "7.3"})
  @Test
  void singletonFanout_toForeignChannel_deliversInOwnerContext() {

    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "singleton.observer");
    final var channel = conduit.get(cortex.name("channel"));

    entry.pipe(List.of(channel)).emit(1);
    settle();

    assertEquals(List.of(1), observed.values, "the emission must reach the registration");
    assertEquals(
      List.of(owner.current()),
      observed.deliveries,
      "a one-element list is no less confined than any other"
    );
    assertEquals(
      List.of(owner.current()),
      observed.callbacks,
      "and neither is the subscriber callback it triggers"
    );

  }

  /// A sink owned by one circuit with its endpoint on another. Three separate
  /// facts meet here and are asserted apart: the capture is minted by the sink
  /// that captured it, the subject names that sink's channel, and the context
  /// the endpoint observes is the endpoint's own.
  @SpecRef({"5.1", "6.3", "10.5", "11.1"})
  @Test
  void sink_withForeignEndpoint_mintsHereAndDeliversInOwnerContext() {

    final var captures = new ArrayList< Capture< Integer > >();
    final var contexts = new ArrayList< Current >();

    final Pipe< Capture< Integer > > endpoint =
      owner.pipe(
        (Capture< Integer > capture) -> {
          captures.add(capture);
          contexts.add(cortex.current());
        }
      );

    final var sink = entry.sink(endpoint);
    final var name = cortex.name("sink.channel");
    final var channel = sink.get(name);

    channel.emit(7);
    settle();

    assertEquals(1, captures.size(), "one emission mints one capture");
    assertEquals(7, captures.getFirst().emission(), "the payload crosses unchanged");
    assertSame(
      channel.subject(),
      captures.getFirst().subject(),
      "the capture names the channel it was captured at, not the endpoint"
    );
    assertEquals(
      List.of(owner.current()),
      contexts,
      "while the endpoint runs in the circuit that owns it"
    );

  }

  @AfterEach
  void tearDown() {

    entry.closeAwait();
    owner.closeAwait();

  }

  /// `tee` crosses the boundary sideways: the fiber itself runs in the emitting
  /// circuit, and only the side pipe is foreign. The downstream target must
  /// keep running here, so this is the one shape where both contexts appear in
  /// a single emission.
  @SpecRef({"5.1", "6.2.2", "6.3"})
  @Test
  void tee_toForeignChannel_deliversInOwnerContextAndContinuesHere() {

    final var conduit = owner.conduit(Integer.class);
    final var observed = channels(conduit, "tee.observer");
    final var channel = conduit.get(cortex.name("channel"));

    final var downstream = new ArrayList< Current >();

    cortex.fiber(Integer.class)
      .tee(channel)
      .pipe(entry.pipe((Integer _) -> downstream.add(cortex.current())))
      .emit(1);

    settle();

    assertEquals(List.of(1), observed.values, "the side pipe receives the emission");
    assertEquals(
      List.of(owner.current()),
      observed.deliveries,
      "the side pipe's circuit runs it, not the fiber's"
    );
    assertEquals(
      List.of(entry.current()),
      downstream,
      "and the main line continues in the fiber's own context"
    );

  }

  /// A ticker owned by one circuit driving a channel owned by another. The
  /// ticker's own scheduling belongs to its circuit; the delivery does not.
  @SpecRef({"5.1", "6.3", "11.4"})
  @Test
  void ticker_targetingForeignChannel_deliversInOwnerContext()
    throws InterruptedException {

    final var conduit = owner.conduit(Long.class);
    final var delivered = new CountDownLatch(1);
    final var context = new AtomicReference< Current >();

    conduit.subscribe(
      owner.subscriber(
        cortex.name("ticker.observer"),
        (_, registrar) ->
          registrar.register(
            (Long _) -> {
              context.compareAndSet(null, cortex.current());
              delivered.countDown();
            }
          )
      )
    );

    final var channel = conduit.get(cortex.name("channel"));
    final var ticker = entry.ticker(Duration.ofMillis(5), channel);

    try {

      await(delivered, "a tick to reach the foreign channel");

      assertSame(
        owner.current(),
        context.get(),
        "a tick scheduled by one circuit is delivered by the circuit it targets"
      );

    } finally {

      ticker.close();

    }

  }

  /// What a delivery and a subscriber callback observed, in arrival order.
  private static final class Channels {

    final List< Current > callbacks = new ArrayList<>();
    final List< Current > deliveries = new ArrayList<>();
    final List< Integer > values = new ArrayList<>();

  }

}
