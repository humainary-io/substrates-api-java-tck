// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for closing one part of a connected topology: SPEC §§7.5,
/// 9.1–9.3 and 10.4–10.5.
///
/// Resource-local cleanup is covered thoroughly elsewhere. What is not is the
/// distinction these cases exist for: **the ownership graph is not the
/// communication graph**. Closing a source, a bridging subscription, a bank or
/// a circuit ends what that resource owns. It confers no authority over a
/// destination on another circuit that merely happens to be receiving from it,
/// and it is not a distributed cancellation.
///
/// Every case quiesces before it closes anything. That is deliberate, not
/// incidental: §§9.1 and 9.3 permit documented implementation choices about
/// work still racing a close, so a case that closed while work was in flight
/// would be asserting one provider's drain policy as a portable requirement.
/// The assertions here are about what is still usable **after** a close, which
/// the contract does fix.
///
/// For the same reason nothing here demands a callback after a circuit has
/// terminated — §9.1 explicitly permits cleanup never to drain in that
/// situation — and scope close order is not treated as a global callback
/// ordering across circuits.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class ConnectedShutdownContractTest
  extends TestSupport {

  private final List< String > received = new CopyOnWriteArrayList<>();
  private Cortex cortex;
  /// The circuit that produces, and whose parts are closed.
  private Circuit source;
  /// The circuit that receives, and must survive every close below.
  private Circuit destination;

  /// Warms a bridge from a channel on `conduit` to a destination pipe, and
  /// returns the subscription holding it.
  private Subscription bridge(
    final Conduit< String > conduit,
    final String name,
    final Pipe< String > target
  ) {

    final Subscriber< String > subscriber =
      source.subscriber(
        cortex.name(name),
        (_, registrar) -> registrar.register(target)
      );

    return
      conduit.subscribe(subscriber);

  }

  /// Closing the bridging subscription ends the bridge and nothing else. The
  /// destination keeps working, including through a second bridge that was
  /// never closed.
  @SpecRef({"7.5", "9.1", "10.5"})
  @Test
  void close_bridgeSubscription_endsOnlyThatBridge() {

    final var conduit = source.conduit(String.class);
    final var channel = conduit.get(cortex.name("channel"));

    final var first = bridge(conduit, "first", sink("first"));
    bridge(conduit, "second", sink("second"));

    channel.emit("warm");
    settle();

    // Two separate subscriptions. §6.3 orders registrations within a channel;
    // nothing orders one subscription's delivery against another's, so this is
    // membership rather than sequence.
    assertEquals(
      Set.of("first:warm", "second:warm"),
      Set.copyOf(received),
      "both bridges are warm before anything is closed"
    );

    received.clear();

    first.close();
    settle();                 // the close is established before anything else is asserted

    channel.emit("after");
    settle();

    assertEquals(
      List.of("second:after"),
      received,
      "the surviving bridge is untouched by its sibling's close"
    );

    // And the destination is still a usable circuit in its own right.
    destination.pipe((String value) -> received.add("direct:" + value)).emit("ping");
    destination.await();

    assertTrue(received.contains("direct:ping"), "the destination remains usable");

  }

  /// Closing the whole source circuit ends what it owns. The destination is
  /// not one of those things, however much it was receiving from it.
  @SpecRef({"9.1", "9.2", "10.4"})
  @Test
  void close_sourceCircuit_leavesTheDestinationUsable() {

    final var conduit = source.conduit(String.class);
    final var channel = conduit.get(cortex.name("channel"));

    bridge(conduit, "bridge", sink("bridge"));

    channel.emit("warm");
    settle();

    assertEquals(List.of("bridge:warm"), received, "the bridge is warm");

    received.clear();

    source.closeAwait();
    destination.await();

    // The destination still accepts work, still drains, and still awaits.
    final var independent = destination.conduit(String.class);

    independent.subscribe(
      destination.subscriber(
        cortex.name("independent"),
        (_, registrar) -> registrar.register((String value) -> received.add("independent:" + value))
      )
    );

    independent.get(cortex.name("channel")).emit("after");
    destination.await();

    assertEquals(
      List.of("independent:after"),
      received,
      "a circuit that was only ever a destination survives its producer's close"
    );

  }

  /// The destination closes first, and the source keeps emitting through
  /// bridges it still holds.
  ///
  /// Emission is a queued operation, so §9.3 forbids it throwing synchronously
  /// merely because the far side has closed; the emission is simply dropped
  /// when it gets there. The source's own local work must continue, which is
  /// the part a provider propagating the destination's state back across the
  /// boundary would break.
  @SpecRef({"9.1", "9.3", "10.5"})
  @Test
  void emit_throughBridgeToClosedDestination_dropsWithoutDisturbingTheSource() {

    final var conduit = source.conduit(String.class);
    final var channel = conduit.get(cortex.name("channel"));

    final var localTrace = new CopyOnWriteArrayList< String >();

    conduit.subscribe(
      source.subscriber(
        cortex.name("mixed"),
        (_, registrar) -> {
          registrar.register(sink("remote"));
          registrar.register(source.pipe((String value) -> localTrace.add("local:" + value)));
        }
      )
    );

    channel.emit("warm");
    settle();

    assertEquals(List.of("remote:warm"), received, "the remote leg is warm");
    assertEquals(List.of("local:warm"), localTrace, "and so is the local one");

    destination.closeAwait();

    received.clear();
    localTrace.clear();

    assertDoesNotThrow(
      () -> {
        channel.emit("after");
        source.await();
      },
      "a queued emission must not fail synchronously because its destination closed"
    );

    assertEquals(
      List.of("local:after"),
      localTrace,
      "the local sibling keeps receiving after the remote one became undeliverable"
    );

    assertTrue(received.isEmpty(), "and nothing reaches the closed destination");

  }

  /// A subscription's `onClose` runs in the context of the circuit that owns
  /// the subscriber, exactly once, while that circuit can still drain.
  ///
  /// The circuit is drained rather than terminated before the assertion, since
  /// §9.1 permits cleanup never to run once a circuit has terminated — a case
  /// this does not test and must not accidentally depend on.
  @SpecRef({"7.5", "9.1"})
  @Test
  void onClose_ofBridgingSubscription_runsOnceInTheOwningContext() {

    final var conduit = source.conduit(String.class);
    final var channel = conduit.get(cortex.name("channel"));
    final var contexts = new CopyOnWriteArrayList< Current >();

    final Subscriber< String > subscriber =
      source.subscriber(
        cortex.name("bridge"),
        (_, registrar) -> registrar.register(sink("bridge"))
      );

    final var subscription =
      conduit.subscribe(
        subscriber,
        _ -> contexts.add(cortex.current())
      );

    channel.emit("warm");
    settle();

    subscription.close();
    subscription.close();          // idempotent
    source.await();

    assertEquals(
      List.of(source.current()),
      contexts,
      "onClose runs once, in the context of the circuit owning the subscriber"
    );

  }

  /// A scope holding resources from several circuits closes each of them, and
  /// grants itself no authority beyond the ones it was given.
  ///
  /// The unregistered circuit is the assertion. A scope that reached through
  /// its members into what they communicate with would take it down too.
  @SpecRef({"9.1", "9.2", "10.4"})
  @Test
  void scope_holdingResourcesFromSeveralCircuits_closesOnlyItsMembers() {

    final var first = cortex.circuit(cortex.name("scoped.first"));
    final var second = cortex.circuit(cortex.name("scoped.second"));
    final var outside = cortex.circuit(cortex.name("outside"));

    try {

      final var scope = cortex.scope();

      scope.register(first);
      scope.register(second);

      // `outside` receives from both, and is deliberately not registered.
      final Pipe< String > target =
        outside.pipe((String value) -> received.add("outside:" + value));

      first.pipe(target).emit("a");
      second.pipe(target).emit("b");

      first.await();
      second.await();
      outside.await();

      assertEquals(2, received.size(), "both members reached the unscoped circuit");

      received.clear();
      scope.close();

      // The unregistered circuit is still live and still its own.
      outside.pipe((String value) -> received.add("outside:" + value)).emit("c");
      outside.await();

      assertEquals(
        List.of("outside:c"),
        received,
        "a scope closes what it was given, not what those resources were talking to"
      );

    } finally {

      outside.closeAwait();

    }

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    source = cortex.circuit(cortex.name("source"));
    destination = cortex.circuit(cortex.name("destination"));

  }

  private void settle() {

    source.await();
    destination.await();

  }

  /// A pipe on the destination circuit that records what reaches it.
  private Pipe< String > sink(
    final String label
  ) {

    return
      destination.pipe((String value) -> received.add(label + ":" + value));

  }

  @AfterEach
  void tearDown() {

    source.closeAwait();
    destination.closeAwait();

  }

}
