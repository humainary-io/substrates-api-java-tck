// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for what survives a lazy rebuild: SPEC §§6.2, 6.2.3,
/// 7.3–7.6.2 and 10.1.
///
/// A provider is free to defer building a channel's dispatch list until the
/// channel is used, and to rebuild it when the source's subscriptions change.
/// That freedom has a cost the rest of the suite does not price. Independent
/// materializations are covered, and so are subscription changes; **the
/// combination is not**, and it is where the freedom is spent. A rebuild that
/// reruns a surviving discovery callback, or that discards the operator state
/// a surviving registration had accumulated, looks correct in a suite that
/// changes a subscription and then asks only whether delivery resumed.
///
/// Each case here therefore does three things in order: warm a route, change
/// an **unrelated** subscription, and resume. The assertion is on what the
/// route remembered across that change, not on whether it still works. A
/// channel left dormant through the churn appears in every case, since a route
/// that is rebuilt only when used is exactly the route no emission is covering
/// while the topology moves.
///
/// The destinations are on another circuit throughout: a rebuild that loses a
/// route's owner and a rebuild that loses its state are different defects, and
/// a local target cannot tell them apart.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class RebuildContractTest
  extends TestSupport {

  /// Discovery callbacks observed, by channel name, in the order they ran.
  private final List< String > discoveries = new CopyOnWriteArrayList<>();
  /// Deliveries observed, as `channel:value`, in the order they arrived.
  private final List< String > received = new CopyOnWriteArrayList<>();
  private Cortex cortex;
  /// The circuit owning the conduits whose subscriptions churn.
  private Circuit source;
  /// The circuit owning every delivery target.
  private Circuit destination;

  /// Subscribes an unrelated observer, emits through `active` so a rebuild is
  /// actually due, and removes it again. Whatever else the provider does here,
  /// the routes installed before it must come through unchanged.
  private void churn(
    final Conduit< Integer > conduit,
    final Pipe< Integer > active,
    final int value
  ) {

    final var unrelated =
      conduit.subscribe(
        source.subscriber(
          cortex.name("unrelated"),
          (_, registrar) -> registrar.register((Integer _) -> {
          })
        )
      );

    active.emit(value);

    unrelated.close();

    settle();

  }

  /// A callback that registers one valid pipe and then throws keeps that
  /// registration, and is not retried when an unrelated subscription changes.
  ///
  /// [SubscriberContractTest#callback_registersThenThrows_preservesRegistrationWithoutRetry]
  /// establishes this across subsequent emissions. A rebuild is the case it
  /// cannot reach: a provider that treats a throwing callback as incomplete
  /// has no reason to retry it until it next rebuilds that route, and every
  /// emission before then looks correct.
  @SpecRef({"6.2", "7.3", "7.6", "15.4"})
  @Test
  void rebuild_afterCallbackThrew_keepsRegistrationWithoutRetry() {

    final var conduit = source.conduit(Integer.class);
    final var one = conduit.get(cortex.name("one"));
    final var calls = new AtomicInteger();

    conduit.subscribe(
      source.subscriber(
        cortex.name("partial"),
        (_, registrar) -> {

          calls.incrementAndGet();

          registrar.register(
            destination.pipe((Integer value) -> received.add("kept:" + value))
          );

          throw new IllegalStateException("deliberate");

        }
      )
    );

    one.emit(1);
    settle();

    churn(conduit, one, 2);

    one.emit(3);
    settle();

    assertEquals(1, calls.get(), "a completed callback is not retried by a rebuild");
    assertEquals(
      List.of("kept:1", "kept:2", "kept:3"),
      received,
      "the registration it completed before throwing keeps delivering across the change"
    );

  }

  /// Two conduits, one channel name. A provider that keys a materialization by
  /// name rather than by subscription/channel pair shares one operator between
  /// them, and the second conduit's first emission is silently swallowed as a
  /// duplicate of the first conduit's.
  @SpecRef({"6.2", "7.3", "10.1"})
  @Test
  void rebuild_sameNameInDifferentConduits_keepsMaterializationsSeparate() {

    final var first = source.conduit(Integer.class);
    final var second = source.conduit(Integer.class);
    final var shared = cortex.name("shared");

    first.subscribe(stateful("first", Fiber::diff));
    second.subscribe(stateful("second", Fiber::diff));

    first.get(shared).emit(1);
    second.get(shared).emit(1);     // the same name, a different conduit
    first.get(shared).emit(1);      // and this one really is a duplicate
    settle();

    assertEquals(
      List.of("shared:1", "shared:1"),
      received,
      "a channel name is not a cache key across conduits"
    );

    assertEquals(
      List.of("shared", "shared"),
      discoveries,
      "each conduit discovers the name for itself"
    );

  }

  /// A `limit` is a budget rather than a cadence, so a rebuild that resets it
  /// hands out a second budget. Nothing about resumed delivery reveals that:
  /// the extra emission a reset admits is indistinguishable from a legitimate
  /// one until the cap is reached twice.
  @SpecRef({"6.2", "6.2.3", "7.3", "7.6"})
  @Test
  void rebuild_unrelatedSubscriptionChange_doesNotRefillLimit() {

    final var conduit = source.conduit(Integer.class);
    final var one = conduit.get(cortex.name("one"));

    conduit.subscribe(stateful("capped", fiber -> fiber.limit(2)));

    one.emit(10);           // 1 of 2
    settle();

    churn(conduit, one, 20);  // 2 of 2

    one.emit(30);           // the budget is spent
    settle();

    assertEquals(
      List.of("one:10", "one:20"),
      received,
      "the cap counts emissions, not emissions since the last rebuild"
    );

  }

  /// A `diff` installed before an unrelated subscription change still holds the
  /// value it last passed — on the channel that carried the change, and on the
  /// one that lay dormant through it.
  ///
  /// The dormant channel is the sharper of the two. Its route is rebuilt, if at
  /// all, at the first emission after the change, which is the same emission
  /// whose duplicate-ness is being asserted; a provider that mints a fresh
  /// `diff` there delivers `two:1` twice and passes every other test in this
  /// repository.
  @SpecRef({"6.2", "6.2.3", "7.3", "7.6"})
  @Test
  void rebuild_unrelatedSubscriptionChange_preservesOperatorState() {

    final var conduit = source.conduit(Integer.class);
    final var one = conduit.get(cortex.name("one"));
    final var two = conduit.get(cortex.name("two"));

    conduit.subscribe(stateful("deduplicating", Fiber::diff));

    one.emit(1);
    two.emit(1);
    settle();

    churn(conduit, one, 2);

    one.emit(2);            // one last passed 2, during the churn
    two.emit(1);            // two last passed 1, before it
    one.emit(3);            // and a change still passes
    settle();

    assertEquals(
      List.of("one", "two"),
      discoveries,
      "a surviving subscription/channel pair is discovered once, not again per rebuild"
    );

    assertEquals(
      List.of("one:1", "two:1", "one:2", "one:3"),
      received,
      "each channel's operator remembers across the change, dormant or not"
    );

  }

  /// Closing a subscription and subscribing again is not a rebuild: it makes a
  /// new subscription/channel pair, which is discovered once and starts with
  /// the state its own callback minted.
  ///
  /// This is the boundary of the cases above. They require state to survive;
  /// this one requires it not to, and a provider that satisfied them by
  /// caching materializations against the channel alone would fail here.
  @SpecRef({"6.2", "7.3", "7.5", "7.6"})
  @Test
  void resubscribe_afterSubscriptionClose_startsFreshPerPair() {

    final var conduit = source.conduit(Integer.class);
    final var one = conduit.get(cortex.name("one"));
    final var subscriber = stateful("renewed", Fiber::diff);

    final var subscription = conduit.subscribe(subscriber);

    one.emit(1);
    settle();

    subscription.close();
    conduit.subscribe(subscriber);

    one.emit(1);            // a duplicate for the old pair, a first for the new
    settle();

    assertEquals(
      List.of("one", "one"),
      discoveries,
      "a new subscription discovers the channel for itself"
    );

    assertEquals(
      List.of("one:1", "one:1"),
      received,
      "and the fiber its callback minted starts empty"
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    source = cortex.circuit(cortex.name("source"));
    destination = cortex.circuit(cortex.name("destination"));

  }

  /// Drains both circuits, in the order work flows through them.
  private void settle() {

    source.await();
    destination.await();

  }

  /// A subscriber whose callback mints a **fresh** fiber per channel and
  /// forwards it to a pipe on the other circuit. One materialization per
  /// subscription/channel pair is the property every case below leans on.
  private Subscriber< Integer > stateful(
    final String name,
    final UnaryOperator< Fiber< Integer > > recipe
  ) {

    return
      source.subscriber(
        cortex.name(name),
        (subject, registrar) -> {

          final var label = subject.name().toString();

          discoveries.add(label);

          registrar.register(
            recipe.apply(cortex.fiber(Integer.class))
              .pipe(
                destination.pipe(
                  (Integer value) -> received.add(label + ":" + value)
                )
              )
          );

        }
      );

  }

  @AfterEach
  void tearDown() {

    source.closeAwait();
    destination.closeAwait();

  }

}
