// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import io.humainary.substrates.api.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §§7.1–7.6 Source, Subscriber, Registrar, Subscription lifecycle,
/// lazy discovery, visibility-window, circuit-affinity, and pool-backed behavior.
/// @author William David Louth
/// @since 1.0

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.0/SPEC.md")
final class SubscriberContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;

  /// A Subscriber callback runs exactly once per Subscription and named Pipe.
  @SpecRef("7.3")
  @Test
  void callback_multipleEmissionsOnSamePipe_isInvokedExactlyOnce() {

    final var callbacks = new AtomicInteger();
    final var deliveries = new AtomicInteger();
    final var conduit = circuit.conduit(Integer.class);
    final var subscriber = circuit.< Integer > subscriber(
      cortex.name("exactly.once"),
      (_, registrar) -> {
        callbacks.incrementAndGet();
        registrar.register(_ -> deliveries.incrementAndGet());
      }
    );

    conduit.subscribe(subscriber);
    final var pipe = conduit.get(cortex.name("same.pipe"));
    pipe.emit(1);
    pipe.emit(2);
    circuit.await();

    assertEquals(1, callbacks.get());
    assertEquals(2, deliveries.get());

  }

  /// Validates that subscriber callbacks are lazy until the first emission.
  ///
  /// Subscriber discovery is lazy. This means:
  ///
  /// 1. subscribe() alone does NOT invoke the callback
  /// 2. get() (channel creation) does NOT invoke the callback
  /// 3. Only emit() triggers the lazy rebuild that invokes the callback
  ///
  /// This lazy design avoids unnecessary work when subscriptions change frequently
  /// and ensures the callback executes on the circuit thread (not the calling thread).
  ///
  /// Expected: callbackCount is zero after subscribe and Pipe lookup, then one after emit.
  /// Version-tracked Subscriber discovery is lazy until Pipe emission.
  @SpecRef("7.6.2")
  @Test
  void callback_pipeLookupWithoutEmission_isNotInvoked() {

    final var conduit =
      circuit.conduit(
        Long.class
      );

    final var callbackCount = new AtomicInteger(0);

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("lazy"),
        (_, registrar) -> {
          callbackCount.incrementAndGet();
          registrar.register(_ -> {
          });
        }
      )
    );

    // Pipe lookup alone does not trigger the callback.
    final var pipe =
      conduit.get(
        cortex.name("test")
      );

    circuit.await();

    assertEquals(
      0,
      callbackCount.get(),
      "Subscriber callback should not be invoked by subscribe or Pipe lookup"
    );

    // First emission triggers lazy rebuild
    pipe.emit(1L);

    circuit.await();

    assertEquals(
      1,
      callbackCount.get(),
      "Subscriber callback should be invoked by first emission"
    );

    // Subsequent emission without subscription change — no rebuild
    pipe.emit(2L);

    circuit.await();

    assertEquals(
      1,
      callbackCount.get(),
      "Subscriber callback should not be re-invoked without subscription changes"
    );

  }

  /// A failing callback is not retried and registrations completed
  /// before failure remain active for the triggering and subsequent emissions.
  @SpecRef({"7.3", "15.4"})
  @Test
  void callback_registersThenThrows_preservesRegistrationWithoutRetry() {

    final var callbacks = new AtomicInteger();
    final var deliveries = new AtomicInteger();
    final var conduit = circuit.conduit(Integer.class);
    final var subscriber = circuit.< Integer > subscriber(
      cortex.name("failure.once"),
      (_, registrar) -> {
        callbacks.incrementAndGet();
        registrar.register(_ -> deliveries.incrementAndGet());
        throw new RuntimeException("expected callback failure");
      }
    );

    conduit.subscribe(subscriber);
    final var pipe = conduit.get(cortex.name("failure.pipe"));
    pipe.emit(1);
    pipe.emit(2);
    circuit.await();

    assertEquals(1, callbacks.get());
    assertEquals(2, deliveries.get());

  }

  /// Validates that subscription.close() and subscriber.close() interact correctly.
  ///
  /// This test verifies behavior when individual subscriptions are closed before
  /// the subscriber is closed. The subscriber should handle already-closed
  /// subscriptions gracefully.
  ///
  /// Timeline:
  /// 1. Subscribe to two conduits
  /// 2. Emit values (counter = 100)
  /// 3. Close subscription1 individually
  /// 4. Emit values (counter = 150, only conduit2 receives)
  /// 5. Close subscriber (should close remaining subscription2)
  /// 6. Emit values (counter stays 150)
  ///
  /// Expected: Partial close via subscription, full cleanup via subscriber.close()
  /// Mixed Subscription and Subscriber close paths remain idempotent.
  @SpecRef({"7.2", "7.5"})
  @Test
  void close_mixedSubscriptionAndSubscriberPaths_remainsIdempotent() {

    final var conduit1 =
      circuit.conduit(
        Long.class
      );

    final var conduit2 =
      circuit.conduit(
        Long.class
      );

    final var counter = new AtomicInteger(0);

    final var subscriber =
      circuit.< Long > subscriber(
        cortex.name("mixed"),
        (_, registrar) ->
          registrar.register(
            _ -> counter.incrementAndGet()
          )
      );

    final var subscription1 = conduit1.subscribe(subscriber);
    conduit2.subscribe(subscriber);

    final var pipe1 =
      conduit1.get(
        cortex.name("test1")
      );

    final var pipe2 =
      conduit2.get(
        cortex.name("test2")
      );

    // Emit to both
    for (int i = 0; i < 50; i++) {
      pipe1.emit((long) i);
      pipe2.emit((long) i);
    }

    circuit.await();

    assertEquals(100, counter.get());

    // Close only subscription1
    subscription1.close();

    circuit.await();

    // Emit again - only conduit2 should receive
    for (int i = 0; i < 50; i++) {
      pipe1.emit((long) i);
      pipe2.emit((long) i);
    }

    circuit.await();

    assertEquals(150, counter.get());

    // Close subscriber - should handle already-closed subscription1
    subscriber.close();

    circuit.await();

    // Emit again - neither should receive
    for (int i = 0; i < 50; i++) {
      pipe1.emit((long) i);
      pipe2.emit((long) i);
    }

    circuit.await();

    assertEquals(150, counter.get());

  }

  /// Validates subscriber close with multiple subscribers and mixed operations.
  ///
  /// This test verifies that closing one subscriber does not affect other
  /// subscribers to the same conduit. Each subscriber maintains its own
  /// subscription list independently.
  ///
  /// Timeline:
  /// 1. Two subscribers subscribe to same conduit
  /// 2. Emit values (both counters = 50)
  /// 3. Close subscriber1
  /// 4. Emit values (counter1 stays 50, counter2 = 100)
  /// 5. Close subscriber2
  /// 6. Emit values (both stay same)
  ///
  /// Expected: Subscriber isolation - closing one doesn't affect others
  /// Closing one Subscription does not affect sibling Subscriptions.
  @SpecRef("7.5")
  @Test
  void close_oneOfMultipleSubscriptions_preservesSiblings() {

    final var conduit =
      circuit.conduit(
        Long.class
      );

    final var counter1 = new AtomicInteger(0);
    final var counter2 = new AtomicInteger(0);

    final var subscriber1 =
      circuit.< Long > subscriber(
        cortex.name("subscriber1"),
        (_, registrar) ->
          registrar.register(
            _ -> counter1.incrementAndGet()
          )
      );

    final var subscriber2 =
      circuit.< Long > subscriber(
        cortex.name("subscriber2"),
        (_, registrar) ->
          registrar.register(
            _ -> counter2.incrementAndGet()
          )
      );

    conduit.subscribe(subscriber1);
    conduit.subscribe(subscriber2);

    final var pipe =
      conduit.get(
        cortex.name("test")
      );

    // Emit values - both should receive
    for (int i = 0; i < 50; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    assertEquals(50, counter1.get());
    assertEquals(50, counter2.get());

    // Close subscriber1
    subscriber1.close();

    circuit.await();

    // Emit again - only subscriber2 should receive
    for (int i = 0; i < 50; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    assertEquals(50, counter1.get());
    assertEquals(100, counter2.get());

    // Close subscriber2
    subscriber2.close();

    circuit.await();

    // Emit again - neither should receive
    for (int i = 0; i < 50; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    assertEquals(50, counter1.get());
    assertEquals(100, counter2.get());

  }

  /// Validates that closing a subscriber is safe when called multiple times.
  ///
  /// This test verifies idempotency: calling subscriber.close() multiple times
  /// does not cause errors or unexpected behavior. This is important for
  /// defensive programming patterns where close() may be called in finally
  /// blocks or by multiple cleanup paths.
  ///
  /// Expected: No exceptions on repeated close calls
  /// Subscriber close is idempotent.
  @SpecRef({"7.2", "9.1"})
  @Test
  void close_repeatedSubscriberCalls_areIdempotent() {

    final var conduit =
      circuit.conduit(
        Long.class
      );

    final var counter = new AtomicInteger(0);

    final var subscriber =
      circuit.< Long > subscriber(
        cortex.name("idempotent"),
        (_, registrar) ->
          registrar.register(
            _ -> counter.incrementAndGet()
          )
      );

    conduit.subscribe(subscriber);

    final var pipe =
      conduit.get(
        cortex.name("test")
      );

    // Emit some values
    for (int i = 0; i < 10; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    assertEquals(10, counter.get());

    // Close multiple times - should not throw
    assertDoesNotThrow(subscriber::close);

    circuit.await();

    assertDoesNotThrow(subscriber::close);

    circuit.await();

    assertDoesNotThrow(subscriber::close);

    circuit.await();

    // Emit after multiple closes
    for (int i = 0; i < 10; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    // Counter should not have changed after first close
    assertEquals(10, counter.get());

  }

  /// Validates subscriber close with subscriptions across many conduits.
  ///
  /// This stress test verifies that subscriber.close() correctly handles
  /// subscriptions across many conduits, including lifecycle re-entrancy.
  ///
  /// Expected: All 10 subscriptions closed, no emissions after close
  /// Subscriber close unregisters across every subscribed Source.
  @SpecRef("7.2")
  @SuppressWarnings("unchecked")
  @Test
  void close_subscriberAcrossMultipleConduits_unregistersEverySource() {

    final var counter = new AtomicInteger(0);

    final var subscriber =
      circuit.< Long > subscriber(
        cortex.name("many"),
        (_, registrar) ->
          registrar.register(
            _ -> counter.incrementAndGet()
          )
      );

    // Create 10 conduits and subscribe to each
    final Conduit< Long >[] conduits =
      new Conduit[10];

    final Pipe< Long >[] pipes =
      new Pipe[10];

    for (int i = 0; i < 10; i++) {
      conduits[i] = circuit.conduit(Long.class);
      conduits[i].subscribe(subscriber);
      pipes[i] = conduits[i].get(cortex.name("test" + i));
    }

    // Emit to all conduits
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        pipes[j].emit((long) i);
      }
    }

    circuit.await();

    assertEquals(100, counter.get());

    // Close subscriber
    subscriber.close();

    circuit.await();

    // Emit to all conduits again
    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 10; j++) {
        pipes[j].emit((long) i);
      }
    }

    circuit.await();

    // Counter should not have changed
    assertEquals(100, counter.get());

  }

  /// Validates that closing a subscriber closes all its subscriptions across multiple conduits.
  ///
  /// This test verifies the new subscriber lifecycle management: when subscriber.close()
  /// is called, ALL subscriptions created by that subscriber across ALL conduits are
  /// automatically closed. This enables clean subscriber shutdown without tracking
  /// individual subscriptions.
  ///
  /// Timeline:
  /// 1. Create two conduits
  /// 2. Subscribe same subscriber to both conduits
  /// 3. Emit values and verify both receive (counter = 100, 50 from each)
  /// 4. Close subscriber (not individual subscriptions)
  /// 5. Emit more values
  /// 6. Verify counter unchanged (both subscriptions were closed)
  ///
  /// Key behaviors verified:
  /// - Subscriber tracks all its subscriptions internally
  /// - subscriber.close() closes ALL subscriptions atomically
  /// - Emissions after close are NOT delivered to any conduit
  /// - No exceptions from emitting to channels with closed subscriber
  ///
  /// Why this matters:
  /// - Simplified cleanup (close one thing, not many)
  /// - Resource leak prevention (no orphaned subscriptions)
  /// - Clean component shutdown (subscriber represents component boundary)
  /// - Avoids subscription tracking burden on caller
  ///
  /// Expected: 100 total before close (50 each), 100 total after close (no change)
  /// Subscriber close unregisters all of its Subscriptions.
  @SpecRef("7.2")
  @Test
  void close_subscriberWithMultipleSubscriptions_unregistersAll() {

    final var conduit1 =
      circuit.conduit(
        Long.class
      );

    final var conduit2 =
      circuit.conduit(
        Long.class
      );

    final var counter = new AtomicInteger(0);

    final var subscriber =
      circuit.< Long > subscriber(
        cortex.name("shared"),
        (_, registrar) ->
          registrar.register(
            _ -> counter.incrementAndGet()
          )
      );

    // Subscribe to both conduits
    conduit1.subscribe(subscriber);
    conduit2.subscribe(subscriber);

    final var pipe1 =
      conduit1.get(
        cortex.name("test1")
      );

    final var pipe2 =
      conduit2.get(
        cortex.name("test2")
      );

    // Emit to both conduits
    for (int i = 0; i < 50; i++) {
      pipe1.emit((long) i);
      pipe2.emit((long) i);
    }

    circuit.await();

    assertEquals(100, counter.get());

    // Close subscriber (should close ALL subscriptions)
    subscriber.close();

    circuit.await();

    // Emit after subscriber closed
    for (int i = 0; i < 50; i++) {
      pipe1.emit((long) i);
      pipe2.emit((long) i);
    }

    circuit.await();

    // Counter should not have changed
    assertEquals(100, counter.get());

  }

  /// Validates that closing a subscriber with no subscriptions is safe.
  ///
  /// This test verifies that a newly created subscriber (with no subscriptions)
  /// can be closed without errors. This is an edge case but important for
  /// error handling paths where a subscriber might be created but never used.
  ///
  /// Expected: No exceptions
  /// Closing an unused Subscriber is safe.
  @SpecRef({"7.2", "9.1"})
  @Test
  void close_subscriberWithoutSubscriptions_completesSafely() {

    final var subscriber =
      circuit.< Long > subscriber(
        cortex.name("unused"),
        (_, registrar) ->
          registrar.register(
            _ -> {
            }
          )
      );

    // Close without ever subscribing
    assertDoesNotThrow(subscriber::close);

    circuit.await();

  }

  /// Mixed registrations deliver independently while Pipe
  /// registrations retain their relative order. Cross-kind invocation order is unspecified.
  @SpecRef({"6.3", "7.4"})
  @Test
  void dispatch_mixedRegistrations_preservesPipeOrderAndDuplicateDelivery() {

    final var conduit = circuit.conduit(Integer.class);
    final var trace = new ArrayList< String >();
    final var receptorDeliveries = new AtomicInteger();
    final Pipe< Integer > first = circuit.pipe(_ -> trace.add("pipe:first"));
    final Receptor< Integer > second = _ -> receptorDeliveries.incrementAndGet();
    final Pipe< Integer > third = circuit.pipe(_ -> trace.add("pipe:third"));

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("registration.mixed"),
        (_, registrar) -> {
          registrar.register(first);
          registrar.register(second);
          registrar.register(first);
          registrar.register(second);
          registrar.register(third);
        }
      )
    );

    conduit.get(cortex.name("registration.channel")).emit(1);
    circuit.await();

    assertEquals(
      List.of("pipe:first", "pipe:first", "pipe:third"),
      trace
    );
    assertEquals(2, receptorDeliveries.get());

  }

  /// Validates that closing a subscription stops emission delivery immediately.
  ///
  /// This test verifies the complementary operation to dynamic subscription:
  /// subscriptions can be removed at runtime, and the subscriber will immediately
  /// stop receiving emissions. This enables clean detachment of observers without
  /// affecting the rest of the system.
  ///
  /// Timeline:
  /// 1. Subscribe counter to conduit
  /// 2. Emit 50 values → counter receives all 50 (counter = 50)
  /// 3. Close subscription via subscription.close()
  /// 4. Emit another 50 values → counter receives NONE (counter still = 50)
  ///
  /// The Rebuild Mechanism (Removal):
  /// When subscription.close() is called, the conduit triggers another rebuild
  /// of all channels' pipe lists. The closed subscriber is removed from the
  /// subscription registry, so it is NOT called during rebuild. Its pipes are
  /// removed from all channels, and future emissions bypass it entirely.
  ///
  /// Key behaviors verified:
  /// - Emissions while subscribed are delivered (first 50)
  /// - subscription.close() cleanly detaches subscriber
  /// - Emissions after unsubscribe are NOT delivered (second 50 ignored)
  /// - Counter value frozen at 50 (proving no emissions after close)
  /// - No errors or exceptions from emitting to channels with removed subscribers
  ///
  /// Why this matters:
  /// - Memory leak prevention (remove unused observers)
  /// - Clean shutdown (detach monitoring before stopping service)
  /// - Dynamic topology changes (rewire connections without restart)
  /// - Resource management (release expensive subscribers)
  /// - Testing/debugging (attach observer, collect data, detach)
  ///
  /// This is critical for long-running systems where observers may be ephemeral
  /// (e.g., temporary debuggers, time-limited metrics collectors).
  ///
  /// Expected: 50 emissions while subscribed, 0 after unsubscribe
  /// Closing a Subscription removes its registered delivery paths.
  @SpecRef("7.5")
  @Test
  void emit_afterSubscriptionClose_doesNotDeliver() {

    final var conduit =
      circuit.conduit(
        Long.class
      );

    final var counter = new AtomicInteger(0);

    final var subscription =
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("counter"),
          (_, registrar) ->
            registrar.register(
              _ -> counter.incrementAndGet()
            )
        )
      );

    final var pipe =
      conduit.get(
        cortex.name("test")
      );

    // Emit with subscriber
    for (int i = 0; i < 50; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    assertEquals(50, counter.get());

    // Remove subscription
    subscription.close();

    circuit.await();

    // Emit after subscriber removed
    for (int i = 0; i < 50; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    // Counter should not have changed
    assertEquals(50, counter.get());

  }

  // ===========================
  // Subscriber.close() Tests - Unregister Subscriptions Across Sources
  // ===========================

  /// Validates that emissions enqueued *at or after* the subscription's
  /// close job are not visible to the subscription.
  ///
  /// Sequence (single caller thread):
  ///   subscribe → emit(X) → subscription.close() → emit(Y) → await
  ///
  /// Per-caller FIFO guarantees the enqueue order: subscribe, X, close, Y.
  /// The circuit context processes them in that order: X falls within
  /// the visibility window [subscribe_enqueue, close_enqueue] and is
  /// delivered; Y falls at-or-after close_enqueue and is not.
  ///
  /// This is the strict form of the contract — no intermediate `await()`
  /// between close and the post-close emission, so the close effect is
  /// purely a consequence of ingress-queue position, not of synchronous
  /// caller-side side effects.
  ///
  /// Expected: the subscription receives X only. Y is outside the
  /// visibility window.
  /// Emissions admitted at or after close are outside the visibility window.
  @SpecRef("7.6.1")
  @Test
  void emit_afterVisibilityWindowClose_isNotDelivered() {

    final var conduit =
      circuit.conduit(Integer.class);

    final var pipe =
      conduit.get(cortex.name("visibility.post-close"));

    final List< Integer > captured = new ArrayList<>();

    final var subscription =
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("visibility.post-close.sub"),
          (_, registrar) ->
            registrar.register(captured::add)
        )
      );

    // X is enqueued inside the window.
    pipe.emit(10);

    // Close enqueues a close job at position N. Y at position N+1 is
    // at-or-after close_enqueue and must not be delivered.
    subscription.close();
    pipe.emit(20);

    circuit.await();

    assertEquals(
      List.of(10),
      captured,
      "Subscription must see X (enqueued before close) but not Y "
        + "(enqueued at-or-after close)"
    );

  }

  /// Validates that emissions enqueued *before* the subscription's
  /// registration job are not visible to the new subscriber.
  ///
  /// Sequence (single caller thread):
  ///   emit(X) → subscribe → emit(Y) → await
  ///
  /// Per-caller FIFO guarantees that X is enqueued before the registration
  /// job, which is enqueued before Y. The circuit context processes them
  /// in that order: X is dispatched to existing subscribers (of which
  /// there are none), then the registration is installed, then Y is
  /// dispatched to the newly installed subscription.
  ///
  /// Expected: the subscription receives Y only. X is outside the
  /// visibility window (enqueued before subscribe_enqueue).
  /// Emissions admitted before subscribe are outside the visibility window.
  @SpecRef("7.6.1")
  @Test
  void emit_beforeVisibilityWindowOpen_isNotDelivered() {

    final var conduit =
      circuit.conduit(Integer.class);

    final var pipe =
      conduit.get(cortex.name("visibility.pre-subscribe"));

    final List< Integer > captured = new ArrayList<>();

    // Enqueue X before the subscribe job. X must not be visible to the
    // subscription we are about to install.
    pipe.emit(10);

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("visibility.pre-subscribe.sub"),
        (_, registrar) ->
          registrar.register(captured::add)
      )
    );

    // Enqueue Y after the subscribe job. Y must be visible.
    pipe.emit(20);

    circuit.await();

    assertEquals(
      List.of(20),
      captured,
      "Subscription must see Y (enqueued after subscribe) but not X "
        + "(enqueued before subscribe)"
    );

  }

  /// Validates that a derived pool (from `Conduit#pool(Function)`) may be
  /// passed to the pool-based subscriber factory.
  ///
  /// This exercises the broader Pool contract — not just the Conduit-as-Pool
  /// shortcut — by wrapping each target pipe in an identity transformation
  /// before it is registered with the source's channels.
  ///
  /// Expected: emissions flow through the derived pool's pipes and reach
  /// the downstream counter.
  /// A derived-Pool-backed Subscriber delivers transformed paths.
  @SpecRef({"7.2", "10.1"})
  @Test
  void emit_derivedPoolBackedSubscriber_deliversEmission() {

    final var source =
      circuit.conduit(Long.class);

    final var target =
      circuit.conduit(Long.class);

    final var counter = new AtomicInteger(0);

    target.subscribe(
      circuit.subscriber(
        cortex.name("derived.counter"),
        (_, registrar) ->
          registrar.register(
            _ -> counter.incrementAndGet()
          )
      )
    );

    final Pool< Pipe< Long > > pipes =
      target.pool(
        pipe -> pipe
      );

    source.subscribe(
      circuit.subscriber(
        cortex.name("derived.forwarder"),
        pipes
      )
    );

    final var pipe =
      source.get(
        cortex.name("channel")
      );

    pipe.emit(1L);
    pipe.emit(2L);
    pipe.emit(3L);

    circuit.await();

    assertEquals(
      3,
      counter.get()
    );

  }

  /// Validates fan-out behavior: multiple subscribers receive all emissions.
  ///
  /// This test verifies that a single channel can broadcast to multiple
  /// subscribers simultaneously, with each subscriber receiving every emission.
  /// This is the foundation of the observer pattern and enables separation
  /// of concerns across different observability dimensions.
  ///
  /// Setup:
  /// - Create conduit with two subscribers (counter1 and counter2)
  /// - Each subscriber registers its own counting pipe
  /// - Get channel and emit 100 values
  ///
  /// Fan-Out Semantics:
  /// When a channel emits a value, it forwards to ALL pipes registered by
  /// ALL subscribers. The pipes run sequentially in Circuit context, ensuring
  /// deterministic order.
  ///
  /// Execution flow for each emission:
  /// ```
  /// emit(value)
  ///   ↓
  /// channel.pipe.emit(value)  // enters circuit queue
  ///   ↓
  /// [circuit thread processes]
  ///   ↓
  /// counter1.pipe.emit(value) → counter1++
  ///   ↓
  /// counter2.pipe.emit(value) → counter2++
  /// ```
  ///
  /// Key behaviors verified:
  /// - Both subscribers receive ALL emissions (100 each)
  /// - No emissions are lost or duplicated
  /// - Subscribers execute independently (counter1 doesn't affect counter2)
  /// - Order is deterministic (sequential on circuit thread)
  ///
  /// Why this matters:
  /// - Separation of concerns (metrics, logging, tracing as separate subscribers)
  /// - Independent observability (add/remove observers without affecting others)
  /// - Fan-out pattern (one source, many sinks)
  /// - Modular monitoring (compose different observers)
  ///
  /// Real-world example:
  /// One HTTP request channel broadcasting to:
  /// - Latency metrics subscriber
  /// - Error logging subscriber
  /// - Distributed tracing subscriber
  /// - Request counting subscriber
  ///
  /// Expected: Both counters reach 100 (proving fan-out works)
  /// Multiple Subscribers independently receive each visible emission.
  @SpecRef({"7.3", "7.4"})
  @Test
  void emit_multipleSubscribers_deliversToEverySubscriber() {

    final var conduit =
      circuit.conduit(
        Long.class
      );

    final var counter1 = new AtomicInteger(0);
    final var counter2 = new AtomicInteger(0);

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("counter1"),
        (_, registrar) ->
          registrar.register(
            _ -> counter1.incrementAndGet()
          )
      )
    );

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("counter2"),
        (_, registrar) ->
          registrar.register(
            _ -> counter2.incrementAndGet()
          )
      )
    );

    final var pipe =
      conduit.get(
        cortex.name("test")
      );

    // Emit values - both subscribers should receive
    for (int i = 0; i < 100; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    assertEquals(100, counter1.get());
    assertEquals(100, counter2.get());

  }

  /// Validates that the pool-based subscriber factory delivers emissions to
  /// pipes drawn from the given pool.
  ///
  /// The factory is sugar for the BiConsumer form: each new channel registers
  /// `pool.get(subject)`. This test wires a `source` conduit's emissions
  /// through a `target` conduit (which is itself a `Pool<Pipe<Long>>`) and
  /// counts deliveries via a downstream subscriber on the target.
  ///
  /// Expected: every emission reaches the counter exactly once.
  /// A pool-backed Subscriber routes emissions by discovered Pipe subject.
  @SpecRef("7.2")
  @Test
  void emit_poolBackedSubscriber_deliversEmission() {

    final var source =
      circuit.conduit(Long.class);

    final var target =
      circuit.conduit(Long.class);

    final var counter = new AtomicInteger(0);

    target.subscribe(
      circuit.subscriber(
        cortex.name("pool.counter"),
        (_, registrar) ->
          registrar.register(
            _ -> counter.incrementAndGet()
          )
      )
    );

    // Conduit<Long> is a Pool<Pipe<Long>>, passed directly to the new factory.
    source.subscribe(
      circuit.subscriber(
        cortex.name("pool.forwarder"),
        target
      )
    );

    final var pipe =
      source.get(
        cortex.name("channel")
      );

    for (int i = 0; i < 25; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    assertEquals(
      25,
      counter.get()
    );

  }

  /// An active Subscriber registers a path receiving the triggering emission.
  @SpecRef("7.3")
  @Test
  void emit_withActiveSubscriber_deliversEmission() {

    final var conduit =
      circuit.conduit(
        Long.class
      );

    final var counter = new AtomicInteger(0);

    final var subscription =
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("counter"),
          (_, registrar) ->
            registrar.register(
              _ -> counter.incrementAndGet()
            )
        )
      );

    final var pipe =
      conduit.get(
        cortex.name("test")
      );

    // Emit values with subscriber registered
    for (int i = 0; i < 100; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    assertEquals(100, counter.get());

    subscription.close();

  }

  /// Validates that all emissions enqueued *within* the visibility
  /// window are delivered to the subscription.
  ///
  /// Sequence (single caller thread):
  ///   subscribe → emit(X) → emit(Y) → await
  ///
  /// Both emissions are enqueued after the registration job; both fall
  /// within the subscription's visibility window.
  ///
  /// Expected: the subscription receives both X and Y in order.
  /// Every emission admitted within the visibility window is delivered.
  @SpecRef("7.6.1")
  @Test
  void emit_withinVisibilityWindow_isDelivered() {

    final var conduit =
      circuit.conduit(Integer.class);

    final var pipe =
      conduit.get(cortex.name("visibility.within"));

    final List< Integer > captured = new ArrayList<>();

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("visibility.within.sub"),
        (_, registrar) ->
          registrar.register(captured::add)
      )
    );

    pipe.emit(10);
    pipe.emit(20);

    circuit.await();

    assertEquals(
      List.of(10, 20),
      captured,
      "Subscription must see all emissions enqueued after subscribe"
    );

  }

  /// Validates that emissions without subscribers are safe (no-op behavior).
  ///
  /// This test verifies a critical robustness property: channels can emit values
  /// even when no subscribers are registered, and the system handles this gracefully
  /// without errors, exceptions, or performance degradation.
  ///
  /// Scenario:
  /// - Create conduit and channel with ZERO subscribers
  /// - Emit 1000 values into the void
  /// - Verify no exceptions or errors occur
  ///
  /// Why This is Important:
  /// In real systems, subscribers may be:
  /// - Not yet attached (startup race condition)
  /// - Temporarily removed (dynamic reconfiguration)
  /// - Conditionally absent (optional observability)
  ///
  /// The substrate MUST handle "emitting to nobody" gracefully rather than:
  /// - Throwing NullPointerException
  /// - Requiring null checks everywhere
  /// - Forcing sentinel/dummy subscribers
  ///
  /// Observable behavior:
  /// A channel without subscribers accepts emissions without invoking a target
  /// or requiring a sentinel registration.
  ///
  /// Performance characteristics:
  /// - Emissions without subscribers have minimal overhead (empty loop)
  /// - No allocations or heap pressure
  /// - Circuit queue processes and discards quickly
  /// - Safe for high-frequency emissions during startup
  ///
  /// Real-world scenarios:
  /// - Application starting before monitoring connects
  /// - Testing/debugging with selective instrumentation
  /// - Feature flags disabling certain observers
  /// - Graceful degradation when monitoring service is down
  ///
  /// This design choice (no-op vs error) enables:
  /// - Simpler emission code (no defensive checks needed)
  /// - More robust systems (degrades gracefully)
  /// - Easier testing (no mock subscribers required)
  ///
  /// Expected: 1000 emissions complete without errors or exceptions
  /// Source emission without Subscribers completes without downstream delivery.
  @SpecRef("7.1")
  @Test
  void emit_withoutSubscribers_completesSafely() {

    final var conduit =
      circuit.conduit(
        Long.class
      );

    final var pipe =
      conduit.get(
        cortex.name("test")
      );

    // Emit many values without any subscribers
    for (int i = 0; i < 1000; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    // No assertions needed - just verify no exceptions

  }

  /// Validates that registrar enforces temporal constraint.
  ///
  /// The registrar is only valid during the subscriber callback. Calling
  /// register() after the callback has returned must throw IllegalStateException.
  /// This prevents silent mutations that would have no effect until the next
  /// rebuild, enforcing the @Temporal contract documented in the API.
  ///
  /// Expected: IllegalStateException when register() called after callback
  /// Both Registrar overloads signal temporal misuse after the
  /// callback scope.
  @SpecRef({"6.4", "6.4.1", "7.4"})
  @Test
  void register_afterSubscriberCallback_throwsFault() {

    final var conduit =
      circuit.conduit(
        Long.class
      );

    final var captured =
      new AtomicReference< Substrates.Registrar< Long > >();

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("capture"),
        (_, registrar) -> {
          registrar.register(_ -> {
          });
          captured.set(registrar);
        }
      )
    );

    final var pipe =
      conduit.get(
        cortex.name("test")
      );

    // Emit to trigger rebuild, which fires the subscriber callback
    pipe.emit(1L);

    // Ensure the callback has completed on the circuit thread
    circuit.await();

    final var registrar = captured.get();

    assertNotNull(registrar);

    // Registrar should be closed — both register overloads must throw.
    assertAll(
      () ->
        assertThrows(
          IllegalStateException.class,
          () -> registrar.register(
            _ -> {
            }
          )
        ),
      () ->
        assertThrows(
          IllegalStateException.class,
          () -> registrar.register(
            circuit.pipe()
          )
        )
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

    circuit = cortex.circuit();

  }

  // =====================================================================
  // Visibility Window Tests (SPEC §7.6.1)
  //
  // A subscription's visibility window is the half-open interval of
  // ingress-queue positions [subscribe_enqueue, close_enqueue]. A
  // subscription receives exactly those emissions whose ingress-queue
  // enqueue position falls within this window.
  //
  // These tests exercise the three canonical cases from §7.6.1 using a
  // single caller thread, where per-caller FIFO makes the enqueue order
  // deterministic.
  // =====================================================================

  /// Validates that subscribing to a closed source faults.
  ///
  /// Sequence (single caller thread):
  ///   conduit.close() → conduit.subscribe(newSub, onClose) → pipe.emit(X) → await
  ///
  /// Non-close operations attempted after the component has begun closing must
  /// fail in the caller context. No subscription is registered, the receptor
  /// never fires, and the caller-owned onClose callback is not synthesized.
  /// Subscribing after Source close signals closed resource.
  @SpecRef({"7.2", "9.1"})
  @Test
  void subscribe_afterSourceClose_throwsFault() {

    final var conduit =
      circuit.conduit(Integer.class);

    final var pipe =
      conduit.get(cortex.name("subscribe.after.close.channel"));

    final var receptorFired = new AtomicInteger(0);
    final var onCloseFired = new AtomicInteger(0);

    // Enqueue the conduit close job first. Any subsequent non-close operation
    // on this conduit must fault.
    conduit.close();

    final var subscriber =
      circuit.< Integer > subscriber(
        cortex.name("subscribe.after.close.sub"),
        (_, registrar) ->
          registrar.register(_ -> receptorFired.incrementAndGet())
      );

    // Subscribe after close.
    final var exception =
      assertThrows(
        Substrates.Fault.class,
        () ->
          conduit.subscribe(
            subscriber,
            _ -> onCloseFired.incrementAndGet()
          ),
        "Subscribing after conduit.close() must fault"
      );

    assertSame(
      conduit.subject(),
      exception.subject()
    );

    assertEquals(
      "subscribe",
      exception.operation()
    );

    // Emit after close must not throw and must not be delivered.
    assertDoesNotThrow(() -> pipe.emit(42));

    circuit.await();

    assertEquals(
      0,
      receptorFired.get(),
      "Receptor of a subscription attempted after conduit.close() must "
        + "never fire"
    );

    assertEquals(
      0,
      onCloseFired.get(),
      "No subscription is created, so the caller-owned onClose callback must "
        + "not fire"
    );

  }

  /// Validates that subscribing with a subscriber from a different circuit throws an exception.
  ///
  /// This test verifies a critical safety mechanism: subscribers are bound to the circuit
  /// that created them and cannot be used with a different circuit. This prevents subtle
  /// threading bugs that would occur if a subscriber's pipes were invoked through a
  /// different Circuit context.
  ///
  /// Why this matters: Subscribers register Pipes that execute in their owning
  /// Circuit context; cross-Circuit subscription would violate that confinement.
  ///
  /// The check happens at subscription time (fail-fast) rather than at emit time,
  /// making the error immediately obvious during development.
  ///
  /// Expected: Substrates.Fault thrown with descriptive message
  /// Cross-Circuit subscription is rejected synchronously without side effects.
  @SpecRef({"7.2", "15.1"})
  @Test
  void subscribe_foreignCircuitSubscriber_throwsFaultWithoutSideEffects() {

    final var circuit2 = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(
          Long.class
        );

      // Create subscriber from circuit2
      final var subscriber =
        circuit2.< Long > subscriber(
          cortex.name("cross-circuit"),
          (_, registrar) ->
            registrar.register(_ -> {
            })
        );

      // Attempt to subscribe to conduit from circuit1
      final var exception =
        assertThrows(
          Substrates.Fault.class,
          () -> conduit.subscribe(subscriber)
        );

      assertTrue(
        exception.getMessage().startsWith("Subscriber belongs to a different circuit")
      );

      assertTrue(
        exception.getMessage().contains(subscriber.subject().toString()),
        "fault message should render the offending subscriber"
      );

      assertSame(
        conduit.subject(),
        exception.subject()
      );

      assertEquals(
        "subscribe",
        exception.operation()
      );

    } finally {

      circuit2.close();

    }

  }

  /// Validates dynamic subscription: subscribers can be added after channel creation.
  ///
  /// This test verifies a critical feature of the substrate: subscribers can be
  /// added to conduits at runtime, and will immediately start receiving emissions
  /// from existing channels. This enables runtime topology reconfiguration without
  /// stopping the system.
  ///
  /// Timeline:
  /// 1. Create conduit and get channel
  /// 2. Emit 50 values with NO subscribers
  /// 3. Add subscriber (registers counter pipe)
  /// 4. Emit 50 values with subscriber active
  ///
  /// The Rebuild Mechanism:
  /// When a subscriber is added via conduit.subscribe(), the conduit triggers
  /// a rebuild of all existing channels' pipe lists. The subscriber is called
  /// for each existing channel, allowing it to register pipes that will receive
  /// future emissions.
  ///
  /// Key behaviors verified:
  /// - Emissions before subscription are dropped (counter = 0 after phase 2)
  /// - Subscriber sees existing channel via callback
  /// - Emissions after subscription are delivered (counter = 50 after phase 4)
  /// - No emissions are retroactively delivered (only future ones)
  ///
  /// Why this matters:
  /// - Hot-swappable observability (add metrics/tracing without restart)
  /// - Dynamic monitoring (attach debuggers to running circuits)
  /// - Runtime topology changes (rewire neural networks on the fly)
  /// - Gradual system evolution (add features without downtime)
  ///
  /// This is analogous to hot module replacement or live reloading, but for
  /// event-driven data flows.
  ///
  /// Expected: 0 emissions before subscription, 50 after subscription
  /// A Subscriber dynamically discovers active named Pipes.
  @SpecRef("7.3")
  @Test
  void subscribe_newNamedPipes_discoversDynamically() {

    final var conduit =
      circuit.conduit(
        Long.class
      );

    final var counter = new AtomicInteger(0);

    final var pipe =
      conduit.get(
        cortex.name("test")
      );

    // Emit before subscription
    for (int i = 0; i < 50; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    assertEquals(0, counter.get());

    // Add subscription
    conduit.subscribe(
      circuit.subscriber(
        cortex.name("counter"),
        (_, registrar) ->
          registrar.register(
            _ -> counter.incrementAndGet()
          )
      )
    );

    // Emit after subscription
    for (int i = 0; i < 50; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    assertEquals(50, counter.get());

  }

  /// Validates that subscribing with the same circuit's subscriber works correctly.
  ///
  /// This is the positive counterpart to the cross-Circuit rejection case.
  ///
  /// Expected: No exception, subscription succeeds, emissions are received
  /// A Subscriber may subscribe to a Source on its owning Circuit.
  @SpecRef("7.2")
  @Test
  void subscribe_sameCircuitSubscriber_returnsSubscription() {

    final var conduit =
      circuit.conduit(
        Long.class
      );

    final var counter = new AtomicInteger(0);

    // Create subscriber from same circuit - should work

    // This should NOT throw
    assertDoesNotThrow(
      () -> conduit.subscribe(
        circuit.subscriber(
          cortex.name("same-circuit"),
          (_, registrar) ->
            registrar.register(_ -> counter.incrementAndGet())
        )
      )
    );

    final var pipe =
      conduit.get(
        cortex.name("test")
      );

    pipe.emit(1L);
    pipe.emit(2L);
    pipe.emit(3L);

    circuit.await();

    assertEquals(3, counter.get());

  }

  /// Closed-Subscriber rejection identifies the offending argument.
  @SpecRef({"7.2", "15.3"})
  @Test
  void subscribe_withClosedSubscriber_throwsFaultIdentifyingSubscriber() {

    final var conduit =
      circuit.conduit(Integer.class);

    final var subscriber =
      circuit.< Integer > subscriber(
        cortex.name("closed.subscriber.argument"),
        (_, registrar) ->
          registrar.register(_ -> {
          })
      );

    subscriber.close();

    final var exception =
      assertThrows(
        Substrates.Fault.class,
        () -> conduit.subscribe(subscriber)
      );

    assertSame(
      conduit.subject(),
      exception.subject()
    );

    assertEquals(
      "subscribe",
      exception.operation()
    );

    assertTrue(
      exception.getMessage().contains(subscriber.subject().toString()),
      "fault message should render the offending subscriber"
    );

  }

  /// Validates that the pool-based subscriber factory accepts a pool whose
  /// pipe element type is a supertype of the subscriber's emission type.
  ///
  /// `Registrar#register(Pipe)` accepts `Pipe<? super E>`, and the factory
  /// follows the same variance: a `Pool<Pipe<Object>>` is acceptable when
  /// wiring a `Subscriber<Long>`, because each `Pipe<Object>` can receive
  /// `Long` emissions. This test proves the API shape compiles in that
  /// configuration and that the wider pipes still receive emissions.
  ///
  /// Expected: emissions on a `Long` source land in the wider `Object`
  /// target's downstream counter.
  /// Pool-backed Subscriber accepts a contravariant Pipe Pool.
  @Test
  void subscriber_contravariantPipePool_isAccepted() {

    final var source =
      circuit.conduit(Long.class);

    final var target =
      circuit.conduit(Object.class);

    final var counter = new AtomicInteger(0);

    target.subscribe(
      circuit.subscriber(
        cortex.name("variance.counter"),
        (_, registrar) ->
          registrar.register(
            _ -> counter.incrementAndGet()
          )
      )
    );

    // Pool<Pipe<Object>> wired into a Subscriber<Long>.
    @SuppressWarnings("UnnecessaryLocalVariable") final Pool< Pipe< Object > > wider = target;

    source.subscribe(
      circuit.subscriber(
        cortex.name("variance.forwarder"),
        wider
      )
    );

    final var pipe =
      source.get(
        cortex.name("channel")
      );

    pipe.emit(7L);
    pipe.emit(8L);

    circuit.await();

    assertEquals(
      2,
      counter.get()
    );

  }

  /// Validates that the pool-based subscriber factory rejects null arguments.
  ///
  /// Both [Circuit#subscriber(Name, Pool)] parameters are non-null per contract.
  /// This guards the API surface so misuse fails fast at the boundary rather
  /// than later when the subscriber is registered.
  /// Pool-backed Subscriber factory rejects null arguments.
  @SpecRef("15.2")
  @Test
  void subscriber_poolFactoryWithNullArguments_throwsNullPointerException() {

    final var conduit =
      circuit.conduit(Long.class);

    assertThrows(
      NullPointerException.class,
      () -> circuit.subscriber(
        null,
        conduit
      )
    );

    assertThrows(
      NullPointerException.class,
      () -> circuit.subscriber(
        cortex.name("pool.null"),
        (Pool< Pipe< Long > >) null
      )
    );

  }

  @AfterEach
  void tearDown() {

    circuit.closeAwait();

  }

}
