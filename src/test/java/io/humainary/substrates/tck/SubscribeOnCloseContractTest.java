// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.*;
import java.util.function.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for the [Source#subscribe(Subscriber, Consumer)] onClose lifecycle extension,
/// including every termination path, fire-once behavior, context, identity, rejection, isolation,
/// and absence validation.

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.2/SPEC.md")
final class SubscribeOnCloseContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;

  /// A callback that throws does not break other subscriptions' cleanup when the
  /// source is closed.
  /// A failing onClose callback does not block sibling cleanup.
  @SpecRef("15.4")
  @Test
  void onClose_callbackThrows_preservesSiblingCleanup() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var goodFired = new AtomicInteger(0);

    // Buggy subscription - its callback throws
    conduit.subscribe(
      circuit.subscriber(
        cortex.name("bad-observer"),
        (_, registrar) ->
          registrar.register(
            _ -> {
            }
          )
      ),
      _ -> {
        throw new RuntimeException("deliberate-failure");
      }
    );

    // Good subscription - its callback must still fire
    conduit.subscribe(
      circuit.subscriber(
        cortex.name("good-observer"),
        (_, registrar) ->
          registrar.register(
            _ -> {
            }
          )
      ),
      _ -> goodFired.incrementAndGet()
    );

    conduit.close();
    circuit.await();

    assertEquals(1, goodFired.get());

  }

  /// Rejected subscribe creates no Subscription and does not invoke onClose.
  @SpecRef("7.2")
  @Test
  void onClose_closedSourceRejectsSubscription_isNotInvoked() {

    final var conduit = circuit.conduit(Integer.class);
    final var subscriber = circuit.< Integer > subscriber(
      cortex.name("rejected.observer"),
      (_, _) -> {
      }
    );
    final var callbacks = new AtomicInteger();

    conduit.closeAwait();

    assertThrows(
      Fault.class,
      () -> conduit.subscribe(subscriber, _ -> callbacks.incrementAndGet())
    );
    assertEquals(0, callbacks.get());

  }

  /// Fires when the source conduit is closed via Conduit#close().
  /// Source close terminates its Subscription and invokes onClose.
  @SpecRef({"7.1", "7.5"})
  @Test
  void onClose_conduitCloses_firesCallback() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var fired = new AtomicInteger(0);

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("observer"),
        (_, registrar) ->
          registrar.register(
            _ -> {
            }
          )
      ),
      _ -> fired.incrementAndGet()
    );

    conduit.close();
    circuit.await();

    assertEquals(1, fired.get());

  }

  /// Fires exactly once even when multiple close paths are invoked.
  /// Competing close paths invoke onClose at most once.
  @SpecRef("7.5")
  @Test
  void onClose_multipleClosePaths_firesExactlyOnce() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var fired = new AtomicInteger(0);

    final var subscriber =
      circuit.< Integer > subscriber(
        cortex.name("observer"),
        (_, registrar) ->
          registrar.register(
            _ -> {
            }
          )
      );

    final var sub =
      conduit.subscribe(
        subscriber,
        _ -> fired.incrementAndGet()
      );

    // Close via subscription, subscriber, and conduit - callback fires once
    sub.close();
    circuit.await();

    subscriber.close();
    circuit.await();

    conduit.close();
    circuit.await();

    assertEquals(1, fired.get());

  }

  /// Repeated subscription.close() calls must fire onClose exactly once.
  /// Repeated Subscription close calls are idempotent and notify once.
  @SpecRef("7.5")
  @Test
  void onClose_repeatedSubscriptionClose_firesExactlyOnce() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var fired = new AtomicInteger(0);

    final var sub =
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("observer"),
          (_, registrar) ->
            registrar.register(
              _ -> {
              }
            )
        ),
        _ -> fired.incrementAndGet()
      );

    sub.close();
    sub.close();
    sub.close();
    circuit.await();

    assertEquals(1, fired.get());

  }

  /// Fires when the subscriber's close() cascades to its subscriptions.
  /// Subscriber close cascades termination and invokes onClose.
  @SpecRef({"7.2", "7.5"})
  @Test
  void onClose_subscriberCloses_firesCallback() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var fired = new AtomicInteger(0);

    final var subscriber =
      circuit.< Integer > subscriber(
        cortex.name("observer"),
        (_, registrar) ->
          registrar.register(
            _ -> {
            }
          )
      );

    conduit.subscribe(
      subscriber,
      _ -> fired.incrementAndGet()
    );

    subscriber.close();
    circuit.await();

    assertEquals(1, fired.get());

  }

  /// Fires when the caller closes the returned subscription directly.
  /// Direct Subscription close invokes onClose after termination processing.
  @SpecRef("7.5")
  @Test
  void onClose_subscriptionCloses_firesCallback() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var fired = new AtomicInteger(0);

    final var sub =
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("observer"),
          (_, registrar) ->
            registrar.register(
              _ -> {
              }
            )
        ),
        _ -> fired.incrementAndGet()
      );

    sub.close();
    circuit.await();

    assertEquals(1, fired.get());

  }

  /// OnClose executes in the owning Circuit context.
  @SpecRef("7.5")
  @Test
  void onClose_subscriptionTerminates_executesInCircuitContext() {

    final var conduit = circuit.conduit(Integer.class);
    final var observed = new AtomicReference< Current >();
    final var subscription = conduit.subscribe(
      circuit.subscriber(
        cortex.name("context.observer"),
        (_, _) -> {
        }
      ),
      _ -> observed.set(cortex.current())
    );

    subscription.close();
    circuit.await();

    assertSame(circuit.current(), observed.get());

  }

  /// Callback receives the subscription that was terminated.
  /// OnClose receives the Subscription being terminated.
  @Test
  void onClose_subscriptionTerminates_receivesTerminatedSubscription() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var received = new AtomicReference< Subscription >();

    final var sub =
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("observer"),
          (_, registrar) ->
            registrar.register(
              _ -> {
              }
            )
        ),
        received::set
      );

    sub.close();
    circuit.await();

    assertSame(sub, received.get());

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    circuit = cortex.circuit();

  }

  /// A null onClose argument throws NullPointerException.
  /// Source#subscribe rejects an absent onClose callback.
  @SpecRef("15.2")
  @Test
  void subscribe_nullOnClose_throwsNullPointerException() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var subscriber =
      circuit.< Integer > subscriber(
        cortex.name("observer"),
        (_, registrar) ->
          registrar.register(
            _ -> {
            }
          )
      );

    assertThrows(
      NullPointerException.class,
      () -> conduit.subscribe(subscriber, null)
    );

  }

  /// The single-argument subscribe overload (default method) continues to work
  /// without any callback.
  /// The single-argument Source#subscribe overload needs no callback.
  @Test
  void subscribe_withoutOnClose_returnsSubscription() {

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    final var received = new AtomicInteger(0);

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("observer"),
        (_, registrar) ->
          registrar.register(
            _ -> received.incrementAndGet()
          )
      )
    );

    final var pipe =
      conduit.get(
        cortex.name("channel")
      );

    pipe.emit(1);
    pipe.emit(2);
    circuit.await();

    assertEquals(2, received.get());

  }

  @AfterEach
  void tearDown() {

    circuit.closeAwait();

  }

}
