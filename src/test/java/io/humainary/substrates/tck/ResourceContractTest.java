// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.time.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for the universal SPEC §9.1 Resource `closeAwait` contract.
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.0/SPEC.md")
final class ResourceContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;

  /// Both a harmless terminal operation and an open-required factory are checked after closeAwait,
  /// separating a fully reached terminal state from an incomplete asynchronous shutdown.
  ///
  /// Circuit#closeAwait reaches terminal state before return.
  @SpecRef({"9.1", "9.3"})
  @Test
  void closeAwait_circuit_reachesTerminalState() {

    circuit.closeAwait();

    assertDoesNotThrow(circuit::await);
    assertThrows(Fault.class, () -> circuit.conduit(Integer.class));
    assertDoesNotThrow(circuit::closeAwait);

  }

  /// Conduit#closeAwait completes before open-required Source operations.
  @SpecRef("9.1")
  @Test
  void closeAwait_conduit_rejectsOpenRequiredOperations() {

    final var conduit = circuit.conduit(Integer.class);
    final var subscriber =
      circuit.< Integer > subscriber(
        cortex.name("resource.conduit"),
        (_, _) -> {
        }
      );

    conduit.closeAwait();

    assertThrows(Fault.class, () -> conduit.subscribe(subscriber));
    assertDoesNotThrow(conduit::closeAwait);

  }

  /// Calling closeAwait from a Circuit callback would wait for the callback itself. The test makes
  /// that self-deadlock guard explicit instead of relying on a timeout to reveal it.
  ///
  /// Circuit-backed closeAwait rejects invocation from its owning
  /// Circuit context to avoid self-deadlock.
  @SpecRef({"9.1", "15.1"})
  @Test
  void closeAwait_fromOwningCircuit_throwsIllegalStateException() {

    final var observed = new AtomicBoolean();
    final var pipe =
      circuit.< Integer > pipe(_ -> {
        assertThrows(IllegalStateException.class, circuit::closeAwait);
        observed.set(true);
      });

    pipe.emit(1);
    circuit.await();

    assertTrue(observed.get());

  }

  /// A delivery before close establishes the subscription. Re-emitting after close distinguishes
  /// completed detachment from a close operation that only requests eventual removal.
  ///
  /// Subscriber#closeAwait removes its owned subscriptions before return.
  @SpecRef("9.1")
  @Test
  void closeAwait_subscriber_removesOwnedSubscriptions() {

    final var conduit = circuit.conduit(Integer.class);
    final var deliveries = new AtomicInteger();
    final var subscriber =
      circuit.< Integer > subscriber(
        cortex.name("resource.subscriber"),
        (_, registrar) -> registrar.register(_ -> deliveries.incrementAndGet())
      );
    final var subscription = conduit.subscribe(subscriber);
    final var pipe = conduit.get(cortex.name("resource.channel"));

    pipe.emit(1);
    circuit.await();
    assertEquals(1, deliveries.get());

    subscriber.closeAwait();
    pipe.emit(2);
    circuit.await();

    assertEquals(1, deliveries.get());
    assertDoesNotThrow(subscription::closeAwait);

  }

  /// The callback flips observable state, so returning from closeAwait proves that close-side effects
  /// are complete rather than merely scheduled.
  ///
  /// Subscription#closeAwait returns after its onClose callback completes.
  @SpecRef("9.1")
  @Test
  void closeAwait_subscription_waitsForOnClose() {

    final var conduit = circuit.conduit(Integer.class);
    final var closed = new AtomicBoolean();
    final var subscriber =
      circuit.< Integer > subscriber(
        cortex.name("resource.subscription"),
        (_, _) -> {
        }
      );
    final var subscription = conduit.subscribe(subscriber, _ -> closed.set(true));

    subscription.closeAwait();

    assertTrue(closed.get());
    assertDoesNotThrow(subscription::closeAwait);

  }

  /// The first tick establishes a live scheduler; the post-close observation window then detects any
  /// tick admitted after closeAwait claims completion.
  ///
  /// Ticker#closeAwait stops future scheduling before return.
  @SpecRef({"9.1", "11.4"})
  @Test
  void closeAwait_ticker_stopsFutureScheduling() throws InterruptedException {

    final var ticks = new AtomicInteger();
    final var first = new CountDownLatch(1);
    final var ticker =
      circuit.ticker(
        Duration.ofMillis(5L),
        circuit.pipe(_ -> {
          ticks.incrementAndGet();
          first.countDown();
        })
      );

    await(first, "the first ticker emission");

    ticker.closeAwait();
    circuit.await();
    final int afterClose = ticks.get();

    Thread.sleep(30L);
    circuit.await();

    assertEquals(afterClose, ticks.get());
    assertDoesNotThrow(ticker::closeAwait);

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
