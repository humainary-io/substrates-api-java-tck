// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Tests verifying SPEC §15.4 External Callback Isolation invariants.
///
/// The contract pins four safety invariants for external (client-supplied)
/// callbacks — receptors, flow operator functions, subscriber callbacks, and
/// onClose handlers:
///
/// 1. **Isolation** — uncaught exceptions MUST NOT propagate to the circuit
///    dispatch loop.
/// 2. **Liveness** — dispatch MUST continue to sibling receptors on the same
///    channel and to subsequent queued operations.
/// 3. **No silent success** — a failing Flow operator MUST NOT cause the stage
///    chain to behave as if the operator had returned a valid value.
/// 4. **Observability** — failure reporting is implementation-defined; these tests assert only
///    the isolation and liveness guarantees that are portable across providers.
///
/// These tests exercise all four invariants against the configured provider.
///
/// A class-level timeout guards against the most concerning failure mode: a
/// throwing callback wedging the circuit's dispatch loop so that `await()`
/// never returns.
/// @author William David Louth
/// @since 1.0

@Timeout(
  value = 10,
  unit = TimeUnit.SECONDS,
  threadMode = Timeout.ThreadMode.SEPARATE_THREAD
)
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.2/SPEC.md")
final class ExceptionIsolationContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;

  /// Validates that an exception from a subscriber callback during channel
  /// discovery does not affect other subscribers on the same source.
  ///
  /// Exercises §15.4 invariants 1 and 2 for subscriber-callback failures.
  /// A failing subscriber callback does not block siblings.
  @SpecRef("15.4")
  @Test
  void discovery_subscriberCallbackThrows_preservesOtherSubscribers() {

    final var conduit =
      circuit.conduit(
        cortex.name("iso.sub-callback"),
        Long.class
      );

    // This subscriber's callback throws during the lazy rebuild that happens
    // when the first emission reaches a new channel.

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("iso.throwing-callback"),
        (_, _) -> {
          throw new RuntimeException("callback boom");
        }
      )
    );

    final List< Long > captured = new ArrayList<>();

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("iso.capturing"),
        (_, registrar) ->
          registrar.register(captured::add)
      )
    );

    final var pipe = conduit.get(cortex.name("channel"));

    pipe.emit(1L);
    pipe.emit(2L);
    pipe.emit(3L);

    circuit.await();

    assertEquals(
      List.of(1L, 2L, 3L),
      captured,
      "a throwing subscriber callback MUST NOT block discovery or dispatch " +
        "for other subscribers on the same source (SPEC §15.4 invariants 1, 2)"
    );

  }

  /// Validates that an exception from one receptor does not prevent a sibling
  /// receptor (registered via a separate subscriber on the same conduit) from
  /// receiving the same emission.
  ///
  /// Exercises §15.4 invariants 1 (isolation) and 2 (liveness across sibling
  /// receptors on the same channel).
  /// A failing receptor does not block sibling delivery.
  @SpecRef("15.4")
  @Test
  void dispatch_receptorThrows_preservesSiblingDelivery() {

    final var conduit =
      circuit.conduit(
        cortex.name("iso.sibling-sub"),
        Long.class
      );

    // Subscribe the throwing receptor FIRST so that registration order puts
    // it ahead of the capturing receptor in the dispatch loop.

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("iso.throwing"),
        (_, registrar) ->
          registrar.register(
            _ -> {
              throw new RuntimeException("boom");
            }
          )
      )
    );

    final List< Long > captured = new ArrayList<>();

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("iso.capturing"),
        (_, registrar) ->
          registrar.register(
            captured::add
          )
      )
    );

    final var pipe = conduit.get(cortex.name("channel"));

    pipe.emit(1L);
    pipe.emit(2L);
    pipe.emit(3L);

    circuit.await();

    assertEquals(
      List.of(1L, 2L, 3L),
      captured,
      "sibling receptor MUST still receive every emission even when an earlier " +
        "sibling throws on each dispatch (SPEC §15.4 invariant 2)"
    );

  }

  /// Validates that a persistently throwing receptor does not wedge the
  /// circuit — subsequent emissions continue to be dispatched and other
  /// receptors keep functioning.
  ///
  /// Exercises §15.4 invariants 1 (isolation) and 2 (liveness of the queue
  /// across multiple throws on multiple emissions).
  /// A failing receptor does not block later emissions.
  @SpecRef("15.4")
  @Test
  void dispatch_receptorThrows_preservesSubsequentEmissions() {

    final var conduit =
      circuit.conduit(
        cortex.name("iso.subsequent"),
        Long.class
      );

    final var throwCount = new AtomicInteger();

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("iso.throwing"),
        (_, registrar) ->
          registrar.register(
            _ -> {
              throwCount.incrementAndGet();
              throw new RuntimeException("boom");
            }
          )
      )
    );

    final var receiveCount = new AtomicInteger();

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("iso.counting"),
        (_, registrar) ->
          registrar.register(
            _ -> receiveCount.incrementAndGet()
          )
      )
    );

    final var pipe = conduit.get(cortex.name("channel"));

    final int emissions = 100;

    for (int i = 0; i < emissions; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    assertEquals(
      emissions,
      throwCount.get(),
      "throwing receptor MUST be invoked for every emission; earlier throws " +
        "must not short-circuit subsequent dispatch"
    );

    assertEquals(
      emissions,
      receiveCount.get(),
      "sibling receptor MUST receive every emission despite persistent throws " +
        "from the throwing receptor"
    );

  }

  /// Validates end-to-end liveness of the dispatch engine under a throwing
  /// receptor: after many failed dispatches, a newly registered receptor
  /// still receives emissions and the circuit still closes cleanly.
  ///
  /// This is the headline liveness test — if isolation is broken, the circuit's
  /// single dispatch thread terminates and await hangs (or close fails).
  /// Repeated callback failures do not stop circuit progress.
  @SpecRef("15.4")
  @Test
  void dispatch_repeatedCallbackFailures_preservesCircuitLiveness() {

    final var conduit =
      circuit.conduit(
        cortex.name("iso.liveness"),
        Long.class
      );

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("iso.always-throws"),
        (_, registrar) ->
          registrar.register(
            _ -> {
              throw new RuntimeException("boom");
            }
          )
      )
    );

    final var pipe = conduit.get(cortex.name("channel"));

    for (int i = 0; i < 1_000; i++) {
      pipe.emit((long) i);
    }

    circuit.await();

    // After 1000 thrown exceptions, attach a fresh receptor and verify the
    // engine still dispatches to new subscriptions.

    final var tailCaptured = new ArrayList< Long >();

    conduit.subscribe(
      circuit.subscriber(
        cortex.name("iso.late-joiner"),
        (_, registrar) ->
          registrar.register(tailCaptured::add)
      )
    );

    pipe.emit(10_000L);
    pipe.emit(10_001L);

    circuit.await();

    assertEquals(
      List.of(10_000L, 10_001L),
      tailCaptured,
      "after repeated isolated throws, the circuit MUST still accept new " +
        "subscriptions and dispatch emissions to them"
    );

  }

  /// Validates that when a Flow operator function throws, downstream operators
  /// in the same chain do not see that emission, while prior and subsequent
  /// emissions flow through normally.
  ///
  /// Exercises §15.4 invariant 3 (no silent success): a failing operator is
  /// treated as a dropped emission for that receptor chain, not as a successful
  /// pass-through.
  /// A failing Flow operator drops that receptor-chain
  /// emission without affecting the Circuit dispatch loop.
  @SpecRef({"6.2", "15.4"})
  @Test
  void flow_operatorThrows_dropsFailedChainEmission() {

    final List< Long > captured = new ArrayList<>();

    final var flow =
      cortex.fiber(Long.class)
        .peek(
          value -> {
            if (value==2L) {
              throw new RuntimeException("boom on 2");
            }
          }
        )
        .peek(captured::add);

    final var conduit =
      circuit.conduit(
        cortex.name("iso.flow-chain"),
        Long.class
      );

    final var pool = conduit.pool(flow::pipe);

    final Pipe< Long > pipe = pool.get(cortex.name("channel"));

    pipe.emit(1L);
    pipe.emit(2L);
    pipe.emit(3L);

    circuit.await();

    assertEquals(
      List.of(1L, 3L),
      captured,
      "downstream peek MUST NOT observe the value for which an earlier stage " +
        "threw (SPEC §15.4 invariant 3), but MUST observe values for which " +
        "earlier stages succeeded"
    );

  }

  /// Validates that an exception from one subscription's onClose callback
  /// does not prevent other subscriptions' onClose callbacks from firing and
  /// does not prevent the circuit from closing cleanly.
  ///
  /// Exercises §15.4 invariants 1 and 2 for onClose failures.
  /// A failing onClose callback does not block siblings.
  @SpecRef("15.4")
  @Test
  void onClose_callbackThrows_preservesSiblingCallbacks() {

    final var conduit =
      circuit.conduit(
        cortex.name("iso.onclose"),
        Long.class
      );

    final var siblingCloseFired = new AtomicInteger();

    final var throwingSub =
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("iso.throwing-onclose-sub"),
          (_, registrar) ->
            registrar.register(_ -> {
            })
        ),
        _ -> {
          throw new RuntimeException("onClose boom");
        }
      );

    final var siblingSub =
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("iso.sibling-onclose-sub"),
          (_, registrar) ->
            registrar.register(_ -> {
            })
        ),
        _ -> siblingCloseFired.incrementAndGet()
      );

    // Force lazy rebuild so the subscriptions are materially installed before
    // close. Otherwise, the subscriber callbacks may never run at all and the
    // test degenerates to verifying close() of an unrealized subscription.

    conduit.get(cortex.name("channel")).emit(1L);

    circuit.await();

    // Close both subscriptions. The throwing onClose must not prevent the
    // sibling onClose from firing or wedge the circuit.

    throwingSub.close();
    siblingSub.close();

    circuit.await();

    assertEquals(
      1,
      siblingCloseFired.get(),
      "sibling onClose callback MUST fire exactly once even when a peer " +
        "subscription's onClose throws (SPEC §15.4 invariants 1, 2)"
    );

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
