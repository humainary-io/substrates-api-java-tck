// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for subscription changes raised from **inside** the
/// circuit context: SPEC §§5.3, 7.5 and 7.6.1.
///
/// Until 3.2 the specification left these undecided, and not by oversight in
/// one place. §5.3 enumerated subscription registration and close under ingress
/// and enumerated nothing under transit, so a call made from a callback matched
/// the second definition's opening sentence and the first definition's list.
/// §7.6.1 then defined the visibility window over ingress-admission positions —
/// a coordinate work raised inside a cascade does not have. The Java projection
/// stated outright that a close "enqueues a close job on the circuit's ingress
/// queue", while the provider admitted it to transit. There was no reading under
/// which all three agreed.
///
/// The resolution is that a queued operation's class follows the **context that
/// submitted it** rather than the kind of operation it is, and that visibility
/// is defined over the circuit's logical processing order rather than over
/// admission position. These three cases are what that decides, and each one
/// separates the two readings that were previously both defensible.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class CallbackTopologyContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;

  /// Closing a subscription ends future **selection**; it does not retract
  /// deliveries the current emission has already selected.
  ///
  /// The subscription registers two receptors on one channel. The first closes
  /// the subscription while the emission is still being dispatched. Its sibling
  /// must still run: a close cannot reach back into a dispatch already under
  /// way, only into the selections that follow it.
  ///
  /// The second emission is the other half of the same rule — by then the close
  /// has executed, so nothing is selected at all.
  @SpecRef({"7.5", "7.6.1"})
  @Test
  void selfCloseFromReceptor_keepsAlreadySelectedSiblingDeliveries() {

    final var conduit = circuit.conduit(Integer.class);
    final var channel = conduit.get(cortex.name("channel"));

    final List< String > seen = new CopyOnWriteArrayList<>();
    final var handle = new AtomicReference< Subscription >();

    handle.set(
      conduit.subscribe(
        circuit.subscriber(
          cortex.name("self-closing"),
          (_, registrar) -> {

            registrar.register(
              (Integer value) -> {
                seen.add("first:" + value);
                handle.get().close();
              }
            );

            registrar.register(
              (Integer value) -> seen.add("second:" + value)
            );

          }
        )
      )
    );

    channel.emit(1);        // selected before the close executes
    channel.emit(2);        // selected after it, so not at all

    circuit.await();

    // Both registrations are receptors, and §6.3 gives receptors the channel
    // visibility, temporal validity, confinement and failure isolation of pipe
    // registrations but *not* their registration order — the Java projection
    // says so outright. So membership is asserted and sequence is not. Do not
    // "tighten" this to an ordered list: it would fail a conformant provider
    // that invokes the two the other way round.
    assertEquals(2, seen.size(), "one emission selected both registrations, and only one did");

    assertEquals(
      Set.of("first:1", "second:1"),
      Set.copyOf(seen),
      "a close ends later selection without retracting the dispatch it ran inside"
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    circuit = cortex.circuit(cortex.name("callback.topology"));

  }

  /// A subscribe issued from within the circuit context takes effect inside the
  /// current cascade, so the subscription sees ingress admitted **behind** the
  /// emission that created it.
  ///
  /// This is the discriminator between the two classifications. The caller
  /// admits `X` and then `Y`, so ingress holds them in that order. Processing
  /// `X` subscribes. Under the transit classification the registration executes
  /// during `X`'s cascade, ahead of `Y`, and the subscription receives `Y`.
  /// Under the ingress classification the registration is admitted behind `Y`,
  /// and the subscription receives nothing at all.
  @SpecRef({"5.3", "7.6.1"})
  @Test
  void subscribeFromCallback_isTransit_andSeesIngressBehindIt() {

    final var conduit = circuit.conduit(Integer.class);
    final var channel = conduit.get(cortex.name("channel"));
    final List< Integer > late = new CopyOnWriteArrayList<>();

    final Pipe< Integer > trigger =
      circuit.pipe(
        (Integer _) ->
          conduit.subscribe(
            circuit.subscriber(
              cortex.name("late"),
              (_, registrar) -> registrar.register(late::add)
            )
          )
      );

    trigger.emit(0);        // X — subscribes from within the circuit context
    channel.emit(7);        // Y — admitted to ingress behind X

    circuit.await();

    assertEquals(
      List.of(7),
      late,
      "a subscription raised inside a cascade is effective before the ingress behind it"
    );

  }

  @AfterEach
  void tearDown() {

    circuit.closeAwait();

  }

  /// A topology change inside a cascade takes effect at **its own** position,
  /// not at the position of the ingress item the cascade descends from.
  ///
  /// The callback emits `E1`, subscribes, then emits `E2`, leaving transit
  /// holding `[E1, register(S), E2]`. Processing `E1` raises `E1a`, which joins
  /// the **back** of transit rather than running immediately, giving
  /// `[register(S), E2, E1a]`.
  ///
  /// So `S` misses `E1` and receives both `E2` and `E1a` — including `E1a`,
  /// which is caused by the one emission `S` never saw. That is the case which
  /// rules out defining visibility by inheriting the cascade's originating
  /// admission position: under inheritance all four operations share one
  /// position and the window orders none of them. Causal ancestry does not
  /// decide visibility; the subscriptions effective when a channel processes an
  /// emission do.
  @SpecRef({"5.3", "7.6.1"})
  @Test
  void topologyChangeInsideCascade_takesEffectAtItsOwnPosition() {

    final var conduit = circuit.conduit(String.class);
    final var alpha = conduit.get(cortex.name("alpha"));
    final var beta = conduit.get(cortex.name("beta"));

    final List< String > late = new CopyOnWriteArrayList<>();

    // Makes alpha's emission raise a further one on beta, one hop later.
    conduit.subscribe(
      circuit.subscriber(
        cortex.name("cascader"),
        (subject, registrar) -> {

          if (cortex.name("alpha").equals(subject.name())) {
            registrar.register((String value) -> beta.emit(value + "a"));
          }

        }
      )
    );

    final Pipe< String > trigger =
      circuit.pipe(
        (String _) -> {

          alpha.emit("E1");

          conduit.subscribe(
            circuit.subscriber(
              cortex.name("late"),
              (_, registrar) -> registrar.register(late::add)
            )
          );

          beta.emit("E2");

        }
      );

    trigger.emit("go");
    circuit.await();

    assertEquals(
      List.of("E2", "E1a"),
      late,
      "the subscription is effective from its own position, and E1a is behind E2"
    );

  }

}
