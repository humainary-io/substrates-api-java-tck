// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for protocols that circulate between circuits: SPEC
/// §§5.3–5.5 and 14.
///
/// The suites around this one drive a topology and then drain it. That works
/// for a one-way shape, where awaiting the producer and then the consumer
/// happens to be a barrier, and it is not one in general: a circuit's `await`
/// is local, and a single pass of it over a cycle establishes nothing about a
/// return message still in flight. Draining A then B leaves a message A sent
/// while B was draining, and draining in the other order leaves the reply.
///
/// Every case here therefore ends in a **terminal acknowledgement** raised by
/// the protocol itself — a latch the last hop counts down — which the test
/// thread waits on before it drains anything. Draining afterwards is for the
/// side emissions each node makes along the way, not for the protocol.
///
/// Cycles are bounded by a lap count carried in the message. An unbounded
/// feedback loop has no termination to wait for and belongs in no conformance
/// suite; a bounded one is the shape that can be asserted hop for hop.
///
/// Where the topology branches, the branches are asserted separately. `B` and
/// `C` are given no order relative to each other at the join, because §16.3
/// fixes only the order they were enqueued in, and their workers decide the
/// rest. What is asserted is that each branch arrived once, in its own order.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class NetworkContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit alpha;
  private Circuit beta;

  /// Records the node's context inline, so a wrong one shows up in the trace
  /// rather than in a separate list the reader has to align with it. The
  /// marker stays short deliberately: the trace position already names the
  /// node, and a `Current`'s own rendering would bury the sequence being
  /// compared.
  private String assertingContext(
    final Circuit expected
  ) {

    return
      cortex.current()==expected.current()
        ? ""
        :"!wrong-context";

  }

  /// A bounded `A → B → C → A` protocol. The third circuit is what separates
  /// forwarding from relaying: B is neither the origin nor the destination of
  /// the message it passes on, so a provider that resolved a target's owner
  /// from the emitting circuit rather than from the target reaches the wrong
  /// one here in a way a two-circuit loop cannot show.
  @SpecRef({"5.3", "5.4", "5.5", "14"})
  @Test
  void feedback_threeCircuitRing_completesEveryLapOnce()
    throws InterruptedException {

    final var gamma = cortex.circuit(cortex.name("gamma"));

    try {

      final int laps = 2;

      final List< String > trace = new CopyOnWriteArrayList<>();
      final var done = new CountDownLatch(1);

      @SuppressWarnings("unchecked") final Pipe< Hop >[] toBeta = new Pipe[1];

      final Pipe< Hop > toAlpha =
        alpha.pipe(
          (Hop hop) -> {

            final var here = hop.via("A");

            trace.add("A:" + here.lap() + assertingContext(alpha));

            if (here.lap() < laps - 1) {
              toBeta[0].emit(here.next());
            } else {
              done.countDown();
            }

          }
        );

      final Pipe< Hop > toGamma =
        gamma.pipe(
          (Hop hop) -> {

            final var here = hop.via("C");

            trace.add("C:" + here.lap() + assertingContext(gamma));
            toAlpha.emit(here);

          }
        );

      toBeta[0] =
        beta.pipe(
          (Hop hop) -> {

            final var here = hop.via("B");

            trace.add("B:" + here.lap() + assertingContext(beta));
            toGamma.emit(here);

          }
        );

      toBeta[0].emit(new Hop(1, 0, ""));

      await(done, "the ring's terminal acknowledgement");

      alpha.await();
      beta.await();
      gamma.await();

      assertEquals(
        List.of("B:0", "C:0", "A:0", "B:1", "C:1", "A:1"),
        trace,
        "a relayed message reaches each circuit in turn, once per lap"
      );

    } finally {

      gamma.closeAwait();

    }

  }

  /// A bounded `A → B → A` protocol, run to a terminal acknowledgement.
  ///
  /// Each node records the context it ran in and makes a local side emission
  /// before forwarding, so the assertion covers both the protocol's own path
  /// and the work it raised locally along the way. The lap count is carried in
  /// the message rather than held by a node, so no circuit needs to agree with
  /// another about how far along the protocol is.
  @SpecRef({"5.3", "5.4", "5.5", "14"})
  @Test
  void feedback_twoCircuitRoundTrip_completesEveryLapOnce()
    throws InterruptedException {

    final int laps = 3;

    final List< String > trace = new CopyOnWriteArrayList<>();
    final List< String > aside = new CopyOnWriteArrayList<>();
    final var done = new CountDownLatch(1);
    final var finished = new AtomicReference< Hop >();

    @SuppressWarnings("unchecked") final Pipe< Hop >[] toBeta = new Pipe[1];

    final Pipe< String > sideAtAlpha = alpha.pipe(aside::add);

    final Pipe< Hop > toAlpha =
      alpha.pipe(
        (Hop hop) -> {

          final var here = hop.via("A");

          trace.add("A:" + here.lap() + assertingContext(alpha));
          sideAtAlpha.emit("side-A:" + here.lap());

          if (here.lap() < laps - 1) {
            toBeta[0].emit(here.next());
          } else {
            finished.set(here);
            done.countDown();
          }

        }
      );

    toBeta[0] =
      beta.pipe(
        (Hop hop) -> {

          final var here = hop.via("B");

          trace.add("B:" + here.lap() + assertingContext(beta));
          toAlpha.emit(here);

        }
      );

    toBeta[0].emit(new Hop(1, 0, ""));

    await(done, "the protocol's terminal acknowledgement");

    // Only now, and only for the side emissions the protocol raised.
    alpha.await();
    beta.await();

    assertEquals(
      List.of("B:0", "A:0", "B:1", "A:1", "B:2", "A:2"),
      trace,
      "every lap visits every node exactly once, in order"
    );

    assertEquals(
      List.of("side-A:0", "side-A:1", "side-A:2"),
      aside,
      "and the local work each hop raised completed too"
    );

    assertEquals(
      "B>A>B>A>B>A",
      finished.get().path(),
      "the message carries the whole route it travelled"
    );

  }

  /// An `A → {B,C} → D` diamond, joined at D by correlation identifier.
  ///
  /// The join is the completion protocol: D counts an identifier done when
  /// both of its branches have arrived, and the test waits for every
  /// identifier rather than for any circuit. What is asserted is that each
  /// branch delivered each identifier exactly once and in the order A sent
  /// them. What is deliberately **not** asserted is whether B or C reached D
  /// first — for a given identifier or across them — since the fan-out fixes
  /// only the order the two were enqueued in.
  @SpecRef({"5.3", "5.4", "5.5", "14", "16.3"})
  @Test
  void merge_diamondJoinedByCorrelation_deliversEachBranchOnce()
    throws InterruptedException {

    final var gamma = cortex.circuit(cortex.name("gamma"));
    final var delta = cortex.circuit(cortex.name("delta"));

    try {

      final int count = 5;

      final Map< Integer, List< String > > joined = new ConcurrentHashMap<>();
      final List< Integer > throughBeta = new CopyOnWriteArrayList<>();
      final List< Integer > throughGamma = new CopyOnWriteArrayList<>();
      final var complete = new CountDownLatch(count);

      final Pipe< Hop > atDelta =
        delta.pipe(
          (Hop hop) -> {

            final var branches =
              joined.computeIfAbsent(hop.id(), _ -> new ArrayList<>());

            branches.add(hop.path());

            if (branches.size()==2) {
              complete.countDown();
            }

          }
        );

      final Pipe< Hop > atBeta =
        beta.pipe(
          (Hop hop) -> {
            throughBeta.add(hop.id());
            atDelta.emit(hop.via("B"));
          }
        );

      final Pipe< Hop > atGamma =
        gamma.pipe(
          (Hop hop) -> {
            throughGamma.add(hop.id());
            atDelta.emit(hop.via("C"));
          }
        );

      final Pipe< Hop > fanout = alpha.pipe(List.of(atBeta, atGamma));

      for (int id = 0; id < count; id++) {
        fanout.emit(new Hop(id, 0, "A"));
      }

      await(complete, "every correlation identifier to join at D");

      alpha.await();
      beta.await();
      gamma.await();
      delta.await();

      final var order = List.of(0, 1, 2, 3, 4);

      assertEquals(order, throughBeta, "one branch sees every identifier, in order");
      assertEquals(order, throughGamma, "and so does the other, independently");

      assertEquals(count, joined.size(), "every identifier joined, and no others arrived");

      for (int id = 0; id < count; id++) {

        assertEquals(
          Set.of("A>B", "A>C"),
          Set.copyOf(joined.get(id)),
          "identifier " + id + " arrived once by each branch"
        );

        assertEquals(
          2,
          joined.get(id).size(),
          "identifier " + id + " arrived exactly twice"
        );

      }

    } finally {

      delta.closeAwait();
      gamma.closeAwait();

    }

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    alpha = cortex.circuit(cortex.name("alpha"));
    beta = cortex.circuit(cortex.name("beta"));

  }

  @AfterEach
  void tearDown() {

    alpha.closeAwait();
    beta.closeAwait();

  }

  /// A message that records where it has been. Immutable, so a node's view of
  /// it cannot be changed by a later hop on another circuit.
  private record Hop(
    int id,
    int lap,
    String path
  ) {

    /// The same message, beginning its next lap.
    Hop next() {

      return
        new Hop(id, lap + 1, path);

    }

    /// The same message, one node further along.
    Hop via(
      final String node
    ) {

      return
        new Hop(
          id,
          lap,
          path.isEmpty() ? node:path + ">" + node
        );

    }

  }

}
