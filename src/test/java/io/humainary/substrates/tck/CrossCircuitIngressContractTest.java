// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for the **admission class** of work that arrives from
/// another circuit: SPEC §§5.1, 5.3, 5.4.3 and 5.6.
///
/// [CrossCircuitContractTest] asks whose context a foreign delivery observes.
/// This suite asks a question that a correct answer there leaves open: **where
/// in the receiving circuit's queue does the arrival land?** A provider can run
/// an arrival in the right context and still admit it wrongly — onto the
/// destination's transit tier, where it takes priority over ingress that was
/// admitted before it, or concurrently with the destination's own work. Neither
/// mistake changes a context, and neither is visible to a test that admits one
/// thing at a time.
///
/// Every case here therefore establishes a **necessary** admission relation
/// with a gate and asserts only that. Where two producers are genuinely
/// concurrent, no order between them is asserted — only each one's own FIFO,
/// the exact multiplicity, and that the destination never ran two callbacks at
/// once. Asserting a global order over concurrent producers would assert a
/// schedule, not a contract.
///
/// **On §5.4.3 and the word "concurrently".** That clause leaves the relative
/// admission order of *concurrent* caller contexts unspecified, and the marker
/// and the arrival below reach the destination from two different contexts. They
/// are not concurrent. The marker's `emit` has returned before the forwarding is
/// even requested, and the forwarding circuit cannot admit work it has not yet
/// been given; `source.await()` closes the chain. The admission order is
/// therefore determined, and §5.4.3's first sentence — emissions MUST be
/// observed in strict admission order — is what these cases rest on. Do not
/// weaken them to membership on the strength of the "not specified" sentence:
/// it is about races, and this is not one.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class CrossCircuitIngressContractTest
  extends TestSupport {

  private Cortex cortex;
  /// The circuit that forwards.
  private Circuit source;
  /// The circuit that receives, and whose queue every case reasons about.
  private Circuit destination;

  /// The values one producer sent, in the order it sent them.
  private static List< String > expected(
    final String tag,
    final int count
  ) {

    final List< String > values = new ArrayList<>(count);

    for (int i = 0; i < count; i++) {
      values.add(tag + i);
    }

    return values;

  }

  /// Blocks the calling circuit worker until the gate opens, recording a
  /// distinguishable entry rather than hanging the fork if it never does.
  private static void hold(
    final CountDownLatch gate,
    final List< String > trace
  ) {

    try {

      if (!gate.await(10, TimeUnit.SECONDS)) {
        trace.add("GATE-TIMEOUT");
      }

    } catch (final InterruptedException exception) {

      Thread.currentThread().interrupt();
      trace.add("GATE-INTERRUPTED");

    }

  }

  /// One producer's arrivals, in the order the destination observed them.
  private static List< String > subsequence(
    final List< String > trace,
    final String tag
  ) {

    return
      trace.stream()
        .filter(value -> value.startsWith(tag))
        .toList();

  }

  /// A foreign arrival must not overtake ingress admitted to the destination
  /// before it.
  ///
  /// The gate is what makes the question answerable. It holds the destination
  /// inside its first callback, so the marker and the arrival are both pending
  /// when it resumes, and the order they are released in is the provider's
  /// answer rather than a race the test happened to win. The marker is admitted
  /// by the test thread and the arrival by the forwarding circuit's worker, but
  /// they are not concurrent: `source.await()` returns only once the forwarding
  /// has been performed, and it is called after the marker's `emit` has
  /// returned.
  ///
  /// A provider that admits an arrival to the destination's **transit** tier
  /// yields `[GATE, REMOTE, MARKER]` here, in the correct context, from a
  /// circuit that looks healthy in every other test.
  @SpecRef({"5.1", "5.3", "5.4.3", "5.6"})
  @Test
  void arrival_behindEarlierIngress_doesNotOvertakeIt()
    throws InterruptedException {

    final List< String > trace = new CopyOnWriteArrayList<>();
    final var entered = new CountDownLatch(1);
    final var gate = new CountDownLatch(1);

    final Pipe< String > held =
      destination.pipe(
        (String value) -> {
          trace.add(value);
          entered.countDown();
          hold(gate, trace);
        }
      );

    final Pipe< String > observer = destination.pipe(trace::add);

    held.emit("GATE");
    await(entered, "the destination to reach its gate");

    // Both of these are admitted while the destination is held, in this order.
    observer.emit("MARKER");
    source.pipe(observer).emit("REMOTE");
    source.await();

    // The forwarding circuit drained while the destination was held, so it did
    // not wait on the destination to accept; and nothing the destination owes
    // has run, so none of it ran on the forwarding worker.
    assertEquals(
      List.of("GATE"),
      trace,
      "the forwarding circuit completes its own work without running the destination's"
    );

    gate.countDown();
    destination.await();

    assertEquals(
      List.of("GATE", "MARKER", "REMOTE"),
      trace,
      "an arrival joins the destination's ingress, behind what was admitted first"
    );

  }

  /// Three producers — two foreign circuits and an external thread — driving
  /// one destination.
  ///
  /// No order is asserted between them, because none is owed. What is owed is
  /// asserted in full: every value arrives exactly once, each producer's own
  /// values arrive in the order it sent them, two registrations on the same
  /// channel observe one identical sequence, and the destination never runs two
  /// callbacks at once however many circuits are feeding it.
  @SpecRef({"5.1", "5.3", "5.6", "6.3"})
  @Test
  void concurrentProducers_intoOneDestination_keepEachFifoWithoutOverlap()
    throws InterruptedException, ExecutionException {

    final var third = cortex.circuit(cortex.name("third"));

    try (var tasks = TestTasks.fixed(1)) {

      final int each = 200;

      final List< String > primary = new ArrayList<>();
      final List< String > secondary = new ArrayList<>();

      final var inside = new AtomicBoolean();
      final var overlaps = new AtomicInteger();

      final var conduit = destination.conduit(String.class);

      conduit.subscribe(
        destination.subscriber(
          cortex.name("ingress.observer"),
          (_, registrar) -> {

            registrar.register(
              (String value) -> {
                if (!inside.compareAndSet(false, true)) {
                  overlaps.incrementAndGet();
                }
                primary.add(value);
              }
            );

            registrar.register(
              (String value) -> {
                secondary.add(value);
                inside.set(false);
              }
            );

          }
        )
      );

      final var channel = conduit.get(cortex.name("channel"));

      // Each forwarding circuit has one worker, so its admissions to the
      // destination are issued in the order it processed them.
      final var fromSource = source.pipe(channel);
      final var fromThird = third.pipe(channel);

      final var external = tasks.submit(() -> {
        for (int i = 0; i < each; i++) {
          channel.emit("E" + i);
        }
      });

      for (int i = 0; i < each; i++) {
        fromSource.emit("A" + i);
        fromThird.emit("C" + i);
      }

      get(external, "the external producer");

      source.await();
      third.await();
      destination.await();

      assertEquals(3 * each, primary.size(), "every value arrives exactly once");
      assertEquals(
        primary,
        secondary,
        "two registrations on one channel observe one sequence"
      );
      assertEquals(0, overlaps.get(), "the destination never runs two callbacks at once");

      assertEquals(expected("A", each), subsequence(primary, "A"), "the source circuit's FIFO");
      assertEquals(expected("C", each), subsequence(primary, "C"), "the third circuit's FIFO");
      assertEquals(expected("E", each), subsequence(primary, "E"), "the external caller's FIFO");

    } finally {

      third.closeAwait();

    }

  }

  /// The cascade a held ingress raises must complete before the destination
  /// returns to its queue — including to an arrival from another circuit.
  ///
  /// The gated callback raises a breadth-two, depth-two cascade on its own
  /// circuit the moment it resumes. Those emissions are transit and take
  /// priority; the marker and the arrival were admitted to ingress while the
  /// gate was shut and must wait for all four. Transit is FIFO rather than a
  /// stack, so the second level follows both of the first rather than
  /// interleaving with it.
  @SpecRef({"5.1", "5.3", "5.4.3"})
  @Test
  void heldIngress_localCascade_completesBeforeAnyPendingArrival()
    throws InterruptedException {

    final List< String > trace = new CopyOnWriteArrayList<>();
    final var entered = new CountDownLatch(1);
    final var gate = new CountDownLatch(1);

    final Pipe< String > observer = destination.pipe(trace::add);

    final Pipe< String > leaf =
      destination.pipe(trace::add);

    final Pipe< String > first =
      destination.pipe(
        (String value) -> {
          trace.add(value);
          leaf.emit(value + "a");
        }
      );

    final Pipe< String > second =
      destination.pipe(
        (String value) -> {
          trace.add(value);
          leaf.emit(value + "a");
        }
      );

    final Pipe< String > held =
      destination.pipe(
        (String value) -> {
          trace.add(value);
          entered.countDown();
          hold(gate, trace);
          first.emit("C1");
          second.emit("C2");
        }
      );

    held.emit("GATE");
    await(entered, "the destination to reach its gate");

    observer.emit("MARKER");
    source.pipe(observer).emit("REMOTE");
    source.await();

    gate.countDown();
    destination.await();

    assertEquals(
      List.of("GATE", "C1", "C2", "C1a", "C2a", "MARKER", "REMOTE"),
      trace,
      "transit drains to completion before ingress, whatever admitted the ingress"
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    source = cortex.circuit(cortex.name("source"));
    destination = cortex.circuit(cortex.name("destination"));

  }

  @AfterEach
  void tearDown() {

    source.closeAwait();
    destination.closeAwait();

  }

}
