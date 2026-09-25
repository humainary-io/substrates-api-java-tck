// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static io.humainary.substrates.api.Substrates.Routing.*;
import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for what happens to work **already accepted** when the
/// code that accepted it then fails: SPEC §§6.2, 7.3 and 15.4.
///
/// [ExceptionIsolationContractTest] establishes that a throwing callback does
/// not take the worker down and does not deny its siblings. That is the first
/// half of §15.4 and the easier half. The second is what becomes of the work
/// the failing callback had already raised before it threw, and of the routes
/// around it — a question that only has content once there is a graph, because
/// in a single-receptor topology there is nothing accepted and nothing
/// adjacent.
///
/// The distinction each case turns on is between **raising** work and
/// **completing** the callback that raised it. A provider that treats a throw
/// as invalidating the callback's whole turn has a coherent story and a wrong
/// one: an emission accepted onto a queue is accepted, and §15.4's isolation
/// is of the failure, not of everything the failing frame touched.
///
/// Failures here are deliberate and are marked as such in the traces, so a
/// case that stopped provoking its failure would be visible rather than
/// silently passing.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class FailureContinuationContractTest
  extends TestSupport {

  private final List< String > trace = new CopyOnWriteArrayList<>();
  private Cortex cortex;
  private Circuit local;
  private Circuit remote;

  /// A discovery callback that throws while the route is **cold** leaves the
  /// other subscribers' routes on that channel intact, and does not prevent
  /// the channel from serving a subscription added afterwards.
  ///
  /// Cold discovery is the moment a provider has the least state to fall back
  /// on: the dispatch list is being built when the failure arrives.
  @SpecRef({"7.3", "15.4"})
  @Test
  void discoveryFailure_whileCold_leavesSiblingSubscribersAndLaterOnesIntact() {

    final var conduit = local.conduit(String.class);

    conduit.subscribe(
      local.subscriber(
        cortex.name("throwing"),
        (_, _) -> {
          trace.add("deliberate-failure");
          throw new IllegalStateException("deliberate");
        }
      )
    );

    conduit.subscribe(
      local.subscriber(
        cortex.name("healthy"),
        (_, registrar) -> registrar.register((String value) -> trace.add("healthy:" + value))
      )
    );

    final var channel = conduit.get(cortex.name("channel"));

    channel.emit("x");          // cold: both callbacks run here
    local.await();

    assertEquals(
      List.of("deliberate-failure", "healthy:x"),
      trace,
      "a failed discovery does not suppress a sibling subscriber's discovery or delivery"
    );

    trace.clear();

    channel.emit("y");          // warm
    local.await();

    assertEquals(
      List.of("healthy:y"),
      trace,
      "and the failed callback is not retried once the route is warm"
    );

  }

  /// A throwing operator drops the emission for its own chain only. The
  /// sibling attachment on the same channel is a separate materialization and
  /// keeps both its delivery and its own state.
  ///
  /// The stateful operator is the part worth asserting: a provider that
  /// unwound the failing chain might plausibly unwind its state too, and
  /// nothing in a stateless case would show it.
  @SpecRef({"6.2", "15.4"})
  @Test
  void operatorFailure_dropsOnlyItsOwnChain_andKeepsSiblingState() {

    final var conduit = local.conduit(String.class);

    conduit.subscribe(
      local.subscriber(
        cortex.name("chains"),
        (_, registrar) -> {

          registrar.register(
            cortex.fiber(String.class)
              .peek(value -> {
                if (value.startsWith("bad")) {
                  trace.add("deliberate-failure:" + value);
                  throw new IllegalStateException("deliberate");
                }
              })
              .pipe(local.pipe((String value) -> trace.add("failing-chain:" + value)))
          );

          registrar.register(
            cortex.fiber(String.class)
              .diff()
              .pipe(local.pipe((String value) -> trace.add("sibling-chain:" + value)))
          );

        }
      )
    );

    final var channel = conduit.get(cortex.name("channel"));

    channel.emit("ok");
    channel.emit("bad");
    channel.emit("bad");        // a duplicate for the sibling's diff
    channel.emit("ok");         // a change for it

    local.await();

    assertEquals(
      List.of(
        "failing-chain:ok",         // both chains pass the first value, in registration order
        "sibling-chain:ok",
        "deliberate-failure:bad",   // the failing chain drops this one, and only this one
        "sibling-chain:bad",
        "deliberate-failure:bad",   // the sibling's diff drops the repeat, the other chain throws
        "failing-chain:ok",         // the failing chain has no diff, so it passes ok again
        "sibling-chain:ok"          // and the sibling's diff sees a change from bad
      ),
      trace,
      "the failing chain drops its own emissions; the sibling delivers and keeps deduplicating"
    );

  }

  /// A receptor raises a local child and a remote child, then throws. Both
  /// children were accepted before the throw and both MUST still run.
  ///
  /// This is the case the isolation suite cannot reach: there, a throwing
  /// receptor has raised nothing, so "the failure is isolated" and "the
  /// failure discarded accepted work" are indistinguishable. Here they are
  /// not — one of the children is on another circuit, which the failing
  /// circuit has no authority to cancel in any case.
  @SpecRef({"5.3", "6.3", "15.4"})
  @Test
  void receptor_raisesThenThrows_stillRunsBothAcceptedChildren() {

    final Pipe< String > localChild =
      local.pipe((String value) -> trace.add("local-child:" + value));

    final Pipe< String > remoteChild =
      remote.pipe((String value) -> trace.add("remote-child:" + value));

    final var conduit = local.conduit(String.class);

    conduit.subscribe(
      local.subscriber(
        cortex.name("raiser"),
        (_, registrar) -> {

          registrar.register(
            (String value) -> {
              localChild.emit(value);
              remoteChild.emit(value);
              trace.add("deliberate-failure");
              throw new IllegalStateException("deliberate");
            }
          );

          registrar.register(
            (String value) -> trace.add("sibling:" + value)
          );

        }
      )
    );

    final var channel = conduit.get(cortex.name("channel"));

    channel.emit("x");
    settle();

    assertTrue(trace.contains("deliberate-failure"), "the failure must actually have been raised");
    assertTrue(trace.contains("sibling:x"), "a sibling registration still receives");
    assertTrue(trace.contains("local-child:x"), "local work accepted before the throw still runs");
    assertTrue(trace.contains("remote-child:x"), "and so does work accepted on another circuit");

    // Both circuits remain usable for later work and later topology changes.
    trace.clear();

    conduit.subscribe(
      local.subscriber(
        cortex.name("added.after"),
        (_, registrar) -> registrar.register((String value) -> trace.add("added:" + value))
      )
    );

    channel.emit("y");
    settle();

    assertTrue(trace.contains("added:y"), "a subscription added after the failure takes effect");
    assertTrue(trace.contains("sibling:y"), "and the surviving registrations keep delivering");

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    local = cortex.circuit(cortex.name("local"));
    remote = cortex.circuit(cortex.name("remote"));

  }

  private void settle() {

    local.await();
    remote.await();

  }

  /// A throwing leaf receptor under STEM routing does not suppress its
  /// siblings on the leaf, and does not truncate the ancestor chain behind it.
  ///
  /// Hierarchical routing is leaf-first, so a failure at the leaf sits ahead
  /// of every ancestor delivery the same emission owes. A provider that let
  /// the throw unwind the traversal would lose all of them, and would look
  /// correct in any flat topology.
  @SpecRef({"10.3", "15.4"})
  @Test
  void stemLeafFailure_suppressesNeitherSiblingsNorAncestors() {

    final var conduit = local.conduit(cortex.name("stem"), String.class, STEM);

    conduit.subscribe(
      local.subscriber(
        cortex.name("hierarchy"),
        (subject, registrar) -> {

          final var name = subject.name().toString();

          if ("app.leaf".equals(name)) {

            registrar.register(
              (String _) -> {
                trace.add("deliberate-failure");
                throw new IllegalStateException("deliberate");
              }
            );

            registrar.register((String value) -> trace.add("leaf-sibling:" + value));

          } else if ("app".equals(name)) {

            registrar.register((String value) -> trace.add("ancestor:" + value));

          }

        }
      )
    );

    conduit.get(cortex.name("app.leaf")).emit("x");
    local.await();

    // The two leaf registrations are receptors, whose relative order §6.3 does
    // not fix; what §10.3 fixes is that the leaf is reached before the ancestor.
    // The assertion is split along exactly that line.
    assertEquals(3, trace.size(), "every observation the emission owes was made");

    assertEquals(
      Set.of("deliberate-failure", "leaf-sibling:x"),
      Set.copyOf(trace.subList(0, 2)),
      "a failed leaf receptor does not stop its sibling on the same leaf"
    );

    assertEquals(
      "ancestor:x",
      trace.get(2),
      "and leaf-first still reaches the ancestor behind it"
    );

  }

  @AfterEach
  void tearDown() {

    local.closeAwait();
    remote.closeAwait();

  }

}
