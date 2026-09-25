// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static io.humainary.substrates.api.Substrates.Routing.*;
import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for hierarchical routing under topology change and across
/// a circuit boundary: SPEC §10.3, with §§5.1, 6.3, 7.3 and 15.4.
///
/// **This suite exercises an optional capability.** §10.3's hierarchy is
/// supplied at the provider's discretion; a projection that omits it is not
/// failing these cases, it is not offering what they test. Nothing here should
/// be read as a requirement on such a projection.
///
/// [ConduitContractTest.StemRouting] establishes leaf-first traversal, shared
/// ancestors, and that a chain is published complete. What it does not do is
/// move anything afterwards. A STEM route is the one place where a rebuild has
/// to reconcile more than a channel's own dispatch list — an ancestor is shared
/// between siblings, so a rebuild driven by one leaf's emission touches state
/// the other leaf depends on and is not covering. Each case below therefore
/// alternates which sibling is active across a subscription change.
///
/// Values carry their originating leaf, because an ancestor's callback receives
/// the *ancestor's* subject and cannot otherwise say which descendant an
/// observation came from.
/// @author William David Louth
/// @since 3.2

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.6.0/SPEC.md")
final class StemTopologyContractTest
  extends TestSupport {

  private final List< String > trace = new CopyOnWriteArrayList<>();
  private Cortex cortex;
  private Circuit source;
  private Circuit destination;

  /// A subscriber that records its discovery of each channel and every value
  /// reaching it, both tagged with the level it observed at.
  private Subscriber< String > observer(
    final String label
  ) {

    return
      source.subscriber(
        cortex.name(label),
        (subject, registrar) -> {

          final var level = subject.name().toString();

          trace.add(label + "/discover:" + level);

          registrar.register(
            (String value) -> trace.add(label + "@" + level + ":" + value)
          );

        }
      );

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

  /// Ancestor observations forwarded to another circuit keep the identity of
  /// the leaf they came from, and are delivered in the destination's context.
  ///
  /// The ancestor's own callback is handed the ancestor's subject, so source
  /// identity has to travel in the value. That is what makes the forwarding
  /// case worth having: a provider could deliver the right count of ancestor
  /// observations across the boundary while collapsing which descendant each
  /// belonged to.
  @SpecRef({"10.3", "5.1", "6.3"})
  @Test
  void stem_ancestorObservations_forwardedAbroad_keepLeafIdentityAndContext() {

    final var conduit = source.conduit(cortex.name("stem"), String.class, STEM);
    final var contexts = new CopyOnWriteArrayList< Current >();

    final Pipe< String > abroad =
      destination.pipe(
        (String value) -> {
          trace.add("abroad:" + value);
          contexts.add(cortex.current());
        }
      );

    final Subscriber< String > forwarder =
      source.subscriber(
        cortex.name("forwarder"),
        (subject, registrar) -> {

          if ("app".equals(subject.name().toString())) {
            registrar.register(abroad);
          }

        }
      );

    conduit.subscribe(forwarder);

    conduit.get(cortex.name("app.one")).emit("from.one");
    conduit.get(cortex.name("app.two")).emit("from.two");

    settle();

    assertEquals(
      List.of("abroad:from.one", "abroad:from.two"),
      trace,
      "each leaf's ancestor observation crosses once, still naming its origin"
    );

    assertEquals(
      List.of(destination.current(), destination.current()),
      contexts,
      "and both run in the circuit that owns the destination"
    );

  }

  /// Two sibling leaves sharing one ancestor, warmed in turn. Each
  /// subscription/channel pair is discovered exactly once, and an emission at
  /// either leaf is observed leaf-first and then at the shared ancestor.
  ///
  /// Discovery **interleaves** with delivery rather than preceding it: the leaf
  /// is discovered, the leaf delivers, then the ancestor is discovered and
  /// delivers. Pre-walking the chain to publish every level before any delivery
  /// is the obvious shape and the wrong one — it would run the ancestor's
  /// subscriber callback ahead of the leaf's delivery, which §7.3 orders the
  /// other way round. The trace is asserted whole here precisely to hold that
  /// down.
  @SpecRef({"10.3", "7.3"})
  @Test
  void stem_siblingLeaves_shareAnAncestorAndDiscoverEachPairOnce() {

    final var conduit = source.conduit(cortex.name("stem"), String.class, STEM);

    conduit.subscribe(observer("obs"));

    conduit.get(cortex.name("app.one")).emit("x");
    source.await();

    assertEquals(
      List.of(
        "obs/discover:app.one",
        "obs@app.one:x",
        "obs/discover:app",
        "obs@app:x"
      ),
      trace,
      "each level is discovered immediately before its own delivery, leaf first"
    );

    trace.clear();

    conduit.get(cortex.name("app.two")).emit("y");
    source.await();

    assertEquals(
      List.of(
        "obs/discover:app.two",
        "obs@app.two:y",
        "obs@app:y"
      ),
      trace,
      "the sibling discovers only itself; the shared ancestor is not rediscovered"
    );

  }


  /// A subscription change while one sibling is active, with the other left
  /// dormant across it.
  ///
  /// The shared ancestor is what makes this more than the flat rebuild case:
  /// the active leaf's emission drives a rebuild that touches an ancestor the
  /// dormant leaf also routes through. Afterwards the surviving subscriber must
  /// not rediscover any pair it already had — at either level, on either leaf —
  /// and the removed subscriber must be gone from both.
  @SpecRef({"10.3", "7.3", "7.6"})
  @Test
  void stem_subscriptionChurn_keepsSurvivingRoutesAndDropsRemovedOnes() {

    final var conduit = source.conduit(cortex.name("stem"), String.class, STEM);
    final var one = conduit.get(cortex.name("app.one"));
    final var two = conduit.get(cortex.name("app.two"));

    conduit.subscribe(observer("keep"));

    one.emit("warm");
    two.emit("warm");
    source.await();
    trace.clear();

    // A second subscriber arrives, sees only the leaf that is active while it
    // is subscribed, and leaves again.
    final var transient_ = conduit.subscribe(observer("gone"));

    one.emit("during");
    source.await();

    transient_.close();
    source.await();

    trace.clear();

    // Both leaves resume, including the one dormant throughout the change.
    one.emit("after");
    two.emit("after");
    source.await();

    assertEquals(
      List.of(
        "keep@app.one:after",
        "keep@app:after",
        "keep@app.two:after",
        "keep@app:after"
      ),
      trace,
      "the surviving subscriber rediscovers nothing and keeps both levels of both leaves, "
        + "and the removed one leaves no stale route behind"
    );

  }

  @AfterEach
  void tearDown() {

    source.closeAwait();
    destination.closeAwait();

  }

}
