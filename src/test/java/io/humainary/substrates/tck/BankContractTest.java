// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §10.4 Bank canonical lookup, concurrency, ownership, and lifecycle.

@SuppressWarnings("TryFinallyCanBeTryWithResources")
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.1.2/SPEC.md")
final class BankContractTest
  extends TestSupport {

  private Cortex cortex;

  /// A routed Bank applies hierarchical routing to each
  /// materialized Conduit.
  @SpecRef({"10.3", "10.4", "16.3"})
  @Test
  void bank_hierarchicalRouting_materializesRoutedConduits() {

    final var circuit = cortex.circuit();
    final var bank = circuit.bank(Integer.class, Routing.STEM);

    try {

      final var conduit = bank.get(cortex.name("routed"));
      final List< String > deliveries = new ArrayList<>();

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("observer"),
          (subject, registrar) -> {

            final var path = subject.name().toString();

            if (path.equals("app") || path.equals("app.leaf")) {
              registrar.register(_ -> deliveries.add(path));
            }

          }
        )
      );

      conduit.get(cortex.name("app.leaf")).emit(1);
      circuit.await();

      assertEquals(List.of("app.leaf", "app"), deliveries);

    } finally {

      bank.close();
      circuit.close();

    }

  }

  /// The no-argument form uses default per-pipe routing: an emission reaches the
  /// emitting name alone, never its ancestors.
  @SpecRef("10.4")
  @Test
  void bank_noArgument_usesDefaultRouting() {

    final var circuit = cortex.circuit();

    final Bank< Conduit< Integer > > bank =
      circuit.bank();

    try {

      final var conduit = bank.get(cortex.name("routed"));
      final List< String > deliveries = new ArrayList<>();

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("observer"),
          (subject, registrar) -> {

            final var path = subject.name().toString();

            if (path.equals("app") || path.equals("app.leaf")) {
              registrar.register(_ -> deliveries.add(path));
            }

          }
        )
      );

      conduit.get(cortex.name("app.leaf")).emit(1);
      circuit.await();

      assertEquals(
        List.of("app.leaf"),
        deliveries,
        "the default form routed to ancestors, so it did not select per-pipe routing"
      );

    } finally {

      bank.close();
      circuit.close();

    }

  }

  /// A null routing is rejected by the witness-free form.
  @SuppressWarnings("DataFlowIssue")
  @SpecRef({"10.4", "15.2"})
  @Test
  void bank_nullRouting_throwsNullPointerException() {

    final var circuit = cortex.circuit();

    try {

      assertThrows(
        NullPointerException.class,
        () -> circuit.bank((Routing) null)
      );

    } finally {

      circuit.close();

    }

  }

  /// The witness-free routing form applies the selected routing to every materialized
  /// Conduit, matching the class-witness form.
  @SpecRef({"10.3", "10.4"})
  @Test
  void bank_witnessFreeRouting_matchesWitnessForm() {

    final var circuit = cortex.circuit();

    final Bank< Conduit< Integer > > bank =
      circuit.bank(Routing.STEM);

    try {

      final var conduit = bank.get(cortex.name("routed"));
      final List< String > deliveries = new ArrayList<>();

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("observer"),
          (subject, registrar) -> {

            final var path = subject.name().toString();

            if (path.equals("app") || path.equals("app.leaf")) {
              registrar.register(_ -> deliveries.add(path));
            }

          }
        )
      );

      conduit.get(cortex.name("app.leaf")).emit(1);
      circuit.await();

      assertEquals(
        List.of("app.leaf", "app"),
        deliveries,
        "the witness-free routing form did not apply hierarchical routing"
      );

    } finally {

      bank.close();
      circuit.close();

    }

  }

  /// A conduit the bank handed out must not outlive the bank, including one handed out by a lookup
  /// that raced the close. Sixteen threads take distinct names while a seventeenth closes; a lookup
  /// may legitimately be refused, but whatever it returned is the bank's to own. Every conduit
  /// collected — including one materialized before the race, so the assertion is never vacuous — is
  /// probed after the close has drained, and an open one means the bank leaked it.
  ///
  /// Bank close closes every Conduit it returned, including across a concurrent close.
  @SpecRef({"9.1", "10.4"})
  @Test
  void close_concurrentRetrieval_closesEveryReturnedConduit()
    throws Exception {

    final var circuit = cortex.circuit();

    try {

      final var bank = circuit.bank(Integer.class);
      final var returned = ConcurrentHashMap.< Conduit< Integer > > newKeySet();

      returned.add(bank.get(cortex.name("retained")));

      final int threads = 16;
      final var start = new CountDownLatch(1);

      final var executor = TestTasks.virtual();
      final var futures = new ArrayList< Future< ? > >();

      try {

        for (int i = 0; i < threads; i++) {

          final var name = cortex.name("racing." + i);

          futures.add(
            executor.submit(() -> {

              await(start, "the concurrent bank retrieval start gate");

              try {
                returned.add(bank.get(name));
              } catch (final Fault refused) {
                // A lookup that loses the race to close is correctly refused.
              }

              return null;

            })
          );

        }

        futures.add(
          executor.submit(() -> {
            await(start, "the concurrent bank retrieval start gate");
            bank.close();
            return null;
          })
        );

        start.countDown();

        for (final var future : futures) {
          get(future, "a bank lookup racing close");
        }

      } finally {

        executor.close();

      }

      bank.closeAwait();

      final var probe =
        circuit.< Integer > subscriber(
          cortex.name("probe"),
          (_, _) -> {
          }
        );

      for (final var conduit : returned) {

        assertThrows(
          Fault.class,
          () -> conduit.subscribe(probe),
          "a conduit the bank returned survived bank close"
        );

      }

    } finally {

      circuit.close();

    }

  }

  /// Bank close does not affect a Conduit created independently.
  @SpecRef("10.4")
  @Test
  void close_independentlyCreatedConduit_leavesConduitOpen() {

    final var circuit = cortex.circuit();

    try {

      final var bank = circuit.bank(Integer.class);
      final var independent = circuit.conduit(Integer.class);

      bank.get(cortex.name("bank.owned"));
      bank.closeAwait();

      assertDoesNotThrow(() -> independent.get(cortex.name("independent.pipe")));

    } finally {

      circuit.close();

    }

  }

  /// Closing the bank closes all materialized conduits and refuses further gets.
  ///
  /// After bank.close() drains:
  /// - [Bank#get(Name)] raises a [Fault] — the bank is closed
  /// - A previously returned conduit is itself closed: any [Source] mutating
  ///   operation on it raises a [Fault]
  /// Closing a Bank closes every materialized Conduit.
  @SpecRef({"9.1", "10.4"})
  @Test
  void close_materializedConduits_closesEveryConduit() {

    final var circuit = cortex.circuit();

    try {

      final var bank = circuit.bank(Integer.class);
      final var conduit = bank.get(cortex.name("owned"));
      final var subscriber = circuit.< Integer > subscriber(
        cortex.name("sub"),
        (_, _) -> {
        }
      );

      bank.closeAwait();

      assertThrows(
        Fault.class,
        () -> bank.get(cortex.name("owned"))
      );

      assertThrows(
        Fault.class,
        () -> conduit.subscribe(subscriber)
      );

    } finally {

      circuit.close();

    }

  }

  /// Closing the bank a second time does not throw.
  /// Bank close is idempotent.
  @SpecRef({"9.1", "10.4"})
  @Test
  void close_repeatedBankCalls_areIdempotent() {

    final var circuit = cortex.circuit();

    try {

      final var bank = circuit.bank(Integer.class);

      bank.get(cortex.name("x"));

      assertDoesNotThrow(() -> {
        bank.close();
        bank.close();
      });

    } finally {

      circuit.close();

    }

  }

  /// Different names produce independent conduit instances.
  /// Different Bank names produce distinct Conduits.
  @SpecRef("10.4")
  @Test
  void get_differentNames_returnsDistinctConduits() {

    final var circuit = cortex.circuit();

    try {

      final var bank = circuit.bank(Integer.class);

      assertNotSame(
        bank.get(cortex.name("a")),
        bank.get(cortex.name("b"))
      );

      bank.close();

    } finally {

      circuit.close();

    }

  }

  /// `close()` accepts the request without waiting for cleanup, so this pins the window the
  /// accepting call opens rather than the state after it has drained. A receptor holds the circuit
  /// thread, which is the only thread cleanup can run on, so when `close()` returns the cleanup it
  /// queued provably has not executed. A lookup for an already-materialized name must still be
  /// refused there: the refusal follows from the acceptance, not from the cleanup.
  ///
  /// Here the Bank is itself the closed receiver, so the fault identifies the Bank — the one
  /// attribution `Bank.get` owns, as distinct from a lookup rejected downstream by a closed
  /// Circuit, which identifies the Circuit instead (§9.1).
  ///
  /// Bank lookup of a materialized name is refused once close is accepted, naming the Bank.
  @SpecRef({"9.1", "10.4", "15.3", "16.3"})
  @Test
  void get_materializedNameAfterCloseAccepted_throwsFaultNamingBank()
    throws Exception {

    final var circuit = cortex.circuit();
    final var bank = circuit.bank(Integer.class);
    final var name = cortex.name("materialized");
    final var conduit = bank.get(name);
    final var occupied = new CountDownLatch(1);
    final var release = new CountDownLatch(1);

    try {

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("observer"),
          (_, registrar) -> registrar.register(_ -> {

            occupied.countDown();

            try {
              release.await();
            } catch (final InterruptedException interrupted) {
              Thread.currentThread().interrupt();
            }

          })
        )
      );

      circuit.await();

      conduit.get(cortex.name("holder")).emit(1);

      await(occupied, "the circuit thread entering the holding receptor");

      bank.close();

      final var fault =
        assertThrows(
          Fault.class,
          () -> bank.get(name),
          "a materialized name stayed reachable after close was accepted"
        );

      assertSame(
        bank.subject(),
        fault.subject(),
        "a Bank that refused its own lookup did not identify itself"
      );

    } finally {

      release.countDown();
      circuit.close();

    }

  }

  /// Concurrent gets for the same name all return the exact same conduit.
  ///
  /// Twenty threads race to materialize the same name simultaneously.
  /// Every thread must receive the identical instance — the factory must
  /// be invoked at most once.
  /// Concurrent same-name Bank lookup returns one canonical Conduit.
  @SpecRef("10.4")
  @Test
  void get_sameNameConcurrently_returnsSameConduit()
    throws Exception {

    final var circuit = cortex.circuit();

    try {

      final var bank = circuit.bank(Integer.class);
      final var name = cortex.name("concurrent");
      final var latch = new CountDownLatch(1);
      final var results = new ConcurrentHashMap< Integer, Conduit< Integer > >();
      final int threads = 20;
      final var executor = TestTasks.fixed(threads);

      try {

        final var futures =
          new java.util.ArrayList< java.util.concurrent.Future< ? > >();

        for (int i = 0; i < threads; i++) {

          final var index = i;

          futures.add(
            executor.submit(() -> {
              await(latch, "the concurrent bank-lookup start gate");
              results.put(index, bank.get(name));
              return null;
            })
          );

        }

        latch.countDown();

        for (final var future : futures) {
          get(future, "a concurrent bank lookup");
        }

      } finally {

        executor.close();

      }

      final var first = results.get(0);

      results.values().forEach(
        conduit -> assertSame(first, conduit)
      );

    } finally {

      circuit.close();

    }

  }

  /// Same name returns the same conduit instance on every call.
  /// Repeated same-name Bank lookup returns canonical identity.
  @SpecRef("10.4")
  @Test
  void get_sameName_returnsSameConduit() {

    final var circuit = cortex.circuit();

    try {

      final var bank = circuit.bank(Integer.class);
      final var name = cortex.name("a");

      assertSame(
        bank.get(name),
        bank.get(name)
      );

      bank.close();

    } finally {

      circuit.close();

    }

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

}
