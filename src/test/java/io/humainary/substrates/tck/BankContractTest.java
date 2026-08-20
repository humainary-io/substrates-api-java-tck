// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §10.4 Bank canonical lookup, concurrency, ownership, and lifecycle.

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.1/SPEC.md")
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

  }

  /// Closing the bank a second time does not throw.
  /// Bank close is idempotent.
  @SpecRef({"9.1", "10.4"})
  @Test
  void close_repeatedBankCalls_areIdempotent() {

    final var circuit = cortex.circuit();
    final var bank = circuit.bank(Integer.class);

    bank.get(cortex.name("x"));

    assertDoesNotThrow(() -> {
      bank.close();
      bank.close();
    });

  }

  /// Different names produce independent conduit instances.
  /// Different Bank names produce distinct Conduits.
  @SpecRef("10.4")
  @Test
  void get_differentNames_returnsDistinctConduits() {

    final var circuit = cortex.circuit();
    final var bank = circuit.bank(Integer.class);

    assertNotSame(
      bank.get(cortex.name("a")),
      bank.get(cortex.name("b"))
    );

  }

  /// Concurrent gets for the same name all return the exact same conduit.
  ///
  /// Twenty threads race to materialize the same name simultaneously.
  /// Every thread must receive the identical instance — the factory must
  /// be invoked at most once.
  /// Concurrent same-name Bank lookup returns one canonical Conduit.
  @SpecRef("10.4")
  @SuppressWarnings("resource")
  @Test
  void get_sameNameConcurrently_returnsSameConduit()
    throws Exception {

    final var circuit = cortex.circuit();
    final var bank = circuit.bank(Integer.class);
    final var name = cortex.name("concurrent");
    final var latch = new CountDownLatch(1);
    final var results = new ConcurrentHashMap< Integer, Conduit< Integer > >();
    final int threads = 20;
    final var executor = Executors.newFixedThreadPool(threads);

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

      executor.shutdown();

    }

    final var first = results.get(0);

    results.values().forEach(
      conduit -> assertSame(first, conduit)
    );

  }

  /// Same name returns the same conduit instance on every call.
  /// Repeated same-name Bank lookup returns canonical identity.
  @SpecRef("10.4")
  @Test
  void get_sameName_returnsSameConduit() {

    final var circuit = cortex.circuit();
    final var bank = circuit.bank(Integer.class);
    final var name = cortex.name("a");

    assertSame(
      bank.get(name),
      bank.get(name)
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

}
