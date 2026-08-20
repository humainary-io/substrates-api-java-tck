// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// TCK tests for root pools created via [Cortex#pool(java.util.function.Function)].

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.0/SPEC.md")
final class PoolContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;

  /// A derived Pool rejects and caches an absent transform result.
  @SpecRef({"10.1", "15.2"})
  @Test
  void get_derivedFactoryReturnsNull_replaysCachedAbsenceViolation() {

    final var invocations = new AtomicInteger();
    final Pool< String > source = cortex.pool(Name::toString);
    final Pool< Integer > derived =
      source.pool(_ -> {
        invocations.incrementAndGet();
        return null;
      });
    final var name = cortex.name("pool.derived.absent");

    assertThrows(NullPointerException.class, () -> derived.get(name));
    assertThrows(NullPointerException.class, () -> derived.get(name));
    assertEquals(1, invocations.get());

  }

  /// A derived Pool caches a thrown transform outcome per name.
  @SpecRef("10.1")
  @Test
  void get_derivedFactoryThrows_replaysCachedFailurePerName() {

    final var invocations = new AtomicInteger();
    final Pool< String > source = cortex.pool(Name::toString);
    final Pool< Integer > derived =
      source.pool(_ -> {
        invocations.incrementAndGet();
        throw new IllegalStateException("boom");
      });
    final var name = cortex.name("pool.derived.failing");

    assertThrows(IllegalStateException.class, () -> derived.get(name));
    assertThrows(IllegalStateException.class, () -> derived.get(name));
    assertEquals(1, invocations.get());

  }

  /// A null factory result is rejected and cached per name.
  @SpecRef({"10.1", "15.2"})
  @Test
  void get_factoryReturnsNull_replaysCachedAbsenceViolation() {

    final var invocations =
      new AtomicInteger();

    final Pool< String > pool =
      cortex.pool(
        _ -> {
          invocations.incrementAndGet();
          return null;
        }
      );

    final var name =
      cortex.name(
        "pool.absent"
      );

    assertThrows(
      NullPointerException.class,
      () -> pool.get(name)
    );

    assertThrows(
      NullPointerException.class,
      () -> pool.get(name)
    );

    assertEquals(
      1,
      invocations.get()
    );

  }

  /// A root Pool caches a thrown factory outcome per name.
  @SpecRef("10.1")
  @Test
  void get_factoryThrows_replaysCachedFailurePerName() {

    final var invocations =
      new AtomicInteger();

    final Pool< String > pool =
      cortex.pool(
        _ -> {
          invocations.incrementAndGet();
          throw new IllegalStateException("boom");
        }
      );

    final var name =
      cortex.name(
        "pool.failing"
      );

    assertThrows(
      IllegalStateException.class,
      () -> pool.get(name)
    );

    assertThrows(
      IllegalStateException.class,
      () -> pool.get(name)
    );

    assertEquals(
      1,
      invocations.get()
    );

  }

  /// A root Pool lazily materializes keyed Cells with matching names.
  @Test
  void get_keyedCellPool_materializesLazilyAndBindsName() {

    final Pool< Cell< Integer > > cells =
      cortex.pool(
        name ->
          circuit.cell(
            name,
            0
          )
      );

    final var name =
      cortex.name(
        "cells.reading"
      );

    final Cell< Integer > cell =
      cells.get(
        name
      );

    assertSame(
      cell,
      cells.get(name)
    );

    assertEquals(
      name,
      cell.subject().name()
    );

    cell.pipe().emit(7);
    circuit.await();

    assertEquals(
      7,
      cells.get(name).get()
    );

  }

  /// A pool-backed Subscriber mirrors each channel into a keyed Cell.
  @Test
  void get_keyedCellSubscriber_mirrorsLatestChannelValue() {

    final Pool< Cell< Integer > > cells =
      cortex.pool(
        name ->
          circuit.cell(
            name,
            0
          )
      );

    final var conduit =
      circuit.conduit(
        Integer.class
      );

    conduit.subscribe(
      circuit.subscriber(
        cortex.name(
          "mirror"
        ),
        cells.pool(
          Cell::pipe
        )
      )
    );

    final var left =
      cortex.name(
        "channel.left"
      );

    final var right =
      cortex.name(
        "channel.right"
      );

    conduit.get(left).emit(1);
    conduit.get(right).emit(2);
    conduit.get(left).emit(3);

    circuit.await();

    assertEquals(
      3,
      cells.get(left).get()
    );

    assertEquals(
      2,
      cells.get(right).get()
    );

  }

  /// A root Pool invokes its factory at most once per name.
  @SpecRef("10.1")
  @Test
  void get_repeatedName_invokesFactoryOnce() {

    final var invocations =
      new AtomicInteger();

    final Pool< Object > pool =
      cortex.pool(
        _ -> {
          invocations.incrementAndGet();
          return new Object();
        }
      );

    final var name =
      cortex.name(
        "pool.once"
      );

    assertSame(
      pool.get(name),
      pool.get(name)
    );

    assertEquals(
      1,
      invocations.get()
    );

    pool.get(
      cortex.name(
        "pool.other"
      )
    );

    assertEquals(
      2,
      invocations.get()
    );

  }

  /// Concurrent derived-Pool retrieval invokes its transform once and returns
  /// one canonical result for the name.
  @SuppressWarnings("resource")
  @SpecRef("10.1")
  @Test
  void get_sameNameConcurrently_invokesDerivedFactoryOnceAndReturnsSameInstance()
    throws Exception {

    final var invocations = new AtomicInteger();
    final Pool< Name > source = cortex.pool(name -> name);
    final Pool< Object > derived = source.pool(_ -> {
      invocations.incrementAndGet();
      return new Object();
    });
    final var name = cortex.name("pool.derived.concurrent");
    final var start = new CountDownLatch(1);
    final var results = new ConcurrentHashMap< Integer, Object >();
    final int threads = 20;
    final var executor = Executors.newFixedThreadPool(threads);

    try {

      final var futures = new java.util.ArrayList< java.util.concurrent.Future< ? > >();

      for (int index = 0; index < threads; index++) {
        final int key = index;
        futures.add(executor.submit(() -> {
          await(start, "the concurrent conduit-pool lookup start gate");
          results.put(key, derived.get(name));
          return null;
        }));
      }

      start.countDown();

      for (final var future : futures) {
        get(future, "a concurrent conduit-pool lookup");
      }

    } finally {

      executor.shutdown();

    }

    final var first = results.get(0);
    results.values().forEach(value -> assertSame(first, value));
    assertEquals(1, invocations.get());

  }

  /// Concurrent root-Pool retrieval invokes the factory at most once per name.
  @SuppressWarnings("resource")
  @SpecRef("10.1")
  @Test
  void get_sameNameConcurrently_invokesFactoryOnceAndReturnsSameInstance() throws Exception {

    final var invocations = new AtomicInteger();
    final Pool< Object > pool = cortex.pool(_ -> {
      invocations.incrementAndGet();
      return new Object();
    });
    final var name = cortex.name("pool.concurrent");
    final var start = new CountDownLatch(1);
    final var results = new ConcurrentHashMap< Integer, Object >();
    final int threads = 20;
    final var executor = Executors.newFixedThreadPool(threads);

    try {

      final var futures = new java.util.ArrayList< java.util.concurrent.Future< ? > >();

      for (int index = 0; index < threads; index++) {
        final int key = index;
        futures.add(executor.submit(() -> {
          await(start, "the concurrent pool-lookup start gate");
          results.put(key, pool.get(name));
          return null;
        }));
      }

      start.countDown();

      for (final var future : futures) {
        get(future, "a concurrent pool lookup");
      }

    } finally {

      executor.shutdown();

    }

    final var first = results.get(0);
    results.values().forEach(value -> assertSame(first, value));
    assertEquals(1, invocations.get());

  }

  /// Pool Subject and Substrate lookup overloads delegate by Name.
  @Test
  void get_subjectAndSubstrateOverloads_delegateToName() {

    final Pool< String > pool =
      cortex.pool(
        Object::toString
      );

    assertSame(
      pool.get(circuit.subject().name()),
      pool.get(circuit.subject())
    );

    assertSame(
      pool.get(circuit.subject().name()),
      pool.get(circuit)
    );

  }

  /// Root Pools compose derived views with canonical per-name results.
  @SpecRef("10.1")
  @Test
  void pool_composedDerivedViews_cacheEachTransformedResult() {

    final Pool< String > pool =
      cortex.pool(
        Object::toString
      );

    final Pool< Integer > lengths =
      pool.pool(
        String::length
      );

    final var name =
      cortex.name(
        "pool.derived"
      );

    assertEquals(
      "pool.derived".length(),
      lengths.get(name)
    );

    assertSame(
      lengths.get(name),
      lengths.get(name)
    );

  }

  /// A root Pool is not a Resource and does not own materialized resources.
  @SpecRef("10.1")
  @Test
  void pool_materializedResource_exposesNoOwningLifecycle() {

    final var resource = cortex.circuit();
    final Pool< Circuit > pool = cortex.pool(_ -> resource);

    try {

      assertSame(resource, pool.get(cortex.name("pool.resource")));
      assertFalse(pool instanceof AutoCloseable);
      assertDoesNotThrow(() -> resource.conduit(Integer.class));

    } finally {

      resource.close();

    }

  }

  /// Root Pool operations reject required null arguments.
  @SpecRef("15.2")
  @Test
  void pool_nullRequiredArguments_throwNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> cortex.pool(
        null
      )
    );

    final Pool< String > pool =
      cortex.pool(
        Object::toString
      );

    assertThrows(
      NullPointerException.class,
      () -> pool.get(
        (Name) null
      )
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
