// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import io.humainary.substrates.api.*;
import org.junit.jupiter.api.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §11.3 Current and the Java projection of
/// [Substrates.Cortex#current()].
/// @author William David Louth
/// @since 1.0

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.2/SPEC.md")
final class CurrentContractTest
  extends TestSupport {

  /// Distinct execution contexts have distinct Current identifiers.
  @SpecRef({"4.2", "11.3"})
  @Test
  void current_distinctExecutionContexts_returnsDistinctIds()
    throws InterruptedException {

    final var cortex =
      cortex();

    final var mainId =
      cortex.current().subject().id();

    final var latch =
      new CountDownLatch(1);

    final var otherId =
      new AtomicReference< Id >();

    final var thread =
      Thread.ofPlatform().start(() -> {

        otherId.set(
          cortex.current().subject().id()
        );

        latch.countDown();

      });

    await(latch, "the platform-thread Current identifier lookup");
    join(thread, "the platform-thread Current identifier lookup");

    assertNotNull(otherId.get());
    assertNotEquals(
      mainId,
      otherId.get(),
      "Different threads should have different IDs"
    );

  }

  /// Distinct execution contexts have distinct Current instances.
  @SpecRef("11.3")
  @Test
  void current_distinctExecutionContexts_returnsDistinctInstances()
    throws InterruptedException {

    final var cortex =
      cortex();

    final var mainCurrent =
      cortex.current();

    final var latch =
      new CountDownLatch(1);

    final var otherCurrent =
      new AtomicReference< Current >();

    final var thread =
      Thread.ofPlatform().start(() -> {

        otherCurrent.set(
          cortex.current()
        );

        latch.countDown();

      });

    await(latch, "the platform-thread Current lookup");
    join(thread, "the platform-thread Current lookup");

    assertNotNull(otherCurrent.get());
    assertNotSame(
      mainCurrent,
      otherCurrent.get(),
      "Different threads should have different Current instances"
    );

  }

  /// Current is identity-bearing and exposes a subject.
  @SpecRef({"4.3", "11.3"})
  @Test
  void current_fromExecutionContext_hasSubject() {

    final var cortex =
      cortex();

    final var current =
      cortex.current();

    final var subject =
      current.subject();

    assertNotNull(subject);

  }

  /// Every execution context has exactly one Current.
  @SpecRef("11.3")
  @Test
  void current_fromExecutionContext_returnsNonNull() {

    final var cortex =
      cortex();

    final var current =
      cortex.current();

    assertNotNull(current);

  }

  /// Substrates.Cortex#current supports a virtual-thread context
  /// (SPEC Appendix A.2, virtual-thread execution-context binding).
  @Test
  void current_fromVirtualThread_returnsIdentityBearingInstance()
    throws InterruptedException {

    final var cortex =
      cortex();

    final var latch =
      new CountDownLatch(1);

    final var virtualCurrent =
      new AtomicReference< Current >();

    final var thread =
      Thread.ofVirtual().start(() -> {

        virtualCurrent.set(
          cortex.current()
        );

        latch.countDown();

      });

    await(latch, "the virtual-thread Current lookup");
    join(thread, "the virtual-thread Current lookup");

    assertNotNull(
      virtualCurrent.get(),
      "Virtual threads should have Current instances"
    );

    assertNotNull(
      virtualCurrent.get().subject().id(),
      "Virtual thread Current should have an ID"
    );

    assertNotNull(
      virtualCurrent.get().subject().name(),
      "Virtual thread Current should have a name"
    );

  }

  /// Platform and virtual threads are distinct Current contexts
  /// (SPEC Appendix A.2, virtual-thread execution-context binding).
  @Test
  void current_platformAndVirtualThreads_returnsDistinctInstances()
    throws InterruptedException {

    final var cortex =
      cortex();

    final var platformLatch =
      new CountDownLatch(1);

    final var virtualLatch =
      new CountDownLatch(1);

    final var platformCurrent =
      new AtomicReference< Current >();

    final var virtualCurrent =
      new AtomicReference< Current >();

    final var platformThread =
      Thread.ofPlatform().start(() -> {

        platformCurrent.set(
          cortex.current()
        );

        platformLatch.countDown();

      });

    final var virtualThread =
      Thread.ofVirtual().start(() -> {

        virtualCurrent.set(
          cortex.current()
        );

        virtualLatch.countDown();

      });

    await(platformLatch, "the platform-thread Current lookup");
    await(virtualLatch, "the virtual-thread Current lookup");

    join(platformThread, "the platform-thread Current lookup");
    join(virtualThread, "the virtual-thread Current lookup");

    assertNotNull(platformCurrent.get());
    assertNotNull(virtualCurrent.get());

    assertNotSame(
      platformCurrent.get(),
      virtualCurrent.get(),
      "Platform and virtual threads should have different Current instances"
    );

  }

  /// An execution context's Current identifier remains stable.
  @SpecRef({"4.2", "11.3"})
  @Test
  void current_repeatedLookup_returnsSameId() {

    final var cortex =
      cortex();

    final var id1 =
      cortex.current().subject().id();

    final var id2 =
      cortex.current().subject().id();

    assertEquals(
      id1,
      id2,
      "Current ID should be stable across calls"
    );

  }

  /// Repeated lookup in one execution context returns its interned Current.
  @SpecRef("11.3")
  @Test
  void current_repeatedLookup_returnsSameInstance() {

    final var cortex =
      cortex();

    final var current1 =
      cortex.current();

    final var current2 =
      cortex.current();

    assertSame(
      current1,
      current2,
      "Same thread should return same Current instance"
    );

  }

  /// A retained Current preserves its original identity and
  /// immutable subject when inspected from other execution contexts.
  @SpecRef({"6.4", "11.3"})
  @Test
  void current_retainedAcrossContexts_preservesOriginalIdentity()
    throws InterruptedException {

    final var cortex = cortex();
    final var retained = new AtomicReference< Current >();
    final var originalSubject = new AtomicReference< Subject< Current > >();

    final var producer =
      Thread.ofPlatform().start(() -> {
        final var current = cortex.current();
        retained.set(current);
        originalSubject.set(current.subject());
      });

    join(producer, "the Current producer thread");

    assertNotNull(retained.get());
    assertNotSame(cortex.current(), retained.get());
    assertSame(originalSubject.get(), retained.get().subject());

    final var observedSubject = new AtomicReference< Subject< Current > >();
    final var observer =
      Thread.ofPlatform().start(() -> observedSubject.set(retained.get().subject()));

    join(observer, "the Current observer thread");

    assertSame(originalSubject.get(), observedSubject.get());
    assertEquals(originalSubject.get().id(), observedSubject.get().id());
    assertEquals(originalSubject.get().name(), observedSubject.get().name());

  }

  /// A Current subject carries an identifier.
  @SpecRef({"4.2", "4.3", "11.3"})
  @Test
  void subject_current_hasId() {

    final var cortex =
      cortex();

    final var current =
      cortex.current();

    final var id =
      current.subject().id();

    assertNotNull(id);

  }

  /// A Current subject carries a name.
  @SpecRef({"4.1", "4.3", "11.3"})
  @Test
  void subject_current_hasName() {

    final var cortex =
      cortex();

    final var current =
      cortex.current();

    final var name =
      current.subject().name();

    assertNotNull(name);

  }

  /// A Current subject has a valid, non-empty name.
  @SpecRef({"4.1", "4.3", "11.3"})
  @Test
  void subject_name_isNonEmpty() {

    final var cortex =
      cortex();

    final var current =
      cortex.current();

    final var name =
      current.subject().name();

    final var nameString =
      name.toString();

    assertNotNull(nameString);
    assertFalse(
      nameString.isEmpty(),
      "Current name should not be empty"
    );

  }

}
