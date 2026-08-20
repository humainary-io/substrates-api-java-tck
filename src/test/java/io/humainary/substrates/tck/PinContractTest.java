// Copyright (c) 2026 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §11.6 Pin initialization, immediate owner-context access, context
/// guards, handle emission, identity, naming, validation, and capability boundaries.

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.1/SPEC.md")
final class PinContractTest
  extends TestSupport {

  private Cortex cortex;
  private Circuit circuit;

  // ----- factory: subject and initialization -----

  /// A Pin may be emitted and retained as a managed substrate
  /// handle.
  @SpecRef({"6.1.1", "11.6"})
  @Test
  void emit_pinHandle_deliversManagedHandle() {

    final Pin< Integer > pin =
      circuit.pin(7);

    final AtomicReference< Pin< Integer > > received =
      new AtomicReference<>();

    final Pipe< Pin< Integer > > handlePipe =
      circuit.pipe(received::set);

    handlePipe.emit(pin);
    circuit.await();

    assertSame(pin, received.get());

  }

  /// Pin get and set execute immediately in the owner context.
  @SpecRef("11.6")
  @Test
  void getAndSet_fromOwnerContext_executeImmediately() {

    final Pin< Integer > pin =
      circuit.pin(1);

    final AtomicReference< Integer > before =
      new AtomicReference<>();
    final AtomicReference< Integer > after =
      new AtomicReference<>();

    final Pipe< Integer > driver =
      circuit.pipe((Integer step) -> {
        before.set(pin.get());
        pin.set(pin.get() + step);
        after.set(pin.get());
      });

    driver.emit(5);
    circuit.await();

    assertEquals(1, before.get());
    assertEquals(6, after.get());

  }

  // ----- context guard -----

  /// A managed Pin may cross contexts and be dereferenced
  /// after returning to its owner.
  @SpecRef({"6.1.1", "11.6"})
  @Test
  void get_afterForeignCircuitRoundTrip_succeedsOnOwnerContext() {

    final Pin< Integer > pin =
      circuit.pin(11);

    final var foreign = cortex.circuit();

    try {

      final AtomicReference< Integer > observed =
        new AtomicReference<>();

      // Owner-side pipe: dereferences the pin on owner context.
      final Pipe< Pin< Integer > > applyOnOwner =
        circuit.pipe((Pin< Integer > p) ->
          observed.set(p.get())
        );

      // Coordinator-side pipe: routes the handle back to the owner without
      // dereferencing it on its own context.
      final Pipe< Pin< Integer > > forwarder =
        foreign.pipe(applyOnOwner::emit
        );

      forwarder.emit(pin);
      foreign.await();
      circuit.await();

      assertEquals(11, observed.get());

    } finally {

      foreign.close();

    }

  }

  /// An initialized Pin begins with its non-null seed value.
  @SpecRef("11.6")
  @Test
  void get_beforeFirstSet_returnsSeed() {

    final Pin< Integer > pin =
      circuit.pin(42);

    final AtomicReference< Integer > observed =
      new AtomicReference<>();

    // get() is owner-only — invoke through a circuit-owned pipe receptor.
    final Pipe< Integer > probe =
      circuit.pipe((Integer ignored) ->
        observed.set(pin.get())
      );

    probe.emit(0);
    circuit.await();

    assertEquals(42, observed.get());

  }

  /// Emission transports the handle to a foreign Circuit but does not transfer its ownership. The
  /// receiver therefore attempts to dereference in the only context relevant to the rule.
  ///
  /// An emitted Pin cannot be dereferenced in a foreign Circuit context.
  @SpecRef("11.6")
  @Test
  void get_emittedToForeignCircuit_throwsIllegalStateException() {

    final Pin< Integer > pin =
      circuit.pin(7);

    final var foreign = cortex.circuit();

    try {

      final AtomicReference< Throwable > captured =
        new AtomicReference<>();

      final Pipe< Pin< Integer > > probe =
        foreign.pipe((Pin< Integer > p) -> {
          try {
            p.get();
          } catch (final Throwable t) {
            captured.set(t);
          }
        });

      probe.emit(pin);
      foreign.await();

      assertInstanceOf(
        IllegalStateException.class,
        captured.get()
      );

    } finally {

      foreign.close();

    }

  }

  /// Pin get from caller context signals illegal context use.
  @SpecRef({"11.6", "15.1"})
  @Test
  void get_fromCallerContext_throwsIllegalStateException() {

    final Pin< Integer > pin =
      circuit.pin(1);

    assertThrows(
      IllegalStateException.class,
      pin::get
    );

  }

  // ----- Pin as managed emission handle -----

  /// The probe runs inside a second Circuit because Pin validity is defined by Circuit context, not by
  /// the Java caller thread. Capturing the callback exception makes that context violation observable.
  ///
  /// Pin get from another Circuit signals illegal context use.
  @SpecRef({"11.6", "15.1"})
  @Test
  void get_fromForeignCircuitContext_throwsIllegalStateException() {

    final Pin< Integer > pin =
      circuit.pin(1);

    final var foreign = cortex.circuit();

    try {

      final AtomicReference< Throwable > captured =
        new AtomicReference<>();

      final Pipe< Integer > probe =
        foreign.pipe((Integer ignored) -> {
          try {
            pin.get();
            captured.set(null);
          } catch (final Throwable t) {
            captured.set(t);
          }
        });

      probe.emit(0);
      foreign.await();

      assertInstanceOf(
        IllegalStateException.class,
        captured.get()
      );

    } finally {

      foreign.close();

    }

  }

  /// Pin creation after Circuit close signals closed resource.
  @SpecRef({"9.1", "11.6"})
  @Test
  void pin_afterCircuitClose_throwsFault() {

    final var owner = cortex.circuit();
    owner.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> owner.pin(0)
      );

    assertSame(
      owner.subject(),
      fault.subject()
    );

  }

  /// Pin is a Substrate but not a Pipe, Source, or Resource.
  @SpecRef("11.6")
  @Test
  void pin_capabilitySurface_excludesPipeSourceAndResource() {

    final Object pin =
      circuit.pin(0);

    assertInstanceOf(Substrate.class, pin);
    assertFalse(pin instanceof Pipe< ? >);
    assertFalse(pin instanceof Receptor< ? >);

  }

  // ----- null arguments (caller-thread synchronous rejection) -----

  /// Named Pin creation after Circuit close signals closed resource.
  @SpecRef({"9.1", "11.6"})
  @Test
  void pin_namedAfterCircuitClose_throwsFault() {

    final var owner = cortex.circuit();
    final Name name = cortex.name("p");
    owner.close();

    final var fault =
      assertThrows(
        Fault.class,
        () -> owner.pin(name, 0)
      );

    assertSame(
      owner.subject(),
      fault.subject()
    );

  }

  /// Pin factories reject required null arguments.
  @SpecRef({"11.6", "15.2"})
  @Test
  void pin_nullRequiredArguments_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> circuit.pin((Integer) null)
    );

    final Name name = cortex.name("p");

    assertThrows(
      NullPointerException.class,
      () -> circuit.pin(null, 0)
    );

    assertThrows(
      NullPointerException.class,
      () -> circuit.pin(name, null)
    );

  }

  // ----- closed-circuit Fault -----

  /// Equal-name Pin factories return distinct handles and identities.
  @SpecRef("11.6")
  @Test
  void pin_repeatedNamedFactory_returnsDistinctHandles() {

    final Name name =
      cortex.name("ledger");

    final Pin< Integer > a =
      circuit.pin(name, 0);

    final Pin< Integer > b =
      circuit.pin(name, 0);

    assertNotSame(a, b);
    assertNotEquals(
      a.subject().id(),
      b.subject().id()
    );

  }

  /// Named Pin creation binds the supplied subject name.
  @SpecRef("11.6")
  @Test
  void pin_withExplicitName_bindsSuppliedSubjectName() {

    final Name name =
      cortex.name("ledger");

    final Pin< Integer > pin =
      circuit.pin(name, 0);

    assertEquals(name, pin.subject().name());

  }

  /// An unnamed Pin uses its owning Circuit's name.
  @SpecRef("11.6")
  @Test
  void pin_withoutExplicitName_usesCircuitName() {

    final Pin< Integer > pin =
      circuit.pin(0);

    assertEquals(
      Pin.class,
      pin.subject().type()
    );

    assertEquals(
      circuit.subject().name(),
      pin.subject().name()
    );

    assertTrue(
      pin.subject().enclosure().isPresent()
    );

    pin.subject().enclosure(
      parent -> assertEquals(
        circuit.subject().id(),
        parent.id()
      )
    );

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();
    circuit = cortex.circuit();

  }

  /// Pin set from caller context signals illegal context use.
  @SpecRef({"11.6", "15.1"})
  @Test
  void set_fromCallerContext_throwsIllegalStateException() {

    final Pin< Integer > pin =
      circuit.pin(1);

    assertThrows(
      IllegalStateException.class,
      () -> pin.set(2)
    );

  }

  /// The write attempt executes in a second Circuit context, separating this affinity rule from a
  /// simple caller-thread check and preserving the observable exception for assertion.
  ///
  /// Pin set from another Circuit signals illegal context use.
  @SpecRef({"11.6", "15.1"})
  @Test
  void set_fromForeignCircuitContext_throwsIllegalStateException() {

    final Pin< Integer > pin =
      circuit.pin(1);

    final var foreign = cortex.circuit();

    try {

      final AtomicReference< Throwable > captured =
        new AtomicReference<>();

      final Pipe< Integer > probe =
        foreign.pipe((Integer ignored) -> {
          try {
            pin.set(2);
            captured.set(null);
          } catch (final Throwable t) {
            captured.set(t);
          }
        });

      probe.emit(0);
      foreign.await();

      assertInstanceOf(
        IllegalStateException.class,
        captured.get()
      );

    } finally {

      foreign.close();

    }

  }

  /// Pin set is visible to the immediately following owner-context get.
  @SpecRef("11.6")
  @Test
  void set_fromOwnerContext_isImmediatelyVisible() {

    final Pin< Integer > pin =
      circuit.pin(0);

    final AtomicReference< Integer > observed =
      new AtomicReference<>();

    final Pipe< Integer > driver =
      circuit.pipe((Integer next) -> {
        pin.set(next);
        // The very next statement should observe the new value — this is the
        // defining contract of Pin vs Port.
        observed.set(pin.get());
      });

    driver.emit(99);
    circuit.await();

    assertEquals(99, observed.get());

  }

  /// Pin set rejects absence in the owner context.
  @SpecRef({"11.6", "15.2"})
  @Test
  void set_nullValueOnOwnerContext_throwsNullPointerException() {

    final Pin< Integer > pin =
      circuit.pin(1);

    final AtomicReference< Throwable > captured =
      new AtomicReference<>();

    final Pipe< Integer > driver =
      circuit.pipe((Integer ignored) -> {
        try {
          pin.set(null);
        } catch (final Throwable t) {
          captured.set(t);
        }
      });

    driver.emit(0);
    circuit.await();

    assertInstanceOf(
      NullPointerException.class,
      captured.get()
    );

  }

  @AfterEach
  void tearDown() {

    circuit.closeAwait();

  }

}
