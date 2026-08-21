// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §§8.1–8.3 State persistence, upsert, ordering, lookup, and required
/// value types, plus the Java projection's iteration, enum, and template conveniences.
/// @author William David Louth
/// @since 1.0
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.2/SPEC.md")
final class StateContractTest
  extends TestSupport {

  private Cortex cortex;

  /// ForEach traverses most-recent slot first.
  @SpecRef("8.1")
  @Test
  void forEach_populatedState_traversesMostRecentFirst() {

    final var alpha = cortex.name("state.foreach.alpha");
    final var beta = cortex.name("state.foreach.beta");
    final var gamma = cortex.name("state.foreach.gamma");

    final var state = cortex.state()
      .state(alpha, 1)
      .state(beta, 2)
      .state(gamma, 3);

    final List< String > names = new java.util.ArrayList<>();

    state.forEach(slot -> names.add(slot.name().path().toString()));

    assertEquals(
      List.of(
        "state.foreach.gamma",
        "state.foreach.beta",
        "state.foreach.alpha"
      ),
      names
    );

  }

  /// An exhausted State iterator signals NoSuchElementException.
  @Test
  void iterator_exhausted_throwsNoSuchElementException() {

    final var metric = cortex.name("state.base.iterator");

    final var state = cortex.state()
      .state(metric, 10);

    final var iterator = state.iterator();

    assertTrue(iterator.hasNext());
    assertEquals(metric, iterator.next().name());
    assertFalse(iterator.hasNext());
    assertThrows(NoSuchElementException.class, iterator::next);

  }

  /// State iterator and stream expose the same order.
  @Test
  void iterator_populatedState_matchesStreamOrder() {

    final var first = cortex.name("state.iter.first");
    final var second = cortex.name("state.iter.second");

    final var state = cortex.state()
      .state(first, 1)
      .state(second, 2);

    final var itr = new java.util.ArrayList< String >();
    state.iterator().forEachRemaining(slot ->
      itr.add(slot.name().path().toString())
    );

    final var streamList = state.stream()
      .map(slot -> slot.name().path().toString())
      .toList();

    assertEquals(streamList, itr);

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

  /// A State retains the stored slot's name, type, and value.
  @SpecRef("8.2")
  @Test
  void slot_fromState_exposesStoredProperties() {

    final var slotName = cortex.name("state.slot.test");
    final Slot< Integer > slot = cortex.slot(slotName, 42);

    assertEquals(slotName, slot.name());
    assertEquals(int.class, slot.type());
    assertEquals(42, slot.value());

  }

  /// State spliterator and stream expose the same order.
  @Test
  void spliterator_populatedState_matchesStreamOrder() {

    final var alpha = cortex.name("state.spliterator.alpha");
    final var beta = cortex.name("state.spliterator.beta");
    final var gamma = cortex.name("state.spliterator.gamma");

    final var state = cortex.state()
      .state(alpha, 1)
      .state(beta, 2)
      .state(gamma, 3);

    final var viaStream = state.stream()
      .map(slot -> slot.name().path().toString())
      .toList();

    final var viaSpliterator = java.util.stream.StreamSupport.stream(
        state.spliterator(),
        false
      ).map(slot -> slot.name().path().toString())
      .toList();

    assertEquals(viaStream, viaSpliterator);
    assertEquals(3, state.spliterator().getExactSizeIfKnown());

  }

  /// Cortex State factories return usable State values.
  @Test
  void state_cortexFactories_returnNonNullStates() {

    final var testName = cortex.name("state.factory.test");

    assertEquals(10, cortex.state().state(testName, 10).value(cortex.slot(testName, 0)));
    assertEquals(20L, cortex.state().state(testName, 20L).value(cortex.slot(testName, 0L)));
    assertEquals(1.5f, cortex.state().state(testName, 1.5f).value(cortex.slot(testName, 0f)), 0.001f);
    assertEquals(2.5, cortex.state().state(testName, 2.5).value(cortex.slot(testName, 0.0)), 0.001);
    assertTrue(cortex.state().state(testName, true).value(cortex.slot(testName, false)));
    assertEquals("test", cortex.state().state(testName, "test").value(cortex.slot(testName, "")));

  }

  /// Validates immutability through structural sharing: base state unaffected by derivation.
  ///
  /// This test demonstrates State's fundamental immutability guarantee: when a new
  /// state is derived from an existing state by adding a slot, the original state
  /// remains completely unchanged. State achieves this through STRUCTURAL SHARING:
  /// derived states prepend new slots to existing chains without copying, creating
  /// an efficient persistent data structure.
  ///
  /// Test Scenario:
  /// ```
  /// base = state().state(alpha, 1)        // [alpha=1]
  /// derived = base.state(beta, 2)         // [beta=2, alpha=1]
  ///
  /// base unchanged:    [alpha=1]          (still has 1 slot)
  /// derived extended:  [beta=2, alpha=1] (has 2 slots)
  /// ```
  ///
  /// Structural Sharing Implementation:
  /// ```
  /// State is an immutable linked list:
  ///
  /// base state:
  ///   [alpha=1] → null
  ///
  /// derived state (shares structure):
  ///   [beta=2] → [alpha=1] → null
  ///              ^
  ///              └─ Shares base's node (no copy!)
  ///
  /// Adding to base does NOT affect derived:
  ///   base2 = base.state(gamma, 3)
  ///   [gamma=3] → [alpha=1] → null
  ///
  /// All three states coexist:
  ///   base:     [alpha=1]
  ///   derived:  [beta=2] → [alpha=1]
  ///   base2:    [gamma=3] → [alpha=1]
  /// ```
  ///
  /// Why structural sharing matters:
  /// - **True immutability**: Original state never changes (safe to share)
  /// - **Memory efficiency**: No copying of entire state (O(1) space per addition)
  /// - **Performance**: O(1) time to add slot (no iteration/copying)
  /// - **Versioning**: Multiple versions coexist efficiently (like Git commits)
  ///
  /// Real-world implications:
  ///
  /// 1. **Configuration inheritance**:
  /// ```java
  /// // Base production config
  /// State prodConfig = cortex.state()
  ///   .state(name("server.port"), 8080)
  ///   .state(name("db.host"), "prod.db");
  ///
  /// // Override for testing (prod config unchanged)
  /// State testConfig = prodConfig
  ///   .state(name("db.host"), "test.db");
  ///
  /// // Both coexist independently
  /// prodConfig.value(dbHost); // "prod.db"
  /// testConfig.value(dbHost); // "test.db"
  /// ```
  ///
  /// 2. **Request context layering**:
  /// ```java
  /// // Base request context
  /// State requestContext = cortex.state()
  ///   .state(name("user.id"), userId);
  ///
  /// // Add trace info (request context unchanged)
  /// State withTrace = requestContext
  ///   .state(name("trace.id"), traceId);
  ///
  /// // Fork for async work (both independent)
  /// State async1 = withTrace.state(name("worker"), "1");
  /// State async2 = withTrace.state(name("worker"), "2");
  /// ```
  ///
  /// 3. **Temporal state snapshots**:
  /// ```java
  /// State t0 = cortex.state().state(name("counter"), 0);
  /// State t1 = t0.state(name("counter"), 1);
  /// State t2 = t1.state(name("counter"), 2);
  ///
  /// // All versions accessible (time-travel)
  /// t0.value(counter); // 0
  /// t1.value(counter); // 1
  /// t2.value(counter); // 2
  /// ```
  ///
  /// Performance characteristics:
  /// - **Add slot**: O(1) time, O(1) space (prepend to linked list)
  /// - **Lookup**: O(n) time where n = chain length (linear search)
  /// - **Memory**: Shared nodes → minimal duplication
  /// - **GC-friendly**: Old unreferenced states collected naturally
  ///
  /// Critical behaviors verified:
  /// - Base state has 1 slot (alpha=1)
  /// - Derived state has 2 slots (beta=2, alpha=1)
  /// - Base state unchanged after derivation (still 1 slot)
  /// - Values independent (base doesn't see beta)
  /// - No defensive copying (efficient)
  ///
  /// Contrast with mutable approach:
  /// ```
  /// // BAD: Mutable state (unsafe to share)
  /// MutableState state = new MutableState();
  /// state.put("alpha", 1);
  ///
  /// MutableState shared = state;  // Same reference!
  /// shared.put("beta", 2);        // Modifies original!
  ///
  /// state.get("beta");  // 2 (unexpected side effect!)
  /// ```
  ///
  /// Expected: base=`[alpha=1]`, derived=`[beta=2, alpha=1]` (independent states)
  /// Deriving a State leaves the prior State unchanged.
  @SpecRef("8.1")
  @Test
  void state_derivedState_leavesOriginalUnchanged() {

    final var alpha = cortex.name("state.share.alpha");
    final var beta = cortex.name("state.share.beta");

    final var base = cortex.state()
      .state(alpha, 1);

    final var derived = base.state(beta, 2);

    assertEquals(1, base.value(cortex.slot(alpha, 0)));
    assertEquals(1, base.stream().count());

    assertEquals(2, derived.stream().count());
    assertEquals(2, derived.value(cortex.slot(beta, 0)));

  }

  /// Distinct enum values produce distinct Name slots.
  @Test
  void state_distinctEnumValues_retainsBothSlots() {

    final var state = cortex.state()
      .state(TestMode.DEBUG)
      .state(Level.LOW)
      .state(TestMode.RELEASE);

    final var modeSlot = cortex.slot(cortex.name(TestMode.class), cortex.name("fallback"));
    final var levelSlot = cortex.slot(cortex.name(Level.class), cortex.name("fallback"));

    assertEquals(cortex.name(TestMode.RELEASE), state.value(modeSlot));
    assertEquals(cortex.name(Level.LOW), state.value(levelSlot));

  }

  /// An empty State contains no slots.
  @SpecRef("8.1")
  @Test
  void state_empty_containsNoSlots() {

    final var empty = cortex.state();

    assertTrue(empty.stream().toList().isEmpty());
    assertFalse(empty.iterator().hasNext());

  }

  /// An enum-derived Slot upserts its matching key.
  @Test
  void state_enumSlot_overridesMatchingNameSlot() {

    final var enumSlot = cortex.slot(TestMode.DEBUG);

    final var state = cortex.state()
      .state(enumSlot)
      .state(cortex.name(TestMode.DEBUG.getDeclaringClass()), cortex.name("CUSTOM"));

    assertEquals(cortex.name("CUSTOM"), state.value(enumSlot));

  }

  /// Enum-derived Slot insertion preserves slot properties.
  @Test
  void state_enumSlot_preservesDerivedProperties() {

    final var enumSlot = cortex.slot(TestMode.RELEASE);
    final var state = cortex.state()
      .state(enumSlot);

    final var expectedName = cortex.name(TestMode.RELEASE.getDeclaringClass());

    assertTrue(
      state.stream()
        .anyMatch(slot ->
          slot.name().equals(expectedName) &&
            slot.value().equals(cortex.name(TestMode.RELEASE))
        )
    );

  }

  /// State#state(Slot) stores an enum-derived Name slot.
  @Test
  void state_enumSlot_storesNameValue() {

    final var enumSlot = cortex.slot(TestMode.DEBUG);
    final var state = cortex.state()
      .state(enumSlot);

    assertEquals(cortex.name(TestMode.DEBUG), state.value(enumSlot));

  }

  /// An enum-derived Slot uses Name as its value type.
  @Test
  void state_enumSlot_usesNameValueType() {

    final var enumSlot = cortex.slot(Level.MEDIUM);

    final var state = cortex.state()
      .state(enumSlot);

    final var slot = state.stream()
      .findFirst()
      .orElseThrow();

    assertEquals(Name.class, slot.type());

  }

  /// Enum-derived Slot insertion supports chaining.
  @Test
  void state_enumSlots_supportsChaining() {

    final var state = cortex.state()
      .state(cortex.slot(TestMode.DEBUG))
      .state(cortex.slot(Level.MEDIUM))
      .state(cortex.slot(TestMode.RELEASE));

    // DEBUG and RELEASE share the TestMode-derived name, so the RELEASE write
    // replaces DEBUG. Level.MEDIUM has its own name. Result: 2 slots.
    assertEquals(2, state.stream().count());

  }

  /// State#state(Enum) adds an enum-derived Name slot.
  @Test
  void state_enumValue_addsNameSlot() {

    final var state = cortex.state()
      .state(TestMode.DEBUG);

    final var enumName = cortex.name(TestMode.DEBUG.getDeclaringClass());
    final var slot = cortex.slot(enumName, cortex.name("fallback"));

    assertEquals(cortex.name(TestMode.DEBUG), state.value(slot));

  }

  /// State#state(Enum) derives the slot name from the enum type.
  @Test
  void state_enumValue_derivesNameFromEnumType() {

    final var state = cortex.state()
      .state(Level.HIGH);

    final var expectedName = cortex.name(Level.class);

    assertTrue(
      state.stream()
        .anyMatch(s ->
          s.name().equals(expectedName) &&
            s.value().equals(cortex.name(Level.HIGH)) &&
            s.type().equals(Name.class)
        )
    );

  }

  /// An enum write upserts its matching Name slot.
  @Test
  void state_enumValue_overridesMatchingNameSlot() {

    final var enumName = cortex.name(Level.LOW.getDeclaringClass());

    final var state = cortex.state()
      .state(Level.LOW)
      .state(enumName, cortex.name("CUSTOM_LOW"));

    final var slot = cortex.slot(enumName, cortex.name("fallback"));

    assertEquals(cortex.name("CUSTOM_LOW"), state.value(slot));

  }

  /// State#state(Enum) stores the enum as a Name value.
  @Test
  void state_enumValue_usesNameValueType() {

    final var state = cortex.state()
      .state(Level.MEDIUM);

    final var slot = state.stream()
      .findFirst()
      .orElseThrow();

    assertEquals(Name.class, slot.type());
    assertFalse(slot.type().isPrimitive());

  }

  /// State#state(Enum) supports persistent chaining.
  @Test
  void state_enumValues_supportsChaining() {

    final var state = cortex.state()
      .state(TestMode.DEBUG)
      .state(Level.MEDIUM)
      .state(TestMode.RELEASE);

    // DEBUG and RELEASE share the TestMode-derived name, so the RELEASE write
    // replaces DEBUG. Level.MEDIUM has its own name. Result: 2 slots.
    assertEquals(2, state.stream().count());

  }

  /// An equivalent enum-derived Slot returns the same State.
  @Test
  void state_equivalentEnumSlot_returnsSameInstance() {

    final var enumSlot = cortex.slot(TestMode.DEBUG);

    final var state1 = cortex.state()
      .state(enumSlot);

    final var state2 = state1.state(enumSlot);

    assertSame(state1, state2);

  }

  /// An equivalent enum write returns the same State instance.
  @Test
  void state_equivalentEnumWrite_returnsSameInstance() {

    final var state1 = cortex.state()
      .state(TestMode.PRODUCTION);

    final var state2 = state1.state(TestMode.PRODUCTION);

    assertSame(state1, state2);

  }

  /// Insertion of an equivalent Slot may return the same State.
  @SpecRef("8.1")
  @Test
  void state_equivalentSlot_returnsSameInstance() {

    final var counter = cortex.name("state.slot.idempotent");
    final var slot = cortex.slot(counter, 100);

    final var state = cortex.state().state(slot);
    final var unchanged = state.state(slot);

    assertSame(state, unchanged, "Adding same slot should return same state instance");

  }

  /// Validates idempotent update: writing a slot with identical (name, type, value)
  /// returns the same state instance, regardless of where the existing slot sits in
  /// the chain. Writing the same name with a different value produces a new state.
  /// An equivalent write may return the same State instance.
  @SpecRef("8.1")
  @Test
  void state_equivalentWrite_returnsSameInstance() {

    final var counter = cortex.name("state.idempotent.counter");

    final var base = cortex.state();
    final var withValue = base.state(counter, 5);
    final var unchanged = withValue.state(counter, 5);
    final var updated = withValue.state(counter, 6);

    assertSame(withValue, unchanged);
    assertNotSame(withValue, updated);
    assertEquals(6, updated.value(cortex.slot(counter, 0)));

  }

  /// Float and Double State factories preserve IEEE 754 precision, extrema,
  /// signed zero, infinities, and NaN.
  @SpecRef("8.3")
  @Test
  void state_floatingPointBoundaries_preserveIeeePrecisionAndRange() {

    final float[] floats = {
      Float.MIN_VALUE,
      -Float.MAX_VALUE,
      Math.nextUp(1.0f),
      Float.MAX_VALUE,
      -0.0f,
      Float.NEGATIVE_INFINITY,
      Float.POSITIVE_INFINITY,
      Float.NaN
    };
    final double[] doubles = {
      Double.MIN_VALUE,
      -Double.MAX_VALUE,
      Math.nextUp(1.0d),
      Double.MAX_VALUE,
      -0.0d,
      Double.NEGATIVE_INFINITY,
      Double.POSITIVE_INFINITY,
      Double.NaN
    };

    State state = cortex.state();
    final Name[] floatNames = new Name[floats.length];
    final Name[] doubleNames = new Name[doubles.length];

    for (int index = 0; index < floats.length; index++) {
      floatNames[index] = cortex.name("state.range.float." + index);
      doubleNames[index] = cortex.name("state.range.double." + index);
      state = state.state(floatNames[index], floats[index]);
      state = state.state(doubleNames[index], doubles[index]);
    }

    for (int index = 0; index < floats.length; index++) {
      final float actualFloat = state.value(cortex.slot(floatNames[index], 0.0f));
      final double actualDouble = state.value(cortex.slot(doubleNames[index], 0.0d));

      assertEquals(
        Float.floatToRawIntBits(floats[index]),
        Float.floatToRawIntBits(actualFloat)
      );
      assertEquals(
        Double.doubleToRawLongBits(doubles[index]),
        Double.doubleToRawLongBits(actualDouble)
      );
    }

  }

  /// Integer and Long State factories preserve their complete signed ranges.
  @SpecRef("8.3")
  @Test
  void state_integerBoundaries_preserveSignedRanges() {

    final var intMinimum = cortex.name("state.range.int.minimum");
    final var intMaximum = cortex.name("state.range.int.maximum");
    final var longMinimum = cortex.name("state.range.long.minimum");
    final var longMaximum = cortex.name("state.range.long.maximum");

    final var state = cortex.state()
      .state(intMinimum, Integer.MIN_VALUE)
      .state(intMaximum, Integer.MAX_VALUE)
      .state(longMinimum, Long.MIN_VALUE)
      .state(longMaximum, Long.MAX_VALUE);

    assertEquals(Integer.MIN_VALUE, state.value(cortex.slot(intMinimum, 0)));
    assertEquals(Integer.MAX_VALUE, state.value(cortex.slot(intMaximum, 0)));
    assertEquals(Long.MIN_VALUE, state.value(cortex.slot(longMinimum, 0L)));
    assertEquals(Long.MAX_VALUE, state.value(cortex.slot(longMaximum, 0L)));

  }

  /// Interleaved upserts retain the latest value for each key.
  @SpecRef("8.1")
  @Test
  void state_interleavedRepeatWrites_retainsLatestValues() {

    final var alpha = cortex.name("state.interleaved.alpha");
    final var beta = cortex.name("state.interleaved.beta");
    final var gamma = cortex.name("state.interleaved.gamma");

    final var state = cortex.state()
      .state(alpha, 1)
      .state(beta, 10)
      .state(alpha, 2)
      .state(gamma, 3);

    assertEquals(3, state.stream().count());
    assertEquals(2, state.value(cortex.slot(alpha, 0)));
    assertEquals(10, state.value(cortex.slot(beta, 0)));
    assertEquals(3, state.value(cortex.slot(gamma, 0)));

  }

  /// Multiple enum-derived Slots retain distinct names.
  @Test
  void state_multipleEnumSlots_retainsDistinctNames() {

    final var modeSlot = cortex.slot(TestMode.PRODUCTION);
    final var levelSlot = cortex.slot(Level.HIGH);

    final var state = cortex.state()
      .state(modeSlot)
      .state(levelSlot);

    assertEquals(cortex.name(TestMode.PRODUCTION), state.value(modeSlot));
    assertEquals(cortex.name(Level.HIGH), state.value(levelSlot));

  }

  /// State#state(Enum) rejects null.
  @Test
  void state_nullEnumValue_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> cortex.state().state((Enum< ? >) null)
    );

  }

  /// State factories reject absent references.
  @SpecRef("15.2")
  @Test
  void state_nullReferenceArguments_throwNullPointerException() {

    final var name = cortex.name("state.null.guard");

    assertThrows(
      NullPointerException.class,
      () -> cortex.state().state(null, 1)
    );

    assertThrows(
      NullPointerException.class,
      () -> cortex.state().state(name, (String) null)
    );

    assertThrows(
      NullPointerException.class,
      () -> cortex.state().state(null, cortex.state())
    );

    assertThrows(
      NullPointerException.class,
      () -> cortex.state().state(
        name,
        (Name) null
      )
    );

    assertThrows(
      NullPointerException.class,
      () -> cortex.state().state(
        name,
        (State) null
      )
    );

  }

  /// State#state(Slot) rejects absence.
  @SpecRef("15.2")
  @Test
  void state_nullSlot_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> cortex.state().state((Slot< ? >) null)
    );

  }

  /// State supports every required primitive numeric and Boolean type.
  @SpecRef("8.3")
  @Test
  void state_primitiveTypeFactories_storeRequiredValues() {

    final var intName = cortex.name("state.primitives.int");
    final var longName = cortex.name("state.primitives.long");
    final var floatName = cortex.name("state.primitives.float");
    final var doubleName = cortex.name("state.primitives.double");
    final var boolName = cortex.name("state.primitives.bool");

    final var state = cortex.state()
      .state(intName, 42)
      .state(longName, 123456789L)
      .state(floatName, 3.14f)
      .state(doubleName, 2.718281828)
      .state(boolName, true);

    assertEquals(42, state.value(cortex.slot(intName, 0)));
    assertEquals(123456789L, state.value(cortex.slot(longName, 0L)));
    assertEquals(3.14f, state.value(cortex.slot(floatName, 0f)), 0.001f);
    assertEquals(2.718281828, state.value(cortex.slot(doubleName, 0.0)), 0.00001);
    assertTrue(state.value(cortex.slot(boolName, false)));

  }

  /// State supports every required reference value type.
  @SpecRef("8.3")
  @Test
  void state_referenceTypeFactories_storeRequiredValues() {

    final var nameKey = cortex.name("state.factory.name");
    final var stateKey = cortex.name("state.factory.state");

    final var storedName = cortex.name("state.factory.stored");
    final var storedState = cortex.state().state(cortex.name("state.factory.nested"), 123);

    final var nameState = cortex.state().state(nameKey, storedName);
    final var nestedState = cortex.state().state(stateKey, storedState);

    assertEquals(
      storedName,
      nameState.value(cortex.slot(nameKey, cortex.name("fallback")))
    );

    assertEquals(
      storedState,
      nestedState.value(cortex.slot(stateKey, cortex.state()))
    );

  }

  /// Repeated enum-derived Slots collapse to one key.
  @Test
  void state_repeatedEnumSlot_collapsesToSingleSlot() {

    final var debugSlot = cortex.slot(TestMode.DEBUG);
    final var releaseSlot = cortex.slot(TestMode.RELEASE);
    final var levelSlot = cortex.slot(Level.LOW);

    final var state = cortex.state()
      .state(debugSlot)
      .state(levelSlot)
      .state(releaseSlot)
      .state(debugSlot);

    // DEBUG, RELEASE, DEBUG share the TestMode-derived name and collapse to a
    // single slot (final value = DEBUG). Level.LOW has a different name and
    // is kept. Result: 2 slots.
    assertEquals(2, state.stream().count());

  }

  /// Repeated enum writes collapse to one Name slot.
  @Test
  void state_repeatedEnumWrite_collapsesToSingleSlot() {

    final var state = cortex.state()
      .state(TestMode.DEBUG)
      .state(Level.MEDIUM)
      .state(TestMode.DEBUG);

    assertEquals(2, state.stream().count());

  }

  /// An upsert leaves at most one slot per name and type pair.
  @SpecRef("8.1")
  @Test
  void state_repeatedMatchingWrite_collapsesToSingleSlot() {

    final var counter = cortex.name("state.repeat.counter");

    final var state = cortex.state()
      .state(counter, 1)
      .state(counter, 2)
      .state(counter, 3)
      .state(counter, 4)
      .state(counter, 5);

    assertEquals(1, state.stream().count());
    assertEquals(5, state.value(cortex.slot(counter, 0)));

  }

  /// Validates that upsert moves the rewritten slot to the head of iteration,
  /// preserving the "most-recently-written first" invariant of the iteration
  /// order. The intermediate write to `beta` does not prevent `alpha` from
  /// becoming the head after it is rewritten.
  /// An upsert moves the matching slot to the most-recent position.
  @SpecRef("8.1")
  @Test
  void state_repeatedMatchingWrite_movesSlotToMostRecentPosition() {

    final var alpha = cortex.name("state.head.alpha");
    final var beta = cortex.name("state.head.beta");

    final var state = cortex.state()
      .state(alpha, 1)
      .state(beta, 2)
      .state(alpha, 99);

    final var names = state.stream()
      .map(slot -> slot.name().path().toString())
      .toList();

    assertEquals(
      List.of("state.head.alpha", "state.head.beta"),
      names
    );

    assertEquals(2, state.stream().count());
    assertEquals(99, state.value(cortex.slot(alpha, 0)));
    assertEquals(2, state.value(cortex.slot(beta, 0)));

  }

  /// Validates that repeated writes to the same (name, type) replace in place.
  ///
  /// Under upsert semantics, writing `alpha`, `beta`, `gamma`, `beta`, `alpha`
  /// produces a state with exactly three slots — the two re-writes replace the
  /// earlier entries rather than shadowing them. The retained values are the
  /// most recent writes (alpha=10, beta=20, gamma=3).
  /// An upsert replaces the matching slot with its latest value.
  @SpecRef("8.1")
  @Test
  void state_repeatedMatchingWrite_replacesWithLatestValue() {

    final var alpha = cortex.name("state.order.alpha");
    final var beta = cortex.name("state.order.beta");
    final var gamma = cortex.name("state.order.gamma");

    final var state = cortex.state()
      .state(alpha, 1)
      .state(beta, 2)
      .state(gamma, 3)
      .state(beta, 20)
      .state(alpha, 10);

    assertEquals(3, state.stream().count());
    assertEquals(10, state.value(cortex.slot(alpha, 0)));
    assertEquals(20, state.value(cortex.slot(beta, 0)));
    assertEquals(3, state.value(cortex.slot(gamma, 0)));

  }

  /// Repeated matching Slot writes collapse to one entry.
  @SpecRef("8.1")
  @Test
  void state_repeatedSlotWrite_collapsesToSingleSlot() {

    final var key = cortex.name("state.slot.repeat");

    final var slot1 = cortex.slot(key, 1);
    final var slot2 = cortex.slot(key, 2);
    final var slot3 = cortex.slot(key, 3);

    final var state = cortex.state()
      .state(slot1)
      .state(slot2)
      .state(slot3)
      .state(slot1);

    assertEquals(1, state.stream().count());
    assertEquals(1, state.value(cortex.slot(key, 0)));

  }

  /// Validates that slots with the same name but different types coexist and
  /// are not collapsed by upsert — upsert matches on (name, type), so an int
  /// and a float with the same name are independent entries.
  /// Equal names with different types are distinct keys.
  @SpecRef({"8.1", "8.2"})
  @Test
  void state_sameNameDifferentTypes_retainsBothSlots() {

    final var value = cortex.name("state.typed.value");

    final var state = cortex.state()
      .state(value, 1)
      .state(value, 2)
      .state(value, 1.0f)
      .state(value, 2.0f);

    assertEquals(2, state.stream().count());
    assertEquals(2, state.value(cortex.slot(value, 0)));
    assertEquals(2.0f, state.value(cortex.slot(value, 0f)), 0.001f);

  }

  /// State#state(Slot) stores the provided Slot.
  @Test
  void state_slotParameter_storesProvidedSlot() {

    final var name = cortex.name("state.slot.name");
    final var age = cortex.name("state.slot.age");

    final var nameSlot = cortex.slot(name, "Alice");
    final var ageSlot = cortex.slot(age, 30);

    final var state = cortex.state()
      .state(nameSlot)
      .state(ageSlot);

    assertEquals("Alice", state.value(cortex.slot(name, "")));
    assertEquals(30, state.value(cortex.slot(age, 0)));

  }

  /// Inserting a Slot with a matching key upserts its value.
  @SpecRef("8.1")
  @Test
  void state_slotWithMatchingKey_overridesExistingValue() {

    final var key = cortex.name("state.slot.override");

    final var slot1 = cortex.slot(key, 10);
    final var slot2 = cortex.slot(key, 20);

    final var state = cortex.state()
      .state(slot1)
      .state(slot2);

    assertEquals(1, state.stream().count());
    assertEquals(20, state.value(cortex.slot(key, 0)), "Most recent slot value should be used");

  }

  /// State retains slots across mixed required value types.
  @SpecRef({"8.1", "8.3"})
  @Test
  void state_slotsWithMixedTypes_retainsEverySlot() {

    final var name = cortex.name("state.slot.mixed");

    final var intSlot = cortex.slot(name, 42);
    final var stringSlot = cortex.slot(name, "test");

    final var state = cortex.state()
      .state(intSlot)
      .state(stringSlot);

    // Different types on same name should coexist
    assertEquals(42, state.value(cortex.slot(name, 0)));
    assertEquals("test", state.value(cortex.slot(name, "")));

  }

  /// Validates that upsert preserves the persistent data structure contract:
  /// writing to a derived state does not mutate any ancestor, and ancestors
  /// remain observable with their original contents even after a slot that
  /// they held is overwritten in a descendant.
  /// Descendant upserts leave every ancestor State unchanged.
  @SpecRef("8.1")
  @Test
  void state_upsertInDescendant_leavesAncestorStatesUnchanged() {

    final var alpha = cortex.name("state.upsert.alpha");
    final var beta = cortex.name("state.upsert.beta");

    final var base = cortex.state()
      .state(alpha, 1)
      .state(beta, 2);

    final var derived = base.state(alpha, 99);

    assertEquals(2, base.stream().count());
    assertEquals(1, base.value(cortex.slot(alpha, 0)));
    assertEquals(2, base.value(cortex.slot(beta, 0)));

    assertEquals(2, derived.stream().count());
    assertEquals(99, derived.value(cortex.slot(alpha, 0)));
    assertEquals(2, derived.value(cortex.slot(beta, 0)));

  }

  /// State#stream traverses most-recent slot first.
  @SpecRef("8.1")
  @Test
  void stream_populatedState_traversesMostRecentFirst() {

    final var first = cortex.name("state.order.first");
    final var second = cortex.name("state.order.second");

    final var state = cortex.state()
      .state(first, 1)
      .state(second, 2);

    final var order = state.stream()
      .map(slot -> slot.name().path().toString())
      .toList();

    assertEquals(
      List.of("state.order.second", "state.order.first"),
      order
    );

  }

  /// State#value(Slot) returns its template fallback when absent.
  @Test
  void value_absentMatch_returnsTemplateFallback() {

    final var empty = cortex.state();
    final var missing = cortex.name("state.missing.key");

    assertEquals(999, empty.value(cortex.slot(missing, 999)));
    assertEquals("default", empty.value(cortex.slot(missing, "default")));

  }

  /// Enum-derived Slot lookup returns the stored Name.
  @Test
  void value_enumSlotTemplate_returnsStoredName() {

    final var template = cortex.slot(Level.LOW);

    final var state = cortex.state()
      .state(template);

    assertEquals(cortex.name(Level.LOW), state.value(template));

  }

  /// Enum template lookup returns the stored Name.
  @Test
  void value_enumTemplate_returnsStoredName() {

    final var state = cortex.state()
      .state(Level.HIGH);

    final var template = cortex.slot(cortex.name(Level.class), cortex.name("fallback"));

    assertEquals(cortex.name(Level.HIGH), state.value(template));

  }

  /// Lookup returns every required reference type.
  @SpecRef("8.3")
  @Test
  void value_referenceTypes_returnsStoredValues() {

    final var stringName = cortex.name("state.ref.string");
    final var nameName = cortex.name("state.ref.name");
    final var stateName = cortex.name("state.ref.state");

    final var nestedName = cortex.name("nested.value");
    final var nestedState = cortex.state().state(nestedName, 99);

    final var state = cortex.state()
      .state(stringName, "hello")
      .state(nameName, nestedName)
      .state(stateName, nestedState);

    assertEquals("hello", state.value(cortex.slot(stringName, "")));
    assertEquals(nestedName, state.value(cortex.slot(nameName, cortex.name("default"))));
    assertEquals(nestedState, state.value(cortex.slot(stateName, cortex.state())));

  }

  /// Lookup selects a slot using both name and type.
  @SpecRef("8.2")
  @Test
  void value_sameNameDifferentTypes_selectsByType() {

    final var counter = cortex.name("state.typed.counter");

    final var state = cortex.state()
      .state(counter, 10)
      .state(counter, 20L)
      .state(counter, 30.0f);

    assertEquals(10, state.value(cortex.slot(counter, 0)));
    assertEquals(20L, state.value(cortex.slot(counter, 0L)));
    assertEquals(30.0f, state.value(cortex.slot(counter, 0f)), 0.001f);

    assertEquals(3, state.stream().count());

  }

  enum TestMode {
    DEBUG,
    RELEASE,
    PRODUCTION
  }

  enum Level {
    LOW,
    MEDIUM,
    HIGH
  }

}
