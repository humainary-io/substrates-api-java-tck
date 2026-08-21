// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import io.humainary.substrates.api.Substrates.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §4.4 Extent and tests for the Java projection's [Extent] traversal
/// conveniences described by Appendix A.2.
/// @author William David Louth
/// @since 1.0

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.2/SPEC.md")
final class ExtentContractTest {

  // ===========================
  // Test Implementation
  // ===========================

  // ===========================
  // Basic Extent Tests
  // ===========================

  /// Depth counts every level from the extent through its root.
  @SpecRef("4.4")
  @Test
  void depth_nestedExtent_returnsEnclosureCountIncludingSelf() {

    final var root = TestExtent.root("a");
    final var b = root.child("b");
    final var c = b.child("c");
    final var d = c.child("d");

    assertEquals(1, root.depth());
    assertEquals(2, b.depth());
    assertEquals(3, c.depth());
    assertEquals(4, d.depth());

  }

  /// Depth includes the root extent itself.
  @SpecRef("4.4")
  @Test
  void depth_root_returnsOne() {

    final var root = TestExtent.root("root");
    assertEquals(1, root.depth());

  }

  // ===========================
  // Depth Tests
  // ===========================

  /// A child exposes its immediate enclosing extent.
  @SpecRef("4.4")
  @Test
  void enclosure_child_returnsImmediateEnclosure() {

    final var root = TestExtent.root("root");
    final var child = root.child("child");

    final var enclosure = child.enclosure();
    assertTrue(enclosure.isPresent());
    assertSame(root, enclosure.get());

  }

  /// Extent#enclosure(Consumer) invokes once for one enclosure.
  @Test
  void enclosure_consumerOnChild_isCalledOnce() {

    final var root = TestExtent.root("root");
    final var child = root.child("child");

    final var counter = new AtomicInteger(0);
    child.enclosure(_ -> counter.incrementAndGet());

    assertEquals(1, counter.get());

  }

  // ===========================
  // Extremity Tests
  // ===========================

  /// Extent#enclosure(Consumer) receives the immediate enclosure.
  @Test
  void enclosure_consumerOnChild_receivesImmediateEnclosure() {

    final var root = TestExtent.root("root");
    final var child = root.child("child");
    final var result = new String[1];

    child.enclosure(parent -> result[0] = parent.part());

    assertEquals("root", result[0]);

  }

  /// Extent#enclosure(Consumer) does not invoke for a root.
  @Test
  void enclosure_consumerOnRoot_isNotCalled() {

    final var root = TestExtent.root("root");
    final var called = new boolean[1];

    root.enclosure(_ -> called[0] = true);

    assertFalse(called[0]);

  }

  /// The root has no enclosure.
  @SpecRef("4.4")
  @Test
  void enclosure_root_returnsEmpty() {

    final var root = TestExtent.root("root");
    assertTrue(root.enclosure().isEmpty());

  }

  // ===========================
  // Iterator Tests
  // ===========================

  /// Extent#extent preserves an empty projection-specific part.
  @Test
  void extent_emptyPart_preservesPart() {

    final var root = TestExtent.root("");
    assertEquals("", root.part());
    assertEquals("", root.path().toString());

  }

  /// Extent#extent returns the concrete receiver.
  @Test
  void extent_receiver_returnsSelf() {

    final var extent = TestExtent.root("test");
    assertSame(extent, extent.extent());

  }

  /// Extent#extent preserves a projection-specific part verbatim.
  @Test
  void extent_specialCharacterPart_preservesPart() {

    final var root = TestExtent.root("test-name_123");
    final var child = root.child("child@#$");

    assertEquals("test-name_123/child@#$", child.path().toString());

  }

  /// Extremity returns the outermost root.
  @SpecRef("4.4")
  @Test
  void extremity_child_returnsRoot() {

    final var root = TestExtent.root("root");
    final var child = root.child("child");
    final var grandchild = child.child("grandchild");

    assertSame(root, grandchild.extremity());
    assertSame(root, child.extremity());
    assertSame(root, root.extremity());

  }

  // ===========================
  // Fold Tests
  // ===========================

  /// Extremity traverses an arbitrarily deep enclosure chain.
  @SpecRef("4.4")
  @Test
  void extremity_deepHierarchy_returnsRoot() {

    var current = TestExtent.root("level0");

    for (var i = 1; i <= 10; i++) {
      current = current.child("level" + i);
    }

    final var root = current.extremity();
    assertEquals("level0", root.part());

  }

  /// A root is its own extremity.
  @SpecRef("4.4")
  @Test
  void extremity_root_returnsSelf() {

    final var root = TestExtent.root("root");
    assertSame(root, root.extremity());

  }

  /// Validates contrasting traversal directions: fold (right-to-left) vs foldTo (left-to-right).
  ///
  /// This is THE critical test for understanding Extent traversal semantics. It demonstrates
  /// that fold and foldTo traverse the hierarchy in OPPOSITE directions but can produce the
  /// SAME result when the accumulator function is adjusted appropriately. This bidirectional
  /// capability enables efficient operations regardless of natural data flow direction.
  ///
  /// Hierarchy Structure:
  /// ```
  /// first (root) → second → third (current extent)
  /// ```
  ///
  /// fold Traversal (right-to-left):
  /// ```
  /// Step 1: initializer(third)  → acc = "third"
  /// Step 2: accumulator(acc="third", second) → acc = "second.third"
  /// Step 3: accumulator(acc="second.third", first) → acc = "first.second.third"
  /// Result: "first.second.third"
  /// ```
  ///
  /// foldTo Traversal (left-to-right):
  /// ```
  /// Step 1: initializer(first)  → acc = "first"
  /// Step 2: accumulator(acc="first", second) → acc = "first.second"
  /// Step 3: accumulator(acc="first.second", third) → acc = "first.second.third"
  /// Result: "first.second.third"
  /// ```
  ///
  /// Key Insight - Symmetric Operations:
  /// Both operations reach the SAME result through OPPOSITE traversal:
  /// - **fold**: Right-to-left, accumulator prepends: `e.part() + "." + acc`
  /// - **foldTo**: Left-to-right, accumulator appends: `acc + "." + e.part()`
  ///
  /// This symmetry enables choosing the most efficient direction:
  /// - **fold** preferred when: natural recursion, tail-call optimization, immediate parent access
  /// - **foldTo** preferred when: left-associative operations, streaming, progressive computation
  ///
  /// Real-world examples:
  ///
  /// Path construction (naturally left-to-right):
  /// ```java
  /// String path = extent.foldTo(
  ///   e -> e.part(),
  ///   (acc, e) -> acc + "/" + e.part()
  /// );
  /// // Produces: "root/parent/child"
  /// ```
  ///
  /// Type inference (naturally right-to-left):
  /// ```java
  /// Type resolvedType = extent.fold(
  ///   e -> e.localType(),
  ///   (childType, parent) -> parent.resolveType(childType)
  /// );
  /// // Resolves from most specific (child) to general (root)
  /// ```
  ///
  /// Why bidirectional matters:
  /// - **Performance**: Choose traversal matching data dependency direction
  /// - **Expressiveness**: Write accumulator naturally for problem domain
  /// - **Composition**: Combine fold/foldTo operations in same hierarchy
  /// - **Flexibility**: Same abstraction for opposite traversal needs
  ///
  /// Critical behaviors verified:
  /// - fold with prepend accumulator produces correct result
  /// - foldTo with append accumulator produces correct result
  /// - Both produce IDENTICAL final result ("first.second.third")
  /// - Symmetry: direction + accumulator strategy = same output
  ///
  /// Expected: Both fold and foldTo produce "first.second.third"
  /// Extent#fold and Extent#foldTo expose opposite traversal orders.
  @Test
  void foldAndFoldTo_oppositeTraversal_produceEquivalentPath() {

    final var a = TestExtent.root("first");
    final var b = a.child("second");
    final var c = b.child("third");

    // fold builds right-to-left
    final var foldResult = c.fold(
      TestExtent::part,
      (acc, e) -> e.part() + "." + acc
    );

    // foldTo builds left-to-right
    final var foldToResult = c.foldTo(
      TestExtent::part,
      (acc, e) -> acc + "." + e.part()
    );

    assertEquals(foldResult, foldToResult);
    assertEquals("first.second.third", foldResult);

  }

  /// Extent#foldTo traverses from root toward receiver.
  @Test
  void foldTo_nestedExtent_visitsRootToSelf() {

    final var a = TestExtent.root("a");
    final var b = a.child("b");
    final var c = b.child("c");

    // foldTo goes from left (a) to right (c)
    final var result = c.foldTo(
      TestExtent::part,
      (acc, e) -> acc + "." + e.part()
    );

    assertEquals("a.b.c", result);

  }

  // ===========================
  // FoldTo Tests
  // ===========================

  /// Extent#foldTo rejects a null accumulator.
  @Test
  void foldTo_nullAccumulator_throwsNullPointerException() {

    final var extent = TestExtent.root("test");

    assertThrows(
      NullPointerException.class,
      () -> extent.foldTo(_ -> 1, null)
    );

  }

  /// Extent#foldTo rejects a null initializer.
  @Test
  void foldTo_nullInitializer_throwsNullPointerException() {

    final var extent = TestExtent.root("test");

    assertThrows(
      NullPointerException.class,
      () -> extent.foldTo(null, (acc, _) -> acc)
    );

  }

  /// Extent#foldTo initializes correctly at a root.
  @Test
  void foldTo_root_appliesInitializerOnly() {

    final var root = TestExtent.root("single");

    final var result = root.foldTo(
      TestExtent::part,
      (acc, e) -> acc + "." + e.part()
    );

    assertEquals("single", result);

  }

  /// Extent#foldTo applies the supplied transformation.
  @Test
  void foldTo_transformingAccumulator_returnsTransformedResult() {

    final var root = TestExtent.root("one");
    final var child = root.child("two");

    final var result = child.foldTo(
      e -> e.part().toUpperCase(),
      (acc, e) -> acc + "-" + e.part().toUpperCase()
    );

    assertEquals("ONE-TWO", result);

  }

  // ===========================
  // Path Tests
  // ===========================

  /// Extent#fold accumulates across the enclosure chain.
  @Test
  void fold_nestedExtent_accumulatesFromSelfToRoot() {

    final var root = TestExtent.root("10");
    final var child = root.child("20");
    final var grandchild = child.child("30");

    final var sum = grandchild.fold(
      e -> Integer.parseInt(e.part()),
      (acc, e) -> acc + Integer.parseInt(e.part())
    );

    assertEquals(60, sum);

  }

  /// Extent#fold visits every level once.
  @Test
  void fold_nestedExtent_countsAllLevels() {

    final var root = TestExtent.root("a");
    final var child = root.child("b");
    final var grandchild = child.child("c");

    final var count = grandchild.fold(
      _ -> 1,
      (acc, _) -> acc + 1
    );

    assertEquals(3, count);

  }

  /// Extent#fold traverses from receiver toward root.
  @Test
  void fold_nestedExtent_visitsSelfToRoot() {

    final var a = TestExtent.root("a");
    final var b = a.child("b");
    final var c = b.child("c");

    // fold goes from right (c) to left (a)
    final var result = c.fold(
      TestExtent::part,
      (acc, e) -> e.part() + "/" + acc
    );

    assertEquals("a/b/c", result);

  }

  /// Extent#fold rejects a null accumulator.
  @Test
  void fold_nullAccumulator_throwsNullPointerException() {

    final var extent = TestExtent.root("test");

    assertThrows(
      NullPointerException.class,
      () -> extent.fold(_ -> 1, null)
    );

  }

  /// Extent#fold rejects a null initializer.
  @Test
  void fold_nullInitializer_throwsNullPointerException() {

    final var extent = TestExtent.root("test");

    assertThrows(
      NullPointerException.class,
      () -> extent.fold(null, (acc, _) -> acc)
    );

  }

  /// Extent#fold initializes correctly at a root.
  @Test
  void fold_root_appliesInitializerOnly() {

    final var root = TestExtent.root("test");

    final var result = root.fold(
      TestExtent::part,
      (acc, e) -> acc + "/" + e.part()
    );

    assertEquals("test", result);

  }

  // ===========================
  // Stream Tests
  // ===========================

  /// An exhausted Extent iterator signals NoSuchElementException.
  @Test
  void iterator_exhausted_throwsNoSuchElementException() {

    final var root = TestExtent.root("root");
    final var iterator = root.iterator();

    iterator.next(); // consume the only element

    assertThrows(NoSuchElementException.class, iterator::next);

  }

  /// Validates iterator traversal order: right-to-left (child to root).
  ///
  /// This test verifies the fundamental iteration direction of Extent hierarchies:
  /// iteration proceeds from the current extent (rightmost/leaf) toward the root
  /// (leftmost/extremity). This right-to-left traversal is consistent across all
  /// Extent traversal operations (iterator, fold, stream).
  ///
  /// Hierarchy Structure:
  /// ```
  /// a (root/extremity) → b → c (current extent)
  /// ```
  ///
  /// Iterator Traversal Order:
  /// ```
  /// iterator.next() → c (start at current extent)
  /// iterator.next() → b (move toward root)
  /// iterator.next() → a (reach root/extremity)
  /// iterator.hasNext() → false (exhausted)
  /// ```
  ///
  /// Why right-to-left matters:
  /// - **Natural path construction**: Build paths from most specific to least specific
  /// - **Efficient enclosure access**: Immediate parent always first
  /// - **Consistent with fold**: Both iterate in same direction (right-to-left)
  /// - **Memory locality**: Access recently created (child) extents first
  ///
  /// Real-world analogy:
  /// File path "/home/user/documents/file.txt":
  /// - Iterator starts at "file.txt" (most specific)
  /// - Moves to "documents", then "user", then "home"
  /// - Reaches "/" (root/extremity)
  ///
  /// Usage Pattern:
  /// ```java
  /// // Find first ancestor matching predicate
  /// for (Extent<E> extent : hierarchy) {
  ///   if (predicate.test(extent)) {
  ///     return extent;  // Found closest match
  ///   }
  /// }
  /// ```
  ///
  /// Critical behaviors verified:
  /// - First element is current extent (c)
  /// - Second element is immediate parent (b)
  /// - Last element is root (a)
  /// - Iterator exhausted after root (hasNext = false)
  ///
  /// Expected: Traversal order `[c, b, a]` (right-to-left)
  /// Extent#iterator traverses from receiver toward root.
  @Test
  void iterator_nestedExtent_visitsSelfToRoot() {

    final var a = TestExtent.root("a");
    final var b = a.child("b");
    final var c = b.child("c");

    final var iterator = c.iterator();

    assertTrue(iterator.hasNext());
    assertSame(c, iterator.next());

    assertTrue(iterator.hasNext());
    assertSame(b, iterator.next());

    assertTrue(iterator.hasNext());
    assertSame(a, iterator.next());

    assertFalse(iterator.hasNext());

  }

  /// Extent#iterator creates independent traversals.
  @Test
  void iterator_repeatedTraversal_returnsIndependentIterators() {

    final var root = TestExtent.root("a");
    final var child = root.child("b");

    // First iteration
    final var values1 = new ArrayList< String >();
    child.iterator().forEachRemaining(
      e -> values1.add(e.part())
    );

    // Second iteration
    final var values2 = new ArrayList< String >();
    child.iterator().forEachRemaining(
      e -> values2.add(e.part())
    );

    assertEquals(values1, values2);
    assertEquals(List.of("b", "a"), values1);

  }

  /// Extent#iterator visits a root exactly once.
  @Test
  void iterator_root_visitsRootOnce() {

    final var root = TestExtent.root("root");
    final var iterator = root.iterator();

    assertTrue(iterator.hasNext());
    assertSame(root, iterator.next());
    assertFalse(iterator.hasNext());

  }

  /// Extent#iterator excludes sibling extents.
  @Test
  void iterator_siblingExtents_visitsOnlySelectedEnclosureChain() {

    final var root = TestExtent.root("root");
    final var child1 = root.child("child1");
    final var child2 = root.child("child2");

    // child1's iterator should only include child1 and root
    final var values1 = child1.stream()
      .map(TestExtent::part)
      .toList();

    assertEquals(List.of("child1", "root"), values1);

    // child2's iterator should only include child2 and root
    final var values2 = child2.stream()
      .map(TestExtent::part)
      .toList();

    assertEquals(List.of("child2", "root"), values2);

  }

  // ===========================
  // CompareTo Tests
  // ===========================

  /// Extent#path(char) joins parts with the supplied character.
  @Test
  void path_characterSeparator_joinsPartsWithCharacter() {

    final var root = TestExtent.root("x");
    final var child = root.child("y");

    assertEquals("x-y", child.path('-').toString());

  }

  /// Non-name extents use slash as the default path separator.
  @SpecRef("4.4")
  @Test
  void path_defaultSeparator_joinsPartsWithSlash() {

    final var a = TestExtent.root("a");
    final var b = a.child("b");
    final var c = b.child("c");

    assertEquals("a/b/c", c.path().toString());

  }

  /// Extent#path(Function,String) maps and joins every part.
  @Test
  void path_mapperAndStringSeparator_transformsAndJoinsParts() {

    final var a = TestExtent.root("foo");
    final var b = a.child("bar");

    final var result = b.path(
      e -> e.part().toUpperCase(),
      " -> "
    );

    assertEquals("FOO -> BAR", result.toString());

  }

  /// Extent#path(Function) maps each part.
  @Test
  void path_mapper_transformsEachPart() {

    final var root = TestExtent.root("hello");
    final var child = root.child("world");

    final var result = child.path(
      e -> e.part().toUpperCase(),
      '/'
    );

    assertEquals("HELLO/WORLD", result.toString());

  }

  // ===========================
  // Within Tests
  // ===========================

  /// Extent#path(Function,String) rejects a null mapper.
  @Test
  void path_nullMapperWithStringSeparator_throwsNullPointerException() {

    final var extent = TestExtent.root("test");

    assertThrows(
      NullPointerException.class,
      () -> extent.path(null, "::")
    );

    assertThrows(
      NullPointerException.class,
      () -> extent.path(Extent::part, null)
    );

  }

  /// Extent#path(Function) rejects a null mapper.
  @Test
  void path_nullMapper_throwsNullPointerException() {

    final var extent = TestExtent.root("test");

    assertThrows(
      NullPointerException.class,
      () -> extent.path(null, '/')
    );

  }

  /// Extent#path(String) rejects a null separator.
  @Test
  void path_nullStringSeparator_throwsNullPointerException() {

    final var extent = TestExtent.root("test");

    assertThrows(
      NullPointerException.class,
      () -> extent.path(null)
    );

  }

  /// A root path consists only of its part.
  @SpecRef("4.4")
  @Test
  void path_root_returnsRootPart() {

    final var root = TestExtent.root("root");
    assertEquals("root", root.path().toString());

  }

  /// Extent#path(String) joins parts with the supplied string.
  @Test
  void path_stringSeparator_joinsPartsWithString() {

    final var a = TestExtent.root("a");
    final var b = a.child("b");
    final var c = b.child("c");

    assertEquals("a::b::c", c.path("::").toString());

  }

  /// Extent#stream cardinality matches normative depth.
  @Test
  void stream_nestedExtent_countMatchesDepth() {

    final var root = TestExtent.root("a");
    final var child = root.child("b");
    final var grandchild = child.child("c");

    assertEquals(grandchild.depth(), grandchild.stream().count());
    assertEquals(child.depth(), child.stream().count());
    assertEquals(root.depth(), root.stream().count());

  }

  // ===========================
  // Edge Cases and Integration Tests
  // ===========================

  /// Extent#stream visits every level once.
  @Test
  void stream_nestedExtent_countsEveryLevel() {

    final var root = TestExtent.root("a");
    final var child = root.child("b");
    final var grandchild = child.child("c");

    assertEquals(3L, grandchild.stream().count());
    assertEquals(2L, child.stream().count());
    assertEquals(1L, root.stream().count());

  }

  /// Extent#stream and Extent#fold traverse consistently.
  @Test
  void stream_nestedExtent_matchesFoldTraversal() {

    final var root = TestExtent.root("a");
    final var child = root.child("b");
    final var grandchild = child.child("c");

    final var streamCount = grandchild.stream().count();

    final var foldCount = grandchild.fold(
      _ -> 1,
      (acc, _) -> acc + 1
    );

    assertEquals(streamCount, (long) foldCount);

  }

  /// Extent#stream traverses from receiver toward root.
  @Test
  void stream_nestedExtent_visitsSelfToRoot() {

    final var a = TestExtent.root("a");
    final var b = a.child("b");
    final var c = b.child("c");

    final var values = c.stream()
      .map(TestExtent::part)
      .toList();

    assertEquals(List.of("c", "b", "a"), values);

  }

  /// Extent#stream exposes standard Java stream operations.
  @Test
  void stream_pipeline_supportsJavaStreamOperations() {

    final var a = TestExtent.root("alpha");
    final var b = a.child("beta");
    final var c = b.child("gamma");

    final var maxLength = c.stream()
      .map(TestExtent::part)
      .mapToInt(String::length)
      .max()
      .orElse(0);

    assertEquals(5, maxLength); // "alpha" and "gamma" are 5 chars

    final var hasShortName = c.stream()
      .anyMatch(e -> e.part().length() < 5);

    assertTrue(hasShortName); // "beta" is 4 chars

  }

  /// Validates consistency across all traversal mechanisms: depth, stream, fold, iterator.
  ///
  /// This test verifies a fundamental invariant: all traversal operations on an Extent
  /// must visit the SAME number of elements. Whether using depth(), stream(), fold(),
  /// or iterator(), the count must be consistent. This consistency guarantee enables
  /// developers to choose traversal methods based on convenience without worrying
  /// about semantic differences.
  ///
  /// Hierarchy Structure:
  /// ```
  /// a (root) → b → c → d (depth = 4)
  /// ```
  ///
  /// Traversal Operation Equivalence:
  /// ```
  /// depth()           → 4 (constant-time calculation)
  /// stream().count()  → 4 (lazy traversal, terminal operation)
  /// fold(counting)    → 4 (eager traversal with accumulation)
  /// iterator + loop   → 4 (manual traversal with external counter)
  /// ```
  ///
  /// Why consistency matters:
  /// - **Interchangeability**: Switch between operations without behavior change
  /// - **Predictability**: Count is invariant regardless of traversal method
  /// - **Correctness**: All methods represent the same logical hierarchy
  /// - **Testing**: Can verify one method against another
  ///
  /// Performance characteristics (though all yield same count):
  /// - **depth()**: O(n) recursive parent chain traversal, no heap allocation
  /// - **stream()**: O(n) lazy iteration, allocates stream infrastructure
  /// - **fold()**: O(n) eager traversal, custom accumulator logic
  /// - **iterator()**: O(n) manual traversal, no intermediate allocations
  ///
  /// Real-world implications:
  /// ```java
  /// // All equivalent for counting:
  /// int depth = extent.depth();
  /// long streamCount = extent.stream().count();
  /// int foldCount = extent.fold(_ -> 1, (acc, _) -> acc + 1);
  ///
  /// // Choose based on use case:
  /// if (extent.depth() > 10) { ... }           // Simplest for threshold checks
  /// extent.stream().filter(...).count();        // Best for filtering
  /// extent.fold(0, (sum, e) -> sum + e.cost()); // Best for custom aggregation
  /// for (Extent e : extent) { ... }            // Best for early termination
  /// ```
  ///
  /// Critical behaviors verified:
  /// - depth() returns 4 (hierarchy length calculation)
  /// - stream().count() returns 4 (lazy stream traversal)
  /// - fold counting returns 4 (eager accumulation)
  /// - Iterator manual counting returns 4 (explicit traversal)
  /// - All four methods produce IDENTICAL counts
  ///
  /// Relationship to other invariants:
  /// - `depth()` represents hierarchy length from root to current
  /// - `stream().count()` materializes all elements
  /// - `fold()` processes all elements with accumulator
  /// - `iterator()` exposes all elements for manual iteration
  /// - All traverse the SAME logical elements
  ///
  /// Expected: All traversal operations count 4 elements consistently
  /// Extent traversal conveniences visit one consistent chain.
  @Test
  void traversal_allOperations_visitSameExtentChain() {

    final var a = TestExtent.root("a");
    final var b = a.child("b");
    final var c = b.child("c");
    final var d = c.child("d");

    // Depth should match stream count
    assertEquals(d.depth(), d.stream().count());

    // Fold count should match depth
    final var foldCount = d.fold(
      _ -> 1,
      (acc, _) -> acc + 1
    );
    assertEquals(d.depth(), foldCount);

    // Iterator count should match depth
    final Iterator< TestExtent > iterator = d.iterator();
    var iteratorCount = 0;
    while (iterator.hasNext()) {
      iterator.next();
      iteratorCount++;
    }
    assertEquals(d.depth(), iteratorCount);

  }

  /// Traversal visits every enclosure in a deep chain.
  @SpecRef("4.4")
  @Test
  void traversal_longHierarchy_visitsEveryLevel() {

    var current = TestExtent.root("level0");

    for (var i = 1; i <= 100; i++) {
      current = current.child("level" + i);
    }

    assertEquals(101, current.depth());
    assertEquals(101L, current.stream().count());
    assertEquals("level0", current.extremity().part());

  }

  /// Extent#within recognizes an enclosure at any depth.
  @Test
  void within_ancestorAtAnyDepth_returnsTrue() {

    var current = TestExtent.root("level0");
    final var root = current;

    for (var i = 1; i <= 10; i++) {
      current = current.child("level" + i);
    }

    assertTrue(current.within(root));

  }

  /// Extent#within does not treat a descendant as an enclosure.
  @Test
  void within_descendant_returnsFalse() {

    final var root = TestExtent.root("root");
    final var child = root.child("child");

    assertFalse(root.within(child));

  }

  /// Extent#within recognizes the immediate enclosure.
  @Test
  void within_directEnclosure_returnsTrue() {

    final var root = TestExtent.root("root");
    final var child = root.child("child");
    final var grandchild = child.child("grandchild");

    assertTrue(child.within(root));
    assertTrue(grandchild.within(root));
    assertTrue(grandchild.within(child));

  }

  /// Extent#within rejects null.
  @Test
  void within_null_throwsNullPointerException() {

    final var extent = TestExtent.root("test");

    assertThrows(
      NullPointerException.class,
      () -> extent.within(null)
    );

  }

  /// Extent#within does not treat the receiver as its enclosure.
  @Test
  void within_self_returnsFalse() {

    final var root = TestExtent.root("root");
    assertFalse(root.within(root));

  }

  /// Extent#within rejects an unrelated extent.
  @Test
  void within_unrelatedExtent_returnsFalse() {

    final var tree1 = TestExtent.root("tree1");
    final var tree2 = TestExtent.root("tree2");

    assertFalse(tree1.within(tree2));
    assertFalse(tree2.within(tree1));

  }

  /// A simple test implementation of Extent for testing default methods.
  /// Represents a hierarchical structure of string values.

  private record TestExtent( String value, TestExtent parent )
    implements Extent< TestExtent, TestExtent > {

    private TestExtent(
      final String value
    ) {

      this(value, null);

    }

    /// Creates a root extent with no parent

    private static TestExtent root(
      final String value
    ) {

      return new TestExtent(value);

    }

    /// Returns the parent (enclosure) of this extent

    @NotNull
    @Override
    public Optional< TestExtent > enclosure() {

      return Optional.ofNullable(parent);

    }

    /// Returns the part (value) of this extent

    @NotNull
    @Override
    public String part() {

      return value;

    }

    /// Creates a child extent

    private TestExtent child(
      final String value
    ) {

      return new TestExtent(value, this);

    }

  }

}
