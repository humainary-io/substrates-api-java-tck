// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.SpecDoc;
import io.humainary.specs.api.Specs.SpecRef;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.function.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §§4.1, 4.4, and 12 Name structure and interning, plus tests for the
/// Java projection's construction, extension, comparison, traversal, and rendering conveniences.
/// @author William David Louth
/// @since 1.0

@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.0.2/SPEC.md")
final class NameContractTest
  extends TestSupport {

  private Cortex cortex;

  /// Name#compareTo orders names lexicographically.
  @Test
  @SuppressWarnings("EqualsWithItself")
  void compareTo_distinctNames_ordersLexicographically() {

    final var a = cortex.name("a");
    final var b = cortex.name("b");
    final var c = cortex.name("c");

    assertTrue(a.compareTo(b) < 0);
    assertTrue(c.compareTo(b) > 0);
    assertEquals(0, a.compareTo(a));

  }

  /// Name#compareTo orders a name before its extension.
  @Test
  void compareTo_nameAndExtension_ordersParentFirst() {

    final var root = cortex.name("root");
    final var rootChild = cortex.name("root.child");

    assertTrue(root.compareTo(rootChild) < 0);

  }

  /// Name depth counts every segment through the root.
  @SpecRef({"4.1", "4.4"})
  @Test
  void depth_nestedName_returnsSegmentCount() {

    assertEquals(1, cortex.name("root").depth());
    assertEquals(2, cortex.name("root.child").depth());
    assertEquals(4, cortex.name("a.b.c.d").depth());

  }

  // ===========================
  // Basic Name Creation Tests
  // ===========================

  /// Name#enclosure(Consumer) receives the immediate parent.
  @Test
  void enclosure_consumerOnNestedName_receivesImmediateParent() {

    final var name = cortex.name("parent.child");
    final var result = new String[1];

    name.enclosure(parent -> result[0] = parent.part());

    assertEquals("parent", result[0]);

  }

  /// Name#enclosure(Consumer) does not invoke for a root.
  @Test
  void enclosure_consumerOnRoot_isNotInvoked() {

    final var name = cortex.name("root");
    final var called = new boolean[1];

    name.enclosure(_ -> called[0] = true);

    assertFalse(called[0]);

  }

  /// Name enclosures traverse toward the root.
  @SpecRef({"4.1", "4.4"})
  @Test
  void enclosure_nestedName_returnsParentChain() {

    final var name = cortex.name("a.b.c.d");

    final var c = name.enclosure().orElseThrow();
    assertEquals("c", c.part());

    final var b = c.enclosure().orElseThrow();
    assertEquals("b", b.part());

    final var a = b.enclosure().orElseThrow();
    assertEquals("a", a.part());

    assertFalse(a.enclosure().isPresent());

  }

  /// Standard equality observes canonical Name identity.
  @SpecRef({"1.2", "4.1", "12"})
  @Test
  void equality_equivalentNames_returnsTrue() {

    final var name1 = cortex.name("test.name");
    final var name2 = cortex.name("test.name");

    assertEquals(name1, name2);

  }

  /// Name#extent returns the concrete receiver.
  @Test
  void extent_name_returnsSelf() {

    final var name = cortex.name("test");
    assertSame(name, name.extent());

  }

  /// A nested Name's extremity is its root segment.
  @SpecRef("4.4")
  @Test
  void extremity_nestedName_returnsRootSegment() {

    final var name = cortex.name("a.b.c.d");
    final var extremity = name.extremity();

    assertEquals("a", extremity.part());
    assertFalse(extremity.enclosure().isPresent());

  }

  /// A root Name is its own extremity.
  @SpecRef("4.4")
  @Test
  void extremity_rootName_returnsSelf() {

    final var name = cortex.name("root");
    assertSame(name, name.extremity());

  }

  /// Name fold and foldTo operations preserve the same hierarchy.
  @Test
  void foldAndFoldTo_nameHierarchy_preserveEquivalentParts() {

    final var name = cortex.name("a.b.c.d");

    // Count total characters
    final var totalChars = name.fold(
      n -> n.part().length(),
      (acc, n) -> acc + n.part().length()
    );

    assertEquals(4, totalChars);

    // Build reversed path
    final var reversedPath = name.fold(
      Name::part,
      (acc, n) -> n.part() + "/" + acc
    );

    assertEquals("a/b/c/d", reversedPath);

  }

  /// Demonstrates the directional difference between fold (RTL) and foldTo (LTR).
  ///
  /// Tests hierarchical name "first.second.third" with both fold operations,
  /// showing that they traverse the hierarchy in opposite directions but can
  /// produce the same result when operations are symmetric.
  ///
  /// Traversal directions:
  /// - fold():   Right-to-left (leaf → root):  third → second → first
  /// - foldTo(): Left-to-right (root → leaf):  first → second → third
  ///
  /// In this test:
  /// - fold builds "third.second.first" by prepending each value
  /// - foldTo builds "first.second.third" by appending each value
  /// - Both produce same result due to symmetric string concatenation
  ///
  /// When to use each:
  /// - fold: Natural for reducing from most specific to least specific
  ///         (e.g., computing depth, building paths from leaf)
  /// - foldTo: Natural for accumulating from general to specific
  ///           (e.g., applying nested transformations, building paths from root)
  ///
  /// This is analogous to List.foldRight vs List.foldLeft in functional
  /// programming, where direction matters for non-commutative operations.
  ///
  /// Expected: Both produce "first.second.third" (though via different orders)
  /// Name fold and foldTo expose opposite traversal orders.
  @Test
  void foldAndFoldTo_nestedName_exposeOppositeOrders() {

    final var name = cortex.name("first.second.third");

    // fold goes right-to-left (third -> second -> first)
    final var foldResult = name.fold(
      Name::part,
      (acc, n) -> n.part() + "." + acc
    );

    // foldTo goes left-to-right (first -> second -> third)
    final var foldToResult = name.foldTo(
      Name::part,
      (acc, n) -> acc + "." + n.part()
    );

    assertEquals(foldResult, foldToResult);

  }

  /// Name#foldTo traverses from root toward receiver.
  @Test
  void foldTo_nestedName_traversesRootToSelf() {

    final var name = cortex.name("a.b.c");

    final var result = name.foldTo(
      Name::part,
      (acc, n) -> acc + "." + n.part()
    );

    assertEquals("a.b.c", result);

  }

  /// Name#foldTo visits a root exactly once.
  @Test
  void foldTo_rootName_visitsOnce() {

    final var name = cortex.name("solo");

    final var result = name.foldTo(
      Name::part,
      (acc, n) -> acc + "." + n.part()
    );

    assertEquals("solo", result);

  }

  /// Name#fold traverses from receiver toward root.
  @Test
  void fold_nestedName_traversesSelfToRoot() {

    final var name = cortex.name("a.b.c");

    final var result = name.fold(
      n -> n.part().length(),
      (acc, n) -> acc + n.part().length()
    );

    assertEquals(3, result); // c(1) + b(1) + a(1)

  }

  /// Name#fold visits a root exactly once.
  @Test
  void fold_rootName_visitsOnce() {

    final var name = cortex.name("test");

    final var result = name.fold(
      _ -> 1,
      (acc, _) -> acc + 1
    );

    assertEquals(1, result);

  }

  /// Canonically identical Names have a stable hash code.
  @Test
  void hashCode_canonicalName_isStable() {

    final var name1 = cortex.name("test.name");
    final var name2 = cortex.name("test.name");

    assertEquals(name1.hashCode(), name2.hashCode());

  }

  /// Structurally different Names have distinct canonical identities.
  @SpecRef("4.1")
  @Test
  void identity_differentNames_returnsDistinctInstances() {

    final var name1 = cortex.name("first");
    final var name2 = cortex.name("second");

    assertNotEquals(name1, name2);

  }

  /// Equivalent construction paths return canonical identity.
  @SpecRef({"4.1", "12"})
  @Test
  void identity_equivalentConstructionPaths_returnsSameInstance() {

    final var root = cortex.name("root");
    final var path1 = root.name("a").name("b");
    final var path2 = cortex.name("root.a.b");

    assertSame(path1, path2);

  }

  /// Equivalent Name creation returns the same canonical
  /// instance through ordinary reference identity.
  @SpecRef({"1.2", "4.1", "12"})
  @Test
  void identity_equivalentNames_returnsSameInstance() {

    final var name1 = cortex.name("test.name");
    final var name2 = cortex.name("test.name");

    assertSame(name1, name2); // Names should be interned

  }

  /// An exhausted Name iterator signals NoSuchElementException.
  @Test
  void iterator_exhausted_throwsNoSuchElementException() {

    final var name = cortex.name("single");
    final Iterator< Name > iterator = name.iterator();

    iterator.next(); // consume the only element

    assertThrows(NoSuchElementException.class, iterator::next);

  }

  /// Name#iterator traverses from receiver toward root.
  @Test
  void iterator_nestedName_traversesSelfToRoot() {

    final var name = cortex.name("a.b.c");
    final Iterator< Name > iterator = name.iterator();

    assertTrue(iterator.hasNext());
    assertEquals("c", iterator.next().part());

    assertTrue(iterator.hasNext());
    assertEquals("b", iterator.next().part());

    assertTrue(iterator.hasNext());
    assertEquals("a", iterator.next().part());

    assertFalse(iterator.hasNext());

  }

  /// `cortex.name(String[].class)` and the equivalent string parse should
  /// resolve to the same interned Name.
  /// Cortex#name(Class) uses the canonical array-class spelling.
  @Test
  void name_arrayClass_matchesCanonicalStringName() {

    final var fromClass = cortex.name(String[].class);
    final var fromString = cortex.name("java.lang.String[]");

    assertSame(fromClass, fromString);

  }

  /// Names are interned: repeated lookups for the same array Class return
  /// the same Name instance.
  /// Cortex#name(Class) interns array-class names.
  @Test
  void name_arrayClass_repeatedCreationReturnsSameInstance() {

    final var first = cortex.name(String[].class);
    final var second = cortex.name(String[].class);

    assertSame(first, second);

  }

  /// Name#name(Class) appends the class's canonical name.
  @Test
  void name_classExtension_appendsCanonicalClassName() {

    final var root = cortex.name("packages");
    final var extended = root.name(Integer.class);

    assertTrue(extended.part().contains("Integer"));

  }

  /// Cortex#name(Class) uses the class's canonical name.
  @Test
  void name_class_usesCanonicalClassName() {

    final var name = cortex.name(String.class);

    assertEquals("java.lang.String", name.path().toString());

  }

  /// A Name exposes its part, enclosure, extremity, depth, and path.
  @SpecRef({"4.1", "4.4"})
  @Test
  void name_complexHierarchy_exposesRequiredExtentProperties() {

    final var name = cortex.name("root.level1.level2.level3.level4");

    assertEquals(5, name.depth());
    assertEquals("level4", name.part());
    assertEquals("root", name.extremity().part());

    final var collected = name.stream()
      .map(Name::part)
      .toList();

    assertEquals(
      List.of("level4", "level3", "level2", "level1", "root"),
      collected
    );

  }

  /// Name#name(Name) appends every segment of the supplied Name.
  @Test
  void name_compositeNameExtension_appendsEverySegment() {

    final var root = cortex.name("root");
    final var suffix = cortex.name("child.grandchild");
    final var extended = root.name(suffix);

    assertEquals("root.child.grandchild", extended.path().toString());
    assertEquals("grandchild", extended.part());
    assertEquals("root.child", extended.enclosure().orElseThrow().path().toString());

  }

  /// Name creation rejects consecutive dots.
  @SpecRef("4.1")
  @Test
  void name_consecutiveDots_throwsIllegalArgumentException() {

    // Various patterns with empty segments should all be rejected
    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name("a...b")
    );

    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name("a..b..c")
    );

    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name("..a")
    );

    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name("a..")
    );

  }

  /// Extending from an empty iterable returns the receiver unchanged.
  @Test
  void name_emptyIterableExtension_returnsSameInstance() {

    final var name = cortex.name("base");
    final var extended = name.name(List.of());

    // With empty iterable, should return same name
    assertSame(name, extended);

  }

  // ===========================
  // Name Extension Tests
  // ===========================

  /// A Name cannot contain an empty segment.
  @SpecRef("4.1")
  @Test
  void name_emptyString_throwsIllegalArgumentException() {

    // Empty string is not a valid name
    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name("")
    );

  }

  /// Enum constant subclasses use their declaring enum type in names.
  @Test
  void name_enumConstantWithBody_usesDeclaringType() {

    final var name = cortex.name(EnumWithBody.SPECIAL);

    assertEquals(
      "io.humainary.substrates.tck.NameContractTest.EnumWithBody.SPECIAL",
      name.path().toString()
    );

  }

  /// Name#name(Enum) appends the declaring type and constant.
  @Test
  void name_enumExtension_appendsDeclaringTypeAndConstant() {

    enum Status {ACTIVE, INACTIVE}

    final var root = cortex.name("system");
    final var extended = root.name(Status.ACTIVE);

    assertEquals("ACTIVE", extended.part());

  }

  /// Cortex#name(Enum) includes the declaring type and constant.
  @Test
  void name_enum_usesDeclaringTypeAndConstant() {

    enum TestEnum {FIRST, SECOND}

    final var name = cortex.name(TestEnum.FIRST);

    assertEquals("FIRST", name.part());

  }

  /// Equivalent extensions return canonical identity.
  @SpecRef({"4.1", "12"})
  @Test
  void name_equivalentExtensions_returnSameInstance() {

    final var root = cortex.name("root");
    final var child1 = root.name("child");
    final var child2 = root.name("child");

    assertSame(child1, child2);

  }

  /// Name#name(Class) renders inner classes with canonical dots.
  @Test
  void name_innerClassExtension_usesCanonicalDots() {

    final var base = cortex.name(NameContractTest.class);
    final var extended = base.name(Outer.Inner.class);

    assertEquals(
      "io.humainary.substrates.tck.NameContractTest.io.humainary.substrates.tck.NameContractTest.Outer.Inner",
      extended.path().toString()
    );

  }

  /// Inner-class names use canonical dot separators.
  @Test
  void name_innerClass_usesCanonicalDots() {

    final var name = cortex.name(Outer.Inner.class);

    assertEquals(
      "io.humainary.substrates.tck.NameContractTest.Outer.Inner",
      name.path().toString()
    );

  }

  /// Iterable creation accepts parts containing multiple segments.
  @Test
  void name_iterableContainingCompositeParts_flattensSegments() {

    // Parts containing dots should be parsed as composite paths
    final var parts = List.of("root", "child.grandchild");
    final var name = cortex.name(parts);

    assertEquals("grandchild", name.part());
    assertEquals(3, name.depth());
    assertEquals("root.child.grandchild", name.path().toString());

  }

  /// Iterable creation rejects empty segments between consecutive dots.
  @SpecRef("4.1")
  @Test
  void name_iterableContainingConsecutiveDots_throwsIllegalArgumentException() {

    final var parts = List.of("root", "a..b");

    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name(parts)
    );

  }

  /// Iterable creation rejects an empty segment.
  @SpecRef("4.1")
  @Test
  void name_iterableContainingEmptySegment_throwsIllegalArgumentException() {

    final var parts = new ArrayList< String >();

    parts.add("root");
    parts.add("");

    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name(parts)
    );

  }

  /// Iterable creation rejects a leading dot.
  @SpecRef("4.1")
  @Test
  void name_iterableContainingLeadingDot_throwsIllegalArgumentException() {

    final var parts = List.of("root", ".invalid");

    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name(parts)
    );

  }

  /// Iterable creation rejects an absent segment.
  @SpecRef("15.2")
  @Test
  void name_iterableContainingNullSegment_throwsNullPointerException() {

    final var parts = new ArrayList< String >();

    parts.add("root");
    parts.add(null);

    assertThrows(
      NullPointerException.class,
      () -> cortex.name(parts)
    );

  }

  /// Iterable creation rejects a trailing dot.
  @SpecRef("4.1")
  @Test
  void name_iterableContainingTrailingDot_throwsIllegalArgumentException() {

    final var parts = List.of("root", "invalid.");

    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name(parts)
    );

  }

  // ===========================
  // Enclosure and Hierarchy Tests
  // ===========================

  /// Iterable extension flattens composite parts.
  @Test
  void name_iterableExtensionContainingCompositeParts_flattensSegments() {

    // Parts containing dots should be parsed as composite paths
    final var base = cortex.name("base");
    final var parts = List.of("level1", "level2.level3");
    final var name = base.name(parts);

    assertEquals("level3", name.part());
    assertEquals(4, name.depth());
    assertEquals("base.level1.level2.level3", name.path().toString());

  }

  /// Iterable extension rejects consecutive dots.
  @SpecRef("4.1")
  @Test
  void name_iterableExtensionContainingConsecutiveDots_throwsIllegalArgumentException() {

    final var name = cortex.name("base");
    final var parts = List.of("child", "a..b");

    assertThrows(
      IllegalArgumentException.class,
      () -> name.name(parts)
    );

  }

  /// Iterable extension rejects an empty segment.
  @SpecRef("4.1")
  @Test
  void name_iterableExtensionContainingEmptySegment_throwsIllegalArgumentException() {

    final var name = cortex.name("base");
    final var parts = new ArrayList< String >();

    parts.add("child");
    parts.add("");

    assertThrows(
      IllegalArgumentException.class,
      () -> name.name(parts)
    );

  }

  /// Iterable extension rejects a leading dot.
  @SpecRef("4.1")
  @Test
  void name_iterableExtensionContainingLeadingDot_throwsIllegalArgumentException() {

    final var name = cortex.name("base");
    final var parts = List.of("child", ".invalid");

    assertThrows(
      IllegalArgumentException.class,
      () -> name.name(parts)
    );

  }

  /// Iterable extension rejects an absent segment.
  @SpecRef("15.2")
  @Test
  void name_iterableExtensionContainingNullSegment_throwsNullPointerException() {

    final var name = cortex.name("base");
    final var parts = new ArrayList< String >();

    parts.add("child");
    parts.add(null);

    assertThrows(
      NullPointerException.class,
      () -> name.name(parts)
    );

  }

  /// Iterable extension rejects a trailing dot.
  @SpecRef("4.1")
  @Test
  void name_iterableExtensionContainingTrailingDot_throwsIllegalArgumentException() {

    final var name = cortex.name("base");
    final var parts = List.of("child", "invalid.");

    assertThrows(
      IllegalArgumentException.class,
      () -> name.name(parts)
    );

  }

  /// Name#name(Iterable,Function) maps and appends parts.
  @Test
  void name_iterableExtensionWithMapper_mapsAndAppendsParts() {

    final var root = cortex.name("root");
    final var numbers = List.of(1, 2);
    final var extended = root.name(numbers, n -> "item" + n);

    assertEquals("item2", extended.part());

  }

  /// Name#name(Iterable) appends iterable parts.
  @Test
  void name_iterableExtension_appendsParts() {

    final var root = cortex.name("root");
    final var parts = List.of("level1", "level2");
    final var extended = root.name(parts);

    assertEquals("level2", extended.part());

  }

  /// Cortex#name(Iterable,Function) maps and concatenates parts.
  @Test
  void name_iterableWithMapper_mapsAndConcatenatesParts() {

    final var numbers = List.of(1, 2, 3);
    final var name = cortex.name(numbers, Object::toString);

    assertEquals("3", name.part());
    assertEquals(3, name.depth());

  }

  /// Cortex#name(Iterable) concatenates iterable name parts.
  @Test
  void name_iterable_concatenatesParts() {

    final var parts = List.of("first", "second", "third");
    final var name = cortex.name(parts);

    assertEquals("third", name.part());
    assertEquals(3, name.depth());

  }

  /// Iterator creation accepts parts containing multiple segments.
  @Test
  void name_iteratorContainingCompositeParts_flattensSegments() {

    // Parts containing dots should be parsed as composite paths
    final var parts = List.of("root", "child.grandchild");
    final var name = cortex.name(parts.iterator());

    assertEquals("grandchild", name.part());
    assertEquals(3, name.depth());
    assertEquals("root.child.grandchild", name.path().toString());

  }

  /// Iterator creation rejects empty segments between consecutive dots.
  @SpecRef("4.1")
  @Test
  void name_iteratorContainingConsecutiveDots_throwsIllegalArgumentException() {

    final var parts = List.of("root", "a..b");

    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name(parts.iterator())
    );

  }

  /// Iterator creation rejects an empty segment.
  @SpecRef("4.1")
  @Test
  void name_iteratorContainingEmptySegment_throwsIllegalArgumentException() {

    final var parts = new ArrayList< String >();

    parts.add("root");
    parts.add("");

    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name(parts.iterator())
    );

  }

  /// Iterator creation rejects a leading dot.
  @SpecRef("4.1")
  @Test
  void name_iteratorContainingLeadingDot_throwsIllegalArgumentException() {

    final var parts = List.of("root", ".invalid");

    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name(parts.iterator())
    );

  }

  /// Iterator creation rejects an absent segment.
  @SpecRef("15.2")
  @Test
  void name_iteratorContainingNullSegment_throwsNullPointerException() {

    final var parts = new ArrayList< String >();

    parts.add("root");
    parts.add(null);

    assertThrows(
      NullPointerException.class,
      () -> cortex.name(parts.iterator())
    );

  }

  /// Iterator creation rejects a trailing dot.
  @SpecRef("4.1")
  @Test
  void name_iteratorContainingTrailingDot_throwsIllegalArgumentException() {

    final var parts = List.of("root", "invalid.");

    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name(parts.iterator())
    );

  }

  /// Iterator extension flattens composite parts.
  @Test
  void name_iteratorExtensionContainingCompositeParts_flattensSegments() {

    // Parts containing dots should be parsed as composite paths
    final var base = cortex.name("base");
    final var parts = List.of("level1", "level2.level3");
    final var name = base.name(parts.iterator());

    assertEquals("level3", name.part());
    assertEquals(4, name.depth());
    assertEquals("base.level1.level2.level3", name.path().toString());

  }

  /// Iterator extension rejects consecutive dots.
  @SpecRef("4.1")
  @Test
  void name_iteratorExtensionContainingConsecutiveDots_throwsIllegalArgumentException() {

    final var name = cortex.name("base");
    final var parts = List.of("child", "a..b");

    assertThrows(
      IllegalArgumentException.class,
      () -> name.name(parts.iterator())
    );

  }

  /// Iterator extension rejects an empty segment.
  @SpecRef("4.1")
  @Test
  void name_iteratorExtensionContainingEmptySegment_throwsIllegalArgumentException() {

    final var name = cortex.name("base");
    final var parts = new ArrayList< String >();

    parts.add("child");
    parts.add("");

    assertThrows(
      IllegalArgumentException.class,
      () -> name.name(parts.iterator())
    );

  }

  // ===========================
  // Iteration Tests
  // ===========================

  /// Iterator extension rejects a leading dot.
  @SpecRef("4.1")
  @Test
  void name_iteratorExtensionContainingLeadingDot_throwsIllegalArgumentException() {

    final var name = cortex.name("base");
    final var parts = List.of("child", ".invalid");

    assertThrows(
      IllegalArgumentException.class,
      () -> name.name(parts.iterator())
    );

  }

  /// Iterator extension rejects an absent segment.
  @SpecRef("15.2")
  @Test
  void name_iteratorExtensionContainingNullSegment_throwsNullPointerException() {

    final var name = cortex.name("base");
    final var parts = new ArrayList< String >();

    parts.add("child");
    parts.add(null);

    assertThrows(
      NullPointerException.class,
      () -> name.name(parts.iterator())
    );

  }

  /// Iterator extension rejects a trailing dot.
  @SpecRef("4.1")
  @Test
  void name_iteratorExtensionContainingTrailingDot_throwsIllegalArgumentException() {

    final var name = cortex.name("base");
    final var parts = List.of("child", "invalid.");

    assertThrows(
      IllegalArgumentException.class,
      () -> name.name(parts.iterator())
    );

  }

  /// Name#name(Iterator,Function) maps and appends parts.
  @Test
  void name_iteratorExtensionWithMapper_mapsAndAppendsParts() {

    final var root = cortex.name("root");
    final var numbers = List.of(5, 10).iterator();
    final var extended = root.name(numbers, n -> "val" + n);

    assertEquals("val10", extended.part());

  }

  // ===========================
  // Fold Operations Tests
  // ===========================

  /// Name#name(Iterator) appends iterator parts.
  @Test
  void name_iteratorExtension_appendsParts() {

    final var root = cortex.name("root");
    final var parts = List.of("a", "b").iterator();
    final var extended = root.name(parts);

    assertEquals("b", extended.part());

  }

  /// Cortex#name(Iterator,Function) maps and concatenates parts.
  @Test
  void name_iteratorWithMapper_mapsAndConcatenatesParts() {

    final var numbers = List.of(10, 20, 30);
    final var name = cortex.name(numbers.iterator(), Object::toString);

    assertEquals("30", name.part());

  }

  /// Cortex#name(Iterator) concatenates iterator name parts.
  @Test
  void name_iterator_concatenatesParts() {

    final var parts = List.of("alpha", "beta", "gamma");
    final var name = cortex.name(parts.iterator());

    assertEquals("gamma", name.part());

  }

  /// Name creation rejects a leading dot.
  @SpecRef("4.1")
  @Test
  void name_leadingDot_throwsIllegalArgumentException() {

    // Leading dot creates empty segment which should be rejected
    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name(".a")
    );

  }

  /// Name hierarchy supports many non-empty segments.
  @SpecRef("4.1")
  @Test
  void name_manySegments_preservesFullHierarchy() {

    final var longPart = "a".repeat(1000);
    final var name = cortex.name(longPart);

    assertEquals(1000, name.part().length());

  }

  // ===========================
  // Path Representation Tests
  // ===========================

  /// Name#name(Member) appends the declaring type and member.
  @Test
  void name_memberExtension_appendsDeclaringTypeAndMember() throws java.lang.Exception {

    final var root = cortex.name("methods");
    final var method = String.class.getMethod("isEmpty");
    final var extended = root.name(method);

    assertNotNull(extended);

  }

  /// Cortex#name(Member) uses the declaring type and member name.
  @Test
  void name_member_usesDeclaringTypeAndMemberName() throws java.lang.Exception {

    final var method = String.class.getMethod("length");
    final var name = cortex.name(method);

    assertNotNull(name);
    assertTrue(name.part().contains("length"));

  }

  /// Multidimensional reference array appends one `[]` per dimension to
  /// the leaf segment.
  /// Multidimensional object-array classes use canonical names.
  @Test
  void name_multidimensionalObjectArrayClass_usesCanonicalName() {

    final var name = cortex.name(String[][].class);

    assertEquals("java.lang.String[][]", name.path().toString());
    assertEquals("String[][]", name.part());

  }

  /// Multi-dimensional primitive array.
  /// Multidimensional primitive-array classes use canonical names.
  @Test
  void name_multidimensionalPrimitiveArrayClass_usesCanonicalName() {

    final var name = cortex.name(int[][].class);

    assertEquals("int[][]", name.path().toString());
    assertEquals("int[][]", name.part());
    assertEquals(1, name.depth());

  }

  /// String extension appends every supplied segment.
  @SpecRef("4.1")
  @Test
  void name_multipartStringExtension_appendsEverySegment() {

    final var root = cortex.name("root");
    final var extended = root.name("child.grandchild");

    assertEquals("grandchild", extended.part());
    assertTrue(extended.enclosure().isPresent());

  }

  /// Multiple dot-separated non-empty segments are valid.
  @SpecRef("4.1")
  @Test
  void name_multipleDotSeparators_createsHierarchy() {

    // Multiple consecutive separators create empty segments which should be rejected
    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name("a..b")
    );

  }

  /// Dot-separated segments create a hierarchical Name.
  @SpecRef("4.1")
  @Test
  void name_multipleSegments_createsNestedHierarchy() {

    final var name = cortex.name("root.child.grandchild");

    assertEquals("grandchild", name.part());
    assertTrue(name.enclosure().isPresent());

  }

  // ===========================
  // Equality and Comparison Tests
  // ===========================

  /// Name#name(Name) appends the supplied Name hierarchy.
  @Test
  void name_nameExtension_appendsHierarchy() {

    final var root = cortex.name("root");
    final var suffix = cortex.name("suffix");
    final var extended = root.name(suffix);

    assertTrue(extended.enclosure().isPresent());

  }

  /// Name#name(Name) accepts a Name as an extension part.
  @Test
  void name_namePart_appendsSuppliedName() {

    final var root = cortex.name("root");
    final var suffix = cortex.name("child.grandchild");
    final var combined = root.name(suffix);

    assertTrue(combined.path().toString().contains("root"));
    assertEquals("grandchild", combined.part());

  }

  /// Non-dot characters are preserved within a Name segment.
  @SpecRef("4.1")
  @Test
  void name_nonDotSpecialCharacters_preservesSegment() {

    final var name = cortex.name("test-name_123");
    assertEquals("test-name_123", name.part());

  }

  /// Cortex#name(Class) rejects absence.
  @SpecRef("15.2")
  @Test
  void name_nullClass_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> cortex.name((Class< ? >) null)
    );

  }

  /// Cortex#name(Enum) rejects absence.
  @SpecRef("15.2")
  @Test
  void name_nullEnum_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> cortex.name((Enum< ? >) null)
    );

  }

  /// Cortex#name(Iterable) rejects absence.
  @SpecRef("15.2")
  @Test
  void name_nullIterable_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> cortex.name((Iterable< String >) null)
    );

  }

  // ===========================
  // Edge Cases and Null Tests
  // ===========================

  /// Cortex#name(Iterator) rejects absence.
  @SpecRef("15.2")
  @Test
  void name_nullIterator_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> cortex.name((Iterator< String >) null)
    );

  }

  /// String Name creation rejects absence.
  @SpecRef("15.2")
  @Test
  void name_nullString_throwsNullPointerException() {

    assertThrows(
      NullPointerException.class,
      () -> cortex.name((String) null)
    );

  }

  /// Single-dimensional reference array uses the canonical form
  /// `<componentCanonical>[]`. The leaf segment retains the brackets so
  /// the array-ness of the source class is visible in the Name.
  /// Object-array classes use canonical names.
  @Test
  void name_objectArrayClass_usesCanonicalName() {

    final var name = cortex.name(String[].class);

    assertEquals("java.lang.String[]", name.path().toString());
    assertEquals("String[]", name.part());
    assertEquals(3, name.depth());

  }

  /// Single-dimensional primitive array — no package prefix, the bracketed
  /// form is the only segment.
  /// Primitive-array classes use canonical names.
  @Test
  void name_primitiveArrayClass_usesCanonicalName() {

    final var name = cortex.name(int[].class);

    assertEquals("int[]", name.path().toString());
    assertEquals("int[]", name.part());
    assertEquals(1, name.depth());

  }

  /// A single non-empty character is a valid Name segment.
  @SpecRef("4.1")
  @Test
  void name_singleCharacter_createsRoot() {

    final var name = cortex.name("x");
    assertEquals("x", name.part());

  }

  /// One non-empty segment creates a root Name.
  @SpecRef("4.1")
  @Test
  void name_singleSegment_createsRoot() {

    final var name = cortex.name("root");

    assertEquals("root", name.part());
    assertFalse(name.enclosure().isPresent());

  }

  /// Extending a Name appends segments into a new immutable Name.
  @SpecRef("4.1")
  @Test
  void name_stringExtension_returnsExtendedName() {

    final var root = cortex.name("root");
    final var extended = root.name("child");

    assertEquals("child", extended.part());
    assertEquals(root, extended.enclosure().orElseThrow());

  }

  /// Name creation rejects a trailing dot.
  @SpecRef("4.1")
  @Test
  void name_trailingDot_throwsIllegalArgumentException() {

    // Trailing dot creates empty segment which should be rejected
    assertThrows(
      IllegalArgumentException.class,
      () -> cortex.name("a.")
    );

  }

  /// Part is terminal while path contains the full hierarchy.
  @SpecRef({"4.1", "4.4"})
  @Test
  void partAndPath_nestedName_exposeTerminalAndHierarchy() {

    final var name = cortex.name("root.child");

    assertEquals("child", name.part());
    assertEquals("root.child", name.path().toString());

  }

  /// Name#path(char) joins parts with the supplied character.
  @Test
  void path_characterSeparator_joinsNameParts() {

    final var name = cortex.name("x.y.z");
    assertEquals("x-y-z", name.path('-').toString());

  }

  /// Name paths use dot as their default separator.
  @SpecRef("4.4")
  @Test
  void path_defaultSeparator_joinsPartsWithDot() {

    final var name = cortex.name("a.b.c");
    assertEquals("a.b.c", name.path().toString());

  }

  /// Name#path(Function,char) maps and joins every part.
  @Test
  void path_mapperAndCharacterSeparator_transformsAndJoinsParts() {

    final var name = cortex.name("test.name");

    assertEquals(
      "TEST|NAME",
      name.path(n -> n.part().toUpperCase(), '|').toString()
    );

  }

  /// Name#path(Function,String) maps and joins every part.
  @Test
  void path_mapperAndStringSeparator_transformsAndJoinsParts() {

    final var name = cortex.name("foo.bar");

    assertEquals(
      "FOO -> BAR",
      name.path(n -> n.part().toUpperCase(), " -> ").toString()
    );

  }

  /// Name#path(Function) maps every part.
  @Test
  void path_mapper_transformsEveryNamePart() {

    final var name = cortex.name("a.b.c");

    final Function< String, String > upperMapper = String::toUpperCase;
    assertEquals("A.B.C", name.path(upperMapper).toString());

  }

  // ===========================
  // Integration Tests
  // ===========================

  /// Name#path(String) joins parts with the supplied string.
  @Test
  void path_stringSeparator_joinsNameParts() {

    final var name = cortex.name("one.two.three");
    assertEquals("one::two::three", name.path("::").toString());

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

  /// Name stream cardinality matches normative depth.
  @Test
  void stream_nestedName_countMatchesDepth() {

    final var name = cortex.name("one.two.three");
    assertEquals(3L, name.stream().count());

  }

  /// Name#stream supports standard Java stream operations.
  @Test
  void stream_nestedName_supportsJavaOperations() {

    final var name = cortex.name("alpha.beta.gamma.delta");

    final var maxLength = name.stream()
      .map(Name::part)
      .mapToInt(String::length)
      .max()
      .orElse(0);

    assertEquals(5, maxLength); // "alpha", "gamma", "delta" are all 5 chars

    final var hasShortName = name.stream()
      .map(Name::part)
      .anyMatch(v -> v.length() < 5);

    assertTrue(hasShortName); // "beta" is 4 chars

  }

  /// Name#stream traverses from receiver toward root.
  @Test
  void stream_nestedName_traversesSelfToRoot() {

    final var name = cortex.name("a.b.c.d");
    final var values = name.stream()
      .map(Name::part)
      .toList();

    assertEquals(List.of("d", "c", "b", "a"), values);

  }

  /// Name#toString returns the default dotted path.
  @Test
  void toString_nestedName_returnsDottedPath() {

    final var name = cortex.name("test.example");
    final var str = name.toString();

    assertNotNull(str);
    assertFalse(str.isEmpty());

  }

  /// All Name extent operations share one hierarchy.
  @SpecRef("4.4")
  @Test
  void traversal_nameExtentOperations_visitSameHierarchy() {

    final var name = cortex.name("one.two.three.four");

    // Verify depth equals stream count
    assertEquals(name.depth(), name.stream().count());

    // Verify fold and stream produce same count
    final var foldCount = name.fold(
      _ -> 1,
      (acc, _) -> acc + 1
    );

    assertEquals((long) foldCount, name.stream().count());

  }

  /// Name#within recognizes enclosing Names only.
  @Test
  void within_nameHierarchy_recognizesEnclosures() {

    final var root = cortex.name("root");
    final var child = cortex.name("root.child");
    final var grandchild = cortex.name("root.child.grandchild");

    assertTrue(child.within(root));
    assertTrue(grandchild.within(root));
    assertTrue(grandchild.within(child));
    assertFalse(root.within(child));
    assertFalse(root.within(root));

  }

  enum EnumWithBody {

    BASIC,

    SPECIAL {
      @Override
      public String toString() {

        return
          "SPECIAL";

      }
    }

  }

  private static final class Outer {

    private static final class Inner {
    }

  }

}
