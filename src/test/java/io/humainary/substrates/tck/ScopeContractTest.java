// Copyright (c) 2025 William David Louth

package io.humainary.substrates.tck;

import io.humainary.specs.api.Specs.*;
import org.junit.jupiter.api.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.junit.jupiter.api.Assertions.*;

/// Conformance tests for SPEC §9.2 Scope hierarchy, registration, ordering, error suppression, and
/// terminal close behavior, plus foreign-provider checks from the Java projection.
/// @author William David Louth
/// @since 1.0
@SpecDoc("https://github.com/humainary-io/substrates-api-spec/blob/3.15.1/SPEC.md")
final class ScopeContractTest
  extends TestSupport {

  private Cortex cortex;

  /// Validates that scope closes registered resources when scope is closed.
  ///
  /// When a scope closes, every registered resource closes. Each circuit is
  /// checked through an open-required operation, which §9.1 requires to raise
  /// a Fault synchronously once the circuit has accepted close.
  ///
  /// Expected: All three circuits are closed when scope closes
  /// Closing a Scope closes every registered resource.
  @SpecRef("9.2")
  @Test
  void close_multipleRegisteredResources_closesEveryResource() {

    final var scope = cortex.scope();

    final var c1 = cortex.circuit(cortex.name("scope.all.first"));
    final var c2 = cortex.circuit(cortex.name("scope.all.second"));
    final var c3 = cortex.circuit(cortex.name("scope.all.third"));

    scope.register(c1);
    scope.register(c2);
    scope.register(c3);

    // Emit to each circuit to prove they're alive
    final var conduit1 = c1.conduit(Integer.class);
    final var conduit2 = c2.conduit(Integer.class);
    final var conduit3 = c3.conduit(Integer.class);

    final var pipe1 = conduit1.get(cortex.name("p1"));
    final var pipe2 = conduit2.get(cortex.name("p2"));
    final var pipe3 = conduit3.get(cortex.name("p3"));

    pipe1.emit(1);
    pipe2.emit(2);
    pipe3.emit(3);

    c1.await();
    c2.await();
    c3.await();

    // Close scope — should close all three circuits
    scope.close();

    // Each circuit has accepted close: an open-required operation raises
    assertThrows(Fault.class, () -> c1.conduit(Integer.class));
    assertThrows(Fault.class, () -> c2.conduit(Integer.class));
    assertThrows(Fault.class, () -> c3.conduit(Integer.class));

    // and a pure queued emit is still a silent no-op (§9.1)
    assertDoesNotThrow(() -> pipe1.emit(99));
    assertDoesNotThrow(() -> pipe2.emit(99));
    assertDoesNotThrow(() -> pipe3.emit(99));

  }

  /// Validates that scope.close() releases registered resources in
  /// **reverse registration order** (LIFO).
  ///
  /// This is a normative requirement: SPEC.md §16.1 item 11 (Scope ordering)
  /// — "Resources registered with a scope MUST close in reverse registration
  /// order."
  ///
  /// The existing scope-close test verifies that all registered resources
  /// *are* closed; this test verifies *the order in which they close*. The
  /// distinction matters for any user code that depends on dependency-safe
  /// teardown (e.g., a derived resource that must be torn down before the
  /// resource it derives from).
  ///
  /// Test design: register three Subscriptions with the scope, each with an
  /// `onClose` callback that records its registration index. Close the scope.
  /// Assert the recorded order is reverse-of-registration: [3,2,1].
  /// Scope closes resources in reverse registration order.
  @SpecRef("9.2")
  @Test
  void close_multipleRegisteredResources_usesReverseRegistrationOrder() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(
          cortex.name("scope.lifo.conduit"),
          Integer.class
        );

      final var scope = cortex.scope();

      final List< Integer > closeOrder = new ArrayList<>();

      // Three subscriptions, each onClose appends its index. The onClose
      // callbacks fire on the circuit thread, which serializes appends.
      final var sub1 =
        conduit.subscribe(
          circuit.subscriber(
            cortex.name("scope.lifo.sub1"),
            (_, _) -> {
            }
          ),
          _ -> closeOrder.add(1)
        );

      final var sub2 =
        conduit.subscribe(
          circuit.subscriber(
            cortex.name("scope.lifo.sub2"),
            (_, _) -> {
            }
          ),
          _ -> closeOrder.add(2)
        );

      final var sub3 =
        conduit.subscribe(
          circuit.subscriber(
            cortex.name("scope.lifo.sub3"),
            (_, _) -> {
            }
          ),
          _ -> closeOrder.add(3)
        );

      scope.register(sub1);
      scope.register(sub2);
      scope.register(sub3);

      // Close the scope. Per §16.1 item 11, registered resources must close
      // in reverse registration order: sub3, then sub2, then sub1.
      scope.close();

      // onClose callbacks fire asynchronously on the circuit thread; await
      // to ensure all three have run before asserting.
      circuit.await();

      assertEquals(
        List.of(3, 2, 1),
        closeOrder,
        "scope.close() must close registered resources in reverse registration order (LIFO)"
      );

    } finally {

      circuit.close();

    }

  }

  /// The Java projection closes a scope's resources and child scopes together in reverse creation
  /// order, each child closing completely before the next older member. This interleaving is
  /// specified by Scope.close(), beyond portable §9.2's LIFO and child-closure requirements, so
  /// this test deliberately has no SpecRef.
  @Test
  void close_nestedScopes_closesMembersInReverseCreationOrder() {

    final var circuit = cortex.circuit();

    try (var root = cortex.scope()) {

      final var conduit = circuit.conduit(Integer.class);
      final List< String > closeOrder = new ArrayList<>();
      final var subscriber = circuit.< Integer > subscriber(
        cortex.name("scope.phases"), (_, _) -> {
        }
      );

      // Children are created between the root's registrations, so neither "resources first"
      // nor "children first" produces the expected order.
      root.register(conduit.subscribe(subscriber, _ -> closeOrder.add("root.1")));
      final var child = root.scope();
      child.register(conduit.subscribe(subscriber, _ -> closeOrder.add("child.1")));
      final var grandchild = child.scope();
      grandchild.register(conduit.subscribe(subscriber, _ -> closeOrder.add("grandchild.1")));
      child.register(conduit.subscribe(subscriber, _ -> closeOrder.add("child.2")));
      root.register(conduit.subscribe(subscriber, _ -> closeOrder.add("root.2")));
      final var later = root.scope();
      later.register(conduit.subscribe(subscriber, _ -> closeOrder.add("later.1")));
      circuit.await();

      root.close();
      circuit.await();

      assertEquals(
        List.of("later.1", "root.2", "child.2", "grandchild.1", "child.1", "root.1"),
        closeOrder,
        "Scope.close() must close resources and child scopes in reverse creation order"
      );

    } finally {
      circuit.close();
    }

  }

  /// Validates scope hierarchical structure and parent-child relationships.
  ///
  /// This test verifies the hierarchical nature of scopes, including parent-child
  /// relationships, enclosure navigation, and cascading closure behavior:
  ///
  /// Hierarchy Setup:
  /// ```
  ///     root (try-with-resources)
  ///      ├── named ("scope.test.named")
  ///      └── anonymous (auto-generated name)
  /// ```
  ///
  /// Parent-Child Relationships:
  /// - Child scopes created via parent.scope() or parent.scope(name)
  /// - Children maintain reference to parent (enclosure)
  /// - Named children have explicit hierarchical names
  /// - Anonymous children get auto-generated names
  ///
  /// Enclosure Navigation:
  /// - child.enclosure() returns Optional<Scope> of parent
  /// - child.enclosure(Consumer) invokes consumer with parent
  /// - Enables traversal up the scope tree
  /// - Root scope has no enclosure (empty Optional)
  ///
  /// Subject and Path:
  /// - Each scope has a Subject with hierarchical Name
  /// - scope.path() returns full hierarchical path
  /// - scope.toString() returns path string representation
  /// - Enables tracing scope relationships
  ///
  /// Cascading Closure:
  /// - Closing parent scope closes all children
  /// - Closed children reject further operations (Fault)
  /// - Try-with-resources on root ensures proper cleanup
  /// - Prevents partial cleanup bugs
  ///
  /// Critical for resource management:
  /// - Hierarchical scoping (like try-with-resources nesting)
  /// - Automatic cleanup of entire scope tree
  /// - Parent responsible for children's lifecycle
  /// - Prevents orphaned child scopes
  ///
  /// Expected: Children closed when parent closes, enclosure accessible,
  /// hierarchical naming preserved
  /// Closing a parent Scope closes its enclosed child scopes.
  @SpecRef("9.2")
  @Test
  void close_parentScope_closesEnclosedChildren() {

    try (final var root = cortex.scope()) {

      final var named =
        root.scope(cortex.name("scope.test.named"));

      final var anonymous =
        root.scope();

      assertSame(root, named.enclosure().orElseThrow());

      final var captured = new AtomicReference< Scope >();
      named.enclosure(captured::set);

      assertSame(root, captured.get());

      assertEquals(named.path().toString(), named.toString());
      assertNotNull(anonymous.subject());

      root.close();

      assertThrows(Fault.class, named::scope);
      assertThrows(Fault.class, anonymous::scope);

    }

  }

  /// Validates that scope.close() is idempotent.
  ///
  /// SPEC.md §16.1 item 9 (Idempotent close) — "All resource close operations
  /// MUST be idempotent and concurrency-safe." Calling close() more than once
  /// must be safe and must not re-execute close-time side effects.
  ///
  /// This test registers a single subscription whose onClose callback
  /// increments a counter, calls scope.close() multiple times, and asserts
  /// the callback fired exactly once.
  /// Repeated Scope close calls are safe no-ops.
  @SpecRef("9.2")
  @Test
  void close_repeatedScopeCalls_areIdempotent() {

    final var circuit = cortex.circuit();

    try {

      final var conduit =
        circuit.conduit(
          cortex.name("scope.idempotent.conduit"),
          Integer.class
        );

      final var scope = cortex.scope();
      final var closeCount = new AtomicInteger(0);

      final var subscription =
        conduit.subscribe(
          circuit.subscriber(
            cortex.name("scope.idempotent.sub"),
            (_, _) -> {
            }
          ),
          _ -> closeCount.incrementAndGet()
        );

      scope.register(subscription);

      // First close performs the cleanup.
      scope.close();

      // Subsequent closes must be safe no-ops.
      assertDoesNotThrow(scope::close);
      assertDoesNotThrow(scope::close);
      assertDoesNotThrow(scope::close);

      circuit.await();

      assertEquals(
        1,
        closeCount.get(),
        "Repeated scope.close() calls must not re-fire onClose callbacks"
      );

    } finally {

      circuit.close();

    }

  }

  /// Validates that scope close is resilient to individual resource failures.
  ///
  /// A resource already closed before its scope closes does not stop the scope
  /// closing the others.
  ///
  /// This is not the error-suppression case of §9.2: every resource the API
  /// can produce closes without throwing, so a close that raises cannot be
  /// reached from here. The provider's own tests cover suppression.
  ///
  /// Expected: scope.close() completes without exception, all circuits closed
  @SpecRef("9.2")
  @Test
  void close_alreadyClosedResource_othersStillClose() {

    final var scope = cortex.scope();

    // Create circuits — they are Resource implementations
    final var c1 = cortex.circuit(cortex.name("scope.suppress.first"));
    final var c2 = cortex.circuit(cortex.name("scope.suppress.second"));
    final var c3 = cortex.circuit(cortex.name("scope.suppress.third"));

    scope.register(c1);
    scope.register(c2);
    scope.register(c3);

    // Close c2 manually first — closing again during scope.close()
    // exercises the idempotent close path (not an exception, but validates resilience)
    c2.close();

    // Scope close should complete without exception
    assertDoesNotThrow(scope::close);

    // All circuits should be safely closed. Per spec §9.1, synchronous
    // factory operations on a closed resource throw — Fault, not silent
    // inert object — so creating a conduit on a closed circuit raises.
    assertThrows(Fault.class, () -> c1.conduit(Integer.class));
    assertThrows(Fault.class, () -> c3.conduit(Integer.class));

  }

  /// Validates closure lifecycle: reuse semantics and one-time consumption behavior.
  ///
  /// This test demonstrates the complete lifecycle of closures within a scope:
  ///
  /// Phase 1 - Closure Creation:
  /// - Creates closure for a circuit resource
  ///
  /// Phase 2 - Consumption:
  /// - Calls closure.consume() to access the wrapped resource
  /// - Consumer executes immediately while scope is open
  /// - Resource is passed to consumer for one-time use
  ///
  /// Phase 3 - Post-Consumption:
  /// - After consumption, scope.closure(circuit) returns NEW instance
  /// - First closure is "spent" - cannot be reused
  /// - New closure can be consumed again
  ///
  /// This pattern enables safe lazy resource initialization:
  /// - Resources wrapped in closures aren't created until consumed
  /// - Consumption is one-time, preventing accidental reuse
  /// - Scope tracks all closures for cleanup
  /// - After consumption, closure is released and new one can be created
  ///
  /// Critical for resource management:
  /// - Prevents resource leaks (scope closes all)
  /// - Lazy initialization (defer creation until needed)
  /// - One-time consumption prevents double-use bugs
  ///
  /// Expected behavior:
  /// 1. Consumer executes when scope is open
  /// 2. A consumed closure does not run its consumer again
  /// 3. scope.closure(x) after consumption is a new, usable closure
  ///
  /// Whether two closures requested for the same resource before consumption are one object is
  /// not specified, so it is not asserted.
  /// A Closure is single-use and a consumed handle is replaced.
  @SpecRef("9.2")
  @Test
  void closure_consumedHandle_isReplacedForSameResource() {

    final var scope = cortex.scope();
    final var circuit = cortex.circuit();

    final var registered = cortex.circuit();

    try {

      assertSame(registered, scope.register(registered));

      final var first =
        scope.closure(circuit);

      final var invoked = new AtomicBoolean(false);

      first.consume(resource -> {
        invoked.set(true);
        assertSame(circuit, resource);
      });

      assertTrue(invoked.get(), "closure should invoke consumer while scope open");

      first.consume(_ -> fail("a consumed closure must not run its consumer again"));

      final var second =
        scope.closure(circuit);

      assertNotSame(first, second);

      final var reused = new AtomicBoolean(false);

      second.consume(_ -> reused.set(true));

      assertTrue(reused.get(), "a closure requested after consumption should be usable");

    } finally {

      scope.close();
      circuit.close();
      registered.close();

    }

  }

  /// Validates that register and closure reject foreign Resource implementations.
  ///
  /// A scope must only manage resources produced by this provider. Passing a
  /// user-implemented `Subscription` (or any other Resource subtype not from this
  /// provider's package) raises a Fault, consistent with the broader provider-check
  /// pattern used elsewhere in the API.
  ///
  /// Expected: Fault on register and closure when the resource is foreign.
  /// Scope rejects resources supplied by a foreign provider.
  @Test
  void register_foreignProviderResource_throwsFault() {

    final Subscription foreign =
      new Subscription() {
        @Override
        public void close() {
        }

        @Override
        public void closeAwait() {
        }

        // The provider check faults before the subject is read, so this never runs.
        @NotNull
        @SuppressWarnings("DataFlowIssue")
        @Override
        public Subject< Subscription > subject() {
          return null;
        }
      };

    try (final var scope = cortex.scope()) {

      assertThrows(
        Fault.class,
        () -> scope.register(foreign)
      );

      assertThrows(
        Fault.class,
        () -> scope.closure(foreign)
      );

    }

  }

  /// Registering the same resource twice is a safe no-op that preserves its
  /// original close-order position and closes it only once.
  @SpecRef("9.2")
  @Test
  void register_sameResourceTwice_preservesOriginalPositionAndClosesOnce() {

    final var circuit = cortex.circuit();

    try {

      final var conduit = circuit.conduit(Integer.class);
      final var scope = cortex.scope();
      final List< Integer > closeOrder = new ArrayList<>();

      final var first = conduit.subscribe(
        circuit.subscriber(
          cortex.name("scope.duplicate.first"),
          (_, _) -> {
          }
        ),
        _ -> closeOrder.add(1)
      );

      final var second = conduit.subscribe(
        circuit.subscriber(
          cortex.name("scope.duplicate.second"),
          (_, _) -> {
          }
        ),
        _ -> closeOrder.add(2)
      );

      assertSame(first, scope.register(first));
      assertSame(second, scope.register(second));
      assertSame(first, scope.register(first));

      scope.close();
      circuit.await();

      assertEquals(List.of(2, 1), closeOrder);

    } finally {

      circuit.close();

    }

  }

  /// Validates that closing a scope prevents all further operations and consumption.
  ///
  /// This test verifies the safety guarantees of scope closure, ensuring that
  /// once a scope is closed, it becomes inert and rejects all operations:
  ///
  /// Setup:
  /// - Creates scope with a closure wrapping a circuit
  /// - Closes the scope while closure is unconsumed
  ///
  /// Closure Consumption After Close:
  /// - Attempts to consume the closure after scope is closed
  /// - Consumer MUST NOT execute (invoked flag remains false)
  /// - This prevents use-after-close bugs where closed resources are accessed
  /// - Closure silently ignores consumption rather than throwing (fail-safe)
  ///
  /// Registration After Close:
  /// - Attempts to register new resource to closed scope
  /// - MUST raise Fault (fail-fast for invalid operation, consistent with Resource post-close contract)
  /// - Prevents accumulating resources in dead scope
  ///
  /// Child Scope Creation After Close:
  /// - Attempts to create child scope from closed parent
  /// - MUST raise Fault
  /// - Prevents building hierarchy from dead root
  ///
  /// Why this matters:
  /// - Prevents resource leaks (no new registrations after close)
  /// - Prevents use-after-free bugs (closures don't execute after scope close)
  /// - Structured Fault (carries Subject) for programming mistakes
  /// - Fail-safe for closures (silent ignore) vs fail-fast for operations (Fault)
  ///
  /// Design rationale:
  /// - Closures use silent failure (common in cleanup paths)
  /// - Registration/creation raise Fault (synchronous @New on closed receiver)
  /// - Once closed, scope is permanently disabled (no reopen)
  ///
  /// Expected: Closure doesn't execute, operations raise Fault
  /// A closed Scope is terminal and cannot manage new resources.
  @SpecRef("9.2")
  @Test
  void scope_afterClose_rejectsFurtherManagementOperations() {

    final var scope = cortex.scope();
    final var circuit = cortex.circuit();

    try {

      final var closure =
        scope.closure(circuit);

      scope.close();

      final var invoked = new AtomicBoolean(false);

      closure.consume(_ -> invoked.set(true));

      assertFalse(invoked.get(), "closure should not run after scope is closed");

      final var extra = cortex.circuit();

      try {
        assertThrows(
          Fault.class,
          () -> scope.register(extra)
        );
      } finally {
        extra.close();
      }

      assertThrows(
        Fault.class,
        scope::scope
      );

    } finally {

      scope.close();
      circuit.close();

    }

  }

  /// Cortex#scope assigns a valid default subject name when omitted.
  @SpecRef("16.3")
  @Test
  void scope_withoutExplicitName_usesDefaultSubjectName() {

    final var scope = cortex.scope();

    assertNotNull(scope.subject());
    assertNotNull(scope.subject().name());
    assertFalse(scope.subject().name().path().isEmpty());

    scope.close();

  }

  @BeforeEach
  void setUp() {

    cortex = cortex();

  }

}
