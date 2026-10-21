package io.humainary.substrates.tck;

import io.humainary.substrates.api.*;

import java.lang.reflect.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;

/// Common support for substrate TCK classes.
///
/// Provides access to the singleton Cortex and common utility methods
/// for test implementations. All TCK test classes extend this base class
/// to inherit Substrates types and helper methods.
///
/// The SPI provider is configured in the module pom.xml.
/// @author William David Louth
/// @since 1.0
abstract class TestSupport
  implements Substrates {

  private static final long COORDINATION_TIMEOUT_SECONDS =
    Long.getLong("tck.coordination.timeout.seconds", 10L);

  /// Waits for a test coordination gate and reports the causal condition on timeout.
  static void await(
    final CountDownLatch latch,
    final String condition
  ) throws InterruptedException {

    if (!latch.await(COORDINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      throw new AssertionError("Timed out waiting for " + condition);
    }

  }

  /// Polls `condition` until it holds, without permitting an unbounded test hang.
  ///
  /// For a condition no latch can signal, such as a count reached on a thread the
  /// provider schedules. A fixed sleep would assert a latency bound nothing in the
  /// specification gives; this waits as long as the coordination timeout allows.
  static void await(
    final BooleanSupplier condition,
    final String what
  ) throws InterruptedException {

    final long deadline =
      System.nanoTime() + TimeUnit.SECONDS.toNanos(COORDINATION_TIMEOUT_SECONDS);

    while (!condition.getAsBoolean()) {

      if (System.nanoTime() - deadline > 0L) {
        throw new AssertionError("Timed out waiting for " + what);
      }

      Thread.sleep(5L);

    }

  }

  /// Waits for all barrier parties without permitting an unbounded test hang.
  static void await(
    final CyclicBarrier barrier,
    final String condition
  ) throws InterruptedException, BrokenBarrierException {

    try {
      barrier.await(COORDINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final TimeoutException exception) {
      throw new AssertionError("Timed out waiting for " + condition, exception);
    }

  }

  /// Returns the singleton Cortex instance for test use.
  /// @return the cortex instance
  static Cortex cortex() {
    return Substrates.cortex();
  }

  /// Whether `message` identifies `subject`, by its id, its name, or its own
  /// rendering.
  ///
  /// SPEC §15.1 requires a closed-resource error to identify a closed argument's
  /// subject, and lets a projection choose the mechanism; the Java Fault carries
  /// the receiver as its subject and renders offending arguments into its message.
  /// The form of that rendering is the provider's, so any of the three counts.
  static boolean identifies(
    final String message,
    final Subject< ? > subject
  ) {

    return
      message!=null && (
        message.contains(subject.id().toString())
          || message.contains(subject.name().toString())
          || message.contains(subject.toString())
      );

  }

  /// Whether the configured provider offers hierarchical routing (SPEC §10.3).
  ///
  /// The capability is OPTIONAL, and a provider without it MUST behave as if
  /// per-pipe routing were in effect, so nothing can ask whether it is offered:
  /// the only way to know is to look. A test of the capability assumes it, and
  /// is skipped rather than failed where it is absent. A test whose subject is
  /// something else — a factory overload passing the routing on — expects
  /// per-pipe behaviour instead, since that is still a requirement then.
  /// @return true if a leaf emission on a STEM conduit reaches its ancestor
  static boolean hierarchicalRouting() {

    var offered = hierarchicalRoutingProbed;

    if (offered==null) {

      synchronized (TestSupport.class) {

        offered = hierarchicalRoutingProbed;

        if (offered==null) {
          hierarchicalRoutingProbed = offered = probeHierarchicalRouting();
        }

      }

    }

    return offered;

  }

  /// The probe's result, computed once on first use so the suite pays for one
  /// circuit however many tests ask. Lazily rather than in a holder class's
  /// initializer: the subscriber callback below runs on the circuit's worker,
  /// and a lambda declared in a class still being initialized would block that
  /// worker on the initialization while this thread waits for the worker.
  private static volatile Boolean hierarchicalRoutingProbed;

  private static boolean probeHierarchicalRouting() {

    final var cortex = Substrates.cortex();
    final var circuit = cortex.circuit();

    try {

      final var ancestor = cortex.name("probe");
      final var reached = new AtomicBoolean();

      final Conduit< Integer > conduit =
        circuit.conduit(cortex.name("routing.probe"), Integer.class, Routing.STEM);

      conduit.subscribe(
        circuit.subscriber(
          cortex.name("routing.probe.observer"),
          (subject, registrar) -> {
            if (ancestor.equals(subject.name())) {
              registrar.register(_ -> reached.set(true));
            }
          }
        )
      );

      conduit.get(cortex.name("probe.leaf")).emit(1);
      circuit.await();

      return reached.get();

    } finally {

      circuit.close();

    }

  }

  /// Creates a public-interface stub that cannot belong to the configured provider.
  @SuppressWarnings("unchecked")
  static < T > T foreignProviderStub(
    final Class< ? > contract
  ) {

    return
      (T) Proxy.newProxyInstance(
        contract.getClassLoader(),
        new Class< ? >[]{contract},
        (proxy, method, arguments) -> {
          if (method.getDeclaringClass()==Object.class) {
            return
              switch (method.getName()) {
                case "equals" -> proxy==arguments[0];
                case "hashCode" -> System.identityHashCode(proxy);
                case "toString" -> "foreign-provider " + contract.getSimpleName();
                default -> throw new AssertionError(method);
              };
          }

          final var returnType = method.getReturnType();

          return
            returnType.isPrimitive() && returnType!=void.class
              ? Array.get(Array.newInstance(returnType, 1), 0)
              :null;
        }
      );

  }

  /// Retrieves asynchronous test work with the shared coordination bound.
  static < T > T get(
    final Future< T > future,
    final String condition
  ) throws InterruptedException, ExecutionException {

    try {
      return future.get(COORDINATION_TIMEOUT_SECONDS, TimeUnit.SECONDS);
    } catch (final TimeoutException exception) {
      throw new AssertionError("Timed out waiting for " + condition, exception);
    }

  }

  /// Joins a test thread with the shared coordination bound.
  static void join(
    final Thread thread,
    final String condition
  ) throws InterruptedException {

    thread.join(TimeUnit.SECONDS.toMillis(COORDINATION_TIMEOUT_SECONDS));
    if (thread.isAlive()) {
      thread.interrupt();
      throw new AssertionError("Timed out waiting for " + condition);
    }

  }

}
