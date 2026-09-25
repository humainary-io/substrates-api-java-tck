package io.humainary.substrates.tck;

import io.humainary.substrates.api.*;

import java.lang.reflect.*;
import java.util.concurrent.*;

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
