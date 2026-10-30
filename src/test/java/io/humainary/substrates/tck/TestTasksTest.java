package io.humainary.substrates.tck;

import org.junit.jupiter.api.*;

import java.nio.file.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.junit.jupiter.api.Assertions.*;

/// Tests the harness itself, with no provider or portable-contract claims.
@Tag("harness-check")
final class TestTasksTest {

  /// A child JVM deliberately leaves a worker in an interruption-resistant wait.
  /// If cleanup waits, or the worker is non-daemon, the child cannot exit. The
  /// parent bounds the entire experiment and forcibly reaps a failing child.
  @SuppressWarnings("resource")
  @Test
  void cleanupCancelsWorkWithoutPreventingJvmExit() throws Exception {

    for (final var mode : new String[]{"fixed", "virtual"}) {

      final var output = Files.createTempFile("tck-worker-cleanup-", ".log");
      Process process = null;

      try {

        process = new ProcessBuilder(
          Path.of(System.getProperty("java.home"), "bin", "java").toString(),
          "-cp",
          // The child uses only TestTasks and this nested main, plus JDK classes.
          // Resolve their actual location instead of depending on a test runner's classpath.
          Path.of(TestTasks.class.getProtectionDomain().getCodeSource().getLocation().toURI()).toString(),
          StalledWorker.class.getName(),
          mode
        ).redirectErrorStream(true).redirectOutput(output.toFile()).start();

        assertTrue(process.waitFor(10, SECONDS),
          mode + " cleanup prevented JVM exit: " + Files.readString(output));
        assertEquals(0, process.exitValue(), Files.readString(output));
        assertTrue(Files.readString(output).contains("cleanup completed"));

      } finally {

        if (process!=null && process.isAlive()) {
          process.destroyForcibly();
          assertTrue(process.waitFor(5, SECONDS), "could not reap worker-cleanup child JVM");
        }
        Files.deleteIfExists(output);

      }

    }

  }

  @Test
  void completionPreservesResultsAndReportsWorkerFailures() throws Exception {

    final var failure = new IllegalStateException("worker failed");

    try (final var tasks = TestTasks.fixed(1)) {

      final var result = tasks.submit(() -> 42);
      final var failed = tasks.submit(() -> {
        throw failure;
      });

      assertEquals(42, TestSupport.get(result, "the worker result"));
      assertSame(failure, assertThrows(
        ExecutionException.class,
        () -> TestSupport.get(failed, "the failing worker")
      ).getCause());

    }

  }

  public static final class StalledWorker {

    @SuppressWarnings("InfiniteLoopStatement")
    static void main(String[] args) throws Exception {

      final boolean fixed = args[0].equals("fixed");
      final var entered = new CountDownLatch(1);
      final var neverReleased = new CountDownLatch(1);
      final var queuedRan = new AtomicBoolean();

      try (final var tasks = fixed ? TestTasks.fixed(1):TestTasks.virtual()) {

        final var running = tasks.submit(() -> {
          if (!Thread.currentThread().isDaemon()) {
            throw new AssertionError("worker is not daemon");
          }
          entered.countDown();
          while (true) {
            try {
              neverReleased.await();
            } catch (final InterruptedException ignored) {
              // Model a provider operation that does not abort on interruption.
            }
          }
        });

        if (!entered.await(5, SECONDS)) {
          throw new AssertionError("worker never entered");
        }

        // With one occupied fixed worker, this second task must stay queued.
        final var queued = fixed ? tasks.submit(() -> queuedRan.set(true)):null;

        tasks.close();

        if (!running.isCancelled() || fixed && (!queued.isCancelled() || queuedRan.get())) {
          throw new AssertionError("running or queued work escaped cancellation");
        }

        System.out.println("cleanup completed");

      }

    }

  }

}
