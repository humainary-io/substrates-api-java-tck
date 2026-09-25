package io.humainary.substrates.tck;

import java.util.*;
import java.util.concurrent.*;

/// Test-thread-confined ownership of asynchronous test work. Fixed pools retain
/// their worker count and queueing; virtual workers run one task per thread.
/// Both use daemon threads, so a broken provider cannot keep the JVM alive.
///
/// Tests must check successful completion explicitly with TestSupport.get before
/// cleanup. close cancels tracked futures and requests shutdown without waiting:
/// a provider may ignore interruption. Such tasks can outlive the test, although
/// they cannot prevent JVM exit. Gates and circuits remain the test's responsibility.
final class TestTasks implements AutoCloseable {

  private final ExecutorService executor;
  private final List< Future< ? > > futures = new ArrayList<>();

  private TestTasks(ExecutorService executor) {
    this.executor = executor;
  }

  static TestTasks fixed(int workers) {
    return new TestTasks(Executors.newFixedThreadPool(
      workers,
      Thread.ofPlatform().daemon().name("tck-worker-", 0).factory()
    ));
  }

  static TestTasks virtual() {
    return new TestTasks(Executors.newThreadPerTaskExecutor(
      Thread.ofVirtual().name("tck-worker-", 0).factory()
    ));
  }

  @Override
  public void close() {
    futures.forEach(future -> future.cancel(true));
    executor.shutdownNow();
    futures.clear();
  }

  < T > Future< T > submit(Callable< T > task) {
    final var future = executor.submit(task);
    futures.add(future);
    return future;
  }

  Future< ? > submit(Runnable task) {
    final var future = executor.submit(task);
    futures.add(future);
    return future;
  }

}
