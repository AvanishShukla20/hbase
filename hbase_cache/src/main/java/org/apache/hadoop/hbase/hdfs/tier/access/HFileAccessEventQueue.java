package org.apache.hadoop.hbase.hdfs.tier.access;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Asynchronous event queue for HFile access tracking.
 *
 * DESIGN GOALS:
 * 1. Non-blocking enqueue
 * 2. Scalable processing (configurable worker threads)
 * 3. Graceful degradation
 * 4. Observable (metrics for queue health)
 *
 * CONCURRENCY ASPECT:
 * - Multiple RegionServer threads enqueue events (thread-safe LinkedBlockingQueue)
 * - Multiple worker threads process events in parallel
 * - Monitor thread logs statistics periodically
 *
 * FAILURE HANDLING:
 * - Queue full → perform work synchronously without dropping request
 * - Worker error → log and continue (doesn't crash other workers)
 * - Shutdown → drain queue with timeout
 */

public class HFileAccessEventQueue {
  private static final Logger LOG = LoggerFactory.getLogger(HFileAccessEventQueue.class);

  private static volatile HFileAccessEventQueue instance;

  private final LinkedBlockingQueue<HFileAccessEvent> eventQueue;
  private final ExecutorService executorService;
  private final MetadataUpdateMonitor monitor;

  private final AtomicLong fallbackProcessedEvents = new AtomicLong(0);
  private final AtomicLong enqueuedEvents = new AtomicLong(0);

  private final int maxQueueSize;
  private final int workerThreads;
  private volatile boolean running = true;

  private HFileAccessEventQueue(Configuration conf) {
    // Queue size: 200K events = ~20MB memory (typical event ~100 bytes)
    this.maxQueueSize = conf.getInt("hbase.hfile.access.queue.size", 200000);

    // Worker threads: Auto-tune to CPU cores, but keep reasonable bounds
    // Formula: cores/4, min 2, max 8
    int cores = Runtime.getRuntime().availableProcessors();
    int defaultWorkers = Math.max(2, Math.min(8, cores / 4));
    this.workerThreads = conf.getInt("hbase.hfile.access.worker.threads", defaultWorkers);

    this.eventQueue = new LinkedBlockingQueue<>(maxQueueSize);
    this.monitor = new MetadataUpdateMonitor(conf);

    // Create daemon thread pool for workers
    ThreadFactory threadFactory = new ThreadFactory() {
      private int counter = 0;
      @Override
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r, "hfile-access-worker-" + counter++);
        t.setDaemon(true);
        return t;
      }
    };
    this.executorService = Executors.newFixedThreadPool(workerThreads, threadFactory);

    startWorkers();
    startMonitoring(conf);

    LOG.info("HFileAccessEventQueue initialized: {} workers, queue size: {}",
        workerThreads, maxQueueSize);
  }

  /**
   * Singleton accessor with double-checked locking.
   * Share queue across all RegionServer operations.
   */
  public static HFileAccessEventQueue getInstance(Configuration conf) {
    if (instance == null) {
      synchronized (HFileAccessEventQueue.class) {
        if (instance == null) {
          instance = new HFileAccessEventQueue(conf);
        }
      }
    }
    return instance;
  }

  /**
   * Enqueue an access event for processing.
   *
   * PERFORMANCE: O(1) operation, typically non-blocking.
   * FALLBACK STRATEGY: If queue full, process synchronously to avoid dropping.
   *
   * BEHAVIOR:
   * - Normal case: Enqueue and return immediately
   * - Queue full: Process synchronously (blocks caller thread)
   * - Never drops events
   */
  public boolean enqueue(HFileAccessEvent event) {
    if (!running) {
      return false;
    }

    boolean added = eventQueue.offer(event);

    if (added) {
      enqueuedEvents.incrementAndGet();
      monitor.recordUpdateAttempt(event.getHfilePath());
    } else {
      // Queue full - handle synchronously instead of dropping
      handleSynchronousFallback(event);
    }

    return added;
  }

  /**
   * Fallback handler when queue is full.
   * Processes the event synchronously to avoid data loss.
   *
   * WHY SYNCHRONOUS FALLBACK:
   * - Prevents metadata loss during high load
   * - Caller thread blocks, but ensures consistency
   * - IDEA : Temporary performance impact acceptable. Access data loss is not acceptable.
   *
   * PERFORMANCE:
   * - Only triggered when queue is full
   * - Adds 10-50ms latency to caller
   * - Self-throttles the system naturally
   */
  private void handleSynchronousFallback(HFileAccessEvent event) {
    long fallbackCount = fallbackProcessedEvents.incrementAndGet();

    // Log warning periodically
    if (fallbackCount % 100 == 0) {
      LOG.warn("Queue full ({} events), processing synchronously. " +
          "Fallback count: {}. Consider increasing queue size or worker threads.",
          maxQueueSize, fallbackCount);
    }

    try {
      // Create dedicated updater for synchronous processing
      Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
      HFileAccessMetadataUpdater syncUpdater = new HFileAccessMetadataUpdater(monitor, conf);

      // Record attempt
      monitor.recordUpdateAttempt(event.getHfilePath());

      // Process synchronously - blocks caller thread
      syncUpdater.updateLastAccess(event);

      // Cleanup
      syncUpdater.close();

      if (LOG.isDebugEnabled()) {
        LOG.debug("Synchronous fallback succeeded for: {}", event.getHfilePath());
      }

    } catch (Exception e) {
      // Even synchronous processing failed - log error
      LOG.error("Synchronous fallback processing failed for {}: {}",
          event.getHfilePath(), e.getMessage(), e);
      monitor.recordFailure(event.getHfilePath(), e);
    }
  }

  /**
   * Start worker threads to process events.
   */
  private void startWorkers() {
    for (int i = 0; i < workerThreads; i++) {
      executorService.submit(new HFileAccessWorker(i));
    }
  }

  /**
   * Start monitoring thread for periodic stats logging.
   */
  private void startMonitoring(Configuration conf) {
    long monitorIntervalSec = conf.getLong("hbase.hfile.access.monitor.interval.sec", 60);

    Thread monitorThread = new Thread(() -> {
      while (running) {
        try {
          Thread.sleep(monitorIntervalSec * 1000);
          logStatistics();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }, "hfile-access-monitor");

    monitorThread.setDaemon(true);
    monitorThread.start();
  }

  /**
   * Log queue health statistics.
   */
  private void logStatistics() {
    MetadataUpdateMonitor.MetadataUpdateStats stats = monitor.getStats();

    LOG.info("HFileAccessQueue Stats: Queue size: {}/{}, Enqueued: {}, " +
        "SyncFallback: {}, {}",
        eventQueue.size(), maxQueueSize,
        enqueuedEvents.get(), fallbackProcessedEvents.get(),
        stats);

    // Alert if system is unhealthy
    if (!monitor.isHealthy()) {
      LOG.error("Metadata update system is UNHEALTHY - {}", stats);
    }
  }

  /**
   * Graceful shutdown with queue draining.
   */
  public void shutdown() {
    LOG.info("Shutting down HFileAccessEventQueue...");
    running = false;

    executorService.shutdown();
    try {
      // Wait 30 seconds for workers to finish processing queue
      if (!executorService.awaitTermination(30, TimeUnit.SECONDS)) {
        LOG.warn("Workers did not finish in 30s, forcing shutdown");
        executorService.shutdownNow();
      }
    } catch (InterruptedException e) {
      executorService.shutdownNow();
      Thread.currentThread().interrupt();
    }

    logStatistics();
    LOG.info("HFileAccessEventQueue shut down. Remaining events: {}", eventQueue.size());
  }

  /**
   * Worker thread that processes events from the queue.
   */
  private class HFileAccessWorker implements Runnable {
    private final int workerId;
    private final HFileAccessMetadataUpdater updater;
    private long processedCount = 0;

    HFileAccessWorker(int workerId) {
      this.workerId = workerId;
      Configuration conf = org.apache.hadoop.hbase.HBaseConfiguration.create();
      this.updater = new HFileAccessMetadataUpdater(monitor, conf);
    }

    @Override
    public void run() {
      LOG.info("Worker-{} started", workerId);

      while (running || !eventQueue.isEmpty()) {
        try {
          // Poll with timeout to allow periodic checks of running flag
          HFileAccessEvent event = eventQueue.poll(1, TimeUnit.SECONDS);

          if (event != null) {
            processEvent(event);
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          // Catch all exceptions to prevent worker death
          LOG.error("Worker-{} caught unexpected exception", workerId, e);
        }
      }

      // Cleanup
      updater.close();
      LOG.info("Worker-{} stopped after processing {} events", workerId, processedCount);
    }

    /**
     * Process a single access event.
     * FAIL-SAFE: Never throws exceptions.
     */
    private void processEvent(HFileAccessEvent event) {
      try {
        updater.updateLastAccess(event);
        processedCount++;
      } catch (Exception e) {
        // Should not reach here (updater handles its own errors)
        // But defensive catch to prevent worker death
        LOG.error("Worker-{} failed to process event: {}", workerId, event, e);
        monitor.recordFailure(event.getHfilePath(), e);
      }
    }
  }

  // Accessors for monitoring/testing

  public long getQueueSize() {
    return eventQueue.size();
  }

  public long getEnqueuedEvents() {
    return enqueuedEvents.get();
  }

  public long getFallbackProcessedEvents() {
    return fallbackProcessedEvents.get();
  }

  public MetadataUpdateMonitor getMonitor() {
    return monitor;
  }

  public boolean isRunning() {
    return running;
  }
}
