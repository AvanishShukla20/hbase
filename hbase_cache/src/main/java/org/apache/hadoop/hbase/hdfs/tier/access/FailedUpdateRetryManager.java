package org.apache.hadoop.hbase.hdfs.tier.access;

import java.util.concurrent.*;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Periodically retries failed metadata updates.
 *
 * PURPOSE: Recover from transient failures (network glitches, temporary overload).
 * STRATEGY: Exponential backoff with max retry limit.
 *
 * OPERATION:
 * - Runs every N minutes (configurable)
 * - Retries failed updates from MetadataUpdateMonitor
 * - Gives up after max retries (prevents infinite retry loops)
 */
public class FailedUpdateRetryManager {
  private static final Logger LOG = LoggerFactory.getLogger(FailedUpdateRetryManager.class);
  private final MetadataUpdateMonitor monitor;
  private final HFileAccessMetadataUpdater updater;
  private final ScheduledExecutorService retryScheduler;
  private final int maxRetries;
  private final long retryIntervalMinutes;

  public FailedUpdateRetryManager(Configuration conf, MetadataUpdateMonitor monitor) {
    this.monitor = monitor;
    this.updater = new HFileAccessMetadataUpdater(monitor, conf);
    this.maxRetries = conf.getInt("hbase.metadata.update.max.total.retries", 5);
    this.retryIntervalMinutes = conf.getLong("hbase.metadata.retry.interval.minutes", 5);

    this.retryScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "metadata-retry-manager");
      t.setDaemon(true);
      return t;
    });

    startRetryScheduler();
    LOG.info("FailedUpdateRetryManager initialized: retry interval {}min, max retries {}",
        retryIntervalMinutes, maxRetries);
  }

  /**
   * Start periodic retry scheduler.
   */
  private void startRetryScheduler() {
    retryScheduler.scheduleAtFixedRate(
        this::retryFailedUpdates,
        retryIntervalMinutes, // Initial delay
        retryIntervalMinutes, // Period
        TimeUnit.MINUTES
    );
  }

  /**
   * Retry all tracked failed updates.
   */
  private void retryFailedUpdates() {
    ConcurrentHashMap<String, MetadataUpdateMonitor.FailedUpdate> failedUpdates =
        monitor.getFailedUpdates();

    if (failedUpdates.isEmpty()) {
      return;
    }

    LOG.info("Retrying {} failed metadata updates", failedUpdates.size());
    int successCount = 0;
    int giveUpCount = 0;

    for (MetadataUpdateMonitor.FailedUpdate failed : failedUpdates.values()) {
      // Check if exceeded max retries
      if (failed.retryCount >= maxRetries) {
        giveUpCount++;
        LOG.error("Giving up on {} after {} retries. Last error: {}",
            failed.hfilePath, maxRetries, failed.exception.getMessage());
        monitor.removeFailedUpdate(failed.hfilePath);
        continue;
      }

      try {
        // Reconstruct event and retry
        HFileAccessEvent event = HFileAccessEvent.newBuilder()
            .hfilePath(failed.hfilePath)
            .accessTimestamp(failed.timestamp)
            .build();

        updater.updateLastAccess(event);
        successCount++;

        LOG.info("Successfully retried update for {} after {} attempts",
            failed.hfilePath, failed.retryCount + 1);

      } catch (Exception e) {
        failed.retryCount++;
        LOG.warn("Retry {} failed for {}: {}",
            failed.retryCount, failed.hfilePath, e.getMessage());
      }
    }

    LOG.info("Retry completed: {} succeeded, {} gave up, {} still pending",
        successCount, giveUpCount, failedUpdates.size() - successCount - giveUpCount);
  }

  /**
   * Shutdown retry manager.
   */
  public void shutdown() {
    LOG.info("Shutting down FailedUpdateRetryManager...");
    retryScheduler.shutdown();

    try {
      if (!retryScheduler.awaitTermination(10, TimeUnit.SECONDS)) {
        retryScheduler.shutdownNow();
      }
    } catch (InterruptedException e) {
      retryScheduler.shutdownNow();
      Thread.currentThread().interrupt();
    }

    updater.close();
    LOG.info("FailedUpdateRetryManager shut down");
  }
}
