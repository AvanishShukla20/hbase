package org.apache.hadoop.hbase.hdfs.tier.access;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors metadata update operations and tracks success/failure rates.
 *
 * PURPOSE: Detect systematic failures in metadata updates.
 * THREAD-SAFE: All operations use atomic counters.
 * ALERTING: Logs critical errors when failure rate exceeds threshold.
 */
public class MetadataUpdateMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(MetadataUpdateMonitor.class);

  private final AtomicLong successCount = new AtomicLong(0);
  private final AtomicLong failureCount = new AtomicLong(0);
  private final AtomicLong pendingCount = new AtomicLong(0);

  // Track individual failed updates for retry
  private final ConcurrentHashMap<String, FailedUpdate> failedUpdates = new ConcurrentHashMap<>();

  private final double failureThreshold;
  private final int maxTrackedFailures;

  public MetadataUpdateMonitor(Configuration conf) {
    // Alert if >5% of updates fail
    this.failureThreshold = conf.getDouble("hbase.metadata.update.failure.threshold", 0.05);

    // Limit memory usage - track max 10K failed updates
    this.maxTrackedFailures = conf.getInt("hbase.metadata.max.tracked.failures", 10000);

    LOG.info("MetadataUpdateMonitor initialized with failure threshold: {}%",
        failureThreshold * 100);
  }

  /**
   * Called when metadata update is queued for processing.
   */
  public void recordUpdateAttempt(String hfilePath) {
    pendingCount.incrementAndGet();
    if (LOG.isTraceEnabled()) {
      LOG.trace("Queued update for: {}", hfilePath);
    }
  }

  /**
   * Called when metadata update succeeds.
   * Clears any previous failure tracking for this HFile.
   */
  public void recordSuccess(String hfilePath) {
    pendingCount.decrementAndGet();
    successCount.incrementAndGet();
    failedUpdates.remove(hfilePath);
  }

  /**
   * Called when metadata update fails after all retries.
   * Tracks the failure for potential later recovery.
   */
  public void recordFailure(String hfilePath, Exception e) {
    pendingCount.decrementAndGet();
    long failCount = failureCount.incrementAndGet();

    // Track failed update if under limit
    if (failedUpdates.size() < maxTrackedFailures) {
      FailedUpdate failed = new FailedUpdate(hfilePath, e, System.currentTimeMillis());
      failedUpdates.put(hfilePath, failed);
    }

    // Check if failure rate exceeds threshold
    long total = successCount.get() + failCount;
    if (total >= 100) { // Only check after 100 operations
      double failureRate = (double) failCount / total;

      if (failureRate > failureThreshold) {
        LOG.error("Metadata update failure rate {}% exceeds threshold {}%. " +
            "Success: {}, Failures: {}, Pending: {}",
            String.format("%.2f", failureRate * 100),
            String.format("%.2f", failureThreshold * 100),
            successCount.get(), failCount, pendingCount.get());
      }
    }
  }

  /**
   * Check if the system is healthy based on failure rate.
   */
  public boolean isHealthy() {
    long total = successCount.get() + failureCount.get();
    if (total < 10) return true; // Not enough data

    double failureRate = (double) failureCount.get() / total;
    return failureRate <= failureThreshold;
  }

  /**
   * Get current statistics snapshot.
   */
  public MetadataUpdateStats getStats() {
    return new MetadataUpdateStats(
        successCount.get(),
        failureCount.get(),
        pendingCount.get(),
        failedUpdates.size()
    );
  }

  /**
   * Get map of failed updates for retry processing.
   */
  public ConcurrentHashMap<String, FailedUpdate> getFailedUpdates() {
    return new ConcurrentHashMap<>(failedUpdates);
  }

  /**
   * Remove a failed update from tracking (after successful retry).
   */
  public void removeFailedUpdate(String hfilePath) {
    failedUpdates.remove(hfilePath);
  }

  /**
   * Reset all counters (for testing or manual intervention).
   */
  public void reset() {
    successCount.set(0);
    failureCount.set(0);
    pendingCount.set(0);
    failedUpdates.clear();
    LOG.info("MetadataUpdateMonitor counters reset");
  }

  /**
   * Represents a single failed metadata update attempt.
   */
  public static class FailedUpdate {
    public final String hfilePath;
    public final Exception exception;
    public final long timestamp;
    public int retryCount = 0;

    FailedUpdate(String path, Exception e, long ts) {
      this.hfilePath = path;
      this.exception = e;
      this.timestamp = ts;
    }

    @Override
    public String toString() {
      return String.format("FailedUpdate{path=%s, retries=%d, age=%dms}",
          hfilePath, retryCount, System.currentTimeMillis() - timestamp);
    }
  }

  /**
   * Immutable statistics snapshot.
   */
  public static class MetadataUpdateStats {
    public final long successCount;
    public final long failureCount;
    public final long pendingCount;
    public final int failedUpdatesCount;

    MetadataUpdateStats(long success, long failure, long pending, int failed) {
      this.successCount = success;
      this.failureCount = failure;
      this.pendingCount = pending;
      this.failedUpdatesCount = failed;
    }

    public double getFailureRate() {
      long total = successCount + failureCount;
      return total == 0 ? 0.0 : (double) failureCount / total;
    }

    public long getTotalProcessed() {
      return successCount + failureCount;
    }

    @Override
    public String toString() {
      return String.format("MetadataUpdateStats{success=%d, failure=%d, pending=%d, " +
          "failedTracked=%d, failureRate=%.2f%%, total=%d}",
          successCount, failureCount, pendingCount, failedUpdatesCount,
          getFailureRate() * 100, getTotalProcessed());
    }
  }
}
