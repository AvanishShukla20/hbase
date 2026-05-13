package org.apache.hadoop.hbase.hdfs.tier.eviction;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.hdfs.tier.HdfsTierMetadataCapture;
import org.apache.hadoop.hbase.hdfs.tier.HdfsTierStorageMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates HFile eviction from HDFS cache tier.
 *
 * RESPONSIBILITIES:
 * 1. Coordinate eviction policy selection and execution
 * 2. Calculate bytes to evict based on thresholds
 * 3. Delegate file selection to EvictionPolicy
 * 4. Execute eviction via HDFSTierEvictionExecutor
 * 5. Ensure storage returns to acceptable limits
 *
 * ARCHITECTURE:
 * HDFSTierEvictionChore (periodic trigger)
 *   └─> HDFSTierEvictionCoordinator (orchestration)
 *         ├─> EvictionPolicy (strategy - selects files)
 *         │     └─> LRUEvictionPolicy
 *         └─> HDFSTierEvictionExecutor (executes eviction)
 *
 * CONFIGURATION:
 * - hbase.hdfstier.eviction.policy: Policy name (default: LRU)
 * - hbase.hdfstier.eviction.threshold: % usage to trigger eviction (default: 90)
 * - hbase.hdfstier.eviction.target: % usage to evict down to (default: 70)
 * - hbase.hdfstier.eviction.enabled: Enable/disable eviction (default: true)
 * - hbase.hdfstier.eviction.chore.period.sec: Chore check interval (default: 300)
 *
 * @see HDFSTierEvictionChore for periodic trigger mechanism
 */
public class HDFSTierEvictionCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(HDFSTierEvictionCoordinator.class);

  private final Configuration conf;
  private final HdfsTierStorageMonitor storageMonitor;
  private final HdfsTierMetadataCapture metadataCapture;
  private final HDFSTierEvictionExecutor executor;

  private EvictionPolicy evictionPolicy;

  // Configuration parameters
  private final boolean evictionEnabled;
  private final double evictionThresholdPercent;
  private final double evictionTargetPercent;
  private final long maxStorageBytes;

  // Statistics
  private volatile long lastEvictionTime = 0;
  private volatile long totalBytesEvicted = 0;
  private volatile int evictionRuns = 0;

  public HDFSTierEvictionCoordinator(Configuration conf,
                                     HdfsTierStorageMonitor storageMonitor,
                                     HdfsTierMetadataCapture metadataCapture) throws IOException {
    this.conf = conf;
    this.storageMonitor = storageMonitor;
    this.metadataCapture = metadataCapture;

    // Load configuration
    this.evictionEnabled = conf.getBoolean("hbase.hdfstier.eviction.enabled", true);
    this.evictionThresholdPercent = conf.getDouble("hbase.hdfstier.eviction.threshold", 90.0);
    this.evictionTargetPercent = conf.getDouble("hbase.hdfstier.eviction.target", 70.0);
    this.maxStorageBytes = conf.getLong("hbase.hdfstier.storage.max.bytes",
                                        100L * 1024 * 1024 * 1024); // 100GB default

    // Initialize executor
    this.executor = new HDFSTierEvictionExecutor(conf, metadataCapture);

    // Create eviction policy from configuration
    this.evictionPolicy = EvictionPolicyFactory.createPolicy(conf);

    LOG.info("HDFSTierEvictionCoordinator initialized: enabled={}",
             evictionEnabled);
  }

  /**
   * Perform eviction to bring storage usage to target level.
   * This method is called by HDFSTierEvictionChore periodically.
   *
   * @param currentUsageBytes Current storage usage in bytes
   */
  public void performEviction(long currentUsageBytes) {
    evictionRuns++;
    lastEvictionTime = System.currentTimeMillis();

    // Calculate target bytes to evict
    long targetUsageBytes = (long) (maxStorageBytes * evictionTargetPercent / 100.0);
    long bytesToEvict = currentUsageBytes - targetUsageBytes;

    if (bytesToEvict <= 0) {
      LOG.info("Usage already under limits. No eviction needed");
      return;
    }

    LOG.info("Starting eviction: need to free {} bytes to reach target {}% ({} bytes)",
             bytesToEvict, evictionTargetPercent, targetUsageBytes);

    try {
      // Select files to evict using configured policy
      LOG.info("Selecting files for eviction using {} policy", evictionPolicy.getPolicyName());
      List<HFileEvictionCandidate> filesToEvict = evictionPolicy.selectFilesForEviction(bytesToEvict);

      if (filesToEvict.isEmpty()) {
        LOG.warn("No eviction candidates found - cannot free space");
        return;
      }

      LOG.info("Selected {} files for eviction", filesToEvict.size());

      // Step 2: Execute eviction
      long bytesFreed = executor.executeEviction(filesToEvict);
      totalBytesEvicted += bytesFreed;

      // Step 3: Verify storage usage
      long newUsageBytes = currentUsageBytes - bytesFreed;
      double newUsagePercent = (newUsageBytes * 100.0) / maxStorageBytes;

      LOG.info("Eviction complete: freed {} bytes, new usage: {} bytes ({}%)",
               bytesFreed, newUsageBytes, String.format("%.2f", newUsagePercent));

      // Step 4: Check if we reached target
      if (newUsagePercent > evictionTargetPercent) {
        LOG.warn("Storage usage {}% still above target {}% after eviction",
                 String.format("%.2f", newUsagePercent), String.format("%.2f", evictionTargetPercent));
      } else {
        LOG.info("Successfully reduced storage usage to target level");
      }

    } catch (Exception e) {
      LOG.error("Eviction failed", e);
    }
  }


  /**
   * Get eviction statistics.
   */
  public EvictionStats getStats() {
    return new EvictionStats(
        evictionRuns,
        totalBytesEvicted,
        lastEvictionTime,
        executor.getTotalEvictions(),
        executor.getFailedEvictions(),
        evictionPolicy.getPolicyName()
    );
  }

  /**
   * Shutdown the coordinator and clean up resources.
   */
  public void shutdown() {
    LOG.info("Shutting down HDFSTierEvictionCoordinator");



    // Close policy if it has resources
    if (evictionPolicy instanceof LRUEvictionPolicy) {
      try {
        ((LRUEvictionPolicy) evictionPolicy).close();
      } catch (IOException e) {
        LOG.error("Error closing eviction policy", e);
      }
    }

    LOG.info("HDFSTierEvictionCoordinator shutdown complete");
  }

  /**
   * Eviction statistics snapshot.
   */
  public static class EvictionStats {
    public final int evictionRuns;
    public final long totalBytesEvicted;
    public final long lastEvictionTime;
    public final int totalEvictions;
    public final int failedEvictions;
    public final String policyName;

    public EvictionStats(int runs, long bytes, long lastTime,
                         int total, int failed, String policy) {
      this.evictionRuns = runs;
      this.totalBytesEvicted = bytes;
      this.lastEvictionTime = lastTime;
      this.totalEvictions = total;
      this.failedEvictions = failed;
      this.policyName = policy;
    }

    @Override
    public String toString() {
      return String.format("EvictionStats{runs=%d, bytesEvicted=%d, totalEvictions=%d, " +
                           "failed=%d, policy=%s, lastRun=%d}",
                           evictionRuns, totalBytesEvicted, totalEvictions,
                           failedEvictions, policyName, lastEvictionTime);
    }
  }
}
