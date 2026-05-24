/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.hdfs.tier.eviction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ScheduledChore;
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.hdfs.tier.HdfsTierStorageMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Scheduled chore that periodically checks HDFS tier storage utilization
 * and triggers eviction when storage usage exceeds configured threshold.
 *
 * <p>This chore runs at configurable intervals (default: 5 minutes) and performs
 * the following operations:
 * <ol>
 *   <li>Queries storage utilization from {@link HdfsTierStorageMonitor}</li>
 *   <li>Compares usage against configured threshold percentage</li>
 *   <li>If threshold exceeded, triggers eviction via {@link HDFSTierEvictionCoordinator}</li>
 *   <li>Eviction continues until storage usage drops to target level</li>
 * </ol>
 *
 * <p><b>Configuration:</b>
 * <ul>
 *   <li>hbase.hdfstier.eviction.chore.period.sec - Chore execution period in seconds (default: 300)</li>
 *   <li>hbase.hdfstier.eviction.chore.delay.sec - Initial delay before first run (default: 60)</li>
 *   <li>hbase.hdfstier.eviction.threshold - Usage % to trigger eviction (default: 90.0)</li>
 *   <li>hbase.hdfstier.eviction.target - Target usage % after eviction (default: 70.0)</li>
 *   <li>hbase.hdfstier.eviction.enabled - Enable/disable eviction (default: true)</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> This chore is executed by HBase's ChoreService in a
 * single-threaded manner, ensuring only one eviction check runs at a time.
 *
 * <p><b>Failure Handling:</b> Exceptions during eviction checks are caught and logged,
 * allowing the chore to continue scheduling future runs.
 *
 * @see HDFSTierEvictionCoordinator
 * @see HdfsTierStorageMonitor
 */
public class HDFSTierEvictionChore extends ScheduledChore {

  private static final Logger LOG = LoggerFactory.getLogger(HDFSTierEvictionChore.class);

  // Configuration keys
  private static final String CONFIG_CHORE_PERIOD = "hbase.hdfstier.eviction.chore.period.sec";
  private static final String CONFIG_CHORE_DELAY = "hbase.hdfstier.eviction.chore.delay.sec";
  private static final String CONFIG_EVICTION_ENABLED = "hbase.hdfstier.eviction.enabled";
  private static final String CONFIG_EVICTION_THRESHOLD = "hbase.hdfstier.eviction.threshold";
  private static final String CONFIG_EVICTION_TARGET = "hbase.hdfstier.eviction.target";
  private static final String CONFIG_MAX_STORAGE = "hbase.hdfstier.max.storage.size";

  // Default values
  private static final int DEFAULT_CHORE_PERIOD_SEC = 300; // 5 minutes
  private static final int DEFAULT_CHORE_DELAY_SEC = 60;   // 1 minute initial delay
  private static final double DEFAULT_THRESHOLD = 90.0;
  private static final double DEFAULT_TARGET = 70.0;
  private static final long DEFAULT_MAX_STORAGE = 1073741824L; // 1 GB (1024*1024*1024)

  // Dependencies
  private final HDFSTierEvictionCoordinator evictionCoordinator;
  private final HdfsTierStorageMonitor storageMonitor;
  private final Configuration conf;

  // Configuration values
  private final boolean evictionEnabled;
  private final double evictionThresholdPercent;
  private final double evictionTargetPercent;
  private final long maxStorageBytes;

  // Statistics
  private volatile long totalChecks = 0;
  private volatile long evictionTriggers = 0;
  private volatile long lastCheckTime = 0;
  private volatile long lastEvictionTime = 0;

  /**
   * Construct HDFSTierEvictionChore.
   *
   * @param stopper the stopper to coordinate shutdown
   * @param conf HBase configuration
   * @param storageMonitor monitor for storage utilization
   * @param evictionCoordinator coordinator to trigger eviction
   */
  public HDFSTierEvictionChore(Stoppable stopper,
                                Configuration conf,
                                HdfsTierStorageMonitor storageMonitor,
                                HDFSTierEvictionCoordinator evictionCoordinator) {
    super("HDFSTierEvictionChore",
          stopper,
          (int) TimeUnit.SECONDS.toMillis(conf.getInt(CONFIG_CHORE_PERIOD, DEFAULT_CHORE_PERIOD_SEC)),
          conf.getLong(CONFIG_CHORE_DELAY, DEFAULT_CHORE_DELAY_SEC) * 1000,
          TimeUnit.MILLISECONDS);

    this.conf = conf;
    this.storageMonitor = storageMonitor;
    this.evictionCoordinator = evictionCoordinator;

    // Load configuration
    this.evictionEnabled = conf.getBoolean(CONFIG_EVICTION_ENABLED, true);
    this.evictionThresholdPercent = conf.getDouble(CONFIG_EVICTION_THRESHOLD, DEFAULT_THRESHOLD);
    this.evictionTargetPercent = conf.getDouble(CONFIG_EVICTION_TARGET, DEFAULT_TARGET);
    this.maxStorageBytes = conf.getLong(CONFIG_MAX_STORAGE, DEFAULT_MAX_STORAGE);

    LOG.info("HDFSTierEvictionChore initialized:");
    LOG.info("  - Chore period: {} seconds", getPeriod() / 1000);
    LOG.info("  - Initial delay: {} seconds", getInitialDelay() / 1000);
    LOG.info("  - Eviction enabled: {}", evictionEnabled);
    LOG.info("  - Eviction threshold: {}%", evictionThresholdPercent);
    LOG.info("  - Eviction target: {}%", evictionTargetPercent);
    LOG.info("  - Max storage: {} bytes ({} GB)",
             maxStorageBytes, maxStorageBytes / (1024.0 * 1024 * 1024));
  }

  /**
   * Main chore execution method - called periodically by ChoreService.
   *
   * <p>This method:
   * <ol>
   *   <li>Checks if eviction is enabled</li>
   *   <li>Queries current storage utilization</li>
   *   <li>Calculates usage percentage</li>
   *   <li>Triggers eviction if threshold exceeded</li>
   * </ol>
   *
   * <p>All exceptions are caught to prevent chore cancellation.
   */
  @Override
  protected void chore() {
    totalChecks++;
    lastCheckTime = System.currentTimeMillis();

    LOG.debug("HDFSTierEvictionChore: Starting periodic eviction check (run #{})", totalChecks);

    // Check if eviction is disabled
    if (!evictionEnabled) {
      LOG.debug("Eviction is disabled - skipping check");
      return;
    }

    try {
      // Get current storage statistics
      HdfsTierStorageMonitor.StorageStats stats = storageMonitor.getStorageStats();
      long currentUsageBytes = stats.getTotalUsedBytes();
      double usagePercent = (currentUsageBytes * 100.0) / maxStorageBytes;

      LOG.info("HDFSTierEvictionChore: Storage check #{} - {} bytes used ({} MB, {:.2f}% of {} GB max)",
               totalChecks,
               currentUsageBytes,
               currentUsageBytes / (1024 * 1024),
               usagePercent,
               maxStorageBytes / (1024.0 * 1024 * 1024));

      // Check if eviction threshold exceeded
      if (usagePercent >= evictionThresholdPercent) {
        LOG.warn("Storage usage {:.2f}% exceeds threshold {}% - triggering eviction",
                 usagePercent, evictionThresholdPercent);

        triggerEviction(currentUsageBytes, usagePercent);

      } else {
        LOG.debug("Storage usage {:.2f}% is below threshold {}% - no eviction needed",
                  usagePercent, evictionThresholdPercent);
      }

    } catch (Exception e) {
      LOG.error("Error during eviction check", e);
      // Don't rethrow - allow chore to continue scheduling
    }
  }

  /**
   * Triggers eviction through the coordinator.
   *
   * @param currentUsageBytes current storage usage in bytes
   * @param usagePercent current usage percentage
   */
  private void triggerEviction(long currentUsageBytes, double usagePercent) {
    evictionTriggers++;
    lastEvictionTime = System.currentTimeMillis();

    LOG.info("Triggering eviction #{}: current usage={} bytes ({:.2f}%), target={}%",
             evictionTriggers, currentUsageBytes, usagePercent, evictionTargetPercent);

    try {
      // Delegate to eviction coordinator
      evictionCoordinator.performEviction(currentUsageBytes);

      LOG.info("Eviction trigger #{} completed successfully", evictionTriggers);

    } catch (Exception e) {
      LOG.error("Eviction trigger #{} failed", evictionTriggers, e);
    }
  }

  /**
   * Initial chore execution - runs once before periodic executions.
   *
   * @return true if initialization successful
   */
  @Override
  protected boolean initialChore() {
    LOG.info("HDFSTierEvictionChore: Running initial check");
    try {
      chore();
      return true;
    } catch (Exception e) {
      LOG.error("Initial chore execution failed", e);
      return false;
    }
  }

  /**
   * Cleanup when chore is stopped.
   */
  @Override
  protected void cleanup() {
    LOG.info("HDFSTierEvictionChore: Shutting down");
    LOG.info("  - Total checks performed: {}", totalChecks);
    LOG.info("  - Eviction triggers: {}", evictionTriggers);
    LOG.info("  - Last check time: {}", new java.util.Date(lastCheckTime));
    if (lastEvictionTime > 0) {
      LOG.info("  - Last eviction time: {}", new java.util.Date(lastEvictionTime));
    }
  }

  /**
   * Get statistics for monitoring.
   *
   * @return statistics object
   */
  public ChoreStats getStats() {
    return new ChoreStats(totalChecks, evictionTriggers, lastCheckTime, lastEvictionTime);
  }

  /**
   * Statistics holder for chore execution.
   */
  public static class ChoreStats {
    private final long totalChecks;
    private final long evictionTriggers;
    private final long lastCheckTime;
    private final long lastEvictionTime;

    public ChoreStats(long totalChecks, long evictionTriggers,
                      long lastCheckTime, long lastEvictionTime) {
      this.totalChecks = totalChecks;
      this.evictionTriggers = evictionTriggers;
      this.lastCheckTime = lastCheckTime;
      this.lastEvictionTime = lastEvictionTime;
    }

    public long getTotalChecks() {
      return totalChecks;
    }

    public long getEvictionTriggers() {
      return evictionTriggers;
    }

    public long getLastCheckTime() {
      return lastCheckTime;
    }

    public long getLastEvictionTime() {
      return lastEvictionTime;
    }

    @Override
    public String toString() {
      return String.format("ChoreStats{checks=%d, triggers=%d, lastCheck=%s, lastEviction=%s}",
                           totalChecks, evictionTriggers,
                           new java.util.Date(lastCheckTime),
                           lastEvictionTime > 0 ? new java.util.Date(lastEvictionTime) : "Never");
    }
  }
}
