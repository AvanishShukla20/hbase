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
import org.apache.hadoop.hbase.Stoppable;
import org.apache.hadoop.hbase.hdfs.tier.HdfsTierStorageMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Manages the lifecycle of HDFSTierEvictionChore within the HDFS tier module.
 *
 * <p>This service wrapper provides a simple mechanism to start and stop the
 * eviction chore without depending on HBase's ChoreService infrastructure,
 * making it suitable for use within a coprocessor.
 *
 * <p><b>Responsibilities:</b>
 * <ul>
 *   <li>Create and manage ScheduledExecutorService for chore execution</li>
 *   <li>Schedule periodic eviction checks</li>
 *   <li>Handle graceful shutdown of the chore</li>
 *   <li>Provide statistics about chore execution</li>
 * </ul>
 *
 * <p><b>Thread Safety:</b> This class is thread-safe and manages its own executor lifecycle.
 *
 * @see HDFSTierEvictionChore
 */
public class HDFSTierEvictionChoreService implements Stoppable {

  private static final Logger LOG = LoggerFactory.getLogger(HDFSTierEvictionChoreService.class);

  // Configuration keys
  private static final String CONFIG_CHORE_PERIOD = "hbase.hdfstier.eviction.chore.period.sec";
  private static final String CONFIG_CHORE_DELAY = "hbase.hdfstier.eviction.chore.delay.sec";
  private static final String CONFIG_EVICTION_ENABLED = "hbase.hdfstier.eviction.enabled";

  // Default values
  private static final int DEFAULT_CHORE_PERIOD_SEC = 300; // 5 minutes
  private static final int DEFAULT_CHORE_DELAY_SEC = 60;   // 1 minute

  private final Configuration conf;
  private final HdfsTierStorageMonitor storageMonitor;
  private final HDFSTierEvictionCoordinator evictionCoordinator;

  private ScheduledExecutorService executor;
  private final AtomicBoolean stopped = new AtomicBoolean(false);

  private final int chorePeriodSec;
  private final int choreDelaySec;
  private final boolean evictionEnabled;

  // Statistics
  private volatile long startTime = 0;
  private HDFSTierEvictionChore.ChoreStats latestStats;

  /**
   * Construct the chore service.
   *
   * @param conf HBase configuration
   * @param storageMonitor storage monitor instance
   * @param evictionCoordinator eviction coordinator instance
   */
  public HDFSTierEvictionChoreService(Configuration conf,
                                       HdfsTierStorageMonitor storageMonitor,
                                       HDFSTierEvictionCoordinator evictionCoordinator) {
    this.conf = conf;
    this.storageMonitor = storageMonitor;
    this.evictionCoordinator = evictionCoordinator;

    // Load configuration
    this.evictionEnabled = conf.getBoolean(CONFIG_EVICTION_ENABLED, true);
    this.chorePeriodSec = conf.getInt(CONFIG_CHORE_PERIOD, DEFAULT_CHORE_PERIOD_SEC);
    this.choreDelaySec = conf.getInt(CONFIG_CHORE_DELAY, DEFAULT_CHORE_DELAY_SEC);

    LOG.info("HDFSTierEvictionChoreService created:");
    LOG.info("  - Eviction enabled: {}", evictionEnabled);
    LOG.info("  - Chore period: {} seconds", chorePeriodSec);
    LOG.info("  - Initial delay: {} seconds", choreDelaySec);
  }

  /**
   * Start the eviction chore service.
   * Creates executor and schedules periodic eviction checks.
   */
  public void start() {
    if (!evictionEnabled) {
      LOG.info("Eviction is disabled - chore service not started");
      return;
    }

    if (stopped.get()) {
      LOG.warn("Cannot start - service has been stopped");
      return;
    }

    synchronized (this) {
      if (executor != null) {
        LOG.warn("Chore service already started");
        return;
      }

      // Create single-threaded executor for chore
      executor = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "HDFSTier-Eviction-Chore");
        t.setDaemon(true);
        return t;
      });

      // Create and schedule the chore
      Runnable choreTask = () -> {
        if (stopped.get()) {
          return;
        }

        try {
          performChore();
        } catch (Exception e) {
          LOG.error("Error during eviction chore execution", e);
        }
      };

      executor.scheduleAtFixedRate(
          choreTask,
          choreDelaySec,
          chorePeriodSec,
          TimeUnit.SECONDS
      );

      startTime = System.currentTimeMillis();

      LOG.info("HDFSTierEvictionChoreService started successfully");
    }
  }

  /**
   * Perform the chore logic - check storage and trigger eviction if needed.
   */
  private void performChore() {
    try {
      // Get current storage statistics
      HdfsTierStorageMonitor.StorageStats stats = storageMonitor.getStorageStats();
      long currentUsageBytes = stats.getTotalUsedBytes();

      // Get threshold configuration
      double evictionThresholdPercent = conf.getDouble("hbase.hdfstier.eviction.threshold", 90.0);
      long maxStorageBytes = conf.getLong("hbase.hdfstier.max.storage.size",
                                          1073741824L); // 1 GB default (1024*1024*1024)

      double usagePercent = (currentUsageBytes * 100.0) / maxStorageBytes;

      LOG.info("HDFSTier Eviction Chore: Storage check - {} bytes used ({} MB, {}% of {} GB max)",
               currentUsageBytes,
               currentUsageBytes / (1024 * 1024),
               String.format("%.2f", usagePercent),
               maxStorageBytes / (1024.0 * 1024 * 1024));

      // Check if eviction threshold exceeded
      if (usagePercent >= evictionThresholdPercent) {
        LOG.warn("Storage usage {}% exceeds threshold {}% - triggering eviction",
                 String.format("%.2f", usagePercent), evictionThresholdPercent);

        evictionCoordinator.performEviction(currentUsageBytes);

      } else {
        LOG.debug("Storage usage {}% is below threshold {}% - no eviction needed",
                  String.format("%.2f", usagePercent), evictionThresholdPercent);
      }

    } catch (Exception e) {
      LOG.error("Error during eviction chore", e);
    }
  }

  /**
   * Stop the chore service and shutdown executor.
   * Blocks until executor terminates or timeout occurs.
   */
  @Override
  public void stop(String why) {
    if (!stopped.compareAndSet(false, true)) {
      LOG.warn("Chore service already stopped");
      return;
    }

    LOG.info("Stopping HDFSTierEvictionChoreService: {}", why);

    synchronized (this) {
      if (executor != null) {
        executor.shutdown();

        try {
          if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
            LOG.warn("Executor did not terminate within timeout - forcing shutdown");
            executor.shutdownNow();

            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
              LOG.error("Executor did not terminate after forced shutdown");
            }
          }
        } catch (InterruptedException e) {
          LOG.warn("Interrupted while waiting for executor termination", e);
          executor.shutdownNow();
          Thread.currentThread().interrupt();
        }

        executor = null;
      }
    }

    LOG.info("HDFSTierEvictionChoreService stopped");
  }

  @Override
  public boolean isStopped() {
    return stopped.get();
  }

  /**
   * Get runtime statistics for the chore service.
   *
   * @return service statistics
   */
  public ServiceStats getStats() {
    return new ServiceStats(
        startTime,
        !isStopped(),
        chorePeriodSec,
        choreDelaySec
    );
  }

  /**
   * Statistics holder for chore service.
   */
  public static class ServiceStats {
    private final long startTime;
    private final boolean isRunning;
    private final int periodSec;
    private final int delaySec;

    public ServiceStats(long startTime, boolean isRunning, int periodSec, int delaySec) {
      this.startTime = startTime;
      this.isRunning = isRunning;
      this.periodSec = periodSec;
      this.delaySec = delaySec;
    }

    public long getStartTime() {
      return startTime;
    }

    public boolean isRunning() {
      return isRunning;
    }

    public int getPeriodSec() {
      return periodSec;
    }

    public int getDelaySec() {
      return delaySec;
    }

    public long getUptimeSeconds() {
      return startTime > 0 ? (System.currentTimeMillis() - startTime) / 1000 : 0;
    }

    @Override
    public String toString() {
      return String.format("ServiceStats{running=%s, uptime=%ds, period=%ds, delay=%ds}",
                           isRunning, getUptimeSeconds(), periodSec, delaySec);
    }
  }
}
