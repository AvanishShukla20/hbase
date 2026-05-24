package org.apache.hadoop.hbase.hdfs.tier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.hdfs.tier.access.StoreFileScannerAccessTracker;
import org.apache.hadoop.hbase.hdfs.tier.access.MetadataUpdateMonitor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HdfsTierStorageMonitor tracks real-time storage metrics for the HDFS tier cache layer.
 *
 * This class maintains atomic counters for:
 * - Total storage used (bytes)
 * - Total allocated storage size (from configuration)
 * - Number of HFiles flushed
 * - Number of HFiles compacted
 *
 * Includes reconciliation mechanism that scans hdfsTier:meta table periodically
 * to ensure accuracy after restarts or failures.
 *
 * Thread-safe implementation using atomic operations for concurrent updates from
 * multiple RegionServers and coprocessor threads.
 */
public class HdfsTierStorageMonitor {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsTierStorageMonitor.class);

  // Singleton instance
  private static volatile HdfsTierStorageMonitor instance;

  // Reconciliation executor
  private final ScheduledExecutorService reconciliationExecutor;
  private final AtomicBoolean reconciliationInProgress = new AtomicBoolean(false);
  private volatile long lastReconciliationTime = 0;

  // Configuration key for max storage size
  private static final String CONFIG_KEY_MAX_STORAGE = "hbase.hdfstier.max.storage.size";
  private static final long DEFAULT_MAX_STORAGE = 1073741824L; // 1 GB default (1024*1024*1024)

  // Configuration key for reconciliation interval (in minutes)
  private static final String CONFIG_KEY_RECONCILIATION_INTERVAL = "hbase.hdfstier.reconciliation.interval.minutes";
  private static final int DEFAULT_RECONCILIATION_INTERVAL = 10; // 10 minutes

  // HBase connection for scanning metadata table
  private final Connection connection;
  private final Configuration conf;

  // Atomic counters for thread-safe concurrent updates
  private final AtomicLong totalStorageUsed = new AtomicLong(0);
  private final AtomicInteger totalHFilesFlushed = new AtomicInteger(0);
  private final AtomicInteger totalHFilesCompacted = new AtomicInteger(0);

  // Allocated storage size from configuration (immutable after init)
  private final long totalAllocatedSize;

  // Track last flush details for real-time display
  private volatile long lastFlushedHFileSize = 0;
  private volatile String lastFlushedHFileName = "";
  private volatile long lastFlushedTimestamp = 0;

  // Track last eviction details for real-time display
  private volatile int lastEvictionFilesCount = 0;
  private volatile long lastEvictionBytesFreed = 0;
  private volatile long lastEvictionTimestamp = 0;
  private volatile String lastEvictedFilesNames = "";

  /**
   * Private constructor for singleton pattern.
   * Reads max storage size from HBase configuration.
   * Initializes reconciliation task.
   */
  private HdfsTierStorageMonitor(Configuration conf) {
    this.conf = conf;
    this.totalAllocatedSize = conf.getLong(CONFIG_KEY_MAX_STORAGE, DEFAULT_MAX_STORAGE);

    // Initialize HBase connection for reconciliation
    Connection tempConnection = null;
    try {
      tempConnection = ConnectionFactory.createConnection(conf);
      LOG.info("HBase connection created for reconciliation");
    } catch (IOException e) {
      LOG.warn("Failed to create HBase connection for reconciliation: {}." , e.getMessage());
    }
    this.connection = tempConnection;

    // Create scheduled executor for reconciliation
    this.reconciliationExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
      Thread t = new Thread(r, "HdfsTier-Reconciliation");
      t.setDaemon(true);
      return t;
    });

    int reconciliationInterval = conf.getInt(
      CONFIG_KEY_RECONCILIATION_INTERVAL,
      DEFAULT_RECONCILIATION_INTERVAL
    );

    LOG.info("HdfsTierStorageMonitor initialized:");
    LOG.info("  - Max storage: {} bytes ({} GB)",
             totalAllocatedSize, totalAllocatedSize / (1024.0 * 1024 * 1024));
    LOG.info("  - Reconciliation interval: {} minutes", reconciliationInterval);

    // Trigger immediate reconciliation on startup
    reconciliationExecutor.submit(this::reconcileStorageMetrics);

    // Schedule periodic reconciliation
    reconciliationExecutor.scheduleAtFixedRate(
      this::reconcileStorageMetrics,
      reconciliationInterval,
      reconciliationInterval,
      TimeUnit.MINUTES
    );

    LOG.info("Reconciliation task scheduled successfully");
  }

  /**
   * Get singleton instance of HdfsTierStorageMonitor.
   * Thread-safe double-checked locking.
   */
  public static HdfsTierStorageMonitor getInstance(Configuration conf) {
    if (instance == null) {
      synchronized (HdfsTierStorageMonitor.class) {
        if (instance == null) {
          instance = new HdfsTierStorageMonitor(conf);
        }
      }
    }
    return instance;
  }

  /**
   * Called when an HFile is flushed.
   * Increments total storage used and flush counter.
   *
   * @param hfileName Name of the flushed HFile
   * @param hfileSize Size of the flushed HFile in bytes
   */
  public void recordFlush(String hfileName, long hfileSize) {
    // Atomic increment of storage and counter
    long newTotal = totalStorageUsed.addAndGet(hfileSize);
    int newCount = totalHFilesFlushed.incrementAndGet();

    // Update last flush details for monitoring
    this.lastFlushedHFileSize = hfileSize;
    this.lastFlushedHFileName = hfileName;
    this.lastFlushedTimestamp = System.currentTimeMillis();

    LOG.info("FLUSH: HFile={}, Size={} bytes ({} MB), TotalUsed={} bytes ({} GB), FlushCount={}",
      hfileName,
      hfileSize,
      hfileSize / (1024.0 * 1024),
      newTotal,
      newTotal / (1024.0 * 1024 * 1024),
      newCount);
  }

  /**
   * Called when HFiles are compacted.
   * Subtracts parent file sizes and adds new compacted file size.
   *
   * LOGIC - Track Only What's in Metadata Table:
   * - Remove parent file sizes from total (they're marked COMPACTED in metadata)
   * - Add new compacted file size (it's marked ACTIVE in metadata)
   * - Always ensure non-negative storage values
   * - Reconciliation every 10 mins ensures long-term accuracy
   *
   * - Parent files may not be fully tracked if flush was missed
   * - Periodic reconciliation scans metadata table for ground truth
   *
   * @param newFileName Name of the new compacted HFile
   * @param newFileSize Size of the new compacted HFile in bytes
   * @param totalParentSize Total size of all parent HFiles being compacted (to subtract)
   */
  public void recordCompaction(String newFileName, long newFileSize, long totalParentSize) {
    // Calculate net change: subtract parents, add new file
    // This can be negative if compaction reduced total size
    long netChange = newFileSize - totalParentSize;

    // Use compareAndSet loop for thread-safe atomic update
    long currentTotal;
    long newTotal;
    do {
      currentTotal = totalStorageUsed.get();
      newTotal = currentTotal + netChange;


      if (newTotal < 0) {
        LOG.warn("(current={} MB, parents={} MB, new={} MB). " +
                 "Clamping to 0. Reconciliation will fix drift.",
          currentTotal / (1024.0 * 1024),
          totalParentSize / (1024.0 * 1024),
          newFileSize / (1024.0 * 1024));

        newTotal = 0;
      }
    } while (!totalStorageUsed.compareAndSet(currentTotal, newTotal));

    int newCount = totalHFilesCompacted.incrementAndGet();

    // Always log final non-negative value
    LOG.info("COMPACTION: File={}, NewSize={} MB, ParentSize={} MB, " +
        "NetChange={} MB, FinalTotal={} MB, Count={}",
      newFileName,
      newFileSize / (1024.0 * 1024),
      totalParentSize / (1024.0 * 1024),
      netChange / (1024.0 * 1024),
      newTotal / (1024.0 * 1024),
      newCount);
  }

  /**
   * Called when HFiles are evicted.
   * Subtracts evicted file sizes from total storage.
   *
   * LOGIC:
   * - Files marked as EVICTED in metadata table should not count toward storage
   * - Reduce totalStorageUsed by the bytes that were evicted
   * - Track eviction count for monitoring
   *
   * @param bytesEvicted Total size of evicted HFiles in bytes
   * @param filesEvicted Number of files evicted
   * @param evictedFileNames Comma-separated list of evicted file names (for display)
   */
  public void recordEviction(long bytesEvicted, int filesEvicted, String evictedFileNames) {
    if (bytesEvicted <= 0) {
      LOG.warn("Invalid eviction size: {} bytes", bytesEvicted);
      return;
    }

    // Use compareAndSet loop for thread-safe atomic update
    long currentTotal;
    long newTotal;
    do {
      currentTotal = totalStorageUsed.get();
      newTotal = currentTotal - bytesEvicted;

      // Ensure we don't go negative
      if (newTotal < 0) {
        LOG.warn("Eviction would cause negative storage (current={} MB, evicting={} MB). " +
                 "Clamping to 0. Reconciliation will fix drift.",
          currentTotal / (1024.0 * 1024),
          bytesEvicted / (1024.0 * 1024));
        newTotal = 0;
      }
    } while (!totalStorageUsed.compareAndSet(currentTotal, newTotal));

    // Update last eviction details for monitoring
    this.lastEvictionFilesCount = filesEvicted;
    this.lastEvictionBytesFreed = bytesEvicted;
    this.lastEvictionTimestamp = System.currentTimeMillis();
    this.lastEvictedFilesNames = evictedFileNames != null ? evictedFileNames : "";

    // Calculate new utilization percentage
    double utilizationPercent = (newTotal * 100.0) / totalAllocatedSize;

    LOG.info("EVICTION: Files={}, BytesEvicted={} MB, " +
        "RemainingStorage={} MB ({}% utilization), FileNames={}",
      filesEvicted,
      bytesEvicted / (1024.0 * 1024),
      newTotal / (1024.0 * 1024),
      String.format("%.2f", utilizationPercent),
      evictedFileNames);
  }

  /**
   * Reconcile storage metrics by scanning hdfsTier:meta table.
   *
   * THIS IS THE GROUND TRUTH - Tracks Only What's in Metadata Table:
   * - Scans hdfsTier:meta table and counts only ACTIVE files (not compacted/evicted)
   * - Recalculates totalStorageUsed from actual metadata
   * - Fixes any drift caused by missed flush events or negative clamping
   *
   * WHY NEEDED:
   * - After HBase restart, in-memory counters reset to 0
   * - Parent files may not be fully tracked during compaction
   * - Real-time recordFlush/recordCompaction can have minor drift
   * - This scans metadata table for accurate totals
   *
   * WHAT IT DOES:
   * 1. Scans all rows in hdfsTier:meta table
   * 2. Filters for files with state=ACTIVE and evicted=false
   * 3. Sums up their sizes to get accurate totalStorageUsed
   * 4. Updates the atomic counter with reconciled value
   *
   * WHEN CALLED:
   * - Immediately on startup (via constructor)
   * - Every 10 minutes (configurable) via scheduled task
   * - Can be triggered manually via forceReconciliation()
   *
   * PERFORMANCE:
   * - Full table scan with column filtering
   * - With 100K HFiles: ~5-10 seconds scan time
   * - Cached at 1000 rows/batch for efficiency
   * - Runs in background thread (doesn't block flush/compaction)
   */
  private void reconcileStorageMetrics() {
    // Prevent concurrent reconciliation
    if (!reconciliationInProgress.compareAndSet(false, true)) {
      LOG.warn("Reconciliation already in progress, skipping");
      return;
    }

    try {
      if (connection == null) {
        LOG.warn("No HBase connection available, skipping reconciliation");
        return;
      }

      LOG.info("🔄 Starting storage metrics reconciliation...");
      long startTime = System.currentTimeMillis();

      TableName metaTableName = HdfsTierMetaTable.TABLE_NAME;

      // Check if table exists
      try (Admin admin = connection.getAdmin()) {
        if (!admin.tableExists(metaTableName)) {
          LOG.warn("hdfsTier:meta table does not exist yet, skipping reconciliation");
          return;
        }
      }

      long totalSize = 0;
      int activeFileCount = 0;
      int compactedFileCount = 0;
      int evictedFileCount = 0;

      try (Table metaTable = connection.getTable(metaTableName)) {
        Scan scan = new Scan();
        scan.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_SIZE);
        scan.addColumn(HdfsTierMetaTable.CF_TRANSITION, HdfsTierMetaTable.COL_CURR_STATE);
        scan.addColumn(HdfsTierMetaTable.CF_TRANSITION, HdfsTierMetaTable.COL_EVICTED);
        scan.setCaching(1000); // Batch rows for efficiency

        try (ResultScanner scanner = metaTable.getScanner(scan)) {
          for (Result result : scanner) {
            if (result.isEmpty()) {
              continue;
            }

            // Get file size
            byte[] sizeBytes = result.getValue(
              HdfsTierMetaTable.CF_INFO,
              HdfsTierMetaTable.COL_SIZE
            );

            // Get current state
            byte[] stateBytes = result.getValue(
              HdfsTierMetaTable.CF_TRANSITION,
              HdfsTierMetaTable.COL_CURR_STATE
            );

            // Get evicted flag
            byte[] evictedBytes = result.getValue(
              HdfsTierMetaTable.CF_TRANSITION,
              HdfsTierMetaTable.COL_EVICTED
            );

            if (sizeBytes == null) {
              continue;
            }

            long fileSize = Bytes.toLong(sizeBytes);
            String state = stateBytes != null ? Bytes.toString(stateBytes) : "active";
            boolean isEvicted = evictedBytes != null && Bytes.toBoolean(evictedBytes);

            // Only count ACTIVE files that are NOT evicted
            if ("active".equalsIgnoreCase(state) && !isEvicted) {
              totalSize += fileSize;
              activeFileCount++;
            } else if ("compacted".equalsIgnoreCase(state)) {
              compactedFileCount++;
            } else if (isEvicted) {
              evictedFileCount++;
            }
          }
        }
      }

      // Update the atomic counter with reconciled value
      long previousTotal = totalStorageUsed.getAndSet(totalSize);
      long duration = System.currentTimeMillis() - startTime;

      LOG.info("  Reconciliation COMPLETE in {}ms:", duration);
      LOG.info("   Active files: {}", activeFileCount);
      LOG.info("   Compacted files: {}", compactedFileCount);
      LOG.info("   Evicted files: {}", evictedFileCount);
      LOG.info("   Previous total: {} bytes ({} MB)",
               previousTotal, previousTotal / (1024.0 * 1024));
      LOG.info("   Reconciled total: {} bytes ({} MB)",
               totalSize, totalSize / (1024.0 * 1024));
      LOG.info("   Difference: {} bytes ({} MB)",
               (totalSize - previousTotal), (totalSize - previousTotal) / (1024.0 * 1024));

      lastReconciliationTime = System.currentTimeMillis();

    } catch (Exception e) {
      LOG.error(" Failed to reconcile storage metrics: {}", e.getMessage(), e);
    } finally {
      reconciliationInProgress.set(false);
    }
  }

  /**
   * Force immediate reconciliation (useful for testing or manual triggers).
   */
  public void forceReconciliation() {
    LOG.info("Manual reconciliation triggered");
    reconciliationExecutor.submit(this::reconcileStorageMetrics);
  }

  /**
   * Get last reconciliation timestamp.
   */
  public long getLastReconciliationTime() {
    return lastReconciliationTime;
  }

  /**
   * Check if reconciliation is currently in progress.
   */
  public boolean isReconciliationInProgress() {
    return reconciliationInProgress.get();
  }

  /**
   * Get current storage metrics as a formatted JSON-compatible string.
   * Used by the metrics endpoint for real-time monitoring.
   */
  public StorageMetrics getMetrics() {
    return new StorageMetrics(
      totalStorageUsed.get(),
      totalAllocatedSize,
      totalHFilesFlushed.get(),
      totalHFilesCompacted.get(),
      lastFlushedHFileName,
      lastFlushedHFileSize,
      lastFlushedTimestamp,
      lastReconciliationTime,
      reconciliationInProgress.get(),
      lastEvictionFilesCount,
      lastEvictionBytesFreed,
      lastEvictionTimestamp,
      lastEvictedFilesNames
    );
  }

  /**
   * Get current storage statistics for eviction coordinator.
   * This is an alias that provides a simplified view for eviction logic.
   */
  public StorageStats getStorageStats() {
    return new StorageStats(totalStorageUsed.get());
  }

  /**
   * Reset all counters (for testing purposes only).
   */
  public void reset() {
    totalStorageUsed.set(0);
    totalHFilesFlushed.set(0);
    totalHFilesCompacted.set(0);
    lastFlushedHFileSize = 0;
    lastFlushedHFileName = "";
    lastFlushedTimestamp = 0;
    lastEvictionFilesCount = 0;
    lastEvictionBytesFreed = 0;
    lastEvictionTimestamp = 0;
    lastEvictedFilesNames = "";
    LOG.info("Storage monitor metrics reset");
  }

  /**
   * Shutdown the monitor and clean up resources.
   * Should be called when HBase is shutting down.
   */
  public void shutdown() {
    LOG.info("Shutting down HdfsTierStorageMonitor...");

    // Shutdown reconciliation executor
    if (reconciliationExecutor != null && !reconciliationExecutor.isShutdown()) {
      reconciliationExecutor.shutdown();
      try {
        if (!reconciliationExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
          reconciliationExecutor.shutdownNow();
        }
        LOG.info("Reconciliation executor shut down");
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        reconciliationExecutor.shutdownNow();
      }
    }

    // Close HBase connection
    if (connection != null) {
      try {
        connection.close();
        LOG.info("HBase connection closed");
      } catch (IOException e) {
        LOG.error("Error closing HBase connection: {}", e.getMessage());
      }
    }

    LOG.info("HdfsTierStorageMonitor shut down successfully");
  }

  /**
   * Simplified storage statistics for eviction coordinator.
   */
  public static class StorageStats {
    private final long totalUsedBytes;

    public StorageStats(long totalUsedBytes) {
      this.totalUsedBytes = totalUsedBytes;
    }

    public long getTotalUsedBytes() {
      return totalUsedBytes;
    }
  }

  /**
   * Immutable data class holding storage metrics snapshot.
   */
  public static class StorageMetrics {
    public final long totalStorageUsed;
    public final long totalAllocatedSize;
    public final int totalHFilesFlushed;
    public final int totalHFilesCompacted;
    public final String lastFlushedHFileName;
    public final long lastFlushedHFileSize;
    public final long lastFlushedTimestamp;
    public final long lastReconciliationTime;
    public final boolean reconciliationInProgress;
    public final int lastEvictionFilesCount;
    public final long lastEvictionBytesFreed;
    public final long lastEvictionTimestamp;
    public final String lastEvictedFilesNames;

    public StorageMetrics(
      long totalStorageUsed,
      long totalAllocatedSize,
      int totalHFilesFlushed,
      int totalHFilesCompacted,
      String lastFlushedHFileName,
      long lastFlushedHFileSize,
      long lastFlushedTimestamp,
      long lastReconciliationTime,
      boolean reconciliationInProgress,
      int lastEvictionFilesCount,
      long lastEvictionBytesFreed,
      long lastEvictionTimestamp,
      String lastEvictedFilesNames) {
      this.totalStorageUsed = totalStorageUsed;
      this.totalAllocatedSize = totalAllocatedSize;
      this.totalHFilesFlushed = totalHFilesFlushed;
      this.totalHFilesCompacted = totalHFilesCompacted;
      this.lastFlushedHFileName = lastFlushedHFileName;
      this.lastFlushedHFileSize = lastFlushedHFileSize;
      this.lastFlushedTimestamp = lastFlushedTimestamp;
      this.lastReconciliationTime = lastReconciliationTime;
      this.reconciliationInProgress = reconciliationInProgress;
      this.lastEvictionFilesCount = lastEvictionFilesCount;
      this.lastEvictionBytesFreed = lastEvictionBytesFreed;
      this.lastEvictionTimestamp = lastEvictionTimestamp;
      this.lastEvictedFilesNames = lastEvictedFilesNames;
    }

    /**
     * Convert metrics to JSON string for HTTP endpoint.
     * All storage values are guaranteed to be non-negative.
     */
    public String toJson() {
      // Calculate time since last reconciliation in human-readable format
      long timeSinceReconciliation = System.currentTimeMillis() - lastReconciliationTime;
      long minutesSinceReconciliation = timeSinceReconciliation / (60 * 1000);

      // Ensure all storage values are non-negative (defensive programming)
      // This prevents misleading negative values on UI
      long safeStorageUsed = Math.max(0, totalStorageUsed);
      double safeStorageUsedGB = safeStorageUsed / (1024.0 * 1024 * 1024);

      // Calculate utilization percentage (always non-negative)
      double utilizationPercent = 0.0;
      if (totalAllocatedSize > 0) {
        utilizationPercent = Math.max(0.0, Math.min(100.0, (safeStorageUsed * 100.0 / totalAllocatedSize)));
      }

      return String.format(
        "{\n" +
          "  \"totalStorageUsed\": %d,\n" +
          "  \"totalStorageUsedGB\": %.3f,\n" +
          "  \"totalAllocatedSize\": %d,\n" +
          "  \"totalAllocatedSizeGB\": %.3f,\n" +
          "  \"storageUtilizationPercent\": %.2f,\n" +
          "  \"totalHFilesFlushed\": %d,\n" +
          "  \"totalHFilesCompacted\": %d,\n" +
          "  \"lastFlush\": {\n" +
          "    \"fileName\": \"%s\",\n" +
          "    \"size\": %d,\n" +
          "    \"sizeMB\": %.3f,\n" +
          "    \"timestamp\": %d\n" +
          "  },\n" +
          "  \"lastEviction\": {\n" +
          "    \"filesCount\": %d,\n" +
          "    \"bytesFreed\": %d,\n" +
          "    \"bytesFreedMB\": %.3f,\n" +
          "    \"timestamp\": %d,\n" +
          "    \"fileNames\": \"%s\"\n" +
          "  },\n" +
          "  \"reconciliation\": {\n" +
          "    \"lastReconciliationTime\": %d,\n" +
          "    \"minutesSinceReconciliation\": %d,\n" +
          "    \"reconciliationInProgress\": %b\n" +
          "  }\n" +
          "}",
        safeStorageUsed,
        safeStorageUsedGB,
        totalAllocatedSize,
        totalAllocatedSize / (1024.0 * 1024 * 1024),
        utilizationPercent,
        totalHFilesFlushed,
        totalHFilesCompacted,
        lastFlushedHFileName,
        lastFlushedHFileSize,
        lastFlushedHFileSize / (1024.0 * 1024),
        lastFlushedTimestamp,
        lastEvictionFilesCount,
        lastEvictionBytesFreed,
        lastEvictionBytesFreed / (1024.0 * 1024),
        lastEvictionTimestamp,
        lastEvictedFilesNames,
        lastReconciliationTime,
        minutesSinceReconciliation,
        reconciliationInProgress
      );
    }
  }
}
