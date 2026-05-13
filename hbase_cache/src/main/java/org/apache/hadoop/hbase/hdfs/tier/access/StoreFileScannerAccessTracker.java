package org.apache.hadoop.hbase.hdfs.tier.access;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight facade for tracking HFile access events.
 *
 * INTEGRATION POINT: Called from StoreFileScanner.open() and similar locations.
 *
 * PERFORMANCE GUARANTEE:
 * - enqueue() call takes <1 microsecond
 * - Never blocks HBase operations
 * - Fail-safe: exceptions caught and logged
 *
 * USAGE:
 * 1. Initialize once during RegionServer startup
 * 2. Call trackAccess() whenever StoreFileScanner opens an HFile
 * 3. Shutdown during RegionServer shutdown
 */
public class StoreFileScannerAccessTracker {
  private static final Logger LOG = LoggerFactory.getLogger(StoreFileScannerAccessTracker.class);

  private static volatile boolean enabled = true;
  private static HFileAccessEventQueue eventQueue;
  private static FailedUpdateRetryManager retryManager;

  /**
   * Initialize the access tracking system.
   * MUST be called during RegionServer startup before any reads.
   */
  public static void initialize(Configuration conf) {
    enabled = conf.getBoolean("hbase.hfile.access.tracking.enabled", true);

    if (!enabled) {
      LOG.info("HFile access tracking is DISABLED");
      return;
    }

    try {
      eventQueue = HFileAccessEventQueue.getInstance(conf);
      retryManager = new FailedUpdateRetryManager(conf, eventQueue.getMonitor());
      LOG.info("HFile access tracking initialized and ENABLED");
    } catch (Exception e) {
      LOG.error("Failed to initialize HFile access tracking, disabling", e);
      enabled = false;
    }
  }

  /**
   * Track HFile access event.
   *
   * CALLED FROM: StoreFileScanner.open() or similar read paths.
   *
   * @param reader StoreFileReader for the accessed HFile
   * @param region Region containing the HFile
   */
  public static void trackAccess(StoreFileReader reader, HRegion region) {
    if (!enabled || eventQueue == null || reader == null) {
      return;
    }

    // region is mandatory for rowkey formation
    if (region == null || region.getRegionInfo() == null) {
      LOG.error("Cannot track access - Region information is required for HFile: {}",
        reader.getHFileReader() != null ? reader.getHFileReader().getName() : "unknown");
      return;
    }

    try {
      String hfilePath = null;
      if (reader.getHFileReader() != null && reader.getHFileReader().getPath() != null) {
        hfilePath = reader.getHFileReader().getPath().toString();
      }

      if (hfilePath == null) {
        LOG.warn("Cannot track access - HFile path is null");
        return;
      }

      String regionEncodedName = region.getRegionInfo().getEncodedName();

      HFileAccessEvent event = HFileAccessEvent.newBuilder()
        .hfilePath(hfilePath)
        .regionEncodedName(regionEncodedName)
        .build();


      // Enqueue - O(1), non-blocking
      boolean enqueued = eventQueue.enqueue(event);

      // Debug logging (disabled in production)
      if (LOG.isTraceEnabled()) {
        LOG.trace("HFile access {}: {}", enqueued ? "tracked" : "dropped", event);
      }

    } catch (Exception e) {
      // Fail silently - never crash HBase operations due to metadata tracking
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to track HFile access (non-fatal)", e);
      }
    }
  }

  /**
   * Overload for cases where we only have the reader.
   */
  public static void trackAccess(StoreFileReader reader) {
    trackAccess(reader, null);
  }

  /**
   * Shutdown the tracking system.
   * MUST be called during RegionServer shutdown.
   */
  public static void shutdown() {
    if (retryManager != null) {
      retryManager.shutdown();
    }
    if (eventQueue != null) {
      eventQueue.shutdown();
    }
    enabled = false;
    LOG.info("HFile access tracking shut down");
  }

  /**
   * Check if tracking is enabled and operational.
   */
  public static boolean isEnabled() {
    return enabled && eventQueue != null;
  }

  /**
   * Get the event queue for monitoring/testing.
   */
  public static HFileAccessEventQueue getEventQueue() {
    return eventQueue;
  }

  /**
   * Get current tracking statistics.
   */
  public static String getStats() {
    if (!enabled || eventQueue == null) {
      return "HFile access tracking: DISABLED";
    }

    MetadataUpdateMonitor.MetadataUpdateStats stats = eventQueue.getMonitor().getStats();
    return String.format("HFile access tracking: Queue=%d, Enqueued=%d, SyncFallback=%d, %s",
        eventQueue.getQueueSize(),
        eventQueue.getEnqueuedEvents(),
        eventQueue.getFallbackProcessedEvents(),
        stats);
  }
}
