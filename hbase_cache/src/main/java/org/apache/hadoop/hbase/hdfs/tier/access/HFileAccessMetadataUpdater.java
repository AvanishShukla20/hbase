package org.apache.hadoop.hbase.hdfs.tier.access;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.hdfs.tier.HdfsTierMetaTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Updates HFile access metadata in hdfsTier:meta table.
 *
 * DESIGN DECISIONS:
 * - Uses single shared Connection
 * - Retry logic with exponential backoff
 * - Fail-safe: logs errors but doesn't crash caller
 * - Reports results to MetadataUpdateMonitor
 */
public class HFileAccessMetadataUpdater {
  private static final Logger LOG = LoggerFactory.getLogger(HFileAccessMetadataUpdater.class);


  private final MetadataUpdateMonitor monitor;
  private Connection connection;
  private Table metaTable;
  private final int maxRetries;
  private final long retryDelayMs;

  public HFileAccessMetadataUpdater(MetadataUpdateMonitor monitor, Configuration conf) {
    this.monitor = monitor;
    this.maxRetries = conf.getInt("hbase.metadata.update.max.retries", 3);
    this.retryDelayMs = conf.getLong("hbase.metadata.update.retry.delay.ms", 100);
  }

  /**
   * Lazy initialization of connection.
   * Avoid connection overhead during object construction.
   * SYNCHRONIZED: Prevent multiple threads creating duplicate connections.
   */
  private synchronized void ensureConnection() throws IOException {
    if (connection == null || connection.isClosed()) {
      Configuration conf = HBaseConfiguration.create();
      connection = ConnectionFactory.createConnection(conf);
      metaTable = connection.getTable(HdfsTierMetaTable.TABLE_NAME);
      LOG.debug("Connection to metadata table for access support established");
    }
  }

  /**
   * Update access timestamp for an HFile with retry logic.
   *
   * FLOW:
   * 1. Try update with retry
   * 2. Report success/failure to monitor
   * 3. Does not throw exception (fail-safe)
   */
  public void updateLastAccess(HFileAccessEvent event) {
    String hfilePath = event.getHfilePath();
    Exception lastException = null;

    for (int attempt = 0; attempt < maxRetries; attempt++) {
      try {
        ensureConnection();
        performUpdate(event);
        monitor.recordSuccess(hfilePath);
        return; // Success
      } catch (Exception e) {
        lastException = e;

        if (attempt < maxRetries - 1) {
          // Exponential backoff: 100ms, 200ms, 400ms
          long delay = retryDelayMs * (1L << attempt);
          try {
            Thread.sleep(delay);
          } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            break;
          }
        }
      }
    }

    // All retries failed
    monitor.recordFailure(hfilePath, lastException);
    LOG.error("Failed to update metadata after {} retries for {}",
        maxRetries, hfilePath, lastException);
  }

  /**
   * Perform the actual metadata update.
   *
   * SIMPLIFIED: With new row key format {regionEncodedName}#{hfileName},
   * we can directly Get the row without scanning.
   *
   * ROW KEY FORMAT: {regionEncodedName}#{hfileName}
   *
   * STRATEGY:
   * 1. Build exact row key from region + hfileName
   * 2. Direct Get to check if row exists
   * 3. Update lastAccess and increment accessCount on that row
   */
  private void performUpdate(HFileAccessEvent event) throws IOException {
    String hfileName = event.getHfileName();
    String regionName = event.getRegionEncodedName();

    // FAIL FAST: Region name is mandatory for rowkey
    if (regionName == null || regionName.isEmpty()) {
      throw new IllegalArgumentException(
        String.format("Region encoded name is required for HFile: %s. " +
          "Cannot form valid rowkey without it.", hfileName));
    }

    // Build exact row key: {regionEncodedName}#{hfileName}
    byte[] rowKey = HdfsTierMetaTable.createRowKey(regionName, hfileName);

    // Check if row exists with a quick Get (faster than scan)
    Get get = new Get(rowKey);
    get.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_HFILE_NAME);

    Result result = metaTable.get(get);

    if (result.isEmpty()) {
      // HFile not found in metadata table
      // This can happen if:
      // 1. HFile was just created and metadata capture hasn't finished yet
      // 2. HFile is from external source (restored from backup, etc.)
      // 3. Metadata capture failed for this HFile
      LOG.warn("Cannot update access for HFile {} in region {} - no existing metadata row found. " +
          "This HFile may not have been captured during flush/compaction yet.",
          hfileName, regionName);

      // Don't throw exception - this is expected for newly created HFiles
      // The metadata capture will create the row, and next access will update it
      return;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("Found existing row for HFile {}: {}", hfileName, Bytes.toString(rowKey));
    }

    // Update the row's lastAccess timestamp
    Put put = new Put(rowKey);
    put.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_LAST_ACCESS,
        Bytes.toBytes(event.getAccessTimestamp()));
    metaTable.put(put);

    // Atomically increment access count
    Increment increment = new Increment(rowKey);
    increment.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_ACCESS_COUNT, 1L);
    metaTable.increment(increment);

    if (LOG.isDebugEnabled()) {
      LOG.debug("Updated access for HFile {} at row {}: lastAccess={}, accessCount incremented",
          hfileName, Bytes.toString(rowKey), event.getAccessTimestamp());
    }
  }

  /**
   * Close resources gracefully.
   */
  public void close() {
    try {
      if (metaTable != null) {
        metaTable.close();
      }
      if (connection != null) {
        connection.close();
      }
      LOG.info("HFileAccessMetadataUpdater closed");
    } catch (IOException e) {
      LOG.error("Error closing resources", e);
    }
  }
}
