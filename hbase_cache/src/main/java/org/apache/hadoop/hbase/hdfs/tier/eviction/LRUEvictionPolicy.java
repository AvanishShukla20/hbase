package org.apache.hadoop.hbase.hdfs.tier.eviction;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.hdfs.tier.HdfsTierMetaTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * LRU (Least Recently Used) eviction policy.
 *
 * STRATEGY:
 * - Prioritizes HFiles with oldest lastAccess timestamp
 * - Evicts files that haven't been accessed recently
 * - Only considers ACTIVE files (not already COMPACTED or EVICTED)
 *
 * PERFORMANCE:
 * - Scans metadata table for all ACTIVE files
 * - Sorts by lastAccessTime (ascending)
 * - Selects files until target bytes reached
 */
public class LRUEvictionPolicy implements EvictionPolicy {

  private static final Logger LOG = LoggerFactory.getLogger(LRUEvictionPolicy.class);

  private final Configuration conf;
  private Connection connection;

  public LRUEvictionPolicy(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public List<HFileEvictionCandidate> selectFilesForEviction(long targetBytesToEvict) throws IOException {
    ensureConnection();

    List<HFileEvictionCandidate> allCandidates = new ArrayList<>();

    try (Table metaTable = connection.getTable(HdfsTierMetaTable.TABLE_NAME)) {

      // Scan for all ACTIVE files
      Scan scan = new Scan();
      scan.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_HFILE_NAME);
      scan.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_ENC_REG_NAME);
      scan.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_SIZE);
      scan.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_LAST_ACCESS);
      scan.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_CREATE_TIME);
      scan.addColumn(HdfsTierMetaTable.CF_TRANSITION, HdfsTierMetaTable.COL_CURR_STATE);
      scan.addColumn(HdfsTierMetaTable.CF_TRANSITION, HdfsTierMetaTable.COL_EVICTED);
      scan.setCaching(1000); // Batch for performance

      try (ResultScanner scanner = metaTable.getScanner(scan)) {
        for (Result result : scanner) {
          if (result.isEmpty()) {
            continue;
          }

          // Get current state
          byte[] stateBytes = result.getValue(HdfsTierMetaTable.CF_TRANSITION,
                                              HdfsTierMetaTable.COL_CURR_STATE);
          String state = stateBytes != null ? Bytes.toString(stateBytes) : "UNKNOWN";

          // Get evicted flag
          byte[] evictedBytes = result.getValue(HdfsTierMetaTable.CF_TRANSITION,
                                                 HdfsTierMetaTable.COL_EVICTED);
          boolean isEvicted = evictedBytes != null && Bytes.toBoolean(evictedBytes);

          // Only consider ACTIVE, non-evicted files
          if (!"ACTIVE".equals(state) || isEvicted) {
            continue;
          }

          // Extract metadata
          byte[] hfileNameBytes = result.getValue(HdfsTierMetaTable.CF_INFO,
                                                   HdfsTierMetaTable.COL_HFILE_NAME);
          byte[] regionBytes = result.getValue(HdfsTierMetaTable.CF_INFO,
                                               HdfsTierMetaTable.COL_ENC_REG_NAME);
          byte[] sizeBytes = result.getValue(HdfsTierMetaTable.CF_INFO,
                                             HdfsTierMetaTable.COL_SIZE);
          byte[] lastAccessBytes = result.getValue(HdfsTierMetaTable.CF_INFO,
                                                    HdfsTierMetaTable.COL_LAST_ACCESS);
          byte[] createTimeBytes = result.getValue(HdfsTierMetaTable.CF_INFO,
                                                    HdfsTierMetaTable.COL_CREATE_TIME);

          if (hfileNameBytes == null || regionBytes == null || sizeBytes == null) {
            continue;
          }

          String hfileName = Bytes.toString(hfileNameBytes);
          String region = Bytes.toString(regionBytes);
          long size = Bytes.toLong(sizeBytes);
          long lastAccess = lastAccessBytes != null ? Bytes.toLong(lastAccessBytes) : 0L;
          long createTime = createTimeBytes != null ? Bytes.toLong(createTimeBytes) : 0L;

          // If no lastAccess, use createTime (never accessed)
          if (lastAccess == 0L) {
            lastAccess = createTime;
          }

          HFileEvictionCandidate candidate = new HFileEvictionCandidate(
              region, hfileName, size, lastAccess, createTime, state);

          allCandidates.add(candidate);
        }
      }
    }

    LOG.info("Found {} eviction candidates (ACTIVE, non-evicted)", allCandidates.size());

    // Check if there are no ACTIVE files available for eviction
    if (allCandidates.isEmpty()) {
      LOG.warn("No ACTIVE files found in metadata table - eviction cannot proceed");
      return new ArrayList<>();
    }

    // Sort by lastAccessTime (ascending) - oldest first
    allCandidates.sort(Comparator.comparingLong(HFileEvictionCandidate::getLastAccessTime));

    // Select files until we reach target bytes OR run out of ACTIVE files
    List<HFileEvictionCandidate> selectedFiles = new ArrayList<>();
    long bytesSelected = 0;

    for (HFileEvictionCandidate candidate : allCandidates) {
      selectedFiles.add(candidate);
      bytesSelected += candidate.getSize();

      if (bytesSelected >= targetBytesToEvict) {
        LOG.info("Target bytes reached: selected {} bytes from {} files (target: {} bytes)",
                 bytesSelected, selectedFiles.size(), targetBytesToEvict);
        break;
      }
    }

    // Check if we ran out of files before reaching target
    if (bytesSelected < targetBytesToEvict) {
      LOG.warn("Insufficient ACTIVE files to reach target: selected {} bytes from {} files (target: {} bytes, shortfall: {} bytes)",
               bytesSelected, selectedFiles.size(), targetBytesToEvict, (targetBytesToEvict - bytesSelected));
    }

    LOG.info("Selected {} files for eviction ({} bytes) using LRU policy",
             selectedFiles.size(), bytesSelected);

    return selectedFiles;
  }

  @Override
  public String getPolicyName() {
    return "LRU";
  }

  /**
   * Lazy connection initialization.
   */
  private void ensureConnection() throws IOException {
    if (connection == null || connection.isClosed()) {
      connection = ConnectionFactory.createConnection(conf);
    }
  }

  /**
   * Close connection.
   */
  public void close() throws IOException {
    if (connection != null) {
      connection.close();
    }
  }
}
