package org.apache.hadoop.hbase.hdfs.tier.eviction;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.hdfs.tier.HdfsTierMetadataCapture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes eviction of HFiles from HDFS cache tier.
 *
 * RESPONSIBILITIES:
 * 1. Mark files as EVICTED in metadata table (NO physical deletion)
 * 2. Track eviction statistics
 * 3. Handle failures gracefully
 *
 * DESIGN NOTE:
 * - Physical files are NOT deleted from HDFS
 * - Only metadata table is updated to mark files as evicted
 * - This allows for potential recovery or analysis
 *
 * FAILURE HANDLING:
 * - Continues eviction even if individual files fail
 * - Logs errors for manual intervention
 * - Returns actual bytes marked for eviction
 */
public class HDFSTierEvictionExecutor {

  private static final Logger LOG = LoggerFactory.getLogger(HDFSTierEvictionExecutor.class);

  private final Configuration conf;
  private final HdfsTierMetadataCapture metadataCapture;

  // Statistics
  private final AtomicInteger totalEvictions = new AtomicInteger(0);
  private final AtomicInteger failedEvictions = new AtomicInteger(0);

  public HDFSTierEvictionExecutor(Configuration conf, HdfsTierMetadataCapture metadataCapture) {
    this.conf = conf;
    this.metadataCapture = metadataCapture;

    LOG.info("HDFSTierEvictionExecutor initialized (logical eviction mode - no physical deletion)");
  }

  /**
   * Execute eviction for selected files.
   *
   * NOTE: Files are NOT physically deleted, only marked as EVICTED in metadata table.
   *
   * @param filesToEvict List of files to evict
   * @return Actual bytes marked for eviction
   */
  public long executeEviction(List<HFileEvictionCandidate> filesToEvict) {
    if (filesToEvict == null || filesToEvict.isEmpty()) {
      LOG.warn("No files to evict");
      return 0L;
    }

    LOG.info("Starting eviction of {} files", filesToEvict.size());

    long totalBytesFreed = 0L;
    int successCount = 0;

    for (HFileEvictionCandidate candidate : filesToEvict) {
      try {
        long bytesFreed = evictSingleFile(candidate);
        if (bytesFreed > 0) {
          totalBytesFreed += bytesFreed;
          successCount++;
          totalEvictions.incrementAndGet();
        }
      } catch (Exception e) {
        LOG.error("Failed to evict file: {} in region {}",
                  candidate.getHfileName(), candidate.getRegionEncodedName(), e);
        failedEvictions.incrementAndGet();
        // Continue with next file
      }
    }

    LOG.info("Eviction complete: {} succeeded, {} failed, {} bytes freed",
             successCount, filesToEvict.size() - successCount, totalBytesFreed);

    return totalBytesFreed;
  }

  /**
   * Evict a single HFile (logical eviction only).
   *
   * OPERATION:
   * - Updates metadata table to mark file as EVICTED
   * - Does NOT delete physical file from HDFS
   * - File remains on disk but is marked as evicted for tracking
   *
   * @return Bytes marked for eviction (0 if failed)
   */
  private long evictSingleFile(HFileEvictionCandidate candidate) throws IOException {
    String hfileName = candidate.getHfileName();
    String region = candidate.getRegionEncodedName();
    long fileSize = candidate.getSize();

    LOG.info("Marking HFile as evicted: {} from region {} (size: {} bytes)",
             hfileName, region, fileSize);

    // Update metadata table - mark as EVICTED
    try {
      metadataCapture.updateFileStateToEvicted(region, hfileName, 0L);
      LOG.info("Successfully marked {} as EVICTED in metadata table", hfileName);
      return fileSize;
    } catch (IOException e) {
      LOG.error("Failed to mark {} as evicted in metadata table", hfileName, e);
      throw e;
    }
  }

  /**
   * Get total successful evictions.
   */
  public int getTotalEvictions() {
    return totalEvictions.get();
  }

  /**
   * Get total failed evictions.
   */
  public int getFailedEvictions() {
    return failedEvictions.get();
  }
}
