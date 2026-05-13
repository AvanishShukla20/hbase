package org.apache.hadoop.hbase.hdfs.tier.eviction;

import java.io.IOException;
import java.util.List;

/**
 * Strategy interface for HFile eviction policies.
 *
 * Implementations define the criteria for selecting which HFiles to evict
 * when storage limits are exceeded.
 *
 * DESIGN PATTERN: Strategy Pattern
 * - Allows pluggable eviction algorithms
 * - Each policy implements its own selection logic
 *
 * THREAD-SAFETY: Implementations must be thread-safe
 */
public interface EvictionPolicy {

  /**
   * Select HFiles to evict based on policy criteria.
   *
   * @param targetBytesToEvict Number of bytes that need to be freed
   * @return List of HFileEvictionCandidate objects to evict (sorted by priority)
   * @throws IOException If metadata table query fails
   */
  List<HFileEvictionCandidate> selectFilesForEviction(long targetBytesToEvict) throws IOException;

  /**
   * Get the name of this eviction policy.
   *
   * @return Policy name (e.g., "LRU", "LFU", "FIFO")
   */
  String getPolicyName();
}
