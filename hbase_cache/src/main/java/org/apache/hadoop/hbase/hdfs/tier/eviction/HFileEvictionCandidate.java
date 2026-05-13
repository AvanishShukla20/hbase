package org.apache.hadoop.hbase.hdfs.tier.eviction;

/**
 * Represents an HFile candidate for eviction.
 * Contains metadata needed to identify and evict the file.
 */
public class HFileEvictionCandidate {

  private final String regionEncodedName;
  private final String hfileName;
  private final long size;
  private final long lastAccessTime;
  private final long createTime;
  private final String currentState;

  public HFileEvictionCandidate(String regionEncodedName, String hfileName,
                                 long size, long lastAccessTime,
                                 long createTime, String currentState) {
    this.regionEncodedName = regionEncodedName;
    this.hfileName = hfileName;
    this.size = size;
    this.lastAccessTime = lastAccessTime;
    this.createTime = createTime;
    this.currentState = currentState;
  }

  public String getRegionEncodedName() {
    return regionEncodedName;
  }

  public String getHfileName() {
    return hfileName;
  }

  public long getSize() {
    return size;
  }

  public long getLastAccessTime() {
    return lastAccessTime;
  }

  public long createTime() {
    return createTime;
  }

  public String getCurrentState() {
    return currentState;
  }

  /**
   * Get row key for this HFile in metadata table.
   */
  public String getRowKey() {
    return regionEncodedName + "#" + hfileName;
  }

  @Override
  public String toString() {
    return String.format("HFileEvictionCandidate{region=%s, file=%s, size=%d, lastAccess=%d, state=%s}",
        regionEncodedName, hfileName, size, lastAccessTime, currentState);
  }
}
