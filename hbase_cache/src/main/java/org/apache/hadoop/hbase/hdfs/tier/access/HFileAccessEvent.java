package org.apache.hadoop.hbase.hdfs.tier.access;


/**
 * Immutable event object representing an HFile access.
 * Uses Builder pattern
 */
public class HFileAccessEvent {
  private final String hfilePath;
  private final long accessTimestamp;
  private final String regionEncodedName;


  private HFileAccessEvent(Builder builder) {
    this.hfilePath = builder.hfilePath;
    this.accessTimestamp = builder.accessTimestamp;
    this.regionEncodedName = builder.regionEncodedName;

  }

  public String getHfilePath() {
    return hfilePath;
  }

  public long getAccessTimestamp() {
    return accessTimestamp;
  }

  public String getRegionEncodedName() {
    return regionEncodedName;
  }



  /**
   * Extract HFile name from full path.
   * Example: /hbase/data/table/region/cf/abc123 → abc123
   */
  public String getHfileName() {
    if (hfilePath == null) return null;
    int lastSlash = hfilePath.lastIndexOf('/');
    return lastSlash >= 0 ? hfilePath.substring(lastSlash + 1) : hfilePath;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private String hfilePath;
    private long accessTimestamp = System.currentTimeMillis();
    private String regionEncodedName;

    public Builder hfilePath(String path) {
      this.hfilePath = path;
      return this;
    }

    public Builder accessTimestamp(long timestamp) {
      this.accessTimestamp = timestamp;
      return this;
    }

    public Builder regionEncodedName(String name) {
      this.regionEncodedName = name;
      return this;
    }

    public HFileAccessEvent build() {
      if (hfilePath == null || hfilePath.isEmpty()) {
        throw new IllegalArgumentException("HFile path cannot be null or empty");
      }
      if (regionEncodedName == null || regionEncodedName.isEmpty()) {
        throw new IllegalArgumentException(
          "Region encoded name cannot be null or empty - required for rowkey formation");
      }
      return new HFileAccessEvent(this);
    }
  }

  @Override
  public String toString() {
    return "HFileAccessEvent{" +
        "hfilePath='" + hfilePath + '\'' +
        ", accessTimestamp=" + accessTimestamp +
        ", regionEncodedName='" + regionEncodedName + '\'' +
        '}';
  }
}
