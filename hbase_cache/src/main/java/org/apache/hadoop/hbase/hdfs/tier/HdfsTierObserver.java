package org.apache.hadoop.hbase.hdfs.tier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

/**
 * HBase Coprocessor Observer captures HFile metadata during flush and compaction operations.
 *
 * This observer tracks HFile lifecycle events and stores metadata in the hdfsTier:meta table
 * for later use in tiering decisions, pre-warming, and cache eviction.
 */
public class HdfsTierObserver implements RegionCoprocessor, RegionObserver {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsTierObserver.class);
  private HdfsTierMetadataCapture metadataCapture;
  private Configuration conf;
  private HdfsTierStorageMonitor storageMonitor;
  private HdfsTierMetricsServer metricsServer;

  @Override
  public Optional<RegionObserver> getRegionObserver() {
    return Optional.of(this);
  }

  /**
   * Initializes the coprocessor when loaded by HBase.
   * Creates the metadata capture instance for tracking HFile operations.
   */
  @Override
  public void start(CoprocessorEnvironment env) {
    this.conf = env.getConfiguration();

    try {
      this.metadataCapture = new HdfsTierMetadataCapture(conf);

      // Initialize storage monitor singleton
      this.storageMonitor = HdfsTierStorageMonitor.getInstance(conf);

      // Start metrics HTTP server (only once, shared across all regions)
      synchronized (HdfsTierObserver.class) {
        if (this.metricsServer == null) {
          try {
            this.metricsServer = new HdfsTierMetricsServer(conf, storageMonitor);
            LOG.info("HdfsTier Metrics Server started successfully");
          } catch (IOException e) {
            LOG.warn("Failed to start metrics server: {}. Metrics will not be available via HTTP.",
              e.getMessage());
            this.metricsServer = null;
          }
        }
      }

      LOG.info("HdfsTierObserver started successfully");
    } catch (Exception e) {
      LOG.warn("Failed to initialize HdfsTierObserver: {}. Will retry on first flush.",
        e.getMessage() != null ? e.getMessage() : e.getClass().getName());
      this.metadataCapture = null;
    }
  }

  /**
   * Cleanup when coprocessor is stopped.
   * Ensures all buffered metadata is flushed and resources are released.
   */
  @Override
  public void stop(CoprocessorEnvironment env) throws IOException {
    LOG.info("HdfsTierObserver stopping");

    // Stop metrics server
    if (metricsServer != null) {
      try {
        metricsServer.stop();
        LOG.info("HdfsTier Metrics Server stopped successfully");
      } catch (Exception e) {
        LOG.error("Error stopping metrics server: {}", e.getMessage(), e);
      }
    }

    // Shutdown storage monitor
    if (storageMonitor != null) {
      try {
        storageMonitor.shutdown();
        LOG.info("HdfsTier Storage Monitor shut down successfully");
      } catch (Exception e) {
        LOG.error("Error shutting down storage monitor: {}", e.getMessage(), e);
      }
    }

    if (metadataCapture != null) {
      try {
        metadataCapture.close();
        LOG.info("HdfsTierMetadataCapture closed successfully");
      } catch (IOException e) {
        LOG.error("Error closing HdfsTierMetadataCapture: {}", e.getMessage(), e);
        throw e;
      }
    }
  }


  /**
   * Called after a memstore flush completes.
   * Captures metadata for all newly created HFiles.
   */
  @Override
  public void postFlush(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    FlushLifeCycleTracker tracker) throws IOException {
    try {
      captureFlushMetadata(ctx);
    } catch (Exception e) {
      LOG.error("Failed to capture flush metadata: {}", e.getMessage(), e);
    }
  }

  /**
   * Internal method to capture metadata for flushed HFiles.
   * Lazy-initializes metadata capture if it failed during start().
   */
  private void captureFlushMetadata(ObserverContext<? extends RegionCoprocessorEnvironment> ctx)
    throws IOException {
    // Lazy initialization: retry if start() failed
    if (metadataCapture == null) {
      synchronized (this) {
        if (metadataCapture == null) {
          try {
            LOG.info("Lazy-initializing HdfsTierMetadataCapture on first flush");
            this.metadataCapture = new HdfsTierMetadataCapture(conf);
          } catch (Exception e) {
            LOG.error("Failed to initialize metadata capture: {}. Skipping.",
              e.getMessage() != null ? e.getMessage() : e.getClass().getName());
            return;
          }
        }
      }
    }

    try {
      Region region = ctx.getEnvironment().getRegion();
      if (region == null) {
        LOG.warn("Region is null in postFlush callback");
        return;
      }

      // Skip system tables to avoid capturing system metadata
      String tableName = region.getTableDescriptor().getTableName().getNameAsString();
      if (tableName.startsWith("hbase:") || tableName.contains("hdfsTier:meta")) {
        LOG.debug("Skipping system table: {}", tableName);
        return;
      }

      String regionEncodedName = region.getRegionInfo().getEncodedName();

      // Iterate through all stores (column families) in this region
      for (Store store : region.getStores()) {
        if (store == null) {
          continue;
        }

        // Process each newly flushed HFile
        for (StoreFile storeFile : store.getStorefiles()) {
          try {
            Path originalFilePath = storeFile.getPath();
            if (originalFilePath == null) {
              LOG.warn("StoreFile path is null, skipping metadata capture");
              continue;
            }

            // Check if this is a reference file (from split regions)
            if (storeFile.isReference()) {
              // This is a reference file in a child region after split
              // Track the reference mapping for later compaction handling
              String refFileName = originalFilePath.getName();
              String parentFileName = refFileName.replace(".Reference", "");

              // Extract parent region from reference file name structure
              // Reference file format: parentFile.parentRegionEncoded
              String parentRegionName = extractParentRegionFromReference(storeFile);

              if (parentRegionName != null) {
                metadataCapture.recordReferenceFile(
                  regionEncodedName,
                  parentRegionName,
                  parentFileName,
                  refFileName
                );
                LOG.info("📝 Detected reference file during flush: child={}, ref={}, parent_region={}, parent_file={}",
                         regionEncodedName, refFileName, parentRegionName, parentFileName);
              } else {
                LOG.warn("⚠️ Could not extract parent region for reference: {}", refFileName);
              }

              // Skip capturing metadata for reference files
              // They don't represent actual data in our cache layer
              continue;
            }

            // Regular HFile (not a reference) - capture metadata
            // HDFS CACHE PATH: Placeholder for future HDFS cache directory
            // Later implementation will copy the file to this path
            String hdfsCachePath = "hdfs://placeholder/hbase_cache/" +
              originalFilePath.getName();

            // Convert String to Path for method signature consistency
            Path hdfsPath = new Path(hdfsCachePath);

            // Capture metadata with both original and HDFS cache paths
            metadataCapture.captureHFileMetadata(
              storeFile,
              region,
              originalFilePath,  // Source path for reading file properties
              hdfsPath          // Future HDFS cache path for storage
            );

            // Record flush in storage monitor for real-time metrics
            if (storageMonitor != null) {
              long fileSize = 0;
              try {
                if (storeFile instanceof HStoreFile) {
                  HStoreFile hstoreFile = (HStoreFile) storeFile;
                  StoreFileReader reader = hstoreFile.getReader();
                  if (reader != null) {
                    fileSize = reader.length();
                  }
                }
                if (fileSize > 0) {
                  storageMonitor.recordFlush(originalFilePath.getName(), fileSize);
                }
              } catch (Exception e) {
                LOG.warn("Failed to record flush in storage monitor: {}", e.getMessage());
              }
            }

            LOG.debug("Captured metadata for flushed file: {} in region {}",
              originalFilePath.getName(), regionEncodedName);
          } catch (Exception e) {
            LOG.error("Failed to capture metadata for store file {}: {}",
              storeFile.getPath(), e.getMessage(), e);
          }
        }
      }
    } catch (Exception e) {
      LOG.error("Unexpected error in flush metadata capture: {}", e.getMessage(), e);
    }
  }

  /**
   * Called after a compaction completes.
   * Updates state of old files to COMPACTED and captures metadata for new compacted file.
   */
  @Override
  public void postCompact(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    Store store,
    StoreFile resultFile,
    CompactionLifeCycleTracker tracker,
    CompactionRequest request) throws IOException {
    LOG.info("🔄 postCompact CALLED - store={}, resultFile={}, request={}",
             store != null ? store.getColumnFamilyName() : "null",
             resultFile != null ? resultFile.getPath() : "null",
             request != null ? "present" : "null");
    try {
      captureCompactionMetadata(ctx, store, resultFile, request);
    } catch (Exception e) {
      LOG.error("Failed to capture compaction metadata: {}", e.getMessage(), e);
    }
  }

  /**
   * Internal method to capture compaction metadata.
   * Marks input files as COMPACTED and captures metadata for output file.
   */
  private void captureCompactionMetadata(
    ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    Store store,
    StoreFile resultFile,
    CompactionRequest request) throws IOException {


    // Lazy initialization: retry if start() failed
    if (metadataCapture == null) {
      synchronized (this) {
        if (metadataCapture == null) {
          try {
            LOG.info("Lazy-initializing HdfsTierMetadataCapture on first compaction");
            this.metadataCapture = new HdfsTierMetadataCapture(conf);
          } catch (Exception e) {
            LOG.error("Failed to initialize metadata capture: {}. Skipping.",
              e.getMessage() != null ? e.getMessage() : e.getClass().getName());
            return;
          }
        }
      }
    }

    try {
      Region region = ctx.getEnvironment().getRegion();
      if (region == null) {
        LOG.warn("Region is null in postCompact callback");
        return;
      }

      // Skip system tables to avoid capturing system metadata
      String tableName = region.getTableDescriptor().getTableName().getNameAsString();
      if (tableName.startsWith("hbase:") || tableName.contains("hdfsTier:meta")) {
        LOG.debug("Skipping system table: {}", tableName);
        return;
      }

      LOG.info("📋 Processing compaction for table: {}", tableName);

      String regionEncodedName = region.getRegionInfo().getEncodedName();

      // Get parent files - try multiple sources
      java.util.Collection<? extends StoreFile> parentFiles = null;

      if (request != null && request.getFiles() != null && !request.getFiles().isEmpty()) {
        parentFiles = request.getFiles();
        LOG.info("✅ Got {} parent files from CompactionRequest", parentFiles.size());
      } else {
        // Fallback: try to get from store's compacted files
        try {
          parentFiles = store.getCompactedFiles();
          if (parentFiles != null && !parentFiles.isEmpty()) {
            LOG.info("✅ Got {} parent files from store.getCompactedFiles()", parentFiles.size());
          } else {
            LOG.warn("⚠️ No parent files found in request or store.getCompactedFiles()");
          }
        } catch (Exception e) {
          LOG.warn("Failed to get compacted files from store: {}", e.getMessage());
        }
      }

      // Calculate total size of parent files for storage monitor
      long totalParentSize = 0;
      int parentFileCount = 0;

      if (parentFiles != null && !parentFiles.isEmpty()) {
        for (StoreFile oldFile : parentFiles) {
          try {
            long fileSize = 0;
            if (oldFile instanceof HStoreFile) {
              HStoreFile hstoreFile = (HStoreFile) oldFile;
              StoreFileReader reader = hstoreFile.getReader();
              if (reader != null) {
                fileSize = reader.length();
                totalParentSize += fileSize;
                parentFileCount++;
              }
            }
            LOG.info("  📄 Parent file: {}, size: {} bytes",
                     oldFile.getPath().getName(), fileSize);
          } catch (Exception e) {
            LOG.warn("Failed to get size for parent file {}: {}",
              oldFile.getPath(), e.getMessage());
          }
        }
        LOG.info("📊 Total parent files: {}, Total size: {} bytes ({} MB)",
                 parentFileCount, totalParentSize, totalParentSize / (1024.0 * 1024));
      }

      // Mark old input files as COMPACTED
      if (parentFiles != null && !parentFiles.isEmpty()) {
        for (StoreFile oldFile : parentFiles) {
          try {
            Path oldFilePath = oldFile.getPath();
            String oldFileName = oldFilePath.getName();

            // Check if this is a reference file
            if (oldFile.isReference()) {
              // This is a reference file being compacted away
              // Remove ".Reference" suffix to get actual parent HFile name
              String parentFileName = oldFileName.replace(".Reference", "");

              // Get the parent region name from our reference tracking metadata
              String parentRegionName = metadataCapture.getParentRegionForReference(
                regionEncodedName,
                oldFileName
              );

              if (parentRegionName != null) {
                // Query for the parent file's creation timestamp
                Long parentTimestamp = metadataCapture.getFileTimestamp(
                  parentRegionName,
                  parentFileName
                );

                if (parentTimestamp != null) {
                  // Mark the ACTUAL parent HFile as compacted in parent region
                  metadataCapture.updateFileStateToCompacted(
                    parentRegionName,      // Parent region (correct!)
                    parentFileName,        // Actual HFile name (correct!)
                    parentTimestamp        // Parent file's timestamp
                  );

                  LOG.info("✅ Marked parent HFile as COMPACTED via reference: " +
                      "parent_region={}, parent_file={}, parent_ts={}, child_region={}, ref_file={}",
                    parentRegionName, parentFileName, parentTimestamp, regionEncodedName, oldFileName);
                } else {
                  LOG.warn("⚠️ Could not find timestamp for parent file: region={}, file={}",
                           parentRegionName, parentFileName);
                }
              } else {
                LOG.warn("⚠️ No parent region mapping found for reference: child={}, ref={}",
                         regionEncodedName, oldFileName);
              }

              // Remove the reference file record after marking parent as compacted
              metadataCapture.removeReferenceFile(regionEncodedName, oldFileName);

            } else {
              // Regular HFile compaction - get timestamp for this file
              Long fileTimestamp = metadataCapture.getFileTimestamp(
                regionEncodedName,
                oldFileName
              );

              if (fileTimestamp != null) {
                metadataCapture.updateFileStateToCompacted(
                  regionEncodedName,
                  oldFileName,
                  fileTimestamp
                );

                LOG.info("✅ Marked HFile as COMPACTED: region={}, file={}, timestamp={}",
                  regionEncodedName, oldFileName, fileTimestamp);
              } else {
                LOG.warn("⚠️ Could not find timestamp for file: region={}, file={}",
                         regionEncodedName, oldFileName);
              }
            }
          } catch (Exception e) {
            LOG.error("Failed to mark old file as compacted: {}", oldFile.getPath(), e);
          }
        }
      }

      // Capture metadata for new compacted output file
      if (resultFile != null) {
        try {
          Path newFilePath = resultFile.getPath();
          LOG.info("📝 Capturing NEW compacted file: {}", newFilePath.getName());

          // HDFS CACHE PATH: Placeholder for future HDFS cache directory
          String hdfsCachePath = "hdfs://placeholder/hbase_cache/" +
            newFilePath.getName();
          Path hdfsPath = new Path(hdfsCachePath);

          // Capture metadata for the newly created compacted file
          metadataCapture.captureHFileMetadata(
            resultFile,
            region,
            newFilePath,  // Original file path
            hdfsPath      // HDFS cache path (placeholder)
          );

          // Record compaction in storage monitor for real-time metrics
          if (storageMonitor != null) {
            long newFileSize = 0;
            try {
              if (resultFile instanceof HStoreFile) {
                HStoreFile hstoreFile = (HStoreFile) resultFile;
                StoreFileReader reader = hstoreFile.getReader();
                if (reader != null) {
                  newFileSize = reader.length();
                }
              }
              if (newFileSize > 0) {
                storageMonitor.recordCompaction(
                  newFilePath.getName(),
                  newFileSize,
                  totalParentSize
                );
                LOG.info("✅ StorageMonitor updated: new={} bytes, parent={} bytes, net={} bytes",
                         newFileSize, totalParentSize, (newFileSize - totalParentSize));
              }
            } catch (Exception e) {
              LOG.warn("Failed to record compaction in storage monitor: {}", e.getMessage());
            }
          } else {
            LOG.warn("⚠️ StorageMonitor is null - metrics not recorded");
          }

          LOG.info("✅ Captured metadata for new compacted file: {} in region {}",
            newFilePath.getName(), regionEncodedName);
        } catch (Exception e) {
          LOG.error("❌ Failed to capture metadata for new compacted file: {}",
            resultFile.getPath(), e);
          e.printStackTrace();
        }
      } else {
        LOG.warn("⚠️ ResultFile is null - no new compacted file to capture");
      }

      LOG.info("📊 captureCompactionMetadata COMPLETE");

    } catch (Exception e) {
      LOG.error("❌ Unexpected error in compaction metadata capture: {}", e.getMessage(), e);
    }
  }

  /**
   * Called after region split completes on PARENT region.
   * Marks parent HFiles as SPLIT_REFERENCE state since they are now referenced by child regions.
   *
   * NOTE: In HBase 4.0, split hooks are in MasterObserver, not RegionObserver.
   * This method can be called manually or through custom split tracking mechanism.
   *
   * @param ctx Observer context
   */
  public void postCompleteSplit(ObserverContext<RegionCoprocessorEnvironment> ctx)
      throws IOException {

    LOG.info("🔀 SPLIT EVENT: postCompleteSplit (Parent Region)");

    // Lazy initialization
    if (metadataCapture == null) {
      synchronized (this) {
        if (metadataCapture == null) {
          try {
            LOG.info("Lazy-initializing HdfsTierMetadataCapture for split tracking");
            this.metadataCapture = new HdfsTierMetadataCapture(conf);
          } catch (Exception e) {
            LOG.error("Failed to initialize metadata capture: {}", e.getMessage());
            return;
          }
        }
      }
    }

    try {
      Region region = ctx.getEnvironment().getRegion();
      if (region == null) {
        LOG.warn("Region is null in postCompleteSplit");
        return;
      }

      String parentRegionName = region.getRegionInfo().getEncodedName();
      String tableName = region.getTableDescriptor().getTableName().getNameAsString();

      // Skip system tables
      if (tableName.startsWith("hbase:") || tableName.contains("hdfsTier:meta")) {
        LOG.debug("Skipping system table split: {}", tableName);
        return;
      }

      LOG.info("Parent region {} split completed for table {}", parentRegionName, tableName);

      // Mark all parent HFiles as SPLIT_REFERENCE state
      int markedCount = 0;
      for (Store store : region.getStores()) {
        String columnFamily = store.getColumnFamilyName();

        for (StoreFile storeFile : store.getStorefiles()) {
          try {
            String hfileName = storeFile.getPath().getName();

            // Get timestamp for this HFile
            Long timestamp = metadataCapture.getFileTimestamp(parentRegionName, hfileName);

            if (timestamp != null) {
              // Mark as SPLIT_REFERENCE (parent file now referenced by children)
              metadataCapture.updateFileStateToSplitReference(
                parentRegionName,
                hfileName,
                timestamp
              );
              markedCount++;

              LOG.info("✅ Marked parent HFile as SPLIT_REFERENCE: region={}, cf={}, file={}",
                       parentRegionName, columnFamily, hfileName);
            } else {
              LOG.warn("⚠️ No timestamp found for parent HFile: {}", hfileName);
            }
          } catch (Exception e) {
            LOG.error("Failed to mark parent HFile: {}", storeFile.getPath(), e);
          }
        }
      }

      LOG.info("✅ Marked {} parent HFiles as SPLIT_REFERENCE in region {}",
               markedCount, parentRegionName);

    } catch (Exception e) {
      LOG.error("❌ Failed to track parent HFiles in postCompleteSplit: {}", e.getMessage(), e);
    }
  }

  /**
   * Called on CHILD regions after split completes.
   * Tracks reference files created in child regions and maps them to parent HFiles.
   *
   * NOTE: In HBase 4.0, split hooks are in MasterObserver, not RegionObserver.
   * This method can be called manually or through custom split tracking mechanism.
   *
   * @param ctx Observer context
   * @param leftChild Left child region (bottom key range)
   * @param rightChild Right child region (top key range)
   */
  public void postSplit(ObserverContext<RegionCoprocessorEnvironment> ctx,
                        Region leftChild, Region rightChild) throws IOException {

    LOG.info("🔀 SPLIT EVENT: postSplit (Child Regions)");

    // Lazy initialization
    if (metadataCapture == null) {
      synchronized (this) {
        if (metadataCapture == null) {
          try {
            LOG.info("Lazy-initializing HdfsTierMetadataCapture for split tracking");
            this.metadataCapture = new HdfsTierMetadataCapture(conf);
          } catch (Exception e) {
            LOG.error("Failed to initialize metadata capture: {}", e.getMessage());
            return;
          }
        }
      }
    }

    try {
      String leftName = leftChild.getRegionInfo().getEncodedName();
      String rightName = rightChild.getRegionInfo().getEncodedName();

      LOG.info("Child regions created: left={}, right={}", leftName, rightName);

      // Track reference files in both child regions
      trackChildRegionReferences(leftChild);
      trackChildRegionReferences(rightChild);

      LOG.info("✅ Split tracking complete for children: left={}, right={}", leftName, rightName);

    } catch (Exception e) {
      LOG.error("❌ Failed to track child references in postSplit: {}", e.getMessage(), e);
    }
  }

  /**
   * Internal helper to track reference files in a child region after split.
   * Creates mapping: child reference file → parent region/HFile
   *
   * @param child Child region created by split
   */
  private void trackChildRegionReferences(Region child) {
    if (child == null) {
      LOG.warn("Child region is null");
      return;
    }

    String childRegionName = child.getRegionInfo().getEncodedName();
    String tableName = child.getRegionInfo().getTable().getNameAsString();

    // Skip system tables
    if (tableName.startsWith("hbase:") || tableName.contains("hdfsTier:meta")) {
      LOG.debug("Skipping system table: {}", tableName);
      return;
    }

    LOG.info("🔍 Scanning child region {} for reference files", childRegionName);

    int refCount = 0;
    try {
      // Iterate through all stores (column families) in child region
      for (Store store : child.getStores()) {
        if (store == null) {
          continue;
        }

        String columnFamily = store.getColumnFamilyName();

        // Check each StoreFile for references
        for (StoreFile sf : store.getStorefiles()) {
          if (sf.isReference()) {
            try {
              // Extract reference file information
              String refFileName = sf.getPath().getName();
              String parentFileName = refFileName.replace(".Reference", "");

              // Get parent region from reference using HBase API
              String parentRegionName = extractParentRegionFromReference(sf);

              if (parentRegionName != null) {
                // Record the reference mapping in metadata table
                metadataCapture.recordReferenceFile(
                  childRegionName,
                  parentRegionName,
                  parentFileName,
                  refFileName
                );
                refCount++;

                LOG.info("📝 Tracked reference: child={}, cf={}, parent_region={}, " +
                         "parent_file={}, ref_file={}",
                         childRegionName, columnFamily, parentRegionName,
                         parentFileName, refFileName);
              } else {
                LOG.warn("⚠️ Could not determine parent region for reference: {}", refFileName);
              }

            } catch (Exception e) {
              LOG.error("Failed to track reference file {}: {}", sf.getPath(), e.getMessage(), e);
            }
          }
        }
      }

      LOG.info("✅ Tracked {} reference files in child region {}", refCount, childRegionName);

    } catch (Exception e) {
      LOG.error("Error scanning child region {} for references: {}",
               childRegionName, e.getMessage(), e);
    }
  }

  /**
   * Extracts parent region name from a reference StoreFile.
   * Reference file names have format: "parentFileName.parentRegionEncodedName"
   * Example: "abc123.def456789abc" where "def456789abc" is the parent region encoded name
   *
   * @param storeFile Reference StoreFile
   * @return Parent region encoded name, or null if cannot be determined
   */
  private String extractParentRegionFromReference(StoreFile storeFile) {
    try {
      if (storeFile == null || !storeFile.isReference()) {
        return null;
      }

      // Reference file path structure:
      // .../table/childRegion/cf/parentFile.parentRegionEncoded
      // Example: .../mytable/abc123/cf/file001.def456789abc

      Path refPath = storeFile.getPath();
      String refFileName = refPath.getName();

      // Reference file name format: "parentFileName.parentRegionEncodedName"
      // We need to extract the parent region encoded name (part after last dot)
      int lastDot = refFileName.lastIndexOf('.');
      if (lastDot > 0 && lastDot < refFileName.length() - 1) {
        String parentRegionEncoded = refFileName.substring(lastDot + 1);

        LOG.debug("Extracted parent region: {} from reference: {}",
                 parentRegionEncoded, refFileName);

        return parentRegionEncoded;
      } else {
        LOG.warn("Cannot parse parent region from reference file name: {}", refFileName);
      }

    } catch (Exception e) {
      LOG.warn("Failed to extract parent region from reference {}: {}",
              storeFile.getPath(), e.getMessage());
    }

    return null;
  }

  /**
   * Helper method to extract parent region name from a reference StoreFile.
   * Reference files contain metadata about which region/file they reference.
   *
   * @param storeFile Reference StoreFile
   * @return Parent region encoded name, or null if cannot be determined
   * @deprecated Use extractParentRegionFromReference instead
   */
  private String extractParentRegionName(StoreFile storeFile) {
    return extractParentRegionFromReference(storeFile);
  }
}




