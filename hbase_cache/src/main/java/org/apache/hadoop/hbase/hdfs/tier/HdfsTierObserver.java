package org.apache.hadoop.hbase.hdfs.tier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.*;
import org.apache.hadoop.hbase.hdfs.tier.access.StoreFileScannerAccessTracker;
import org.apache.hadoop.hbase.hdfs.tier.eviction.HDFSTierEvictionChoreService;
import org.apache.hadoop.hbase.hdfs.tier.eviction.HDFSTierEvictionCoordinator;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionLifeCycleTracker;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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
  private HDFSTierEvictionCoordinator evictionCoordinator;
  private HDFSTierEvictionChoreService evictionChoreService;

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

      // Initialize HFile access tracking system
      try {
        StoreFileScannerAccessTracker.initialize(conf);
        LOG.info("HFile access tracking initialized successfully");
      } catch (Exception e) {
        LOG.error("Failed to initialize HFile access tracking: {}", e.getMessage(), e);
      }

      // Initialize storage monitor singleton
      this.storageMonitor = HdfsTierStorageMonitor.getInstance(conf);

      // Trigger immediate reconciliation as soon as HdfsTierObserver is loaded
      LOG.info("Triggering storage reconciliation on HdfsTierObserver initialization");
      this.storageMonitor.forceReconciliation();

      // Initialize eviction coordinator
      try {
        this.evictionCoordinator = new HDFSTierEvictionCoordinator(conf, storageMonitor, metadataCapture);
        LOG.info("HDFSTier Eviction Coordinator initialized successfully");
      } catch (Exception e) {
        LOG.error("Failed to initialize eviction coordinator: {}", e.getMessage(), e);
        this.evictionCoordinator = null;
      }

      // Initialize and start eviction chore service (handles periodic eviction checks)
      if (this.evictionCoordinator != null) {
        try {
          this.evictionChoreService = new HDFSTierEvictionChoreService(conf, storageMonitor, evictionCoordinator);
          this.evictionChoreService.start();
          LOG.info("HDFSTier Eviction Chore Service started successfully");
        } catch (Exception e) {
          LOG.error("Failed to start eviction chore service: {}", e.getMessage(), e);
          this.evictionChoreService = null;
        }
      }

      // Start metrics HTTP server (only once, shared across all regions)
      synchronized (HdfsTierObserver.class) {
        if (this.metricsServer == null) {
          try {
            this.metricsServer = new HdfsTierMetricsServer(conf, storageMonitor);
            LOG.info("HdfsTier Metrics Server started successfully");
          } catch (IOException e) {
            LOG.warn("Failed to start metrics server: {}.",
              e.getMessage());
            this.metricsServer = null;
          }
        }
      }

      LOG.info("HdfsTierObserver started successfully");
    } catch (Exception e) {
      LOG.warn("Failed to initialize HdfsTierObserver: {}.",
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
    // Shutdown HFile access tracking
    try {
      StoreFileScannerAccessTracker.shutdown();
      LOG.info("HFile access tracking shut down successfully");
    } catch (Exception e) {
      LOG.error("Error shutting down access tracking: {}", e.getMessage(), e);
    }

    // Shutdown eviction chore service
    if (evictionChoreService != null) {
      try {
        evictionChoreService.stop("HdfsTierObserver stopping");
        LOG.info("HDFSTier Eviction Chore Service stopped successfully");
      } catch (Exception e) {
        LOG.error("Error stopping eviction chore service: {}", e.getMessage(), e);
      }
    }

    // Shutdown eviction coordinator
    if (evictionCoordinator != null) {
      try {
        evictionCoordinator.shutdown();
        LOG.info("HDFSTier Eviction Coordinator shut down successfully");
      } catch (Exception e) {
        LOG.error("Error shutting down eviction coordinator: {}", e.getMessage(), e);
      }
    }

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
   * Captures regular HFiles - reference files are ignored (handled during compaction).
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

      // Skip system tables
      String tableName = region.getTableDescriptor().getTableName().getNameAsString();
      if (tableName.startsWith("hbase:") || tableName.contains("hdfsTier:meta")) {
        LOG.debug("Skipping system table: {}", tableName);
        return;
      }

      String regionEncodedName = region.getRegionInfo().getEncodedName();

      // Iterate through all stores (column families)
      for (Store store : region.getStores()) {
        if (store == null) {
          continue;
        }

        // Process each newly flushed HFile
        for (StoreFile storeFile : store.getStorefiles()) {
          try {
            Path originalFilePath = storeFile.getPath();
            if (originalFilePath == null) {
              LOG.warn("StoreFile path is null, skipping");
              continue;
            }

            if (storeFile.isReference()) {
              LOG.debug("Skipping reference file during flush: {}", originalFilePath.getName());
              continue;
            }

            // Regular HFile - capture metadata
            String hdfsCachePath = "hdfs://hbase_cache/" + originalFilePath.getName();
            Path hdfsPath = new Path(hdfsCachePath);

            metadataCapture.captureHFileMetadata(
              storeFile,
              region,
              originalFilePath,
              hdfsPath
            );

            // Record flush in storage monitor
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

            LOG.debug(" Captured flush metadata: {} in region {}",
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
   * EVENT-AGNOSTIC: Handles regular compaction, split reference compaction, and merge reference compaction uniformly.
   *
   * Actions:
   * 1. Mark old input files as COMPACTED
   * 2. Capture metadata for new compacted output file
   * 3. Update storage monitor (remove old sizes, add new size)
   */
  @Override
  public void postCompact(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    Store store,
    StoreFile resultFile,
    CompactionLifeCycleTracker tracker,
    CompactionRequest request) throws IOException {
    LOG.info(" postCompact triggered - store={}, resultFile={}",
             store != null ? store.getColumnFamilyName() : "null",
             resultFile != null ? resultFile.getPath().getName() : "null");
    try {
      captureCompactionMetadata(ctx, store, resultFile, request);
    } catch (Exception e) {
      LOG.error("Failed to capture compaction metadata: {}", e.getMessage(), e);
    }
  }

  /**
   * EVENT-AGNOSTIC compaction handler.
   * Works uniformly for:
   * - Regular compaction: Marks regular HFiles as COMPACTED
   * - Split compaction: Marks parent region's HFiles as COMPACTED (via reference detection)
   * - Merge compaction: Marks multiple parent regions' HFiles as COMPACTED (via reference detection)
   */
  private void captureCompactionMetadata(
    ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
    Store store,
    StoreFile resultFile,
    CompactionRequest request) throws IOException {

    // Lazy initialization
    if (metadataCapture == null) {
      synchronized (this) {
        if (metadataCapture == null) {
          try {
            LOG.info("initializing HdfsTierMetadataCapture");
            this.metadataCapture = new HdfsTierMetadataCapture(conf);
          } catch (Exception e) {
            LOG.error("Failed to initialize metadata capture: {}. Skipping.", e.getMessage());
            return;
          }
        }
      }
    }

    try {
      Region region = ctx.getEnvironment().getRegion();
      if (region == null) {
        LOG.warn("Region is null in postCompact");
        return;
      }

      // Skip system tables
      String tableName = region.getTableDescriptor().getTableName().getNameAsString();
      if (tableName.startsWith("hbase:") || tableName.contains("hdfsTier:meta")) {
        return;
      }

      String currentRegionName = region.getRegionInfo().getEncodedName();
      LOG.info("Compaction in region: {}, table: {}", currentRegionName, tableName);

      // Get input files being compacted
      java.util.Collection<? extends StoreFile> inputFiles = null;
      if (request != null && request.getFiles() != null && !request.getFiles().isEmpty()) {
        inputFiles = request.getFiles();
      } else {
        try {
          inputFiles = store.getCompactedFiles();
        } catch (Exception e) {
          LOG.warn("Failed to get compacted files: {}", e.getMessage());
        }
      }

      if (inputFiles == null || inputFiles.isEmpty()) {
        LOG.warn("No input files found for compaction");
        return;
      }

      LOG.info("Processing {} input files", inputFiles.size());

      // Track total size removed from storage
      long totalSizeRemoved = 0;

      // Process each input file
      for (StoreFile inputFile : inputFiles) {
        try {
          Path inputPath = inputFile.getPath();
          String inputFileName = inputPath.getName();

          // Get file size for storage tracking
          long fileSize = 0;
          try {
            if (inputFile instanceof HStoreFile) {
              StoreFileReader reader = ((HStoreFile) inputFile).getReader();
              if (reader != null) {
                fileSize = reader.length();
                totalSizeRemoved += fileSize;
              }
            }
          } catch (Exception e) {
            LOG.warn("Failed to get size for file {}: {}", inputFileName, e.getMessage());
          }

          // CASE 1: Regular HFile compaction
          if (!inputFile.isReference()) {
            // Row key: {currentRegionName}#{inputFileName}
            metadataCapture.updateFileStateToCompacted(
              currentRegionName,
              inputFileName,
              0L  // timestamp not used for row key lookup
            );
            LOG.info("Regular HFile → COMPACTED: {} in region {} (size: {} bytes)",
                     inputFileName, currentRegionName, fileSize);
          }
          // CASE 2: Reference file compaction (from split or merge)
          else {
            String parentFileName = extractParentFileNameFromReference(inputFileName);
            String parentRegionName = extractParentRegionFromReference(inputFile);

            if (parentRegionName != null && parentFileName != null) {
              // Row key: {parentRegionName}#{parentFileName}
              metadataCapture.updateFileStateToCompacted(
                parentRegionName,
                parentFileName,
                0L  // timestamp not used for row key lookup
              );
              LOG.info("Reference HFile → Parent COMPACTED: {} in region {}",
                       parentFileName, parentRegionName);
            } else {
              LOG.warn("Could not resolve parent for reference: {}", inputFileName);
            }
          }
        } catch (Exception e) {
          LOG.error("Failed to process input file {}: {}", inputFile.getPath(), e.getMessage());
        }
      }

      LOG.info("Total size removed: {} bytes ({} MB)",
               totalSizeRemoved, totalSizeRemoved / (1024.0 * 1024));

      // Capture metadata for NEW compacted output file(s)
      // NOTE: HBase can produce multiple output files from a single compaction if the result is large
      // The resultFile parameter may only reference one of them, so we check the store for all new files

      List<StoreFile> newFiles = new ArrayList<>();
      long totalNewFileSize = 0;

      // Start with the provided resultFile
      if (resultFile != null) {
        newFiles.add(resultFile);
      }

      // Check store for additional files that may have been created during this compaction
      // This handles cases where compaction produces multiple output files
      try {
        Collection<? extends StoreFile> storeFiles = store.getStorefiles();
        if (storeFiles != null && !storeFiles.isEmpty()) {
          // Get timestamp of when compaction started (approximate)
          long compactionTime = System.currentTimeMillis();

          // Look for files created very recently (within last 5 seconds)
          // that aren't in the input files list
          Set<String> inputFileNames = new HashSet<>();
          for (StoreFile inputFile : inputFiles) {
            inputFileNames.add(inputFile.getPath().getName());
          }

          for (StoreFile sf : storeFiles) {
            String fileName = sf.getPath().getName();
            // Skip if it was an input file
            if (inputFileNames.contains(fileName)) {
              continue;
            }

            // If we already have this file from resultFile, skip
            if (resultFile != null && fileName.equals(resultFile.getPath().getName())) {
              continue;
            }

            // Check if file is new (created recently)
            try {
              FileSystem fs = sf.getPath().getFileSystem(conf);
              FileStatus fileStatus = fs.getFileStatus(sf.getPath());
              long fileModTime = fileStatus.getModificationTime();

              // If modified within last 10 seconds, likely from this compaction
              if (compactionTime - fileModTime < 10000) {
                newFiles.add(sf);
                LOG.info("Detected additional compaction output file: {}", fileName);
              }
            } catch (Exception e) {
              LOG.warn("Failed to check modification time for {}: {}", fileName, e.getMessage());
            }
          }
        }
      } catch (Exception e) {
        LOG.warn("Failed to check store for additional output files: {}", e.getMessage());
      }

      // Process all new output files
      if (newFiles.isEmpty()) {
        LOG.warn("No result files found from compaction");
      } else {
        LOG.info("Processing {} compaction output file(s)", newFiles.size());

        for (StoreFile newFile : newFiles) {
          try {
            Path newFilePath = newFile.getPath();
            String newFileName = newFilePath.getName();

            // Result file should be a regular HFile, not a reference
            if (newFile.isReference()) {
              LOG.warn("Result file is a reference (unexpected): {}", newFileName);
              continue;
            }

            // HDFS cache path placeholder
            String hdfsCachePath = "hdfs://hbase_cache/" + newFileName;
            Path hdfsPath = new Path(hdfsCachePath);

            // Capture metadata for new file
            metadataCapture.captureHFileMetadata(
              newFile,
              region,
              newFilePath,
              hdfsPath
            );

            // Get new file size
            long newFileSize = 0;
            try {
              if (newFile instanceof HStoreFile) {
                StoreFileReader reader = ((HStoreFile) newFile).getReader();
                if (reader != null) {
                  newFileSize = reader.length();
                  totalNewFileSize += newFileSize;
                }
              }
            } catch (Exception e) {
              LOG.warn("Failed to get size for new file {}: {}", newFileName, e.getMessage());
            }

            LOG.info("Captured compaction output file: {} (size: {} bytes)", newFileName, newFileSize);

          } catch (Exception e) {
            LOG.error("Failed to capture compaction output file: {}", newFile.getPath(), e);
          }
        }

        // Update storage monitor with aggregated values
        if (storageMonitor != null && totalNewFileSize > 0) {
          // Use first file name for logging purposes
          String firstFileName = newFiles.get(0).getPath().getName();
          storageMonitor.recordCompaction(firstFileName, totalNewFileSize, totalSizeRemoved);
          LOG.info("StorageMonitor: added {} bytes (from {} files), removed {} bytes, net change: {} bytes",
                   totalNewFileSize, newFiles.size(), totalSizeRemoved, (totalNewFileSize - totalSizeRemoved));
        }
      }

      LOG.info("Compaction metadata capture complete");

    } catch (Exception e) {
      LOG.error("Unexpected error in compaction capture: {}", e.getMessage(), e);
    }
  }

  /**
   * Extracts parent HFile name from reference file name.
   * Reference format: "parentFile.parentRegionEncoded" or "parentFile.Reference"
   * Example: "abc123.def456789" → "abc123"
   */
  private String extractParentFileNameFromReference(String referenceFileName) {
    if (referenceFileName == null) {
      return null;
    }

    // Remove common reference suffixes
    String cleaned = referenceFileName.replace(".Reference", "");

    // Extract base file name (before last dot)
    int lastDot = cleaned.lastIndexOf('.');
    if (lastDot > 0) {
      return cleaned.substring(0, lastDot);
    }

    return cleaned;
  }

  /**
   * Extracts parent region name from reference file.
   * Reference file naming: "parentFileName.parentRegionEncoded"
   * Example: "abc123.def456789abc" → "def456789abc" (parent region)
   */
  private String extractParentRegionFromReference(StoreFile storeFile) {
    try {
      if (storeFile == null || !storeFile.isReference()) {
        return null;
      }

      Path refPath = storeFile.getPath();
      String refFileName = refPath.getName();

      // Extract parent region encoded name
      int lastDot = refFileName.lastIndexOf('.');
      if (lastDot > 0 && lastDot < refFileName.length() - 1) {
        String parentRegionEncoded = refFileName.substring(lastDot + 1);
        LOG.debug("Extracted parent region: {} from reference: {}",
                 parentRegionEncoded, refFileName);
        return parentRegionEncoded;
      } else {
        LOG.warn("Cannot parse parent region from reference");
      }
    } catch (Exception e) {
      LOG.warn("Failed to extract parent region from reference {}: {}",
              storeFile.getPath(), e.getMessage());
    }
    return null;
  }

  /**
   * Called after a Get operation completes.
   * Tracks access to HFiles for read tracking.
   */
  @Override
  public void postGetOp(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
                        org.apache.hadoop.hbase.client.Get get,
                        java.util.List<org.apache.hadoop.hbase.Cell> results) throws IOException {
    trackRegionAccess(ctx);
  }

  /**
   * Called before scanner is opened.
   * Tracks access to HFiles for read tracking.
   */
  @Override
  public void preScannerOpen(ObserverContext<? extends RegionCoprocessorEnvironment> ctx,
                             org.apache.hadoop.hbase.client.Scan scan) throws IOException {
    trackRegionAccess(ctx);
  }

  /**
   * Tracks access to HFiles in the current region
   * Called from Get and Scan operations to record HFile reads
   */
  private void trackRegionAccess(ObserverContext<? extends RegionCoprocessorEnvironment> ctx) {
    try {
      Region region = ctx.getEnvironment().getRegion();
      if (region == null) {
        return;
      }

      // Skip system tables
      String tableName = region.getTableDescriptor().getTableName().getNameAsString();
      if (tableName.startsWith("hbase:") || tableName.contains("hdfsTier:meta")) {
        return;
      }

      // Track access for all store files in all column families
      for (Store store : region.getStores()) {
        if (store == null) {
          continue;
        }

        for (StoreFile storeFile : store.getStorefiles()) {
          if (storeFile == null) {
            continue;
          }

          try {
            // Cast to HStoreFile to access getReader()
            if (storeFile instanceof HStoreFile) {
              HStoreFile hstoreFile = (HStoreFile) storeFile;
              StoreFileReader reader = hstoreFile.getReader();
              if (reader != null) {
                // Track this HFile access
                StoreFileScannerAccessTracker.trackAccess(reader, (org.apache.hadoop.hbase.regionserver.HRegion) region);
              }
            }
          } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Failed to track access for store file: {}", e.getMessage());
            }
          }
        }
      }
    } catch (Exception e) {
      // Fail silently
      if (LOG.isDebugEnabled()) {
        LOG.debug("Failed to track region access: {}", e.getMessage());
      }
    }
  }
}
