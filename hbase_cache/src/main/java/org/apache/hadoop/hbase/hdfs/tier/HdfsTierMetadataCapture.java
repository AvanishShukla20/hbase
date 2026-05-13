package org.apache.hadoop.hbase.hdfs.tier;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.HStoreFile;
import org.apache.hadoop.hbase.regionserver.Region;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileReader;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Captures HFile metadata when files are written to hbase_cache directory.
 * Hook into flush and compaction operations to populate hdfsTier:meta table.
 * THREAD-SAFETY DESIGN:
 * - Uses BufferedMutator for high-throughput concurrent writes
 * - Connection pooling via shared ConnectionFactory
 * - Atomic operations for state tracking
 * - Graceful shutdown with flush guarantees
 *
 * CONCURRENCY CONTROL:
 * - Multiple threads can call captureHFileMetadata() concurrently
 * - BufferedMutator handles batching and async writes internally
 * - No explicit locks needed - HBase client handles synchronization
 *
 * DEADLOCK PREVENTION:
 * - No nested locks
 * - No circular dependencies on resources
 * - Uses timeout-based operations where applicable
 *
 * FAILURE HANDLING:
 * - ExceptionListener captures write failures
 * - Dead-letter queue with exponential backoff retry (10s, 30s, 60s)
 * - Max 3 retries per failed write
 * - After max retries, log for manual intervention
 * - Flush/compaction operations succeed even if metadata write fails
 */
public class HdfsTierMetadataCapture {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsTierMetadataCapture.class);

    // Thread-safe connection shared across operations
    private final Connection connection;
    private final Configuration conf;

    // For high-throughput concurrent writes
    private final BufferedMutator bufferedMutator;

    // for async operations
    private final ExecutorService asyncExecutor;

    // Tracks pending writes for graceful shutdown
    private final AtomicInteger pendingWrites = new AtomicInteger(0);

    // Timeout for flush operations to prevent indefinite blocking
    private static final long FLUSH_TIMEOUT_SECONDS = 60;

    // Dead letter queue for failed writes. It stores failed mutations for later retry or manual intervention
    private final BlockingQueue<FailedWrite> deadLetterQueue = new LinkedBlockingQueue<>(20000);

    // Statistics for monitoring
    private final AtomicInteger totalFailures = new AtomicInteger(0);
    private final AtomicInteger retriedSuccesses = new AtomicInteger(0);

    /**
     * Represents a failed write operation for retry.
     */
    private static class FailedWrite {
        final Put mutation;
        final String rowKey;
        final Throwable cause;
        final long timestamp;
        int retryCount;

        FailedWrite(Put mutation, String rowKey, Throwable cause) {
            this.mutation = mutation;
            this.rowKey = rowKey;
            this.cause = cause;
            this.timestamp = System.currentTimeMillis();
            this.retryCount = 0;
        }
    }

    /**
     * Constructs metadata capture instance with thread-safe resources.
     *
     * @param conf HBase configuration
     * @throws IOException If connection or table creation fails
     */
    public HdfsTierMetadataCapture(Configuration conf) throws IOException {
        this.conf = conf;

        // Establishing a thread-safe connection
        this.connection = ConnectionFactory.createConnection(conf);

        BufferedMutatorParams params = new BufferedMutatorParams(HdfsTierMetaTable.TABLE_NAME)
            .writeBufferSize(16 * 1024 * 1024) // 16MB buffer (~1000-2000 HFile metadata rows)
            .setWriteBufferPeriodicFlushTimeoutMs(5000) // Auto-flush every 5s
            .listener(new BufferedMutator.ExceptionListener() {
                @Override
                public void onException(RetriesExhaustedWithDetailsException e,
                                       BufferedMutator mutator) {

                    for (int i = 0; i < e.getNumExceptions(); i++) {
                        try {
                            Row failedRow = e.getRow(i);
                            String rowKeyStr = Bytes.toString(failedRow.getRow());
                            Throwable cause = e.getCause(i);

                            LOG.error("Write failed for {}, queuing retry", rowKeyStr);

                            if (failedRow instanceof Put) {
                                FailedWrite failedWrite = new FailedWrite(
                                    (Put) failedRow,
                                    rowKeyStr,
                                    cause
                                );

                                boolean queued = deadLetterQueue.offer(failedWrite);
                                if (!queued) {
                                    LOG.error("Retry queue full for {}", rowKeyStr);

                                }

                                totalFailures.incrementAndGet();
                            }

                        } catch (Exception queueError) {
                            LOG.error("Failed to queue retry: {}", queueError.getMessage());
                        }
                    }

                    LOG.warn("Failures: {}, Queue: {}, Retried: {}",
                        totalFailures.get(), deadLetterQueue.size(), retriedSuccesses.get());
                }
            });

        this.bufferedMutator = connection.getBufferedMutator(params);

        // Executor for async state updates
        // It prevents blocking the main flush/compaction thread
        this.asyncExecutor = new ThreadPoolExecutor(
            5,  // core threads
            20, // max threads
            60L, TimeUnit.SECONDS, // idle thread timeout
            new LinkedBlockingQueue<>(5000), // bounded queue prevents OOM
            new ThreadFactory() {
                private final AtomicInteger threadNum = new AtomicInteger(0);
                @Override
                public Thread newThread(Runnable r) {
                    Thread t = new Thread(r, "HdfsTierMetaCapture-" + threadNum.incrementAndGet());
                    t.setDaemon(true); // Daemon threads don't prevent JVM shutdown
                    return t;
                }
            },
            new ThreadPoolExecutor.CallerRunsPolicy() // Backpressure: caller executes if queue full
        );

        // Start background retry thread for dead-letter queue
        startDeadLetterQueueProcessor();

        LOG.info("HdfsTierMetadataCapture initialized");
    }

    /**
     * Starts background thread to process dead-letter queue.
     * Retries failed writes with exponential backoff.
     *
     * RETRY STRATEGY:
     * - Max 3 retries per failed write
     * - Exponential backoff: 10s, 30s, 60s
     * - After max retries, log for manual intervention
     *
     * THREAD-SAFETY: Single daemon thread, won't block shutdown
     */
    private void startDeadLetterQueueProcessor() {
        Thread retryThread = new Thread(() -> {           LOG.info("Retry processor started");

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    FailedWrite failedWrite = deadLetterQueue.poll(5, TimeUnit.SECONDS);

                    if (failedWrite == null) {
                        continue;
                    }

                    if (failedWrite.retryCount >= 3) {
                        LOG.error("Max retries for {}", failedWrite.rowKey);
                        continue;
                    }

                    // Exponential backoff: 10s, 30s, 60s
                    long backoffMs;
                    if (failedWrite.retryCount == 0) {
                        backoffMs = 10 * 1000; // 10 seconds
                    } else if (failedWrite.retryCount == 1) {
                        backoffMs = 30 * 1000; // 30 seconds
                    } else {
                        backoffMs = 60 * 1000; // 60 seconds
                    }

                    long timeSinceFailure = System.currentTimeMillis() - failedWrite.timestamp;
                    long remainingBackoffMs = backoffMs - timeSinceFailure;

                    if (timeSinceFailure < backoffMs) {
                        deadLetterQueue.offer(failedWrite);
                        Thread.sleep(Math.min(remainingBackoffMs, 5000));
                        continue;
                    }

                    LOG.info("Retry {}/3: {}", failedWrite.retryCount + 1, failedWrite.rowKey);

                    try {
                        try (Table table = connection.getTable(HdfsTierMetaTable.TABLE_NAME)) {
                            table.put(failedWrite.mutation);
                            retriedSuccesses.incrementAndGet();
                            LOG.info("Retry success: {}", failedWrite.rowKey);
                        }

                    } catch (IOException retryError) {
                        failedWrite.retryCount++;
                        LOG.warn("Retry {}/3 failed: {}", failedWrite.retryCount, failedWrite.rowKey);
                        deadLetterQueue.offer(failedWrite);
                    }

                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOG.info("Retry processor stopping");
                    break;
                } catch (Exception e) {
                    LOG.error("Retry processor error: {}", e.getMessage());
                }
            }

            LOG.info("Retry processor stopped, remaining: {}", deadLetterQueue.size());

        }, "DeadLetterQueue-Processor");

        retryThread.setDaemon(true); // Don't prevent JVM shutdown
        retryThread.start();
    }


    /**
     * Captures metadata when HFile is written.
     *
     * THREAD-SAFETY: Can be called concurrently from multiple flush/compaction threads.
     * BufferedMutator handles synchronization internally.
     *
     * PATH HANDLING:
     * - hfilePath: Original HFile path on disk (used for metadata extraction like size, column family)
     * - hdfsHfilePath: Path where HFile will be stored in HDFS hbase_cache directory
     * - It is stored in info:path column for later reference (eviction, pre-warming)
     *
     * @param storeFile StoreFile reference containing HFile metadata
     * @param region Region containing this file
     * @param hfilePath Original HFile path on disk (for metadata extraction)
     * @param hdfsHfilePath Path in HDFS hbase_cache directory
     */
    public void captureHFileMetadata(StoreFile storeFile, Region region, Path hfilePath, Path hdfsHfilePath)
            throws IOException {

        pendingWrites.incrementAndGet();

        try {
            // Extract metadata from StoreFile and Region
            // StoreFile reader might close soon after flush/compaction
            String hfileName = hfilePath.getName();
            String hdfsCacheHfilePath = hdfsHfilePath.toString();
            String regName = region.getRegionInfo().getRegionNameAsString();
            String encRegName = region.getRegionInfo().getEncodedName();
            String tableName = region.getTableDescriptor().getTableName().getNameAsString();

            // EXTRACT COLUMN FAMILY from file path structure
            // HFile path: .../namespace/table/region/columnfamily/hfilename
            // Column family is the parent directory name

            String colFamily = "";
            if (hfilePath.getParent() != null) {
                colFamily = hfilePath.getParent().getName();
            }

            // Get region ID through RegionInfo
            long regId = region.getRegionInfo().getRegionId();

            long createTime = System.currentTimeMillis();

            // Use original hfilePath for size extraction
            // StoreFile interface doesn't expose getReader(), but HStoreFile implementation does
            long size = 0;
            try {

              if (storeFile instanceof HStoreFile) {
                HStoreFile hstoreFile = (HStoreFile) storeFile;
                StoreFileReader reader = hstoreFile.getReader();
                if (reader != null) {
                  size = reader.length();
                }
              }

              // Fallback: Use FileSystem if reader unavailable
              // Query original hfilePath (source file) for size
              if (size == 0) {
                org.apache.hadoop.fs.FileSystem fs = hfilePath.getFileSystem(conf);
                FileStatus fileStatus = fs.getFileStatus(hfilePath);
                size = fileStatus.getLen();
                LOG.debug("Got size from FileSystem: {} bytes", size);
              }

            } catch (Exception e) {
              LOG.warn("Could not get size for {}: {}", hfileName, e.getMessage());
              size = 0; // Safe default
            }

            // ROW KEY: {regionEncodedName}#{hfileName}
            byte[] rowKey = HdfsTierMetaTable.createRowKey(encRegName, hfileName);

            Put put = new Put(rowKey);

            // Populate info CF
            put.addColumn(HdfsTierMetaTable.CF_INFO,
                         HdfsTierMetaTable.COL_HFILE_NAME,
                         Bytes.toBytes(hfileName));
            put.addColumn(HdfsTierMetaTable.CF_INFO,
                         HdfsTierMetaTable.COL_REG_NAME,
                         Bytes.toBytes(regName));
            put.addColumn(HdfsTierMetaTable.CF_INFO,
                         HdfsTierMetaTable.COL_ENC_REG_NAME,
                         Bytes.toBytes(encRegName));
            put.addColumn(HdfsTierMetaTable.CF_INFO,
                         HdfsTierMetaTable.COL_TABLE_NAME,
                         Bytes.toBytes(tableName));
            put.addColumn(HdfsTierMetaTable.CF_INFO,
                         HdfsTierMetaTable.COL_COLFAMILY,
                         Bytes.toBytes(colFamily));
            put.addColumn(HdfsTierMetaTable.CF_INFO,
                         HdfsTierMetaTable.COL_REG_ID,
                         Bytes.toBytes(regId));
            put.addColumn(HdfsTierMetaTable.CF_INFO,
                         HdfsTierMetaTable.COL_PATH,
                         Bytes.toBytes(hdfsCacheHfilePath));
            put.addColumn(HdfsTierMetaTable.CF_INFO,
                         HdfsTierMetaTable.COL_CREATE_TIME,
                         Bytes.toBytes(createTime));
            put.addColumn(HdfsTierMetaTable.CF_INFO,
                         HdfsTierMetaTable.COL_SIZE,
                         Bytes.toBytes(size));

            // Initialize transition CF with "ACTIVE" state
            put.addColumn(HdfsTierMetaTable.CF_TRANSITION,
                         HdfsTierMetaTable.COL_CURR_STATE,
                         Bytes.toBytes("ACTIVE"));
            put.addColumn(HdfsTierMetaTable.CF_TRANSITION,
                         HdfsTierMetaTable.COL_TRANS_TIME,
                         Bytes.toBytes(createTime));
            put.addColumn(HdfsTierMetaTable.CF_TRANSITION,
                         HdfsTierMetaTable.COL_EVICTED,
                         Bytes.toBytes(false));


            bufferedMutator.mutate(put);

            LOG.debug("Captured: region={}, file={}", encRegName, hfileName);

        } catch (Exception e) {
            LOG.error("Capture failed for {}: {}", hfilePath.getName(), e.getMessage());
        } finally {
            pendingWrites.decrementAndGet();
        }
    }

    /**
     * Updates state when HFile undergoes compaction (ACTIVE → COMPACTED).
     *
     * THREAD-SAFETY: Can be called concurrently for different files.
     * Uses async executor to prevent blocking caller's compaction thread.
     *
     * STATE TRANSITION: ACTIVE → COMPACTED
     * This marks the HFile as compacted, meaning it's been merged into a new compacted file.
     * The physical file still exists in cache but is logically obsolete.
     *
     * @param regionEncodedName Encoded region name to identify correct row
     * @param hfileName Name of the HFile being compacted
     * @param createTimestamp Creation timestamp
     */
    public void updateFileStateToCompacted(String regionEncodedName, String hfileName, long createTimestamp) {
        asyncExecutor.submit(() -> {
            try {
                updateFileStateSync(regionEncodedName, hfileName, "COMPACTED");
            } catch (Exception e) {
                LOG.error("Async task failed for {}#{}, adding to retry queue", hfileName, regionEncodedName);

                // forms failedWrite and offers to dead-letter queue
                try {
                    byte[] rowKey = HdfsTierMetaTable.createRowKey(regionEncodedName, hfileName);
                    String rowKeyStr = Bytes.toString(rowKey);

                    // Reconstruct the Put that failed
                    Put put = new Put(rowKey);
                    long transTime = System.currentTimeMillis();
                    put.addColumn(HdfsTierMetaTable.CF_TRANSITION,
                                 HdfsTierMetaTable.COL_CURR_STATE,
                                 Bytes.toBytes("COMPACTED"));
                    put.addColumn(HdfsTierMetaTable.CF_TRANSITION,
                                 HdfsTierMetaTable.COL_TRANS_TIME,
                                 Bytes.toBytes(transTime));

                    FailedWrite failedWrite = new FailedWrite(put, rowKeyStr, e);

                    if (!deadLetterQueue.offer(failedWrite)) {
                        LOG.error("Dead-letter queue full, cannot retry {}", hfileName);
                    } else {
                        totalFailures.incrementAndGet();
                    }
                } catch (Exception retryQueueError) {
                    LOG.error("Failed to queue retry for {}: {}", hfileName, retryQueueError.getMessage());
                }
            }
        });
    }


    /**
     * Updates state when HFile is evicted from cache (ACTIVE → EVICTED).
     *
     * THREAD-SAFETY: Can be called concurrently from eviction service.
     * Uses BufferedMutator for batched async writes.
     *
     * STATE TRANSITION: ACTIVE → EVICTED.
     * This marks the HFile as evicted from hbase_cache directory.
     * The row remains in metadata table for audit/history purposes.
     *
     * @param regionEncodedName Encoded region name to identify correct row
     * @param hfileName Name of the evicted HFile
     * @param createTimestamp Creation timestamp
     * @throws IOException If update fails (caller should handle)
     */
    public void updateFileStateToEvicted(String regionEncodedName, String hfileName, long createTimestamp) throws IOException {
        // Eviction service can retry on failure

        byte[] rowKey = HdfsTierMetaTable.createRowKey(regionEncodedName, hfileName);

        try {
            Put put = new Put(rowKey);
            long evictTime = System.currentTimeMillis();

            // Update state to EVICTED
            put.addColumn(HdfsTierMetaTable.CF_TRANSITION,
                         HdfsTierMetaTable.COL_CURR_STATE,
                         Bytes.toBytes("EVICTED"));
            put.addColumn(HdfsTierMetaTable.CF_TRANSITION,
                         HdfsTierMetaTable.COL_TRANS_TIME,
                         Bytes.toBytes(evictTime));

            // Mark evicted flag
            put.addColumn(HdfsTierMetaTable.CF_TRANSITION,
                         HdfsTierMetaTable.COL_EVICTED,
                         Bytes.toBytes(true));
            put.addColumn(HdfsTierMetaTable.CF_TRANSITION,
                         HdfsTierMetaTable.COL_EVICT_TIME,
                         Bytes.toBytes(evictTime));

            // Use BufferedMutator for batching efficiency
            bufferedMutator.mutate(put);

            LOG.info("Marked EVICTED: {}#{}", hfileName, createTimestamp);

        } catch (Exception e) {
            LOG.error("Eviction mark failed for {}#{}: {}", hfileName, createTimestamp, e.getMessage());
            throw new IOException("Failed to update eviction state", e);
        }
    }

    /**
     * Synchronous state update using exact row key.
     *
     * CONCURRENCY:
     * Uses direct Put with exact row key.
     * Multiple threads updating different rows won't conflict.
     * Same row updated by multiple threads: last write wins.
     *
     * FAILURE HANDLING:
     * - Retries on transient failures (handled by HBase client)
     * - Logs permanent failures but doesn't crash caller
     *
     * @param regionEncodedName Encoded region name for row key
     * @param hfileName HFile name
     * @param newState New state value (COMPACTED, EVICTED, etc.)
     */
    private void updateFileStateSync(String regionEncodedName, String hfileName, String newState)
            throws IOException {

        // Build row key: regionEncodedName#hfileName
        byte[] rowKey = HdfsTierMetaTable.createRowKey(regionEncodedName, hfileName);

        try {
            Put put = new Put(rowKey);
            long transTime = System.currentTimeMillis();

            // Update state columns
            put.addColumn(HdfsTierMetaTable.CF_TRANSITION,
                         HdfsTierMetaTable.COL_CURR_STATE,
                         Bytes.toBytes(newState));
            put.addColumn(HdfsTierMetaTable.CF_TRANSITION,
                         HdfsTierMetaTable.COL_TRANS_TIME,
                         Bytes.toBytes(transTime));

            // Use BufferedMutator for async batching
            bufferedMutator.mutate(put);

            LOG.debug("Updated state: {}#{} -> {}", regionEncodedName, hfileName,newState);

        } catch (Exception e) {
            LOG.error("State update failed for {}#{}: {}", hfileName, regionEncodedName, e.getMessage());
            throw new IOException("State update failed", e);
        }
    }

    /**
     * Flushes pending writes and closes resources.
     *
     * GRACEFUL SHUTDOWN:
     * - Flushes BufferedMutator to ensure all writes complete
     * - Shuts down async executor with timeout
     * - Closes connection
     *
     * DEADLOCK PREVENTION:
     * - Uses timeout on executor shutdown
     * - If timeout expires, forces shutdown (no indefinite waiting)
     *
     * @throws IOException If flush or close fails
     */
    public void close() throws IOException {
        LOG.info("Shutting down, pending writes: {}", pendingWrites.get());

        try {
            // Step 1: Stop accepting new async tasks
            asyncExecutor.shutdown();

            // Step 2: Wait for in-flight async tasks with timeout
            boolean terminated = asyncExecutor.awaitTermination(
                FLUSH_TIMEOUT_SECONDS, TimeUnit.SECONDS);

            if (!terminated) {
                LOG.warn("Async executor timeout, forcing shutdown");
                asyncExecutor.shutdownNow();
            }

            // Step 3: Flush BufferedMutator to ensure all writes reach HBase
            if (bufferedMutator != null) {
                bufferedMutator.flush();
                bufferedMutator.close();
                LOG.info("BufferedMutator flushed");
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted during shutdown");
        } finally {
            // Step 4: Close connection
            if (connection != null) {
                connection.close();
                LOG.info("Connection closed");
            }

            LOG.info("Shutdown complete. Failures: {}, Retried: {}, Queue: {}",
                totalFailures.get(), retriedSuccesses.get(), deadLetterQueue.size());
        }
    }
}
