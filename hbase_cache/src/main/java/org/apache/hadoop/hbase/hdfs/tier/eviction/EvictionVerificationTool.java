package org.apache.hadoop.hbase.hdfs.tier.eviction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.hdfs.tier.HdfsTierMetaTable;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Manual verification tool for LRU eviction policy.
 *
 * USAGE:
 *   java -cp <classpath> org.apache.hadoop.hbase.hdfs.tier.eviction.EvictionVerificationTool <bytes-to-free>
 *
 * WHAT IT DOES:
 * 1. Lists all ACTIVE HFiles from hdfsTier:meta
 * 2. Shows their lastAccessTime and size
 * 3. Runs LRU policy to select files for eviction
 * 4. Displays selected files in order
 * 5. Verifies that files are sorted by lastAccess (oldest first)
 * 6. Optionally triggers actual eviction (dry-run by default)
 */
public class EvictionVerificationTool {
  private static final Logger LOG = LoggerFactory.getLogger(EvictionVerificationTool.class);
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: EvictionVerificationTool <bytes-to-free> [--execute]");
      System.err.println("  bytes-to-free: Target bytes to evict (e.g., 1000000 for 1MB)");
      System.err.println("  --execute: Actually mark files as evicted (default: dry-run)");
      System.exit(1);
    }

    long targetBytes = Long.parseLong(args[0]);
    boolean execute = args.length > 1 && "--execute".equals(args[1]);

    Configuration conf = HBaseConfiguration.create();

    System.out.println("╔══════════════════════════════════════════════════════════════════════╗");
    System.out.println("║          LRU EVICTION POLICY VERIFICATION TOOL                       ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════╝");
    System.out.println();
    System.out.println("Target bytes to evict: " + formatBytes(targetBytes));
    System.out.println("Mode: " + (execute ? "EXECUTION" : "DRY-RUN"));
    System.out.println();

    // Step 1: List all ACTIVE files
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("STEP 1: Scanning hdfsTier:meta for ACTIVE HFiles...");
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    listAllActiveFiles(conf);

    // Step 2: Run LRU policy
    System.out.println("\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("STEP 2: Running LRU Eviction Policy...");
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    LRUEvictionPolicy policy = new LRUEvictionPolicy(conf);
    List<HFileEvictionCandidate> selected = policy.selectFilesForEviction(targetBytes);

    if (selected.isEmpty()) {
      System.out.println("✗ No files selected for eviction!");
      System.exit(0);
    }

    System.out.println("✓ Selected " + selected.size() + " files for eviction");
    System.out.println();

    // Step 3: Display selected files
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("STEP 3: Eviction Candidates (Sorted by LRU)");
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.printf("%-4s %-45s %-20s %-12s %-10s%n",
                      "#", "HFile Name", "Last Access", "Size", "Age (hrs)");
    System.out.println("─────────────────────────────────────────────────────────────────────────────");

    long totalSize = 0;
    long now = System.currentTimeMillis();

    for (int i = 0; i < selected.size(); i++) {
      HFileEvictionCandidate file = selected.get(i);
      totalSize += file.getSize();

      String lastAccessStr = file.getLastAccessTime() > 0
          ? DATE_FORMAT.format(new Date(file.getLastAccessTime()))
          : "Never";

      long ageHours = file.getLastAccessTime() > 0
          ? (now - file.getLastAccessTime()) / (1000 * 60 * 60)
          : -1;

      String ageStr = ageHours >= 0 ? String.valueOf(ageHours) : "N/A";

      System.out.printf("%-4d %-45s %-20s %-12s %-10s%n",
                        i + 1,
                        truncate(file.getHfileName(), 45),
                        lastAccessStr,
                        formatBytes(file.getSize()),
                        ageStr);
    }

    System.out.println("─────────────────────────────────────────────────────────────────────────────");
    System.out.println("Total selected: " + formatBytes(totalSize) + " from " + selected.size() + " files");
    System.out.println();

    // Step 4: Verify ordering
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("STEP 4: Verifying LRU Ordering...");
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

    boolean isCorrectOrder = verifyLRUOrdering(selected);

    if (isCorrectOrder) {
      System.out.println("✓ PASSED: Files are correctly sorted by lastAccessTime (oldest first)");
    } else {
      System.out.println("✗ FAILED: Files are NOT in correct LRU order!");
      System.exit(1);
    }
    System.out.println();

    // Step 5: Check if target met
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("STEP 5: Target Verification");
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("Target bytes to evict: " + formatBytes(targetBytes));
    System.out.println("Actual bytes selected: " + formatBytes(totalSize));

    if (totalSize >= targetBytes) {
      System.out.println("✓ PASSED: Target met");
    } else {
      System.out.println("✗ WARNING: Target not fully met (may be insufficient files)");
    }
    System.out.println();

    // Step 6: Execute eviction if requested
    if (execute) {
      System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      System.out.println("STEP 6: Executing Eviction...");
      System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");

      org.apache.hadoop.hbase.hdfs.tier.HdfsTierMetadataCapture metadataCapture = null;
      try {
        metadataCapture = new org.apache.hadoop.hbase.hdfs.tier.HdfsTierMetadataCapture(conf);

        HDFSTierEvictionExecutor executor = new HDFSTierEvictionExecutor(conf, metadataCapture);
        long bytesEvicted = executor.executeEviction(selected);

        System.out.println("✓ Marked " + selected.size() + " files as EVICTED in hdfsTier:meta");
        System.out.println("✓ Total bytes marked for eviction: " + formatBytes(bytesEvicted));
        System.out.println();

        // Verify eviction marks
        System.out.println("Verifying eviction marks...");
        verifyEvictionMarks(conf, selected);
      } finally {
        if (metadataCapture != null) {
          metadataCapture.close();
        }
      }
    } else {
      System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      System.out.println("DRY-RUN MODE: No files were actually evicted");
      System.out.println("Run with --execute to perform actual eviction");
      System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    }

    System.out.println("\n✓ Verification complete!");
    policy.close();
  }

  private static void listAllActiveFiles(Configuration conf) throws IOException {
    try (Connection conn = ConnectionFactory.createConnection(conf);
         Table table = conn.getTable(HdfsTierMetaTable.TABLE_NAME)) {

      Scan scan = new Scan();
      scan.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_HFILE_NAME);
      scan.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_SIZE);
      scan.addColumn(HdfsTierMetaTable.CF_INFO, HdfsTierMetaTable.COL_LAST_ACCESS);
      scan.addColumn(HdfsTierMetaTable.CF_TRANSITION, HdfsTierMetaTable.COL_CURR_STATE);
      scan.addColumn(HdfsTierMetaTable.CF_TRANSITION, HdfsTierMetaTable.COL_EVICTED);
      scan.setCaching(1000);

      int activeCount = 0;
      int evictedCount = 0;
      int compactedCount = 0;
      long totalSize = 0;

      try (ResultScanner scanner = table.getScanner(scan)) {
        for (Result result : scanner) {
          if (result.isEmpty()) continue;

          byte[] stateBytes = result.getValue(HdfsTierMetaTable.CF_TRANSITION,
                                              HdfsTierMetaTable.COL_CURR_STATE);
          String state = stateBytes != null ? Bytes.toString(stateBytes) : "UNKNOWN";

          byte[] evictedBytes = result.getValue(HdfsTierMetaTable.CF_TRANSITION,
                                                 HdfsTierMetaTable.COL_EVICTED);
          boolean isEvicted = evictedBytes != null && Bytes.toBoolean(evictedBytes);

          byte[] sizeBytes = result.getValue(HdfsTierMetaTable.CF_INFO,
                                             HdfsTierMetaTable.COL_SIZE);
          long size = sizeBytes != null ? Bytes.toLong(sizeBytes) : 0;

          if ("ACTIVE".equals(state) && !isEvicted) {
            activeCount++;
            totalSize += size;
          } else if (isEvicted) {
            evictedCount++;
          } else if ("COMPACTED".equals(state)) {
            compactedCount++;
          }
        }
      }

      System.out.println("Total ACTIVE files: " + activeCount + " (" + formatBytes(totalSize) + ")");
      System.out.println("Total EVICTED files: " + evictedCount);
      System.out.println("Total COMPACTED files: " + compactedCount);
    }
  }

  private static boolean verifyLRUOrdering(List<HFileEvictionCandidate> files) {
    for (int i = 1; i < files.size(); i++) {
      long prevAccess = files.get(i - 1).getLastAccessTime();
      long currAccess = files.get(i).getLastAccessTime();

      if (currAccess < prevAccess) {
        System.out.println("✗ Ordering violation at position " + i);
        System.out.println("  File " + (i - 1) + ": " + files.get(i - 1).getHfileName()
                          + " lastAccess=" + prevAccess);
        System.out.println("  File " + i + ": " + files.get(i).getHfileName()
                          + " lastAccess=" + currAccess);
        return false;
      }
    }
    return true;
  }

  private static void verifyEvictionMarks(Configuration conf,
                                           List<HFileEvictionCandidate> expected)
      throws IOException {

    try (Connection conn = ConnectionFactory.createConnection(conf);
         Table table = conn.getTable(HdfsTierMetaTable.TABLE_NAME)) {

      int verified = 0;
      int notFound = 0;

      for (HFileEvictionCandidate file : expected) {
        String rowKey = file.getRegionEncodedName() + "#" + file.getHfileName();
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(HdfsTierMetaTable.CF_TRANSITION, HdfsTierMetaTable.COL_EVICTED);

        Result result = table.get(get);
        if (!result.isEmpty()) {
          byte[] evictedBytes = result.getValue(HdfsTierMetaTable.CF_TRANSITION,
                                                 HdfsTierMetaTable.COL_EVICTED);
          boolean isEvicted = evictedBytes != null && Bytes.toBoolean(evictedBytes);

          if (isEvicted) {
            verified++;
          } else {
            System.out.println("✗ File not marked as evicted: " + file.getHfileName());
          }
        } else {
          notFound++;
          System.out.println("✗ File not found in meta table: " + file.getHfileName());
        }
      }

      System.out.println("✓ Verified " + verified + "/" + expected.size() + " files marked as EVICTED");
      if (notFound > 0) {
        System.out.println("✗ " + notFound + " files not found in meta table");
      }
    }
  }

  private static String formatBytes(long bytes) {
    if (bytes < 1024) return bytes + " B";
    if (bytes < 1024 * 1024) return String.format("%.2f KB", bytes / 1024.0);
    if (bytes < 1024 * 1024 * 1024) return String.format("%.2f MB", bytes / (1024.0 * 1024));
    return String.format("%.2f GB", bytes / (1024.0 * 1024 * 1024));
  }

  private static String truncate(String str, int maxLen) {
    if (str == null) return "";
    return str.length() <= maxLen ? str : str.substring(0, maxLen - 3) + "...";
  }
}
