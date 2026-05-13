package org.apache.hadoop.hbase.hdfs.tier.eviction;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

/**
 * Manual trigger tool for LRU eviction policy testing.
 *
 * This tool allows you to manually invoke the LRU policy to see which HFiles
 * it would select for eviction without actually executing the eviction.
 *
 * USAGE:
 *   ./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger <bytes-to-free>
 *
 * EXAMPLE:
 *   ./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 10485760
 *   (This will show which files would be evicted to free 10MB)
 */
public class ManualEvictionTrigger {
  private static final Logger LOG = LoggerFactory.getLogger(ManualEvictionTrigger.class);
  private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: ManualEvictionTrigger <bytes-to-free>");
      System.err.println("  bytes-to-free: Target bytes to evict (e.g., 10485760 for 10MB)");
      System.err.println();
      System.err.println("Example:");
      System.err.println("  ./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 10485760");
      System.exit(1);
    }

    long targetBytes = Long.parseLong(args[0]);
    Configuration conf = HBaseConfiguration.create();

    System.out.println();
    System.out.println("╔══════════════════════════════════════════════════════════════════════╗");
    System.out.println("║          MANUAL LRU EVICTION POLICY TRIGGER                          ║");
    System.out.println("╚══════════════════════════════════════════════════════════════════════╝");
    System.out.println();
    System.out.println("Target bytes to evict: " + formatBytes(targetBytes));
    System.out.println("Eviction policy: " + conf.get("hbase.hdfstier.eviction.policy", "LRU"));
    System.out.println();

    // Step 1: Initialize LRU policy
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("Initializing LRU Eviction Policy...");
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println();

    LRUEvictionPolicy policy = new LRUEvictionPolicy(conf);

    // Step 2: Select candidates
    System.out.println("Scanning hdfsTier:meta table for ACTIVE HFiles...");
    long startTime = System.currentTimeMillis();

    List<HFileEvictionCandidate> candidates = policy.selectFilesForEviction(targetBytes);

    long elapsed = System.currentTimeMillis() - startTime;
    System.out.println("Scan completed in " + elapsed + " ms");
    System.out.println();

    if (candidates.isEmpty()) {
      System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      System.out.println("No HFiles selected for eviction");
      System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
      System.out.println();
      System.out.println("Possible reasons:");
      System.out.println("  - No ACTIVE HFiles in hdfsTier:meta table");
      System.out.println("  - All files are already marked as EVICTED");
      System.out.println("  - Total size of ACTIVE files is less than target");
      System.out.println();
      policy.close();
      System.exit(0);
    }

    // Step 3: Display selected candidates
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("LRU EVICTION CANDIDATES (Sorted by Last Access Time)");
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println();
    System.out.printf("%-4s %-50s %-22s %-12s %-10s%n",
                      "#", "HFile Name", "Last Access", "Size", "Age (hrs)");
    System.out.println("─".repeat(105));

    long totalSize = 0;
    long now = System.currentTimeMillis();

    for (int i = 0; i < candidates.size(); i++) {
      HFileEvictionCandidate file = candidates.get(i);
      totalSize += file.getSize();

      String lastAccessStr = file.getLastAccessTime() > 0
          ? DATE_FORMAT.format(new Date(file.getLastAccessTime()))
          : "Never";

      long ageHours = file.getLastAccessTime() > 0
          ? (now - file.getLastAccessTime()) / (1000 * 60 * 60)
          : -1;

      String ageStr = ageHours >= 0 ? String.valueOf(ageHours) : "N/A";

      System.out.printf("%-4d %-50s %-22s %-12s %-10s%n",
                        i + 1,
                        truncate(file.getHfileName(), 50),
                        lastAccessStr,
                        formatBytes(file.getSize()),
                        ageStr);
    }

    System.out.println("─".repeat(105));
    System.out.println();
    System.out.println("Total files selected: " + candidates.size());
    System.out.println("Total bytes selected: " + formatBytes(totalSize));
    System.out.println("Target bytes:         " + formatBytes(targetBytes));
    System.out.println();

    // Step 4: Verify ordering
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("Verifying LRU Ordering...");
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println();

    boolean isCorrectOrder = verifyLRUOrdering(candidates);

    if (isCorrectOrder) {
      System.out.println("✓ PASSED: Files are correctly sorted by lastAccessTime (oldest first)");
    } else {
      System.out.println("✗ FAILED: Files are NOT in correct LRU order!");
    }
    System.out.println();

    // Step 5: Show target verification
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("Target Verification");
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println();

    if (totalSize >= targetBytes) {
      System.out.println("✓ Target met: Selected " + formatBytes(totalSize) + " >= " + formatBytes(targetBytes));
    } else {
      System.out.println("⚠ Target not fully met: Selected " + formatBytes(totalSize) + " < " + formatBytes(targetBytes));
      System.out.println("  (May indicate insufficient ACTIVE files)");
    }
    System.out.println();

    // Step 6: Show what would happen
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("What Happens Next?");
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println();
    System.out.println("This was a DRY-RUN. No files were actually evicted.");
    System.out.println();
    System.out.println("To execute actual eviction, use:");
    System.out.println("  ./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.EvictionVerificationTool "
                       + targetBytes + " --execute");
    System.out.println();
    System.out.println("Or wait for automatic eviction to trigger when:");
    System.out.println("  - Storage usage exceeds threshold ("
                       + conf.getDouble("hbase.hdfstier.eviction.threshold", 90.0) + "%)");
    System.out.println("  - Scheduled check runs (every "
                       + conf.getLong("hbase.hdfstier.eviction.check.interval.sec", 300) + " seconds)");
    System.out.println();

    // Step 7: Show row keys for manual verification
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println("Manual Verification Commands");
    System.out.println("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━");
    System.out.println();
    System.out.println("To verify these files in HBase shell:");
    System.out.println();

    if (candidates.size() > 0) {
      System.out.println("# Check first candidate:");
      HFileEvictionCandidate first = candidates.get(0);
      System.out.println("get 'hdfsTier:meta', '" + first.getRowKey() + "'");
      System.out.println();

      if (candidates.size() > 1) {
        System.out.println("# Check last candidate:");
        HFileEvictionCandidate last = candidates.get(candidates.size() - 1);
        System.out.println("get 'hdfsTier:meta', '" + last.getRowKey() + "'");
        System.out.println();
      }
    }

    System.out.println("# Scan all candidates:");
    System.out.println("./bin/hbase shell");
    for (int i = 0; i < Math.min(3, candidates.size()); i++) {
      System.out.println("hbase> get 'hdfsTier:meta', '" + candidates.get(i).getRowKey() + "'");
    }
    if (candidates.size() > 3) {
      System.out.println("... and " + (candidates.size() - 3) + " more");
    }
    System.out.println();

    policy.close();

    System.out.println("✓ Manual trigger complete!");
    System.out.println();
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
