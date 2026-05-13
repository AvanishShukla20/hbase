# How to Manually Trigger LRU Eviction Policy for Testing

## Quick Command

```bash
cd <HBASE_HOME>

# Build if needed
mvn clean compile -pl hbase_cache -am

# Run manual trigger (10MB example)
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 10485760
```

## What This Does

1. **Scans** hdfsTier:meta table for ACTIVE HFiles
2. **Runs** your configured LRU eviction policy
3. **Shows** the complete list of HFiles that would be evicted
4. **Displays** details for each candidate:
   - HFile name
   - Last access timestamp
   - File size
   - Age (hours since last access)
5. **Verifies** the LRU ordering is correct (oldest first)
6. **Does NOT** actually evict any files (dry-run only)

## Example Output

```
╔══════════════════════════════════════════════════════════════════════╗
║          MANUAL LRU EVICTION POLICY TRIGGER                          ║
╚══════════════════════════════════════════════════════════════════════╝

Target bytes to evict: 10.00 MB
Eviction policy: LRU

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LRU EVICTION CANDIDATES (Sorted by Last Access Time)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

#    HFile Name                                         Last Access            Size         Age (hrs)
─────────────────────────────────────────────────────────────────────────────────────────────────────
1    abc123def456789.hfile                              2026-05-06 10:15:23   1.50 MB      25
2    xyz789abc012345.hfile                              2026-05-06 11:30:45   2.30 MB      24
3    def456ghi789012.hfile                              2026-05-06 12:45:10   1.80 MB      22
4    ghi012jkl345678.hfile                              2026-05-06 14:20:33   3.20 MB      21
5    jkl345mno678901.hfile                              2026-05-06 15:55:12   1.70 MB      19
─────────────────────────────────────────────────────────────────────────────────────────────────────

Total files selected: 5
Total bytes selected: 10.50 MB
Target bytes:         10.00 MB

✓ PASSED: Files are correctly sorted by lastAccessTime (oldest first)
✓ Target met: Selected 10.50 MB >= 10.00 MB
```

## Different Target Sizes

```bash
# Test with 10MB
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 10485760

# Test with 100MB
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 104857600

# Test with 1GB
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 1073741824

# Test with custom size (calculate: bytes = MB * 1024 * 1024)
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger <bytes>
```

## Verify Results in HBase Shell

After seeing the list, verify specific HFiles:

```bash
./bin/hbase shell

# Check a specific file from the output
hbase> get 'hdfsTier:meta', '<regionName>#<hfileName>'

# Example from output above:
hbase> get 'hdfsTier:meta', 'a1b2c3d4e5f6#abc123def456789.hfile'

# Look for these columns:
# - info:lastAccessTimestamp
# - info:accessCount
# - transition:currentState (should be ACTIVE)
# - transition:evicted (should be false)
```

## Troubleshooting

### No HFiles Found

If you see "No HFiles selected for eviction", you need test data:

```bash
./bin/hbase shell

hbase> create 'test_table', 'cf'
hbase> put 'test_table', 'row1', 'cf:col1', 'value1'
hbase> put 'test_table', 'row2', 'cf:col2', 'value2'
hbase> flush 'test_table'
hbase> scan 'test_table'  # Triggers access tracking

# Wait 30 seconds, then retry the manual trigger
```

### Compile Errors

```bash
cd <HBASE_HOME>
mvn clean compile -pl hbase_cache -am
```

### Class Not Found

Make sure you're in the HBase home directory:

```bash
cd <HBASE_HOME>
pwd  # Should show your HBase installation directory
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 10485760
```

## What's Next?

After verifying the candidate list:

1. **To actually evict files** (marks them in metadata, doesn't delete physically):
   ```bash
   ./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.EvictionVerificationTool 10485760 --execute
   ```

2. **Wait for automatic eviction** (runs every 5 minutes by default when storage > threshold)

3. **Check automatic eviction logs**:
   ```bash
   grep -i "LRU" logs/hbase-*-regionserver-*.log | tail -20
   ```

## Full Documentation

For complete step-by-step verification guide, see:
- [EVICTION_VERIFICATION_GUIDE.md](./EVICTION_VERIFICATION_GUIDE.md)

For configuration details, see:
- [HDFS_TIER_CONFIGURATION_GUIDE.md](./HDFS_TIER_CONFIGURATION_GUIDE.md)
- [EVICTION_CONFIGURATION.md](./EVICTION_CONFIGURATION.md)
