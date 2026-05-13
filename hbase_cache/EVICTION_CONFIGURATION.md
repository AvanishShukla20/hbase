# HDFS Tier Eviction Configuration Guide

## Overview

The HDFS Tier Eviction system automatically manages storage by removing least-recently-used (LRU) HFiles when storage limits are exceeded.

---

## Configuration Properties

Add these properties to `hbase-site.xml`:

### 1. Enable/Disable Eviction

```xml
<property>
  <name>hbase.hdfstier.eviction.enabled</name>
  <value>true</value>
  <description>
    Enable automatic eviction of HFiles from HDFS cache tier.
    Default: true
  </description>
</property>
```

### 2. Eviction Policy

```xml
<property>
  <name>hbase.hdfstier.eviction.policy</name>
  <value>LRU</value>
  <description>
    Eviction policy to use. Options:
    - LRU: Least Recently Used (evicts oldest accessed files first)
    - FIFO: First In First Out (not yet implemented)
    - Custom: Fully qualified class name implementing EvictionPolicy interface
    Default: LRU
  </description>
</property>
```

### 3. Storage Limits

```xml
<property>
  <name>hbase.hdfstier.storage.max.bytes</name>
  <value>107374182400</value>
  <description>
    Maximum storage capacity for HDFS cache tier in bytes.
    Example: 107374182400 = 100 GB
    Default: 100 GB (107374182400 bytes)
  </description>
</property>
```

### 4. Eviction Threshold

```xml
<property>
  <name>hbase.hdfstier.eviction.threshold</name>
  <value>90.0</value>
  <description>
    Storage usage percentage that triggers eviction.
    When usage exceeds this threshold, eviction starts.
    Value: 0.0 to 100.0
    Default: 90.0 (90%)
  </description>
</property>
```

### 5. Eviction Target

```xml
<property>
  <name>hbase.hdfstier.eviction.target</name>
  <value>70.0</value>
  <description>
    Target storage usage percentage after eviction.
    Eviction continues until usage drops to this level.
    Value: 0.0 to 100.0 (must be less than threshold)
    Default: 70.0 (70%)
  </description>
</property>
```

### 6. Check Interval

```xml
<property>
  <name>hbase.hdfstier.eviction.chore.period.sec</name>
  <value>300</value>
  <description>
    Interval in seconds between storage usage checks.
    Lower values = more frequent checks, faster response
    Higher values = less overhead
    Default: 300 (5 minutes)
  </description>
</property>

<property>
  <name>hbase.hdfstier.eviction.chore.delay.sec</name>
  <value>60</value>
  <description>
    Initial delay in seconds before first eviction check.
    Allows system to stabilize after startup.
    Default: 60 (1 minute)
  </description>
</property>
```

### 7. HDFS Base Path

```xml
<property>
  <name>hbase.hdfstier.storage.path</name>
  <value>hdfs://hbase_cache</value>
  <description>
    Base path in HDFS where cached HFiles are stored.
    Must match the path used by metadata capture.
    Default: hdfs://hbase_cache
  </description>
</property>
```

---

## Example Configurations

### Production (Conservative)

```xml
<!-- Enable eviction -->
<property>
  <name>hbase.hdfstier.eviction.enabled</name>
  <value>true</value>
</property>

<!-- Use LRU policy -->
<property>
  <name>hbase.hdfstier.eviction.policy</name>
  <value>LRU</value>
</property>

<!-- 1 TB max storage -->
<property>
  <name>hbase.hdfstier.storage.max.bytes</name>
  <value>1099511627776</value> <!-- 1 TB -->
</property>

<!-- Trigger at 90% usage -->
<property>
  <name>hbase.hdfstier.eviction.threshold</name>
  <value>90.0</value>
</property>

<!-- Evict down to 75% -->
<property>
  <name>hbase.hdfstier.eviction.target</name>
  <value>75.0</value>
</property>

<!-- Check every 10 minutes -->
<property>
  <name>hbase.hdfstier.eviction.chore.period.sec</name>
  <value>600</value>
</property>

<!-- Initial delay 2 minutes -->
<property>
  <name>hbase.hdfstier.eviction.chore.delay.sec</name>
  <value>120</value>
</property>
```

### Development/Testing (Aggressive)

```xml
<!-- Enable eviction -->
<property>
  <name>hbase.hdfstier.eviction.enabled</name>
  <value>true</value>
</property>

<!-- Use LRU policy -->
<property>
  <name>hbase.hdfstier.eviction.policy</name>
  <value>LRU</value>
</property>

<!-- 10 GB max storage (small for testing) -->
<property>
  <name>hbase.hdfstier.storage.max.bytes</name>
  <value>10737418240</value> <!-- 10 GB -->
</property>

<!-- Trigger at 80% usage -->
<property>
  <name>hbase.hdfstier.eviction.threshold</name>
  <value>80.0</value>
</property>

<!-- Evict down to 50% -->
<property>
  <name>hbase.hdfstier.eviction.target</name>
  <value>50.0</value>
</property>

<!-- Check every 1 minute -->
<property>
  <name>hbase.hdfstier.eviction.chore.period.sec</name>
  <value>60</value>
</property>

<!-- Initial delay 10 seconds -->
<property>
  <name>hbase.hdfstier.eviction.chore.delay.sec</name>
  <value>10</value>
</property>
```

---

## How It Works

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│ HDFSTierEvictionCoordinator (Orchestrator)              │
│ - Monitors storage usage every N seconds                │
│ - Triggers eviction when threshold exceeded             │
│ - Ensures storage returns to target level               │
└─────────────────┬───────────────────────────────────────┘
                  │
                  ├──> EvictionPolicy (Strategy Pattern)
                  │    └─> LRUEvictionPolicy
                  │        - Queries hdfsTier:meta table
                  │        - Selects ACTIVE files with oldest lastAccess
                  │        - Returns list sorted by priority
                  │
                  └──> HDFSTierEvictionExecutor
                       - Deletes physical files from HDFS
                       - Updates metadata (marks as EVICTED)
                       - Handles failures gracefully
```

### Eviction Flow

1. **Periodic Check** (every `check.interval.sec`)
   ```
   Current Usage: 950 GB
   Max Storage: 1000 GB
   Usage: 95% → EXCEEDS 90% threshold
   ```

2. **Calculate Target**
   ```
   Target Usage: 70% of 1000 GB = 700 GB
   Bytes to Evict: 950 GB - 700 GB = 250 GB
   ```

3. **Select Files** (using LRU policy)
   ```
   Query hdfsTier:meta for ACTIVE files
   Sort by info:lastAccess (ascending)
   Select oldest files totaling ~250 GB
   ```

4. **Execute Eviction**
   ```
   For each file:
     1. Delete from HDFS (hdfs://hbase_cache/file.hfile)
     2. Update metadata (transition:currState = EVICTED)
     3. Update metadata (transition:evicted = true)
   ```

5. **Verify**
   ```
   New Usage: 700 GB (70%)
   Status: SUCCESS - below threshold
   ```

---

## LRU Policy Details

### Selection Criteria

The LRU policy selects files based on:

1. **State Filter**: Only `ACTIVE` files (not `COMPACTED` or already `EVICTED`)
2. **Access Time**: Uses `info:lastAccess` timestamp
3. **Fallback**: If `lastAccess` is null, uses `info:createTime`
4. **Sort Order**: Oldest access first (ascending timestamp)

### Example Query

```sql
-- Pseudo-SQL representation
SELECT region, hfileName, size, lastAccess, state
FROM hdfsTier:meta
WHERE state = 'ACTIVE' 
  AND evicted = false
ORDER BY lastAccess ASC
LIMIT until (SUM(size) >= targetBytesToEvict)
```

### Metadata Columns Used

- `info:hfileName` - File name
- `info:encRegName` - Region name
- `info:size` - File size in bytes
- `info:lastAccess` - Last read timestamp
- `info:createTime` - Creation timestamp (fallback)
- `transition:currState` - Current state (ACTIVE/COMPACTED/EVICTED)
- `transition:evicted` - Boolean flag

---

## Monitoring

### Log Messages

**Eviction Triggered**:
```
WARN  HDFSTierEvictionCoordinator: Storage usage 95.00% exceeds threshold 90.0% - triggering eviction
INFO  HDFSTierEvictionCoordinator: Starting eviction: need to free 250000000000 bytes to reach target 70% (700000000000 bytes)
```

**File Selection**:
```
INFO  LRUEvictionPolicy: Found 1500 eviction candidates (ACTIVE, non-evicted)
INFO  LRUEvictionPolicy: Selected 125 files for eviction (250000000000 bytes) using LRU policy
```

**Execution**:
```
INFO  HDFSTierEvictionExecutor: Starting eviction of 125 files
INFO  HDFSTierEvictionExecutor: Evicting HFile: file1.hfile from region region123 (size: 2000000000 bytes)
INFO  HDFSTierEvictionExecutor: Deleted HDFS file: hdfs://hbase_cache/file1.hfile
INFO  HDFSTierMetadataCapture: Marked file1.hfile as EVICTED
```

**Completion**:
```
INFO  HDFSTierEvictionExecutor: Eviction complete: 125 succeeded, 0 failed, 250000000000 bytes freed
INFO  HDFSTierEvictionCoordinator: Successfully reduced storage usage to target level
```

### Metrics

Access eviction statistics via the metrics server:

```bash
curl http://localhost:16010/hdfs-tier-metrics
```

Response includes:
```json
{
  "eviction": {
    "enabled": true,
    "policy": "LRU",
    "runs": 15,
    "totalBytesEvicted": 3750000000000,
    "totalEvictions": 1875,
    "failedEvictions": 2,
    "lastEvictionTime": 1714896000000
  }
}
```

---

## Troubleshooting

### Eviction Not Triggering

**Problem**: Storage exceeds threshold but no eviction occurs

**Checks**:
1. Verify eviction is enabled:
   ```bash
   grep "hbase.hdfstier.eviction.enabled" conf/hbase-site.xml
   ```

2. Check logs for errors:
   ```bash
   grep "eviction" logs/hbase-*.log | grep ERROR
   ```

3. Verify coordinator started:
   ```bash
   grep "HDFSTierEvictionCoordinator" logs/hbase-*.log
   ```

### No Eviction Candidates Found

**Problem**: Eviction triggered but no files selected

**Possible Causes**:
- All files already `COMPACTED` or `EVICTED`
- No `lastAccess` timestamps (access tracking not working)

**Solution**:
1. Check metadata table:
   ```bash
   echo "scan 'hdfsTier:meta', {COLUMNS => ['transition:currState', 'info:lastAccess'], LIMIT => 10}" | hbase shell
   ```

2. Verify access tracking is working (see ACCESS_TRACKING_FIX_SUMMARY.md)

### Eviction Too Slow

**Problem**: Usage stays above threshold for too long

**Solutions**:
1. Decrease check interval:
   ```xml
   <property>
     <name>hbase.hdfstier.eviction.chore.period.sec</name>
     <value>60</value> <!-- Check every minute -->
   </property>
   ```

2. Increase eviction aggressiveness:
   ```xml
   <property>
     <name>hbase.hdfstier.eviction.target</name>
     <value>60.0</value> <!-- Evict more files -->
   </property>
   ```

### Files Not Deleted from HDFS

**Problem**: Metadata marked as EVICTED but files remain in HDFS

**Checks**:
1. Check HDFS permissions:
   ```bash
   hdfs dfs -ls hdfs://hbase_cache/
   ```

2. Verify HDFS path configuration matches:
   ```bash
   grep "hbase.hdfstier.storage.path" conf/hbase-site.xml
   ```

3. Check executor logs:
   ```bash
   grep "HDFSTierEvictionExecutor" logs/hbase-*.log | grep ERROR
   ```

---

## Custom Eviction Policies

### Implementing a Custom Policy

1. **Create class implementing EvictionPolicy**:

```java
package com.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.hdfs.tier.eviction.EvictionPolicy;
import org.apache.hadoop.hbase.hdfs.tier.eviction.HFileEvictionCandidate;

import java.io.IOException;
import java.util.List;

public class MyCustomEvictionPolicy implements EvictionPolicy {
  
  public MyCustomEvictionPolicy(Configuration conf) {
    // Initialize with configuration
  }
  
  @Override
  public List<HFileEvictionCandidate> selectFilesForEviction(long targetBytesToEvict) 
      throws IOException {
    // Your custom selection logic
    return candidates;
  }
  
  @Override
  public String getPolicyName() {
    return "MyCustomPolicy";
  }
}
```

2. **Configure in hbase-site.xml**:

```xml
<property>
  <name>hbase.hdfstier.eviction.policy</name>
  <value>com.example.hbase.MyCustomEvictionPolicy</value>
</property>
```

3. **Add JAR to HBase classpath**:
```bash
cp my-custom-policy.jar $HBASE_HOME/lib/
```

---

## Testing

### Manual Eviction Trigger

For testing, you can manually trigger an eviction check:

```java
// Access the eviction coordinator
HDFSTierEvictionCoordinator coordinator = ...; // get from observer
coordinator.triggerEvictionCheck();
```

### Verify Eviction Works

1. **Create test data to exceed threshold**:
   ```bash
   # Fill cache tier to >90% of max storage
   ```

2. **Check logs for eviction trigger**:
   ```bash
   tail -f logs/hbase-*.log | grep "eviction"
   ```

3. **Verify files evicted**:
   ```bash
   # Before
   hdfs dfs -du -h hdfs://hbase_cache/
   
   # After eviction
   hdfs dfs -du -h hdfs://hbase_cache/
   # Should show reduced usage
   ```

4. **Check metadata table**:
   ```bash
   echo "scan 'hdfsTier:meta', {FILTER => \"SingleColumnValueFilter('transition', 'currState', =, 'binary:EVICTED')\"}" | hbase shell
   # Should show evicted files
   ```

---

## Best Practices

1. **Set threshold below critical limit**: Leave buffer space (e.g., 90% threshold for 100% capacity)

2. **Target should be comfortably below threshold**: At least 10-20% gap to avoid thrashing

3. **Monitor eviction frequency**: Too frequent = threshold too low or storage too small

4. **Review failed evictions**: Check logs for patterns

5. **Test eviction before production**: Use small storage limits to verify behavior

6. **Keep access tracking enabled**: LRU policy requires accurate lastAccess timestamps

---

## Summary

- **Purpose**: Automatically manage HDFS cache tier storage
- **Default Policy**: LRU (Least Recently Used)
- **Triggers**: When usage exceeds threshold (default: 90%)
- **Target**: Evicts until usage reaches target (default: 70%)
- **Frequency**: Checks every interval (default: 5 minutes)
- **Graceful**: Handles failures, continues with remaining files
- **Pluggable**: Support for custom eviction policies
