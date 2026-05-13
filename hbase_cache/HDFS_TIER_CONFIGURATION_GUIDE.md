# HDFS Tier Configuration Guide

This document describes all configuration properties for the HDFS Tier caching module.

## Table of Contents
1. [Core Configuration](#core-configuration)
2. [Storage Monitor Configuration](#storage-monitor-configuration)
3. [HFile Access Tracking Configuration](#hfile-access-tracking-configuration)
4. [Metadata Update & Retry Configuration](#metadata-update--retry-configuration)
5. [Eviction Configuration](#eviction-configuration)
6. [Metrics Server Configuration](#metrics-server-configuration)
7. [Configuration Examples](#configuration-examples)

---

## Core Configuration

### `hbase.coprocessor.region.classes`
- **Type:** String
- **Default:** (none)
- **Required:** Yes
- **Description:** Register the HdfsTier coprocessor to enable flush/compaction/access tracking
- **Value:** `org.apache.hadoop.hbase.hdfs.tier.HdfsTierObserver`

---

## Storage Monitor Configuration

### `hbase.hdfstier.max.storage.size`
- **Type:** Long (bytes)
- **Default:** `107374182400` (100 GB)
- **Description:** Maximum storage size allocated for HDFS tier cache
- **Example:** `107374182400` = 100 GB, `53687091200` = 50 GB
- **Notes:** This value should match `hbase.hdfstier.storage.max.bytes`

### `hbase.hdfstier.reconciliation.interval.minutes`
- **Type:** Integer (minutes)
- **Default:** `10`
- **Description:** Interval for periodic reconciliation scan of hdfsTier:meta table to correct storage metrics after restarts or failures
- **Recommended:** 5-15 minutes for production, 1-2 minutes for testing

---

## HFile Access Tracking Configuration

### `hbase.hfile.access.tracking.enabled`
- **Type:** Boolean
- **Default:** `true`
- **Description:** Enable/disable HFile access tracking for read operations
- **Impact:** When disabled, lastAccessTimestamp and accessCount will not be updated

### `hbase.hfile.access.queue.size`
- **Type:** Integer
- **Default:** `200000`
- **Description:** Maximum number of access events in the queue before dropping
- **Memory Impact:** ~10-20 MB for 100K events
- **Tuning:** 
  - Increase for high-throughput workloads
  - Decrease if memory is constrained

### `hbase.hfile.access.worker.threads`
- **Type:** Integer
- **Default:** Auto-tuned to `CPU cores / 4`
- **Description:** Number of worker threads processing access events
- **Recommended:** 
  - Small clusters: 2-4 threads
  - Large clusters: 4-8 threads
  - High throughput: 8-16 threads

### `hbase.hfile.access.monitor.interval.sec`
- **Type:** Long (seconds)
- **Default:** `60`
- **Description:** Interval for logging access tracking statistics
- **Example Stats:**
  ```
  AccessTracking: 1523 events processed, 12 pending, 0.15% drop rate, 1.2% failure rate
  ```

---

## Metadata Update & Retry Configuration

### Immediate Retry Configuration (Within Worker Thread)

#### `hbase.metadata.update.max.retries`
- **Type:** Integer
- **Default:** `3`
- **Description:** Immediate retry attempts within the worker thread before queuing for deferred retry
- **Retry Delays:** Exponential backoff (100ms, 200ms, 400ms, ...)

#### `hbase.metadata.update.retry.delay.ms`
- **Type:** Long (milliseconds)
- **Default:** `100`
- **Description:** Initial retry delay for exponential backoff
- **Pattern:** delay * 2^attempt (100ms → 200ms → 400ms → 800ms)

### Deferred Retry Configuration (FailedUpdateRetryManager)

#### `hbase.metadata.update.max.total.retries`
- **Type:** Integer
- **Default:** `5`
- **Description:** Maximum total retries by FailedUpdateRetryManager before giving up
- **Notes:** After exhausting retries, failures are logged for manual intervention

#### `hbase.metadata.retry.interval.minutes`
- **Type:** Long (minutes)
- **Default:** `5`
- **Description:** Interval for retrying failed metadata updates
- **Recommended:** 5-10 minutes for production

### Failure Monitoring Configuration

#### `hbase.metadata.update.failure.threshold`
- **Type:** Double (percentage as decimal)
- **Default:** `0.05` (5%)
- **Description:** Alert threshold for failure rate
- **Alert Trigger:** When `(failures / total_updates) > threshold`

#### `hbase.metadata.max.tracked.failures`
- **Type:** Integer
- **Default:** `10000`
- **Description:** Maximum number of failed updates to track in memory
- **Memory Impact:** ~1-2 MB for 10K failures
- **Notes:** Oldest failures are dropped when limit is reached

---

## Eviction Configuration

### Core Eviction Settings

#### `hbase.hdfstier.eviction.enabled`
- **Type:** Boolean
- **Default:** `true`
- **Description:** Enable/disable automatic eviction
- **Notes:** Set to `false` to disable eviction entirely

#### `hbase.hdfstier.eviction.policy`
- **Type:** String
- **Default:** `LRU`
- **Description:** Eviction policy to use
- **Available Policies:**
  - `LRU` - Least Recently Used (evicts files with oldest lastAccessTimestamp)
  - `FIFO` - First In First Out (future)
  - Custom policy class name (must implement `EvictionPolicy` interface)

### Storage Thresholds

#### `hbase.hdfstier.eviction.threshold`
- **Type:** Double (percentage)
- **Default:** `90.0`
- **Description:** Storage usage percentage that triggers eviction
- **Example:** At 90%, if 90GB of 100GB is used, eviction starts

#### `hbase.hdfstier.eviction.target`
- **Type:** Double (percentage)
- **Default:** `70.0`
- **Description:** Target storage usage percentage after eviction
- **Example:** Evict files until usage drops to 70GB (70% of 100GB)

### Eviction Timing

#### `hbase.hdfstier.eviction.chore.period.sec`
- **Type:** Long (seconds)
- **Default:** `300` (5 minutes)
- **Description:** Interval for periodic eviction checks (chore execution period)
- **Recommended:**
  - Production: 300-600 seconds
  - Testing: 60-120 seconds

#### `hbase.hdfstier.eviction.chore.delay.sec`
- **Type:** Long (seconds)
- **Default:** `60` (1 minute)
- **Description:** Initial delay before first eviction check after startup
- **Purpose:** Allows system to stabilize before starting eviction checks
- **Recommended:**
  - Production: 60-120 seconds
  - Testing: 10-30 seconds

### Storage Size Configuration

#### `hbase.hdfstier.storage.max.bytes`
- **Type:** Long (bytes)
- **Default:** `107374182400` (100 GB)
- **Description:** Maximum storage in bytes for eviction calculations
- **Notes:** Should match `hbase.hdfstier.max.storage.size`

---

## Metrics Server Configuration

### `hbase.hdfstier.metrics.port`
- **Type:** Integer
- **Default:** `8090`
- **Description:** HTTP port for HdfsTier metrics server
- **Endpoints:**
  - `GET /metrics` - Returns JSON metrics (storage usage, HFile counts, etc.)
  - `GET /health` - Health check endpoint
  - `GET /reconcile` - Trigger manual reconciliation

---

## Configuration Examples

### Example 1: Development Environment (Small Storage, Frequent Checks)

```xml
<!-- Enable coprocessor -->
<property>
  <name>hbase.coprocessor.region.classes</name>
  <value>org.apache.hadoop.hbase.hdfs.tier.HdfsTierObserver</value>
</property>

<!-- Small storage (10 GB) -->
<property>
  <name>hbase.hdfstier.max.storage.size</name>
  <value>10737418240</value>
</property>

<property>
  <name>hbase.hdfstier.storage.max.bytes</name>
  <value>10737418240</value>
</property>

<!-- Frequent checks for testing -->
<property>
  <name>hbase.hdfstier.reconciliation.interval.minutes</name>
  <value>2</value>
</property>

<property>
  <name>hbase.hdfstier.eviction.chore.period.sec</name>
  <value>60</value>
</property>

<property>
  <name>hbase.hdfstier.eviction.chore.delay.sec</name>
  <value>10</value>
</property>

<!-- Lower eviction thresholds -->
<property>
  <name>hbase.hdfstier.eviction.threshold</name>
  <value>80.0</value>
</property>

<property>
  <name>hbase.hdfstier.eviction.target</name>
  <value>60.0</value>
</property>
```

### Example 2: Production Environment (Large Storage, Conservative)

```xml
<!-- Enable coprocessor -->
<property>
  <name>hbase.coprocessor.region.classes</name>
  <value>org.apache.hadoop.hbase.hdfs.tier.HdfsTierObserver</value>
</property>

<!-- Large storage (500 GB) -->
<property>
  <name>hbase.hdfstier.max.storage.size</name>
  <value>536870912000</value>
</property>

<property>
  <name>hbase.hdfstier.storage.max.bytes</name>
  <value>536870912000</value>
</property>

<!-- Conservative reconciliation -->
<property>
  <name>hbase.hdfstier.reconciliation.interval.minutes</name>
  <value>15</value>
</property>

<!-- Conservative eviction (less aggressive) -->
<property>
  <name>hbase.hdfstier.eviction.threshold</name>
  <value>95.0</value>
</property>

<property>
  <name>hbase.hdfstier.eviction.target</name>
  <value>80.0</value>
</property>

<property>
  <name>hbase.hdfstier.eviction.chore.period.sec</name>
  <value>600</value>
</property>

<property>
  <name>hbase.hdfstier.eviction.chore.delay.sec</name>
  <value>120</value>
</property>

<!-- Higher queue for high throughput -->
<property>
  <name>hbase.hfile.access.queue.size</name>
  <value>500000</value>
</property>

<property>
  <name>hbase.hfile.access.worker.threads</name>
  <value>8</value>
</property>
```

### Example 3: Eviction Disabled (Manual Management)

```xml
<!-- Disable automatic eviction -->
<property>
  <name>hbase.hdfstier.eviction.enabled</name>
  <value>false</value>
</property>

<!-- Still track access for manual decisions -->
<property>
  <name>hbase.hfile.access.tracking.enabled</name>
  <value>true</value>
</property>
```

---

## Configuration Validation

### Consistency Checks

1. **Storage Size Consistency:**
   ```
   hbase.hdfstier.max.storage.size == hbase.hdfstier.storage.max.bytes
   ```

2. **Eviction Threshold Logic:**
   ```
   hbase.hdfstier.eviction.target < hbase.hdfstier.eviction.threshold
   ```
   Example: target=70%, threshold=90% ✓

3. **Retry Configuration:**
   ```
   hbase.metadata.update.max.retries < hbase.metadata.update.max.total.retries
   ```
   Example: immediate=3, total=5 ✓

### Performance Tuning Guidelines

| Workload Type | Queue Size | Worker Threads | Check Interval | Threshold |
|---------------|------------|----------------|----------------|-----------|
| Light         | 50K-100K   | 2-4            | 600s           | 90%       |
| Medium        | 100K-200K  | 4-8            | 300s           | 90%       |
| Heavy         | 200K-500K  | 8-16           | 120s           | 85%       |

---

## Monitoring Recommendations

### Key Metrics to Monitor

1. **Storage Usage** (from `/metrics` endpoint)
   - `totalStorageUsedBytes`
   - `totalAllocatedStorageBytes`
   - `usagePercent`

2. **Access Tracking** (from logs)
   - Event processing rate
   - Queue depth
   - Drop rate
   - Failure rate

3. **Eviction Activity** (from logs)
   - Eviction runs triggered
   - Bytes evicted
   - Files evicted
   - Time to complete eviction

### Alert Thresholds

- **Critical:** Storage usage > 95%
- **Warning:** Storage usage > eviction.threshold
- **Warning:** Access event drop rate > 1%
- **Warning:** Metadata update failure rate > 5%
- **Info:** Eviction triggered

---

## Troubleshooting

### Issue: Tests not running (0 tests executed)

**Symptoms:**
```
[INFO] Tests run: 0, Failures: 0, Errors: 0, Skipped: 0
```

**Causes:**
1. Test classes not using JUnit 5 annotations (@Test from org.junit.jupiter.api.Test)
2. Missing JUnit 5 dependencies in pom.xml
3. Maven Surefire plugin not configured for JUnit 5

**Solution:**
- Ensure test classes use `org.junit.jupiter.api.Test`
- Add JUnit 5 dependencies to pom.xml
- Configure Surefire plugin with JUnit Platform

### Issue: Access tracking not updating hdfsTier:meta table

**Symptoms:**
- lastAccessTimestamp not updating
- accessCount remains at 0

**Checklist:**
1. ✓ `hbase.hfile.access.tracking.enabled=true`
2. ✓ Coprocessor registered: `hbase.coprocessor.region.classes=org.apache.hadoop.hbase.hdfs.tier.HdfsTierObserver`
3. ✓ HFiles exist in hdfsTier:meta with `status=ACTIVE`
4. ✓ Check logs for `HFile access tracking initialized successfully`
5. ✓ Check logs for metadata update failures

### Issue: Eviction not triggering

**Symptoms:**
- Storage usage > threshold but no eviction

**Checklist:**
1. ✓ `hbase.hdfstier.eviction.enabled=true`
2. ✓ Check logs for `Storage usage X% exceeds threshold Y%`
3. ✓ Verify storage calculations: `hbase.hdfstier.max.storage.size` matches actual usage
4. ✓ Check if eviction coordinator started: `HDFSTier Eviction Coordinator started successfully`
5. ✓ Trigger manual eviction via `/reconcile` endpoint

---

## Configuration Change Checklist

When modifying HDFS Tier configuration:

- [ ] Update `conf/hbase-site.xml`
- [ ] Restart HBase (or affected RegionServers)
- [ ] Verify configuration loaded (check logs for "HdfsTierObserver started")
- [ ] Monitor metrics endpoint for expected behavior
- [ ] Check for configuration-related errors in logs
- [ ] Update documentation if adding new configurations

---

## References

- [ARCHITECTURE.md](./ARCHITECTURE.md) - System architecture overview
- [EVICTION_CONFIGURATION.md](./EVICTION_CONFIGURATION.md) - Detailed eviction configuration
- [HFILE_ACCESS_TRACKING_README.md](./HFILE_ACCESS_TRACKING_README.md) - Access tracking details
