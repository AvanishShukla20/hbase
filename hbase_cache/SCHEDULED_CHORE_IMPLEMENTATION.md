# HDFS Tier Eviction - ScheduledChore Implementation

## Overview

This document describes the implementation of the **HDFSTier Eviction ScheduledChore Service**, which periodically checks storage utilization and triggers eviction when configured thresholds are exceeded.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                    HdfsTierObserver                              │
│                  (Coprocessor Entry Point)                       │
└────────────┬────────────────────────────────┬───────────────────┘
             │                                │
             │ Initializes                    │ Initializes
             ▼                                ▼
┌─────────────────────────┐    ┌──────────────────────────────────┐
│  HDFSTierEvictionChore  │    │ HDFSTierEvictionChoreService     │
│  Service                │    │  (Scheduler & Lifecycle Manager) │
│  (Manages Executor)     │    └────────────┬─────────────────────┘
└────────────┬────────────┘                  │
             │                               │ Schedules
             │ Runs periodically             ▼
             │                    ┌──────────────────────────┐
             └───────────────────►│  Periodic Check Task     │
                                  └────────────┬─────────────┘
                                               │
                                               ▼
                          ┌────────────────────────────────────────┐
                          │  1. Get Storage Stats (StorageMonitor) │
                          │  2. Check Usage % vs Threshold         │
                          │  3. If Exceeded → Trigger Eviction     │
                          └────────────────┬───────────────────────┘
                                           │
                                           ▼
                    ┌──────────────────────────────────────────────┐
                    │     HDFSTierEvictionCoordinator              │
                    │     (Eviction Orchestrator)                  │
                    └──────────────┬───────────────────────────────┘
                                   │
                    ┌──────────────┼──────────────┐
                    ▼              ▼              ▼
        ┌───────────────┐  ┌──────────────┐  ┌──────────────┐
        │ EvictionPolicy│  │   Executor   │  │ Meta Table   │
        │   (LRU)       │  │              │  │   Updates    │
        └───────────────┘  └──────────────┘  └──────────────┘
```

## Components

### 1. HDFSTierEvictionChoreService

**Location:** `hbase_cache/src/main/java/.../eviction/HDFSTierEvictionChoreService.java`

**Purpose:** Manages the lifecycle and scheduling of periodic eviction checks.

**Key Features:**
- Creates and manages `ScheduledExecutorService` for periodic execution
- Implements `Stoppable` interface for clean shutdown
- Configurable period and initial delay
- Safe startup/shutdown with proper executor lifecycle management

**Configuration:**
```xml
<property>
  <name>hbase.hdfstier.eviction.chore.period.sec</name>
  <value>300</value>
  <description>Period in seconds (default: 300 = 5 minutes)</description>
</property>

<property>
  <name>hbase.hdfstier.eviction.chore.delay.sec</name>
  <value>60</value>
  <description>Initial delay in seconds (default: 60 = 1 minute)</description>
</property>
```

**Methods:**
- `start()` - Initializes executor and schedules periodic checks
- `stop(String why)` - Gracefully shuts down executor
- `performChore()` - Main logic executed on each run
- `getStats()` - Returns service statistics

### 2. HDFSTierEvictionCoordinator (Updated)

**Location:** `hbase_cache/src/main/java/.../eviction/HDFSTierEvictionCoordinator.java`

**Changes:**
- Removed internal `ScheduledExecutorService` (now handled by ChoreService)
- Removed `start()` method and internal scheduling logic
- Made `performEviction(long currentUsageBytes)` public for chore to call
- Simplified shutdown (no longer manages scheduler)

**Purpose:** Coordinates eviction logic when triggered by chore service.

**Key Responsibilities:**
1. Calculate bytes to evict based on target percentage
2. Delegate file selection to configured `EvictionPolicy`
3. Execute eviction via `HDFSTierEvictionExecutor`
4. Track eviction statistics

### 3. HdfsTierObserver (Updated)

**Location:** `hbase_cache/src/main/java/.../HdfsTierObserver.java`

**Changes in `start()` method:**
```java
// Initialize eviction coordinator (no scheduler)
this.evictionCoordinator = new HDFSTierEvictionCoordinator(conf, storageMonitor, metadataCapture);

// Initialize and start chore service
this.evictionChoreService = new HDFSTierEvictionChoreService(conf, storageMonitor, evictionCoordinator);
this.evictionChoreService.start();
```

**Changes in `stop()` method:**
```java
// Stop chore service first
evictionChoreService.stop("HdfsTierObserver stopping");

// Then shutdown coordinator
evictionCoordinator.shutdown();
```

## Workflow

### Startup Sequence

1. **HBase starts** → Loads `HdfsTierObserver` coprocessor
2. **Observer.start()** is called
3. **StorageMonitor** is initialized (singleton)
4. **EvictionCoordinator** is created (no internal scheduler)
5. **EvictionChoreService** is created and started
6. **Executor** is created with daemon thread
7. **First check** scheduled after initial delay (default: 60 seconds)
8. **Periodic checks** continue every period (default: 300 seconds)

### Periodic Check Execution

```
Time: 0s
├─ HBase starts
├─ Chore service initializes
│
Time: 60s (Initial Delay)
├─ First eviction check runs
│  ├─ Get storage stats from StorageMonitor
│  ├─ Calculate usage percentage
│  ├─ Compare with threshold (90%)
│  └─ If exceeded → Trigger eviction
│
Time: 360s (60 + 300)
├─ Second eviction check runs
│  └─ [Same logic]
│
Time: 660s (360 + 300)
├─ Third eviction check runs
│  └─ [Continue...]
```

### Eviction Trigger Flow

```
Chore Check
    │
    ├─ Get currentUsageBytes from StorageMonitor
    │
    ├─ Calculate usagePercent = (currentUsageBytes / maxStorageBytes) * 100
    │
    ├─ IF usagePercent >= threshold (90%)
    │   │
    │   └─ Call: evictionCoordinator.performEviction(currentUsageBytes)
    │       │
    │       ├─ Calculate bytesToEvict = currentUsageBytes - targetUsageBytes
    │       │
    │       ├─ Call: evictionPolicy.selectFilesForEviction(bytesToEvict)
    │       │   └─ Returns List<HFileEvictionCandidate> (LRU sorted)
    │       │
    │       ├─ Call: executor.executeEviction(filesToEvict)
    │       │   └─ Marks each file as EVICTED in hdfsTier:meta
    │       │
    │       └─ Update statistics
    │
    └─ ELSE
        └─ Log: "No eviction needed"
```

### Shutdown Sequence

1. **HBase shutdown initiated**
2. **Observer.stop()** is called
3. **ChoreService.stop()** is called
   - Executor.shutdown() called
   - Waits up to 30 seconds for termination
   - Force shutdown if timeout
4. **Coordinator.shutdown()** is called
   - Closes eviction policy resources
5. **All threads stopped**

## Configuration Reference

### Core Eviction Settings

```xml
<!-- Enable/Disable Eviction -->
<property>
  <name>hbase.hdfstier.eviction.enabled</name>
  <value>true</value>
</property>

<!-- Eviction Policy -->
<property>
  <name>hbase.hdfstier.eviction.policy</name>
  <value>LRU</value>
</property>

<!-- Threshold to Trigger Eviction -->
<property>
  <name>hbase.hdfstier.eviction.threshold</name>
  <value>90.0</value>
  <description>Trigger eviction when usage exceeds 90%</description>
</property>

<!-- Target Usage After Eviction -->
<property>
  <name>hbase.hdfstier.eviction.target</name>
  <value>70.0</value>
  <description>Evict files until usage drops to 70%</description>
</property>

<!-- Max Storage Size -->
<property>
  <name>hbase.hdfstier.storage.max.bytes</name>
  <value>107374182400</value>
  <description>100 GB maximum storage</description>
</property>
```

### Chore Scheduling Settings

```xml
<!-- Chore Execution Period -->
<property>
  <name>hbase.hdfstier.eviction.chore.period.sec</name>
  <value>300</value>
  <description>Run every 5 minutes</description>
</property>

<!-- Initial Delay Before First Run -->
<property>
  <name>hbase.hdfstier.eviction.chore.delay.sec</name>
  <value>60</value>
  <description>Wait 1 minute before first run</description>
</property>
```

**Why Initial Delay?**

The `hbase.hdfstier.eviction.chore.delay.sec` configuration provides an initial delay before the first eviction check runs. This is important for several reasons:

1. **System Stabilization**: When HBase RegionServer starts, it needs time to:
   - Initialize all coprocessors
   - Load regions and HFiles
   - Establish connections to other services
   - Build initial metadata cache

2. **Avoid False Triggers**: Running eviction immediately after startup could:
   - Read incomplete storage statistics
   - Trigger unnecessary eviction due to initialization artifacts
   - Interfere with system startup performance

3. **Graceful Startup**: The delay allows:
   - Storage monitor to complete initial reconciliation
   - HFile access tracking to initialize
   - All components to reach stable state

4. **Standard Practice**: This follows the standard chore pattern in HBase where background tasks wait for system stabilization before beginning periodic work.

**Recommended Values:**
- **Production**: 60-120 seconds (allow full startup)
- **Testing**: 10-30 seconds (faster feedback)
- **Development**: 5-10 seconds (immediate testing)

## Logging

### Log Locations

- **RegionServer Logs:** `<HBASE_HOME>/logs/hbase-*-regionserver-*.log`

### Key Log Messages

```log
# Startup
INFO  [main] HdfsTierObserver: HDFSTier Eviction Coordinator initialized successfully
INFO  [main] HdfsTierObserver: HDFSTier Eviction Chore Service started successfully
INFO  [main] HDFSTierEvictionChoreService: HDFSTierEvictionChoreService started successfully

# Periodic Checks
INFO  [HDFSTier-Eviction-Chore] HDFSTierEvictionChoreService: HDFSTier Eviction Chore: Storage check - 50000000 bytes (47 MB, 46.57% of 100.0 GB max)
DEBUG [HDFSTier-Eviction-Chore] HDFSTierEvictionChoreService: Storage usage 46.57% is below threshold 90.0% - no eviction needed

# Eviction Triggered
WARN  [HDFSTier-Eviction-Chore] HDFSTierEvictionChoreService: Storage usage 92.34% exceeds threshold 90.0% - triggering eviction
INFO  [HDFSTier-Eviction-Chore] HDFSTierEvictionChoreService: Triggering eviction #1: current usage=99000000000 bytes (92.34%), target=70.0%
INFO  [HDFSTier-Eviction-Chore] HDFSTierEvictionCoordinator: Starting eviction: need to free 24000000000 bytes to reach target 70.0%
INFO  [HDFSTier-Eviction-Chore] LRUEvictionPolicy: Selected 150 files for eviction (25000000000 bytes) using LRU policy
INFO  [HDFSTier-Eviction-Chore] HDFSTierEvictionExecutor: Successfully marked 150 files as EVICTED
INFO  [HDFSTier-Eviction-Chore] HDFSTierEvictionCoordinator: Eviction complete: freed 25000000000 bytes, new usage: 74000000000 bytes (68.88%)

# Shutdown
INFO  [shutdown-hook] HdfsTierObserver: HdfsTierObserver stopping
INFO  [shutdown-hook] HDFSTierEvictionChoreService: Stopping HDFSTierEvictionChoreService: HdfsTierObserver stopping
INFO  [shutdown-hook] HDFSTierEvictionChoreService: HDFSTierEvictionChoreService stopped
```

## Monitoring

### Check Service Status

```bash
# View recent chore activity
grep "HDFSTier.*Eviction.*Chore" logs/hbase-*-regionserver-*.log | tail -20

# Check eviction triggers
grep "triggering eviction" logs/hbase-*-regionserver-*.log | tail -10

# View storage checks
grep "Storage check" logs/hbase-*-regionserver-*.log | tail -10
```

### Metrics Endpoint

```bash
curl http://localhost:8090/metrics 2>/dev/null | python -m json.tool
```

**Expected Output:**
```json
{
  "storage": {
    "totalUsedBytes": 50000000,
    "totalAllocatedBytes": 107374182400,
    "usagePercent": 0.046566128730773926
  },
  "eviction": {
    "enabled": true,
    "policy": "LRU",
    "threshold": 90.0,
    "target": 70.0
  }
}
```

## Troubleshooting

### Issue: Eviction Never Triggers

**Symptoms:**
- Storage exceeds threshold but eviction doesn't run
- No "triggering eviction" messages in logs

**Diagnosis:**
```bash
# Check if chore service started
grep "Eviction Chore Service started" logs/hbase-*-regionserver-*.log

# Check if eviction is enabled
grep "hbase.hdfstier.eviction.enabled" conf/hbase-site.xml

# Check storage usage
curl http://localhost:8090/metrics
```

**Solutions:**
1. Verify `hbase.hdfstier.eviction.enabled=true`
2. Check chore service logs for errors
3. Verify storage monitor is tracking files correctly
4. Restart HBase to reinitialize services

### Issue: Chore Service Fails to Start

**Symptoms:**
- Error during coprocessor initialization
- No periodic checks running

**Diagnosis:**
```bash
grep "Failed to start eviction chore service" logs/hbase-*-regionserver-*.log
```

**Solutions:**
1. Check configuration syntax in hbase-site.xml
2. Verify eviction coordinator initialized successfully
3. Check for dependency initialization errors
4. Review full stack trace in logs

### Issue: Excessive Eviction Runs

**Symptoms:**
- Eviction triggers on every check
- Storage never reaches target

**Diagnosis:**
```bash
# Count eviction triggers
grep "triggering eviction" logs/hbase-*-regionserver-*.log | wc -l

# Check if target is being met
grep "Eviction complete" logs/hbase-*-regionserver-*.log | tail -5
```

**Solutions:**
1. Lower eviction threshold (e.g., 85% instead of 90%)
2. Increase target gap (e.g., target=60% with threshold=90%)
3. Verify LRU policy is selecting enough files
4. Check that files are actually being marked as EVICTED

### Issue: Executor Won't Shutdown

**Symptoms:**
- HBase shutdown hangs
- "Executor did not terminate" in logs

**Diagnosis:**
```bash
grep "Executor did not terminate" logs/hbase-*-regionserver-*.log
```

**Solutions:**
1. Increase shutdown timeout (currently 30 seconds)
2. Check for long-running eviction operations
3. Force shutdown if needed (already implemented)

## Performance Considerations

### Thread Usage

- **Single daemon thread** for chore execution
- Non-blocking: doesn't hold up HBase operations
- Graceful shutdown with timeout protection

### CPU Impact

- Minimal: Only runs every 5 minutes (configurable)
- Lightweight storage check operation
- Eviction only triggered when needed

### Memory Impact

- Small: Service maintains minimal state
- No caching of large data structures
- Statistics tracked with primitive types

### I/O Impact

- Storage check: 1 query to StorageMonitor (in-memory)
- Eviction trigger: Scan of hdfsTier:meta table (only when threshold exceeded)
- Metadata updates: Batched by eviction executor

## Best Practices

### Production Configuration

```xml
<!-- Conservative settings for production -->
<property>
  <name>hbase.hdfstier.eviction.chore.period.sec</name>
  <value>300</value>
  <description>5 minutes - balance between responsiveness and overhead</description>
</property>

<property>
  <name>hbase.hdfstier.eviction.threshold</name>
  <value>85.0</value>
  <description>Trigger early to avoid hitting absolute limit</description>
</property>

<property>
  <name>hbase.hdfstier.eviction.target</name>
  <value>70.0</value>
  <description>Comfortable gap to avoid immediate re-trigger</description>
</property>
```

### Testing Configuration

```xml
<!-- Aggressive settings for testing -->
<property>
  <name>hbase.hdfstier.eviction.chore.period.sec</name>
  <value>60</value>
  <description>1 minute - faster feedback during testing</description>
</property>

<property>
  <name>hbase.hdfstier.eviction.chore.delay.sec</name>
  <value>10</value>
  <description>10 seconds - quick startup for testing</description>
</property>
```

### Monitoring Recommendations

1. **Set up alerts** for repeated eviction failures
2. **Monitor storage trends** to predict capacity needs
3. **Review eviction logs weekly** to tune thresholds
4. **Track LRU effectiveness** (check if oldest files are being selected)

## Future Enhancements

### Potential Improvements

1. **Adaptive Scheduling**
   - Increase check frequency when approaching threshold
   - Reduce frequency when well below threshold

2. **Predictive Eviction**
   - Analyze storage growth trends
   - Trigger eviction before threshold based on prediction

3. **Multi-Tier Thresholds**
   - Warning threshold (e.g., 80%) - log only
   - Soft threshold (e.g., 90%) - normal eviction
   - Hard threshold (e.g., 95%) - aggressive eviction

4. **Dynamic Policy Selection**
   - Switch policies based on workload characteristics
   - Time-based policy changes (e.g., LRU during night, LFU during day)

## Related Documentation

- **EVICTION_VERIFICATION_GUIDE.md** - How to test and verify eviction
- **ARCHITECTURE.md** - Overall HDFS tier design
- **HDFS_TIER_CONFIGURATION_GUIDE.md** - Complete configuration reference

## Support

For issues or questions:
1. Check logs for error messages
2. Verify configuration settings
3. Review this documentation
4. Check related guides listed above
