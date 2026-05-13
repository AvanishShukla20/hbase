# LRU Eviction Policy Verification Guide

This guide provides step-by-step instructions to verify that the LRU eviction policy correctly selects candidate HFiles for eviction based on their lastAccessTimestamp.

---

## 🚀 Quick Start - Manual Trigger (TL;DR)

**If you just want to quickly test the LRU policy and see which HFiles it selects:**

```bash
cd <HBASE_HOME>

# Build the module (if not already built)
mvn clean compile -pl hbase_cache -am

# Manually trigger LRU policy to select candidates (10MB example)
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 10485760
```

**What this does:**
- ✓ Scans hdfsTier:meta for ACTIVE HFiles
- ✓ Runs LRU policy to select candidates
- ✓ Shows you the list sorted by lastAccessTime (oldest first)
- ✓ Verifies LRU ordering is correct
- ✓ **Does NOT actually evict files** (dry-run only)

**The output will show you:**
- Which HFiles would be evicted
- Their last access timestamps
- File sizes
- How long ago they were accessed

**That's it!** The tool displays the complete list of eviction candidates selected by your LRU policy.

For detailed step-by-step verification, continue reading below.

---

## Prerequisites

1. HBase is running with HDFSTier coprocessor enabled
2. Some HFiles exist in the hdfsTier:meta table with ACTIVE status
3. HFiles have been accessed (read operations) to populate lastAccessTimestamp
4. Configuration in `conf/hbase-site.xml`:
   ```xml
   <property>
     <name>hbase.hdfstier.eviction.policy</name>
     <value>LRU</value>
   </property>
   <property>
     <name>hbase.hdfstier.eviction.enabled</name>
     <value>true</value>
   </property>
   ```

---

## Manual Eviction Trigger 

### Purpose

The `ManualEvictionTrigger` tool lets you:
1. Test your LRU eviction policy without waiting for scheduled runs
2. See exactly which HFiles would be selected for eviction
3. Verify the LRU ordering is correct
4. Inspect candidate details before actual eviction

### Usage

```bash
cd <HBASE_HOME>

# Syntax:
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger <bytes-to-free>

# Example 1: Select files to free 10MB (10485760 bytes)
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 10485760

# Example 2: Select files to free 100MB
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 104857600

# Example 3: Select files to free 1GB
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 1073741824
```

### Expected Output

```
╔══════════════════════════════════════════════════════════════════════╗
║          MANUAL LRU EVICTION POLICY TRIGGER                          ║
╚══════════════════════════════════════════════════════════════════════╝

Target bytes to evict: 10.00 MB
Eviction policy: LRU

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Initializing LRU Eviction Policy...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Scanning hdfsTier:meta table for ACTIVE HFiles...
Scan completed in 234 ms

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

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Verifying LRU Ordering...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✓ PASSED: Files are correctly sorted by lastAccessTime (oldest first)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Target Verification
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✓ Target met: Selected 10.50 MB >= 10.00 MB

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
What Happens Next?
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

This was a DRY-RUN. No files were actually evicted.

To execute actual eviction, use:
  ./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.EvictionVerificationTool 10485760 --execute

Or wait for automatic eviction to trigger when:
  - Storage usage exceeds threshold (90.0%)
  - Scheduled check runs (every 300 seconds)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Manual Verification Commands
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

To verify these files in HBase shell:

# Check first candidate:
get 'hdfsTier:meta', 'a1b2c3d4e5f6#abc123def456789.hfile'

# Check last candidate:
get 'hdfsTier:meta', 'x9y8z7w6v5u4#jkl345mno678901.hfile'

✓ Manual trigger complete!
```

### What This Shows You

1. **List of Candidates**: All HFiles selected by LRU policy in order (oldest first)
2. **Last Access Time**: When each file was last read
3. **File Size**: Size of each HFile
4. **Age**: How many hours since last access
5. **Ordering Verification**: Confirms files are sorted correctly
6. **Target Verification**: Confirms enough bytes were selected

### Important Notes

- ⚠️ **This is a DRY-RUN** - No files are actually evicted
- ✓ Safe to run multiple times for testing
- ✓ Uses the same LRU policy as automatic eviction
- ✓ Reads from actual hdfsTier:meta table data
- ✓ No modifications to any data

### Troubleshooting Manual Trigger

**Issue: "No HFiles selected for eviction"**

```bash
# Check if you have ACTIVE HFiles
./bin/hbase shell
hbase> count 'hdfsTier:meta', {FILTER => "SingleColumnValueFilter('transition','currentState',=,'binary:ACTIVE')"}
```

If count is 0, create test data first (see Troubleshooting section below).

**Issue: Compile errors**

```bash
# Rebuild the module
cd <HBASE_HOME>
mvn clean compile -pl hbase_cache -am
```

**Issue: ClassNotFoundException**

Make sure you're in the HBase home directory and using the correct command:
```bash
cd <HBASE_HOME>
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.ManualEvictionTrigger 10485760
```

---

## Step 1: Verify HBase is Running

```bash
cd <HBASE_HOME>

# Check if HBase is running
./bin/hbase-daemon.sh status master
./bin/hbase-daemon.sh status regionserver

# If not running, start HBase
./bin/start-hbase.sh
```

**Expected Output:**
```
Master is running as process <PID>
RegionServer is running as process <PID>
```

---

## Step 2: Check hdfsTier:meta Table Exists and Has Data

```bash
# Open HBase shell
./bin/hbase shell

# Check if table exists
hbase> exists 'hdfsTier:meta'

# Count total rows
hbase> count 'hdfsTier:meta'

# Exit shell
hbase> exit
```

**Expected Output:**
```
Table hdfsTier:meta does exist
Current count: X, row: ...
X row(s)
```

If count is 0, you need to create some HFiles by inserting data into a test table.

---

## Step 3: View All ACTIVE HFiles with Access Information

### Method A: HBase Shell Query

```bash
./bin/hbase shell
```

Then run this scan command:

```ruby
scan 'hdfsTier:meta', {
  COLUMNS => ['info:hfileName', 'info:size', 'info:lastAccessTimestamp', 
              'info:accessCount', 'transition:currentState'], 
  FILTER => "SingleColumnValueFilter('transition','currentState',=,'binary:ACTIVE')"
}
```

**What to Look For:**
- `info:hfileName` - Name of the HFile
- `info:size` - Size in bytes
- `info:lastAccessTimestamp` - When file was last accessed (Unix timestamp in milliseconds)
- `info:accessCount` - Number of times accessed
- `transition:currentState` - Should be "ACTIVE"

**Note:** Files with older `lastAccessTimestamp` should be selected first by LRU policy.

---

## Step 4: List ACTIVE HFiles Sorted by Last Access (Manual Verification)

Create a temporary file to query sorted HFiles:

```bash
cat > /tmp/list_hfiles_lru.rb << 'EOF'
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import java.text.SimpleDateFormat
import java.util.Date

puts "\n" + "="*100
puts "ACTIVE HFiles Sorted by Last Access Time (LRU Order)"
puts "="*100

table = get_table(TableName.valueOf('hdfsTier:meta'))
scan = Scan.new

scan.addColumn(Bytes.toBytes('info'), Bytes.toBytes('hfileName'))
scan.addColumn(Bytes.toBytes('info'), Bytes.toBytes('encodedRegionName'))
scan.addColumn(Bytes.toBytes('info'), Bytes.toBytes('size'))
scan.addColumn(Bytes.toBytes('info'), Bytes.toBytes('lastAccessTimestamp'))
scan.addColumn(Bytes.toBytes('info'), Bytes.toBytes('accessCount'))
scan.addColumn(Bytes.toBytes('transition'), Bytes.toBytes('currentState'))
scan.addColumn(Bytes.toBytes('transition'), Bytes.toBytes('evicted'))

scanner = table.getScanner(scan)
files = []
date_format = SimpleDateFormat.new("yyyy-MM-dd HH:mm:ss")

scanner.each do |result|
  state = result.getValue(Bytes.toBytes('transition'), Bytes.toBytes('currentState'))
  state_str = state ? Bytes.toString(state) : 'UNKNOWN'
  
  evicted_bytes = result.getValue(Bytes.toBytes('transition'), Bytes.toBytes('evicted'))
  is_evicted = evicted_bytes ? Bytes.toBoolean(evicted_bytes) : false
  
  # Only include ACTIVE, non-evicted files
  if state_str == 'ACTIVE' && !is_evicted
    hfile = result.getValue(Bytes.toBytes('info'), Bytes.toBytes('hfileName'))
    region = result.getValue(Bytes.toBytes('info'), Bytes.toBytes('encodedRegionName'))
    size_bytes = result.getValue(Bytes.toBytes('info'), Bytes.toBytes('size'))
    last_access = result.getValue(Bytes.toBytes('info'), Bytes.toBytes('lastAccessTimestamp'))
    access_count = result.getValue(Bytes.toBytes('info'), Bytes.toBytes('accessCount'))
    
    hfile_name = hfile ? Bytes.toString(hfile) : 'N/A'
    region_name = region ? Bytes.toString(region) : 'N/A'
    size = size_bytes ? Bytes.toLong(size_bytes) : 0
    last_access_time = last_access ? Bytes.toLong(last_access) : 0
    access_cnt = access_count ? Bytes.toLong(access_count) : 0
    
    files << {
      :hfile => hfile_name,
      :region => region_name,
      :size => size,
      :lastAccess => last_access_time,
      :accessCount => access_cnt,
      :rowKey => Bytes.toString(result.getRow())
    }
  end
end

scanner.close()
table.close()

# Sort by lastAccess (ascending) - oldest first (LRU order)
sorted_files = files.sort_by { |f| f[:lastAccess] }

puts "\nFound #{sorted_files.size} ACTIVE HFiles eligible for eviction\n\n"

if sorted_files.empty?
  puts "No ACTIVE HFiles found. Create some data first."
else
  puts "%-4s %-45s %-20s %-12s %-12s %-20s" % ['#', 'HFile Name', 'Region (encoded)', 'Size (bytes)', 'Access Count', 'Last Access Time']
  puts "-" * 120
  
  total_size = 0
  sorted_files.each_with_index do |file, idx|
    time_str = if file[:lastAccess] > 0
      date_format.format(Date.new(file[:lastAccess]))
    else
      'Never accessed'
    end
    
    total_size += file[:size]
    
    hfile_short = file[:hfile].length > 45 ? file[:hfile][0..42] + "..." : file[:hfile]
    region_short = file[:region].length > 20 ? file[:region][0..17] + "..." : file[:region]
    
    puts "%-4d %-45s %-20s %-12d %-12d %-20s" % [
      idx + 1, 
      hfile_short, 
      region_short,
      file[:size], 
      file[:accessCount],
      time_str
    ]
  end
  
  puts "-" * 120
  puts "Total: #{sorted_files.size} files, #{total_size} bytes (#{(total_size / (1024.0 * 1024)).round(2)} MB)"
  puts "\n✓ Files are sorted in LRU order (oldest access first)"
  puts "✓ LRU policy will select files from top to bottom until target bytes are met\n"
end
EOF

./bin/hbase org.jruby.Main /tmp/list_hfiles_lru.rb
```

**Expected Output:**
```
================================================================================
ACTIVE HFiles Sorted by Last Access Time (LRU Order)
================================================================================

Found 5 ACTIVE HFiles eligible for eviction

#    HFile Name                                    Region (encoded)      Size (bytes)  Access Count  Last Access Time
------------------------------------------------------------------------------------------------------------------------
1    abc123def456...                               a1b2c3d4e5f6...      1048576       3            2026-05-06 10:15:23
2    xyz789abc012...                               f6e5d4c3b2a1...      2097152       1            2026-05-06 11:30:45
3    def456ghi789...                               b2c3d4e5f6a1...      524288        5            2026-05-06 12:45:10
...
```

**Verification Points:**
- ✓ Files are sorted by `lastAccessTimestamp` (oldest first)
- ✓ Older files appear at the top
- ✓ Files with `lastAccessTimestamp = 0` or null appear first (never accessed)

---

## Step 5: Test LRU Policy Selection with Java Tool

Now run the Java verification tool to see what the LRU policy actually selects:

```bash
# Build the module first (if not already built)
mvn clean compile -pl hbase_cache -am

# Run verification tool (dry-run mode)
# Replace 10485760 with target bytes to evict (10MB in this example)
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.EvictionVerificationTool 10485760
```

**Expected Output:**
```
╔══════════════════════════════════════════════════════════════════════╗
║          LRU EVICTION POLICY VERIFICATION TOOL                       ║
╚══════════════════════════════════════════════════════════════════════╝

Target bytes to evict: 10.00 MB
Mode: DRY-RUN

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 1: Scanning hdfsTier:meta for ACTIVE HFiles...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total ACTIVE files: 25 (125.50 MB)
Total EVICTED files: 5
Total COMPACTED files: 3

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 2: Running LRU Eviction Policy...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ Selected 8 files for eviction

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 3: Eviction Candidates (Sorted by LRU)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#    HFile Name                                    Last Access          Size         Age (hrs)
─────────────────────────────────────────────────────────────────────────────────────────────
1    abc123def456...                               2026-05-06 10:15:23  1.00 MB     25
2    xyz789abc012...                               2026-05-06 11:30:45  2.00 MB     24
3    def456ghi789...                               2026-05-06 12:45:10  512.00 KB   22
...
─────────────────────────────────────────────────────────────────────────────────────────────
Total selected: 10.50 MB from 8 files

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 4: Verifying LRU Ordering...
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
✓ PASSED: Files are correctly sorted by lastAccessTime (oldest first)

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
STEP 5: Target Verification
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Target bytes to evict: 10.00 MB
Actual bytes selected: 10.50 MB
✓ PASSED: Target met

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
DRY-RUN MODE: No files were actually evicted
Run with --execute to perform actual eviction
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

✓ Verification complete!
```

**Verification Checklist:**
- ✓ Files are sorted by lastAccessTime (oldest first)
- ✓ Selection stops when target bytes are reached
- ✓ No ordering violations (each file has lastAccess >= previous file)

---

## Step 6: Compare Manual List with Tool Output

**Manual Verification:**

1. Compare the HFile names from Step 4 (Ruby script) with Step 5 (Java tool)
2. Verify that:
   - The order matches (oldest files first)
   - The same files are selected
   - The timestamps are consistent

**Create a comparison table:**

| Rank | HFile (Step 4 Ruby) | Last Access (Ruby) | HFile (Step 5 Java) | Last Access (Java) | Match? |
|------|---------------------|--------------------|--------------------|-------------------|--------|
| 1    | abc123...           | 2026-05-06 10:15   | abc123...          | 2026-05-06 10:15  | ✓      |
| 2    | xyz789...           | 2026-05-06 11:30   | xyz789...          | 2026-05-06 11:30  | ✓      |
| ...  | ...                 | ...                | ...                | ...               | ...    |

---

## Step 7: Verify Specific HFile Details

To check details of a specific HFile:

```bash
./bin/hbase shell
```

```ruby
# Replace with actual region#hfile rowkey from previous steps
get 'hdfsTier:meta', '<encodedRegionName>#<hfileName>'

# Example:
get 'hdfsTier:meta', 'a1b2c3d4e5f6#abc123def456.hfile'
```

**Expected Output:**
```
COLUMN                               CELL
info:hfileName                      timestamp=..., value=abc123def456.hfile
info:size                           timestamp=..., value=\x00\x00\x00\x00\x00\x10\x00\x00
info:lastAccessTimestamp            timestamp=..., value=\x00\x00\x01\x8F\x2A\x3B\x4C\x5D
info:accessCount                    timestamp=..., value=\x00\x00\x00\x00\x00\x00\x00\x03
transition:currentState             timestamp=..., value=ACTIVE
transition:evicted                  timestamp=..., value=\x00
```

To decode timestamp:

```ruby
# In HBase shell
require 'java'

# Replace with actual bytes from lastAccessTimestamp
timestamp_bytes = "\x00\x00\x01\x8F\x2A\x3B\x4C\x5D".bytes.to_a
timestamp_long = org.apache.hadoop.hbase.util.Bytes.toLong(timestamp_bytes.to_java(:byte))
puts "Timestamp: #{timestamp_long}"
puts "Date: #{Time.at(timestamp_long / 1000).strftime('%Y-%m-%d %H:%M:%S')}"
```

---

## Step 8: Test Actual Eviction (Optional)

**⚠️ WARNING: This will mark files as EVICTED in the metadata table**

```bash
# Execute eviction (not dry-run)
./bin/hbase org.apache.hadoop.hbase.hdfs.tier.eviction.EvictionVerificationTool 10485760 --execute
```

After execution, verify files are marked as evicted:

```bash
./bin/hbase shell
```

```ruby
# Check specific file is now evicted
get 'hdfsTier:meta', '<encodedRegionName>#<hfileName>', {COLUMN => 'transition:evicted'}

# Expected: value=\x01 (true)

# Count evicted files
count 'hdfsTier:meta', {FILTER => "SingleColumnValueFilter('transition','evicted',=,'binary:\\x01')"}
```

---

## Step 9: Check Logs for LRU Policy Execution

```bash
# Check RegionServer logs for LRU activity
grep -i "LRU" <HBASE_HOME>/logs/hbase-*-regionserver-*.log | tail -30

# Look for these log patterns:
# - "Found X eviction candidates (ACTIVE, non-evicted)"
# - "Selected X files for eviction (Y bytes) using LRU policy"
# - "Successfully marked X files as EVICTED"
```

**Expected Log Output:**
```
2026-05-07 14:30:15,123 INFO [Eviction-Thread] LRUEvictionPolicy: Found 25 eviction candidates (ACTIVE, non-evicted)
2026-05-07 14:30:15,145 INFO [Eviction-Thread] LRUEvictionPolicy: Selected 8 files for eviction (11010048 bytes) using LRU policy
2026-05-07 14:30:15,234 INFO [Eviction-Thread] HDFSTierEvictionExecutor: Successfully marked abc123def456.hfile as EVICTED
...
```

---

## Step 10: Verify LRU Logic Correctness

### Test Case 1: Files with No Access (lastAccessTimestamp = 0)

These should be selected **first** by LRU:

```bash
# Find files never accessed
./bin/hbase shell
```

```ruby
scan 'hdfsTier:meta', {
  FILTER => "SingleColumnValueFilter('info','lastAccessTimestamp',=,'binary:\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00') AND SingleColumnValueFilter('transition','currentState',=,'binary:ACTIVE')"
}
```

These files should appear at the top of the LRU selection list.

### Test Case 2: Recently Accessed Files

Recently accessed files should be at the **end** of selection or not selected at all:

```ruby
# Find recently accessed files (within last hour)
# Use current timestamp - 3600000 ms (1 hour)
# You'll need to calculate this manually

scan 'hdfsTier:meta', {
  COLUMNS => ['info:hfileName', 'info:lastAccessTimestamp'],
  FILTER => "SingleColumnValueFilter('transition','currentState',=,'binary:ACTIVE')"
}
```

Check that these files are NOT in the eviction candidate list (or appear last).

---

## Troubleshooting

### Issue: No ACTIVE HFiles Found

**Solution:**
```bash
# Create test data
./bin/hbase shell

hbase> create 'test_table', 'cf'
hbase> put 'test_table', 'row1', 'cf:col1', 'value1'
hbase> put 'test_table', 'row2', 'cf:col2', 'value2'
hbase> flush 'test_table'
hbase> scan 'test_table'  # This will trigger access tracking
```

Wait 30 seconds for metadata to be written, then retry verification.

### Issue: lastAccessTimestamp is Always 0

**Check Configuration:**
```bash
grep -A 1 "hbase.hfile.access.tracking.enabled" conf/hbase-site.xml
```

Should show:
```xml
<value>true</value>
```

If false, enable it and restart HBase.

### Issue: LRU Order Doesn't Match Expected

**Verify System Time:**
```bash
date +%s000  # Should match HBase timestamps
```

**Check for Clock Skew:**
```bash
# Compare timestamps in hdfsTier:meta with current time
# Large differences indicate clock sync issues
```

### Issue: EvictionVerificationTool Compile Errors

```bash
# Rebuild module
cd <HBASE_HOME>
mvn clean compile -pl hbase_cache

# Check for errors
echo $?  # Should be 0
```

---

## Summary Checklist

Before concluding verification, ensure:

- [ ] HBase is running with HDFSTier coprocessor
- [ ] hdfsTier:meta table has ACTIVE HFiles
- [ ] HFiles have been accessed (lastAccessTimestamp populated)
- [ ] Ruby script shows files sorted by lastAccess (oldest first)
- [ ] Java tool shows same order as Ruby script
- [ ] No ordering violations in LRU selection
- [ ] Target bytes calculation is correct
- [ ] Files never accessed (timestamp=0) appear first
- [ ] Recently accessed files appear last or not selected
- [ ] Logs show successful LRU policy execution

---

## Next Steps

After verification:

1. **Monitor automatic eviction**: Wait for scheduled eviction (every 5 minutes by default)
2. **Check metrics endpoint**: `curl http://localhost:8090/metrics`
3. **Review eviction logs**: Look for scheduled eviction runs
4. **Adjust thresholds**: Modify eviction.threshold and eviction.target in hbase-site.xml if needed

---

## Additional Commands Reference

### Quick Status Check
```bash
# Check eviction coordinator status
grep "HDFSTierEvictionCoordinator" logs/hbase-*-regionserver-*.log | tail -5

# Check storage usage
curl http://localhost:8090/metrics 2>/dev/null | python -m json.tool

# Count files by state
echo "scan 'hdfsTier:meta', {COLUMNS => 'transition:currentState'}" | ./bin/hbase shell | grep -E "ACTIVE|EVICTED|COMPACTED" | sort | uniq -c
```

### Emergency Reset (Clear All Eviction Marks)
```bash
# ⚠️ Use with caution - resets all eviction flags
./bin/hbase shell
```

```ruby
scan 'hdfsTier:meta', {COLUMN => 'transition:evicted'} do |row|
  put 'hdfsTier:meta', row, 'transition:evicted', false
end
```

---

**End of Verification Guide**

For issues or questions, check:
- HDFS_TIER_CONFIGURATION_GUIDE.md
- EVICTION_CONFIGURATION.md  
- ARCHITECTURE.md
