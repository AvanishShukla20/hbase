# HdfsTier Metadata Capture Configuration
# Place these configurations in hbase-site.xml

# ============================================================
# COPROCESSOR REGISTRATION
# ============================================================

# Register HdfsTierObserver as region coprocessor
# This enables automatic metadata capture during flush/compaction
hbase.coprocessor.region.classes=org.apache.hadoop.hbase.hdfs.tier.HdfsTierObserver

# ============================================================
# CACHE TIER CONFIGURATION
# ============================================================

# Base path for hbase_cache directory in HDFS
# Default: /hbase_cache
# This is where cached HFiles are stored with SSD storage policy
hbase.hfile.cache.tier.path=/hbase_cache

# ============================================================
# BUFFERED MUTATOR TUNING (Optional)
# ============================================================

# Write buffer size for metadata capture (default: 4MB)
# Increase for higher flush rates (>100 flushes/sec)
# hbase.client.write.buffer=4194304

# Periodic flush timeout (default: 5000ms = 5 seconds)
# Lower for lower latency, higher for better batching
# hbase.client.write.buffer.periodicflush.ms=5000

# ============================================================
# ASYNC EXECUTOR TUNING (Optional)
# ============================================================

# Core threads for async state updates (default: 2)
# Increase for high compaction rates
# hbase.hdfstier.async.threads.core=2

# Max threads for async state updates (default: 10)
# hbase.hdfstier.async.threads.max=10

# ============================================================
# HBASE CLIENT CONFIGURATION
# ============================================================

# HBase client retries (affects metadata writes)
# Default: 15 retries with exponential backoff
hbase.client.retries.number=15

# RPC timeout for metadata writes (default: 60000ms = 60 sec)
hbase.rpc.timeout=60000

# ============================================================
# MONITORING AND LOGGING
# ============================================================

# Enable debug logging for metadata capture
# Add to log4j.properties:
# log4j.logger.org.apache.hadoop.hbase.hdfs.tier=DEBUG

# ============================================================
# HDFS STORAGE POLICY (for hbase_cache directory)
# ============================================================

# Set SSD storage policy on cache directory
# Run after creating /hbase_cache:
# hdfs storagepolicies -setStoragePolicy -path /hbase_cache -policy SSD

# ============================================================
# USAGE EXAMPLE (hbase-site.xml)
# ============================================================

# <property>
#   <name>hbase.coprocessor.region.classes</name>
#   <value>org.apache.hadoop.hbase.hdfs.tier.HdfsTierObserver</value>
#   <description>Enable HFile metadata capture for cache tier management</description>
# </property>
# 
# <property>
#   <name>hbase.hfile.cache.tier.path</name>
#   <value>/hbase_cache</value>
#   <description>HDFS path for cached HFiles with SSD storage policy</description>
# </property>

# ============================================================
# DEPLOYMENT STEPS
# ============================================================

# 1. Build the JAR containing HdfsTierObserver and HdfsTierMetadataCapture
#    mvn clean package -DskipTests
#
# 2. Copy JAR to HBase lib directory on all RegionServers
#    cp hbase-cache-*.jar $HBASE_HOME/lib/
#
# 3. Add configurations to hbase-site.xml
#
# 4. Create hbase_cache directory in HDFS
#    hdfs dfs -mkdir -p /hbase_cache
#    hdfs dfs -chown hbase:hbase /hbase_cache
#    hdfs dfs -chmod 755 /hbase_cache
#
# 5. Set SSD storage policy
#    hdfs storagepolicies -setStoragePolicy -path /hbase_cache -policy SSD
#
# 6. Create hdfsTier:meta table (done automatically by code)
#    The table is created on first RegionServer start
#
# 7. Restart HBase cluster (rolling restart recommended)
#    $HBASE_HOME/bin/rolling-restart.sh
#
# 8. Verify observer is loaded
#    Check RegionServer logs for: "HdfsTierObserver started successfully"
#
# 9. Monitor metadata capture
#    Watch for log entries: "Captured metadata for HFile"

# ============================================================
# VERIFICATION
# ============================================================

# Check if coprocessor is loaded:
# hbase> describe 'your_table'
# Look for: COPROCESSOR => 'HdfsTierObserver'

# Check metadata table:
# hbase> scan 'hdfsTier:meta', {LIMIT => 10}

# Monitor capture rate:
# grep "Captured metadata" /var/log/hbase/hbase-regionserver.log | wc -l

# Check statistics:
# grep "HdfsTierObserver Statistics" /var/log/hbase/hbase-regionserver.log
