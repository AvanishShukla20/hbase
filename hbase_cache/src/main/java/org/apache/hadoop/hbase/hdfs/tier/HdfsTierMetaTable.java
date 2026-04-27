package org.apache.hadoop.hbase.hdfs.tier;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Manages the hdfsTier:meta table schema and creation.
 * This table stores metadata about HFiles stored in hbase_cache directory.
 *
 * Thread-Safety: All public methods are thread-safe and can be called concurrently.
 */
public class HdfsTierMetaTable {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsTierMetaTable.class);

    // Table and column family names
    public static final TableName TABLE_NAME = TableName.valueOf("hdfsTier:meta");
    public static final byte[] CF_INFO = Bytes.toBytes("info");
    public static final byte[] CF_TRANSITION = Bytes.toBytes("transition");

    // Info column family qualifiers
    public static final byte[] COL_HFILE_NAME = Bytes.toBytes("hfileName");
    public static final byte[] COL_REG_NAME = Bytes.toBytes("regName");
    public static final byte[] COL_ENC_REG_NAME = Bytes.toBytes("encRegName");
    public static final byte[] COL_TABLE_NAME = Bytes.toBytes("tableName");
    public static final byte[] COL_COLFAMILY = Bytes.toBytes("colfamily");
    public static final byte[] COL_REG_ID = Bytes.toBytes("regId");
    public static final byte[] COL_PATH = Bytes.toBytes("path");
    public static final byte[] COL_CREATE_TIME = Bytes.toBytes("createTime");
    public static final byte[] COL_SIZE = Bytes.toBytes("size");

    // Transition column family qualifiers
    public static final byte[] COL_CURR_STATE = Bytes.toBytes("currState");
    public static final byte[] COL_TRANS_TIME = Bytes.toBytes("transTime");
    public static final byte[] COL_EVICTED = Bytes.toBytes("evicted");
    public static final byte[] COL_EVICT_TIME = Bytes.toBytes("evctTime");

    // Row key delimiter - separates components
    public static final String ROW_KEY_DELIMITER = "#";

    /**
     * Creates row key: {regionEncodedName}#{hfileName}#{createTimestamp}
     *
     * EXAMPLE: "abc123def#file_456.hfile#1704067200000"
     *          ↑ region   ↑ filename      ↑ timestamp
     *
     * @param regionEncodedName Encoded region name
     * @param hfileName Name of HFile
     * @param createTimestamp Creation timestamp in milliseconds
     * @return row key bytes
     */
    public static byte[] createRowKey(String regionEncodedName, String hfileName, long createTimestamp) {
        String compositeKey = regionEncodedName
            + ROW_KEY_DELIMITER
            + hfileName
            + ROW_KEY_DELIMITER
            + createTimestamp;
        return Bytes.toBytes(compositeKey);
    }

    /**
     * Parses composite row key back to components.
     *
     * @param rowKey Composite row key bytes
     * @return Array [regionEncodedName, hfileName, timestamp_string]
     */
    public static String[] parseRowKey(byte[] rowKey) {
        String compositeKey = Bytes.toString(rowKey);
        return compositeKey.split(ROW_KEY_DELIMITER);
        // Returns: [0]=regionEncodedName, [1]=hfileName, [2]=timestamp
    }

//    /**
//     * Creates the hdfsTier:meta table if it doesn't exist.
//     *
//     * THREAD-SAFETY: This method uses Admin which is thread-safe.
//     * Multiple threads can call this concurrently - HBase Admin handles
//     * synchronization internally.
//     *
//     * @param connection HBase connection (must be thread-safe ConnectionFactory instance)
//     * @throws IOException If table creation fails
//     */
//    public static void createTableIfNotExists(Connection connection) throws IOException {
//        // Admin is AutoCloseable and thread-safe
//        try (Admin admin = connection.getAdmin()) {
//            // tableExists() is thread-safe - HBase handles concurrent checks
//            if (admin.tableExists(TABLE_NAME)) {
//                LOG.info("Table {} already exists", TABLE_NAME);
//                return;
//            }
//
//            // RACE CONDITION HANDLING:
//            // If multiple threads reach here simultaneously, HBase will throw
//            // TableExistsException for all but the first. We catch and ignore it.
//            try {
//                TableDescriptorBuilder tableBuilder = TableDescriptorBuilder.newBuilder(TABLE_NAME);
//
//                // Info CF - stores core metadata about HFile
//                // MaxVersions=1: Only latest metadata matters (immutable after creation)
//                ColumnFamilyDescriptorBuilder infoCfBuilder =
//                    ColumnFamilyDescriptorBuilder.newBuilder(CF_INFO);
//                infoCfBuilder.setMaxVersions(1); // Keep only latest version
//
//                // Transition CF - tracks state and lifecycle
//                // MaxVersions=5: Keep history of state transitions for audit
//                ColumnFamilyDescriptorBuilder transCfBuilder =
//                    ColumnFamilyDescriptorBuilder.newBuilder(CF_TRANSITION);
//                transCfBuilder.setMaxVersions(5); // Keep history of transitions
//
//                tableBuilder.setColumnFamily(infoCfBuilder.build());
//                tableBuilder.setColumnFamily(transCfBuilder.build());
//
//                admin.createTable(tableBuilder.build());
//                LOG.info("Created table {}", TABLE_NAME);
//            } catch (org.apache.hadoop.hbase.TableExistsException e) {
//                // RACE CONDITION RESOLVED:
//                // Another thread created the table between our check and create.
//                // This is safe - we can proceed as if we found it existed.
//                LOG.info("Table {} already created by another thread", TABLE_NAME);
//            }
//        }
//    }
}
