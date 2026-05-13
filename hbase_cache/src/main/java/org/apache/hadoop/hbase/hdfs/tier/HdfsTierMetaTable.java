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

    // Access tracking columns (added for HFile read tracking)
    public static final byte[] COL_LAST_ACCESS = Bytes.toBytes("lastAccess");
    public static final byte[] COL_ACCESS_COUNT = Bytes.toBytes("accessCount");

    // Transition column family qualifiers
    public static final byte[] COL_CURR_STATE = Bytes.toBytes("currState");
    public static final byte[] COL_TRANS_TIME = Bytes.toBytes("transTime");
    public static final byte[] COL_EVICTED = Bytes.toBytes("evicted");
    public static final byte[] COL_EVICT_TIME = Bytes.toBytes("evctTime");

    // Row key delimiter - separates components
    public static final String ROW_KEY_DELIMITER = "#";

    /**
     * Creates row key: {regionEncodedName}#{hfileName}
     *
     * EXAMPLE: "abc123def#file_456.hfile"
     *          ↑ region   ↑ filename
     *
     * SIMPLIFIED: Removed timestamp for easier direct lookups.
     * Row key is now deterministic - one HFile = one row, always.
     *
     * @param regionEncodedName Encoded region name
     * @param hfileName Name of HFile
     * @return row key bytes
     */
    public static byte[] createRowKey(String regionEncodedName, String hfileName) {
        String compositeKey = regionEncodedName
            + ROW_KEY_DELIMITER
            + hfileName;
        return Bytes.toBytes(compositeKey);
    }

    /**
     * Parses composite row key back to components.
     *
     * @param rowKey Composite row key bytes
     * @return Array [regionEncodedName, hfileName]
     */
    public static String[] parseRowKey(byte[] rowKey) {
        String compositeKey = Bytes.toString(rowKey);
        return compositeKey.split(ROW_KEY_DELIMITER);
    }

}
