package org.apache.hadoop.hbase.hdfs.tier.eviction;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory for creating EvictionPolicy instances based on configuration.
 *
 * PLUGGABLE DESIGN:
 * - Reads policy name from configuration
 * - Instantiates appropriate policy implementation
 * - Supports custom policies via class name
 *
 * CONFIGURATION:
 * - hbase.hdfstier.eviction.policy = "LRU" | "LFU" | "FIFO" | <fully-qualified-class-name>
 */
public class EvictionPolicyFactory {

  private static final Logger LOG = LoggerFactory.getLogger(EvictionPolicyFactory.class);

  public static final String EVICTION_POLICY_KEY = "hbase.hdfstier.eviction.policy";
  public static final String DEFAULT_POLICY = "LRU";

  /**
   * Create eviction policy instance from configuration.
   *
   * @param conf HBase configuration
   * @return EvictionPolicy instance
   * @throws RuntimeException if policy cannot be created
   */
  public static EvictionPolicy createPolicy(Configuration conf) {
    String policyName = conf.get(EVICTION_POLICY_KEY, DEFAULT_POLICY);

    LOG.info("Creating eviction policy: {}", policyName);

    try {
      // Built-in policies
      switch (policyName.toUpperCase()) {
        case "LRU":
          return new LRUEvictionPolicy(conf);

        case "LFU":
          throw new UnsupportedOperationException("LFU policy not yet implemented");

        case "FIFO":
          throw new UnsupportedOperationException("FIFO policy not yet implemented");

        default:
          // Try to load as custom class
          return loadCustomPolicy(policyName, conf);
      }
    } catch (Exception e) {
      LOG.error("Failed to create eviction policy: {}", policyName, e);
      LOG.warn("Falling back to default LRU policy");
      return new LRUEvictionPolicy(conf);
    }
  }

  /**
   * Load custom eviction policy by class name.
   */
  private static EvictionPolicy loadCustomPolicy(String className, Configuration conf) {
    try {
      Class<?> policyClass = Class.forName(className);

      if (!EvictionPolicy.class.isAssignableFrom(policyClass)) {
        throw new IllegalArgumentException(
            "Class " + className + " does not implement EvictionPolicy interface");
      }

      // Try constructor with Configuration parameter
      try {
        return (EvictionPolicy) policyClass.getConstructor(Configuration.class)
            .newInstance(conf);
      } catch (NoSuchMethodException e) {
        // Try no-arg constructor
        return (EvictionPolicy) policyClass.newInstance();
      }

    } catch (Exception e) {
      throw new RuntimeException("Failed to load custom eviction policy: " + className, e);
    }
  }
}
