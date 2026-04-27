package org.apache.hadoop.hbase.hdfs.tier;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

/**
 * Simple HTTP server to expose HdfsTier storage metrics as JSON.
 *
 * Starts on port 8090 and provides endpoints:
 * - GET /metrics - Returns current storage metrics as JSON
 * - GET /health - Health check endpoint
 *
 * This server runs in a background thread and is managed by the HdfsTierObserver.
 */
public class HdfsTierMetricsServer {
  private static final Logger LOG = LoggerFactory.getLogger(HdfsTierMetricsServer.class);

  private static final int DEFAULT_PORT = 8090;
  private static final String CONFIG_KEY_PORT = "hbase.hdfstier.metrics.port";

  private final HttpServer server;
  private final HdfsTierStorageMonitor monitor;
  private final int port;

  /**
   * Creates and starts the metrics HTTP server.
   *
   * @param conf HBase configuration
   * @param monitor Storage monitor instance
   * @throws IOException if server cannot be started
   */
  public HdfsTierMetricsServer(Configuration conf, HdfsTierStorageMonitor monitor)
    throws IOException {
    this.monitor = monitor;
    this.port = conf.getInt(CONFIG_KEY_PORT, DEFAULT_PORT);

    // Create HTTP server
    this.server = HttpServer.create(new InetSocketAddress(port), 0);

    // Register endpoints
    server.createContext("/metrics", new MetricsHandler());
    server.createContext("/health", new HealthHandler());
    server.createContext("/reconcile", new ReconcileHandler());

    // Use default executor (creates a default thread pool)
    server.setExecutor(null);

    // Start server
    server.start();

    LOG.info("HdfsTier Metrics Server started on port {}", port);
    LOG.info("Access metrics at: http://localhost:{}/metrics", port);
  }

  /**
   * Stop the HTTP server gracefully.
   */
  public void stop() {
    if (server != null) {
      server.stop(2); // Wait up to 2 seconds for existing connections
      LOG.info("HdfsTier Metrics Server stopped");
    }
  }

  /**
   * Handler for /metrics endpoint.
   * Returns current storage metrics as JSON.
   */
  private class MetricsHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      try {
        // Add CORS headers to allow dashboard access from any origin
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");

        // Handle OPTIONS preflight request
        if ("OPTIONS".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(204, -1);
          return;
        }

        if (!"GET".equals(exchange.getRequestMethod())) {
          sendResponse(exchange, 405, "Method Not Allowed");
          return;
        }

        // Get current metrics from monitor
        HdfsTierStorageMonitor.StorageMetrics metrics = monitor.getMetrics();
        String jsonResponse = metrics.toJson();

        // Send JSON response
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        sendResponse(exchange, 200, jsonResponse);

        LOG.debug("Served metrics request");
      } catch (Exception e) {
        LOG.error("Error handling metrics request: {}", e.getMessage(), e);
        sendResponse(exchange, 500, "{\"error\": \"Internal Server Error\"}");
      }
    }
  }

  /**
   * Handler for /health endpoint.
   * Simple health check.
   */
  private class HealthHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      try {
        // Add CORS headers
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, OPTIONS");

        if ("OPTIONS".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(204, -1);
          return;
        }

        if (!"GET".equals(exchange.getRequestMethod())) {
          sendResponse(exchange, 405, "Method Not Allowed");
          return;
        }

        String healthResponse = "{\"status\": \"healthy\", \"service\": \"HdfsTier Metrics\"}";
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        sendResponse(exchange, 200, healthResponse);
      } catch (Exception e) {
        LOG.error("Error handling health check: {}", e.getMessage(), e);
        sendResponse(exchange, 500, "{\"status\": \"unhealthy\"}");
      }
    }
  }

  /**
   * Handler for /reconcile endpoint.
   * Manually triggers storage reconciliation.
   */
  private class ReconcileHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      try {
        // Add CORS headers
        exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        exchange.getResponseHeaders().set("Access-Control-Allow-Methods", "POST, OPTIONS");
        exchange.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type");

        if ("OPTIONS".equals(exchange.getRequestMethod())) {
          exchange.sendResponseHeaders(204, -1);
          return;
        }

        if (!"POST".equals(exchange.getRequestMethod())) {
          sendResponse(exchange, 405, "Method Not Allowed - Use POST");
          return;
        }

        // Trigger reconciliation
        monitor.forceReconciliation();

        String response = "{\n" +
          "  \"status\": \"triggered\",\n" +
          "  \"message\": \"Storage reconciliation triggered. Check /metrics for updated values.\"\n" +
          "}";

        exchange.getResponseHeaders().set("Content-Type", "application/json");
        sendResponse(exchange, 200, response);

        LOG.info("Manual reconciliation triggered via HTTP endpoint");
      } catch (Exception e) {
        LOG.error("Error handling reconciliation request: {}", e.getMessage(), e);
        sendResponse(exchange, 500, "{\"error\": \"Failed to trigger reconciliation\"}");
      }
    }
  }

  /**
   * Send HTTP response with given status code and body.
   */
  private void sendResponse(HttpExchange exchange, int statusCode, String response)
    throws IOException {
    byte[] responseBytes = response.getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(statusCode, responseBytes.length);

    try (OutputStream os = exchange.getResponseBody()) {
      os.write(responseBytes);
    }
  }

  /**
   * Get the port the server is running on.
   */
  public int getPort() {
    return port;
  }
}
