package org.apache.hadoop.hbase.hdfs.tier;

import org.apache.hadoop.conf.Configuration;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;

public class HdfsTierMetricsServlet extends HttpServlet {
  private final HdfsTierStorageMonitor monitor;

  public HdfsTierMetricsServlet(Configuration conf) {
    this.monitor = HdfsTierStorageMonitor.getInstance(conf);
  }

  @Override
  protected void doGet(HttpServletRequest req, HttpServletResponse resp) throws IOException {
    resp.setContentType("application/json");
    resp.setCharacterEncoding("UTF-8");

    PrintWriter out = resp.getWriter();
    out.print(monitor.getMetrics().toJson());
    out.flush();
  }
}

