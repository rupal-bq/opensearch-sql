/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cloudwatch.storage;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.sql.cloudwatch.client.CloudWatchClient;
import org.opensearch.sql.cloudwatch.request.CloudWatchQueryRequest;
import org.opensearch.sql.cloudwatch.response.CloudWatchResponse;
import org.opensearch.sql.cloudwatch.storage.model.CloudWatchResponseFieldNames;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.storage.TableScanOperator;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;

/**
 * CloudWatch metric scan operator.
 */
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class CloudWatchMetricScan extends TableScanOperator {

  private final CloudWatchClient cloudwatchClient;

  @EqualsAndHashCode.Include
  @Getter
  @Setter
  @ToString.Include
  private CloudWatchQueryRequest request;

  private Iterator<ExprValue> iterator;

  @Setter
  @Getter
  private Boolean isQueryRangeFunctionScan = Boolean.FALSE;

  @Setter
  private CloudWatchResponseFieldNames cloudwatchResponseFieldNames;

  @Setter
  private String metricName;

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Constructor.
   *
   * @param cloudwatchClient cloudwatchClient.
   */
  public CloudWatchMetricScan(CloudWatchClient cloudwatchClient, String metricName) {
    this.cloudwatchClient = cloudwatchClient;
    this.metricName = metricName;
    this.request = new CloudWatchQueryRequest();
    this.cloudwatchResponseFieldNames = new CloudWatchResponseFieldNames();
  }

  @Override
  public void open() {
    super.open();
    this.iterator = AccessController.doPrivileged((PrivilegedAction<Iterator<ExprValue>>) () -> {
      try {
        JSONObject responseObject = cloudwatchClient.queryRange(
            request.getPromQl(),
            request.getStartTime(), request.getEndTime(), request.getStep(), metricName);
        return new CloudWatchResponse(responseObject, cloudwatchResponseFieldNames,
            isQueryRangeFunctionScan).iterator();
      } catch (IOException e) {
        LOG.error(e.getMessage());
        throw new RuntimeException("Error fetching data from cloudwatch server. " + e.getMessage());
      }
    });
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public String explain() {
    return getRequest().toString();
  }
}
