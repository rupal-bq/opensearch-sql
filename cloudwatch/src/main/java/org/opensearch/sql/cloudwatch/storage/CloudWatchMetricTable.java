/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.cloudwatch.storage;

import lombok.Getter;
import org.opensearch.sql.cloudwatch.client.CloudWatchClient;
import org.opensearch.sql.cloudwatch.planner.logical.CloudWatchLogicalPlanOptimizerFactory;
import org.opensearch.sql.cloudwatch.request.CloudWatchQueryRequest;
import org.opensearch.sql.cloudwatch.request.system.CloudWatchDescribeMetricRequest;
import org.opensearch.sql.cloudwatch.storage.implementor.CloudWatchDefaultImplementor;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.sql.cloudwatch.data.constants.CloudWatchFieldConstants.LABELS;

/**
 * CloudWatch table (metric) implementation.
 * This can be constructed from  a metric Name
 * or from CloudWatchQueryRequest In case of query_range table function.
 */
public class CloudWatchMetricTable implements Table {

  private final CloudWatchClient cloudwatchClient;

  @Getter
  private final String metricName;

  @Getter
  private final CloudWatchQueryRequest cloudwatchQueryRequest;


  /**
   * The cached mapping of field and type in index.
   */
  private Map<String, ExprType> cachedFieldTypes = null;

  /**
   * Constructor only with metric name.
   */
  public CloudWatchMetricTable(CloudWatchClient cloudwatchService, @Nonnull String metricName) {
    this.cloudwatchClient = cloudwatchService;
    this.metricName = metricName;
    this.cloudwatchQueryRequest = null;
  }

  /**
   * Constructor for entire promQl Request.
   */
  public CloudWatchMetricTable(CloudWatchClient cloudwatchService,
                               @Nonnull CloudWatchQueryRequest cloudwatchQueryRequest) {
    this.cloudwatchClient = cloudwatchService;
    this.metricName = null;
    this.cloudwatchQueryRequest = cloudwatchQueryRequest;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException(
        "CloudWatch metric exists operation is not supported");
  }

  @Override
  public void create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException(
        "CloudWatch metric create operation is not supported");
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      if (metricName != null) {
        cachedFieldTypes =
            new CloudWatchDescribeMetricRequest(cloudwatchClient, null,
                metricName).getFieldTypes();
      } else {
        cachedFieldTypes = new HashMap<>(CloudWatchMetricDefaultSchema.DEFAULT_MAPPING
            .getMapping());
        cachedFieldTypes.put(LABELS, ExprCoreType.STRING);
      }
    }
    return cachedFieldTypes;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    CloudWatchMetricScan metricScan =
        new CloudWatchMetricScan(cloudwatchClient, metricName);
    if (cloudwatchQueryRequest != null) {
      metricScan.setRequest(cloudwatchQueryRequest);
      metricScan.setIsQueryRangeFunctionScan(Boolean.TRUE);
    }
    return plan.accept(new CloudWatchDefaultImplementor(), metricScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return CloudWatchLogicalPlanOptimizerFactory.create().optimize(plan);
  }

}