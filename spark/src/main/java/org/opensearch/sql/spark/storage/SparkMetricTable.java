/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.spark.storage;

import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.LABELS;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.scan.SqlFunctionTableScanBuilder;
import org.opensearch.sql.spark.planner.logical.SparkLogicalPlanOptimizerFactory;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.spark.request.system.SparkDescribeMetricRequest;
import org.opensearch.sql.spark.storage.implementor.SparkDefaultImplementor;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

/**
 * Spark table (metric) implementation.
 * This can be constructed from  a metric Name
 * or from SparkQueryRequest In case of sql table function.
 */
public class SparkMetricTable implements Table {

  private final SparkClient sparkClient;

  @Getter
  private final String metricName;

  @Getter
  private final SparkQueryRequest sparkQueryRequest;


  /**
   * The cached mapping of field and type in index.
   */
  private Map<String, ExprType> cachedFieldTypes = null;

  /**
   * Constructor only with metric name.
   */
  public SparkMetricTable(SparkClient sparkService, @Nonnull String metricName) {
    this.sparkClient = sparkService;
    this.metricName = metricName;
    this.sparkQueryRequest = null;
  }

  /**
   * Constructor for entire promQl Request.
   */
  public SparkMetricTable(SparkClient sparkService,
                               @Nonnull SparkQueryRequest sparkQueryRequest) {
    this.sparkClient = sparkService;
    this.metricName = null;
    this.sparkQueryRequest = sparkQueryRequest;
  }

  @Override
  public boolean exists() {
    throw new UnsupportedOperationException(
        "Spark metric exists operation is not supported");
  }

  @Override
  public void create(Map<String, ExprType> schema) {
    throw new UnsupportedOperationException(
        "Spark metric create operation is not supported");
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    if (cachedFieldTypes == null) {
      if (metricName != null) {
        cachedFieldTypes =
            new SparkDescribeMetricRequest(sparkClient, null,
                metricName).getFieldTypes();
      } else {
        cachedFieldTypes = new HashMap<>(SparkMetricDefaultSchema.DEFAULT_MAPPING
            .getMapping());
        cachedFieldTypes.put(LABELS, ExprCoreType.STRING);
      }
    }
    return cachedFieldTypes;
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    SparkMetricScan metricScan =
        new SparkMetricScan(sparkClient);
    if (sparkQueryRequest != null) {
      metricScan.setRequest(sparkQueryRequest);
      metricScan.setIsSqlFunctionScan(Boolean.TRUE);
    }
    return plan.accept(new SparkDefaultImplementor(), metricScan);
  }

  @Override
  public LogicalPlan optimize(LogicalPlan plan) {
    return SparkLogicalPlanOptimizerFactory.create().optimize(plan);
  }

  //Only handling sql function for now.
  //we need to move PPL implementations to ScanBuilder in future.
  @Override
  public TableScanBuilder createScanBuilder() {
    if (metricName == null) {
      return new SqlFunctionTableScanBuilder(sparkClient, sparkQueryRequest);
    } else {
      return null;
    }
  }
}