/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.spark.storage;

import lombok.Getter;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.scan.SqlFunctionTableScanBuilder;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.spark.storage.implementor.SparkDefaultImplementor;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

/**
 * Spark table (metric) implementation.
 * This can be constructed from  a metric Name
 * or from SparkQueryRequest In case of sql table function.
 */
public class SparkMetricTable implements Table {

  private final SparkClient sparkClient;

  @Getter
  private final SparkQueryRequest sparkQueryRequest;

  /**
   * Constructor for entire Sql Request.
   */
  public SparkMetricTable(SparkClient sparkService,
                               @Nonnull SparkQueryRequest sparkQueryRequest) {
    this.sparkClient = sparkService;
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
    return new HashMap<>();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    SparkMetricScan metricScan =
        new SparkMetricScan(sparkClient);
    if (sparkQueryRequest != null) {
      metricScan.setRequest(sparkQueryRequest);
    }
    return plan.accept(new SparkDefaultImplementor(), metricScan);
  }

  //Only handling sql function for now.
  //we need to move PPL implementations to ScanBuilder in future.
  @Override
  public TableScanBuilder createScanBuilder() {
    return new SqlFunctionTableScanBuilder(sparkClient, sparkQueryRequest);
  }
}