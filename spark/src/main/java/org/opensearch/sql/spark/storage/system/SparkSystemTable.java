/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.storage.system;


import static org.opensearch.sql.utils.SystemIndexUtils.systemTable;

import com.google.common.annotations.VisibleForTesting;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.request.system.SparkDescribeMetricRequest;
import org.opensearch.sql.spark.request.system.SparkListMetricsRequest;
import org.opensearch.sql.spark.request.system.SparkSystemRequest;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.SystemIndexUtils;

/**
 * Spark System Table Implementation.
 */
public class SparkSystemTable implements Table {
  /**
   * System Index Name.
   */
  private final Pair<SparkSystemTableSchema, SparkSystemRequest> systemIndexBundle;

  private final DataSourceSchemaName dataSourceSchemaName;

  public SparkSystemTable(
      SparkClient client, DataSourceSchemaName dataSourceSchemaName, String indexName) {
    this.dataSourceSchemaName = dataSourceSchemaName;
    this.systemIndexBundle = buildIndexBundle(client, indexName);
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return systemIndexBundle.getLeft().getMapping();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new SparkSystemTableDefaultImplementor(), null);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public class SparkSystemTableDefaultImplementor
      extends DefaultImplementor<Object> {

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, Object context) {
      return new SparkSystemTableScan(systemIndexBundle.getRight());
    }
  }

  private Pair<SparkSystemTableSchema, SparkSystemRequest> buildIndexBundle(
      SparkClient client, String indexName) {
    SystemIndexUtils.SystemTable systemTable = systemTable(indexName);
    if (systemTable.isSystemInfoTable()) {
      return Pair.of(SparkSystemTableSchema.SYS_TABLE_TABLES,
          new SparkListMetricsRequest(client, dataSourceSchemaName));
    } else {
      return Pair.of(SparkSystemTableSchema.SYS_TABLE_MAPPINGS,
          new SparkDescribeMetricRequest(client,
              dataSourceSchemaName, systemTable.getTableName()));
    }
  }
}
