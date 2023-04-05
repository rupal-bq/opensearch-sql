/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cloudwatch.storage.system;


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
import org.opensearch.sql.cloudwatch.client.CloudWatchClient;
import org.opensearch.sql.cloudwatch.request.system.CloudWatchDescribeMetricRequest;
import org.opensearch.sql.cloudwatch.request.system.CloudWatchListMetricsRequest;
import org.opensearch.sql.cloudwatch.request.system.CloudWatchSystemRequest;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.SystemIndexUtils;

/**
 * CloudWatch System Table Implementation.
 */
public class CloudWatchSystemTable implements Table {
  /**
   * System Index Name.
   */
  private final Pair<CloudWatchSystemTableSchema, CloudWatchSystemRequest> systemIndexBundle;

  private final DataSourceSchemaName dataSourceSchemaName;

  public CloudWatchSystemTable(
          CloudWatchClient client, DataSourceSchemaName dataSourceSchemaName, String indexName) {
    this.dataSourceSchemaName = dataSourceSchemaName;
    this.systemIndexBundle = buildIndexBundle(client, indexName);
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return systemIndexBundle.getLeft().getMapping();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return plan.accept(new CloudWatchSystemTableDefaultImplementor(), null);
  }

  @VisibleForTesting
  @RequiredArgsConstructor
  public class CloudWatchSystemTableDefaultImplementor
      extends DefaultImplementor<Object> {

    @Override
    public PhysicalPlan visitRelation(LogicalRelation node, Object context) {
      return new CloudWatchSystemTableScan(systemIndexBundle.getRight());
    }
  }

  private Pair<CloudWatchSystemTableSchema, CloudWatchSystemRequest> buildIndexBundle(
          CloudWatchClient client, String indexName) {
    SystemIndexUtils.SystemTable systemTable = systemTable(indexName);
    if (systemTable.isSystemInfoTable()) {
      return Pair.of(CloudWatchSystemTableSchema.SYS_TABLE_TABLES,
          new CloudWatchListMetricsRequest(client, dataSourceSchemaName));
    } else {
      return Pair.of(CloudWatchSystemTableSchema.SYS_TABLE_MAPPINGS,
          new CloudWatchDescribeMetricRequest(client,
              dataSourceSchemaName, systemTable.getTableName()));
    }
  }
}
