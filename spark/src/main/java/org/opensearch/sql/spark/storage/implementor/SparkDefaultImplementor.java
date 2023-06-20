/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.storage.implementor;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.spark.planner.logical.SparkLogicalMetricScan;
import org.opensearch.sql.spark.storage.SparkMetricScan;
import org.opensearch.sql.spark.storage.querybuilder.SeriesSelectionQueryBuilder;

/**
 * Default Implementor of Logical plan for spark.
 */
@RequiredArgsConstructor
public class SparkDefaultImplementor
    extends DefaultImplementor<SparkMetricScan> {


  @Override
  public PhysicalPlan visitNode(LogicalPlan plan, SparkMetricScan context) {
    if (plan instanceof SparkLogicalMetricScan) {
      return visitIndexScan((SparkLogicalMetricScan) plan, context);
    } else {
      throw new IllegalStateException(StringUtils.format("unexpected plan node type %s",
          plan.getClass()));
    }
  }

  /**
   * Implement SparkLogicalMetricScan.
   */
  public PhysicalPlan visitIndexScan(SparkLogicalMetricScan node,
                                     SparkMetricScan context) {
    String query = SeriesSelectionQueryBuilder.build(node.getMetricName(), node.getFilter());

    context.getRequest().setSql(query);
    context.getRequest();
    return context;
  }

}