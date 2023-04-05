/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.cloudwatch.planner.logical;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

/**
 * CloudWatch Logical Metric Scan Operation.
 * In an optimized plan this node represents both Relation and Filter Operation.
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class CloudWatchLogicalMetricScan extends LogicalPlan {

  private final String metricName;

  /**
   * Filter Condition.
   */
  private final Expression filter;

  /**
   * CloudWatchLogicalMetricScan constructor.
   *
   * @param metricName metricName.
   * @param filter filter.
   */
  @Builder
  public CloudWatchLogicalMetricScan(String metricName,
      Expression filter) {
    super(ImmutableList.of());
    this.metricName = metricName;
    this.filter = filter;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitNode(this, context);
  }

}
