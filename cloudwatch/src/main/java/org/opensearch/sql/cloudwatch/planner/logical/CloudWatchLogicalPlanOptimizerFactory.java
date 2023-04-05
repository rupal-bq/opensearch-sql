/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cloudwatch.planner.logical;


import java.util.Arrays;
import lombok.experimental.UtilityClass;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.cloudwatch.planner.logical.rules.MergeAggAndIndexScan;
import org.opensearch.sql.cloudwatch.planner.logical.rules.MergeAggAndRelation;
import org.opensearch.sql.cloudwatch.planner.logical.rules.MergeFilterAndRelation;

/**
 * CloudWatch storage engine specified logical plan optimizer.
 */
@UtilityClass
public class CloudWatchLogicalPlanOptimizerFactory {

  /**
   * Create CloudWatch storage specified logical plan optimizer.
   */
  public static LogicalPlanOptimizer create() {
    return new LogicalPlanOptimizer(Arrays.asList(
        new MergeFilterAndRelation(),
        new MergeAggAndIndexScan(),
        new MergeAggAndRelation()
    ));
  }
}
