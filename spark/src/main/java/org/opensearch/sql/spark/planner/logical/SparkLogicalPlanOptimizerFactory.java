/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.planner.logical;


import lombok.experimental.UtilityClass;
import org.opensearch.sql.planner.optimizer.LogicalPlanOptimizer;
import org.opensearch.sql.spark.planner.logical.rules.MergeAggAndIndexScan;

import java.util.Arrays;

/**
 * Spark storage engine specified logical plan optimizer.
 */
@UtilityClass
public class SparkLogicalPlanOptimizerFactory {

  /**
   * Create Spark storage specified logical plan optimizer.
   */
  public static LogicalPlanOptimizer create() {
    return new LogicalPlanOptimizer(Arrays.asList(
        new MergeAggAndIndexScan()
    ));
  }
}
