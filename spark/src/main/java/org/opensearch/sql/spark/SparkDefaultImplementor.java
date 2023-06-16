package org.opensearch.sql.spark;

import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.spark.planner.logical.SparkLogicalScan;

public class SparkDefaultImplementor extends DefaultImplementor<SparkScan> {
    @Override
    public PhysicalPlan visitNode(LogicalPlan plan, SparkScan context) {
        if(plan instanceof SparkScan) {
            return visitIndexScan((SparkScan) plan, context);
        } else {
            throw new IllegalArgumentException(StringUtils.format("unexpected plan node type %s",
                    plan.getClass()));
        }
    }

    /**
     * Implement SparkLogicalScan.
     */
    public PhysicalPlan visitIndexScan(SparkLogicalScan node, SparkScan context) {
        String query = SeriesSelectionQueryBuilder

        return context;
    }
}
