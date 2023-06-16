package org.opensearch.sql.spark.planner.logical;

import com.google.common.collect.ImmutableList;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalPlanNodeVisitor;

@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class SparkLogicalScan extends LogicalPlan {
    @Builder
    public SparkLogicalScan() {
        super(ImmutableList.of());
    }

    @Override
    public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
        return visitor.visitNode(this, context);
    }
}
