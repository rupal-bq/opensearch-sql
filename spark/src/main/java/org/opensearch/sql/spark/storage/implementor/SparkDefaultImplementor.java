/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.storage.implementor;

import java.util.List;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.commons.math3.util.Pair;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.spark.planner.logical.SparkLogicalMetricAgg;
import org.opensearch.sql.spark.planner.logical.SparkLogicalMetricScan;
import org.opensearch.sql.spark.storage.SparkMetricScan;
import org.opensearch.sql.spark.storage.SparkMetricTable;
import org.opensearch.sql.spark.storage.model.SparkResponseFieldNames;
import org.opensearch.sql.spark.storage.querybuilder.AggregationQueryBuilder;
import org.opensearch.sql.spark.storage.querybuilder.SeriesSelectionQueryBuilder;
import org.opensearch.sql.spark.storage.querybuilder.StepParameterResolver;
import org.opensearch.sql.spark.storage.querybuilder.TimeRangeParametersResolver;

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
    } else if (plan instanceof SparkLogicalMetricAgg) {
      return visitIndexAggregation((SparkLogicalMetricAgg) plan, context);
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

    context.getRequest().setPromQl(query);
    setTimeRangeParameters(node.getFilter(), context);
    context.getRequest()
        .setStep(StepParameterResolver.resolve(context.getRequest().getStartTime(),
            context.getRequest().getEndTime(), null));
    return context;
  }

  /**
   * Implement SparkLogicalMetricAgg.
   */
  public PhysicalPlan visitIndexAggregation(SparkLogicalMetricAgg node,
                                            SparkMetricScan context) {
    setTimeRangeParameters(node.getFilter(), context);
    context.getRequest()
        .setStep(StepParameterResolver.resolve(context.getRequest().getStartTime(),
            context.getRequest().getEndTime(), node.getGroupByList()));
    String step = context.getRequest().getStep();
    String seriesSelectionQuery
        = SeriesSelectionQueryBuilder.build(node.getMetricName(), node.getFilter());

    String aggregateQuery
        = AggregationQueryBuilder.build(node.getAggregatorList(),
        node.getGroupByList());

    String finalQuery = String.format(aggregateQuery, seriesSelectionQuery + "[" + step + "]");
    context.getRequest().setPromQl(finalQuery);

    //Since spark response doesn't have any fieldNames in its output.
    //the field names are sent to SparkResponse constructor via context.
    setSparkResponseFieldNames(node, context);
    return context;
  }

  @Override
  public PhysicalPlan visitRelation(LogicalRelation node,
                                    SparkMetricScan context) {
    SparkMetricTable sparkMetricTable = (SparkMetricTable) node.getTable();
    if (sparkMetricTable.getMetricName() != null) {
      String query = SeriesSelectionQueryBuilder.build(node.getRelationName(), null);
      context.getRequest().setPromQl(query);
      setTimeRangeParameters(null, context);
      context.getRequest()
          .setStep(StepParameterResolver.resolve(context.getRequest().getStartTime(),
              context.getRequest().getEndTime(), null));
    }
    return context;
  }

  private void setTimeRangeParameters(Expression filter, SparkMetricScan context) {
    TimeRangeParametersResolver timeRangeParametersResolver = new TimeRangeParametersResolver();
    Pair<Long, Long> timeRange = timeRangeParametersResolver.resolve(filter);
    context.getRequest().setStartTime(timeRange.getFirst());
    context.getRequest().setEndTime(timeRange.getSecond());
  }

  private void setSparkResponseFieldNames(SparkLogicalMetricAgg node,
                                               SparkMetricScan context) {
    Optional<NamedExpression> spanExpression = getSpanExpression(node.getGroupByList());
    if (spanExpression.isEmpty()) {
      throw new RuntimeException(
          "Spark Catalog doesn't support aggregations without span expression");
    }
    SparkResponseFieldNames sparkResponseFieldNames = new SparkResponseFieldNames();
    sparkResponseFieldNames.setValueFieldName(node.getAggregatorList().get(0).getName());
    sparkResponseFieldNames.setValueType(node.getAggregatorList().get(0).type());
    sparkResponseFieldNames.setTimestampFieldName(spanExpression.get().getNameOrAlias());
    sparkResponseFieldNames.setGroupByList(node.getGroupByList());
    context.setSparkResponseFieldNames(sparkResponseFieldNames);
  }

  private Optional<NamedExpression> getSpanExpression(List<NamedExpression> namedExpressionList) {
    if (namedExpressionList == null) {
      return Optional.empty();
    }
    return namedExpressionList.stream()
        .filter(expression -> expression.getDelegated() instanceof SpanExpression)
        .findFirst();
  }


}