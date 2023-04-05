/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.cloudwatch.storage.implementor;

import lombok.RequiredArgsConstructor;
import org.apache.commons.math3.util.Pair;
import org.opensearch.sql.cloudwatch.planner.logical.CloudWatchLogicalMetricAgg;
import org.opensearch.sql.cloudwatch.planner.logical.CloudWatchLogicalMetricScan;
import org.opensearch.sql.cloudwatch.storage.CloudWatchMetricScan;
import org.opensearch.sql.cloudwatch.storage.CloudWatchMetricTable;
import org.opensearch.sql.cloudwatch.storage.model.CloudWatchResponseFieldNames;
import org.opensearch.sql.cloudwatch.storage.querybuilder.AggregationQueryBuilder;
import org.opensearch.sql.cloudwatch.storage.querybuilder.SeriesSelectionQueryBuilder;
import org.opensearch.sql.cloudwatch.storage.querybuilder.StepParameterResolver;
import org.opensearch.sql.cloudwatch.storage.querybuilder.TimeRangeParametersResolver;
import org.opensearch.sql.common.utils.StringUtils;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.span.SpanExpression;
import org.opensearch.sql.planner.DefaultImplementor;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;

import java.util.List;
import java.util.Optional;

/**
 * Default Implementor of Logical plan for cloudwatch.
 */
@RequiredArgsConstructor
public class CloudWatchDefaultImplementor
    extends DefaultImplementor<CloudWatchMetricScan> {


  @Override
  public PhysicalPlan visitNode(LogicalPlan plan, CloudWatchMetricScan context) {
    if (plan instanceof CloudWatchLogicalMetricScan) {
      return visitIndexScan((CloudWatchLogicalMetricScan) plan, context);
    } else if (plan instanceof CloudWatchLogicalMetricAgg) {
      return visitIndexAggregation((CloudWatchLogicalMetricAgg) plan, context);
    } else {
      throw new IllegalStateException(StringUtils.format("unexpected plan node type %s",
          plan.getClass()));
    }
  }

  /**
   * Implement CloudWatchLogicalMetricScan.
   */
  public PhysicalPlan visitIndexScan(CloudWatchLogicalMetricScan node,
                                     CloudWatchMetricScan context) {
    String query = SeriesSelectionQueryBuilder.build(node.getMetricName(), node.getFilter());

    context.getRequest().setPromQl(query);
    setTimeRangeParameters(node.getFilter(), context);
    context.getRequest()
        .setStep(StepParameterResolver.resolve(context.getRequest().getStartTime(),
            context.getRequest().getEndTime(), null));
    return context;
  }

  /**
   * Implement CloudWatchLogicalMetricAgg.
   */
  public PhysicalPlan visitIndexAggregation(CloudWatchLogicalMetricAgg node,
                                            CloudWatchMetricScan context) {
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

    //Since cloudwatch response doesn't have any fieldNames in its output.
    //the field names are sent to CloudWatchResponse constructor via context.
    setCloudWatchResponseFieldNames(node, context);
    return context;
  }

  @Override
  public PhysicalPlan visitRelation(LogicalRelation node,
                                    CloudWatchMetricScan context) {
    CloudWatchMetricTable cloudwatchMetricTable = (CloudWatchMetricTable) node.getTable();
    if (cloudwatchMetricTable.getMetricName() != null) {
      String query = SeriesSelectionQueryBuilder.build(node.getRelationName(), null);
      context.getRequest().setPromQl(query);
      setTimeRangeParameters(null, context);
      context.getRequest()
          .setStep(StepParameterResolver.resolve(context.getRequest().getStartTime(),
              context.getRequest().getEndTime(), null));
    }
    return context;
  }

  private void setTimeRangeParameters(Expression filter, CloudWatchMetricScan context) {
    TimeRangeParametersResolver timeRangeParametersResolver = new TimeRangeParametersResolver();
    Pair<Long, Long> timeRange = timeRangeParametersResolver.resolve(filter);
    context.getRequest().setStartTime(timeRange.getFirst());
    context.getRequest().setEndTime(timeRange.getSecond());
  }

  private void setCloudWatchResponseFieldNames(CloudWatchLogicalMetricAgg node,
                                               CloudWatchMetricScan context) {
    Optional<NamedExpression> spanExpression = getSpanExpression(node.getGroupByList());
    if (spanExpression.isEmpty()) {
      throw new RuntimeException(
          "CloudWatch Catalog doesn't support aggregations without span expression");
    }
    CloudWatchResponseFieldNames cloudwatchResponseFieldNames = new CloudWatchResponseFieldNames();
    cloudwatchResponseFieldNames.setValueFieldName(node.getAggregatorList().get(0).getName());
    cloudwatchResponseFieldNames.setValueType(node.getAggregatorList().get(0).type());
    cloudwatchResponseFieldNames.setTimestampFieldName(spanExpression.get().getNameOrAlias());
    cloudwatchResponseFieldNames.setGroupByList(node.getGroupByList());
    context.setCloudwatchResponseFieldNames(cloudwatchResponseFieldNames);
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