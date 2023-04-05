/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.cloudwatch.functions.implementation;

import static org.opensearch.sql.cloudwatch.functions.resolver.QueryRangeTableFunctionResolver.ENDTIME;
import static org.opensearch.sql.cloudwatch.functions.resolver.QueryRangeTableFunctionResolver.QUERY;
import static org.opensearch.sql.cloudwatch.functions.resolver.QueryRangeTableFunctionResolver.STARTTIME;
import static org.opensearch.sql.cloudwatch.functions.resolver.QueryRangeTableFunctionResolver.STEP;

import java.util.List;
import java.util.stream.Collectors;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.exception.ExpressionEvaluationException;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.TableFunctionImplementation;
import org.opensearch.sql.cloudwatch.client.CloudWatchClient;
import org.opensearch.sql.cloudwatch.request.CloudWatchQueryRequest;
import org.opensearch.sql.cloudwatch.storage.CloudWatchMetricTable;
import org.opensearch.sql.storage.Table;

public class QueryRangeFunctionImplementation extends FunctionExpression implements
    TableFunctionImplementation {

  private final FunctionName functionName;
  private final List<Expression> arguments;
  private final CloudWatchClient cloudwatchClient;

  /**
   * Required argument constructor.
   *
   * @param functionName name of the function
   * @param arguments    a list of expressions
   */
  public QueryRangeFunctionImplementation(FunctionName functionName, List<Expression> arguments,
                                          CloudWatchClient cloudwatchClient) {
    super(functionName, arguments);
    this.functionName = functionName;
    this.arguments = arguments;
    this.cloudwatchClient = cloudwatchClient;
  }

  @Override
  public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
    throw new UnsupportedOperationException(String.format(
        "CloudWatch defined function [%s] is only "
            + "supported in SOURCE clause with cloudwatch connector catalog",
        functionName));
  }

  @Override
  public ExprType type() {
    return ExprCoreType.STRUCT;
  }

  @Override
  public String toString() {
    List<String> args = arguments.stream()
        .map(arg -> String.format("%s=%s", ((NamedArgumentExpression) arg)
            .getArgName(), ((NamedArgumentExpression) arg).getValue().toString()))
        .collect(Collectors.toList());
    return String.format("%s(%s)", functionName, String.join(", ", args));
  }

  @Override
  public Table applyArguments() {
    return new CloudWatchMetricTable(cloudwatchClient, buildQueryFromQueryRangeFunction(arguments));
  }

  private CloudWatchQueryRequest buildQueryFromQueryRangeFunction(List<Expression> arguments) {

    CloudWatchQueryRequest cloudwatchQueryRequest = new CloudWatchQueryRequest();
    arguments.forEach(arg -> {
      String argName = ((NamedArgumentExpression) arg).getArgName();
      Expression argValue = ((NamedArgumentExpression) arg).getValue();
      ExprValue literalValue = argValue.valueOf();
      switch (argName) {
        case QUERY:
          cloudwatchQueryRequest
              .setPromQl((String) literalValue.value());
          break;
        case STARTTIME:
          cloudwatchQueryRequest.setStartTime(((Number) literalValue.value()).longValue());
          break;
        case ENDTIME:
          cloudwatchQueryRequest.setEndTime(((Number) literalValue.value()).longValue());
          break;
        case STEP:
          cloudwatchQueryRequest.setStep(literalValue.value().toString());
          break;
        default:
          throw new ExpressionEvaluationException(
              String.format("Invalid Function Argument:%s", argName));
      }
    });
    return cloudwatchQueryRequest;
  }

}
