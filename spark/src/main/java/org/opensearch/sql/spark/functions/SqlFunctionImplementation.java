package org.opensearch.sql.spark.functions;

import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.Expression;
import org.opensearch.sql.expression.FunctionExpression;
import org.opensearch.sql.expression.NamedArgumentExpression;
import org.opensearch.sql.expression.env.Environment;
import org.opensearch.sql.expression.function.FunctionName;
import org.opensearch.sql.expression.function.TableFunctionImplementation;
import org.opensearch.sql.spark.client.EMRClient;
import org.opensearch.sql.storage.Table;

import java.util.List;
import java.util.stream.Collectors;

public class SqlFunctionImplementation extends FunctionExpression implements TableFunctionImplementation {

    private final FunctionName functionName;
    private final List<Expression> arguments;
    private final EMRClient emrClient;
    public SqlFunctionImplementation(FunctionName functionName, List<Expression> arguments, EMRClient emrClient) {
        super(functionName, arguments);
        this.functionName = functionName;
        this.arguments = arguments;
        this.emrClient = emrClient;
    }

    @Override
    public ExprValue valueOf(Environment<Expression, ExprValue> valueEnv) {
        throw new UnsupportedOperationException(String.format(
                "Spark defined function [%s] is only "
                        + "supported in SOURCE clause with spark connector catalog",
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
        return new SparkTable(emrClient, buildQueryFromSqlFunction(arguments));
    }

    private SparkQueryRequest buildQueryFromSqlFunction(List<Expression> arguments) {

    }
}
