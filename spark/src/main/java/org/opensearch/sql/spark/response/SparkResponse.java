/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.response;

import lombok.NonNull;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprDoubleValue;
import org.opensearch.sql.data.model.ExprIntegerValue;
import org.opensearch.sql.data.model.ExprLongValue;
import org.opensearch.sql.data.model.ExprStringValue;
import org.opensearch.sql.data.model.ExprTimestampValue;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;
import org.opensearch.sql.expression.ReferenceExpression;
import org.opensearch.sql.spark.storage.model.SparkResponseFieldNames;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;

public class SparkResponse implements Iterable<ExprValue> {

  private final JSONObject responseObject;

  private final SparkResponseFieldNames sparkResponseFieldNames;

  private final Boolean isSqlFunctionScan;

  /**
   * Constructor.
   *
   * @param responseObject               Spark responseObject.
   * @param sparkResponseFieldNames data model which
   *                                     contains field names for the metric measurement
   *                                     and timestamp fieldName.
   */
  public SparkResponse(JSONObject responseObject,
                            SparkResponseFieldNames sparkResponseFieldNames,
                            Boolean isSqlFunctionScan) {
    this.responseObject = responseObject;
    this.sparkResponseFieldNames = sparkResponseFieldNames;
    this.isSqlFunctionScan = isSqlFunctionScan;
  }

  @NonNull
  @Override
  public Iterator<ExprValue> iterator() {
    List<ExprValue> result = new ArrayList<>();
    if ("matrix".equals(responseObject.getString("resultType"))) {
      JSONArray itemArray = responseObject.getJSONArray("result");
      for (int i = 0; i < itemArray.length(); i++) {
        JSONObject item = itemArray.getJSONObject(i);
        JSONObject metric = item.getJSONObject("metric");
        JSONArray values = item.getJSONArray("values");
        for (int j = 0; j < values.length(); j++) {
          LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>();
          JSONArray val = values.getJSONArray(j);
          linkedHashMap.put(sparkResponseFieldNames.getTimestampFieldName(),
              new ExprTimestampValue(Instant.ofEpochMilli((long) (val.getDouble(0) * 1000))));
          linkedHashMap.put(sparkResponseFieldNames.getValueFieldName(), getValue(val, 1,
              sparkResponseFieldNames.getValueType()));
          // Concept:
          // {\"instance\":\"localhost:9090\",\"__name__\":\"up\",\"job\":\"spark\"}"
          // This is the label string in the spark response.
          // Q: how do we map this to columns in a table.
          // For queries like source = spark.metric_name | ....
          // we can get the labels list in prior as we know which metric we are working on.
          // In case of commands  like source = spark.sql('promQL');
          // Any arbitrary command can be written and we don't know the labels
          // in the spark response in prior.
          // So for PPL like commands...output structure is @value, @timestamp
          // and each label is treated as a separate column where as in case of sql
          // function irrespective of promQL, the output structure is
          // @value, @timestamp, @labels [jsonfied string of all the labels for a data point]
          insertLabels(linkedHashMap, metric);
          result.add(new ExprTupleValue(linkedHashMap));
        }
      }
    } else {
      throw new RuntimeException(String.format("Unexpected Result Type: %s during Spark "
              + "Response Parsing. 'matrix' resultType is expected",
          responseObject.getString("resultType")));
    }
    return result.iterator();
  }

  private void insertLabels(LinkedHashMap<String, ExprValue> linkedHashMap, JSONObject metric) {
    for (String key : metric.keySet()) {
      linkedHashMap.put(getKey(key), new ExprStringValue(metric.getString(key)));
    }
  }

  private ExprValue getValue(JSONArray jsonArray, Integer index, ExprType exprType) {
    if (INTEGER.equals(exprType)) {
      return new ExprIntegerValue(jsonArray.getInt(index));
    } else if (LONG.equals(exprType)) {
      return new ExprLongValue(jsonArray.getLong(index));
    }
    return new ExprDoubleValue(jsonArray.getDouble(index));
  }

  private String getKey(String key) {
    if (this.sparkResponseFieldNames.getGroupByList() == null) {
      return key;
    } else {
      return this.sparkResponseFieldNames.getGroupByList().stream()
          .filter(expression -> expression.getDelegated() instanceof ReferenceExpression)
          .filter(expression
              -> ((ReferenceExpression) expression.getDelegated()).getAttr().equals(key))
          .findFirst()
          .map(NamedExpression::getName)
          .orElse(key);
    }
  }

}
