/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cloudwatch.response;

import lombok.NonNull;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.cloudwatch.storage.model.CloudWatchResponseFieldNames;
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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;

public class CloudWatchResponse implements Iterable<ExprValue> {

  private final JSONObject responseObject;

  private final CloudWatchResponseFieldNames cloudwatchResponseFieldNames;

  private final Boolean isQueryRangeFunctionScan;

  /**
   * Constructor.
   *
   * @param responseObject               CloudWatch responseObject.
   * @param cloudwatchResponseFieldNames data model which
   *                                     contains field names for the metric measurement
   *                                     and timestamp fieldName.
   */
  public CloudWatchResponse(JSONObject responseObject,
                            CloudWatchResponseFieldNames cloudwatchResponseFieldNames,
                            Boolean isQueryRangeFunctionScan) {
    this.responseObject = responseObject;
    this.cloudwatchResponseFieldNames = cloudwatchResponseFieldNames;
    this.isQueryRangeFunctionScan = isQueryRangeFunctionScan;
  }

  @NonNull
  @Override
  public Iterator<ExprValue> iterator() {
    List<ExprValue> result = new ArrayList<>();
    for(int i=0; i<responseObject.getJSONArray("Timestamps").length(); i++) {
      LinkedHashMap<String, ExprValue> linkedHashMap = new LinkedHashMap<>();
      for (String key : responseObject.keySet()) {
        if (key.contentEquals("Timestamps")) {
          linkedHashMap.put(cloudwatchResponseFieldNames.getTimestampFieldName(),
                  new ExprTimestampValue(Instant.ofEpochMilli((long) responseObject.getJSONArray("Timestamps").getDouble(i) * 1000)));
        } else if (key.contentEquals("Values")) {
          linkedHashMap.put(cloudwatchResponseFieldNames.getValueFieldName(), getValue(responseObject.getJSONArray("Values"), i,
                  cloudwatchResponseFieldNames.getValueType()));
        } else {
          linkedHashMap.put(key, new ExprStringValue(responseObject.get(key).toString()));
        }
      }
      result.add(new ExprTupleValue(linkedHashMap));
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
    if (this.cloudwatchResponseFieldNames.getGroupByList() == null) {
      return key;
    } else {
      return this.cloudwatchResponseFieldNames.getGroupByList().stream()
          .filter(expression -> expression.getDelegated() instanceof ReferenceExpression)
          .filter(expression
              -> ((ReferenceExpression) expression.getDelegated()).getAttr().equals(key))
          .findFirst()
          .map(NamedExpression::getName)
          .orElse(key);
    }
  }

}
