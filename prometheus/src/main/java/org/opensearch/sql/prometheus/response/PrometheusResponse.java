/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.response;

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
import org.opensearch.sql.prometheus.storage.model.PrometheusResponseFieldNames;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;

public class PrometheusResponse implements Iterable<ExprValue> {

  private final JSONObject responseObject;

  private final PrometheusResponseFieldNames prometheusResponseFieldNames;

  private final Boolean isQueryRangeFunctionScan;

  /**
   * Constructor.
   *
   * @param responseObject               Prometheus responseObject.
   * @param prometheusResponseFieldNames data model which
   *                                     contains field names for the metric measurement
   *                                     and timestamp fieldName.
   */
  public PrometheusResponse(JSONObject responseObject,
                            PrometheusResponseFieldNames prometheusResponseFieldNames,
                            Boolean isQueryRangeFunctionScan) {
    this.responseObject = responseObject;
    this.prometheusResponseFieldNames = prometheusResponseFieldNames;
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
          linkedHashMap.put(prometheusResponseFieldNames.getTimestampFieldName(),
                  new ExprTimestampValue(Instant.ofEpochMilli((long) responseObject.getJSONArray("Timestamps").getDouble(i) * 1000)));
        } else if (key.contentEquals("Values")) {
          linkedHashMap.put(prometheusResponseFieldNames.getValueFieldName(), getValue(responseObject.getJSONArray("Values"), i,
                  prometheusResponseFieldNames.getValueType()));
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
    if (this.prometheusResponseFieldNames.getGroupByList() == null) {
      return key;
    } else {
      return this.prometheusResponseFieldNames.getGroupByList().stream()
          .filter(expression -> expression.getDelegated() instanceof ReferenceExpression)
          .filter(expression
              -> ((ReferenceExpression) expression.getDelegated()).getAttr().equals(key))
          .findFirst()
          .map(NamedExpression::getName)
          .orElse(key);
    }
  }

}
