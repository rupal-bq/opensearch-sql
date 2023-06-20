/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.response;

import lombok.NonNull;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

public class SparkResponse implements Iterable<ExprValue> {

  private final JSONObject responseObject;

  /**
   * Constructor.
   *
   * @param responseObject               Spark responseObject.
   */
  public SparkResponse(JSONObject responseObject) {
    this.responseObject = responseObject;
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

}
