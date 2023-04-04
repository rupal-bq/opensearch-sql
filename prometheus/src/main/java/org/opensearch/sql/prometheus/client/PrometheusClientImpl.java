/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.client;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class PrometheusClientImpl implements PrometheusClient {

  private static final Logger logger = LogManager.getLogger(PrometheusClientImpl.class);

  private final OkHttpClient okHttpClient;

  private final URI uri;

  public PrometheusClientImpl(OkHttpClient okHttpClient, URI uri) {
    this.okHttpClient = okHttpClient;
    this.uri = uri;
  }


  @Override
  public JSONObject queryRange(String query, Long start, Long end, String step, String metricName) throws IOException {
    String queryUrl = "https://monitoring.us-west-2.amazonaws.com";
    logger.debug("queryUrl: " + queryUrl);
    MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    RequestBody body = RequestBody.create(JSON, getJsonBody(metricName, query).toString());
    Request request = new Request.Builder()
            .url(queryUrl)
            .post(body)
            .addHeader("X-Amz-Target", "GraniteServiceVersion20100801.GetMetricData")
            .build();
    Response response = this.okHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    return updateQueryResponse(jsonObject, query);
  }

  private JSONObject updateQueryResponse(JSONObject jsonObject, String query){
    JSONObject result = jsonObject.getJSONArray("MetricDataResults").getJSONObject(0);
    if(query != null) {
      String[] tmp = query.substring(query.indexOf('{')+1, query.indexOf('}')).split(",");
      for(String str: tmp) {
        String[] map = str.split("=");
        result.put(map[0].trim(), map[1].trim().replaceAll("\"", ""));
      }
    }
    return result;
  }

  @Override
  public List<String> getLabels(String metricName) throws IOException {
    String queryUrl = "https://monitoring.us-west-2.amazonaws.com";
    logger.debug("queryUrl: " + queryUrl);

    MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    RequestBody body = RequestBody.create(JSON, getJsonBodyForLabels(metricName).toString());
    Request request = new Request.Builder()
        .url(queryUrl)
        .post(body)
        .addHeader("X-Amz-Target", "GraniteServiceVersion20100801.ListMetrics")
        .build();
    Response response = this.okHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    List<String> labels = new ArrayList<>();
    for(Object dimension: jsonObject.getJSONArray("Metrics").getJSONObject(0).getJSONArray("Dimensions")) {
      if(dimension instanceof JSONObject){
        labels.add(((JSONObject) dimension).getString("Name"));
      }
    }
    labels.add("Id");
    labels.add("Label");
    labels.add("StatusCode");
    return labels;
  }

  @Override
  public List<Map<String, String>> getAllMetrics() throws IOException {
    String queryUrl = "https://monitoring.us-west-2.amazonaws.com";
    MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    RequestBody body = RequestBody.create(JSON, "{}");

    logger.debug("queryUrl: " + queryUrl);
    Request request = new Request.Builder()
        .url(queryUrl)
        .post(body)
        .addHeader("X-Amz-Target", "GraniteServiceVersion20100801.ListMetrics")
        .build();
    Response response = this.okHttpClient.newCall(request).execute();
    JSONObject jsonObject = readResponse(response);
    List<Map<String, String>> result = new ArrayList<>();
    for(Object metric: jsonObject.getJSONArray("Metrics")){
      if(metric instanceof JSONObject) {
        Map<String, String> list = new HashMap<>();
        list.put("MetricName", ((JSONObject) metric).getString("MetricName"));
        list.put("Namespace", ((JSONObject) metric).getString("Namespace"));
        result.add(list);
      }
    }
    return result;
  }

  private List<String> toListOfLabels(JSONArray array) {
    List<String> result = new ArrayList<>();
    for (int i = 0; i < array.length(); i++) {
      //__name__ is internal label in prometheus representing the metric name.
      //Exempting this from labels list as it is not required in any of the operations.
      if (!"__name__".equals(array.optString(i))) {
        result.add(array.optString(i));
      }
    }
    return result;
  }

  private JSONObject readResponse(Response response) throws IOException {
    if (response.isSuccessful()) {
      JSONObject jsonObject = new JSONObject(Objects.requireNonNull(response.body()).string());
      return jsonObject;
    } else {
      throw new RuntimeException(
          String.format("Request to Prometheus is Unsuccessful with : %s", Objects.requireNonNull(
              response.body(), "Response body can't be null").string()));
    }
  }

  private JSONObject getJsonBodyForLabels(String metricName){
    JSONObject metric = new JSONObject();
    if(metricName.contains(".")) {
      String[] str = metricName.split("[.]",0);
      metric.put("Namespace", str[0].replace("-", "/"));
      metric.put("MetricName", str[1]);
    } else {
      metric.put("MetricName", metricName);
    }
    return metric;
  }
  private JSONObject getJsonBody(String metricName, String query){

    JSONArray dimensions = new JSONArray();
    if(query != null) {
      String[] tmp = query.substring(query.indexOf('{')+1, query.indexOf('}')).split(",");

      for(String str: tmp) {
        JSONObject dimensionPair = new JSONObject();
        String[] map = str.split("=");
        dimensionPair.put("Name", map[0].trim());
        dimensionPair.put("Value", map[1].trim().replaceAll("\"", ""));
        dimensions.put(dimensionPair);
      }
    }

    JSONObject metric = new JSONObject();
    if(metricName.contains(".")) {
      String[] str = metricName.split("[.]",0);
      metric.put("Namespace", str[0].replace("-", "/"));
      metric.put("MetricName", str[1]);
    } else {
      metric.put("MetricName", metricName);
    }
    metric.put("Dimensions", dimensions);

    JSONObject metricStat = new JSONObject();
    metricStat.put("Metric", metric);
    metricStat.put("Period", 300);
    metricStat.put("Stat", "Average");

    JSONObject queryObject = new JSONObject();
    try {
      queryObject.put("Id", "q1");
      queryObject.put("MetricStat", metricStat);
    } catch (JSONException e) {
      e.printStackTrace();
    }

    JSONArray metricDataQueries = new JSONArray();
    metricDataQueries.put(queryObject);

    JSONObject jsonBody = new JSONObject();
    try {
      jsonBody.put("MetricDataQueries", metricDataQueries);
      jsonBody.put("StartTime", 1674950400);
      jsonBody.put("EndTime", 1680048000);

    } catch (JSONException e) {
      e.printStackTrace();
    }

    return jsonBody;
  }
}