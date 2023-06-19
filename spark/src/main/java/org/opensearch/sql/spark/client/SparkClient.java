/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import org.json.JSONObject;
import org.opensearch.sql.spark.request.system.model.MetricMetadata;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface SparkClient {

  JSONObject sql(String query) throws IOException;

  List<String> getLabels(String metricName) throws IOException;

  Map<String, List<MetricMetadata>> getAllMetrics() throws IOException;
}
