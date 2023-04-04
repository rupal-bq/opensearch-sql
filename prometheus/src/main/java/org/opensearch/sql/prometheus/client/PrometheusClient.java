/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.prometheus.client;

import org.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface PrometheusClient {

  JSONObject queryRange(String query, Long start, Long end, String step, String metricName) throws IOException;

  List<String> getLabels(String metricName) throws IOException;

  List<Map<String, String>> getAllMetrics() throws IOException;
}
