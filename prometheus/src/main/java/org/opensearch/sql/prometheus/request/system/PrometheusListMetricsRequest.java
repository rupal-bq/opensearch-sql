/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.prometheus.request.system;

import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.prometheus.client.PrometheusClient;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

@RequiredArgsConstructor
public class PrometheusListMetricsRequest implements PrometheusSystemRequest {

  private final PrometheusClient prometheusClient;

  private final DataSourceSchemaName dataSourceSchemaName;

  private static final Logger LOG = LogManager.getLogger();


  @Override
  public List<ExprValue> search() {
    return AccessController.doPrivileged((PrivilegedAction<List<ExprValue>>) () -> {
      try {
        List<Map<String, String>> result = prometheusClient.getAllMetrics();
        return result
            .stream()
            .map(x -> {
              return row(x);
            })
            .collect(Collectors.toList());
      } catch (IOException e) {
        LOG.error("Error while fetching metric list for from prometheus: {}",
            e.getMessage());
        throw new RuntimeException(String.format("Error while fetching metric list "
            + "for from prometheus: %s", e.getMessage()));
      }
    });

  }

  private ExprTupleValue row(Map<String, String> metric) {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    valueMap.put("TABLE_CATALOG", stringValue(dataSourceSchemaName.getDataSourceName()));
    valueMap.put("TABLE_NAMESPACE", stringValue(metric.get("Namespace")));
    valueMap.put("TABLE_NAME", stringValue(metric.get("MetricName")));
    return new ExprTupleValue(valueMap);
  }
}
