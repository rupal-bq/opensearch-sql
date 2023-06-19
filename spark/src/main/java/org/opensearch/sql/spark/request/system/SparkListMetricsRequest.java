/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.request.system;

import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.request.system.model.MetricMetadata;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

@RequiredArgsConstructor
public class SparkListMetricsRequest implements SparkSystemRequest {

  private final SparkClient sparkClient;

  private final DataSourceSchemaName dataSourceSchemaName;

  private static final Logger LOG = LogManager.getLogger();


  @Override
  public List<ExprValue> search() {
    return AccessController.doPrivileged((PrivilegedAction<List<ExprValue>>) () -> {
      try {
        Map<String, List<MetricMetadata>> result = sparkClient.getAllMetrics();
        return result.keySet()
            .stream()
            .map(x -> {
              MetricMetadata metricMetadata = result.get(x).get(0);
              return row(x, metricMetadata.getType(),
                  metricMetadata.getUnit(), metricMetadata.getHelp());
            })
            .collect(Collectors.toList());
      } catch (IOException e) {
        LOG.error("Error while fetching metric list for from spark: {}",
            e.getMessage());
        throw new RuntimeException(String.format("Error while fetching metric list "
            + "for from spark: %s", e.getMessage()));
      }
    });

  }

  private ExprTupleValue row(String metricName, String tableType, String unit, String help) {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    valueMap.put("TABLE_CATALOG", stringValue(dataSourceSchemaName.getDataSourceName()));
    valueMap.put("TABLE_SCHEMA", stringValue("default"));
    valueMap.put("TABLE_NAME", stringValue(metricName));
    valueMap.put("TABLE_TYPE", stringValue(tableType));
    valueMap.put("UNIT", stringValue(unit));
    valueMap.put("REMARKS", stringValue(help));
    return new ExprTupleValue(valueMap);
  }
}
