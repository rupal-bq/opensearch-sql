/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */


package org.opensearch.sql.cloudwatch.request.system;

import lombok.NonNull;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.cloudwatch.client.CloudWatchClient;
import org.opensearch.sql.cloudwatch.storage.CloudWatchMetricDefaultSchema;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

/**
 * Describe Metric metadata request.
 * This is triggered in case of both query range table function and relation.
 * In case of table function metric name is null.
 */
@ToString(onlyExplicitlyIncluded = true)
public class CloudWatchDescribeMetricRequest implements CloudWatchSystemRequest {

  private final CloudWatchClient cloudwatchClient;

  @ToString.Include
  private final String metricName;

  private final DataSourceSchemaName dataSourceSchemaName;

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Constructor for CloudWatch Describe Metric Request.
   * In case of pass through queries like query_range function,
   * metric names are optional.
   *
   * @param cloudwatchClient  cloudwatchClient.
   * @param dataSourceSchemaName dataSourceSchemaName.
   * @param metricName        metricName.
   */
  public CloudWatchDescribeMetricRequest(CloudWatchClient cloudwatchClient,
                                         DataSourceSchemaName dataSourceSchemaName,
                                         @NonNull String metricName) {
    this.cloudwatchClient = cloudwatchClient;
    this.metricName = metricName;
    this.dataSourceSchemaName = dataSourceSchemaName;
  }


  /**
   * Get the mapping of field and type.
   * Returns labels and default schema fields.
   *
   * @return mapping of field and type.
   */
  public Map<String, ExprType> getFieldTypes() {
    Map<String, ExprType> fieldTypes = new HashMap<>();
    AccessController.doPrivileged((PrivilegedAction<List<Void>>) () -> {
      try {
        cloudwatchClient.getLabels(metricName)
            .forEach(label -> fieldTypes.put(label, ExprCoreType.STRING));
      } catch (IOException e) {
        LOG.error("Error while fetching labels for {} from cloudwatch: {}",
            metricName, e.getMessage());
        throw new RuntimeException(String.format("Error while fetching labels "
            + "for %s from cloudwatch: %s", metricName, e.getMessage()));
      }
      return null;
    });
    fieldTypes.putAll(CloudWatchMetricDefaultSchema.DEFAULT_MAPPING.getMapping());
    return fieldTypes;
  }

  @Override
  public List<ExprValue> search() {
    List<ExprValue> results = new ArrayList<>();
    for (Map.Entry<String, ExprType> entry : getFieldTypes().entrySet()) {
      results.add(row(entry.getKey(), entry.getValue().legacyTypeName().toLowerCase(),
          dataSourceSchemaName));
    }
    return results;
  }

  private ExprTupleValue row(String fieldName, String fieldType,
                             DataSourceSchemaName dataSourceSchemaName) {
    LinkedHashMap<String, ExprValue> valueMap = new LinkedHashMap<>();
    valueMap.put("TABLE_CATALOG", stringValue(dataSourceSchemaName.getDataSourceName()));
    if(metricName.contains(".")) {
      String[] str = metricName.split("[.]",0);
      valueMap.put("TABLE_SCHEMA", stringValue(str[0].replace("-", "/")));
      valueMap.put("TABLE_NAME", stringValue(str[1]));
    } else {
      valueMap.put("TABLE_SCHEMA", stringValue(dataSourceSchemaName.getSchemaName()));
      valueMap.put("TABLE_NAME", stringValue(metricName));
    }
    valueMap.put("COLUMN_NAME", stringValue(fieldName));
    valueMap.put("DATA_TYPE", stringValue(fieldType));
    return new ExprTupleValue(valueMap);
  }
}
