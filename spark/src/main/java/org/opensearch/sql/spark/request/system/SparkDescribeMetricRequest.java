/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */


package org.opensearch.sql.spark.request.system;

import static org.opensearch.sql.data.model.ExprValueUtils.stringValue;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.NonNull;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.data.model.ExprTupleValue;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.storage.SparkMetricDefaultSchema;

/**
 * Describe Metric metadata request.
 * This is triggered in case of both query range table function and relation.
 * In case of table function metric name is null.
 */
@ToString(onlyExplicitlyIncluded = true)
public class SparkDescribeMetricRequest implements SparkSystemRequest {

  private final SparkClient sparkClient;

  @ToString.Include
  private final String metricName;

  private final DataSourceSchemaName dataSourceSchemaName;

  private static final Logger LOG = LogManager.getLogger();

  /**
   * Constructor for Spark Describe Metric Request.
   * In case of pass through queries like sql function,
   * metric names are optional.
   *
   * @param sparkClient  sparkClient.
   * @param dataSourceSchemaName dataSourceSchemaName.
   * @param metricName        metricName.
   */
  public SparkDescribeMetricRequest(SparkClient sparkClient,
                                         DataSourceSchemaName dataSourceSchemaName,
                                         @NonNull String metricName) {
    this.sparkClient = sparkClient;
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
        sparkClient.getLabels(metricName)
            .forEach(label -> fieldTypes.put(label, ExprCoreType.STRING));
      } catch (IOException e) {
        LOG.error("Error while fetching labels for {} from spark: {}",
            metricName, e.getMessage());
        throw new RuntimeException(String.format("Error while fetching labels "
            + "for %s from spark: %s", metricName, e.getMessage()));
      }
      return null;
    });
    fieldTypes.putAll(SparkMetricDefaultSchema.DEFAULT_MAPPING.getMapping());
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
    valueMap.put("TABLE_SCHEMA", stringValue(dataSourceSchemaName.getSchemaName()));
    valueMap.put("TABLE_NAME", stringValue(metricName));
    valueMap.put("COLUMN_NAME", stringValue(fieldName));
    valueMap.put("DATA_TYPE", stringValue(fieldType));
    return new ExprTupleValue(valueMap);
  }
}
