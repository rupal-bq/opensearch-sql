/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.storage;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.client.SparkClientImpl;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Map;

@RequiredArgsConstructor
public class SparkStorageFactory implements DataSourceFactory {

  public static final String CLUSTER = "emr.cluster";
  public static final String AUTH_TYPE = "emr.auth.type";
  public static final String REGION = "emr.auth.region";
  public static final String ROLE_ARN = "emr.auth.role_arn";
  public static final String ACCESS_KEY = "emr.auth.access_key";
  public static final String SECRET_KEY = "emr.auth.secret_key";
  private static final Integer MAX_LENGTH_FOR_CONFIG_PROPERTY = 1000;

  private final Settings settings;

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.SPARK;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(
        metadata.getName(),
        DataSourceType.SPARK,
        getStorageEngine(metadata.getProperties()));
  }


  //Need to refactor to a separate Validator class.
  private void validateDataSourceConfigProperties(Map<String, String> dataSourceMetadataConfig)
      throws IllegalArgumentException {
    // TODO Update validation
    if(dataSourceMetadataConfig.get(CLUSTER) == null
            && dataSourceMetadataConfig.get(AUTH_TYPE) == null
            && dataSourceMetadataConfig.get(REGION) == null
            && dataSourceMetadataConfig.get(ROLE_ARN) == null)
      throw new IllegalArgumentException("Cluster missing");
  }

  StorageEngine getStorageEngine(Map<String, String> requiredConfig) {
    SparkClient sparkClient;
    sparkClient =
        AccessController.doPrivileged((PrivilegedAction<SparkClientImpl>) () -> {
          try {
            validateDataSourceConfigProperties(requiredConfig);
            return new SparkClientImpl(requiredConfig.get(CLUSTER), requiredConfig.get(REGION), requiredConfig.get(ACCESS_KEY), requiredConfig.get(SECRET_KEY));
          } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(
                String.format("Invalid cluster in spark properties: %s", e.getMessage()));
          }
        });
    return new SparkStorageEngine(sparkClient);
  }
}
