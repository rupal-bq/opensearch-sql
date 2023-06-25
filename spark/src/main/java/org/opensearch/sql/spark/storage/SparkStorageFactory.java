/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.storage;

import lombok.RequiredArgsConstructor;
import org.opensearch.client.Client;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.client.EmrClientImpl;
import org.opensearch.sql.spark.client.LivyClientImpl;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.InvalidParameterException;
import java.security.PrivilegedAction;
import java.util.Map;

import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.EMR;
import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.LIVY;

@RequiredArgsConstructor
public class SparkStorageFactory implements DataSourceFactory {
  private final Client client;
  private final Settings settings;
  public static final String EMR_CLUSTER = "emr.cluster";
  public static final String FLINT_HOST = "spark.datasource.flint.host";
  public static final String FLINT_PORT = "spark.datasource.flint.port";
  public static final String FLINT_SCHEME = "spark.datasource.flint.scheme";
  public static final String FLINT_AUTH = "spark.datasource.flint.auth";
  public static final String FLINT_REGION = "spark.datasource.flint.region";
  public static final String AUTH_TYPE = "emr.auth.type";
  public static final String REGION = "emr.auth.region";
  public static final String ROLE_ARN = "emr.auth.role_arn";
  public static final String ACCESS_KEY = "emr.auth.access_key";
  public static final String SECRET_KEY = "emr.auth.secret_key";
  public static final String LIVY_URI = "livy.uri";
  public static final String CONNECTOR_TYPE = "spark.connector";

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

  private void validateDataSourceConfigProperties(Map<String, String> dataSourceMetadataConfig)
      throws IllegalArgumentException {
    // TODO Update validation
    if(dataSourceMetadataConfig.get(EMR_CLUSTER) == null
            && dataSourceMetadataConfig.get(AUTH_TYPE) == null
            && dataSourceMetadataConfig.get(REGION) == null
            && dataSourceMetadataConfig.get(ROLE_ARN) == null
            && dataSourceMetadataConfig.get(FLINT_HOST) == null)
      throw new IllegalArgumentException("Cluster missing");
  }

  StorageEngine getStorageEngine(Map<String, String> requiredConfig) {
    SparkClient sparkClient;

    if(requiredConfig.get(CONNECTOR_TYPE).equals(LIVY)){
      sparkClient =
              AccessController.doPrivileged((PrivilegedAction<LivyClientImpl>) () -> {
                try {
                  validateDataSourceConfigProperties(requiredConfig);
                  return new LivyClientImpl(
                          client,
                          new URI(requiredConfig.get(LIVY_URI)),
                          requiredConfig.get(FLINT_HOST),
                          requiredConfig.get(FLINT_PORT),
                          requiredConfig.get(FLINT_SCHEME),
                          requiredConfig.get(FLINT_AUTH),
                          requiredConfig.get(FLINT_REGION));
                } catch (IllegalArgumentException e) {
                  throw new IllegalArgumentException(
                          String.format("Invalid cluster in spark properties: %s", e.getMessage()));
                } catch (URISyntaxException e) {
                  throw new RuntimeException(e);
                }
              });
    } else if(requiredConfig.get(CONNECTOR_TYPE).equals(EMR)) {
      sparkClient =
              AccessController.doPrivileged((PrivilegedAction<EmrClientImpl>) () -> {
                try {
                  validateDataSourceConfigProperties(requiredConfig);
                  return new EmrClientImpl(
                          client,
                          requiredConfig.get(EMR_CLUSTER),
                          requiredConfig.get(REGION),
                          requiredConfig.get(ACCESS_KEY),
                          requiredConfig.get(SECRET_KEY),
                          requiredConfig.get(FLINT_HOST),
                          requiredConfig.get(FLINT_PORT),
                          requiredConfig.get(FLINT_SCHEME),
                          requiredConfig.get(FLINT_AUTH),
                          requiredConfig.get(FLINT_REGION));
                } catch (IllegalArgumentException e) {
                  throw new IllegalArgumentException(
                          String.format("Invalid cluster in spark properties: %s", e.getMessage()));
                }
              });

    } else {
      throw new InvalidParameterException("Connector type is invalid");
    }
    return new SparkStorageEngine(sparkClient);
  }
}
