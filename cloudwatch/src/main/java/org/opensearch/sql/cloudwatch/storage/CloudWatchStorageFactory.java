/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.cloudwatch.storage;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import okhttp3.OkHttpClient;
import org.opensearch.sql.cloudwatch.authinterceptors.AwsSigningInterceptor;
import org.opensearch.sql.cloudwatch.client.CloudWatchClient;
import org.opensearch.sql.cloudwatch.client.CloudWatchClientImpl;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.datasource.model.auth.AuthenticationType;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

import java.net.URI;
import java.net.URISyntaxException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class CloudWatchStorageFactory implements DataSourceFactory {

  public static final String URI = "cloudwatch.uri";
  public static final String AUTH_TYPE = "cloudwatch.auth.type";
  public static final String USERNAME = "cloudwatch.auth.username";
  public static final String PASSWORD = "cloudwatch.auth.password";
  public static final String REGION = "cloudwatch.auth.region";
  public static final String ACCESS_KEY = "cloudwatch.auth.access_key";
  public static final String SECRET_KEY = "cloudwatch.auth.secret_key";

  @Override
  public DataSourceType getDataSourceType() {
    return DataSourceType.CLOUDWATCH;
  }

  @Override
  public DataSource createDataSource(DataSourceMetadata metadata) {
    return new DataSource(
        metadata.getName(),
        DataSourceType.CLOUDWATCH,
        getStorageEngine(metadata.getName(), metadata.getProperties()));
  }

  StorageEngine getStorageEngine(String catalogName, Map<String, String> requiredConfig) {
    validateFieldsInConfig(requiredConfig, Set.of(URI));
    CloudWatchClient cloudwatchClient;
    cloudwatchClient =
        AccessController.doPrivileged((PrivilegedAction<CloudWatchClientImpl>) () -> {
          try {
            return new CloudWatchClientImpl(getHttpClient(requiredConfig),
                new URI(requiredConfig.get(URI)));
          } catch (URISyntaxException e) {
            throw new RuntimeException(
                String.format("CloudWatch Client creation failed due to: %s", e.getMessage()));
          }
        });
    return new CloudWatchStorageEngine(cloudwatchClient);
  }


  private OkHttpClient getHttpClient(Map<String, String> config) {
    OkHttpClient.Builder okHttpClient = new OkHttpClient.Builder();
    okHttpClient.callTimeout(1, TimeUnit.MINUTES);
    okHttpClient.connectTimeout(30, TimeUnit.SECONDS);
    if (config.get(AUTH_TYPE) != null) {
      AuthenticationType authenticationType = AuthenticationType.get(config.get(AUTH_TYPE));
      if (AuthenticationType.AWSSIGV4AUTH.equals(authenticationType)) {
        validateFieldsInConfig(config, Set.of(REGION, ACCESS_KEY, SECRET_KEY));
        okHttpClient.addInterceptor(new AwsSigningInterceptor(
            new AWSStaticCredentialsProvider(
                new BasicAWSCredentials(config.get(ACCESS_KEY), config.get(SECRET_KEY))),
            config.get(REGION), "monitoring"));
      } else {
        throw new IllegalArgumentException(
            String.format("AUTH Type : %s is not supported with CloudWatch Connector",
                config.get(AUTH_TYPE)));
      }
    }
    return okHttpClient.build();
  }

  private void validateFieldsInConfig(Map<String, String> config, Set<String> fields) {
    Set<String> missingFields = new HashSet<>();
    for (String field : fields) {
      if (!config.containsKey(field)) {
        missingFields.add(field);
      }
    }
    if (missingFields.size() > 0) {
      throw new IllegalArgumentException(String.format(
          "Missing %s fields in the CloudWatch connector properties.", missingFields));
    }
  }


}
