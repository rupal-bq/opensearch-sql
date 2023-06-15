package org.opensearch.sql.spark;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.datasource.model.DataSource;
import org.opensearch.sql.datasource.model.DataSourceMetadata;
import org.opensearch.sql.datasource.model.DataSourceType;
import org.opensearch.sql.spark.client.EMRClientImpl;
import org.opensearch.sql.storage.DataSourceFactory;
import org.opensearch.sql.storage.StorageEngine;

import java.util.Map;

@RequiredArgsConstructor
public class SparkDataSourceFactory implements DataSourceFactory {
  public static final String CLUSTER = "emr.cluster";
  public static final String AUTH_TYPE = "emr.auth.type";
  public static final String REGION = "emr.auth.region";
  public static final String ROLE_ARN = "emr.auth.role_arn";
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
            getStorageEngine(metadata.getProperties())
    );
  }

  private void validateDataSourceConfigProperties(Map<String, String> dataSourceMetadataConfig)
          throws IllegalArgumentException {
    // Update validation
    if(dataSourceMetadataConfig.get(CLUSTER) == null
            && dataSourceMetadataConfig.get(AUTH_TYPE) == null
            && dataSourceMetadataConfig.get(REGION) == null
            && dataSourceMetadataConfig.get(ROLE_ARN) == null)
      throw new IllegalArgumentException("Cluster missing");
  }

  StorageEngine getStorageEngine(Map<String, String> requiredConfig) {
    /*EMRClient sparkClient;
    sparkClient = AccessController.doPrivileged(PrivilegedAction<EMRClientImpl>) () -> {
      try {
        validateDataSourceConfigProperties(requiredConfig);
        return new EMRClientImpl();
      } catch (IllegalArgumentException e) {
        throw new IllegalArgumentException(e.getMessage());
      }
    });*/
    return new SparkDataSourceEngine(new EMRClientImpl());
  }
}
