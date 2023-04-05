/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.cloudwatch.storage;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.INFORMATION_SCHEMA_NAME;
import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;

import java.util.Collection;
import java.util.Collections;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.cloudwatch.client.CloudWatchClient;
import org.opensearch.sql.cloudwatch.functions.resolver.QueryRangeTableFunctionResolver;
import org.opensearch.sql.cloudwatch.storage.system.CloudWatchSystemTable;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.SystemIndexUtils;


/**
 * CloudWatch storage engine implementation.
 */
@RequiredArgsConstructor
public class CloudWatchStorageEngine implements StorageEngine {

  private final CloudWatchClient cloudwatchClient;

  @Override
  public Collection<FunctionResolver> getFunctions() {
    return Collections.singletonList(
        new QueryRangeTableFunctionResolver(cloudwatchClient));
  }

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName) {
    if (isSystemIndex(tableName)) {
      return new CloudWatchSystemTable(cloudwatchClient, dataSourceSchemaName, tableName);
    } else if (INFORMATION_SCHEMA_NAME.equals(dataSourceSchemaName.getSchemaName())) {
      return resolveInformationSchemaTable(dataSourceSchemaName, tableName);
    } else {
      return new CloudWatchMetricTable(cloudwatchClient, tableName);
    }
  }

  private Table resolveInformationSchemaTable(DataSourceSchemaName dataSourceSchemaName,
                                              String tableName) {
    if (SystemIndexUtils.TABLE_NAME_FOR_TABLES_INFO.equals(tableName)) {
      return new CloudWatchSystemTable(cloudwatchClient,
          dataSourceSchemaName, SystemIndexUtils.TABLE_INFO);
    } else {
      throw new SemanticCheckException(
          String.format("Information Schema doesn't contain %s table", tableName));
    }
  }


}
