/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.storage;

import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.INFORMATION_SCHEMA_NAME;
import static org.opensearch.sql.utils.SystemIndexUtils.isSystemIndex;

import java.util.Collection;
import java.util.Collections;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.functions.resolver.SqlTableFunctionResolver;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.utils.SystemIndexUtils;


/**
 * Spark storage engine implementation.
 */
@RequiredArgsConstructor
public class SparkStorageEngine implements StorageEngine {

  private final SparkClient sparkClient;

  @Override
  public Collection<FunctionResolver> getFunctions() {
    return Collections.singletonList(
        new SqlTableFunctionResolver(sparkClient));
  }

  @Override
  public Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName) {
    return new SparkMetricTable(sparkClient, tableName);
  }

}
