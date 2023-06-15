package org.opensearch.sql.spark;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.DataSourceSchemaName;
import org.opensearch.sql.expression.function.FunctionResolver;
import org.opensearch.sql.spark.client.EMRClient;
import org.opensearch.sql.spark.functions.SqlFunctionResolver;
import org.opensearch.sql.storage.StorageEngine;
import org.opensearch.sql.storage.Table;

import java.util.Collection;
import java.util.Collections;

@RequiredArgsConstructor
public class SparkDataSourceEngine implements StorageEngine {

    private final EMRClient emrClient;

    @Override
    public Table getTable(DataSourceSchemaName dataSourceSchemaName, String tableName) {
        return null;
    }

    @Override
    public Collection<FunctionResolver> getFunctions() {
        return Collections.singletonList(
                new SqlFunctionResolver(emrClient);
        )
    }
}
