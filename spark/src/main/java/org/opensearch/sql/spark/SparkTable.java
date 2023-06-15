package org.opensearch.sql.spark;

import lombok.Getter;
import org.opensearch.sql.spark.client.EMRClient;
import org.opensearch.sql.storage.Table;

public class SparkTable implements Table {
    private final EMRClient emrClient;

    @Getter
    private final SparkQueryRequest sparkQueryRequest;
}
