package org.opensearch.sql.spark;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.model.SparkResponseFieldNames;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.spark.response.SparkResponse;
import org.opensearch.sql.storage.TableScanOperator;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Iterator;

@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class SparkScan  extends TableScanOperator {

    private final SparkClient sparkClient;

    @EqualsAndHashCode.Include
    @Getter
    @Setter
    @ToString.Include
    private SparkQueryRequest request;

    private Iterator<ExprValue> iterator;
    @Setter
    @Getter
    private Boolean isSqlFunctionScan = Boolean.FALSE;

    @Setter
    private SparkResponseFieldNames sparkResponseFieldNames;
    private static final Logger LOG = LogManager.getLogger();
    public SparkScan(SparkClient sparkClient) {
        this.sparkClient = sparkClient;
        this.request = new SparkQueryRequest();
    }

    @Override
    public void open() {
        super.open();
        this.iterator = AccessController.doPrivileged((PrivilegedAction<Iterator<ExprValue>>) () -> {
          try {
              JSONObject responseObject = sparkClient.sql(request.getQuery());
              return new SparkResponse(responseObject, sparkResponseFieldNames, isSqlFunctionScan).iterator();
          } catch (IOException e) {
              LOG.error(e.getMessage());
              throw new RuntimeException("Error fetching data from spark server"+e.getMessage());
          }
        });
    }

    @Override
    public String explain() {
        return getRequest().toString();
    }

    @Override
    public boolean hasNext() {
        return iterator.hasNext();
    }

    @Override
    public ExprValue next() {
        return iterator.next();
    }
}
