package org.opensearch.sql.spark.response;

import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.spark.model.SparkResponseFieldNames;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SparkResponse implements Iterable<ExprValue>{
    private final JSONObject responseObject;
    private final SparkResponseFieldNames sparkResponseFieldNames;
    private final Boolean isSqlFunctionScan;

    public SparkResponse(JSONObject responseObject, SparkResponseFieldNames sparkResponseFieldNames, Boolean isSqlFunctionScan) {
        this.responseObject = responseObject;
        this.sparkResponseFieldNames = sparkResponseFieldNames;
        this.isSqlFunctionScan = isSqlFunctionScan;
    }

    @NotNull
    @Override
    public Iterator<ExprValue> iterator() {
        List<ExprValue> result = new ArrayList<>();
        // TODO add result rows
        return result.iterator();
    }
}
