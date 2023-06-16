package org.opensearch.sql.spark;

import lombok.Getter;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.executor.streaming.StreamingSource;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.logical.LogicalWrite;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.spark.client.SparkClient;
import org.opensearch.sql.spark.request.SparkQueryRequest;
import org.opensearch.sql.storage.Table;
import org.opensearch.sql.storage.read.TableScanBuilder;
import org.opensearch.sql.storage.write.TableWriteBuilder;

import javax.annotation.Nonnull;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.EMR_STEP_ID;

public class SparkTable implements Table {
    private final SparkClient sparkClient;

    @Getter
    private final SparkQueryRequest sparkQueryRequest;

    private Map<String, ExprType> cachedFieldTypes = null;

    public SparkTable(SparkClient sparkClient, @Nonnull SparkQueryRequest sparkQueryRequest) {
        this.sparkClient = sparkClient;
        this.sparkQueryRequest = sparkQueryRequest;
    }

    @Override
    public boolean exists() {
        throw new UnsupportedOperationException(
                "Spark table exists operation is not supported");
    }

    @Override
    public void create(Map<String, ExprType> schema) {
        throw new UnsupportedOperationException(
                "Spark create operation is not supported");
    }

    @Override
    public Map<String, ExprType> getFieldTypes() {
        if(cachedFieldTypes == null) {
            cachedFieldTypes = new HashMap<>();
            cachedFieldTypes.put(EMR_STEP_ID, ExprCoreType.STRING);
        }
        return cachedFieldTypes;
    }

    @Override
    public PhysicalPlan implement(LogicalPlan plan) {
        SparkScan sparkScan = new SparkScan(sparkClient);
        if(sparkQueryRequest != null) {
            sparkScan.setRequest(sparkQueryRequest);
            sparkScan.setIsSqlFunctionScan(Boolean.TRUE);
        }
        return plan.accept(new SparkDefaultImplementor(), sparkScan);
    }

    @Override
    public LogicalPlan optimize(LogicalPlan plan) {
        return Table.super.optimize(plan);
    }

    @Override
    public TableScanBuilder createScanBuilder() {
        return Table.super.createScanBuilder();
    }

    @Override
    public TableWriteBuilder createWriteBuilder(LogicalWrite plan) {
        return Table.super.createWriteBuilder(plan);
    }

    @Override
    public StreamingSource asStreamingSource() {
        return Table.super.asStreamingSource();
    }

}
