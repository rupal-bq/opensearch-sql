package org.opensearch.sql.spark.model;

import lombok.Getter;
import lombok.Setter;
import org.opensearch.sql.data.type.ExprType;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;
import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.EMR_STEP_ID;

@Getter
@Setter
public class SparkResponseFieldNames {
    private String valueFieldName = EMR_STEP_ID;
    private ExprType valueType = STRING;
}
