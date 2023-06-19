/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.storage.model;

import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.TIMESTAMP;
import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.VALUE;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;


@Getter
@Setter
public class SparkResponseFieldNames {

  private String valueFieldName = VALUE;
  private ExprType valueType = DOUBLE;
  private String timestampFieldName = TIMESTAMP;
  private List<NamedExpression> groupByList;

}
