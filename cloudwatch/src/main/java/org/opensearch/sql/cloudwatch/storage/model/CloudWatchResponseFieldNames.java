/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.cloudwatch.storage.model;

import static org.opensearch.sql.data.type.ExprCoreType.DOUBLE;
import static org.opensearch.sql.cloudwatch.data.constants.CloudWatchFieldConstants.TIMESTAMP;
import static org.opensearch.sql.cloudwatch.data.constants.CloudWatchFieldConstants.VALUE;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.expression.NamedExpression;


@Getter
@Setter
public class CloudWatchResponseFieldNames {

  private String valueFieldName = VALUE;
  private ExprType valueType = DOUBLE;
  private String timestampFieldName = TIMESTAMP;
  private List<NamedExpression> groupByList;

}
