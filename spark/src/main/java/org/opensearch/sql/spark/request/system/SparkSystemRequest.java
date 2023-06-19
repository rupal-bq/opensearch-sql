/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.request.system;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;

/**
 * Spark system request query to get metadata Info.
 */
public interface SparkSystemRequest {

  /**
   * Search.
   *
   * @return list of ExprValue.
   */
  List<ExprValue> search();

}
