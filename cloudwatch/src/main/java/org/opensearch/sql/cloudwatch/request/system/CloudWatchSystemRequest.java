/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.cloudwatch.request.system;

import java.util.List;
import org.opensearch.sql.data.model.ExprValue;

/**
 * CloudWatch system request query to get metadata Info.
 */
public interface CloudWatchSystemRequest {

  /**
   * Search.
   *
   * @return list of ExprValue.
   */
  List<ExprValue> search();

}
