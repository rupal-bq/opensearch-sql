/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cloudwatch.storage.system;

import java.util.Iterator;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.cloudwatch.request.system.CloudWatchSystemRequest;
import org.opensearch.sql.storage.TableScanOperator;

/**
 * CloudWatch table scan operator.
 */
@RequiredArgsConstructor
@EqualsAndHashCode(onlyExplicitlyIncluded = true, callSuper = false)
@ToString(onlyExplicitlyIncluded = true)
public class CloudWatchSystemTableScan extends TableScanOperator {

  @EqualsAndHashCode.Include
  private final CloudWatchSystemRequest request;

  private Iterator<ExprValue> iterator;

  @Override
  public void open() {
    iterator = request.search().iterator();
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public ExprValue next() {
    return iterator.next();
  }

  @Override
  public String explain() {
    return request.toString();
  }
}
