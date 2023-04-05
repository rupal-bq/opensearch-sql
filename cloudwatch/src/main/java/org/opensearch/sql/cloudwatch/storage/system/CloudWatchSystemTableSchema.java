/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.cloudwatch.storage.system;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprType;

import java.util.Map;

import static org.opensearch.sql.data.type.ExprCoreType.STRING;

@Getter
@RequiredArgsConstructor
public enum CloudWatchSystemTableSchema {

  SYS_TABLE_TABLES(new ImmutableMap.Builder<String, ExprType>()
      .put("TABLE_CATALOG", STRING)
      .put("TABLE_NAMESPACE", STRING)
      .put("TABLE_NAME", STRING)
      .build()),
  SYS_TABLE_MAPPINGS(new ImmutableMap.Builder<String, ExprType>()
      .put("TABLE_CATALOG", STRING)
      .put("TABLE_SCHEMA", STRING)
      .put("TABLE_NAME", STRING)
      .put("COLUMN_NAME", STRING)
      .put("DATA_TYPE", STRING)
      .build());

  private final Map<String, ExprType> mapping;
}
