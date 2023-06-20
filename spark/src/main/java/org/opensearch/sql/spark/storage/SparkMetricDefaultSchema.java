/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.spark.storage;

import com.google.common.collect.ImmutableMap;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.type.ExprType;

import java.util.Map;

@Getter
@RequiredArgsConstructor
public enum SparkMetricDefaultSchema {

  DEFAULT_MAPPING(new ImmutableMap.Builder<String, ExprType>()
      .build());

  private final Map<String, ExprType> mapping;

}
