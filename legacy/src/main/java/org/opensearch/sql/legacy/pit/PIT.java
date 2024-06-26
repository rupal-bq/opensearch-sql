/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.pit;

/** Point In Time */
public interface PIT {
  /** Create Point In Time */
  String create();

  /** Delete Point In Time */
  boolean delete();

  /** Get Point In Time Identifier */
  String getPitId();
}
