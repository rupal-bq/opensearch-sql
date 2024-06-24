/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.cursor;

import org.apache.lucene.search.SortField;
import org.json.JSONObject;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;

import static org.opensearch.sql.common.setting.Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER;

public class Paginate {

  private static final String SCROLL_ID = "s";

  private static final String PIT_ID = "p";

  private static final String SORT_FILEDS = "h";

  /** To get next batch of result with scroll api */
  private String scrollId;

  /** To get Point In Time */
  private String pitId;

  /** To get next batch of result with search after api */
  private SortField[] sortFields;

  public JSONObject getPaginateInfo() {
    JSONObject jsonObject = new JSONObject();
    if (LocalClusterState.state().getSettingValue(SQL_PAGINATION_API_SEARCH_AFTER)) {
      jsonObject.put(PIT_ID, pitId);
      jsonObject.put(SORT_FILEDS, sortFields);
    } else {
      jsonObject.put(SCROLL_ID, scrollId);
    }
    return jsonObject;
  }
}
