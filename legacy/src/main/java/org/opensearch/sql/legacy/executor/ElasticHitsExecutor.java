/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.executor;

import static org.opensearch.search.sort.FieldSortBuilder.DOC_FIELD_NAME;
import static org.opensearch.search.sort.SortOrder.ASC;
import static org.opensearch.sql.common.setting.Settings.Key.SQL_CURSOR_KEEP_ALIVE;
import static org.opensearch.sql.common.setting.Settings.Key.SQL_PAGINATION_API_SEARCH_AFTER;

import java.io.IOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.search.SearchHits;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.sql.legacy.domain.Select;
import org.opensearch.sql.legacy.esdomain.LocalClusterState;
import org.opensearch.sql.legacy.exception.SqlParseException;
import org.opensearch.sql.legacy.pit.PointInTimeHandler;

/** Created by Eliran on 21/8/2016. */
public abstract class ElasticHitsExecutor {

  protected static final Logger LOG = LogManager.getLogger();
  protected PointInTimeHandler pit;
  protected Client client;

  protected abstract void run() throws IOException, SqlParseException;

  protected abstract SearchHits getHits();

  public SearchResponse getResponseWithHits(
      Client client,
      SearchRequestBuilder request,
      Select select,
      int size,
      SearchResponse previousResponse,
      PointInTimeHandler pit) {
    // Set Size
    request.setSize(size);
    SearchResponse responseWithHits;

    if (LocalClusterState.state().getSettingValue(SQL_PAGINATION_API_SEARCH_AFTER)) {
      // Set sort field for search_after
      boolean ordered = select.isOrderdSelect();
      if (!ordered) {
        request.addSort(DOC_FIELD_NAME, ASC);
      }
      // Set PIT
      request.setPointInTime(new PointInTimeBuilder(pit.getPitId()));
      // from and size is alternate method to paginate result.
      // If select has from clause, search after is not required.
      if (previousResponse != null && select.getFrom().isEmpty()) {
        request.searchAfter(previousResponse.getHits().getSortFields());
      }
      responseWithHits = request.get();
    } else {
      // Set scroll
      TimeValue keepAlive = LocalClusterState.state().getSettingValue(SQL_CURSOR_KEEP_ALIVE);
      if (previousResponse != null) {
        responseWithHits =
            client
                .prepareSearchScroll(previousResponse.getScrollId())
                .setScroll(keepAlive)
                .execute()
                .actionGet();
      } else {
        request.setScroll(keepAlive);
        responseWithHits = request.get();
      }
    }

    return responseWithHits;
  }
}
