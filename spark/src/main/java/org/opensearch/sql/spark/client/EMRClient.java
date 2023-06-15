package org.opensearch.sql.spark.client;

import java.io.IOException;
import org.json.JSONObject;

public interface EMRClient {
    JSONObject sql(String query) throws IOException;
}
