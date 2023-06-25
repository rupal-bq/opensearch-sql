package org.opensearch.sql.spark.client;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;
import org.opensearch.client.Client;
import org.opensearch.sql.spark.response.SparkResponse;

import java.io.IOException;
import java.net.URI;

import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.APPLICATION_ID_FIELD;
import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.APPLICATION_JAR;
import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.AWS_JAVA_SDK_JAR;
import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.INTEGRATION_JAR;
import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.SPARK_INDEX_NAME;

public class LivyClientImpl implements SparkClient {
    private final Client client;
    private static final Logger logger = LogManager.getLogger(LivyClientImpl.class);
    private final OkHttpClient okHttpClient;
    private final URI livyUri;

    private final String flintHost;
    private final String flintPort;
    private final String flintScheme;
    private final String flintAuth;
    private final String flintRegion;

    private final String field = APPLICATION_ID_FIELD;

    public LivyClientImpl(Client client, URI uri, String flintHost, String flintPort, String flintScheme, String flintAuth, String flintRegion) {
        this.client = client;
        this.okHttpClient = new OkHttpClient();
        this.livyUri = uri;
        this.flintHost = flintHost;
        this.flintPort = flintPort;
        this.flintScheme = flintScheme;
        this.flintAuth = flintAuth;
        this.flintRegion = flintRegion;
    }

    @Override
    public JSONObject sql(String query) throws IOException {
        return new SparkResponse(client, runSparkApplication(query), field).getResultFromOpensearchIndex();
    }

    private String runSparkApplication(String query) {
        try{
            MediaType JSON = MediaType.parse("application/json; charset=utf-8");
            String jsonBody = "{\n" +
                    "  \"file\": \""+APPLICATION_JAR+"\",\n" +
                    "  \"className\": \"org.opensearch.sql.SQLJob\",\n" +
                    "  \"args\": [\""+query+"\",\""+SPARK_INDEX_NAME+"\",\""+flintHost+"\","+flintPort+",\""+flintScheme+"\","+flintAuth+"\",\""+flintRegion+"\"],\n" +
                    "  \"jars\": [\""+INTEGRATION_JAR+"\",\""+AWS_JAVA_SDK_JAR+"\"]\n" +
                    "}";
            RequestBody requestBody = RequestBody.create(jsonBody, JSON);
            Request request = new Request.Builder()
                    .url(livyUri.toURL())
                    .post(requestBody)
                    .build();
            Response response = okHttpClient.newCall(request).execute();
            if (response.isSuccessful()) {
                // Handle successful response
                JSONObject jsonObject = new JSONObject(response.body().string());
                if (jsonObject.has("appId")) {
                    return jsonObject.getString("appId");
                } else {
                    throw new IllegalArgumentException("Spark application id is missing");
                }
            } else {
                // Handle error response
                logger.info("Livy Error: " + response.code() + " - " + response.message());
                throw new Exception("Spark Application execution failed");
            }
        } catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }
}
