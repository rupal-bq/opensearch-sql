/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.spark.client;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.ActionOnFailure;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsRequest;
import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepRequest;
import com.amazonaws.services.elasticmapreduce.model.DescribeStepResult;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.elasticmapreduce.model.StepStatus;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.spark.request.system.model.MetricMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.FLINT_INTEGRATION_JAR;
import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.SPARK_APPLICATION_JAR;
import static org.opensearch.sql.spark.data.constants.SparkFieldConstants.SPARK_INDEX_NAME;

public class SparkClientImpl implements SparkClient {

  private static final Logger logger = LogManager.getLogger(SparkClientImpl.class);

  private final Client client;
  private final String emrCluster;
  private final String accessKey;
  private final String secretKey;
  private final String region;
  private final String opensearchDomainEndpoint;

  public SparkClientImpl(Client client, String cluster, String region, String accessKey, String secretKey, String opensearchDomainEndpoint) {
    this.client = client;
    this.emrCluster = cluster;
    this.region = region;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.opensearchDomainEndpoint = opensearchDomainEndpoint;
  }

  @Override
  public JSONObject sql(String query) throws IOException {
    String stepId = runEmrApplication(query);
    return getResultFromOpensearchIndex(stepId);
  }

  @Override
  public List<String> getLabels(String metricName) throws IOException {
    //TODO get schema from opensearch index
    return toListOfLabels(new JSONArray());
  }

  @Override
  public Map<String, List<MetricMetadata>> getAllMetrics() throws IOException {
    // TODO
    JSONObject jsonObject = new JSONObject();
    TypeReference<HashMap<String, List<MetricMetadata>>> typeRef
        = new TypeReference<>() {};
    return new ObjectMapper().readValue(jsonObject.getJSONObject("data").toString(), typeRef);
  }

  private List<String> toListOfLabels(JSONArray array) {
    List<String> result = new ArrayList<>();
    for (int i = 0; i < array.length(); i++) {
      //__name__ is internal label in spark representing the metric name.
      //Exempting this from labels list as it is not required in any of the operations.
      if (!"__name__".equals(array.optString(i))) {
        result.add(array.optString(i));
      }
    }
    return result;
  }

  private String runEmrApplication(String query) {
    AmazonElasticMapReduce emrClient = AmazonElasticMapReduceClientBuilder.standard()
            .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
            .withRegion(Regions.US_WEST_2)
            .build();

    HadoopJarStepConfig stepConfig = new HadoopJarStepConfig()
            .withJar("command-runner.jar")
            .withArgs("spark-submit",
                    "--class","org.opensearch.sql.SQLJob",
                    "--jars",FLINT_INTEGRATION_JAR,
                    SPARK_APPLICATION_JAR,
                    query,
                    SPARK_INDEX_NAME,
                    opensearchDomainEndpoint,
                    "-1",
                    "https",
                    "sigv4",
                    region
            );

    StepConfig emrstep = new StepConfig()
            .withName("Spark Application Step")
            .withActionOnFailure(ActionOnFailure.CONTINUE)
            .withHadoopJarStep(stepConfig);

    AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
            .withJobFlowId(emrCluster)
            .withSteps(emrstep);

    AddJobFlowStepsResult result = emrClient.addJobFlowSteps(request);
    logger.debug("Spark application step IDs: " + result.getStepIds());

    String stepId = result.getStepIds().get(0);
    // Create the DescribeStepRequest
    DescribeStepRequest stepRequest = new DescribeStepRequest()
            .withClusterId(emrCluster)
            .withStepId(stepId);

    // Wait for the step to complete
    boolean completed = false;
    while (!completed) {
      // Get the step status
      DescribeStepResult stepResult = emrClient.describeStep(stepRequest);
      StepStatus statusDetail = stepResult.getStep().getStatus();
      // Check if the step has completed
      if (statusDetail.getState().equals("COMPLETED")) {
        completed = true;
        logger.debug("EMR step completed successfully.");
      } else if (statusDetail.getState().equals("FAILED") || statusDetail.getState().equals("CANCELLED")) {
        completed = true;
        logger.error("EMR step failed or cancelled.");
      } else {
        // Sleep for some time before checking the status again
        try {
          Thread.sleep(2500);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    // Close the EMR client
    emrClient.shutdown();
    return stepId;
  }

  private JSONObject getResultFromOpensearchIndex(String stepId) {
    return searchInSparkIndex(QueryBuilders.termQuery("stepId.keyword", stepId));
  }

  private JSONObject searchInSparkIndex(QueryBuilder query) {
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(SPARK_INDEX_NAME);
    SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
    searchSourceBuilder.query(query);
    searchRequest.source(searchSourceBuilder);
    ActionFuture<SearchResponse> searchResponseActionFuture;
    try (ThreadContext.StoredContext ignored = client.threadPool().getThreadContext()
            .stashContext()) {
      searchResponseActionFuture = client.search(searchRequest);
    }
    SearchResponse searchResponse = searchResponseActionFuture.actionGet();
    if (searchResponse.status().getStatus() != 200) {
      throw new RuntimeException("Fetching result from .query_execution_result index failed with status : "
              + searchResponse.status());
    } else {
      JSONObject data = new JSONObject();
      for(SearchHit searchHit : searchResponse.getHits().getHits()) {
        data.put("data", searchHit.getSourceAsMap());
      }
      return data;
    }
  }
}