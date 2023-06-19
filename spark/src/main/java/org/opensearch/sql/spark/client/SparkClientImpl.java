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
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
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

  private final String cluster;
  private final String accessKey;
  private final String secretKey;
  private final String region;

  public SparkClientImpl(String cluster, String region, String accessKey, String secretKey) {
    this.cluster = cluster;
    this.region = region;
    this.accessKey = accessKey;
    this.secretKey = secretKey;
  }

  @Override
  public JSONObject sql(String query) throws IOException {
    runEmrApplication(query);
    return getResultFromOpensearchIndex();
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

  private void runEmrApplication(String query) {
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
                    //TODO get public domain endpoint
                    "search-test-for-fidelity-fcmtzib4plbqyh4kbkmurk6acu.us-west-2.es.amazonaws.com",
                    "-1",
                    "https",
                    "sigv4",
                    region
            );

    StepConfig emrstep = new StepConfig()
            .withName("Spark Application Step")
            .withActionOnFailure(ActionOnFailure.CONTINUE)
            .withHadoopJarStep(stepConfig);

    //String clusterId = "j-23PYT6MNJB8AY";

    AddJobFlowStepsRequest request = new AddJobFlowStepsRequest()
            .withJobFlowId(cluster)
            .withSteps(emrstep);

    AddJobFlowStepsResult result = emrClient.addJobFlowSteps(request);
    System.out.println("Spark application step submitted successfully. Step IDs: " + result.getStepIds());

    // Create the DescribeStepRequest
    DescribeStepRequest stepRequest = new DescribeStepRequest()
            .withClusterId(cluster)
            .withStepId(result.getStepIds().get(0));

    // Wait for the step to complete
    boolean completed = false;
    while (!completed) {
      // Get the step status
      DescribeStepResult stepResult = emrClient.describeStep(stepRequest);
      StepStatus statusDetail = stepResult.getStep().getStatus();
      System.out.println("Current status: " + statusDetail.getState());
      // Check if the step has completed
      if (statusDetail.getState().equals("COMPLETED")) {
        completed = true;
        System.out.println("Step completed successfully.");
      } else if (statusDetail.getState().equals("FAILED") || statusDetail.getState().equals("CANCELLED")) {
        completed = true;
        System.out.println("Step failed or cancelled.");
      } else {
        // Sleep for some time before checking the status again
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
    // Close the EMR client
    emrClient.shutdown();
  }

  private JSONObject getResultFromOpensearchIndex() {
    return searchInSparkIndex(QueryBuilders.matchAllQuery());
  }

  private JSONObject searchInSparkIndex(QueryBuilder query) {
    SearchRequest searchRequest = new SearchRequest();
    searchRequest.indices(SPARK_INDEX_NAME);
    // TODO: Get result from spark index
    return new JSONObject();
  }
}